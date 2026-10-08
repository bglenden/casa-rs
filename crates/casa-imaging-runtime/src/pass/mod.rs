// SPDX-License-Identifier: LGPL-3.0-or-later
//! The major-cycle pass (plan section 5.4): one traversal of the selected
//! visibilities that accumulates the dirty or residual image terms, and the
//! PSF on the initial pass, for every capability.
//!
//! A pass reads blocks of native rows from a [`BoundedSource`] through a
//! two-slot stream, places each row with the operator's spectral resampler
//! and imaging weights, routes the placements to the owners of a
//! [`Partition`] and accumulates them with the CPU backend: the residual
//! `V − A·m` when a model is present, `V` otherwise. Each wave of planes
//! ([`Residency`]) ends with the per-plane transforms, single-threaded per
//! worker, and hands its [`NormalImages`] to the caller before the next wave
//! starts.

mod block;
mod partition;
mod stream;
mod team;
mod wave;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use casa_imaging_operator::{
    DensityGrid, DensityGridShape, MeasurementOperator, ModeSet, NormalImages, OperatorError,
    PlaneRange, PreparedModelGrids, SampleBuffer, SpectralResampler, WeightingGeneration,
};

pub use block::{NativeBlock, NativeRowHeader, RowAddress};
pub use partition::{Partition, Region, Residency};
pub use team::{WORKER_STACK_BYTES, WorkerTeam};

use stream::stream_blocks;
use wave::Wave;

/// A failure reported by a [`BoundedSource`] or a pass callback.
pub type SourceError = Box<dyn std::error::Error + Send + Sync>;

/// Prepares the model grids of a plane range for prediction.
pub type ModelPreparation<'a> =
    dyn Fn(PlaneRange) -> Result<PreparedModelGrids, PassError> + Sync + 'a;

/// Receives one block and its model visibilities.
pub type VisibilityWrite<'a> =
    dyn FnMut(&NativeBlock, &[num_complex::Complex32]) -> Result<(), SourceError> + 'a;

/// Writes visibilities back to the source during a final pass: each block
/// in source order, with the model visibility of every selected sample,
/// `[row][channel][correlation]`, when `predictions` asks for it and an
/// empty slice otherwise.
pub struct VisibilitySink<'a> {
    /// Whether `write` needs the model visibilities.
    pub predictions: bool,
    /// Receives each block.
    pub write: &'a mut VisibilityWrite<'a>,
}

/// A failure of a major-cycle or density pass.
#[derive(Debug, thiserror::Error)]
pub enum PassError {
    /// A worker team was asked for no workers.
    #[error("a worker team needs at least one worker")]
    Workers,
    /// The measurement operator rejected a row, model or accumulator.
    #[error("measurement operator: {0}")]
    Operator(#[from] OperatorError),
    /// The visibility source failed.
    #[error("visibility source: {0}")]
    Source(#[source] SourceError),
    /// The model grids of a wave could not be prepared.
    #[error("model preparation: {0}")]
    Model(#[source] SourceError),
    /// The consumer of a wave's images failed.
    #[error("wave images: {0}")]
    Images(#[source] SourceError),
    /// The visibility writer failed.
    #[error("visibility write: {0}")]
    VisibilityWrite(#[source] SourceError),
    /// A writing pass must hold every plane at once: a native sample's
    /// prediction can draw on any output channel, and each row is written
    /// once.
    #[error("writing visibilities needs every plane resident")]
    VisibilityWriteWaves,
    /// Not even one plane of the pass fits the memory budget.
    #[error("one plane needs {required} bytes but the pass may use {available}")]
    Memory {
        /// Bytes one plane needs.
        required: u64,
        /// Bytes the pass may use.
        available: u64,
    },
    /// The pass was cancelled at a block boundary.
    #[error("the pass was cancelled")]
    Cancelled,
    /// The source thread panicked or could not start.
    #[error("the visibility source thread failed")]
    ProducerPanicked,
}

/// Cooperative cancellation shared between the caller and running passes;
/// a pass stops at the next block boundary.
#[derive(Clone, Debug, Default)]
pub struct Cancel(Arc<AtomicBool>);

impl Cancel {
    /// A token that is not cancelled.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether cancellation was requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// A re-traversable source of native rows.
///
/// Each wave of a pass begins one traversal; the source may restrict it to
/// the native channels that feed the wave's planes. Rows are delivered in a
/// fixed order, so repeated traversals see the same samples.
pub trait BoundedSource: Send {
    /// Start a traversal covering every native channel that feeds `planes`.
    fn begin(&mut self, planes: PlaneRange) -> Result<(), SourceError>;

    /// Fill `block` with the next rows of the traversal; `Ok(false)` once
    /// the traversal is exhausted.
    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError>;
}

/// One major-cycle pass over the selected visibilities.
pub struct MajorCyclePass<'a> {
    /// The measurement operator.
    pub operator: &'a MeasurementOperator,
    /// Places native rows on the operator's planes.
    pub resampler: &'a SpectralResampler,
    /// The run's imaging weights.
    pub weighting: &'a WeightingGeneration,
    /// Modes accumulated: data and PSF on the initial pass, data later.
    pub modes: ModeSet,
    /// Model grids of a wave's planes; `None` grids the data themselves
    /// (an initial pass without a start model).
    pub model: Option<&'a ModelPreparation<'a>>,
    /// Division of the accumulation among workers.
    pub partition: Partition,
    /// Planes held at once.
    pub residency: Residency,
}

/// What a pass traversed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PassSummary {
    /// Placed samples accumulated, over every wave.
    pub samples: u64,
    /// Source blocks consumed, over every wave.
    pub blocks: u64,
}

/// Run `pass`, handing each wave's normal images to `images` in plane order
/// and, on a final pass that writes visibilities, every block to
/// `visibilities`.
pub fn run_major_cycle(
    pass: &MajorCyclePass<'_>,
    source: &mut dyn BoundedSource,
    team: &WorkerTeam,
    cancel: &Cancel,
    images: &mut dyn FnMut(NormalImages) -> Result<(), PassError>,
    mut visibilities: Option<&mut VisibilitySink<'_>>,
) -> Result<PassSummary, PassError> {
    if visibilities.is_some() && pass.residency != Residency::All {
        return Err(PassError::VisibilityWriteWaves);
    }
    let mut summary = PassSummary::default();
    for planes in pass.residency.waves(pass.operator.basis().planes()) {
        source.begin(planes).map_err(PassError::Source)?;
        let model = pass
            .model
            .map(|prepare| prepare(pass.resampler.model_planes(planes)))
            .transpose()?;
        let mut wave = Wave::new(pass, planes, model.as_ref());
        summary.blocks += stream_blocks(source, cancel, |block| {
            wave.consume(block, team, visibilities.as_deref_mut())
        })?;
        summary.samples += wave.samples();
        images(wave.finish(team)?)?;
    }
    Ok(summary)
}

/// Accumulate the weight-density grid of `shape` over one traversal: each
/// row is placed with [`SpectralResampler::place_density`] on row chunks and
/// the chunks are added in row order.
pub fn run_density_pass(
    operator: &MeasurementOperator,
    resampler: &SpectralResampler,
    shape: DensityGridShape,
    source: &mut dyn BoundedSource,
    team: &WorkerTeam,
    cancel: &Cancel,
) -> Result<DensityGrid, PassError> {
    source
        .begin(PlaneRange::new(0, operator.basis().planes()))
        .map_err(PassError::Source)?;
    let mut grid = DensityGrid::new(shape);
    let mut chunks = (0..team.workers() * 4)
        .map(|_| (0..0, SampleBuffer::new(1)))
        .collect::<Vec<_>>();
    stream_blocks(source, cancel, |block| {
        let rows = block.len();
        let count = chunks.len().min(rows.max(1));
        for (index, (range, _)) in chunks[..count].iter_mut().enumerate() {
            *range = index * rows / count..(index + 1) * rows / count;
        }
        team.for_each_mut(&mut chunks[..count], |_, (range, buffer)| {
            buffer.clear();
            for row in range.clone() {
                resampler.place_density(operator, &block.row(row), &shape, buffer)?;
            }
            Ok::<_, PassError>(())
        })?;
        for (_, buffer) in &chunks[..count] {
            grid.accumulate(&buffer.block());
        }
        Ok(())
    })?;
    Ok(grid)
}
