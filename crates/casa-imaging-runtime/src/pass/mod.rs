// SPDX-License-Identifier: LGPL-3.0-or-later
//! The major-cycle pass (plan section 5.4): one traversal of the selected
//! visibilities that accumulates the dirty or residual image terms, and the
//! PSF on the initial pass, of every image domain, for every capability.
//!
//! A pass reads blocks of native rows, projected on every image domain, from
//! a [`BoundedSource`] through a two-slot stream, places each row with each
//! domain's spectral resampler and the imaging weights on the CPU worker
//! team, routes the placements to the owners of the domain's [`Partition`]
//! and accumulates them with each owner's backend ([`BackendChoice`]): the
//! residual `V − Σ_d A_d·m_d` of every
//! domain's model when a model is present (CASA `SIMapperCollection::degrid`
//! sums every mapper's prediction before `grid` forms the residual), `V`
//! otherwise. Each wave of planes ([`Residency`]) ends with the per-plane
//! transforms, single-threaded per worker, and hands each domain's
//! [`NormalImages`] to the caller before the next wave starts.

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

pub use block::{BlockShape, DomainProjection, NativeBlock, NativeRowHeader, RowAddress};
pub use partition::{Partition, Region, Residency, WaveDemand};
pub use team::{WORKER_STACK_BYTES, WorkerTeam};

use stream::stream_blocks;
use wave::Wave;

/// A failure reported by a [`BoundedSource`] or a pass callback.
pub type SourceError = Box<dyn std::error::Error + Send + Sync>;

/// Prepares the model grids of image domain `domain` over a plane range for
/// prediction.
pub type ModelPreparation<'a> =
    dyn Fn(usize, PlaneRange) -> Result<PreparedModelGrids, PassError> + Sync + 'a;

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
    /// A pass was given no image domain.
    #[error("a pass needs at least one image domain")]
    NoDomains,
    /// The image domains of a pass do not share one plane axis.
    #[error("the image domains of a pass must have the same planes")]
    PlaneAxes,
    /// A row's native channels lie further apart than the spacing that
    /// sized the waves' model halo.
    #[error("native channels {observed_hz} Hz apart exceed the planned spacing {bound_hz} Hz")]
    NativeSpacing {
        /// The widest spacing in the row.
        observed_hz: f64,
        /// The pass's `native_spacing_hz`.
        bound_hz: f64,
    },
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
/// Each wave of a pass begins one traversal. Rows are delivered in a fixed
/// order, so repeated traversals see the same samples.
pub trait BoundedSource: Send {
    /// Start a traversal for a wave over `planes`. With `restrict`, the
    /// source may deliver only the native channels whose samples reach
    /// `planes`, with both interpolation partners of each; without it, it
    /// delivers every selected channel of each row.
    fn begin(&mut self, planes: PlaneRange, restrict: bool) -> Result<(), SourceError>;

    /// Fill `block` with the next rows of the traversal; `Ok(false)` once
    /// the traversal is exhausted.
    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError>;

    /// The largest block any traversal fills, known before one begins: a
    /// pass allocates its per-block storage for it and admits that storage
    /// first ([`WaveDemand::bytes`], [`density_pass_bytes`]).
    fn maximum_block(&self) -> BlockShape;
}

/// One image domain of a pass: its operator, the resampler that places rows
/// on its planes, and the division of its accumulation among workers.
/// Every domain of a pass shares one plane axis.
pub struct PassDomain<'a> {
    /// The measurement operator.
    pub operator: &'a MeasurementOperator,
    /// Places native rows on the operator's planes.
    pub resampler: &'a SpectralResampler,
    /// Division of the accumulation among workers.
    pub partition: Partition,
}

/// The gridding backend every owner of a pass dispatches to.
///
/// With [`BackendChoice::Metal`] each owner grids on the shared Metal
/// device into an `f32` accumulator in device memory (D2), so every
/// operator of the pass must be `f32`; placement and the native-channel
/// predictions of a multi-domain or linearly interpolated residual stay on
/// the CPU workers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendChoice {
    /// [`casa_imaging_operator::CpuBackend`].
    #[default]
    Cpu,
    /// [`casa_imaging_metal::MetalBackend`].
    Metal,
}

/// One major-cycle pass over the selected visibilities.
pub struct MajorCyclePass<'a> {
    /// The image domains; rows arrive projected on each, in this order.
    pub domains: &'a [PassDomain<'a>],
    /// The run's imaging weights.
    pub weighting: &'a WeightingGeneration,
    /// Modes accumulated: data and PSF on the initial pass, data later.
    pub modes: ModeSet,
    /// Model grids of a domain's planes; `None` grids the data themselves
    /// (an initial pass without a start model).
    pub model: Option<&'a ModelPreparation<'a>>,
    /// Planes held at once.
    pub residency: Residency,
    /// The widest spacing between adjacent selected native channels; with
    /// a model it sizes each wave's model halo
    /// ([`SpectralResampler::model_planes`]).
    pub native_spacing_hz: f64,
    /// Where the owners grid.
    pub backend: BackendChoice,
}

/// Whether a pass over `domains` forms its residual at native channels: with
/// a model, when several domains' predictions must be summed before
/// subtracting, or when the resampler interpolates onto the output channels
/// ([`SpectralResampler::forms_native_residuals`]).
fn native_residuals(domains: &[PassDomain<'_>], with_model: bool) -> bool {
    with_model
        && (domains.len() > 1
            || domains
                .iter()
                .any(|domain| domain.resampler.forms_native_residuals()))
}

impl MajorCyclePass<'_> {
    fn native_residuals(&self) -> bool {
        native_residuals(self.domains, self.model.is_some())
    }

    /// Whether a wave must read whole rows: a native-channel prediction under
    /// linear interpolation depends on the whole row's channel map
    /// ([`SpectralResampler::predict_row`]), not only the channels that
    /// reach the wave.
    fn whole_rows(&self) -> bool {
        self.model.is_some()
            && self
                .domains
                .iter()
                .any(|domain| domain.resampler.forms_native_residuals())
    }
}

/// What a pass traversed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PassSummary {
    /// Samples placed on the first image domain, over every wave: the same
    /// count for every pass over the same selection and weights.
    pub samples: u64,
    /// Source blocks consumed, over every wave.
    pub blocks: u64,
}

/// Run `pass`, handing each wave's normal images of each domain to
/// `images` (domain, images) in plane order and, on a final pass that
/// writes visibilities, every block to `visibilities`.
pub fn run_major_cycle(
    pass: &MajorCyclePass<'_>,
    source: &mut dyn BoundedSource,
    team: &WorkerTeam,
    cancel: &Cancel,
    images: &mut dyn FnMut(usize, NormalImages) -> Result<(), PassError>,
    mut visibilities: Option<&mut VisibilitySink<'_>>,
) -> Result<PassSummary, PassError> {
    if visibilities.is_some() && pass.residency != Residency::All {
        return Err(PassError::VisibilityWriteWaves);
    }
    let Some(main) = pass.domains.first() else {
        return Err(PassError::NoDomains);
    };
    let total = main.operator.basis().planes();
    if pass
        .domains
        .iter()
        .any(|domain| domain.operator.basis().planes() != total)
    {
        return Err(PassError::PlaneAxes);
    }
    let restrict = !pass.whole_rows();
    let native_residuals = pass.native_residuals();
    let mut summary = PassSummary::default();
    for planes in pass.residency.waves(total) {
        source.begin(planes, restrict).map_err(PassError::Source)?;
        let models = pass
            .model
            .map(|prepare| {
                pass.domains
                    .iter()
                    .enumerate()
                    .map(|(index, domain)| {
                        prepare(
                            index,
                            domain
                                .resampler
                                .model_planes(planes, pass.native_spacing_hz),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let mut wave = Wave::new(
            pass,
            planes,
            models.as_deref(),
            native_residuals,
            source.maximum_block(),
            team.workers(),
        )?
        .checking_spacing(!restrict && pass.residency != Residency::All);
        summary.blocks += stream_blocks(source, cancel, |block| {
            wave.consume(block, team, visibilities.as_deref_mut())
        })?;
        summary.samples += wave.samples();
        for (domain, domain_images) in wave.finish(team)?.into_iter().enumerate() {
            images(domain, domain_images)?;
        }
    }
    Ok(summary)
}

/// Bytes the passes hold for a source whose largest block is `block`: the
/// native blocks a pass's stream double-buffers and the model visibilities
/// of one block, which a pass with a model predicts. They grow with the
/// source's block, so the source's admission holds them
/// ([`crate::finalize_source_access`]).
#[must_use]
pub const fn source_block_bytes(block: BlockShape) -> u64 {
    stream::bytes(block) + wave::prediction_bytes(block)
}

/// Row chunks per worker of a density pass.
const DENSITY_CHUNKS_PER_WORKER: usize = 4;

/// Placements a row chunk takes at once. A pass places each block in slices
/// of chunks this size, so its sample buffers have one bound whatever rows
/// the source puts in a block, while a slice still spans most of a large
/// block and its workers meet once per slice.
const CHUNK_PLACEMENTS: usize = 1 << 15;

/// Placements a row chunk of a major-cycle pass takes at once when each
/// image domain has at most `owners` owners. Each owner's buffer in a
/// chunk can take every placement of the chunk, and a pass has four chunks
/// per worker, so past four owners a chunk takes proportionally fewer: the
/// chunks' storage then grows linearly with the workers rather than as
/// their square, and a slice places as many samples whatever the workers.
const fn chunk_placements(owners: usize) -> usize {
    if owners <= 4 {
        CHUNK_PLACEMENTS
    } else {
        4 * CHUNK_PLACEMENTS / owners
    }
}

/// Rows a row chunk places at once, for rows of `row` placements each in
/// blocks of at most `block_rows` rows split among `chunks` chunks taking
/// `placements` placements each: as many as fit, at least one, and no more
/// than a block's share.
const fn chunk_rows(row: usize, block_rows: usize, chunks: usize, placements: usize) -> usize {
    let fitting = placements / if row == 0 { 1 } else { row };
    let share = block_rows.div_ceil(if chunks == 0 { 1 } else { chunks });
    let rows = if fitting < share { fitting } else { share };
    if rows == 0 { 1 } else { rows }
}

/// An upper bound of the bytes a pass's row chunks hold per row of the
/// source's block, for rows of `layout` that each place at most `row`
/// samples ([`SpectralResampler::samples_per_row`]) on `workers` workers,
/// while a chunk's rows are a block's share rather than its placement cap:
/// each owner's buffer and the weight owner's can take every placement of
/// a row. The source sizes its blocks with it
/// ([`crate::finalize_source_access`]); [`WaveDemand::bytes`] charges the
/// chunks exactly.
#[must_use]
pub const fn chunk_bytes_per_row(layout: BlockShape, row: usize, workers: usize) -> u64 {
    (workers as u64 + 1) * SampleBuffer::bytes(layout.correlations, row)
}

/// The slices of a block of `rows` rows split among `chunks` chunks of at
/// most `chunk_rows` rows each, in row order.
fn slices(
    rows: usize,
    chunks: usize,
    chunk_rows: usize,
) -> impl Iterator<Item = std::ops::Range<usize>> {
    let step = chunks * chunk_rows;
    (0..rows)
        .step_by(step)
        .map(move |start| start..(start + step).min(rows))
}

/// The chunks of `slice` among at most `chunks` chunks, in row order.
fn chunk_ranges(
    slice: std::ops::Range<usize>,
    chunks: usize,
) -> impl Iterator<Item = std::ops::Range<usize>> {
    let rows = slice.len();
    let count = chunks.min(rows.max(1));
    (0..count).map(move |index| {
        slice.start + index * rows / count..slice.start + (index + 1) * rows / count
    })
}

/// Bytes a density pass over blocks of `block` on `workers` workers holds:
/// its grid of `shape` ([`DensityGridShape::bytes`]), which it returns, and
/// its row chunks' sample buffers, which it frees. The stream's native
/// blocks are the source's ([`stream_bytes`]).
#[must_use]
pub fn density_pass_bytes(
    resampler: &SpectralResampler,
    shape: &DensityGridShape,
    block: BlockShape,
    workers: usize,
) -> u64 {
    let chunks = workers * DENSITY_CHUNKS_PER_WORKER;
    let row = resampler.density_samples_per_row(shape, block.spectrum);
    let placements = chunk_rows(row, block.rows, chunks, CHUNK_PLACEMENTS) * row;
    shape.bytes() + chunks as u64 * SampleBuffer::bytes(1, placements)
}

/// Accumulate the weight-density grid of `shape` over one traversal of the
/// first image domain's projection: each row is placed with
/// [`SpectralResampler::place_density`] on row chunks and the chunks are
/// added in row order. It holds [`density_pass_bytes`] while it runs.
pub fn run_density_pass(
    operator: &MeasurementOperator,
    resampler: &SpectralResampler,
    shape: DensityGridShape,
    source: &mut dyn BoundedSource,
    team: &WorkerTeam,
    cancel: &Cancel,
) -> Result<DensityGrid, PassError> {
    source
        .begin(PlaneRange::new(0, operator.basis().planes()), false)
        .map_err(PassError::Source)?;
    let mut grid = DensityGrid::new(shape);
    let maximum = source.maximum_block();
    let count = team.workers() * DENSITY_CHUNKS_PER_WORKER;
    let row = resampler.density_samples_per_row(&shape, maximum.spectrum);
    let rows_per_chunk = chunk_rows(row, maximum.rows, count, CHUNK_PLACEMENTS);
    let placements = rows_per_chunk * row;
    let mut chunks = (0..count)
        .map(|_| (0..0, SampleBuffer::with_capacity(1, placements)))
        .collect::<Vec<_>>();
    stream_blocks(source, cancel, |block| {
        for slice in slices(block.len(), count, rows_per_chunk) {
            let mut used = 0;
            for ((range, _), rows) in chunks.iter_mut().zip(chunk_ranges(slice, count)) {
                *range = rows;
                used += 1;
            }
            team.for_each_mut(&mut chunks[..used], |_, (range, buffer)| {
                buffer.clear();
                for row in range.clone() {
                    resampler.place_density(operator, &block.row(0, row), &shape, buffer)?;
                }
                debug_assert!(
                    buffer.capacity_bytes() == SampleBuffer::bytes(1, placements),
                    "a density chunk places within the placements it was allocated for"
                );
                Ok::<_, PassError>(())
            })?;
            for (_, buffer) in &chunks[..used] {
                grid.accumulate(&buffer.block());
            }
        }
        Ok(())
    })?;
    Ok(grid)
}
