// SPDX-License-Identifier: LGPL-3.0-or-later
//! One wave of a major-cycle pass: prediction and placement on row chunks,
//! accumulation by owner for each image domain, and the per-plane
//! transforms.

mod pipeline;
mod weight_sums;

use weight_sums::WeightSums;
pub(super) use weight_sums::{GroupCount, WEIGHT_GROUPS};

/// Bytes the weight-image table of one owner holds ([`WeightSums`]) over
/// placements of `npol` correlations.
pub(super) fn weight_sums_bytes(npol: usize) -> u64 {
    WeightSums::bytes(npol, WEIGHT_GROUPS)
}

use std::ops::Range;
use std::sync::PoisonError;

use casa_imaging_metal::MetalBackend;
use casa_imaging_operator::{
    ConvolutionFunctionSet, CpuBackend, GridAccumulator, GridBackend, GridPrecision, Mode,
    NativeModel, NormalImages, Placement, PlaneRange, PreparedModelGrids, SampleBlock,
    SampleBuffer, Work,
};
use num_complex::Complex32;

use super::block::{cell_of, row_at};
use super::partition::Router;
use super::prediction::{BlockPrediction, PredictionDomain, PredictionShape};
use super::{
    BackendChoice, BlockShape, MajorCyclePass, NativeBlock, Partition, PassDomain, PassError,
    VisibilitySink, WeightCache, WorkerTeam, chunk_placements, chunk_ranges, chunk_rows, slices,
};

/// Row chunks per worker in the placement stage; enough to balance rows
/// whose channel counts differ after flagging and the support test.
const CHUNKS_PER_WORKER: usize = 4;

/// One owner's accumulator and backend.
struct Owner<'w> {
    acc: Option<GridAccumulator>,
    backend: OwnerBackend<'w>,
    images: Option<NormalImages>,
    /// Time the owner spent accumulating in the wave, which a region pass
    /// reports beside its strips (#736).
    busy: std::time::Duration,
    /// The weight image's contributions, summed before they are gridded
    /// ([`WeightSums`], #787), when this owner grids the weight image: the
    /// weight owner of a region pass, every owner of a plane pass.
    weights: Option<WeightSums>,
}

impl Owner<'_> {
    /// Wait for the owner's grid work still on the device, reporting
    /// whether it failed.
    fn complete(&mut self) -> Result<(), PassError> {
        match &mut self.backend {
            OwnerBackend::Cpu(_) => Ok(()),
            OwnerBackend::Metal(backend) => Ok(backend.complete()?),
        }
    }

    /// Grid owner `index`'s placements in `chunks`, of `target`'s domain,
    /// in every mode `pass` holds, less what `subtract` names; and sum the
    /// weight image's contributions over every placement when this owner
    /// holds it (`weight_owner`; otherwise each owner sums its own
    /// placements'). The sums are gridded when the table fills and at
    /// [`Self::flush_weights`].
    fn accumulate(
        &mut self,
        pass: &MajorCyclePass<'_>,
        target: &PassDomain<'_>,
        (index, weight_owner): (usize, Option<usize>),
        subtract: Subtract<'_>,
        chunks: &[Chunk],
    ) -> Result<(), PassError> {
        let started = std::time::Instant::now();
        let Self {
            acc,
            backend,
            weights,
            busy,
            ..
        } = self;
        let acc = acc.as_mut().expect("owners accumulate until finished");
        let cf = target.operator.cf();
        let own_weights = if weight_owner.is_none() {
            weights.as_mut()
        } else {
            None
        };
        match &mut *backend {
            OwnerBackend::Cpu(cpu) => {
                for chunk in chunks {
                    let block = chunk.owned[index].block();
                    if !block.is_empty() {
                        let mut apply = |work: Work<'_>| cpu.apply(&block, cf, work);
                        accumulate(pass, &mut apply, subtract, acc)?;
                    }
                }
            }
            OwnerBackend::Metal(metal) => {
                let parts = chunks
                    .iter()
                    .map(|chunk| chunk.owned[index].block())
                    .collect::<Vec<_>>();
                if parts.iter().any(|part| !part.is_empty()) {
                    let mut apply = |work: Work<'_>| metal.apply_parts(&parts, cf, work);
                    accumulate(pass, &mut apply, subtract, acc)?;
                }
            }
        }
        if let Some(sums) = own_weights {
            for chunk in chunks {
                let mut grid = weight_grid(backend, cf, acc);
                sums.add(&chunk.owned[index].block(), &mut grid)?;
            }
        }
        if weight_owner == Some(index)
            && let Some(sums) = weights
        {
            for chunk in chunks {
                let mut grid = weight_grid(backend, cf, acc);
                sums.add(&chunk.weight_owned.block(), &mut grid)?;
            }
        }
        *busy += started.elapsed();
        Ok(())
    }

    /// Grid the weight image's summed contributions still in the owner's
    /// table, of `target`'s domain ([`WeightSums::flush`]), and drop the
    /// table: the wave's last placement is accumulated.
    ///
    /// The flush's time is the owner's ([`Self::busy`]). Its placements,
    /// group grids and grids are returned for the wave's log. A panic while
    /// gridding fails the pass with [`PassError::OwnerPanicked`], as an
    /// owner's panic does in a pipelined pass.
    fn flush_weights(&mut self, target: &PassDomain<'_>) -> Result<[u64; 3], PassError> {
        let Some(mut sums) = self.weights.take() else {
            return Ok([0; 3]);
        };
        let started = std::time::Instant::now();
        let acc = self.acc.as_mut().expect("owners flush before they finish");
        let backend = &mut self.backend;
        let flushed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut grid = weight_grid(backend, target.operator.cf(), acc);
            sums.flush(&mut grid)
        }))
        .unwrap_or(Err(PassError::OwnerPanicked));
        self.busy += started.elapsed();
        flushed?;
        Ok(sums.totals())
    }
}

/// Grid a block of summed weight-image contributions on `backend` into
/// `acc` ([`WeightSums`]).
fn weight_grid<'a>(
    backend: &'a mut OwnerBackend<'_>,
    cf: &'a dyn ConvolutionFunctionSet,
    acc: &'a mut GridAccumulator,
) -> impl FnMut(&SampleBlock<'_>) -> Result<(), PassError> + 'a {
    move |groups: &SampleBlock<'_>| {
        let work = Work::Grid {
            mode: Mode::Weight,
            acc: &mut *acc,
        };
        match backend {
            OwnerBackend::Cpu(cpu) => Ok(cpu.apply(groups, cf, work)?),
            OwnerBackend::Metal(metal) => Ok(metal.apply(groups, cf, work)?),
        }
    }
}

/// The backend an owner dispatches to. The CPU grids each placement chunk
/// as it comes; a Metal owner dispatches its share of every chunk of a slice
/// at once, read from the chunks' buffers ([`MetalBackend::apply_parts`]),
/// so each device dispatch is a slice's worth of samples rather than one
/// chunk's, and leaves it running while the workers place the next slice
/// ([`MetalBackend::with_deferred_grid_completion`]).
enum OwnerBackend<'w> {
    Cpu(CpuBackend),
    Metal(Box<MetalBackend<'w>>),
}

/// One image domain's share of a wave.
struct Domain<'w> {
    router: Router,
    owners: Vec<Owner<'w>>,
    model: Option<&'w PreparedModelGrids>,
    /// The wave's gridding work by anchor row when the domain is split into
    /// regions ([`Router::owner_and_work`]), for later passes' boundaries
    /// ([`Partition::regions_balanced`]).
    row_work: Option<Vec<u64>>,
}

impl Domain<'_> {
    /// Add the gridding work by anchor row `chunks` placed.
    fn add_row_work(&mut self, chunks: &[Chunk]) {
        if let Some(total) = &mut self.row_work {
            for chunk in chunks {
                for (total, work) in total.iter_mut().zip(&chunk.row_work) {
                    *total += work;
                }
            }
        }
    }
}

/// What placing one block's rows on one domain reads ([`Wave::placer`]).
struct Placer<'b, 'w, 'p> {
    pass: &'w MajorCyclePass<'p>,
    target: &'p PassDomain<'p>,
    router: &'b Router,
    owners: usize,
    planes: PlaneRange,
    native_residuals: bool,
    shape: ChunkShape,
    /// The pass's kept weights, for the first domain.
    weights: Option<(&'w WeightCache, bool)>,
    /// The slot of the block's first sample.
    weight_base: usize,
    /// Rows of the domain's work histogram, when it is region-split.
    grid_rows: Option<usize>,
    block: &'b NativeBlock,
    index: usize,
    ordered: bool,
}

impl Placer<'_, '_, '_> {
    /// Place the rows `chunks` hold on `team`: each placement into its
    /// owner's buffer, and every placement into the weight owner's when the
    /// pass grids the weight image on one owner; each chunk's gridding work
    /// by anchor row beside them.
    fn place(&self, team: &WorkerTeam, chunks: &mut [Chunk]) -> Result<(), PassError> {
        let Self {
            pass,
            target,
            router,
            owners,
            planes,
            native_residuals,
            shape,
            weights,
            weight_base,
            grid_rows,
            block,
            index,
            ordered,
        } = *self;
        let channels = block.channels();
        // With region owners the weight image has one owner, which must see
        // every placement; plane owners grid their own samples' weights.
        let route_weight = pass.modes.weight && router.weight_owner().is_some();
        team.for_each_mut(chunks, |_, chunk| {
            chunk.placed = 0;
            chunk.owned[..owners]
                .iter_mut()
                .for_each(SampleBuffer::clear);
            chunk.weight_owned.clear();
            chunk.row_work.clear();
            if let Some(rows) = grid_rows {
                chunk.row_work.resize(rows, 0);
            }
            for position in chunk.rows.clone() {
                let row_index = row_at(block, ordered, position);
                let native = block.row(index, row_index);
                let slots = weights.and_then(|(cache, filled)| {
                    cache.row(weight_base + row_index * channels, channels, filled)
                });
                let mut row_samples = 0;
                let (owned, weight_owned, placed, row_work) = (
                    &mut chunk.owned,
                    &mut chunk.weight_owned,
                    &mut chunk.placed,
                    &mut chunk.row_work,
                );
                let mut keep = |placement: &Placement, values: &[Complex32], weights: &[f32]| {
                    row_samples += 1;
                    if !planes.contains(placement.plane) {
                        return None;
                    }
                    if route_weight {
                        weight_owned.push(*placement, values, weights);
                    }
                    *placed += 1;
                    let (owner, work) = router.owner_and_work(target.operator, placement);
                    if let Some((row, work)) = work
                        && let Some(total) = row_work.get_mut(row)
                    {
                        *total += work;
                    }
                    Some(owner)
                };
                if native_residuals {
                    target.resampler.place_each_sourced(
                        target.operator,
                        pass.weighting,
                        &native,
                        cell_of(position, channels),
                        slots,
                        |placement, values, weights, source| {
                            if let Some(owner) = keep(&placement, values, weights) {
                                owned[owner].push_sourced(placement, values, weights, source);
                            }
                        },
                    )?;
                } else {
                    target.resampler.place_each(
                        target.operator,
                        pass.weighting,
                        &native,
                        slots,
                        |placement, values, weights| {
                            if let Some(owner) = keep(&placement, values, weights) {
                                owned[owner].push(placement, values, weights);
                            }
                        },
                    )?;
                }
                debug_assert!(
                    row_samples <= shape.row,
                    "a row places within the chunk's per-row bound"
                );
            }
            debug_assert!(
                chunk.placed as usize <= shape.placements
                    && chunk.capacity_bytes() == shape.bytes(),
                "a chunk places within the placements it was allocated for"
            );
            Ok::<_, PassError>(())
        })
    }
}

/// One row chunk's placements on the wave's planes, by owner.
///
/// Every buffer is allocated once, for a chunk's rows of the source's widest
/// row ([`ChunkShape`]), and each owner's can take every placement of the
/// chunk, so a chunk holds [`ChunkShape::bytes`] however its placements fall
/// among the owners. The chunk's rows are predicted apart, in the same
/// slices ([`BlockPrediction`]).
struct Chunk {
    rows: Range<usize>,
    /// Each owner's placements, in row order, with their native endpoints
    /// when the wave forms residuals at native channels.
    owned: Vec<SampleBuffer>,
    /// Every placement of the chunk, in row order, when the weight image has
    /// one owner ([`Router::weight_owner`]): that owner grids `Mode::Weight`
    /// over it.
    weight_owned: SampleBuffer,
    placed: u64,
    /// The gridding work of the chunk's placements by anchor row, on a
    /// region-partitioned domain ([`Router::owner_and_work`]); merged into
    /// the domain's after each slice.
    row_work: Vec<u64>,
}

/// The largest chunk of a wave over blocks of `block`: at most
/// [`chunk_placements`] placements, whatever rows the source puts in a
/// block.
#[derive(Clone, Copy)]
struct ChunkShape {
    /// Placements one native row places on any domain.
    row: usize,
    /// Rows a chunk places at once.
    rows: usize,
    /// Placements the chunk's rows place.
    placements: usize,
    /// Correlations per placement.
    npol: usize,
    /// Channels of one row.
    channels: usize,
    /// Correlations and channels of one row.
    cells: usize,
    /// Owners of the domain with the most.
    owners: usize,
    /// Whether the pass grids the weight image.
    weight: bool,
    /// Whether placements carry native endpoints (a residual at native
    /// channels).
    sourced: bool,
    /// Grid rows of the region-partitioned domain with the most: the
    /// length of the chunk's work histogram ([`Chunk::row_work`]).
    grid_rows: usize,
}

impl ChunkShape {
    /// The chunk of `chunks` chunks over blocks of `block`, each taking at
    /// most `placements` placements before the owner scaling of
    /// [`chunk_placements`].
    fn new(
        domains: &[PassDomain<'_>],
        block: BlockShape,
        weight: bool,
        sourced: bool,
        chunks: usize,
        placements: usize,
    ) -> Self {
        let row = domains
            .iter()
            .map(|domain| domain.resampler.samples_per_row(block.spectrum))
            .max()
            .unwrap_or(0);
        let owners = domains
            .iter()
            .map(|domain| domain.partition.owners())
            .max()
            .unwrap_or(1);
        let rows = chunk_rows(
            row,
            block.rows,
            chunks,
            chunk_placements(placements, owners),
        );
        Self {
            row,
            rows,
            placements: rows * row,
            npol: block.correlations,
            channels: block.channels(),
            cells: block.channels() * block.correlations,
            owners,
            weight,
            sourced,
            grid_rows: domains
                .iter()
                .filter(|domain| domain.partition.splits_rows())
                .map(|domain| domain.operator.geometry().grid_shape()[1])
                .max()
                .unwrap_or(0),
        }
    }

    /// An empty buffer for `placements` placements of this shape, with room
    /// for their native endpoints when they carry them.
    fn buffer(self, placements: usize) -> SampleBuffer {
        if self.sourced {
            SampleBuffer::with_source_capacity(self.npol, placements)
        } else {
            SampleBuffer::with_capacity(self.npol, placements)
        }
    }

    /// Bytes [`Self::buffer`] holds.
    const fn buffer_bytes(self, placements: usize) -> u64 {
        if self.sourced {
            SampleBuffer::sourced_bytes(self.npol, placements)
        } else {
            SampleBuffer::bytes(self.npol, placements)
        }
    }

    /// Placements the weight-owner buffer takes.
    const fn weight_placements(self) -> usize {
        if self.weight { self.placements } else { 0 }
    }

    /// Bytes a chunk of this shape holds: each owner's and the weight
    /// owner's placements and its work histogram.
    const fn bytes(self) -> u64 {
        self.owners as u64 * self.buffer_bytes(self.placements)
            + SampleBuffer::bytes(self.npol, self.weight_placements())
            + (self.grid_rows * size_of::<u64>()) as u64
    }

    /// What the wave's [`BlockPrediction`] predicts this shape's rows
    /// with: the same chunks, of the same rows.
    const fn prediction(self, chunks: usize) -> PredictionShape {
        PredictionShape {
            npol: self.npol,
            channels: self.channels,
            rows: self.rows,
            chunks,
        }
    }
}

impl Chunk {
    fn new(shape: ChunkShape) -> Self {
        Self {
            rows: 0..0,
            owned: (0..shape.owners)
                .map(|_| shape.buffer(shape.placements))
                .collect(),
            weight_owned: SampleBuffer::with_capacity(shape.npol, shape.weight_placements()),
            placed: 0,
            row_work: Vec::with_capacity(shape.grid_rows),
        }
    }

    /// Bytes the chunk holds now, from its buffers' capacities: at most
    /// [`ChunkShape::bytes`] while every buffer is within the shape.
    fn capacity_bytes(&self) -> u64 {
        self.owned
            .iter()
            .map(SampleBuffer::capacity_bytes)
            .sum::<u64>()
            + self.weight_owned.capacity_bytes()
            + (self.row_work.capacity() * size_of::<u64>()) as u64
    }
}

/// Row chunks of a wave on `workers` workers, and of a
/// [`super::BlockPredictor`]'s.
pub(super) const fn chunks(workers: usize) -> usize {
    workers * CHUNKS_PER_WORKER
}

/// Bytes a wave places with on `workers` workers over blocks of `block`,
/// gridding the weight image when `weight`: its row chunks, their
/// prediction ([`BlockPrediction::bytes`]) and, on the CPU with a model,
/// each owner's batch when the pass forms residuals at native channels:
/// the residual values of a chunk's placements ([`CpuBackend::batch_bytes`]).
/// They grow with the source's block only up to `placements` a chunk.
pub(super) fn chunk_bytes(
    domains: &[PassDomain<'_>],
    backend: BackendChoice,
    block: BlockShape,
    workers: usize,
    weight: bool,
    with_model: bool,
    placements: usize,
) -> u64 {
    let count = chunks(workers);
    let sourced = super::native_residuals(domains, with_model);
    let shape = ChunkShape::new(domains, block, weight, sourced, count, placements);
    let owners = match backend {
        BackendChoice::Cpu if with_model && sourced => domains
            .iter()
            .map(|domain| domain.partition.owners())
            .sum::<usize>(),
        BackendChoice::Cpu | BackendChoice::Metal => 0,
    };
    count as u64 * shape.bytes()
        + owners as u64 * CpuBackend::batch_bytes(shape.npol, shape.placements)
        + BlockPrediction::bytes(
            domains
                .iter()
                .map(|domain| (domain.operator, domain.resampler)),
            backend,
            with_model,
            shape.prediction(count),
        )
}

/// Bytes the model at native cells of one block of `block` holds, which a
/// wave with a model forms for every row of the block: in host memory, or
/// on Metal in the device's.
pub(super) const fn prediction_bytes(block: BlockShape) -> u64 {
    (block.rows * block.channels() * block.correlations * size_of::<Complex32>()) as u64
}

pub(super) struct Wave<'w, 'p> {
    pass: &'w MajorCyclePass<'p>,
    planes: PlaneRange,
    domains: Vec<Domain<'w>>,
    native_residuals: bool,
    check_spacing: bool,
    /// What each chunk was allocated for.
    shape: ChunkShape,
    chunks: Vec<Chunk>,
    /// Each domain's operator, resampler and model, when the pass has a
    /// model.
    predicted: Vec<PredictionDomain<'w>>,
    /// The model at the block's native cells, which the wave forms on the
    /// same slices of chunks as it places.
    prediction: BlockPrediction<'w>,
    samples: u64,
    /// The pass's kept weights and whether an earlier pass filled them.
    weights: Option<(&'w WeightCache, bool)>,
    /// The slot of the next block's first sample.
    weight_base: usize,
    /// Wall time in the wave's stages: predicting the native model,
    /// placing samples on the chunks, and accumulating them by owner;
    /// reported beside a region pass's strips (#736).
    stages: WaveStages,
}

/// Wall time of a wave's stages ([`Wave::stages`]), as the placing thread
/// sees them. With one set of chunks, `placing` is the placement and
/// `accumulating` the owners' accumulation, slice by slice. When the owners
/// are pipelined (`pipelined`), they accumulate while the placer places
/// and reads. Then `placing` also counts waits for a free team thread,
/// including drains a waiting thread steals. `accumulating` is only the
/// placer's wait for a free set and for the last sets. The owners' own
/// accumulation times are in the region owners' log line.
#[derive(Clone, Copy, Default)]
struct WaveStages {
    predicting: std::time::Duration,
    placing: std::time::Duration,
    accumulating: std::time::Duration,
    pipelined: bool,
}

impl<'w, 'p> Wave<'w, 'p> {
    /// A wave over `planes` with each domain's prepared `models`, forming
    /// residuals at native channels when `native_residuals`, on `workers`
    /// workers over blocks of at most `block`. It places with
    /// [`chunk_bytes`] and predicts into [`prediction_bytes`].
    pub(super) fn new(
        pass: &'w MajorCyclePass<'p>,
        planes: PlaneRange,
        models: Option<&'w [PreparedModelGrids]>,
        native_residuals: bool,
        block: BlockShape,
        workers: usize,
    ) -> Result<Self, PassError> {
        let count = chunks(workers);
        let shape = ChunkShape::new(
            pass.domains,
            block,
            pass.modes.weight,
            native_residuals,
            count,
            pass.chunk_placements,
        );
        let domains = pass
            .domains
            .iter()
            .enumerate()
            .map(|(index, domain)| {
                let router = Router::new(&domain.partition, planes);
                let operator = domain.operator;
                let weight_owner = router.weight_owner();
                let owners = (0..router.owners())
                    .map(|owner| {
                        let (range, tile) = router.target(&domain.partition, planes, owner);
                        let weights = (pass.modes.weight
                            && weight_owner.is_none_or(|holder| holder == owner))
                        .then(|| WeightSums::new(block.correlations, WEIGHT_GROUPS));
                        Ok(match pass.backend {
                            BackendChoice::Cpu => Owner {
                                acc: Some(
                                    pass.summing(operator.accumulator(range, tile, pass.modes)),
                                ),
                                backend: OwnerBackend::Cpu(CpuBackend::new()),
                                images: None,
                                busy: std::time::Duration::ZERO,
                                weights,
                            },
                            BackendChoice::Metal => {
                                assert_eq!(
                                    operator.precision(),
                                    GridPrecision::F32,
                                    "Metal grids are f32 (D2)"
                                );
                                Owner {
                                    acc: Some(pass.summing(MetalBackend::accumulator(
                                        operator.accumulator_layout(range, tile, pass.modes),
                                    )?)),
                                    backend: OwnerBackend::Metal(Box::new(
                                        MetalBackend::new(operator.cf(), pass.metal_stage_timing)?
                                            .with_deferred_grid_completion(),
                                    )),
                                    images: None,
                                    busy: std::time::Duration::ZERO,
                                    weights,
                                }
                            }
                        })
                    })
                    .collect::<Result<_, PassError>>()?;
                let row_work = domain
                    .partition
                    .splits_rows()
                    .then(|| vec![0; operator.geometry().grid_shape()[1]]);
                Ok(Domain {
                    router,
                    owners,
                    model: models.map(|models| &models[index]),
                    row_work,
                })
            })
            .collect::<Result<_, PassError>>()?;
        // A pass with a model predicts each block, for its native residuals or
        // for a visibility writer.
        let predicted = models.map_or_else(Vec::new, |models| {
            pass.domains
                .iter()
                .zip(models)
                .map(|(domain, model)| PredictionDomain {
                    operator: domain.operator,
                    resampler: domain.resampler,
                    model: model.into(),
                })
                .collect()
        });
        let prediction = BlockPrediction::new(
            pass.domains
                .iter()
                .map(|domain| (domain.operator, domain.resampler)),
            pass.backend,
            pass.metal_stage_timing,
            shape.prediction(count),
            false,
            if pass.model.is_some() {
                block.rows * shape.cells
            } else {
                0
            },
        )?;
        Ok(Self {
            pass,
            planes,
            domains,
            native_residuals,
            check_spacing: false,
            shape,
            chunks: (0..count).map(|_| Chunk::new(shape)).collect(),
            predicted,
            prediction,
            samples: 0,
            weights: pass.weight_cache().map(|cache| (cache, cache.filled())),
            weight_base: 0,
            stages: WaveStages::default(),
        })
    }

    /// Check every row's native spacing against the pass's
    /// `native_spacing_hz`, which sized this wave's model halo.
    pub(super) fn checking_spacing(mut self, check: bool) -> Self {
        self.check_spacing = check;
        self
    }

    /// Samples placed on the first domain so far.
    pub(super) const fn samples(&self) -> u64 {
        self.samples
    }

    /// Place every row of `block` on every domain and accumulate the
    /// placements; with a visibility sink, first hand it the block, with
    /// every selected sample's prediction when it asks for them.
    ///
    /// When the wave forms residuals at native channels, the predictors
    /// first form `Σ_d A_d·m_d` at every row's native cells, and each placed
    /// sample's observed value is gridded less that model at its native
    /// endpoints ([`Work::NativeResidualGrid`]); otherwise the domain's model
    /// is subtracted at each placed sample.
    pub(super) fn consume(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        visibilities: Option<&mut VisibilitySink<'_>>,
    ) -> Result<(), PassError> {
        let rows = block.len();
        if self.check_spacing {
            let bound_hz = self.pass.native_spacing_hz;
            for row in 0..rows {
                let observed_hz = block
                    .row(0, row)
                    .frequencies_hz
                    .windows(2)
                    .fold(0.0_f64, |widest, pair| {
                        widest.max((pair[1] - pair[0]).abs())
                    });
                if observed_hz > bound_hz {
                    return Err(PassError::NativeSpacing {
                        observed_hz,
                        bound_hz,
                    });
                }
            }
        }
        let sink_predictions = visibilities.as_ref().is_some_and(|sink| sink.predictions);
        // Chunks take rows by position in the block's locality order, and
        // predictions are stored by position, unless the sink reads them in
        // the block's row order.
        let ordered = !sink_predictions;
        let cells = block.channels() * block.correlations();
        if self.native_residuals || sink_predictions {
            // The same slices of chunks as placement.
            let started = std::time::Instant::now();
            self.prediction
                .predict(block, team, &self.predicted, ordered)?;
            self.stages.predicting += started.elapsed();
        }
        if let Some(sink) = visibilities {
            let predictions: &[Complex32] = if sink.predictions {
                self.prediction.host_cells(rows * cells)?
            } else {
                &[]
            };
            (sink.write)(block, predictions).map_err(PassError::VisibilityWrite)?;
        }
        // Slices of the block, each owner's in the block's order as a whole
        // block's would be, so the accumulation is the same.
        let chunks = self.chunks.len();
        for slice in slices(rows, chunks, self.shape.rows) {
            let mut count = 0;
            for (chunk, rows) in self.chunks.iter_mut().zip(chunk_ranges(slice, chunks)) {
                chunk.rows = rows;
                count += 1;
            }
            for index in 0..self.domains.len() {
                let placed = self.place(block, team, count, index, ordered)?;
                if index == 0 {
                    self.samples += placed;
                }
            }
        }
        self.weight_base += rows * block.channels();
        Ok(())
    }

    /// What placing `block`'s rows on domain `index` reads: the wave's
    /// state at this block. Chunks hold positions in the block's locality
    /// order when `ordered`, else row indices ([`row_at`]).
    fn placer<'b>(
        &'b self,
        block: &'b NativeBlock,
        index: usize,
        ordered: bool,
    ) -> Placer<'b, 'w, 'p> {
        let domain = &self.domains[index];
        Placer {
            pass: self.pass,
            target: &self.pass.domains[index],
            router: &domain.router,
            owners: domain.router.owners(),
            planes: self.planes,
            native_residuals: self.native_residuals,
            shape: self.shape,
            weights: self.weights.filter(|_| index == 0),
            weight_base: self.weight_base,
            grid_rows: domain.row_work.as_ref().map(Vec::len),
            block,
            index,
            ordered,
        }
    }

    /// Place the rows of `block` on domain `index` and accumulate them;
    /// returns the samples placed. Chunks hold positions in the block's
    /// locality order when `ordered`, else row indices ([`row_at`]).
    fn place(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        count: usize,
        index: usize,
        ordered: bool,
    ) -> Result<u64, PassError> {
        let placing = std::time::Instant::now();
        let mut chunks = std::mem::take(&mut self.chunks);
        let placed = self
            .placer(block, index, ordered)
            .place(team, &mut chunks[..count]);
        self.chunks = chunks;
        placed?;
        let pass = self.pass;
        let target = &pass.domains[index];
        let domain = &mut self.domains[index];
        let chunks = &self.chunks[..count];
        domain.add_row_work(chunks);
        self.stages.placing += placing.elapsed();
        let accumulating = std::time::Instant::now();
        let model = domain.model.filter(|_| !self.native_residuals);
        let native = self
            .native_residuals
            .then(|| self.prediction.native_model());
        let weight_owner = domain.router.weight_owner();
        team.for_each_mut(&mut domain.owners, |owner_index, owner| {
            let subtract = Subtract { model, native };
            owner.accumulate(pass, target, (owner_index, weight_owner), subtract, chunks)
        })?;
        self.stages.accumulating += accumulating.elapsed();
        Ok(chunks.iter().map(|chunk| chunk.placed).sum())
    }

    /// Each domain's gridding work by anchor row, when it is split into
    /// regions; taken once, before [`Self::finish`].
    pub(super) fn take_row_work(&mut self) -> Vec<Option<Vec<u64>>> {
        self.domains
            .iter_mut()
            .map(|domain| domain.row_work.take())
            .collect()
    }

    /// Transform the accumulated grids into this wave's normal images, one
    /// per domain.
    pub(super) fn finish(mut self, team: &WorkerTeam) -> Result<Vec<NormalImages>, PassError> {
        // Metal owners leave their last grid work running on the device, and
        // Metal predictors their native models: complete both, and learn
        // whether any failed, before any accumulator is read.
        self.prediction.complete()?;
        // The weight image's last summed contributions go to the grid first:
        // accumulation, which the stages count as such.
        let flushing = std::time::Instant::now();
        for (domain, target) in self.domains.iter_mut().zip(self.pass.domains) {
            let summed = std::sync::Mutex::new([0_u64; 3]);
            team.for_each_mut(&mut domain.owners, |_, owner| {
                let totals = owner.flush_weights(target)?;
                let mut summed = summed.lock().unwrap_or_else(PoisonError::into_inner);
                for (sum, total) in summed.iter_mut().zip(totals) {
                    *sum += total;
                }
                drop(summed);
                owner.complete()
            })?;
            let [placements, groups, grids] =
                summed.into_inner().unwrap_or_else(PoisonError::into_inner);
            if placements > 0 {
                tracing::info!(
                    "weight image summed: {placements} placements as {groups} group grids in \
                     {grids} grids"
                );
            }
        }
        self.stages.accumulating += flushing.elapsed();
        let pass = self.pass;
        let planes = self.planes;
        let stages = self.stages;
        let transforming = std::time::Instant::now();
        let images = self
            .domains
            .into_iter()
            .zip(pass.domains)
            .map(|(mut domain, target)| {
                let operator = target.operator;
                match &target.partition {
                    Partition::Planes { .. } => {
                        team.for_each_mut(&mut domain.owners, |_, owner| {
                            let acc = owner.acc.take().expect("each owner finishes once");
                            owner.images = Some(operator.finish(acc)?);
                            Ok::<_, PassError>(())
                        })?;
                        Ok(concatenate(
                            domain
                                .owners
                                .into_iter()
                                .map(|owner| owner.images.expect("every owner finished")),
                        ))
                    }
                    Partition::Regions { regions, .. } => {
                        // One owner's tile is the whole grid (`Partition::regions`
                        // gives a lone region every row): it is the sum, and
                        // `finish` refuses a tile. The charge keys on the same
                        // count (`DomainStages`).
                        if let [owner] = domain.owners.as_mut_slice() {
                            let acc = owner.acc.take().expect("each owner finishes once");
                            drop(domain.owners);
                            return Ok(operator.finish(acc)?);
                        }
                        // How evenly the strips shared the gridding (#736).
                        tracing::info!(
                            "region owners: rows {:?}, accumulating {:?} s",
                            regions
                                .iter()
                                .map(|region| region.rows.clone())
                                .collect::<Vec<_>>(),
                            domain
                                .owners
                                .iter()
                                .map(|owner| (owner.busy.as_secs_f64() * 10.0).round() / 10.0)
                                .collect::<Vec<_>>(),
                        );
                        let mut merged =
                            pass.summing(operator.accumulator(planes, None, pass.modes));
                        for owner in &domain.owners {
                            merged
                                .merge_from(owner.acc.as_ref().expect("owners hold their tiles"))?;
                        }
                        drop(domain.owners);
                        Ok(operator.finish(merged)?)
                    }
                }
            })
            .collect::<Result<Vec<_>, PassError>>()?;
        if stages.pipelined {
            tracing::info!(
                "wave stages (owners pipelined): predicting {:.1} s, placing with thread \
                 waits {:.1} s, waiting for owners {:.1} s, transforming {:.1} s",
                stages.predicting.as_secs_f64(),
                stages.placing.as_secs_f64(),
                stages.accumulating.as_secs_f64(),
                transforming.elapsed().as_secs_f64(),
            );
        } else {
            tracing::info!(
                "wave stages: predicting {:.1} s, placing {:.1} s, accumulating {:.1} s, \
                 transforming {:.1} s",
                stages.predicting.as_secs_f64(),
                stages.placing.as_secs_f64(),
                stages.accumulating.as_secs_f64(),
                transforming.elapsed().as_secs_f64(),
            );
        }
        Ok(images)
    }
}

/// What an owner subtracts from its data terms: the domain's model at each
/// placed sample, the model at the samples' native endpoints, or nothing.
#[derive(Clone, Copy)]
struct Subtract<'a> {
    model: Option<&'a PreparedModelGrids>,
    native: Option<NativeModel<'a>>,
}

/// Grid one owner's share of a slice, through `apply`, in the data and PSF
/// modes the pass holds; the data terms receive the residual of what
/// `subtract` names. The weight image is summed apart ([`WeightSums`]).
fn accumulate(
    pass: &MajorCyclePass<'_>,
    apply: &mut dyn FnMut(Work<'_>) -> Result<(), casa_imaging_operator::OperatorError>,
    subtract: Subtract<'_>,
    acc: &mut GridAccumulator,
) -> Result<(), PassError> {
    if pass.modes.data {
        let work = match subtract {
            Subtract {
                native: Some(model),
                ..
            } => Work::NativeResidualGrid { model, acc },
            Subtract {
                model: Some(model), ..
            } => Work::ResidualGrid {
                model,
                acc,
                residual_out: None,
            },
            Subtract { .. } => Work::Grid {
                mode: Mode::Data,
                acc,
            },
        };
        apply(work)?;
    }
    if pass.modes.psf {
        apply(Work::Grid {
            mode: Mode::Psf,
            acc,
        })?;
    }
    Ok(())
}

/// Join per-owner images of consecutive plane ranges.
pub(super) fn concatenate(parts: impl IntoIterator<Item = NormalImages>) -> NormalImages {
    let mut parts = parts.into_iter();
    let mut joined = parts.next().expect("a wave has at least one owner");
    for part in parts {
        assert!(
            part.pols == joined.pols
                && part.data_terms == joined.data_terms
                && part.psf_terms == joined.psf_terms
                && part.first_plane == joined.first_plane + joined.planes.len() as u32,
            "owner images must continue the plane sequence with the same layout"
        );
        joined.planes.extend(part.planes);
    }
    joined
}

#[cfg(test)]
mod tests {
    use super::super::{CHUNK_PLACEMENTS, REGION_CHUNK_PLACEMENTS};
    use super::{CHUNKS_PER_WORKER, chunk_placements, chunks};

    /// Every owner's buffer in a chunk can take all of the chunk's
    /// placements, and a pass has four chunks per worker and up to one
    /// owner per worker; past four owners a chunk takes proportionally
    /// fewer, so the chunks' storage grows linearly with the workers, and up
    /// to four a chunk takes the full cap, whatever the cap.
    #[test]
    fn chunk_storage_grows_linearly_with_workers() {
        for full in std::iter::once(CHUNK_PLACEMENTS).chain(REGION_CHUNK_PLACEMENTS) {
            for workers in 1..=128 {
                let owners = workers;
                let storage = chunks(workers) * owners * chunk_placements(full, owners);
                assert!(
                    storage <= CHUNKS_PER_WORKER * 4 * full * workers,
                    "{workers} workers place into {storage} placements of storage"
                );
            }
            assert_eq!(chunk_placements(full, 4), full);
        }
    }
}
