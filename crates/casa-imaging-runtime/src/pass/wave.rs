// SPDX-License-Identifier: LGPL-3.0-or-later
//! One wave of a major-cycle pass: prediction and placement on row chunks,
//! accumulation by owner for each image domain, and the per-plane
//! transforms.

use std::ops::Range;

use casa_imaging_metal::MetalBackend;
use casa_imaging_operator::{
    CpuBackend, DeviceNativeCells, GridAccumulator, GridBackend, GridPrecision, Mode, NativeModel,
    NativeTarget, NormalImages, Placement, PlaneRange, PredictionScratch, PreparedModelGrids,
    SampleBuffer, SpectralResampler, Work,
};
use num_complex::Complex32;

use super::partition::Router;
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

/// One row chunk's placements on the wave's planes, by owner, and its
/// prediction batch.
///
/// Every buffer is allocated once, for a chunk's rows of the source's widest
/// row ([`ChunkShape`]), and each owner's can take every placement of the
/// chunk, so a chunk holds [`ChunkShape::bytes`] however its placements fall
/// among the owners.
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
    prediction: PredictionScratch,
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
    /// Samples the prediction batch of the chunk's rows degrids at most.
    predicted: usize,
    /// Bytes the prediction batch of the chunk's rows holds
    /// ([`SpectralResampler::prediction_scratch`]) with each row's span in
    /// it.
    prediction: u64,
    /// Grid rows of the region-partitioned domain with the most: the
    /// length of the chunk's work histogram ([`Chunk::row_work`]).
    grid_rows: usize,
}

impl ChunkShape {
    fn new(
        domains: &[PassDomain<'_>],
        block: BlockShape,
        weight: bool,
        sourced: bool,
        chunks: usize,
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
        let rows = chunk_rows(row, block.rows, chunks, chunk_placements(owners));
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
            predicted: rows
                * domains
                    .iter()
                    .map(|domain| {
                        domain
                            .resampler
                            .prediction_samples_per_row(block.channels())
                    })
                    .max()
                    .unwrap_or(0),
            prediction: SpectralResampler::prediction_scratch_bytes(
                domains.iter().map(|domain| domain.resampler),
                block.correlations,
                block.channels(),
                rows,
            ),
            grid_rows: domains
                .iter()
                .filter(|domain| matches!(domain.partition, Partition::Regions { .. }))
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
    /// owner's placements, its prediction batch and its work histogram.
    const fn bytes(self) -> u64 {
        self.owners as u64 * self.buffer_bytes(self.placements)
            + SampleBuffer::bytes(self.npol, self.weight_placements())
            + self.prediction
            + (self.grid_rows * size_of::<u64>()) as u64
    }
}

/// The backend that predicts one domain's batch for one chunk and forms the
/// model at its rows' native cells ([`Work::PredictNative`]): the pass's,
/// so that on Metal the model never leaves the device and the device works
/// on one chunk's batch while the host places the next chunk's.
enum Predictor<'w> {
    Cpu(CpuBackend),
    Metal(Box<MetalBackend<'w>>),
}

/// One predictor per domain on `backend`, for one chunk.
fn predictors<'w>(
    domains: &[PassDomain<'w>],
    backend: BackendChoice,
    stage_timing: bool,
) -> Result<Vec<Predictor<'w>>, PassError> {
    domains
        .iter()
        .map(|domain| {
            Ok(match backend {
                BackendChoice::Cpu => Predictor::Cpu(CpuBackend::new()),
                BackendChoice::Metal => Predictor::Metal(Box::new(MetalBackend::new(
                    domain.operator.cf(),
                    stage_timing,
                )?)),
            })
        })
        .collect()
}

impl Chunk {
    fn new(shape: ChunkShape, domains: &[PassDomain<'_>]) -> Self {
        Self {
            rows: 0..0,
            owned: (0..shape.owners)
                .map(|_| shape.buffer(shape.placements))
                .collect(),
            weight_owned: SampleBuffer::with_capacity(shape.npol, shape.weight_placements()),
            placed: 0,
            prediction: SpectralResampler::prediction_scratch(
                domains.iter().map(|domain| domain.resampler),
                shape.npol,
                shape.channels,
                shape.rows,
            ),
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
            + self.prediction.capacity_bytes()
            + (self.row_work.capacity() * size_of::<u64>()) as u64
    }
}

/// Row chunks of a wave on `workers` workers.
const fn chunks(workers: usize) -> usize {
    workers * CHUNKS_PER_WORKER
}

/// Bytes a wave places with on `workers` workers over blocks of `block`,
/// gridding the weight image when `weight`: its row chunks and, with a
/// model, its backends' batches. On Metal each chunk's predictors keep a
/// ring and a prediction batch ([`casa_imaging_metal::ring_bytes`],
/// [`casa_imaging_metal::batch_bytes`]); on the CPU each chunk's predictors
/// keep their batch's predictions, and each owner of a pass forming
/// residuals at native channels the residual values of a chunk's
/// placements ([`CpuBackend::batch_bytes`]). They grow with the source's
/// block only up to [`CHUNK_PLACEMENTS`](super::CHUNK_PLACEMENTS) a chunk.
pub(super) fn chunk_bytes(
    domains: &[PassDomain<'_>],
    backend: BackendChoice,
    block: BlockShape,
    workers: usize,
    weight: bool,
    with_model: bool,
) -> u64 {
    let count = chunks(workers);
    let sourced = super::native_residuals(domains, with_model);
    let shape = ChunkShape::new(domains, block, weight, sourced, count);
    let device = match backend {
        BackendChoice::Cpu if with_model => {
            let owners = if sourced {
                domains
                    .iter()
                    .map(|domain| domain.partition.owners())
                    .sum::<usize>()
            } else {
                0
            };
            (count * domains.len()) as u64 * CpuBackend::batch_bytes(shape.npol, shape.predicted)
                + owners as u64 * CpuBackend::batch_bytes(shape.npol, shape.placements)
        }
        BackendChoice::Cpu => 0,
        BackendChoice::Metal if with_model => {
            (count * domains.len()) as u64
                * (casa_imaging_metal::ring_bytes(shape.npol)
                    + casa_imaging_metal::batch_bytes(shape.npol, shape.predicted))
        }
        BackendChoice::Metal => 0,
    };
    count as u64 * shape.bytes() + device
}

/// Bytes the model at native cells of one block of `block` holds, which a
/// wave with a model forms for every row of the block: in host memory, or
/// on Metal in the device's.
pub(super) const fn prediction_bytes(block: BlockShape) -> u64 {
    (block.rows * block.channels() * block.correlations * size_of::<Complex32>()) as u64
}

/// The model at a block's native cells, `[position][channel][correlation]`
/// by the rows' positions in the block: in host memory for CPU predictors,
/// in the device's for Metal ones, which form it and read it there.
enum NativeStore {
    Host(Vec<Complex32>),
    Device(DeviceNativeCells),
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
    /// Each chunk's [`Predictor`] per domain.
    predictors: Vec<Vec<Predictor<'w>>>,
    predictions: NativeStore,
    samples: u64,
    /// The pass's kept weights and whether an earlier pass filled them.
    weights: Option<(&'w WeightCache, bool)>,
    /// The slot of the next block's first sample.
    weight_base: usize,
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
        );
        let domains = pass
            .domains
            .iter()
            .enumerate()
            .map(|(index, domain)| {
                let router = Router::new(&domain.partition, planes);
                let operator = domain.operator;
                let owners = (0..router.owners())
                    .map(|owner| {
                        let (range, tile) = router.target(&domain.partition, planes, owner);
                        Ok(match pass.backend {
                            BackendChoice::Cpu => Owner {
                                acc: Some(
                                    pass.summing(operator.accumulator(range, tile, pass.modes)),
                                ),
                                backend: OwnerBackend::Cpu(CpuBackend::new()),
                                images: None,
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
                                }
                            }
                        })
                    })
                    .collect::<Result<_, PassError>>()?;
                let row_work = matches!(domain.partition, Partition::Regions { .. })
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
        let predictions = if pass.model.is_some() {
            block.rows * shape.cells
        } else {
            0
        };
        let predictions = match pass.backend {
            BackendChoice::Metal if predictions > 0 => {
                NativeStore::Device(MetalBackend::native_cells(predictions)?)
            }
            BackendChoice::Cpu | BackendChoice::Metal => {
                NativeStore::Host(Vec::with_capacity(predictions))
            }
        };
        Ok(Self {
            pass,
            planes,
            domains,
            native_residuals,
            check_spacing: false,
            shape,
            chunks: (0..count)
                .map(|_| Chunk::new(shape, pass.domains))
                .collect(),
            predictors: (0..count)
                .map(|_| predictors(pass.domains, pass.backend, pass.metal_stage_timing))
                .collect::<Result<_, _>>()?,
            predictions,
            samples: 0,
            weights: pass.weight_cache().map(|cache| (cache, cache.filled())),
            weight_base: 0,
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
            if let NativeStore::Host(predictions) = &mut self.predictions {
                // The first domain with a model writes every cell of the
                // rows it predicts, so stale cells never reach a sample;
                // without a model the cells stay zero.
                if predictions.len() < rows * cells
                    || self.domains.iter().all(|domain| domain.model.is_none())
                {
                    predictions.clear();
                    predictions.resize(rows * cells, Complex32::default());
                }
            }
            // The same slices as placement, so no chunk predicts more rows
            // than its batch was allocated for.
            let chunks = self.chunks.len();
            for slice in slices(rows, chunks, self.shape.rows) {
                let mut count = 0;
                for (chunk, rows) in self
                    .chunks
                    .iter_mut()
                    .zip(chunk_ranges(slice.clone(), chunks))
                {
                    chunk.rows = rows;
                    count += 1;
                }
                self.predict(block, team, count, slice, ordered)?;
            }
        }
        if let Some(sink) = visibilities {
            let predictions: &[Complex32] = if sink.predictions {
                self.host_predictions(rows * cells)?
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
        let pass = self.pass;
        let target = &pass.domains[index];
        let domain = &mut self.domains[index];
        let owners = domain.owners.len();
        let router = &domain.router;
        let planes = self.planes;
        let native_residuals = self.native_residuals;
        let channels = block.channels();
        let shape = self.shape;
        // With region owners the weight image has one owner, which must see
        // every placement; plane owners grid their own samples' weights.
        let route_weight = pass.modes.weight && router.weight_owner().is_some();
        // A row's kept weights, at its slots from the block's (one domain).
        let weights = self.weights.filter(|_| index == 0);
        let weight_base = self.weight_base;
        let grid_rows = domain.row_work.as_ref().map(Vec::len);
        team.for_each_mut(&mut self.chunks[..count], |_, chunk| {
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
        })?;
        let chunks = &self.chunks[..count];
        if let Some(total) = &mut domain.row_work {
            for chunk in chunks {
                for (total, work) in total.iter_mut().zip(&chunk.row_work) {
                    *total += work;
                }
            }
        }
        let model = domain.model.filter(|_| !native_residuals);
        let native = native_residuals.then(|| match &self.predictions {
            NativeStore::Host(cells) => NativeModel::Host { cells, first: 0 },
            NativeStore::Device(cells) => NativeModel::Device(cells),
        });
        let weight_owner = router.weight_owner();
        team.for_each_mut(&mut domain.owners, |owner_index, owner| {
            let acc = owner
                .acc
                .as_mut()
                .expect("owners accumulate until finished");
            let operator = target.operator;
            // Each owner grids the weight image over its own placements
            // unless the image has one owner, who grids every placement
            // below.
            let own_weight = weight_owner.is_none();
            let subtract = Subtract { model, native };
            let cf = operator.cf();
            match &mut owner.backend {
                OwnerBackend::Cpu(backend) => {
                    for chunk in chunks {
                        let block = chunk.owned[owner_index].block();
                        if !block.is_empty() {
                            let mut apply = |work: Work<'_>| backend.apply(&block, cf, work);
                            accumulate(pass, &mut apply, subtract, acc, own_weight)?;
                        }
                    }
                }
                OwnerBackend::Metal(backend) => {
                    let parts = chunks
                        .iter()
                        .map(|chunk| chunk.owned[owner_index].block())
                        .collect::<Vec<_>>();
                    if parts.iter().any(|part| !part.is_empty()) {
                        let mut apply = |work: Work<'_>| backend.apply_parts(&parts, cf, work);
                        accumulate(pass, &mut apply, subtract, acc, own_weight)?;
                    }
                }
            }
            if pass.modes.weight && weight_owner == Some(owner_index) {
                for chunk in chunks {
                    let block = chunk.weight_owned.block();
                    if block.is_empty() {
                        continue;
                    }
                    let work = Work::Grid {
                        mode: Mode::Weight,
                        acc,
                    };
                    match &mut owner.backend {
                        OwnerBackend::Cpu(backend) => backend.apply(&block, cf, work)?,
                        OwnerBackend::Metal(backend) => backend.apply(&block, cf, work)?,
                    }
                }
            }
            Ok::<_, PassError>(())
        })?;
        Ok(chunks.iter().map(|chunk| chunk.placed).sum())
    }

    /// The model at native cells of every selected sample of the rows
    /// `slice` of `block`, which the first `count` chunks hold, summed over
    /// every domain's model (`SIMapperCollection::degrid`), `[row][channel]
    /// [correlation]`, into their rows of the wave's model store, which stay
    /// zero without a model. Rows are by position in the block's locality
    /// order when `ordered` ([`row_at`]).
    ///
    /// Domain by domain, every chunk places its rows' samples in one batch
    /// with its rows' native terms and hands it to its predictor
    /// ([`Work::PredictNative`]): the first domain writes the cells and
    /// later ones add to them. A Metal predictor leaves the work on the
    /// device, which runs it before any later device work that reads the
    /// cells, while the host places the next chunk's batch.
    fn predict(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        count: usize,
        slice: Range<usize>,
        ordered: bool,
    ) -> Result<(), PassError> {
        if self.domains.iter().all(|domain| domain.model.is_none()) {
            return Ok(());
        }
        let channels = block.channels();
        let cells = channels * block.correlations();
        let pass = self.pass;
        let shape = self.shape;
        let models = self
            .domains
            .iter()
            .map(|domain| domain.model)
            .collect::<Vec<_>>();
        let mut pieces = Vec::with_capacity(count);
        let chunks = self.chunks[..count].iter_mut().zip(&mut self.predictors);
        match &mut self.predictions {
            NativeStore::Host(predictions) => {
                let mut remaining = &mut predictions[slice.start * cells..slice.end * cells];
                for (chunk, predictors) in chunks {
                    let (head, tail) = remaining.split_at_mut(chunk.rows.len() * cells);
                    remaining = tail;
                    pieces.push((chunk, predictors, ChunkCells::Host(head)));
                }
            }
            NativeStore::Device(cells) => {
                for (chunk, predictors) in chunks {
                    pieces.push((chunk, predictors, ChunkCells::Device(cells)));
                }
            }
        }
        let mut add = false;
        for (index, (target, model)) in pass.domains.iter().zip(&models).enumerate() {
            let Some(model) = *model else {
                continue;
            };
            let npol = target.operator.polarization().correlations().len();
            team.for_each_mut(&mut pieces, |_, (chunk, predictors, cells)| {
                chunk.prediction.begin(npol);
                for position in chunk.rows.clone() {
                    let row = block.row(index, row_at(block, ordered, position));
                    let placed = target.resampler.place_prediction(
                        target.operator,
                        model,
                        &row,
                        &mut chunk.prediction,
                    )?;
                    target.resampler.native_terms(
                        &row,
                        npol,
                        placed,
                        &mut chunk.prediction,
                        cell_of(position, channels),
                    );
                }
                let cf = target.operator.cf();
                match (&mut predictors[index], cells) {
                    (Predictor::Cpu(backend), ChunkCells::Host(cells)) => {
                        let first = cell_of(chunk.rows.start, channels);
                        let target = NativeTarget::Host { cells, first };
                        chunk
                            .prediction
                            .apply_native(backend, cf, model, target, add)?;
                    }
                    (Predictor::Metal(backend), ChunkCells::Device(cells)) => {
                        let backend: &mut MetalBackend<'_> = backend;
                        let target = NativeTarget::Device(cells);
                        chunk
                            .prediction
                            .apply_native(backend, cf, model, target, add)?;
                    }
                    _ => unreachable!("a wave's predictors and model store share its backend"),
                }
                Ok::<_, PassError>(())
            })?;
            add = true;
        }
        debug_assert!(
            pieces
                .iter()
                .all(|(chunk, ..)| chunk.capacity_bytes() == shape.bytes()),
            "a chunk predicts within the buffers it was allocated"
        );
        Ok(())
    }

    /// The model at the first `len` native cells of the block for the host:
    /// on Metal once every predictor's work, which alone writes the cells,
    /// has completed.
    fn host_predictions(&mut self, len: usize) -> Result<&[Complex32], PassError> {
        match &self.predictions {
            NativeStore::Host(cells) => Ok(&cells[..len]),
            NativeStore::Device(cells) => {
                for predictor in self.predictors.iter_mut().flatten() {
                    if let Predictor::Metal(backend) = predictor {
                        backend.complete()?;
                    }
                }
                // SAFETY: only the predictors' work writes the cells, every
                // command of it has completed, and none is issued while the
                // slice, which borrows the wave, lives.
                Ok(&unsafe { cells.host_cells() }[..len])
            }
        }
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
        for predictor in self.predictors.iter_mut().flatten() {
            if let Predictor::Metal(backend) = predictor {
                backend.complete()?;
            }
        }
        for domain in &mut self.domains {
            team.for_each_mut(&mut domain.owners, |_, owner| owner.complete())?;
        }
        let pass = self.pass;
        let planes = self.planes;
        self.domains
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
                    Partition::Regions { .. } => {
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
            .collect()
    }
}

/// The row at `position`: of the block's locality order when `ordered`
/// ([`NativeBlock::ordered_row`]), else the row itself.
fn row_at(block: &NativeBlock, ordered: bool, position: usize) -> usize {
    if ordered {
        block.ordered_row(position)
    } else {
        position
    }
}

/// One chunk's share of the wave's model store while it predicts.
enum ChunkCells<'c> {
    /// The chunk's rows' host cells.
    Host(&'c mut [Complex32]),
    /// The device cells of the whole block.
    Device(&'c DeviceNativeCells),
}

/// The first native cell of the row at `position` of a block of `channels`
/// channels.
fn cell_of(position: usize, channels: usize) -> u32 {
    u32::try_from(position * channels).expect("a block's native cells fit u32 indices")
}

/// What an owner subtracts from its data terms: the domain's model at each
/// placed sample, the model at the samples' native endpoints, or nothing.
#[derive(Clone, Copy)]
struct Subtract<'a> {
    model: Option<&'a PreparedModelGrids>,
    native: Option<NativeModel<'a>>,
}

/// Grid one owner's share of a slice, through `apply`, in every mode the
/// pass holds; the data terms receive the residual of what `subtract`
/// names.
fn accumulate(
    pass: &MajorCyclePass<'_>,
    apply: &mut dyn FnMut(Work<'_>) -> Result<(), casa_imaging_operator::OperatorError>,
    subtract: Subtract<'_>,
    acc: &mut GridAccumulator,
    weight: bool,
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
    if pass.modes.weight && weight {
        apply(Work::Grid {
            mode: Mode::Weight,
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
    use super::{CHUNKS_PER_WORKER, chunk_placements, chunks};

    /// Every owner's buffer in a chunk can take all of the chunk's
    /// placements, and a pass has four chunks per worker and up to one
    /// owner per worker; past four owners a chunk takes proportionally
    /// fewer, so the chunks' storage grows linearly with the workers, and up
    /// to four a chunk takes the full cap.
    #[test]
    fn chunk_storage_grows_linearly_with_workers() {
        let full = chunk_placements(1);
        for workers in 1..=128 {
            let owners = workers;
            let storage = chunks(workers) * owners * chunk_placements(owners);
            assert!(
                storage <= CHUNKS_PER_WORKER * 4 * full * workers,
                "{workers} workers place into {storage} placements of storage"
            );
        }
        assert_eq!(chunk_placements(4), full);
    }
}
