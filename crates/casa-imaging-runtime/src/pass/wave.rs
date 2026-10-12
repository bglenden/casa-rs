// SPDX-License-Identifier: LGPL-3.0-or-later
//! One wave of a major-cycle pass: prediction and placement on row chunks,
//! accumulation by owner for each image domain, and the per-plane
//! transforms.

use std::ops::Range;

use casa_imaging_metal::MetalBackend;
use casa_imaging_operator::{
    CpuBackend, GridAccumulator, GridBackend, GridPrecision, Mode, NativeRow, NormalImages,
    PlacedRow, PlaneRange, PredictionScratch, PreparedModelGrids, SampleBuffer, SpectralResampler,
    Work,
};
use num_complex::Complex32;

use super::partition::Router;
use super::{
    BackendChoice, BlockShape, MajorCyclePass, NativeBlock, Partition, PassDomain, PassError,
    VisibilitySink, WorkerTeam, chunk_placements, chunk_ranges, chunk_rows, slices,
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
            OwnerBackend::Metal { backend, .. } => Ok(backend.complete()?),
        }
    }
}

/// The backend an owner dispatches to. The CPU grids each placement chunk
/// as it comes; a Metal owner first gathers its share of every chunk of a
/// source block into `staging`, so each device dispatch is one block's
/// worth of samples rather than one chunk's, and leaves that dispatch
/// running while the workers place the next slice
/// ([`MetalBackend::with_deferred_grid_completion`]).
enum OwnerBackend<'w> {
    Cpu(CpuBackend),
    Metal {
        backend: Box<MetalBackend<'w>>,
        staging: SampleBuffer,
    },
}

/// One image domain's share of a wave.
struct Domain<'w> {
    router: Router,
    owners: Vec<Owner<'w>>,
    model: Option<&'w PreparedModelGrids>,
}

/// One row chunk's placements on the wave's planes, by owner, its
/// prediction batch and its rows' native-channel residuals.
///
/// Every buffer is allocated once, for a chunk's rows of the source's widest
/// row ([`ChunkShape`]), and each owner's can take every placement of the
/// chunk, so a chunk holds [`ChunkShape::bytes`] however its placements fall
/// among the owners.
struct Chunk {
    rows: Range<usize>,
    /// Each owner's placements, in row order.
    owned: Vec<SampleBuffer>,
    /// Every placement of the chunk, in row order, when the weight image has
    /// one owner ([`Router::weight_owner`]): that owner grids `Mode::Weight`
    /// over it.
    weight_owned: SampleBuffer,
    placed: u64,
    /// The chunk's rows' samples in the prediction batch.
    placed_rows: Vec<PlacedRow>,
    prediction: PredictionScratch,
    predicted: Vec<Complex32>,
    residual: Vec<Complex32>,
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
    /// Bytes the prediction batch of the chunk's rows holds
    /// ([`SpectralResampler::prediction_scratch`]) with each row's span in
    /// it.
    prediction: u64,
}

impl ChunkShape {
    fn new(domains: &[PassDomain<'_>], block: BlockShape, weight: bool, chunks: usize) -> Self {
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
            prediction: SpectralResampler::prediction_scratch_bytes(
                domains.iter().map(|domain| domain.resampler),
                block.correlations,
                block.channels(),
                rows,
            ) + (rows * size_of::<PlacedRow>()) as u64,
        }
    }

    /// Bytes a Metal owner's staging holds: its share of every chunk of a
    /// slice, at most the whole slice.
    const fn staging_bytes(self, chunks: usize) -> u64 {
        SampleBuffer::bytes(self.npol, chunks * self.placements)
    }

    /// Placements the weight-owner buffer takes.
    const fn weight_placements(self) -> usize {
        if self.weight { self.placements } else { 0 }
    }

    /// Bytes a chunk of this shape holds: each owner's and the weight
    /// owner's placements, one row's residual and model visibilities, and
    /// its prediction batch.
    const fn bytes(self) -> u64 {
        SampleBuffer::bytes(
            self.npol,
            self.owners * self.placements + self.weight_placements(),
        ) + self.prediction
            + (2 * self.cells * size_of::<Complex32>()) as u64
    }
}

/// The backend that degrids one domain's prediction batch for one chunk:
/// the pass's, so that on Metal the model is predicted on the device, which
/// works on one chunk's batch while the host places the next chunk's.
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
                .map(|_| SampleBuffer::with_capacity(shape.npol, shape.placements))
                .collect(),
            weight_owned: SampleBuffer::with_capacity(shape.npol, shape.weight_placements()),
            placed: 0,
            placed_rows: Vec::with_capacity(shape.rows),
            prediction: SpectralResampler::prediction_scratch(
                domains.iter().map(|domain| domain.resampler),
                shape.npol,
                shape.channels,
                shape.rows,
            ),
            predicted: Vec::with_capacity(shape.cells),
            residual: Vec::with_capacity(shape.cells),
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
            + (self.placed_rows.capacity() * size_of::<PlacedRow>()) as u64
            + ((self.predicted.capacity() + self.residual.capacity()) * size_of::<Complex32>())
                as u64
    }
}

/// Row chunks of a wave on `workers` workers.
const fn chunks(workers: usize) -> usize {
    workers * CHUNKS_PER_WORKER
}

/// Bytes a wave places with on `workers` workers over blocks of `block`,
/// gridding the weight image when `weight`: its row chunks and, on Metal,
/// each owner's staging and, with a model, each chunk's predictors' rings
/// ([`casa_imaging_metal::ring_bytes`]). They grow with the source's block
/// only up to [`CHUNK_PLACEMENTS`](super::CHUNK_PLACEMENTS) a chunk.
pub(super) fn chunk_bytes(
    domains: &[PassDomain<'_>],
    backend: BackendChoice,
    block: BlockShape,
    workers: usize,
    weight: bool,
    with_model: bool,
) -> u64 {
    let count = chunks(workers);
    let shape = ChunkShape::new(domains, block, weight, count);
    let device = match backend {
        BackendChoice::Cpu => 0,
        BackendChoice::Metal => {
            let staging = (domains.len() * shape.owners) as u64 * shape.staging_bytes(count);
            let predictors = if with_model {
                (count * domains.len()) as u64 * casa_imaging_metal::ring_bytes(shape.npol)
            } else {
                0
            };
            staging + predictors
        }
    };
    count as u64 * shape.bytes() + device
}

/// Bytes the model visibilities of one block of `block` hold, which a wave
/// with a model predicts for every row of the block at once.
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
    /// Each chunk's [`Predictor`] per domain.
    predictors: Vec<Vec<Predictor<'w>>>,
    predictions: Vec<Complex32>,
    samples: u64,
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
        let shape = ChunkShape::new(pass.domains, block, pass.modes.weight, count);
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
                                    backend: OwnerBackend::Metal {
                                        backend: Box::new(
                                            MetalBackend::new(
                                                operator.cf(),
                                                pass.metal_stage_timing,
                                            )?
                                            .with_deferred_grid_completion(),
                                        ),
                                        staging: SampleBuffer::with_capacity(
                                            shape.npol,
                                            count * shape.placements,
                                        ),
                                    },
                                    images: None,
                                }
                            }
                        })
                    })
                    .collect::<Result<_, PassError>>()?;
                Ok(Domain {
                    router,
                    owners,
                    model: models.map(|models| &models[index]),
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
            predictions: Vec::with_capacity(predictions),
            samples: 0,
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
    /// When the wave forms residuals at native channels, each row's samples
    /// are placed from `V − Σ_d A_d·m_d` at its native channels and gridded
    /// as data; otherwise the domain's model is subtracted at each placed
    /// sample.
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
        self.predictions.clear();
        if self.native_residuals || sink_predictions {
            let cells = block.channels() * block.correlations();
            self.predictions.resize(rows * cells, Complex32::default());
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
                &self.predictions
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
        let predictions = &self.predictions;
        let native_residuals = self.native_residuals;
        let cells = block.channels() * block.correlations();
        let shape = self.shape;
        // With region owners the weight image has one owner, which must see
        // every placement; plane owners grid their own samples' weights.
        let route_weight = pass.modes.weight && router.weight_owner().is_some();
        team.for_each_mut(&mut self.chunks[..count], |_, chunk| {
            chunk.placed = 0;
            chunk.owned[..owners]
                .iter_mut()
                .for_each(SampleBuffer::clear);
            chunk.weight_owned.clear();
            for position in chunk.rows.clone() {
                let native = block.row(index, row_at(block, ordered, position));
                let native = if native_residuals {
                    let model = &predictions[position * cells..(position + 1) * cells];
                    chunk.residual.clear();
                    chunk.residual.extend(
                        native
                            .values
                            .iter()
                            .zip(model)
                            .map(|(value, model)| value - model),
                    );
                    NativeRow {
                        values: &chunk.residual,
                        ..native
                    }
                } else {
                    native
                };
                let mut row_samples = 0;
                let (owned, weight_owned, placed) =
                    (&mut chunk.owned, &mut chunk.weight_owned, &mut chunk.placed);
                target.resampler.place_each(
                    target.operator,
                    pass.weighting,
                    &native,
                    |placement, values, weights| {
                        row_samples += 1;
                        if !planes.contains(placement.plane) {
                            return;
                        }
                        let owner = router.owner(target.operator, &placement);
                        owned[owner].push(placement, values, weights);
                        if route_weight {
                            weight_owned.push(placement, values, weights);
                        }
                        *placed += 1;
                    },
                )?;
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
        let model = domain.model.filter(|_| !native_residuals);
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
            match &mut owner.backend {
                OwnerBackend::Cpu(backend) => {
                    for chunk in chunks {
                        let block = chunk.owned[owner_index].block();
                        if !block.is_empty() {
                            accumulate(pass, operator, backend, &block, model, acc, own_weight)?;
                        }
                    }
                }
                OwnerBackend::Metal { backend, staging } => {
                    staging.clear();
                    for chunk in chunks {
                        staging.extend_from_block(&chunk.owned[owner_index].block());
                    }
                    if !staging.is_empty() {
                        let backend: &mut MetalBackend<'_> = backend;
                        accumulate(
                            pass,
                            operator,
                            backend,
                            &staging.block(),
                            model,
                            acc,
                            own_weight,
                        )?;
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
                        OwnerBackend::Cpu(backend) => backend.apply(&block, operator.cf(), work)?,
                        OwnerBackend::Metal { backend, .. } => {
                            backend.apply(&block, operator.cf(), work)?;
                        }
                    }
                }
            }
            Ok::<_, PassError>(())
        })?;
        Ok(chunks.iter().map(|chunk| chunk.placed).sum())
    }

    /// Model visibilities of every selected sample of the rows `slice` of
    /// `block`, which the first `count` chunks hold, summed over every
    /// domain's model (`SIMapperCollection::degrid`), `[row][channel]
    /// [correlation]`, into their rows of `self.predictions`, which stay
    /// zero without a model. Rows are by position in the block's locality
    /// order when `ordered` ([`row_at`]).
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
        let cells = block.channels() * block.correlations();
        let pass = self.pass;
        let shape = self.shape;
        let models = self
            .domains
            .iter()
            .map(|domain| domain.model)
            .collect::<Vec<_>>();
        let mut pieces = Vec::with_capacity(count);
        let mut remaining = &mut self.predictions[slice.start * cells..slice.end * cells];
        for (chunk, predictors) in self.chunks[..count].iter_mut().zip(&mut self.predictors) {
            let (head, tail) = remaining.split_at_mut(chunk.rows.len() * cells);
            remaining = tail;
            pieces.push((chunk, predictors, head));
        }
        // Domain by domain, every chunk places its rows' samples in one batch
        // and hands it to its predictor, a device predicting one chunk's
        // batch while the host places the next chunk's; then every chunk
        // reads its predictions back, interpolates them to its rows'
        // channels and adds them, so the domains' predictions sum in domain
        // order.
        for (index, (target, model)) in pass.domains.iter().zip(&models).enumerate() {
            let Some(model) = *model else {
                continue;
            };
            let npol = target.operator.polarization().correlations().len();
            team.for_each_mut(&mut pieces, |_, (chunk, predictors, _)| {
                chunk.prediction.begin(npol);
                chunk.placed_rows.clear();
                for position in chunk.rows.clone() {
                    chunk.placed_rows.push(target.resampler.place_prediction(
                        target.operator,
                        model,
                        &block.row(index, row_at(block, ordered, position)),
                        &mut chunk.prediction,
                    )?);
                }
                match &mut predictors[index] {
                    Predictor::Cpu(backend) => {
                        chunk
                            .prediction
                            .apply(backend, target.operator.cf(), model)?;
                    }
                    Predictor::Metal(backend) => {
                        let out = std::mem::take(chunk.prediction.predictions_mut());
                        backend.submit_prediction(&chunk.prediction.placed(), model, out)?;
                    }
                }
                Ok::<_, PassError>(())
            })?;
            team.for_each_mut(&mut pieces, |_, (chunk, predictors, out)| {
                if let Predictor::Metal(backend) = &mut predictors[index] {
                    let predicted = backend.collect_prediction(&chunk.prediction.placed())?;
                    *chunk.prediction.predictions_mut() = predicted;
                }
                chunk.predicted.resize(cells, Complex32::default());
                for (local, position) in chunk.rows.clone().enumerate() {
                    target.resampler.finish_prediction(
                        target.operator,
                        &block.row(index, row_at(block, ordered, position)),
                        chunk.placed_rows[local],
                        &mut chunk.prediction,
                        &mut chunk.predicted,
                    )?;
                    let out = &mut out[local * cells..(local + 1) * cells];
                    for (sum, value) in out.iter_mut().zip(&chunk.predicted) {
                        *sum += value;
                    }
                }
                Ok::<_, PassError>(())
            })?;
        }
        debug_assert!(
            pieces
                .iter()
                .all(|(chunk, ..)| chunk.capacity_bytes() == shape.bytes()),
            "a chunk predicts within the buffers it was allocated"
        );
        Ok(())
    }

    /// Transform the accumulated grids into this wave's normal images, one
    /// per domain.
    pub(super) fn finish(mut self, team: &WorkerTeam) -> Result<Vec<NormalImages>, PassError> {
        // Metal owners leave their last grid work running on the device:
        // complete it, and learn whether it failed, before any accumulator is
        // read.
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

/// Grid one owner's share of a block in every mode the pass holds; with a
/// model the data terms receive the residual `V − A·m`.
fn accumulate(
    pass: &MajorCyclePass<'_>,
    operator: &casa_imaging_operator::MeasurementOperator,
    backend: &mut dyn GridBackend,
    block: &casa_imaging_operator::SampleBlock<'_>,
    model: Option<&PreparedModelGrids>,
    acc: &mut GridAccumulator,
    weight: bool,
) -> Result<(), PassError> {
    let cf = operator.cf();
    if pass.modes.data {
        let work = match model {
            Some(model) => Work::ResidualGrid {
                model,
                acc,
                residual_out: None,
            },
            None => Work::Grid {
                mode: Mode::Data,
                acc,
            },
        };
        backend.apply(block, cf, work)?;
    }
    if pass.modes.psf {
        backend.apply(
            block,
            cf,
            Work::Grid {
                mode: Mode::Psf,
                acc,
            },
        )?;
    }
    if pass.modes.weight && weight {
        backend.apply(
            block,
            cf,
            Work::Grid {
                mode: Mode::Weight,
                acc,
            },
        )?;
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
