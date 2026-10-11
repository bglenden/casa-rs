// SPDX-License-Identifier: LGPL-3.0-or-later
//! One wave of a major-cycle pass: prediction and placement on row chunks,
//! accumulation by owner for each image domain, and the per-plane
//! transforms.

use std::ops::Range;

use casa_imaging_metal::MetalBackend;
use casa_imaging_operator::{
    CpuBackend, GridAccumulator, GridBackend, GridPrecision, Mode, NativeRow, NormalImages,
    PlaneRange, PredictionScratch, PreparedModelGrids, SampleBlock, SampleBuffer, Work,
};
use num_complex::Complex32;

use super::partition::Router;
use super::{
    BackendChoice, BlockShape, CHUNK_ROWS, MajorCyclePass, NativeBlock, Partition, PassDomain,
    PassError, VisibilitySink, WorkerTeam, chunk_ranges, slices,
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

/// The backend an owner dispatches to. The CPU grids each placement chunk
/// as it comes; a Metal owner first gathers its share of every chunk of a
/// source block into `staging`, so each device dispatch is one block's
/// worth of samples rather than one chunk's.
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

/// One row chunk's placements on the wave's planes, grouped by owner, its
/// prediction scratch and its rows' native-channel residuals.
///
/// Every buffer is allocated once, for the largest chunk of the source's
/// largest block ([`ChunkShape`]), so a chunk holds [`ChunkShape::bytes`]
/// however its placements fall among the owners.
struct Chunk {
    rows: Range<usize>,
    /// One row's placements.
    scratch: SampleBuffer,
    /// The chunk's placements on the wave's planes in row order; the weight
    /// image's single owner ([`Router::weight_owner`]) grids them all.
    placed: SampleBuffer,
    /// The owner of each placement of `placed`.
    owners: Vec<u32>,
    /// `placed` grouped by owner, each owner's in row order: owner `o`'s are
    /// `starts[o]..starts[o + 1]`. Unused with one owner, who reads `placed`.
    routed: SampleBuffer,
    starts: Vec<usize>,
    /// Indices into `placed` in `routed` order.
    order: Vec<u32>,
    backend: CpuBackend,
    prediction: PredictionScratch,
    predicted: Vec<Complex32>,
    residual: Vec<Complex32>,
}

/// The largest chunk of a wave over rows of `block`'s layout: at most
/// [`CHUNK_ROWS`] rows, whatever rows the source puts in a block.
#[derive(Clone, Copy)]
struct ChunkShape {
    /// Placements one native row places on any domain.
    row: usize,
    /// Placements the chunk's rows place.
    placements: usize,
    /// Correlations per placement.
    npol: usize,
    /// Correlations and channels of one row.
    cells: usize,
    /// Owners of the domain with the most.
    owners: usize,
}

impl ChunkShape {
    fn new(domains: &[PassDomain<'_>], block: BlockShape) -> Self {
        let row = domains
            .iter()
            .map(|domain| domain.resampler.samples_per_row(block.channels))
            .max()
            .unwrap_or(0);
        Self {
            row,
            placements: CHUNK_ROWS * row,
            npol: block.correlations,
            cells: block.channels * block.correlations,
            owners: domains
                .iter()
                .map(|domain| domain.partition.owners())
                .max()
                .unwrap_or(1),
        }
    }

    /// Bytes a Metal owner's staging holds: its share of every chunk of a
    /// slice, at most the whole slice.
    const fn staging_bytes(self, chunks: usize) -> u64 {
        SampleBuffer::bytes(self.npol, chunks * self.placements)
    }

    /// Bytes a chunk of this shape holds: its placement, owner and order
    /// buffers, one row's residual and model visibilities, and its
    /// prediction scratch, which holds one row's samples, their sources and
    /// their values.
    const fn bytes(self) -> u64 {
        let routed = if self.owners > 1 { self.placements } else { 0 };
        let prediction = SampleBuffer::bytes(self.npol, self.row)
            + (self.row * (size_of::<usize>() + 2 * self.npol * size_of::<Complex32>())) as u64;
        SampleBuffer::bytes(self.npol, self.row + self.placements + routed)
            + prediction
            + (2 * self.placements * size_of::<u32>()
                + (self.owners + 1) * size_of::<usize>()
                + 2 * self.cells * size_of::<Complex32>()) as u64
    }
}

impl Chunk {
    fn new(shape: ChunkShape) -> Self {
        let routed = if shape.owners > 1 {
            shape.placements
        } else {
            0
        };
        Self {
            rows: 0..0,
            scratch: SampleBuffer::with_capacity(shape.npol, shape.row),
            placed: SampleBuffer::with_capacity(shape.npol, shape.placements),
            owners: Vec::with_capacity(shape.placements),
            routed: SampleBuffer::with_capacity(shape.npol, routed),
            starts: Vec::with_capacity(shape.owners + 1),
            order: Vec::with_capacity(routed),
            backend: CpuBackend::new(),
            prediction: PredictionScratch::default(),
            predicted: Vec::with_capacity(shape.cells),
            residual: Vec::with_capacity(shape.cells),
        }
    }

    /// Group `placed` by owner into `routed`, keeping row order within each
    /// owner, for `owners` owners.
    fn route(&mut self, owners: usize) {
        self.starts.clear();
        self.starts.resize(owners + 1, 0);
        for &owner in &self.owners {
            self.starts[owner as usize + 1] += 1;
        }
        for owner in 0..owners {
            self.starts[owner + 1] += self.starts[owner];
        }
        // A stable counting sort; `starts` is restored as it is consumed.
        self.order.clear();
        self.order.resize(self.owners.len(), 0);
        for (index, &owner) in self.owners.iter().enumerate() {
            let slot = &mut self.starts[owner as usize];
            self.order[*slot] = index as u32;
            *slot += 1;
        }
        self.starts.copy_within(..owners, 1);
        self.starts[0] = 0;
        self.routed.clear();
        let placed = self.placed.block();
        for &index in &self.order {
            let index = index as usize;
            self.routed.push(
                placed.placements[index],
                placed.values_of(index),
                placed.weights_of(index),
            );
        }
    }

    /// Owner `owner`'s placements of `owners`.
    fn owned(&self, owner: usize, owners: usize) -> SampleBlock<'_> {
        if owners == 1 {
            self.placed.block()
        } else {
            self.routed
                .block_range(self.starts[owner]..self.starts[owner + 1])
        }
    }
}

/// Row chunks of a wave on `workers` workers.
const fn chunks(workers: usize) -> usize {
    workers * CHUNKS_PER_WORKER
}

/// Bytes a wave places with on `workers` workers over rows of `block`'s
/// layout: its row chunks and, on Metal, each owner's staging. They do not
/// grow with the source's block ([`CHUNK_ROWS`]).
pub(super) fn chunk_bytes(
    domains: &[PassDomain<'_>],
    backend: BackendChoice,
    block: BlockShape,
    workers: usize,
) -> u64 {
    let count = chunks(workers);
    let shape = ChunkShape::new(domains, block);
    let staging = match backend {
        BackendChoice::Cpu => 0,
        BackendChoice::Metal => (domains.len() * shape.owners) as u64 * shape.staging_bytes(count),
    };
    count as u64 * shape.bytes() + staging
}

/// Bytes the model visibilities of one block of `block` hold, which a wave
/// with a model predicts for every row of the block at once.
pub(super) const fn prediction_bytes(block: BlockShape) -> u64 {
    (block.rows * block.channels * block.correlations * size_of::<Complex32>()) as u64
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
        let shape = ChunkShape::new(pass.domains, block);
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
                                acc: Some(operator.accumulator(range, tile, pass.modes)),
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
                                    acc: Some(MetalBackend::accumulator(
                                        operator.accumulator_layout(range, tile, pass.modes),
                                    )?),
                                    backend: OwnerBackend::Metal {
                                        backend: Box::new(MetalBackend::new(operator.cf())?),
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
            chunks: (0..count).map(|_| Chunk::new(shape)).collect(),
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
        if self.native_residuals || sink_predictions {
            let count = self.chunks.len().min(rows.max(1));
            for (index, chunk) in self.chunks[..count].iter_mut().enumerate() {
                chunk.rows = index * rows / count..(index + 1) * rows / count;
            }
            self.predict(block, team, count)?;
        } else {
            self.predictions.clear();
        }
        if let Some(sink) = visibilities {
            let predictions: &[Complex32] = if sink.predictions {
                &self.predictions
            } else {
                &[]
            };
            (sink.write)(block, predictions).map_err(PassError::VisibilityWrite)?;
        }
        // Slices of the block, each owner's in row order as a whole block's
        // would be, so the accumulation is the same.
        let chunks = self.chunks.len();
        for slice in slices(rows, chunks) {
            let mut count = 0;
            for (chunk, rows) in self.chunks.iter_mut().zip(chunk_ranges(slice, chunks)) {
                chunk.rows = rows;
                count += 1;
            }
            for index in 0..self.domains.len() {
                let placed = self.place(block, team, count, index)?;
                if index == 0 {
                    self.samples += placed;
                }
            }
        }
        Ok(())
    }

    /// Place the rows of `block` on domain `index` and accumulate them;
    /// returns the samples placed.
    fn place(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        count: usize,
        index: usize,
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
        team.for_each_mut(&mut self.chunks[..count], |_, chunk| {
            chunk.placed.clear();
            chunk.owners.clear();
            for row in chunk.rows.clone() {
                chunk.scratch.clear();
                let native = block.row(index, row);
                let native = if native_residuals {
                    let model = &predictions[row * cells..(row + 1) * cells];
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
                target.resampler.place(
                    target.operator,
                    pass.weighting,
                    &native,
                    &mut chunk.scratch,
                )?;
                debug_assert!(
                    chunk.scratch.len() <= shape.row,
                    "a row places within the chunk's per-row bound"
                );
                let placed = chunk.scratch.block();
                for (sample, placement) in placed.placements.iter().enumerate() {
                    if !planes.contains(placement.plane) {
                        continue;
                    }
                    chunk
                        .owners
                        .push(router.owner(target.operator, placement) as u32);
                    chunk.placed.push(
                        *placement,
                        placed.values_of(sample),
                        placed.weights_of(sample),
                    );
                }
            }
            debug_assert!(
                chunk.placed.len() <= shape.placements,
                "a chunk places within the placements it was allocated for"
            );
            if owners > 1 {
                chunk.route(owners);
            }
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
                        let block = chunk.owned(owner_index, owners);
                        if !block.is_empty() {
                            accumulate(pass, operator, backend, &block, model, acc, own_weight)?;
                        }
                    }
                }
                OwnerBackend::Metal { backend, staging } => {
                    staging.clear();
                    for chunk in chunks {
                        let block = chunk.owned(owner_index, owners);
                        for sample in 0..block.len() {
                            staging.push(
                                block.placements[sample],
                                block.values_of(sample),
                                block.weights_of(sample),
                            );
                        }
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
                    let block = chunk.placed.block();
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
        Ok(chunks.iter().map(|chunk| chunk.placed.len() as u64).sum())
    }

    /// Model visibilities of every selected sample of `block`, summed over
    /// every domain's model (`SIMapperCollection::degrid`), `[row][channel]
    /// [correlation]`, into `self.predictions`; zero without a model.
    fn predict(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        count: usize,
    ) -> Result<(), PassError> {
        let cells = block.channels() * block.correlations();
        let rows = block.len();
        self.predictions.clear();
        self.predictions.resize(rows * cells, Complex32::default());
        if self.domains.iter().all(|domain| domain.model.is_none()) {
            return Ok(());
        }
        let pass = self.pass;
        let models = self
            .domains
            .iter()
            .map(|domain| domain.model)
            .collect::<Vec<_>>();
        let mut pieces = Vec::with_capacity(count);
        let mut remaining = self.predictions.as_mut_slice();
        for chunk in &mut self.chunks[..count] {
            let (head, tail) = remaining.split_at_mut(chunk.rows.len() * cells);
            remaining = tail;
            pieces.push((chunk, head));
        }
        team.for_each_mut(&mut pieces, |_, (chunk, out)| {
            chunk.predicted.resize(cells, Complex32::default());
            for (local, row) in chunk.rows.clone().enumerate() {
                let out = &mut out[local * cells..(local + 1) * cells];
                for (index, (target, model)) in pass.domains.iter().zip(&models).enumerate() {
                    let Some(model) = model else {
                        continue;
                    };
                    target.resampler.predict_row(
                        target.operator,
                        &mut chunk.backend,
                        model,
                        &block.row(index, row),
                        &mut chunk.prediction,
                        &mut chunk.predicted,
                    )?;
                    for (sum, value) in out.iter_mut().zip(&chunk.predicted) {
                        *sum += value;
                    }
                }
            }
            Ok::<_, PassError>(())
        })
    }

    /// Transform the accumulated grids into this wave's normal images, one
    /// per domain.
    pub(super) fn finish(self, team: &WorkerTeam) -> Result<Vec<NormalImages>, PassError> {
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
                        let mut merged = operator.accumulator(planes, None, pass.modes);
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
