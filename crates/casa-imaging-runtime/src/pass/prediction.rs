// SPDX-License-Identifier: LGPL-3.0-or-later
//! Predicting a block's rows at their native cells, `Σ_d A_d·m_d` over the
//! image domains (`SIMapperCollection::degrid`), on row chunks of the
//! worker team: the one prediction path of the major-cycle pass, which
//! subtracts the model at native cells or hands it to a visibility writer.
//!
//! Each row chunk places its rows' degridded samples in one batch with the
//! rows' native terms and hands the batch to its predictor for the domain
//! ([`Work::PredictNative`](casa_imaging_operator::Work::PredictNative)):
//! the first domain writes the cells and later ones add to them. On Metal
//! the cells stay on the device, which predicts one chunk's batch while
//! the host places the next.
//!
//! [`BlockPredictor`] is the same path for a caller that is not a major
//! cycle: the simulator, which predicts the rows it writes.

use std::ops::Range;

use casa_imaging_metal::{MetalBackend, MetalNoiseRow, MetalNoiseTerm};
use casa_imaging_operator::{
    CpuBackend, DeviceNativeCells, GridPrecision, MeasurementOperator, NativeModel, NativeTarget,
    PredictionScratch, PreparedModelGrids, SpectralResampler,
};
use num_complex::Complex32;

use super::block::{cell_of, row_at};
use super::{
    BackendChoice, BlockShape, CHUNK_PLACEMENTS, NativeBlock, PassError, WorkerTeam, chunk_ranges,
    chunk_rows, slices,
};

/// One image domain's share of a prediction: its operator, the resampler
/// that maps native rows to its planes, and its prepared model.
///
/// A [`BlockPredictor`] predicts `Σ_d A_d·m_d` over its domains, each row
/// read in the projection of its own domain ([`NativeBlock::row`]).
#[derive(Clone, Copy)]
pub struct PredictionDomain<'a> {
    /// The measurement operator.
    pub operator: &'a MeasurementOperator,
    /// Maps native rows to the operator's planes.
    pub resampler: &'a SpectralResampler,
    /// The model grids of every plane the rows reach.
    pub model: &'a PreparedModelGrids,
}

/// Noise a [`BlockPredictor`] adds on the device to the rows it predicts
/// ([`BlockPredictor::predict_into_with_noise`]): its terms, added in
/// order, and each row's number and autocorrelation flag.
#[derive(Clone, Copy, Debug)]
pub struct DeviceNoise<'a> {
    /// One per row of the block, in order.
    pub rows: &'a [MetalNoiseRow],
    /// The noise terms.
    pub terms: &'a [MetalNoiseTerm],
}

/// What a block prediction's row chunks are allocated for.
#[derive(Clone, Copy, Debug)]
pub(super) struct PredictionShape {
    /// Correlations per row.
    pub npol: usize,
    /// Channels per row.
    pub channels: usize,
    /// Rows a chunk predicts at once.
    pub rows: usize,
    /// Row chunks.
    pub chunks: usize,
}

impl PredictionShape {
    /// Samples the batch of one chunk's rows degrids at most, on any of
    /// `resamplers`.
    fn predicted<'r>(self, resamplers: impl Iterator<Item = &'r SpectralResampler>) -> usize {
        self.rows
            * resamplers
                .map(|resampler| resampler.prediction_samples_per_row(self.channels))
                .max()
                .unwrap_or(0)
    }
}

/// The backend that predicts one domain's batch for one chunk and forms the
/// model at its rows' native cells: the pass's, so that on Metal the model
/// never leaves the device and the device works on one chunk's batch while
/// the host places the next chunk's.
enum Predictor<'w> {
    Cpu(CpuBackend),
    Metal(Box<MetalBackend<'w>>),
}

/// One row chunk: its rows, the batch it places them in and its predictor
/// for each domain.
struct PredictionChunk<'w> {
    rows: Range<usize>,
    scratch: PredictionScratch,
    predictors: Vec<Predictor<'w>>,
}

/// The model at a block's native cells, `[position][channel][correlation]`
/// by the rows' positions in the block: in host memory for CPU predictors,
/// in the device's for Metal ones, which form it and read it there.
enum NativeStore {
    Host(Vec<Complex32>),
    Device(DeviceNativeCells),
}

/// Where one call forms the model at native cells.
enum Cells<'a> {
    /// Host cells of the block's rows.
    Host(&'a mut [Complex32]),
    /// The device cells of the block.
    Device(&'a DeviceNativeCells),
}

/// One chunk's share of the cells while it predicts.
enum ChunkCells<'c> {
    /// The chunk's rows' host cells.
    Host(&'c mut [Complex32]),
    /// The device cells of the whole block.
    Device(&'c DeviceNativeCells),
}

/// The row chunks that predict a block, with their predictors and the
/// model at the block's native cells.
pub(super) struct BlockPrediction<'w> {
    chunks: Vec<PredictionChunk<'w>>,
    /// Rows a chunk predicts at once.
    rows: usize,
    /// Bytes each chunk's batch was allocated with, which predicting never
    /// grows.
    scratch_bytes: u64,
    store: NativeStore,
}

impl<'w> BlockPrediction<'w> {
    /// Bytes [`Self::new`] holds for the domains of `domains` on `backend`
    /// with chunks of `shape`, apart from the store: each chunk's batch
    /// ([`SpectralResampler::prediction_scratch_bytes`]) and, `with_model`,
    /// its predictors' own. On the CPU a predictor keeps its batch's
    /// predictions ([`CpuBackend::batch_bytes`]); on Metal a ring, the
    /// batch's predictions and its own copies of the kernel cells it meets
    /// ([`casa_imaging_metal::ring_bytes`],
    /// [`casa_imaging_metal::batch_bytes`],
    /// [`casa_imaging_metal::kernel_bytes`]; #793 review).
    pub(super) fn bytes<'d>(
        domains: impl Iterator<Item = (&'d MeasurementOperator, &'d SpectralResampler)> + Clone,
        backend: BackendChoice,
        with_model: bool,
        shape: PredictionShape,
    ) -> u64 {
        let count = shape.chunks as u64;
        let scratch = SpectralResampler::prediction_scratch_bytes(
            domains.clone().map(|(_, resampler)| resampler),
            shape.npol,
            shape.channels,
            shape.rows,
        );
        let predicted = shape.predicted(domains.clone().map(|(_, resampler)| resampler));
        let domain_count = domains.clone().count() as u64;
        let predictors = match backend {
            BackendChoice::Cpu if with_model => {
                domain_count * CpuBackend::batch_bytes(shape.npol, predicted)
            }
            BackendChoice::Metal if with_model => {
                let kernels = domains
                    .map(|(operator, _)| casa_imaging_metal::kernel_bytes(operator.cf()))
                    .sum::<u64>();
                domain_count
                    * (casa_imaging_metal::ring_bytes(shape.npol)
                        + casa_imaging_metal::batch_bytes(shape.npol, predicted))
                    + kernels
            }
            BackendChoice::Cpu | BackendChoice::Metal => 0,
        };
        count * (scratch + predictors)
    }

    /// Chunks of `shape` for the domains of `domains`, each with a predictor
    /// per domain on `backend` (with `stage_timing`, a Metal predictor logs
    /// its usage when it drops), and a store of `cells` native cells: in
    /// device memory on Metal, in host memory, which grows to the block,
    /// otherwise.
    ///
    /// With `allocate_batches` a CPU predictor's batch is allocated once, at
    /// its largest, as a [`super::BlockPredictor`], which predicts every
    /// block, needs. A wave leaves it to grow with the blocks it predicts,
    /// as it always has: a fused-residual or pipelined pass never predicts
    /// through it.
    pub(super) fn new(
        domains: impl Iterator<Item = (&'w MeasurementOperator, &'w SpectralResampler)> + Clone,
        backend: BackendChoice,
        stage_timing: bool,
        shape: PredictionShape,
        allocate_batches: bool,
        cells: usize,
    ) -> Result<Self, PassError> {
        let samples = shape.predicted(domains.clone().map(|(_, resampler)| resampler));
        // Records of exactly the chunks and domains, as `record_bytes`
        // charges them: collecting fallibly would size them by growth.
        let mut chunks = Vec::with_capacity(shape.chunks);
        for _ in 0..shape.chunks {
            let mut predictors = Vec::with_capacity(domains.clone().count());
            for (operator, _) in domains.clone() {
                predictors.push(match backend {
                    BackendChoice::Cpu if allocate_batches => {
                        Predictor::Cpu(CpuBackend::for_prediction(
                            operator.polarization().correlations().len(),
                            operator.basis().data_terms(),
                            samples,
                        ))
                    }
                    BackendChoice::Cpu => Predictor::Cpu(CpuBackend::new()),
                    BackendChoice::Metal => {
                        Predictor::Metal(Box::new(MetalBackend::new(operator.cf(), stage_timing)?))
                    }
                });
            }
            chunks.push(PredictionChunk {
                rows: 0..0,
                scratch: SpectralResampler::prediction_scratch(
                    domains.clone().map(|(_, resampler)| resampler),
                    shape.npol,
                    shape.channels,
                    shape.rows,
                ),
                predictors,
            });
        }
        let store = match backend {
            BackendChoice::Metal if cells > 0 => {
                NativeStore::Device(MetalBackend::native_cells(cells)?)
            }
            BackendChoice::Cpu | BackendChoice::Metal => {
                NativeStore::Host(Vec::with_capacity(cells))
            }
        };
        Ok(Self {
            chunks,
            rows: shape.rows,
            scratch_bytes: SpectralResampler::prediction_scratch_bytes(
                domains.map(|(_, resampler)| resampler),
                shape.npol,
                shape.channels,
                shape.rows,
            ),
            store,
        })
    }

    /// The model at native cells of every selected sample of `block`'s rows,
    /// summed over `domains` (the block's projections, in order),
    /// `[row][channel][correlation]`, into the store; rows are by position
    /// in the block's locality order when `ordered` ([`row_at`]). Without
    /// a domain the host store holds zeros for the block.
    pub(super) fn predict(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        domains: &[PredictionDomain<'_>],
        ordered: bool,
    ) -> Result<(), PassError> {
        let cells = block.len() * block.channels() * block.correlations();
        if let NativeStore::Host(store) = &mut self.store {
            // The first domain writes every cell of the rows it predicts, so
            // stale cells never reach a sample; without a model the cells
            // stay zero.
            if store.len() < cells || domains.is_empty() {
                store.clear();
                store.resize(cells, Complex32::default());
            }
        }
        if domains.is_empty() {
            return Ok(());
        }
        let target = match &mut self.store {
            NativeStore::Host(store) => Cells::Host(&mut store[..cells]),
            NativeStore::Device(store) => Cells::Device(store),
        };
        predict_rows(
            &mut self.chunks,
            self.rows,
            block,
            team,
            domains,
            ordered,
            target,
        )?;
        debug_assert!(
            self.chunks
                .iter()
                .all(|chunk| chunk.scratch.capacity_bytes() == self.scratch_bytes),
            "a chunk predicts within the batch it was allocated"
        );
        Ok(())
    }

    /// The model at native cells of every selected sample of `block`'s rows,
    /// summed over `domains`, in row order into `out`: on the CPU directly,
    /// on Metal from the device store once every predictor has completed,
    /// with `noise`'s terms added to each row there first.
    fn predict_into(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        domains: &[PredictionDomain<'_>],
        out: &mut [Complex32],
        noise: Option<DeviceNoise<'_>>,
    ) -> Result<(), PassError> {
        match &self.store {
            NativeStore::Host(_) => {
                assert!(noise.is_none(), "noise is added on the device only");
                predict_rows(
                    &mut self.chunks,
                    self.rows,
                    block,
                    team,
                    domains,
                    false,
                    Cells::Host(out),
                )?;
            }
            NativeStore::Device(store) => {
                predict_rows(
                    &mut self.chunks,
                    self.rows,
                    block,
                    team,
                    domains,
                    false,
                    Cells::Device(store),
                )?;
                if let Some(noise) = noise {
                    // Any backend of the device adds it: its command follows
                    // every native-model command on the one queue.
                    let Some(Predictor::Metal(backend)) = self
                        .chunks
                        .first_mut()
                        .and_then(|chunk| chunk.predictors.first_mut())
                    else {
                        unreachable!("a device store's predictors are Metal's");
                    };
                    backend.add_cell_noise(
                        store,
                        noise.rows,
                        noise.terms,
                        (block.channels(), block.correlations()),
                    )?;
                }
                complete(&mut self.chunks)?;
                // SAFETY: only the predictors' work writes the cells, every
                // command of it has completed, and none is issued while the
                // slice lives.
                out.copy_from_slice(&unsafe { store.host_cells() }[..out.len()]);
            }
        }
        debug_assert!(
            self.chunks
                .iter()
                .all(|chunk| chunk.scratch.capacity_bytes() == self.scratch_bytes),
            "a chunk predicts within the batch it was allocated"
        );
        Ok(())
    }

    /// Bytes [`Self::new`]'s records hold for the domains of `domains` on
    /// `backend` with `chunks` chunks, beside [`Self::bytes`]: each chunk's
    /// record and its predictors' (a Metal one boxed), a CPU predictor's
    /// per-sample buffers ([`CpuBackend::prediction_sample_bytes`]), and a
    /// slice's hand-off of the chunks to the team, with each chunk's result.
    fn record_bytes<'d>(
        domains: impl Iterator<Item = (&'d MeasurementOperator, &'d SpectralResampler)>,
        backend: BackendChoice,
        chunks: usize,
    ) -> u64 {
        let per_domain = domains
            .map(|(operator, _)| {
                size_of::<Predictor<'_>>() as u64
                    + match backend {
                        BackendChoice::Cpu => CpuBackend::prediction_sample_bytes(
                            operator.polarization().correlations().len(),
                            operator.basis().data_terms(),
                        ),
                        BackendChoice::Metal => size_of::<MetalBackend<'_>>() as u64,
                    }
            })
            .sum::<u64>();
        let hand_off = size_of::<(&mut PredictionChunk<'_>, ChunkCells<'_>)>()
            + size_of::<Result<(), PassError>>();
        chunks as u64 * (size_of::<PredictionChunk<'_>>() as u64 + per_domain + hand_off as u64)
    }

    /// The model at native cells, as a residual reads it.
    pub(super) fn native_model(&self) -> NativeModel<'_> {
        match &self.store {
            NativeStore::Host(cells) => NativeModel::Host { cells, first: 0 },
            NativeStore::Device(cells) => NativeModel::Device(cells),
        }
    }

    /// The model at the first `len` native cells of the block for the host:
    /// on Metal once every predictor's work, which alone writes the cells,
    /// has completed.
    pub(super) fn host_cells(&mut self, len: usize) -> Result<&[Complex32], PassError> {
        match &self.store {
            NativeStore::Host(cells) => Ok(&cells[..len]),
            NativeStore::Device(cells) => {
                complete(&mut self.chunks)?;
                // SAFETY: only the predictors' work writes the cells, every
                // command of it has completed, and none is issued while the
                // slice, which borrows the prediction, lives.
                Ok(&unsafe { cells.host_cells() }[..len])
            }
        }
    }

    /// Wait for every Metal predictor's work, and learn whether any failed.
    pub(super) fn complete(&mut self) -> Result<(), PassError> {
        complete(&mut self.chunks)
    }
}

/// Predicts blocks of native rows, `Σ_d A_d·m_d` over its image domains
/// (`SIMapperCollection::degrid`), for a caller that is not a major cycle:
/// the simulator, which predicts the rows it writes. It is the major-cycle
/// pass's own prediction ([`super::MajorCyclePass`]): each row as
/// [`SpectralResampler::predict_row`] predicts it, a flagged row, a flagged
/// sample and an autocorrelation (unless the domain's resampler is
/// [`SpectralResampler::predicting_autocorrelations`]) predicting zero.
///
/// The rows are predicted in row chunks on a [`WorkerTeam`], on the CPU or
/// on Metal ([`BackendChoice`]). Each chunk keeps one prediction batch and
/// one backend per domain, so a predictor holds [`Self::bytes`] whatever
/// blocks within its [`BlockShape`] it predicts; on Metal the model at the
/// block's native cells is formed in device memory and copied out. A
/// domain's model grids, and on Metal their device copy, are the caller's
/// to admit ([`PreparedModelGrids::bytes`]).
///
/// The CPU's predictions do not depend on the workers, the chunking or how
/// the rows are split into blocks; Metal's agree with them to the device's
/// `f32` arithmetic.
pub struct BlockPredictor<'w> {
    domains: Vec<PredictionDomain<'w>>,
    block: BlockShape,
    prediction: BlockPrediction<'w>,
}

impl<'w> BlockPredictor<'w> {
    /// The chunks a predictor over `domains` for blocks of `block` on
    /// `workers` workers predicts in: as many as a major-cycle wave's, each
    /// of the rows whose degridded samples fit [`CHUNK_PLACEMENTS`] and at
    /// most a block's share.
    fn shape(
        domains: &[PredictionDomain<'_>],
        block: BlockShape,
        workers: usize,
    ) -> PredictionShape {
        let chunks = super::wave::chunks(workers);
        let row = domains
            .iter()
            .map(|domain| {
                domain
                    .resampler
                    .prediction_samples_per_row(block.channels())
            })
            .max()
            .unwrap_or(0);
        PredictionShape {
            npol: block.correlations,
            channels: block.channels(),
            rows: chunk_rows(row, block.rows, chunks, CHUNK_PLACEMENTS),
            chunks,
        }
    }

    /// Bytes [`Self::new`] holds, and [`Self::predict_into`] while it runs,
    /// for the same arguments: each chunk's prediction batch and record,
    /// each chunk's backend per domain with what it keeps
    /// ([`CpuBackend::batch_bytes`] and its per-sample buffers; on Metal
    /// [`casa_imaging_metal::ring_bytes`], [`casa_imaging_metal::batch_bytes`]
    /// and [`casa_imaging_metal::kernel_bytes`]), on Metal the model at the
    /// largest block's native cells in device memory, and a call's hand-off
    /// of the chunks to the team. The rows, the model grids and the output
    /// are the caller's.
    #[must_use]
    pub fn bytes(
        domains: &[PredictionDomain<'_>],
        backend: BackendChoice,
        block: BlockShape,
        workers: usize,
    ) -> u64 {
        let shape = Self::shape(domains, block, workers);
        let pairs = domains
            .iter()
            .map(|domain| (domain.operator, domain.resampler));
        let store = match backend {
            BackendChoice::Cpu => 0,
            BackendChoice::Metal => super::wave::prediction_bytes(block),
        };
        BlockPrediction::bytes(pairs.clone(), backend, true, shape)
            + BlockPrediction::record_bytes(pairs, backend, shape.chunks)
            + size_of_val(domains) as u64
            + store
    }

    /// Bytes a predictor of blocks of at most `block` on `backend` holds
    /// beyond [`Self::bytes`] once [`Self::predict_into_with_noise`] has
    /// added noise of at most `terms` terms: on Metal the one backend that
    /// adds it keeps the rows and the terms on the device
    /// ([`casa_imaging_metal::noise_bytes`]); nothing on the CPU, where the
    /// predictor adds none. The [`DeviceNoise`] is the caller's.
    #[must_use]
    pub const fn noise_bytes(backend: BackendChoice, block: BlockShape, terms: usize) -> u64 {
        match backend {
            BackendChoice::Cpu => 0,
            BackendChoice::Metal => casa_imaging_metal::noise_bytes(block.rows, terms),
        }
    }

    /// A predictor of blocks of at most `block` (rows, channels and
    /// correlations; each row projected on every domain) over `domains` on
    /// `backend`, in the chunks of `workers` workers; with `stage_timing`,
    /// each Metal backend logs its usage when it drops
    /// ([`casa_imaging_metal::METAL_STAGE_TIMING`]).
    ///
    /// # Errors
    ///
    /// [`PassError::NoDomains`] without a domain, [`PassError::Workers`]
    /// for no workers, and on Metal [`PassError::Operator`] when the host
    /// has no device.
    ///
    /// # Panics
    ///
    /// On Metal, when an operator is not `f32`: the device grids are `f32`.
    pub fn new(
        domains: &[PredictionDomain<'w>],
        backend: BackendChoice,
        block: BlockShape,
        workers: usize,
        stage_timing: bool,
    ) -> Result<Self, PassError> {
        if domains.is_empty() {
            return Err(PassError::NoDomains);
        }
        if workers == 0 {
            return Err(PassError::Workers);
        }
        if backend == BackendChoice::Metal {
            assert!(
                domains
                    .iter()
                    .all(|domain| domain.operator.precision() == GridPrecision::F32),
                "Metal predicts from f32 grids (D2)"
            );
        }
        let shape = Self::shape(domains, block, workers);
        let cells = match backend {
            BackendChoice::Cpu => 0,
            BackendChoice::Metal => block.rows * block.channels() * block.correlations,
        };
        let prediction = BlockPrediction::new(
            domains
                .iter()
                .map(|domain| (domain.operator, domain.resampler)),
            backend,
            stage_timing,
            shape,
            true,
            cells,
        )?;
        Ok(Self {
            domains: domains.to_vec(),
            block,
            prediction,
        })
    }

    /// The model visibility of every selected sample of `block`'s rows,
    /// summed over the domains, into `out` as `[row][channel][correlation]`
    /// in the block's row order, on `team`. Every value of `out` is
    /// written.
    ///
    /// # Errors
    ///
    /// A row the operators reject ([`PassError::Operator`]), or a device
    /// failure on Metal.
    ///
    /// # Panics
    ///
    /// When `block` exceeds the predictor's [`BlockShape`], does not project
    /// each row on every domain, or `out` is not one value per sample.
    pub fn predict_into(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        out: &mut [Complex32],
    ) -> Result<(), PassError> {
        assert_eq!(
            block.domains(),
            self.domains.len(),
            "a block projects each row on every domain of the predictor"
        );
        assert!(
            block.len() <= self.block.rows
                && block.channels() <= self.block.channels()
                && block.correlations() == self.block.correlations,
            "a block fits the shape the predictor was made for"
        );
        assert_eq!(
            out.len(),
            block.len() * block.channels() * block.correlations(),
            "one prediction per selected sample"
        );
        self.prediction
            .predict_into(block, team, &self.domains, out, None)
    }

    /// Whether the predictor forms the model in device memory, where
    /// [`Self::predict_into_with_noise`] adds noise to it.
    #[must_use]
    pub fn adds_noise_on_device(&self) -> bool {
        matches!(self.prediction.store, NativeStore::Device(_))
    }

    /// [`Self::predict_into`] with `noise` added to every row's model on
    /// the device before it is copied out: the draws of
    /// `casa_numerics::cell_noise::CellNoise::add_to_row` for each term in
    /// order, with each row's number and autocorrelation flag from
    /// `noise.rows` (one per row of `block`). The simulator corrupts the
    /// rows it predicts this way, with no pass of its own over the cells.
    ///
    /// # Errors
    ///
    /// As [`Self::predict_into`].
    ///
    /// # Panics
    ///
    /// As [`Self::predict_into`]; when the predictor does not
    /// [`Self::adds_noise_on_device`], or `noise.rows` is not one row per
    /// row of `block`.
    pub fn predict_into_with_noise(
        &mut self,
        block: &NativeBlock,
        team: &WorkerTeam,
        out: &mut [Complex32],
        noise: DeviceNoise<'_>,
    ) -> Result<(), PassError> {
        assert!(self.adds_noise_on_device(), "noise is added on the device");
        assert_eq!(noise.rows.len(), block.len(), "one noise row per block row");
        assert_eq!(
            out.len(),
            block.len() * block.channels() * block.correlations(),
            "one prediction per selected sample"
        );
        self.prediction
            .predict_into(block, team, &self.domains, out, Some(noise))
    }
}

/// Wait for the Metal predictors of `chunks`, reporting the first failure.
fn complete(chunks: &mut [PredictionChunk<'_>]) -> Result<(), PassError> {
    for predictor in chunks.iter_mut().flat_map(|chunk| &mut chunk.predictors) {
        if let Predictor::Metal(backend) = predictor {
            backend.complete()?;
        }
    }
    Ok(())
}

/// The model at native cells of every row of `block` into `cells`, in
/// slices of `chunks.len()` chunks of at most `rows` rows each: domain by
/// domain, each chunk places its rows' samples in one batch with their
/// native terms and hands it to its predictor; the first domain writes the
/// cells and later ones add to them. A Metal predictor leaves the work on
/// the device, which runs it before any later device work that reads the
/// cells.
fn predict_rows(
    chunks: &mut [PredictionChunk<'_>],
    rows: usize,
    block: &NativeBlock,
    team: &WorkerTeam,
    domains: &[PredictionDomain<'_>],
    ordered: bool,
    mut cells: Cells<'_>,
) -> Result<(), PassError> {
    let count = chunks.len();
    let width = block.channels() * block.correlations();
    for slice in slices(block.len(), count, rows) {
        let mut used = 0;
        for (chunk, rows) in chunks.iter_mut().zip(chunk_ranges(slice.clone(), count)) {
            chunk.rows = rows;
            used += 1;
        }
        let mut pieces = Vec::with_capacity(used);
        match &mut cells {
            Cells::Host(all) => {
                let mut remaining = &mut all[slice.start * width..slice.end * width];
                for chunk in &mut chunks[..used] {
                    let (head, tail) = remaining.split_at_mut(chunk.rows.len() * width);
                    remaining = tail;
                    pieces.push((&mut *chunk, ChunkCells::Host(head)));
                }
            }
            Cells::Device(device) => {
                for chunk in &mut chunks[..used] {
                    pieces.push((&mut *chunk, ChunkCells::Device(device)));
                }
            }
        }
        for (index, domain) in domains.iter().enumerate() {
            let add = index > 0;
            team.for_each_mut(&mut pieces, |_, (chunk, cells)| {
                chunk.predict(index, domain, block, ordered, cells, add)
            })?;
        }
    }
    Ok(())
}

impl PredictionChunk<'_> {
    /// Predict the chunk's rows of `block` on domain `index`, writing their
    /// model at native cells into `cells`, or with `add` adding to it.
    fn predict(
        &mut self,
        index: usize,
        domain: &PredictionDomain<'_>,
        block: &NativeBlock,
        ordered: bool,
        cells: &mut ChunkCells<'_>,
        add: bool,
    ) -> Result<(), PassError> {
        let channels = block.channels();
        let npol = domain.operator.polarization().correlations().len();
        let Self {
            rows,
            scratch,
            predictors,
        } = self;
        scratch.begin(npol);
        for position in rows.clone() {
            let row = block.row(index, row_at(block, ordered, position));
            let placed =
                domain
                    .resampler
                    .place_prediction(domain.operator, domain.model, &row, scratch)?;
            domain
                .resampler
                .native_terms(&row, npol, placed, scratch, cell_of(position, channels));
        }
        let cf = domain.operator.cf();
        match (&mut predictors[index], cells) {
            (Predictor::Cpu(backend), ChunkCells::Host(cells)) => {
                let first = cell_of(rows.start, channels);
                let target = NativeTarget::Host { cells, first };
                scratch.apply_native(backend, cf, domain.model, target, add)?;
            }
            (Predictor::Metal(backend), ChunkCells::Device(cells)) => {
                let backend: &mut MetalBackend<'_> = backend;
                let target = NativeTarget::Device(cells);
                scratch.apply_native(backend, cf, domain.model, target, add)?;
            }
            _ => unreachable!("a block prediction's predictors and store share its backend"),
        }
        Ok(())
    }
}
