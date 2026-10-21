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

use std::ops::Range;

use casa_imaging_metal::MetalBackend;
use casa_imaging_operator::{
    CpuBackend, DeviceNativeCells, MeasurementOperator, NativeModel, NativeTarget,
    PredictionScratch, PreparedModelGrids, SpectralResampler,
};
use num_complex::Complex32;

use super::block::{cell_of, row_at};
use super::{BackendChoice, NativeBlock, PassError, WorkerTeam, chunk_ranges, slices};

/// One image domain's share of a prediction: its operator, the resampler
/// that maps native rows to its planes, and its prepared model.
#[derive(Clone, Copy)]
pub struct PredictionDomain<'a> {
    /// The measurement operator.
    pub operator: &'a MeasurementOperator,
    /// Maps native rows to the operator's planes.
    pub resampler: &'a SpectralResampler,
    /// The model grids of every plane the rows reach.
    pub model: &'a PreparedModelGrids,
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
    pub(super) fn new(
        domains: impl Iterator<Item = (&'w MeasurementOperator, &'w SpectralResampler)> + Clone,
        backend: BackendChoice,
        stage_timing: bool,
        shape: PredictionShape,
        cells: usize,
    ) -> Result<Self, PassError> {
        let chunks = (0..shape.chunks)
            .map(|_| {
                Ok(PredictionChunk {
                    rows: 0..0,
                    scratch: SpectralResampler::prediction_scratch(
                        domains.clone().map(|(_, resampler)| resampler),
                        shape.npol,
                        shape.channels,
                        shape.rows,
                    ),
                    predictors: domains
                        .clone()
                        .map(|(operator, _)| {
                            Ok(match backend {
                                BackendChoice::Cpu => Predictor::Cpu(CpuBackend::new()),
                                BackendChoice::Metal => Predictor::Metal(Box::new(
                                    MetalBackend::new(operator.cf(), stage_timing)?,
                                )),
                            })
                        })
                        .collect::<Result<_, PassError>>()?,
                })
            })
            .collect::<Result<_, PassError>>()?;
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
