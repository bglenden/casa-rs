// SPDX-License-Identifier: LGPL-3.0-or-later
//! The prediction stage of `close`: the model DATA of each write batch's
//! rows, degridded by the imaging runtime's block predictor on the worker
//! team.
//!
//! A scan's rows are predicted from every `predict` step covering it, their
//! predictions summed (one [`PredictionDomain`] per step, as CASA's mappers
//! sum their degrids), each row rotated to each step's tangent point by
//! [`GridFtProjection`] as CASA's `GridFT` rotates it. Scans with the same
//! steps and field share a segment, and one predictor serves a segment; it
//! is rebuilt when the batches reach a scan of another segment. A run of a
//! segment's rows is predicted in blocks, straight into the batch's DATA,
//! which a scan no step covers leaves zero: a block is the whole write
//! batch when the budget allows, else about [`SMALL_BLOCK_BYTES`] of native
//! rows.
//!
//! On Metal the stage also corrupts the rows it predicts: the device adds
//! their scans' noise to the model in its memory, with the draws of
//! `casa_numerics::cell_noise`, before the model is copied into DATA, so
//! the host makes no pass of its own over those cells (#855).
//!
//! The rows are as `observe` writes them: the row flag is FLAG_ROW, every
//! sample is unflagged with unit weight, and an autocorrelation predicts the
//! model's zero spacing (`GridFT`'s `usezero`). On the CPU a row's
//! prediction depends only on the row and the steps, not on the workers,
//! the blocks or the batches.

use std::collections::HashMap;
use std::convert::Infallible;

use casa_imaging_operator::{RowContext, RowSpectrum};
use casa_imaging_runtime::pass::{
    BackendChoice, BlockPredictor, BlockShape, DeviceNoise, DomainProjection, MetalNoiseRow,
    MetalNoiseTerm, NativeBlock, NativeRowHeader, PredictionDomain, RowAddress, WorkerTeam,
};
use casa_ms::derived::GridFtProjection;
use casa_types::Complex32;

use super::geometry::RowGeometry;
use crate::corrupt::NoiseApplication;
use crate::session::Simulator;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::observe::Scan;

/// Bytes of the native rows of one prediction block, about, when the budget
/// cannot hold a whole write batch's ([`super::resources`]). A block ends
/// with the device's work drained and its predictions copied out, so on
/// Metal a batch predicted as one block ran 1.5× faster than in blocks of
/// this size (#855: 1,092-row batches of 4,096 channels, about 120 rows a
/// small block).
const SMALL_BLOCK_BYTES: u64 = 16 << 20;

/// The Metal backend's stage-timing target: with it enabled at `INFO`, each
/// Metal predictor logs its dispatches, the host's preparation and the
/// device's time when it drops.
const METAL_STAGE_TIMING: &str = "casa_imaging::metal_stage_timing";

/// The scans with the same predict steps and field: the domains a
/// predictor sums and each row's rotation to each domain's tangent point.
struct Segment<'s> {
    domains: Vec<PredictionDomain<'s>>,
    projections: Vec<GridFtProjection>,
}

/// The prediction stage's state across batches.
pub(super) struct PredictionStage<'s> {
    backend: BackendChoice,
    correlations: usize,
    channels: usize,
    segments: Vec<Segment<'s>>,
    /// Each scan's segment; `None` for a scan no step covers.
    scan_segments: Vec<Option<usize>>,
    /// Each spectral window's channel frequencies.
    frequencies: Vec<Vec<f64>>,
    /// Bytes of every model grid the steps hold, which a device copies.
    grid_bytes: u64,
    /// Rows of a block, once started.
    block_rows: usize,
    workers: usize,
    block: NativeBlock,
    /// Time spent filling blocks of native rows.
    fill_time: std::time::Duration,
    /// A run's noise terms and its block's rows, as the device reads them
    /// when the stage [`Self::noises_on_device`].
    noise_terms: Vec<MetalNoiseTerm>,
    noise_rows: Vec<MetalNoiseRow>,
    projections: Vec<DomainProjection>,
    /// The predictor of the segment last predicted.
    predictor: Option<(usize, BlockPredictor<'s>)>,
    channel_indices: Vec<u32>,
    correlation_indices: Vec<u32>,
}

impl<'s> PredictionStage<'s> {
    /// The stage for `session`'s predict steps on cells of `(correlations,
    /// channels)`, on `backend`; `None` when no step predicts.
    ///
    /// # Errors
    ///
    /// A covered scan's field without grids, which `predict` prepares for
    /// every field it covers.
    pub(super) fn new(
        session: &'s Simulator,
        (correlations, channels): (usize, usize),
        backend: BackendChoice,
    ) -> SimulatorResult<Option<Self>> {
        let plan = session.data_plan();
        let predictions: Vec<_> = plan
            .predictions()
            .map(|(prediction, _)| prediction)
            .collect();
        if predictions.is_empty() {
            return Ok(None);
        }
        let mut keys = HashMap::new();
        let mut segments = Vec::new();
        let mut scan_segments = Vec::with_capacity(session.scans().len());
        for (index, scan) in session.scans().iter().enumerate() {
            let steps = plan.predictions_for_scan(index);
            if steps.is_empty() {
                scan_segments.push(None);
                continue;
            }
            let key = (steps, scan.field_id);
            let segment = match keys.get(&key) {
                Some(segment) => *segment,
                None => {
                    let mut domains = Vec::with_capacity(key.0.len());
                    let mut projections = Vec::with_capacity(key.0.len());
                    for step in key.0.clone() {
                        let prediction = predictions[step];
                        let model = prediction.grids_for(scan.field_id).ok_or_else(|| {
                            SimulatorError::raised(format!(
                                "predict prepared no grids for field {}",
                                scan.field_id
                            ))
                        })?;
                        domains.push(PredictionDomain {
                            operator: &prediction.model.operator,
                            resampler: &prediction.model.resampler,
                            model,
                        });
                        projections.push(GridFtProjection::between_j2000_directions(
                            scan.phase_center_rad,
                            prediction.tangent_rad,
                        ));
                    }
                    segments.push(Segment {
                        domains,
                        projections,
                    });
                    keys.insert(key, segments.len() - 1);
                    segments.len() - 1
                }
            };
            scan_segments.push(Some(segment));
        }
        let frequencies = session
            .spectral_windows()
            .iter()
            .map(|window| window.channel_frequencies_hz())
            .collect();
        let grid_bytes = predictions
            .iter()
            .map(|prediction| prediction.grid_bytes())
            .sum();
        let noise_terms = (0..session.scans().len())
            .map(|scan| plan.noise_for_scan(scan).len())
            .max()
            .unwrap_or(0);
        Ok(Some(Self {
            backend,
            correlations,
            channels,
            segments,
            scan_segments,
            frequencies,
            grid_bytes,
            block_rows: 0,
            workers: 0,
            block: NativeBlock::default(),
            fill_time: std::time::Duration::ZERO,
            noise_terms: Vec::with_capacity(noise_terms),
            noise_rows: Vec::new(),
            projections: Vec::new(),
            predictor: None,
            channel_indices: (0..channels as u32).collect(),
            correlation_indices: (0..correlations as u32).collect(),
        }))
    }

    /// The backend the stage predicts on.
    pub(super) fn backend(&self) -> BackendChoice {
        self.backend
    }

    /// Time spent filling blocks of native rows, which the producer does
    /// alone before the team predicts them.
    pub(super) fn fill_time(&self) -> std::time::Duration {
        self.fill_time
    }

    /// Whether a step predicts scan `scan`'s rows.
    pub(super) fn predicts(&self, scan: usize) -> bool {
        self.scan_segments[scan].is_some()
    }

    /// The most domains a segment sums.
    fn largest_domains(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| segment.domains.len())
            .max()
            .unwrap_or(0)
    }

    /// The block of `rows` rows projected on `domains` domains.
    fn block_shape(&self, rows: usize, domains: usize) -> BlockShape {
        BlockShape {
            rows,
            spectrum: RowSpectrum::evenly_spaced(self.channels),
            correlations: self.correlations,
            domains,
        }
    }

    /// Rows of a small block for write batches of `batch_rows` rows: as
    /// many as [`SMALL_BLOCK_BYTES`] holds, at most a batch and at least
    /// one.
    pub(super) fn small_block_rows(&self, batch_rows: usize) -> usize {
        let row = self.block_shape(1, self.largest_domains()).bytes().max(1);
        usize::try_from(SMALL_BLOCK_BYTES / row)
            .unwrap_or(usize::MAX)
            .clamp(1, batch_rows.max(1))
    }

    /// Bytes the stage holds while `close` predicts blocks of `rows` rows
    /// with `workers` workers: the predictor of the segment that needs most
    /// ([`BlockPredictor::bytes`]), one block of native rows and each row's
    /// projections, and on Metal the device copy of every model grid
    /// ([`casa_imaging_operator::PreparedModelGrids::device_copy`]) and a
    /// block's noise rows and the most noise terms of a scan, here and in
    /// the predictor ([`BlockPredictor::noise_bytes`]).
    pub(super) fn bytes(&self, rows: usize, workers: usize) -> u64 {
        let predictor = self
            .segments
            .iter()
            .map(|segment| {
                BlockPredictor::bytes(
                    &segment.domains,
                    self.backend,
                    self.block_shape(rows, segment.domains.len()),
                    workers,
                )
            })
            .max()
            .unwrap_or(0);
        let domains = self.largest_domains();
        let shape = self.block_shape(rows, domains);
        let projections = (domains * size_of::<DomainProjection>()) as u64;
        let terms = self.noise_terms.capacity();
        let device = match self.backend {
            BackendChoice::Cpu => 0,
            BackendChoice::Metal => {
                // The noise's rows and terms here, and the predictor's copy.
                let noise = rows * size_of::<MetalNoiseRow>() + terms * size_of::<MetalNoiseTerm>();
                self.grid_bytes
                    + noise as u64
                    + BlockPredictor::noise_bytes(self.backend, shape, terms)
            }
        };
        predictor + shape.bytes() + projections + device
    }

    /// Allocate the block of `rows` rows, predicted with `workers` workers,
    /// as [`Self::bytes`] charged it.
    pub(super) fn start(&mut self, rows: usize, workers: usize) {
        self.block_rows = rows.max(1);
        self.workers = workers.max(1);
        let domains = self.largest_domains();
        self.block = NativeBlock::with_capacity(self.block_shape(self.block_rows, domains));
        self.projections = Vec::with_capacity(domains);
        if self.noises_on_device() {
            self.noise_rows = Vec::with_capacity(self.block_rows);
        }
    }

    /// Whether the stage adds a predicted row's noise itself, on the
    /// device, as it predicts the row ([`Self::predict`]): on Metal, where
    /// the model is formed in device memory.
    pub(super) fn noises_on_device(&self) -> bool {
        self.backend == BackendChoice::Metal
    }

    /// Predict each run of `runs`, (scan, first row, rows) of `rows`, into
    /// `data` (`correlations × channels` samples a row, the rows of `rows`
    /// from the first); the batch's first row is MAIN row `first_row`. Runs
    /// of scans no step covers are left as they are. When the stage
    /// [`Self::noises_on_device`], a predicted run also gets its scan's
    /// noise, `noise(scan)`, on the device before it is copied out.
    pub(super) fn predict<'n>(
        &mut self,
        team: &WorkerTeam,
        (scans, rows): (&[Scan], &[RowGeometry]),
        runs: &[(usize, usize, usize)],
        first_row: u64,
        data: &mut [Complex32],
        noise: impl Fn(usize) -> &'n [NoiseApplication],
    ) -> SimulatorResult<()> {
        let samples = self.correlations * self.channels;
        for &(scan, start, count) in runs {
            let Some(segment) = self.scan_segments[scan] else {
                continue;
            };
            self.select(segment)?;
            self.noise_terms.clear();
            if self.noises_on_device() {
                self.noise_terms
                    .extend(noise(scan).iter().map(|application| {
                        let noise = application.noise();
                        MetalNoiseTerm {
                            key: noise.key(),
                            sigma: noise.sigma(),
                        }
                    }));
            }
            let scan = &scans[scan];
            let end = start + count;
            let mut block_start = start;
            while block_start < end {
                let block_end = (block_start + self.block_rows).min(end);
                let started = std::time::Instant::now();
                self.fill_block(
                    segment,
                    scan,
                    &rows[block_start..block_end],
                    first_row + block_start as u64,
                );
                self.fill_time += started.elapsed();
                let Some((_, predictor)) = &mut self.predictor else {
                    unreachable!("selected above");
                };
                let out = &mut data[block_start * samples..block_end * samples];
                let predicted = if self.noise_terms.is_empty() {
                    predictor.predict_into(&self.block, team, out)
                } else {
                    self.noise_rows.clear();
                    self.noise_rows.extend(
                        rows[block_start..block_end]
                            .iter()
                            .zip(first_row + block_start as u64..)
                            .map(|(row, number)| MetalNoiseRow {
                                row: [number as u32, (number >> 32) as u32],
                                autocorrelation: u32::from(row.antenna1 == row.antenna2),
                            }),
                    );
                    let noise = DeviceNoise {
                        rows: &self.noise_rows,
                        terms: &self.noise_terms,
                    };
                    predictor.predict_into_with_noise(&self.block, team, out, noise)
                };
                predicted.map_err(|error| SimulatorError::raised(format!("predict: {error}")))?;
                block_start = block_end;
            }
        }
        Ok(())
    }

    /// Make the predictor `segment`'s, unless it is.
    fn select(&mut self, segment: usize) -> SimulatorResult<()> {
        if self
            .predictor
            .as_ref()
            .is_some_and(|(current, _)| *current == segment)
        {
            return Ok(());
        }
        // The previous predictor is dropped first, so at most one is held.
        self.predictor = None;
        let domains = &self.segments[segment].domains;
        let predictor = BlockPredictor::new(
            domains,
            self.backend,
            self.block_shape(self.block_rows, domains.len()),
            self.workers,
            tracing::enabled!(target: METAL_STAGE_TIMING, tracing::Level::INFO),
        )
        .map_err(|error| SimulatorError::raised(format!("the predictor: {error}")))?;
        self.predictor = Some((segment, predictor));
        Ok(())
    }

    /// Fill the block with `rows` of `scan`, the first MAIN row
    /// `first_row`, projected on `segment`'s domains.
    fn fill_block(&mut self, segment: usize, scan: &Scan, rows: &[RowGeometry], first_row: u64) {
        let segment = &self.segments[segment];
        let frequencies = &self.frequencies[scan.spectral_window_id];
        self.block.reset(
            segment.domains.len(),
            &self.channel_indices,
            &self.correlation_indices,
        );
        for (offset, row) in rows.iter().enumerate() {
            self.projections.clear();
            self.projections
                .extend(segment.projections.iter().map(|projection| {
                    let (uvw_m, phase_shift_m) = projection.apply(row.uvw_m);
                    DomainProjection {
                        uvw_m,
                        phase_shift_m,
                        pointing_offset_rad: [0.0; 2],
                    }
                }));
            let header = NativeRowHeader {
                row_flag: row.flagged,
                context: RowContext {
                    original_uvw_m: Some(row.uvw_m),
                    time_s: 0.0,
                    antennas: [row.antenna1 as u32, row.antenna2 as u32],
                    antenna_types: [0, 0],
                    parallactic_angle_rad: [0.0, 0.0],
                    field: scan.field_id as u32,
                    spectral_window: scan.spectral_window_id as u32,
                },
                address: RowAddress {
                    physical_row: first_row + offset as u64,
                    data_description: scan.spectral_window_id as i32,
                },
            };
            let pushed = self.block.push_row_with(
                header,
                &self.projections,
                frequencies,
                |_, weights, _| {
                    weights.fill(1.0);
                    Ok::<_, Infallible>(())
                },
            );
            match pushed {
                Ok(()) => {}
                Err(never) => match never {},
            }
        }
    }
}
