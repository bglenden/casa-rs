// SPDX-License-Identifier: LGPL-3.0-or-later
//! MAIN's rows at `close`: each write batch's geometry, prediction and noise
//! computed on the session's worker team, then handed to the writer's
//! thread.
//!
//! Per batch of the write plan's rows (whole bands of DATA tiles):
//! 1. **Geometry.** The integrations the batch reaches beyond the rows the
//!    last batch left are computed, one job per integration:
//!    [`ArrayGeometry::integration_rows`] depends only on the scan and the
//!    time.
//! 2. **Prediction.** DATA is zeroed in the writer's recycled batch, and
//!    the rows of scans a `predict` covers are predicted into it, in blocks
//!    on the team ([`super::prediction`]).
//! 3. **Cells and columns.** Then, on the team at once:
//!    - each scan's noise is added to its rows, a run of rows per job: a
//!      draw depends only on the term, the MAIN row, the channel and the
//!      correlation ([`crate::corrupt`]);
//!    - one job copies FLAG_ROW, UVW, WEIGHT and SIGMA from the geometry and
//!      pushes the scalar columns of the integrations computed for the
//!      batch, in MAIN order.
//! 4. **Send.** The writer's thread writes the batch while the next one is
//!    computed.
//!
//! No result depends on which worker computes what or where batches split,
//! so the MeasurementSet is bitwise the same for any worker count or memory
//! budget. An integration that straddles two batches is computed once: its
//! unwritten rows open the next batch.

use std::time::{Duration, Instant};

use casa_imaging_runtime::pass::WorkerTeam;
use casa_ms::VisibilityColumns;
use casa_types::Complex32;

use super::MainWriter;
use super::geometry::{ArrayGeometry, RowGeometry};
use super::prediction::PredictionStage;
use crate::corrupt::NoiseApplication;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::observe::Scan;

/// Jobs per worker a batch's noise is split into, so that workers finishing
/// early take more.
const NOISE_JOBS_PER_WORKER: usize = 4;

/// The fewest rows of one noise job.
const MINIMUM_NOISE_JOB_ROWS: usize = 16;

/// One integration a batch computes the geometry of.
#[derive(Debug, Clone, Copy)]
pub(super) struct Integration {
    scan: usize,
    time_s: f64,
    /// Its first row in the pipeline's geometry.
    first: usize,
}

/// Where `close`'s time went, for the pass's summary.
#[derive(Debug, Default)]
pub(super) struct StageTimes {
    pub(super) geometry: Duration,
    /// Zeroing DATA.
    pub(super) zero: Duration,
    /// Predicting the model into DATA.
    pub(super) predict: Duration,
    /// Noise, the per-row columns and the scalar columns.
    pub(super) cells: Duration,
    /// Waiting for the writer to take a batch.
    pub(super) send: Duration,
    pub(super) batches: usize,
}

/// The pipeline's state across batches.
pub(super) struct Pipeline<'s> {
    scans: &'s [Scan],
    geometry: &'s ArrayGeometry<'s>,
    team: &'s WorkerTeam,
    /// The model's prediction, when a `predict` covers any scan.
    prediction: Option<PredictionStage<'s>>,
    /// Each scan's noise, in the order its steps add it.
    noise: Vec<Vec<NoiseApplication>>,
    correlations: usize,
    samples: usize,
    /// Rows of the integrations in hand: those the last batch left
    /// unwritten first, then those computed for this one.
    rows: Vec<RowGeometry>,
    /// The scan of the rows the last batch left.
    carried_scan: usize,
    /// The integrations computed for this batch.
    integrations: Vec<Integration>,
    pub(super) times: StageTimes,
}

impl<'s> Pipeline<'s> {
    pub(super) fn new(
        scans: &'s [Scan],
        geometry: &'s ArrayGeometry<'s>,
        team: &'s WorkerTeam,
        (prediction, noise): (Option<PredictionStage<'s>>, Vec<Vec<NoiseApplication>>),
        (correlations, channels): (usize, usize),
    ) -> Self {
        Self {
            scans,
            geometry,
            team,
            prediction,
            noise,
            correlations,
            samples: correlations * channels,
            rows: Vec::new(),
            carried_scan: 0,
            integrations: Vec::new(),
            times: StageTimes::default(),
        }
    }

    /// Write every row of the scans: `next` yields each integration with
    /// rows, as (scan, stored TIME), in MAIN order.
    pub(super) fn write_rows(
        &mut self,
        writer: &mut MainWriter,
        mut next: impl Iterator<Item = (usize, f64)>,
        row_count: usize,
        batch_rows: usize,
    ) -> SimulatorResult<()> {
        let mut first_row = 0;
        while first_row < row_count {
            let rows = batch_rows.min(row_count - first_row);
            let started = Instant::now();
            self.compute_geometry(&mut next, rows)?;
            self.times.geometry += started.elapsed();
            let started = Instant::now();
            let mut batch = writer.visibility_columns();
            batch
                .data
                .resize(rows * self.samples, Complex32::new(0.0, 0.0));
            self.times.zero += started.elapsed();
            let started = Instant::now();
            self.predict(&mut batch.data, first_row as u64, rows)?;
            self.times.predict += started.elapsed();
            let started = Instant::now();
            self.fill(writer, &mut batch, first_row as u64, rows)?;
            self.times.cells += started.elapsed();
            let started = Instant::now();
            writer.send(batch)?;
            self.times.send += started.elapsed();
            self.times.batches += 1;
            self.carry(rows);
            first_row += rows;
        }
        Ok(())
    }

    /// Compute the integrations the next `rows` rows reach beyond those
    /// carried.
    fn compute_geometry(
        &mut self,
        next: &mut impl Iterator<Item = (usize, f64)>,
        rows: usize,
    ) -> SimulatorResult<()> {
        self.integrations.clear();
        let mut covered = self.rows.len();
        while covered < rows {
            let (scan, time_s) = next
                .next()
                .ok_or_else(|| SimulatorError::raised("the scans have fewer rows than planned"))?;
            self.integrations.push(Integration {
                scan,
                time_s,
                first: covered,
            });
            covered += self.scans[scan].baseline_count();
        }
        let carried = self.rows.len();
        self.rows.resize(covered, RowGeometry::default());
        let mut jobs = Vec::with_capacity(self.integrations.len());
        let mut rest = &mut self.rows[carried..];
        for integration in &self.integrations {
            let (rows, after) = rest.split_at_mut(self.scans[integration.scan].baseline_count());
            jobs.push((*integration, rows));
            rest = after;
        }
        let (scans, geometry) = (self.scans, self.geometry);
        self.team.for_each_mut(&mut jobs, |_, (integration, rows)| {
            geometry.integration_rows(&scans[integration.scan], integration.time_s, rows)
        })
    }

    /// Predict the model into the batch's `rows` rows of `data` (zero), the
    /// first of which is MAIN row `first_row`.
    fn predict(
        &mut self,
        data: &mut [Complex32],
        first_row: u64,
        rows: usize,
    ) -> SimulatorResult<()> {
        if self.prediction.is_none() {
            return Ok(());
        }
        let runs = self.segments(rows);
        let Some(stage) = &mut self.prediction else {
            return Ok(());
        };
        stage.predict(self.team, self.scans, &self.rows, &runs, first_row, data)
    }

    /// Fill the batch's `rows` rows, the first of which is MAIN row
    /// `first_row`: the noise of DATA (already holding the model), the
    /// per-row columns, and the scalar columns of the integrations computed
    /// for the batch.
    fn fill(
        &self,
        writer: &mut MainWriter,
        batch: &mut VisibilityColumns,
        first_row: u64,
        rows: usize,
    ) -> SimulatorResult<()> {
        let VisibilityColumns {
            data,
            flag_row,
            uvw,
            weight,
            sigma,
        } = batch;
        let mut jobs = vec![BatchJob::Columns(ColumnsJob {
            writer,
            flag_row,
            uvw,
            weight,
            sigma,
        })];
        self.noise_jobs(first_row, data, rows, &mut jobs);
        let (scans, integrations, geometry) = (self.scans, &self.integrations, &self.rows);
        let (correlations, samples) = (self.correlations, self.samples);
        self.team.for_each_mut(&mut jobs, |_, job| match job {
            BatchJob::Columns(job) => job.run(&geometry[..rows], scans, integrations, geometry),
            BatchJob::Noise(job) => {
                job.run(correlations, samples);
                Ok(())
            }
        })
    }

    /// Split the batch's `rows` rows of `data` into noise jobs, runs of one
    /// scan with noise.
    fn noise_jobs<'a>(
        &'a self,
        first_row: u64,
        data: &'a mut [Complex32],
        rows: usize,
        jobs: &mut Vec<BatchJob<'a>>,
    ) {
        let job_rows = rows
            .div_ceil(self.team.workers() * NOISE_JOBS_PER_WORKER)
            .max(MINIMUM_NOISE_JOB_ROWS);
        let mut rest = data;
        for (scan, start, count) in self.segments(rows) {
            let (segment, after) = rest.split_at_mut(count * self.samples);
            rest = after;
            if self.noise[scan].is_empty() {
                continue;
            }
            for (index, cells) in segment.chunks_mut(job_rows * self.samples).enumerate() {
                let start = start + index * job_rows;
                jobs.push(BatchJob::Noise(NoiseJob {
                    first_row: first_row + start as u64,
                    cells,
                    rows: &self.rows[start..],
                    noise: &self.noise[scan],
                }));
            }
        }
    }

    /// The batch's first `rows` rows as runs of one scan: (scan, first row,
    /// rows).
    fn segments(&self, rows: usize) -> Vec<(usize, usize, usize)> {
        let carried = self
            .integrations
            .first()
            .map_or(self.rows.len(), |integration| integration.first);
        let mut segments: Vec<(usize, usize, usize)> = Vec::new();
        let mut push = |scan: usize, start: usize, end: usize| {
            let end = end.min(rows);
            if start >= end {
                return;
            }
            match segments.last_mut() {
                Some((last, _, count)) if *last == scan => *count += end - start,
                _ => segments.push((scan, start, end - start)),
            }
        };
        push(self.carried_scan, 0, carried);
        for integration in &self.integrations {
            let end = integration.first + self.scans[integration.scan].baseline_count();
            push(integration.scan, integration.first, end);
        }
        segments
    }

    /// Keep the rows after the batch's first `rows` for the next batch.
    fn carry(&mut self, rows: usize) {
        if let Some(last) = self.integrations.last() {
            self.carried_scan = last.scan;
        }
        self.rows.drain(..rows);
    }
}

/// A job of a batch's fill.
enum BatchJob<'a> {
    Columns(ColumnsJob<'a>),
    Noise(NoiseJob<'a>),
}

/// The batch's per-row columns and the scalar columns of its integrations.
struct ColumnsJob<'a> {
    writer: &'a mut MainWriter,
    flag_row: &'a mut Vec<bool>,
    uvw: &'a mut Vec<[f64; 3]>,
    weight: &'a mut Vec<f32>,
    sigma: &'a mut Vec<f32>,
}

impl ColumnsJob<'_> {
    /// Copy the columns of `rows`, the batch's, and push the scalar columns
    /// of `integrations`, whose rows are in `geometry`.
    fn run(
        &mut self,
        rows: &[RowGeometry],
        scans: &[Scan],
        integrations: &[Integration],
        geometry: &[RowGeometry],
    ) -> SimulatorResult<()> {
        for row in rows {
            self.flag_row.push(row.flagged);
            self.uvw.push(row.uvw_m);
            self.weight.push(row.weight);
            self.sigma.push(row.sigma);
        }
        for integration in integrations {
            let scan = &scans[integration.scan];
            let rows = &geometry[integration.first..integration.first + scan.baseline_count()];
            self.writer.push_scalars(scan, integration.time_s, rows)?;
        }
        Ok(())
    }
}

/// One noise job: consecutive rows of one scan.
struct NoiseJob<'a> {
    /// The MAIN row of the first cell.
    first_row: u64,
    cells: &'a mut [Complex32],
    /// The geometry of the rows, from the first.
    rows: &'a [RowGeometry],
    noise: &'a [NoiseApplication],
}

impl NoiseJob<'_> {
    /// Add the noise to every cell of `samples` samples of `correlations`
    /// correlations.
    fn run(&mut self, correlations: usize, samples: usize) {
        for (offset, (cell, row)) in self
            .cells
            .chunks_exact_mut(samples)
            .zip(self.rows)
            .enumerate()
        {
            for application in self.noise {
                application.add_to_row(
                    self.first_row + offset as u64,
                    row.antenna1 == row.antenna2,
                    correlations,
                    cell,
                );
            }
        }
    }
}
