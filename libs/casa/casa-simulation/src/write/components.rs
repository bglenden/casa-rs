// SPDX-License-Identifier: LGPL-3.0-or-later
//! The component lists' visibilities in a write batch: after the model
//! images are predicted into DATA, each `predict(complist=…)` covering a
//! row's scan adds its components' visibilities to the row
//! ([`crate::predict::ComponentPrediction::add_rows`]), on the CPU whatever
//! the backend, a run of rows per job on the team. The sum is the same for
//! any worker count: each row is formed by one job, in the plan's order.

use casa_imaging_runtime::pass::WorkerTeam;
use num_complex::Complex32;

use super::geometry::RowGeometry;
use crate::session::error::{SimulatorError, SimulatorResult};
use crate::session::observe::Scan;
use crate::session::plan::DataPhase as DataPlan;

/// Rows of one job.
pub(super) const JOB_ROWS: usize = 256;

/// Bytes of one job.
pub(super) const JOB_BYTES: usize = size_of::<Job<'static>>() + size_of::<&mut [Job<'static>]>();

struct Job<'a> {
    /// The scan's index in the session.
    index: usize,
    scan: &'a Scan,
    rows: &'a [RowGeometry],
    cells: &'a mut [Complex32],
    beam: Option<crate::predict::BeamContext>,
}

/// Add the component lists `plan` predicts to `data`, the batch's DATA of
/// `rows` (`samples` cells a row), for each run of `runs` (scan, first row,
/// rows).
pub(super) fn add_components(
    team: &WorkerTeam,
    (scans, rows): (&[Scan], &[RowGeometry]),
    plan: &DataPlan,
    runs: &[(usize, usize, usize)],
    (data, samples): (&mut [Complex32], usize),
) -> SimulatorResult<()> {
    let predicted = |scan| plan.components_for_scan(scan).next().is_some();
    // As many jobs as there are, at most a run's plus one every `JOB_ROWS`
    // rows (the patch's admission).
    let mut jobs = Vec::with_capacity(
        runs.iter()
            .filter(|&&(scan, _, _)| predicted(scan))
            .map(|&(_, _, count)| count.div_ceil(JOB_ROWS))
            .sum(),
    );
    let mut rest = data;
    let mut position = 0;
    for &(scan, start, count) in runs {
        let (_, tail) = std::mem::take(&mut rest).split_at_mut((start - position) * samples);
        rest = tail;
        position = start;
        let mut offset = 0;
        while offset < count {
            let take = JOB_ROWS.min(count - offset);
            let (cells, tail) = std::mem::take(&mut rest).split_at_mut(take * samples);
            rest = tail;
            position += take;
            if predicted(scan) {
                jobs.push(Job {
                    index: scan,
                    scan: &scans[scan],
                    rows: &rows[start + offset..start + offset + take],
                    cells,
                    beam: None,
                });
            }
            offset += take;
        }
    }
    if !plan
        .components_for_scan(0)
        .any(|components| components.has_squint())
    {
        return team.for_each_mut(&mut jobs, |worker, job| {
            for components in plan.components_for_scan(job.index) {
                components
                    .add_rows(
                        (job.index, job.scan.data_description_id),
                        job.rows.iter().map(|row| (row.uvw_m, row.time_s)),
                        job.cells,
                        None,
                        worker,
                    )
                    .map_err(|error| {
                        SimulatorError::raised(format!("component prediction: {error}"))
                    })?;
            }
            Ok::<_, SimulatorError>(())
        });
    }
    // Every component prediction covers a prefix of scans. Visit sources in
    // their original order, freezing each job's beam-angle history in row
    // order before workers evaluate independent numerical cells.
    for components in plan.components_for_scan(0) {
        for job in &mut jobs {
            if components.covers_scan(job.index) {
                job.beam = components
                    .prepare_beam_job(
                        job.index,
                        job.scan.field_id,
                        job.rows.iter().map(|row| row.time_s),
                    )
                    .map_err(|error| SimulatorError::raised(format!("component beam: {error}")))?;
            }
        }
        let chunk_rows = jobs.len().div_ceil(team.workers()).max(1);
        let mut chunks: Vec<_> = jobs.chunks_mut(chunk_rows).collect();
        team.for_each_mut(&mut chunks, |slot, chunk| {
            for job in chunk.iter_mut() {
                if !components.covers_scan(job.index) {
                    continue;
                }
                components
                    .add_rows(
                        (job.index, job.scan.data_description_id),
                        job.rows.iter().map(|row| (row.uvw_m, row.time_s)),
                        job.cells,
                        job.beam,
                        slot,
                    )
                    .map_err(|error| {
                        SimulatorError::raised(format!("component prediction: {error}"))
                    })?;
            }
            Ok::<_, SimulatorError>(())
        })?;
    }
    Ok(())
}
