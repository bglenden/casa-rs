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
use crate::session::plan::DataPlan;

/// Rows of one job.
const JOB_ROWS: usize = 256;

struct Job<'a> {
    /// The scan's index in the session.
    index: usize,
    scan: &'a Scan,
    rows: &'a [RowGeometry],
    cells: &'a mut [Complex32],
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
    let mut jobs = Vec::new();
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
            if plan.components_for_scan(scan).next().is_some() {
                jobs.push(Job {
                    index: scan,
                    scan: &scans[scan],
                    rows: &rows[start + offset..start + offset + take],
                    cells,
                });
            }
            offset += take;
        }
    }
    team.for_each_mut(&mut jobs, |_, job| {
        for components in plan.components_for_scan(job.index) {
            components.add_rows(
                (job.index, job.scan.spectral_window_id),
                job.rows.iter().map(|row| row.uvw_m),
                job.cells,
            );
        }
        Ok::<_, SimulatorError>(())
    })
}
