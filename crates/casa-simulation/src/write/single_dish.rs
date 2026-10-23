// SPDX-License-Identifier: LGPL-3.0-or-later
//! The total-power predictions in a write batch: after the model images
//! are degridded into DATA, each `predict` after `setoptions(ftmachine='sd')`
//! covering a row's scan adds its pointing's samples to the row, if it is an
//! unflagged autocorrelation ([`crate::predict::SingleDishPrediction`]).

use num_complex::Complex32;

use super::geometry::RowGeometry;
use crate::session::observe::Scan;
use crate::session::plan::DataPlan;

/// Add the total-power predictions `plan` holds to `data`, the batch's DATA
/// of `rows` (`samples` cells a row), for each run of `runs` (scan, first
/// row, rows).
pub(super) fn add_single_dish(
    (scans, rows): (&[Scan], &[RowGeometry]),
    plan: &DataPlan,
    runs: &[(usize, usize, usize)],
    (data, samples): (&mut [Complex32], usize),
) {
    for &(scan, start, count) in runs {
        let window = scans[scan].spectral_window_id;
        for prediction in plan.single_dish_for_scan(scan) {
            let values = prediction.samples(scan, window);
            for row in start..start + count {
                let geometry = &rows[row];
                // `SDGrid::get` predicts no row it flags, nor (here) a
                // cross-correlation.
                if geometry.antenna1 != geometry.antenna2 || geometry.flagged {
                    continue;
                }
                for (cell, value) in data[row * samples..(row + 1) * samples]
                    .iter_mut()
                    .zip(values)
                {
                    *cell += *value;
                }
            }
        }
    }
}
