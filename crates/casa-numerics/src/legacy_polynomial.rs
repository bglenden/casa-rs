// SPDX-License-Identifier: LGPL-3.0-or-later
//! Evaluation of polynomials stored by legacy CASA calibration tables.

/// Evaluate a BPOLY Chebyshev expansion over the physical frequency interval.
/// CASA stores twice the constant term. An empty expansion represents zero.
#[must_use]
pub fn legacy_bpoly_chebyshev_value(coefficients: &[f64], x_start: f64, x_end: f64, x: f64) -> f64 {
    if coefficients.is_empty() {
        return 0.0;
    }
    if coefficients.len() == 1 || (x_end - x_start).abs() <= f64::EPSILON {
        return 0.5_f64 * coefficients[0];
    }

    let xcap = ((x - x_start) - (x_end - x)) / (x_end - x_start);
    let mut sum = 0.5_f64 * coefficients[0];
    let mut t_prev = 1.0_f64;
    let mut t_curr = xcap;
    sum += coefficients[1] * t_curr;
    for coefficient in coefficients.iter().copied().skip(2) {
        let t_next = 2.0_f64 * xcap * t_curr - t_prev;
        sum += coefficient * t_next;
        t_prev = t_curr;
        t_curr = t_next;
    }
    sum
}

/// Evaluate the clamped cubic B-spline stored by a GSPLINE solution.
/// The first `knots.len() - 4` coefficients carry data. CASA sometimes
/// retains additional padding at the end of each receptor's coefficients.
/// Returns `None` for a malformed interval or a point outside its domain.
#[must_use]
pub fn legacy_cubic_spline(knots: &[f64], coefficients: &[f64], x: f64) -> Option<f64> {
    let count = knots.len().checked_sub(4)?;
    if count < 4 || coefficients.len() < count || x < knots[3] || x > knots[count] || !x.is_finite()
    {
        return None;
    }
    let interval = knots
        .partition_point(|&knot| knot <= x)
        .saturating_sub(1)
        .min(count - 1);
    let start = interval.checked_sub(3)?;
    let mut work: [f64; 4] = coefficients.get(start..=interval)?.try_into().ok()?;
    for depth in 1..=3 {
        for j in (depth..=3).rev() {
            let left = start + j;
            let span = knots[left + 4 - depth] - knots[left];
            if span <= 0.0 {
                return None;
            }
            let weight = (x - knots[left]) / span;
            work[j] = (1.0 - weight) * work[j - 1] + weight * work[j];
        }
    }
    work[3].is_finite().then_some(work[3])
}
