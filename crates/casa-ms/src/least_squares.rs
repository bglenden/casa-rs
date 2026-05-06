// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared least-squares helpers backed by `nalgebra`.

use nalgebra::{DMatrix, DVector};

/// Solve a weighted linear least-squares system with an SVD-backed solver.
///
/// Each row is `(basis_values, observed_value, weight)`. The implementation
/// applies the usual `sqrt(weight)` row scaling before solving `A x = b`.
pub fn solve_weighted_least_squares(
    rows: &[(Vec<f64>, f64, f64)],
    coefficient_count: usize,
) -> Option<Vec<f64>> {
    if coefficient_count == 0 || rows.len() < coefficient_count {
        return None;
    }

    let mut design = Vec::with_capacity(rows.len() * coefficient_count);
    let mut observed = Vec::with_capacity(rows.len());
    for (basis, value, weight) in rows {
        if basis.len() != coefficient_count
            || !value.is_finite()
            || !weight.is_finite()
            || *weight <= 0.0
        {
            return None;
        }
        let scale = weight.sqrt();
        design.extend(basis.iter().map(|entry| entry * scale));
        observed.push(value * scale);
    }

    let design = DMatrix::from_row_slice(rows.len(), coefficient_count, &design);
    let observed = DVector::from_row_slice(&observed);
    let solution = design.svd(true, true).solve(&observed, 1.0e-12).ok()?;
    Some(solution.as_slice().to_vec())
}

#[cfg(test)]
mod tests {
    use super::solve_weighted_least_squares;

    #[test]
    fn weighted_linear_fit_recovers_line() {
        let rows = vec![
            (vec![1.0, 0.0], 2.0, 1.0),
            (vec![1.0, 1.0], 5.0, 1.0),
            (vec![1.0, 2.0], 8.0, 1.0),
            (vec![1.0, 3.0], 11.0, 1.0),
        ];
        let solution = solve_weighted_least_squares(&rows, 2).expect("nonsingular system");
        assert!((solution[0] - 2.0).abs() < 1.0e-10);
        assert!((solution[1] - 3.0).abs() < 1.0e-10);
    }

    #[test]
    fn invalid_rows_are_rejected() {
        assert!(solve_weighted_least_squares(&[], 1).is_none());
        assert!(solve_weighted_least_squares(&[(vec![1.0], 1.0, 0.0)], 1).is_none());
        assert!(solve_weighted_least_squares(&[(vec![1.0], f64::NAN, 1.0)], 1).is_none());
    }
}
