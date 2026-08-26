// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Shared numerical algorithms without astronomy-domain ownership.

use nalgebra::{DMatrix, DVector};

/// Solve a weighted linear least-squares system with an SVD-backed solver.
///
/// Each row is `(basis_values, observed_value, weight)`. The implementation
/// applies the usual `sqrt(weight)` row scaling before solving `A x = b`.
#[must_use]
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

/// Solve a symmetric system with casacore `LSQFit` ordering.
///
/// The input matrix must contain the symmetric system in its upper triangle.
/// This preserves casacore's LDLT factorization and arithmetic order for
/// algorithms whose serialized results depend on exact rounding.
#[must_use]
pub fn solve_symmetric_ldlt_casacore<const N: usize>(
    mut normal: [[f64; N]; N],
    known: [f64; N],
) -> Option<[f64; N]> {
    for row in 0..N {
        let (prior_rows, current_and_after) = normal.split_at_mut(row);
        let current_row = &mut current_and_after[0];
        let original_diagonal = current_row[row];
        if original_diagonal == 0.0 {
            return None;
        }
        let mut diagonal = original_diagonal;
        for (prior, prior_row) in prior_rows.iter().enumerate() {
            diagonal -= prior_row[row] * prior_row[row] / prior_row[prior];
        }
        if !diagonal.is_finite() || diagonal * diagonal / original_diagonal <= 1.0e-12 {
            return None;
        }
        current_row[row] = diagonal;
        for column in row + 1..N {
            for (prior, prior_row) in prior_rows.iter().enumerate() {
                current_row[column] -= prior_row[row] * prior_row[column] / prior_row[prior];
            }
        }
    }

    let mut solution = [0.0; N];
    for row in 0..N {
        solution[row] = known[row];
        for prior in 0..row {
            solution[row] -= normal[prior][row] * solution[prior] / normal[prior][prior];
        }
    }
    for row in (0..N).rev() {
        for later in row + 1..N {
            solution[row] -= normal[row][later] * solution[later];
        }
        solution[row] /= normal[row][row];
    }
    Some(solution)
}

#[cfg(test)]
mod tests {
    use super::{solve_symmetric_ldlt_casacore, solve_weighted_least_squares};

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
    fn invalid_weighted_rows_are_rejected() {
        assert!(solve_weighted_least_squares(&[], 1).is_none());
        assert!(solve_weighted_least_squares(&[(vec![1.0], 1.0, 0.0)], 1).is_none());
        assert!(solve_weighted_least_squares(&[(vec![1.0], f64::NAN, 1.0)], 1).is_none());
    }

    #[test]
    fn casacore_ldlt_solves_symmetric_system() {
        let normal = [[4.0, 1.0], [0.0, 3.0]];
        let solution =
            solve_symmetric_ldlt_casacore(normal, [9.0, 7.0]).expect("positive-definite system");
        assert!((solution[0] - 20.0 / 11.0).abs() < 1.0e-12);
        assert!((solution[1] - 19.0 / 11.0).abs() < 1.0e-12);
    }

    #[test]
    fn casacore_ldlt_rejects_singular_system() {
        assert!(solve_symmetric_ldlt_casacore([[0.0]], [1.0]).is_none());
    }
}
