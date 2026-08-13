// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared small-system least-squares helpers.

/// Solve a symmetric normal-equation system with casacore `LSQFit` ordering.
///
/// The input matrix must contain the symmetric system in its upper triangle.
/// This deliberately preserves casacore's LDLᵀ factorization and arithmetic
/// order so algorithms whose serialized results depend on exact rounding can
/// share one implementation.
pub(crate) fn solve_symmetric_ldlt_casacore<const N: usize>(
    mut normal: [[f64; N]; N],
    known: [f64; N],
) -> Option<[f64; N]> {
    for row in 0..N {
        let (prior_rows, current_and_after) = normal.split_at_mut(row);
        let current_row = &mut current_and_after[0];
        let original_diagonal = current_row[row];
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
        let (prior_solutions, current_and_after) = solution.split_at_mut(row);
        let current_solution = &mut current_and_after[0];
        *current_solution = known[row];
        for (prior, prior_solution) in prior_solutions.iter().enumerate() {
            *current_solution -= normal[prior][row] * *prior_solution / normal[prior][prior];
        }
    }
    for row in (0..N).rev() {
        let (_, current_and_after) = solution.split_at_mut(row);
        let (current, later_solutions) = current_and_after.split_at_mut(1);
        for (offset, later_solution) in later_solutions.iter().enumerate() {
            current[0] -= normal[row][row + offset + 1] * *later_solution;
        }
        current[0] /= normal[row][row];
    }
    Some(solution)
}

#[cfg(test)]
mod tests {
    use super::solve_symmetric_ldlt_casacore;

    #[test]
    fn solves_symmetric_positive_definite_system() {
        let normal = [[4.0, 1.0, 2.0], [1.0, 3.0, 0.0], [2.0, 0.0, 5.0]];
        let known = [12.0, 7.0, 17.0];

        let solution =
            solve_symmetric_ldlt_casacore(normal, known).expect("nonsingular system should solve");

        for (actual, expected) in solution.into_iter().zip([1.0, 2.0, 3.0]) {
            assert!((actual - expected).abs() <= 3.0 * f64::EPSILON);
        }
    }

    #[test]
    fn rejects_singular_system() {
        let normal = [[1.0, 1.0], [1.0, 1.0]];

        assert!(solve_symmetric_ldlt_casacore(normal, [1.0, 1.0]).is_none());
    }
}
