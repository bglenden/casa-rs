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
        let mut diagonal = normal[row][row];
        for (prior, prior_row) in normal.iter().enumerate().take(row) {
            diagonal -= prior_row[row] * prior_row[row] / prior_row[prior];
        }
        if !diagonal.is_finite() || diagonal * diagonal / normal[row][row] <= 1.0e-12 {
            return None;
        }
        normal[row][row] = diagonal;
        for column in row + 1..N {
            let mut value = normal[row][column];
            for (prior, prior_row) in normal.iter().enumerate().take(row) {
                value -= prior_row[row] * prior_row[column] / prior_row[prior];
            }
            normal[row][column] = value;
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
        for column in row + 1..N {
            solution[row] -= normal[row][column] * solution[column];
        }
        solution[row] /= normal[row][row];
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
