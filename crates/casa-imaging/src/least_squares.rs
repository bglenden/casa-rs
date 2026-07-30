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

/// Invert a symmetric positive-definite matrix in casacore
/// `invertSymPosDef` arithmetic order.
///
/// Casacore stores the diagonal separately, writes the lower Cholesky factor
/// into a copy of the input, and solves one identity column at a time. The
/// descending inner loops are intentional because the MT-MFS model can depend
/// on the final `f64` rounding before the inverse is cast to `f32`.
pub(crate) fn invert_symmetric_positive_definite_casacore(
    input: &[Vec<f64>],
) -> Option<Vec<Vec<f64>>> {
    let n = input.len();
    if n == 0
        || input
            .iter()
            .any(|row| row.len() != n || row.iter().any(|value| !value.is_finite()))
    {
        return None;
    }

    let mut factor = input.to_vec();
    let mut diagonal = vec![0.0f64; n];
    for row in 0..n {
        for column in row..n {
            let mut sum = factor[row][column];
            for prior in (0..row).rev() {
                sum -= factor[row][prior] * factor[column][prior];
            }
            if row == column {
                if sum <= 0.0 {
                    return None;
                }
                diagonal[row] = sum.sqrt();
            } else {
                factor[column][row] = sum / diagonal[row];
            }
        }
    }

    let mut inverse = vec![vec![0.0f64; n]; n];
    let mut solution = vec![0.0f64; n];
    for column in 0..n {
        solution.fill(0.0);
        solution[column] = 1.0;

        for row in 0..n {
            let mut sum = solution[row];
            for prior in (0..row).rev() {
                sum -= factor[row][prior] * solution[prior];
            }
            solution[row] = sum / diagonal[row];
        }
        for row in (0..n).rev() {
            let mut sum = solution[row];
            for following in row + 1..n {
                sum -= factor[following][row] * solution[following];
            }
            solution[row] = sum / diagonal[row];
        }
        for row in 0..n {
            inverse[row][column] = solution[row];
        }
    }
    Some(inverse)
}

#[cfg(test)]
mod tests {
    use super::{invert_symmetric_positive_definite_casacore, solve_symmetric_ldlt_casacore};

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

    #[test]
    fn inverts_symmetric_positive_definite_matrix_in_casacore_order() {
        let matrix = vec![vec![4.0, 1.0], vec![1.0, 3.0]];

        let inverse =
            invert_symmetric_positive_definite_casacore(&matrix).expect("matrix should invert");

        let expected = [[3.0 / 11.0, -1.0 / 11.0], [-1.0 / 11.0, 4.0 / 11.0]];
        for row in 0..2 {
            for column in 0..2 {
                assert!((inverse[row][column] - expected[row][column]).abs() <= 2.0 * f64::EPSILON);
            }
        }
    }

    #[test]
    fn casacore_inverse_rejects_non_positive_definite_matrix() {
        let matrix = vec![vec![1.0, 1.0], vec![1.0, 1.0]];

        assert!(invert_symmetric_positive_definite_casacore(&matrix).is_none());
    }

    #[test]
    fn casacore_inverse_matches_frozen_casa_67518_mtmfs_bits() {
        let cases = [
            (
                [[0x3f7f_ffff, 0x3d27_966e], [0x3d27_966e, 0x3d0f_b1b6]],
                [[0x3f86_69fd, 0xbf9c_c3ac], [0xbf9c_c3ac, 0x41ef_7780]],
            ),
            (
                [[0x3ee2_418e, 0xba0e_a3ac], [0xba0e_a3ac, 0x3c78_bfe6]],
                [[0x4010_d55d, 0x3da6_1a34], [0x3da6_1a34, 0x4283_bc97]],
            ),
            (
                [[0x3e19_9816, 0xbb58_e85e], [0xbb58_e85e, 0x3ba9_073c]],
                [[0x40d8_67cb, 0x408a_da40], [0x408a_da40, 0x4344_a52c]],
            ),
        ];

        for (hessian_bits, expected_bits) in cases {
            let hessian = hessian_bits
                .map(|row| {
                    row.map(|bits| f64::from(f32::from_bits(bits)))
                        .into_iter()
                        .collect::<Vec<_>>()
                })
                .into_iter()
                .collect::<Vec<_>>();
            let inverse = invert_symmetric_positive_definite_casacore(&hessian)
                .expect("frozen CASA MT-MFS Hessian should invert");
            let actual_bits = [
                [
                    (inverse[0][0] as f32).to_bits(),
                    (inverse[0][1] as f32).to_bits(),
                ],
                [
                    (inverse[1][0] as f32).to_bits(),
                    (inverse[1][1] as f32).to_bits(),
                ],
            ];
            assert_eq!(actual_bits, expected_bits);
        }
    }
}
