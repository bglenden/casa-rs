// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Shared numerical algorithms, and CASA's common primary-beam patterns
//! (`PBMath`'s Airy and EVLA beams, chosen by telescope name: [`PrimaryBeam`]),
//! which the simulator's prediction and single-dish gridding both use.

mod annular_aperture;
pub mod cell_noise;
mod elliptical_gaussian;
mod legacy_polynomial;
mod primary_beam;
mod simulated_bandpass;
mod vla_beams;

pub use annular_aperture::AnnularApertureVoltageTable;
pub use elliptical_gaussian::{
    EllipticalGaussian, EllipticalGaussianError, common_enclosing_gaussian, deconvolving_gaussian,
    gaussian_covariance, normalize_position_angle,
};
pub use legacy_polynomial::{legacy_bpoly_chebyshev_value, legacy_cubic_spline};
pub use primary_beam::{BeamPattern, DefaultRadial, FrequencyPattern, PrimaryBeam};
pub use simulated_bandpass::SimulatedBandpass;
pub use vla_beams::{EvlaBand, EvlaPowerPattern, VlaBand};

use nalgebra::{DMatrix, DVector, Dyn, linalg::SVD};

/// A factored real least-squares design for repeated observations at the
/// same basis rows. The solve uses the weighted solver's singular-value cutoff.
pub struct LeastSquaresDesign {
    decomposition: SVD<f64, Dyn, Dyn>,
    rows: usize,
}

impl LeastSquaresDesign {
    /// Factor the unweighted design. Each row must contain `coefficients`
    /// finite basis values, and there must be at least that many rows.
    #[must_use]
    pub fn unweighted<'a>(
        basis: impl Iterator<Item = &'a [f64]>,
        coefficients: usize,
    ) -> Option<Self> {
        if coefficients == 0 {
            return None;
        }
        let mut values = Vec::new();
        let mut rows = 0;
        for row in basis {
            if row.len() != coefficients || row.iter().any(|value| !value.is_finite()) {
                return None;
            }
            values.extend_from_slice(row);
            rows += 1;
        }
        (rows >= coefficients)
            .then(|| Self::factor(DMatrix::from_row_slice(rows, coefficients, &values)))
    }

    /// Solve for one finite observation per basis row, in the same order.
    #[must_use]
    pub fn solve(&self, observed: &[f64]) -> Option<Vec<f64>> {
        if observed.len() != self.rows || observed.iter().any(|value| !value.is_finite()) {
            return None;
        }
        self.solve_vector(&DVector::from_row_slice(observed))
    }

    fn factor(design: DMatrix<f64>) -> Self {
        let rows = design.nrows();
        Self {
            decomposition: design.svd(true, true),
            rows,
        }
    }

    fn solve_vector(&self, observed: &DVector<f64>) -> Option<Vec<f64>> {
        let solution = self.decomposition.solve(observed, 1.0e-12).ok()?;
        Some(solution.as_slice().to_vec())
    }
}

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
    LeastSquaresDesign::factor(design).solve_vector(&observed)
}

/// Solve an unweighted real least-squares system with the default relative
/// rank cutoff used by NumPy's `lstsq`. Rank-deficient systems return the
/// minimum-norm solution.
#[must_use]
pub fn solve_least_squares<const N: usize>(rows: &[([f64; N], f64)]) -> Option<[f64; N]> {
    if N == 0
        || rows.is_empty()
        || rows.iter().any(|(basis, value)| {
            !value.is_finite() || basis.iter().any(|value| !value.is_finite())
        })
    {
        return None;
    }
    let design =
        DMatrix::from_row_iterator(rows.len(), N, rows.iter().flat_map(|(basis, _)| *basis));
    let observations = DVector::from_iterator(rows.len(), rows.iter().map(|(_, value)| *value));
    let decomposition = design.svd(true, true);
    let largest = decomposition.singular_values.max();
    let tolerance = largest * rows.len().max(N) as f64 * f64::EPSILON;
    let fit = decomposition.solve(&observations, tolerance).ok()?;
    fit.as_slice().try_into().ok()
}

/// Solve Hermitian normal equations and report the number of independent pivots.
/// Dependent coefficients are set to zero unless the caller supplies a constraint.
#[must_use]
pub fn solve_complex_normal_equations(
    mut coefficients: Vec<num_complex::Complex64>,
    mut observations: Vec<num_complex::Complex64>,
) -> Option<(Vec<num_complex::Complex64>, usize)> {
    use num_complex::Complex64;

    let size = observations.len();
    if size == 0 || coefficients.len() != size.checked_mul(size)? {
        return None;
    }
    let diagonal = (0..size)
        .map(|index| coefficients[index * size + index].re)
        .collect::<Vec<_>>();
    let mut independent = Vec::new();
    for (index, &original) in diagonal.iter().enumerate() {
        let (_, remaining) = coefficients.split_at_mut(index * size);
        let (row, trailing) = remaining.split_at_mut(size);
        let pivot = row[index].re;
        // LSQFit compares a squared pivot to its own original diagonal.
        // A smaller visibility weight need not make a coefficient dependent.
        if !pivot.is_finite() || !original.is_finite() {
            return None;
        }
        if pivot <= original.sqrt() * 1.0e-6 {
            continue;
        }
        independent.push(index);
        let rhs = observations[index];
        for (offset, current) in trailing.chunks_exact_mut(size).enumerate() {
            let factor = current[index] / pivot;
            observations[index + offset + 1] -= factor * rhs;
            for (value, &source) in current[index + 1..].iter_mut().zip(&row[index + 1..]) {
                *value -= factor * source;
            }
        }
    }
    let mut solution = vec![Complex64::ZERO; size];
    for &index in independent.iter().rev() {
        let row = &coefficients[index * size..(index + 1) * size];
        let residual = observations[index]
            - row[index + 1..]
                .iter()
                .zip(&solution[index + 1..])
                .map(|(coefficient, value)| coefficient * value)
                .sum::<Complex64>();
        solution[index] = residual / row[index].re;
    }
    Some((solution, independent.len()))
}

/// Solve a symmetric system with casacore `LSQFit` ordering.
///
/// The input matrix must contain the symmetric system in its upper triangle.
/// This preserves casacore's LDLT factorization and arithmetic order for
/// algorithms whose serialized results depend on exact rounding.
#[must_use]
pub fn solve_symmetric_ldlt_casacore<const N: usize>(
    normal: [[f64; N]; N],
    known: [f64; N],
) -> Option<[f64; N]> {
    solve_symmetric_ldlt_casacore_with_precision(normal, known, 1.0e-12)
}

/// Solve a symmetric system in casacore `LSQFit` order with a specified
/// squared collinearity tolerance, as used by `LSQFit::invert`.
///
/// Only the upper triangle of `normal` is used. A pivot with
/// `pivot² / original_diagonal <= precision` is rejected. A precision of
/// zero matches the non-SVD `LinearFit` default.
#[must_use]
pub fn solve_symmetric_ldlt_casacore_with_precision<const N: usize>(
    mut normal: [[f64; N]; N],
    known: [f64; N],
    precision: f64,
) -> Option<[f64; N]> {
    let factor = CasacoreLdlt::factor(normal.as_flattened_mut(), N, precision)?;
    let mut solution = known;
    factor.solve(&mut solution)?;
    Some(solution)
}

/// In-place LDLT factorization in casacore `LSQFit` arithmetic order.
///
/// The factor can solve several right-hand sides, for instance to compute
/// the correction and the diagonal of its parameter covariance.
pub struct CasacoreLdlt<'a> {
    normal: &'a [f64],
    size: usize,
}

impl<'a> CasacoreLdlt<'a> {
    /// Factor the upper triangle of a row-major square matrix. Reject a
    /// pivot with `pivot² / original_diagonal <= precision` as collinear.
    pub fn factor(normal: &'a mut [f64], size: usize, precision: f64) -> Option<Self> {
        if normal.len() != size.checked_mul(size)? || !precision.is_finite() || precision < 0.0 {
            return None;
        }
        // Split each current row from the preceding factors so no copied
        // factor matrix is needed.
        for row in 0..size {
            let (prior_rows, current_and_after) = normal.split_at_mut(row * size);
            let current_row = &mut current_and_after[..size];
            let original_diagonal = current_row[row];
            if original_diagonal == 0.0 {
                return None;
            }
            let mut diagonal = original_diagonal;
            for (prior, prior_row) in prior_rows.chunks_exact(size).enumerate() {
                diagonal -= prior_row[row] * prior_row[row] / prior_row[prior];
            }
            if !diagonal.is_finite() || diagonal * diagonal / original_diagonal <= precision {
                return None;
            }
            current_row[row] = diagonal;
            for column in row + 1..size {
                for (prior, prior_row) in prior_rows.chunks_exact(size).enumerate() {
                    current_row[column] -= prior_row[row] * prior_row[column] / prior_row[prior];
                }
            }
        }
        Some(Self { normal, size })
    }

    /// Overwrite the right-hand side with the solution. Its length must
    /// equal the factor's dimension.
    pub fn solve(&self, solution: &mut [f64]) -> Option<()> {
        let size = self.size;
        if solution.len() != size {
            return None;
        }
        for row in 0..size {
            for prior in 0..row {
                let offset = prior * size;
                solution[row] -=
                    self.normal[offset + row] * solution[prior] / self.normal[offset + prior];
            }
        }
        for row in (0..size).rev() {
            let offset = row * size;
            for later in row + 1..size {
                solution[row] -= self.normal[offset + later] * solution[later];
            }
            solution[row] /= self.normal[offset + row];
        }
        Some(())
    }
}

/// Return the Moore-Penrose inverse of a rank-deficient normal matrix.
///
/// Both the input and output use row-major storage. Singular values at
/// working precision are dropped so a dependent parameter receives
/// the minimum-norm correction.
#[must_use]
pub fn pseudoinvert_normal_matrix(normal: &[f64], size: usize) -> Option<Vec<f64>> {
    if size == 0 || normal.len() != size.checked_mul(size)? || !normal.iter().all(|v| v.is_finite())
    {
        return None;
    }
    let decomposition = DMatrix::from_row_slice(size, size, normal).svd(true, true);
    let threshold = size as f64 * f64::EPSILON * decomposition.singular_values.amax();
    let inverse = decomposition.pseudo_inverse(threshold).ok()?;
    let mut entries = Vec::with_capacity(normal.len());
    for row in inverse.row_iter() {
        entries.extend(row.iter().copied());
    }
    entries.iter().all(|v| v.is_finite()).then_some(entries)
}

/// Solve a symmetric positive-definite system by Cholesky factorization.
/// The full symmetric matrix is row-major. Small positive pivots are valid.
#[must_use]
pub fn solve_symmetric_positive_definite(normal: &[f64], known: &[f64]) -> Option<Vec<f64>> {
    let size = known.len();
    if size == 0 || normal.len() != size.checked_mul(size)? {
        return None;
    }
    let factor = DMatrix::from_row_slice(size, size, normal).cholesky()?;
    let result = factor.solve(&DVector::from_column_slice(known));
    result
        .iter()
        .all(|value| value.is_finite())
        .then(|| result.as_slice().to_vec())
}

/// Invert a symmetric positive-definite matrix, as casacore's
/// `invertSymPosDef` does, by Cholesky factorization.
///
/// `normal` contains the full symmetric matrix in row-major order. The
/// returned inverse is also symmetric and in row-major order. A positive
/// pivot remains valid even when its physical scale is small.
#[must_use]
pub fn invert_symmetric_positive_definite(normal: &[f64], size: usize) -> Option<Vec<f64>> {
    if size == 0 || normal.len() != size.checked_mul(size)? {
        return None;
    }
    let inverse = DMatrix::from_row_slice(size, size, normal)
        .cholesky()?
        .inverse();
    let mut entries = Vec::with_capacity(normal.len());
    for row in inverse.row_iter() {
        entries.extend(row.iter().copied());
    }
    entries
        .iter()
        .all(|value| value.is_finite())
        .then_some(entries)
}

#[cfg(test)]
mod tests {
    use super::{
        CasacoreLdlt, invert_symmetric_positive_definite, solve_symmetric_ldlt_casacore,
        solve_weighted_least_squares,
    };

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

    #[test]
    fn casacore_ldlt_reuses_a_factor_with_a_negative_pivot() {
        let mut curvature = [1., 2., 0., 1.];
        let factor = CasacoreLdlt::factor(&mut curvature, 2, 0.).unwrap();
        for (mut rhs, expected) in [([3., 3.], [1., 1.]), ([1., 2.], [1., 0.])] {
            factor.solve(&mut rhs).unwrap();
            assert_eq!(rhs, expected);
        }
    }

    #[test]
    fn cholesky_inverts_a_tiny_positive_taylor_pivot() {
        // A 2-term mosaic has a second spectral moment of this scale
        // when the selected channel's converted mean is near the reference.
        let tiny = 1.78185e-13;
        let inverse = invert_symmetric_positive_definite(&[1.0, 0.0, 0.0, tiny], 2).unwrap();
        assert_eq!(inverse[..3], [1.0, 0.0, 0.0]);
        assert!((inverse[3] * tiny - 1.0).abs() < 1e-12);
    }

    #[test]
    fn cholesky_refuses_nonpositive_taylor_pivots() {
        for normal in [[1.0, 1.0, 1.0, 1.0], [1.0, 2.0, 2.0, 1.0]] {
            assert!(invert_symmetric_positive_definite(&normal, 2).is_none());
        }
    }
}
