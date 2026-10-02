// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction-owned paired polarization and Mueller operator.

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use num_complex::Complex64;
use smallvec::SmallVec;
use thiserror::Error;

type Coherency = [[Complex64; 2]; 2];
type Jones = [[Complex64; 2]; 2];

/// Physical receptor basis of one selected correlation layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedBasis {
    /// Values already expressed in Stokes coordinates.
    Stokes,
    /// Orthogonal X/Y receptors.
    Linear,
    /// Orthogonal R/L receptors.
    Circular,
}

/// One evaluated 4-by-4 complex Mueller response in PP/PQ/QP/QQ order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MuellerMatrix([[Complex64; 4]; 4]);

impl MuellerMatrix {
    /// Construct one finite full Mueller matrix.
    #[must_use]
    pub fn new(elements: [[Complex64; 4]; 4]) -> Option<Self> {
        elements
            .iter()
            .flatten()
            .all(|value| value.re.is_finite() && value.im.is_finite())
            .then_some(Self(elements))
    }

    /// Return the identity response.
    #[must_use]
    pub fn identity() -> Self {
        let mut elements = [[Complex64::default(); 4]; 4];
        for (index, row) in elements.iter_mut().enumerate() {
            row[index] = Complex64::new(1.0, 0.0);
        }
        Self(elements)
    }

    /// Return elements in PP/PQ/QP/QQ row and column order.
    #[must_use]
    pub const fn elements(self) -> [[Complex64; 4]; 4] {
        self.0
    }
}

/// A requested coordinate or source response could not form one paired operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PolarizationOperatorError {
    /// The requested model coordinate set is empty, duplicated, or mixed.
    #[error("requested polarization coordinates do not define one coordinate space")]
    InvalidModelCoordinates,
    /// Selected correlations are empty, duplicated, or mix feed bases.
    #[error("selected correlations do not define one feed basis")]
    InvalidCorrelationLayout,
    /// One parallactic angle is not finite.
    #[error("selected parallactic angles must be finite")]
    InvalidParallacticAngle,
    /// A vector does not match the compiled operator dimensions.
    #[error("polarization operator vector shape mismatch")]
    ShapeMismatch,
    /// An active model or visibility value is not finite.
    #[error("polarization operator received a non-finite active value")]
    NonFiniteValue,
    /// An input weight is negative or non-finite.
    #[error("polarization operator weights must be finite and nonnegative")]
    InvalidWeight,
}

/// One compiled row-local linear map from reconstruction coordinates to correlations.
///
/// The same coefficient matrix is used directly for prediction and conjugate-
/// transposed for the weighted adjoint. Flags and weights therefore cannot
/// silently select a different polarization transform.
#[derive(Debug, Clone, PartialEq)]
pub struct PolarizationOperator {
    model_coordinates: SmallVec<[PolarizationCoordinate; 4]>,
    correlations: SmallVec<[CorrelationType; 4]>,
    feed_basis: FeedBasis,
    coefficients: SmallVec<[Complex64; 16]>,
}

impl PolarizationOperator {
    /// Compile one ideal-feed/parallactic map followed by an evaluated Mueller response.
    pub fn compile(
        model_coordinates: &[PolarizationCoordinate],
        correlations: &[CorrelationType],
        parallactic_angles_rad: [f64; 2],
        mueller: MuellerMatrix,
    ) -> Result<Self, PolarizationOperatorError> {
        validate_model_coordinates(model_coordinates)?;
        let feed_basis = correlation_basis(correlations)?;
        if parallactic_angles_rad
            .iter()
            .any(|angle| !angle.is_finite())
        {
            return Err(PolarizationOperatorError::InvalidParallacticAngle);
        }
        let mut coefficients = SmallVec::<[Complex64; 16]>::new();
        if feed_basis == FeedBasis::Stokes {
            if coordinate_category(model_coordinates[0]) != PolarizationFamily::Stokes
                || mueller != MuellerMatrix::identity()
            {
                return Err(PolarizationOperatorError::InvalidCorrelationLayout);
            }
            for correlation in correlations {
                let coordinate = stokes_coordinate(*correlation)
                    .ok_or(PolarizationOperatorError::InvalidCorrelationLayout)?;
                coefficients.extend(model_coordinates.iter().map(|model| {
                    Complex64::new(if *model == coordinate { 1.0 } else { 0.0 }, 0.0)
                }));
            }
        } else {
            let first = feed_jones(feed_basis, parallactic_angles_rad[0]);
            let second = feed_jones(feed_basis, parallactic_angles_rad[1]);
            let mueller = mueller.elements();
            for correlation in correlations {
                let output = correlation_index(*correlation)
                    .ok_or(PolarizationOperatorError::InvalidCorrelationLayout)?;
                for coordinate in model_coordinates {
                    let sky = coordinate_coherency(*coordinate);
                    let ideal = flatten(mul2(mul2(first, sky), adjoint2(second)));
                    coefficients.push(dot4(mueller[output], ideal));
                }
            }
        }
        Ok(Self {
            model_coordinates: model_coordinates.iter().copied().collect(),
            correlations: correlations.iter().copied().collect(),
            feed_basis,
            coefficients,
        })
    }

    /// Return the physical selected feed basis.
    #[must_use]
    pub const fn feed_basis(&self) -> FeedBasis {
        self.feed_basis
    }

    /// Return requested reconstruction coordinates in operator-column order.
    #[must_use]
    pub fn model_coordinates(&self) -> &[PolarizationCoordinate] {
        &self.model_coordinates
    }

    /// Return selected correlations in operator-row order.
    #[must_use]
    pub fn correlations(&self) -> &[CorrelationType] {
        &self.correlations
    }

    /// Return the compiled row-major correlation-by-model coefficient matrix.
    #[must_use]
    pub fn coefficients(&self) -> &[Complex64] {
        &self.coefficients
    }

    /// Predict selected correlations from one model-coordinate vector.
    pub fn predict(
        &self,
        model: &[Complex64],
    ) -> Result<SmallVec<[Complex64; 4]>, PolarizationOperatorError> {
        if model.len() != self.model_coordinates.len() {
            return Err(PolarizationOperatorError::ShapeMismatch);
        }
        if model.iter().any(|value| !finite(*value)) {
            return Err(PolarizationOperatorError::NonFiniteValue);
        }
        Ok(self
            .coefficients
            .chunks_exact(model.len())
            .map(|row| {
                row.iter()
                    .zip(model)
                    .map(|(coefficient, value)| coefficient * value)
                    .sum()
            })
            .collect())
    }

    /// Apply the exact weighted adjoint to selected correlation values.
    ///
    /// A flagged lane contributes exactly zero. Unflagged lanes require a
    /// finite value and a finite nonnegative input weight.
    pub fn weighted_adjoint(
        &self,
        visibilities: &[Complex64],
        weights: &[f64],
        flags: &[bool],
    ) -> Result<SmallVec<[Complex64; 4]>, PolarizationOperatorError> {
        if visibilities.len() != self.correlations.len()
            || weights.len() != self.correlations.len()
            || flags.len() != self.correlations.len()
        {
            return Err(PolarizationOperatorError::ShapeMismatch);
        }
        let mut result = SmallVec::<[Complex64; 4]>::new();
        result.resize(self.model_coordinates.len(), Complex64::default());
        for (row_index, ((visibility, weight), flagged)) in
            visibilities.iter().zip(weights).zip(flags).enumerate()
        {
            if *flagged {
                continue;
            }
            if !weight.is_finite() || *weight < 0.0 {
                return Err(PolarizationOperatorError::InvalidWeight);
            }
            if *weight == 0.0 {
                continue;
            }
            if !finite(*visibility) {
                return Err(PolarizationOperatorError::NonFiniteValue);
            }
            let weighted = visibility * *weight;
            let row_start = row_index * self.model_coordinates.len();
            for (column, output) in result.iter_mut().enumerate() {
                *output += self.coefficients[row_start + column].conj() * weighted;
            }
        }
        Ok(result)
    }
}

fn validate_model_coordinates(
    coordinates: &[PolarizationCoordinate],
) -> Result<(), PolarizationOperatorError> {
    if coordinates.is_empty() {
        return Err(PolarizationOperatorError::InvalidModelCoordinates);
    }
    let category = coordinate_category(coordinates[0]);
    if coordinates.iter().enumerate().any(|(index, coordinate)| {
        coordinate_category(*coordinate) != category || coordinates[..index].contains(coordinate)
    }) {
        return Err(PolarizationOperatorError::InvalidModelCoordinates);
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PolarizationFamily {
    Stokes,
    Linear,
    Circular,
}

const fn coordinate_category(coordinate: PolarizationCoordinate) -> PolarizationFamily {
    match coordinate {
        PolarizationCoordinate::StokesI
        | PolarizationCoordinate::StokesQ
        | PolarizationCoordinate::StokesU
        | PolarizationCoordinate::StokesV => PolarizationFamily::Stokes,
        PolarizationCoordinate::LinearXx
        | PolarizationCoordinate::LinearXy
        | PolarizationCoordinate::LinearYx
        | PolarizationCoordinate::LinearYy => PolarizationFamily::Linear,
        PolarizationCoordinate::CircularRr
        | PolarizationCoordinate::CircularRl
        | PolarizationCoordinate::CircularLr
        | PolarizationCoordinate::CircularLl => PolarizationFamily::Circular,
    }
}

fn correlation_basis(
    correlations: &[CorrelationType],
) -> Result<FeedBasis, PolarizationOperatorError> {
    let Some(first) = correlations.first().and_then(|value| basis(*value)) else {
        return Err(PolarizationOperatorError::InvalidCorrelationLayout);
    };
    if correlations.iter().enumerate().any(|(index, correlation)| {
        basis(*correlation) != Some(first) || correlations[..index].contains(correlation)
    }) {
        return Err(PolarizationOperatorError::InvalidCorrelationLayout);
    }
    Ok(first)
}

const fn basis(correlation: CorrelationType) -> Option<FeedBasis> {
    match correlation {
        CorrelationType::StokesI
        | CorrelationType::StokesQ
        | CorrelationType::StokesU
        | CorrelationType::StokesV => Some(FeedBasis::Stokes),
        CorrelationType::LinearXx
        | CorrelationType::LinearXy
        | CorrelationType::LinearYx
        | CorrelationType::LinearYy => Some(FeedBasis::Linear),
        CorrelationType::CircularRr
        | CorrelationType::CircularRl
        | CorrelationType::CircularLr
        | CorrelationType::CircularLl => Some(FeedBasis::Circular),
        _ => None,
    }
}

const fn stokes_coordinate(correlation: CorrelationType) -> Option<PolarizationCoordinate> {
    match correlation {
        CorrelationType::StokesI => Some(PolarizationCoordinate::StokesI),
        CorrelationType::StokesQ => Some(PolarizationCoordinate::StokesQ),
        CorrelationType::StokesU => Some(PolarizationCoordinate::StokesU),
        CorrelationType::StokesV => Some(PolarizationCoordinate::StokesV),
        _ => None,
    }
}

const fn correlation_index(correlation: CorrelationType) -> Option<usize> {
    match correlation {
        CorrelationType::LinearXx | CorrelationType::CircularRr => Some(0),
        CorrelationType::LinearXy | CorrelationType::CircularRl => Some(1),
        CorrelationType::LinearYx | CorrelationType::CircularLr => Some(2),
        CorrelationType::LinearYy | CorrelationType::CircularLl => Some(3),
        _ => None,
    }
}

fn coordinate_coherency(coordinate: PolarizationCoordinate) -> Coherency {
    let zero = Complex64::default();
    let one = Complex64::new(1.0, 0.0);
    let imaginary = Complex64::new(0.0, 1.0);
    match coordinate {
        PolarizationCoordinate::StokesI => [[one, zero], [zero, one]],
        PolarizationCoordinate::StokesQ => [[one, zero], [zero, -one]],
        PolarizationCoordinate::StokesU => [[zero, one], [one, zero]],
        PolarizationCoordinate::StokesV => [[zero, imaginary], [-imaginary, zero]],
        PolarizationCoordinate::LinearXx => [[one, zero], [zero, zero]],
        PolarizationCoordinate::LinearXy => [[zero, one], [zero, zero]],
        PolarizationCoordinate::LinearYx => [[zero, zero], [one, zero]],
        PolarizationCoordinate::LinearYy => [[zero, zero], [zero, one]],
        PolarizationCoordinate::CircularRr
        | PolarizationCoordinate::CircularRl
        | PolarizationCoordinate::CircularLr
        | PolarizationCoordinate::CircularLl => {
            let mut circular = [[zero; 2]; 2];
            let index = match coordinate {
                PolarizationCoordinate::CircularRr => (0, 0),
                PolarizationCoordinate::CircularRl => (0, 1),
                PolarizationCoordinate::CircularLr => (1, 0),
                PolarizationCoordinate::CircularLl => (1, 1),
                _ => unreachable!(),
            };
            circular[index.0][index.1] = one;
            let conversion = circular_conversion();
            mul2(mul2(adjoint2(conversion), circular), conversion)
        }
    }
}

fn feed_jones(basis: FeedBasis, angle: f64) -> Jones {
    let rotation = [
        [
            Complex64::new(angle.cos(), 0.0),
            Complex64::new(angle.sin(), 0.0),
        ],
        [
            Complex64::new(-angle.sin(), 0.0),
            Complex64::new(angle.cos(), 0.0),
        ],
    ];
    match basis {
        FeedBasis::Stokes => [
            [Complex64::new(1.0, 0.0), Complex64::default()],
            [Complex64::default(), Complex64::new(1.0, 0.0)],
        ],
        FeedBasis::Linear => rotation,
        FeedBasis::Circular => mul2(circular_conversion(), rotation),
    }
}

fn circular_conversion() -> Jones {
    let scale = std::f64::consts::FRAC_1_SQRT_2;
    [
        [Complex64::new(scale, 0.0), Complex64::new(0.0, scale)],
        [Complex64::new(scale, 0.0), Complex64::new(0.0, -scale)],
    ]
}

fn mul2(left: Jones, right: Jones) -> Jones {
    let mut output = [[Complex64::default(); 2]; 2];
    for (row, values) in output.iter_mut().enumerate() {
        for (column, value) in values.iter_mut().enumerate() {
            *value = (0..2)
                .map(|inner| left[row][inner] * right[inner][column])
                .sum();
        }
    }
    output
}

fn adjoint2(input: Jones) -> Jones {
    [
        [input[0][0].conj(), input[1][0].conj()],
        [input[0][1].conj(), input[1][1].conj()],
    ]
}

const fn flatten(input: Coherency) -> [Complex64; 4] {
    [input[0][0], input[0][1], input[1][0], input[1][1]]
}

fn dot4(left: [Complex64; 4], right: [Complex64; 4]) -> Complex64 {
    left.into_iter().zip(right).map(|(a, b)| a * b).sum()
}

const fn finite(value: Complex64) -> bool {
    value.re.is_finite() && value.im.is_finite()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const CASA_ORACLE: &str = include_str!(
        "../../../resources/imaging-architecture/baselines/issue519-polarization-oracle.json"
    );

    const STOKES: [PolarizationCoordinate; 4] = [
        PolarizationCoordinate::StokesI,
        PolarizationCoordinate::StokesQ,
        PolarizationCoordinate::StokesU,
        PolarizationCoordinate::StokesV,
    ];
    const LINEAR: [CorrelationType; 4] = [
        CorrelationType::LinearXx,
        CorrelationType::LinearXy,
        CorrelationType::LinearYx,
        CorrelationType::LinearYy,
    ];
    const CIRCULAR: [CorrelationType; 4] = [
        CorrelationType::CircularRr,
        CorrelationType::CircularRl,
        CorrelationType::CircularLr,
        CorrelationType::CircularLl,
    ];

    #[test]
    fn polarized_point_source_matches_the_pinned_casa_linear_and_circular_oracle() {
        let oracle: Value = serde_json::from_str(CASA_ORACLE).unwrap();
        assert_eq!(oracle["schema"], "casa-rs.issue519-polarization-oracle.v2");
        assert_eq!(oracle["casa_version"], "6.7.6.14");
        let model = parse_real_complex_array(&oracle["case"]["stokes_jy"]);

        for (name, correlations, basis) in [
            ("linear", LINEAR.as_slice(), FeedBasis::Linear),
            ("circular", CIRCULAR.as_slice(), FeedBasis::Circular),
        ] {
            let case = &oracle[name];
            let angles = parse_pair(&case["operator_angles_rad"]);
            assert!(angles.iter().all(|angle| angle.abs() > 0.1));
            let casa_correlations = parse_complex_array(&case["data"]);
            let operator = PolarizationOperator::compile(
                &STOKES,
                correlations,
                angles,
                MuellerMatrix::identity(),
            )
            .unwrap();
            let predicted = operator.predict(&model).unwrap();
            assert!(
                complex_nrms(&predicted, &casa_correlations) <= 1.0e-3,
                "{name} prediction does not match CASA: nrms={}",
                complex_nrms(&predicted, &casa_correlations)
            );

            let weighted = &case["weighted_flagged_correlations"];
            let flags = parse_bool_array(&weighted["flags"]);
            let weights = parse_f64_array(&weighted["weights"]);
            let adjoint = operator
                .weighted_adjoint(&casa_correlations, &weights, &flags)
                .unwrap();
            assert!(adjoint.iter().all(|value| finite(*value)));

            let formed = &case["casa_visbuffer_form_stokes"];
            let casa_stokes = parse_complex_array(&formed["data"]);
            assert_casa_close(&form_stokes(&casa_correlations, basis), &casa_stokes);
            assert_eq!(
                parse_bool_array(&formed["flags"]),
                [
                    flags[0] || flags[3],
                    flags[0] || flags[3],
                    flags[1] || flags[2],
                    flags[1] || flags[2]
                ]
            );
            assert_eq!(
                parse_f64_array(&formed["weights"]),
                [
                    weights[0] + weights[3],
                    weights[0] + weights[3],
                    weights[1] + weights[2],
                    weights[1] + weights[2]
                ]
            );
        }
    }

    #[test]
    fn full_mueller_and_flags_obey_weighted_adjoint_law() {
        let response = MuellerMatrix::new([
            [c(0.9, 0.1), c(0.2, -0.1), c(0.0, 0.0), c(0.1, 0.0)],
            [c(-0.1, 0.3), c(0.8, 0.0), c(0.2, 0.2), c(0.0, 0.0)],
            [c(0.0, 0.0), c(0.1, -0.2), c(0.7, 0.1), c(0.2, 0.0)],
            [c(0.05, 0.0), c(0.0, 0.0), c(-0.1, 0.1), c(1.1, -0.1)],
        ])
        .unwrap();
        let operator =
            PolarizationOperator::compile(&STOKES, &LINEAR, [0.37, -0.21], response).unwrap();
        let model = [c(1.2, -0.2), c(-0.4, 0.3), c(0.2, 0.1), c(0.05, -0.07)];
        let data = [c(0.7, 0.4), c(-0.3, 0.2), c(0.8, -0.5), c(0.1, 0.9)];
        let weights = [2.0, 0.5, 3.0, 1.25];
        let flags = [false, true, false, false];
        let prediction = operator.predict(&model).unwrap();
        let weighted_data = std::array::from_fn::<_, 4, _>(|index| {
            if flags[index] {
                Complex64::default()
            } else {
                data[index] * weights[index]
            }
        });
        let left: Complex64 = prediction
            .iter()
            .zip(weighted_data)
            .map(|(prediction, value)| prediction.conj() * value)
            .sum();
        let adjoint = operator.weighted_adjoint(&data, &weights, &flags).unwrap();
        let right: Complex64 = model
            .iter()
            .zip(adjoint)
            .map(|(value, adjoint)| value.conj() * adjoint)
            .sum();
        assert!(
            (left - right).norm() < 1.0e-12,
            "left={left:?} right={right:?}"
        );
    }

    #[test]
    fn flagged_lanes_are_zero_without_inspecting_payload_or_weight() {
        let operator =
            PolarizationOperator::compile(&STOKES, &LINEAR, [0.1, -0.2], MuellerMatrix::identity())
                .unwrap();
        let adjoint = operator
            .weighted_adjoint(
                &[c(f64::NAN, 0.0), c(1.0, 0.0), c(2.0, 0.0), c(3.0, 0.0)],
                &[f64::NAN, 0.0, 0.0, 0.0],
                &[true, false, false, false],
            )
            .unwrap();
        assert_eq!(adjoint.as_slice(), &[Complex64::default(); 4]);
    }

    #[test]
    fn parallactic_rotation_changes_linear_qu_but_not_unpolarized_i() {
        let operator =
            PolarizationOperator::compile(&STOKES, &LINEAR, [0.3, 0.3], MuellerMatrix::identity())
                .unwrap();
        let unpolarized = operator
            .predict(&[c(2.0, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)])
            .unwrap();
        assert_close(
            &unpolarized,
            &[c(2.0, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(2.0, 0.0)],
        );
        let polarized = operator
            .predict(&[c(0.0, 0.0), c(1.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)])
            .unwrap();
        assert!((polarized[0].re - (0.6_f64).cos()).abs() < 1.0e-12);
        assert!((polarized[1].re + (0.6_f64).sin()).abs() < 1.0e-12);
    }

    const fn c(re: f64, im: f64) -> Complex64 {
        Complex64::new(re, im)
    }

    fn assert_close(actual: &[Complex64], expected: &[Complex64]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert!(
                (actual - expected).norm() < 1.0e-12,
                "{actual:?} != {expected:?}"
            );
        }
    }

    fn parse_complex_array(value: &Value) -> Vec<Complex64> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| {
                let pair = pair.as_array().unwrap();
                c(pair[0].as_f64().unwrap(), pair[1].as_f64().unwrap())
            })
            .collect()
    }

    fn parse_real_complex_array(value: &Value) -> Vec<Complex64> {
        parse_f64_array(value)
            .into_iter()
            .map(|value| c(value, 0.0))
            .collect()
    }

    fn parse_f64_array(value: &Value) -> Vec<f64> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_f64().unwrap())
            .collect()
    }

    fn parse_bool_array(value: &Value) -> Vec<bool> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_bool().unwrap())
            .collect()
    }

    fn parse_pair(value: &Value) -> [f64; 2] {
        let values = parse_f64_array(value);
        [values[0], values[1]]
    }

    fn complex_nrms(actual: &[Complex64], expected: &[Complex64]) -> f64 {
        let residual_energy: f64 = actual
            .iter()
            .zip(expected)
            .map(|(actual, expected)| (actual - expected).norm_sqr())
            .sum();
        let reference_energy: f64 = expected.iter().map(|value| value.norm_sqr()).sum();
        (residual_energy / reference_energy).sqrt()
    }

    fn form_stokes(correlations: &[Complex64], basis: FeedBasis) -> [Complex64; 4] {
        match basis {
            FeedBasis::Stokes => [
                correlations[0],
                correlations[1],
                correlations[2],
                correlations[3],
            ],
            FeedBasis::Linear => [
                0.5 * (correlations[0] + correlations[3]),
                0.5 * (correlations[0] - correlations[3]),
                0.5 * (correlations[1] + correlations[2]),
                c(0.0, -0.5) * (correlations[1] - correlations[2]),
            ],
            FeedBasis::Circular => [
                0.5 * (correlations[0] + correlations[3]),
                0.5 * (correlations[1] + correlations[2]),
                c(0.0, 0.5) * (correlations[2] - correlations[1]),
                0.5 * (correlations[0] - correlations[3]),
            ],
        }
    }

    fn assert_casa_close(actual: &[Complex64], expected: &[Complex64]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert!(
                (actual - expected).norm() <= 1.0e-6,
                "{actual:?} != CASA {expected:?}"
            );
        }
    }
}
