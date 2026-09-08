// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction-owned polynomial basis and block-normal algebra.

use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct BlockNormalPlan {
    reference_frequency_hz: f64,
    coefficient_terms: usize,
    normal_moments: usize,
}

impl BlockNormalPlan {
    pub(crate) fn constant(reference_frequency_hz: f64) -> Result<Self, BlockNormalError> {
        Self::compile(reference_frequency_hz, 1)
    }

    pub(crate) fn taylor(
        reference_frequency_hz: f64,
        coefficient_terms: usize,
    ) -> Result<Self, BlockNormalError> {
        if coefficient_terms < 2 {
            return Err(BlockNormalError::TaylorTermCount);
        }
        Self::compile(reference_frequency_hz, coefficient_terms)
    }

    pub(crate) fn compile(
        reference_frequency_hz: f64,
        coefficient_terms: usize,
    ) -> Result<Self, BlockNormalError> {
        if !reference_frequency_hz.is_finite() || reference_frequency_hz <= 0.0 {
            return Err(BlockNormalError::InvalidReferenceFrequency);
        }
        let normal_moments = coefficient_terms
            .checked_mul(2)
            .and_then(|terms| terms.checked_sub(1))
            .ok_or(BlockNormalError::SizeOverflow)?;
        i32::try_from(normal_moments - 1).map_err(|_| BlockNormalError::SizeOverflow)?;
        Ok(Self {
            reference_frequency_hz,
            coefficient_terms,
            normal_moments,
        })
    }

    pub(crate) const fn reference_frequency_hz(self) -> f64 {
        self.reference_frequency_hz
    }

    pub(crate) const fn coefficient_term_count(self) -> usize {
        self.coefficient_terms
    }

    pub(crate) const fn normal_moment_count(self) -> usize {
        self.normal_moments
    }

    pub(crate) fn normal_moment_index(self, row: usize, column: usize) -> Option<usize> {
        if row >= self.coefficient_terms || column >= self.coefficient_terms {
            return None;
        }
        row.checked_add(column)
            .filter(|index| *index < self.normal_moments)
    }

    pub(crate) fn normalized_frequency(self, frequency_hz: f64) -> Result<f64, BlockNormalError> {
        if !frequency_hz.is_finite() || frequency_hz <= 0.0 {
            return Err(BlockNormalError::InvalidSampleFrequency);
        }
        let casa_frequency_hz = f64::from(frequency_hz as f32);
        if !casa_frequency_hz.is_finite() || casa_frequency_hz <= 0.0 {
            return Err(BlockNormalError::InvalidSampleFrequency);
        }
        let normalized = ((casa_frequency_hz - self.reference_frequency_hz)
            / self.reference_frequency_hz) as f32;
        if !normalized.is_finite() {
            return Err(BlockNormalError::NonFinitePower);
        }
        Ok(f64::from(normalized))
    }

    pub(crate) fn fill_coefficient_basis(
        self,
        frequency_hz: f64,
        coefficients: &mut [f64],
    ) -> Result<(), BlockNormalError> {
        if coefficients.len() != self.coefficient_terms {
            return Err(BlockNormalError::CoefficientBufferLength {
                expected: self.coefficient_terms,
                actual: coefficients.len(),
            });
        }
        self.fill_scaled_powers(frequency_hz, 1.0, coefficients)
    }

    pub(crate) fn fill_weighted_coefficient_basis(
        self,
        frequency_hz: f64,
        imaging_weight: f64,
        coefficients: &mut [f64],
    ) -> Result<(), BlockNormalError> {
        if coefficients.len() != self.coefficient_terms {
            return Err(BlockNormalError::CoefficientBufferLength {
                expected: self.coefficient_terms,
                actual: coefficients.len(),
            });
        }
        for (output, value) in coefficients
            .iter_mut()
            .zip(self.weighted_coefficients(frequency_hz, imaging_weight)?)
        {
            *output = value?;
        }
        Ok(())
    }

    /// Stream the same CASA-rounded coefficients without a temporary lane buffer.
    pub(crate) fn weighted_coefficients(
        self,
        frequency_hz: f64,
        imaging_weight: f64,
    ) -> Result<impl ExactSizeIterator<Item = Result<f64, BlockNormalError>>, BlockNormalError>
    {
        let (normalized, weight) = if self.coefficient_terms == 1 {
            (0.0, exact_constant_weight(imaging_weight)?)
        } else {
            (
                self.normalized_frequency(frequency_hz)? as f32,
                f64::from(casa_imaging_weight(imaging_weight)?),
            )
        };
        Ok((0..self.coefficient_terms).map(move |order| {
            if self.coefficient_terms == 1 {
                Ok(weight)
            } else {
                casa_scaled_taylor_power(normalized, weight as f32, order)
            }
        }))
    }

    pub(crate) fn fill_normal_moment_weights(
        self,
        frequency_hz: f64,
        imaging_weight: f64,
        moments: &mut [f64],
    ) -> Result<(), BlockNormalError> {
        if moments.len() != self.normal_moments {
            return Err(BlockNormalError::MomentBufferLength {
                expected: self.normal_moments,
                actual: moments.len(),
            });
        }
        if self.coefficient_terms == 1 {
            moments[0] = exact_constant_weight(imaging_weight)?;
            return Ok(());
        }
        let casa_imaging_weight = casa_imaging_weight(imaging_weight)?;
        self.fill_scaled_powers(frequency_hz, casa_imaging_weight, moments)
    }

    fn fill_scaled_powers(
        self,
        frequency_hz: f64,
        scale: f32,
        output: &mut [f64],
    ) -> Result<(), BlockNormalError> {
        let normalized = self.normalized_frequency(frequency_hz)? as f32;
        for (order, value) in output.iter_mut().enumerate() {
            *value = casa_scaled_taylor_power(normalized, scale, order)?;
        }
        Ok(())
    }
}

fn exact_constant_weight(imaging_weight: f64) -> Result<f64, BlockNormalError> {
    if imaging_weight.is_finite() && imaging_weight >= 0.0 {
        Ok(imaging_weight)
    } else {
        Err(BlockNormalError::InvalidImagingWeight)
    }
}

fn casa_imaging_weight(imaging_weight: f64) -> Result<f32, BlockNormalError> {
    if !imaging_weight.is_finite() || imaging_weight < 0.0 {
        return Err(BlockNormalError::InvalidImagingWeight);
    }
    let rounded = imaging_weight as f32;
    if rounded.is_finite() {
        Ok(rounded)
    } else {
        Err(BlockNormalError::InvalidImagingWeight)
    }
}

fn casa_scaled_taylor_power(
    normalized_frequency: f32,
    scale: f32,
    order: usize,
) -> Result<f64, BlockNormalError> {
    let order = i32::try_from(order).map_err(|_| BlockNormalError::SizeOverflow)?;
    let rounded = (f64::from(scale) * f64::from(normalized_frequency).powi(order)) as f32;
    if !rounded.is_finite() {
        return Err(BlockNormalError::NonFinitePower);
    }
    Ok(f64::from(rounded))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BlockNormalError {
    InvalidReferenceFrequency,
    InvalidSampleFrequency,
    InvalidImagingWeight,
    TaylorTermCount,
    SizeOverflow,
    CoefficientBufferLength { expected: usize, actual: usize },
    MomentBufferLength { expected: usize, actual: usize },
    NonFinitePower,
}

impl fmt::Display for BlockNormalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReferenceFrequency => {
                formatter.write_str("reference frequency must be finite and positive")
            }
            Self::InvalidSampleFrequency => {
                formatter.write_str("sample frequency must be finite and positive")
            }
            Self::InvalidImagingWeight => {
                formatter.write_str("imaging weight must be finite and non-negative")
            }
            Self::TaylorTermCount => {
                formatter.write_str("Taylor block-normal algebra requires at least two terms")
            }
            Self::SizeOverflow => formatter.write_str("block-normal cardinality overflowed"),
            Self::CoefficientBufferLength { expected, actual } => write!(
                formatter,
                "coefficient buffer length {actual} does not match {expected} terms"
            ),
            Self::MomentBufferLength { expected, actual } => write!(
                formatter,
                "normal-moment buffer length {actual} does not match {expected} moments"
            ),
            Self::NonFinitePower => formatter.write_str("normalized-frequency power is not finite"),
        }
    }
}

impl Error for BlockNormalError {}

#[cfg(test)]
mod tests {
    use super::{BlockNormalError, BlockNormalPlan, casa_scaled_taylor_power};

    #[test]
    fn t42_constant_uses_the_one_term_polynomial_law() {
        let plan = BlockNormalPlan::constant(100.0).expect("constant plan");
        assert_eq!(plan.reference_frequency_hz(), 100.0);
        assert_eq!(plan.coefficient_term_count(), 1);
        assert_eq!(plan.normal_moment_count(), 1);
        assert_eq!(plan.normal_moment_index(0, 0), Some(0));

        let mut basis = [f64::NAN; 1];
        let mut moments = [f64::NAN; 1];
        plan.fill_coefficient_basis(175.0, &mut basis)
            .expect("constant basis");
        plan.fill_normal_moment_weights(175.0, 3.5, &mut moments)
            .expect("constant moment");
        assert_eq!(basis, [1.0]);
        assert_eq!(moments, [3.5]);

        let exact_weight = 1.000_000_000_1;
        plan.fill_weighted_coefficient_basis(175.0, exact_weight, &mut basis)
            .expect("constant weighted basis");
        plan.fill_normal_moment_weights(175.0, exact_weight, &mut moments)
            .expect("constant exact moment");
        assert_eq!(basis[0].to_bits(), exact_weight.to_bits());
        assert_eq!(moments[0].to_bits(), exact_weight.to_bits());
    }

    #[test]
    fn t42_taylor_basis_and_signed_normal_moments_follow_ascending_powers() {
        let plan = BlockNormalPlan::taylor(100.0, 3).expect("Taylor plan");
        assert_eq!(plan.coefficient_term_count(), 3);
        assert_eq!(plan.normal_moment_count(), 5);
        assert_eq!(plan.normalized_frequency(75.0), Ok(-0.25));

        let mut basis = [f64::NAN; 3];
        let mut moments = [f64::NAN; 5];
        plan.fill_coefficient_basis(75.0, &mut basis)
            .expect("Taylor basis");
        plan.fill_normal_moment_weights(75.0, 2.0, &mut moments)
            .expect("Taylor moments");
        assert_eq!(basis, [1.0, -0.25, 0.0625]);
        assert_eq!(moments, [2.0, -0.5, 0.125, -0.03125, 0.0078125]);
    }

    #[test]
    fn t42_block_normal_is_hankel_and_matches_the_weighted_outer_product() {
        let plan = BlockNormalPlan::taylor(100.0, 3).expect("Taylor plan");
        let mut basis = [0.0; 3];
        let mut moments = [0.0; 5];
        plan.fill_coefficient_basis(125.0, &mut basis)
            .expect("Taylor basis");
        plan.fill_normal_moment_weights(125.0, 4.0, &mut moments)
            .expect("Taylor moments");

        for row in 0..plan.coefficient_term_count() {
            for column in 0..plan.coefficient_term_count() {
                let moment = plan
                    .normal_moment_index(row, column)
                    .expect("valid block entry");
                assert_eq!(moment, row + column);
                assert_eq!(moments[moment], 4.0 * basis[row] * basis[column]);
            }
        }
        assert_eq!(plan.normal_moment_index(3, 0), None);
        assert_eq!(plan.normal_moment_index(0, 3), None);
    }

    #[test]
    fn t42_multiple_frequency_rows_accumulate_the_same_global_moment_family() {
        let plan = BlockNormalPlan::taylor(100.0, 3).expect("Taylor plan");
        let mut total = [0.0; 5];
        let mut sample = [0.0; 5];
        for frequency_hz in [75.0, 125.0] {
            plan.fill_normal_moment_weights(frequency_hz, 2.0, &mut sample)
                .expect("normal moments");
            for (sum, value) in total.iter_mut().zip(sample) {
                *sum += value;
            }
        }
        assert_eq!(total, [4.0, 0.0, 0.25, 0.0, 0.015625]);
        assert_eq!(total[1].to_bits(), 0.0_f64.to_bits());
        assert_eq!(total[3].to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn t42_casa_mixed_precision_boundaries_are_exact() {
        let plan = BlockNormalPlan::taylor(1_111_111_111.0, 2).expect("Taylor plan");
        let frequency_hz = 1_234_567_890.123;
        let normalized = plan
            .normalized_frequency(frequency_hz)
            .expect("normalized frequency") as f32;
        assert_eq!(normalized.to_bits(), 1_038_323_261);
        let retained_f64_frequency =
            ((frequency_hz - plan.reference_frequency_hz()) / plan.reference_frequency_hz()) as f32;
        assert_eq!(retained_f64_frequency.to_bits(), 1_038_323_256);

        let recurrence_x = f32::from_bits(1_023_441_852);
        let casa_power =
            casa_scaled_taylor_power(recurrence_x, 1.0, 3).expect("CASA coefficient power") as f32;
        let mut f32_recurrence = 1.0_f32;
        for _ in 0..3 {
            f32_recurrence *= recurrence_x;
        }
        assert_eq!(casa_power.to_bits(), 939_619_483);
        assert_eq!(f32_recurrence.to_bits(), 939_619_484);

        let weighted_x = f32::from_bits(3_199_001_837);
        let weight = f32::from_bits(1_100_430_393);
        let casa_weighted =
            casa_scaled_taylor_power(weighted_x, weight, 2).expect("CASA weighted power") as f32;
        let rounded_power_then_weighted = weight * (f64::from(weighted_x).powi(2) as f32);
        assert_eq!(casa_weighted.to_bits(), 1_074_394_791);
        assert_eq!(rounded_power_then_weighted.to_bits(), 1_074_394_790);
    }

    #[test]
    fn t42_invalid_inputs_and_cardinality_overflow_fail_closed() {
        assert_eq!(
            BlockNormalPlan::constant(0.0),
            Err(BlockNormalError::InvalidReferenceFrequency)
        );
        assert_eq!(
            BlockNormalPlan::taylor(100.0, 1),
            Err(BlockNormalError::TaylorTermCount)
        );
        assert_eq!(
            BlockNormalPlan::taylor(100.0, usize::MAX),
            Err(BlockNormalError::SizeOverflow)
        );

        let plan = BlockNormalPlan::taylor(100.0, 2).expect("Taylor plan");
        assert_eq!(
            plan.fill_coefficient_basis(100.0, &mut [0.0; 1]),
            Err(BlockNormalError::CoefficientBufferLength {
                expected: 2,
                actual: 1,
            })
        );
        assert_eq!(
            plan.fill_normal_moment_weights(100.0, 1.0, &mut [0.0; 2]),
            Err(BlockNormalError::MomentBufferLength {
                expected: 3,
                actual: 2,
            })
        );
        assert_eq!(
            plan.fill_coefficient_basis(f64::NAN, &mut [0.0; 2]),
            Err(BlockNormalError::InvalidSampleFrequency)
        );
        assert_eq!(
            plan.fill_normal_moment_weights(100.0, -1.0, &mut [0.0; 3]),
            Err(BlockNormalError::InvalidImagingWeight)
        );
    }
}
