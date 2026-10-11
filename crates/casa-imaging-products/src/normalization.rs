// SPDX-License-Identifier: LGPL-3.0-or-later

//! Normalization (plan section 5.7): CASA's rules that turn the
//! unnormalized normal state into image units. The minor cycle and the
//! published products both use them.
//!
//! | Quantity | Rule (CASA) | Here |
//! |---|---|---|
//! | PSF | each plane ÷ the term-0 PSF's peak | `psf_peak`, `normalized_psf_value` |
//! | dirty or residual, standard gridders | ÷ sumwt | [`normalize_plane`] |
//! | PB (mosaic, AW) | √\|weight\| ÷ pbmax, zero where PB ≤ pblimit | [`MosaicSensitivity::primary_beam`], [`MosaicSensitivity::weighted_primary_beam`] |
//! | residual, flat noise | ÷ (pbmax·√\|weight\|) where that exceeds pblimit·pbmax², else 0 | [`MosaicSensitivity::normalize`], [`MosaicSensitivity::normalize_weighted_residual_sample`] |
//! | residual, flat sky | ÷ weight where it exceeds pblimit²·pbmax², else 0 | the same |
//! | model, flat noise | × PB before prediction, ÷ PB after the solve | [`MosaicSensitivity::physical_to_apparent`], [`MosaicSensitivity::apparent_to_physical`] |
//! | pbcor | restored ÷ PB where PB > pblimit | [`MosaicSensitivity::correct_primary_beam`] |
//!
//! The PB and residual rules are `SIImageStore::makePBFromWeight` and
//! `divideResidualByWeight`. Both residual cutoffs are PB > pblimit where
//! the weight is positive; at negative weights, which FFT ringing leaves at
//! a mosaic's edges, flat noise keeps CASA's `abs` and flat sky has no
//! support.

use casa_imaging_model::{
    PrimaryBeamValidityPolicy, ProductNormalization, ProductSupportComparison,
};

use crate::ProductsError;

/// The principal PSF's measured peak, which publication divides every
/// PSF plane by; coupled Taylor terms all use this one divisor.
pub(crate) fn psf_peak(mut values: impl Iterator<Item = f32>) -> Result<f32, ProductsError> {
    values.try_fold(0.0_f32, |peak, value| {
        value
            .is_finite()
            .then_some(peak.max(value))
            .ok_or(ProductsError::GeneratedNonfinite)
    })
}

pub(crate) fn normalized_psf_value(value: f32, peak: f32) -> f32 {
    if peak > 0.0 { value / peak } else { 0.0 }
}

/// Normalize a standard-gridder plane: divide it by its sum of weights.
///
/// A plane without a positive, finite sum of weights blanks to NaN.
#[must_use]
pub fn normalize_plane(values: &[f32], sum_weight: f64) -> Vec<f32> {
    if !(sum_weight.is_finite() && sum_weight > 0.0) {
        return vec![f32::NAN; values.len()];
    }
    values
        .iter()
        .map(|value| value / sum_weight as f32)
        .collect()
}

/// The image units a direction-dependent minor cycle solves in.
///
/// The application binds the requested normalization and PB support to the
/// solver, whichever products are published.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MinorCycleImageResponse {
    normalization: ProductNormalization,
    policy: PrimaryBeamValidityPolicy,
}

impl MinorCycleImageResponse {
    /// Bind CASA's flat-noise or flat-sky image units and PB support.
    ///
    /// # Errors
    ///
    /// Unit-response normalization has no direction-dependent model mapping.
    pub fn new(
        normalization: ProductNormalization,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<Self, ProductsError> {
        if !matches!(
            normalization,
            ProductNormalization::FlatNoise | ProductNormalization::FlatSky
        ) {
            return Err(ProductsError::UnsupportedNormalization);
        }
        Ok(Self {
            normalization,
            policy,
        })
    }

    /// Return the bound image normalization.
    #[must_use]
    pub const fn normalization(self) -> ProductNormalization {
        self.normalization
    }

    /// Return PB support, which is independent of the CLEAN mask.
    #[must_use]
    pub const fn policy(self) -> PrimaryBeamValidityPolicy {
        self.policy
    }
}

/// One plane's direction-dependent sensitivity, borrowed from the normal
/// state.
///
/// It derives CASA's unit-peak PB, the flat-noise and flat-sky residuals,
/// PB support and the apparent and physical model. It owns no CLEAN mask
/// and leaves the normal state unchanged.
#[derive(Debug, Clone, Copy)]
pub struct MosaicSensitivity<'a> {
    values: &'a [f64],
    peak: f64,
    normal_sum_weight: f64,
    model_pb_scale: f32,
}

impl<'a> MosaicSensitivity<'a> {
    /// Bind finite sensitivity with positive support.
    ///
    /// # Errors
    ///
    /// Empty or non-finite sensitivity, or sensitivity with no positive
    /// value: no weighted sample reached the plane.
    pub fn new(values: &'a [f64]) -> Result<Self, ProductsError> {
        if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        let peak = values.iter().copied().fold(0.0_f64, f64::max);
        if peak <= 0.0 {
            return Err(ProductsError::UnsupportedSensitivity);
        }
        Ok(Self {
            values,
            peak,
            normal_sum_weight: 1.0,
            model_pb_scale: (peak as f32).sqrt(),
        })
    }

    /// Bind the sum of weights that forms CASA's published Float weight
    /// image.
    ///
    /// Model conversion evaluates `Float(raw / sum_weight)` before its roots
    /// and support decision. The raw sensitivity is unchanged and no
    /// normalized weight plane is allocated.
    ///
    /// # Errors
    ///
    /// The sum of weights or the scaled peak is not positive and finite.
    pub fn with_normal_sum_weight(mut self, sum_weight: f64) -> Result<Self, ProductsError> {
        if !sum_weight.is_finite() || sum_weight <= 0.0 {
            return Err(ProductsError::UnsupportedSensitivity);
        }
        let scale = ((self.peak / sum_weight) as f32).sqrt();
        if !scale.is_finite() || scale <= 0.0 {
            return Err(ProductsError::UnsupportedSensitivity);
        }
        self.normal_sum_weight = sum_weight;
        self.model_pb_scale = scale;
        Ok(self)
    }

    fn finite(value: f64) -> Result<f64, ProductsError> {
        value
            .is_finite()
            .then_some(value)
            .ok_or(ProductsError::GeneratedNonfinite)
    }

    fn supported(pb: f64, policy: PrimaryBeamValidityPolicy) -> bool {
        match policy.comparison() {
            ProductSupportComparison::StrictlyGreater => pb > f64::from(policy.cutoff()),
        }
    }

    /// Return one unit-peak primary-beam value, `sqrt(|sensitivity| / peak)`.
    #[must_use]
    pub fn primary_beam_at(self, index: usize) -> f64 {
        (self.values[index].abs() / self.peak).sqrt()
    }

    /// Return whether a pixel is inside the PB support.
    #[must_use]
    pub fn valid_at(self, index: usize, policy: PrimaryBeamValidityPolicy) -> bool {
        Self::supported(self.primary_beam_at(index), policy)
    }

    fn model_primary_beam_at(self, index: usize) -> Result<f64, ProductsError> {
        let weight = (self.values[index] / self.normal_sum_weight) as f32;
        let pb = weight.abs().sqrt() / self.model_pb_scale;
        Self::finite(f64::from(pb))
    }

    /// Build CASA's unit-peak PB, `sqrt(|sensitivity| / peak)`.
    #[must_use]
    pub fn primary_beam(self) -> Vec<f32> {
        self.values
            .iter()
            .map(|value| (value.abs() / self.peak).sqrt() as f32)
            .collect()
    }

    /// Derive PB from a published CASA Float weight plane, retaining Float
    /// roots.
    ///
    /// # Errors
    ///
    /// The weight plane is empty, non-finite or has no positive value.
    pub fn primary_beam_from_weight(weight: &[f32]) -> Result<Vec<f32>, ProductsError> {
        if weight.is_empty() || weight.iter().any(|value| !value.is_finite()) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        let peak = weight.iter().copied().fold(0.0_f32, f32::max);
        if peak <= 0.0 {
            return Err(ProductsError::UnsupportedSensitivity);
        }
        let scale = peak.sqrt();
        Ok(weight
            .iter()
            .map(|value| value.abs().sqrt() / scale)
            .collect())
    }

    /// Normalize a raw residual plane to flat noise or flat sky.
    ///
    /// This is [`Self::normalize_weighted_residual_sample`] in the raw
    /// sensitivity's units, where every sum of weights cancels: flat noise
    /// divides by √(|sensitivity|·peak) where that exceeds cutoff·peak, flat
    /// sky by the sensitivity where it exceeds cutoff²·peak, and the
    /// residual is zero elsewhere.
    ///
    /// # Errors
    ///
    /// Unit-response normalization, or a non-finite result.
    ///
    /// # Panics
    ///
    /// If `values` is not one value per sensitivity pixel.
    pub fn normalize(
        self,
        values: &[f32],
        normalization: ProductNormalization,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<Vec<f32>, ProductsError> {
        assert_eq!(
            values.len(),
            self.values.len(),
            "a residual plane and its sensitivity have one shape"
        );
        let limit = f64::from(policy.cutoff().abs());
        values
            .iter()
            .zip(self.values)
            .map(|(value, sensitivity)| {
                let (denominator, cutoff) = match normalization {
                    ProductNormalization::FlatNoise => {
                        ((sensitivity.abs() * self.peak).sqrt(), limit * self.peak)
                    }
                    ProductNormalization::FlatSky => (*sensitivity, limit * limit * self.peak),
                    ProductNormalization::UnitResponse => {
                        return Err(ProductsError::UnsupportedNormalization);
                    }
                };
                if denominator > cutoff {
                    Ok(Self::finite(f64::from(*value) / denominator)? as f32)
                } else {
                    Ok(0.0)
                }
            })
            .collect()
    }

    /// Normalize a residual through CASA's Float sum-weight and weight-image
    /// stages.
    ///
    /// The normal sum weight is bound by [`Self::with_normal_sum_weight`].
    /// All Taylor terms divide by the principal publication sum weight given
    /// here. Flat noise divides by `sqrt(abs(weight)) * PBScale` where that
    /// exceeds `cutoff * PBScale^2`; flat sky divides by `weight` where it
    /// exceeds `cutoff^2 * PBScale^2`. Both are PB > cutoff. Elsewhere the
    /// residual is zero.
    ///
    /// # Errors
    ///
    /// Unit-response normalization, or non-finite input or arithmetic.
    pub fn normalize_weighted_residual_sample(
        self,
        value: f64,
        index: usize,
        normalization: ProductNormalization,
        published_sum_weight: f64,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<f64, ProductsError> {
        let mut residual = value as f32;
        let sum_weight = published_sum_weight as f32;
        if !residual.is_finite() || !sum_weight.is_finite() {
            return Err(ProductsError::GeneratedNonfinite);
        }
        if sum_weight != 1.0 {
            residual = if f64::from(sum_weight) > 1.0e-7 {
                residual / sum_weight
            } else {
                0.0
            };
        }
        let weight = (self.values[index] / self.normal_sum_weight) as f32;
        if !weight.is_finite() {
            return Err(ProductsError::GeneratedNonfinite);
        }
        let pb_scale = f64::from(self.model_pb_scale);
        let limit = f64::from(policy.cutoff().abs());
        let (denominator, cutoff) = match normalization {
            ProductNormalization::FlatNoise => (
                weight.abs().sqrt() * self.model_pb_scale,
                limit * pb_scale * pb_scale,
            ),
            ProductNormalization::FlatSky => (weight, limit * limit * pb_scale * pb_scale),
            ProductNormalization::UnitResponse => {
                return Err(ProductsError::UnsupportedNormalization);
            }
        };
        let normalized = if denominator > cutoff as f32 {
            residual / denominator
        } else {
            0.0
        };
        Self::finite(f64::from(normalized))
    }

    /// Form CASA's cutoff-blanked Float PB from the bound normalized weight.
    ///
    /// This weight-image route takes the absolute value before its square
    /// root, matching model conversion. Scalar mosaic products use the
    /// unnormalized positive-sensitivity [`Self::primary_beam`] instead.
    ///
    /// # Errors
    ///
    /// A non-finite PB value.
    pub fn weighted_primary_beam(
        self,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<Vec<f32>, ProductsError> {
        (0..self.values.len())
            .map(|index| {
                let pb = self.model_primary_beam_at(index)?;
                Ok(if Self::supported(pb, policy) {
                    pb as f32
                } else {
                    0.0
                })
            })
            .collect()
    }

    /// Return PB support, independent of the CLEAN mask and model.
    #[must_use]
    pub fn validity(self, policy: PrimaryBeamValidityPolicy) -> Vec<bool> {
        (0..self.values.len())
            .map(|index| self.valid_at(index, policy))
            .collect()
    }

    /// Map a physical flat-sky model pixel to the units the minor cycle
    /// solves in.
    ///
    /// CASA flat noise multiplies by the PB inside its support and zeroes
    /// the rest; flat sky is the identity. Apply this before
    /// restoring-beam convolution.
    ///
    /// # Errors
    ///
    /// Unit-response normalization, or non-finite arithmetic.
    pub fn physical_to_apparent(
        self,
        value: f64,
        index: usize,
        normalization: ProductNormalization,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<f64, ProductsError> {
        match normalization {
            ProductNormalization::FlatNoise => {
                let pb = self.model_primary_beam_at(index)?;
                if Self::supported(pb, policy) {
                    Self::finite(value * pb)
                } else {
                    Ok(0.0)
                }
            }
            ProductNormalization::FlatSky => Self::finite(value),
            ProductNormalization::UnitResponse => Err(ProductsError::UnsupportedNormalization),
        }
    }

    /// Map a solved pixel to the physical model that prediction consumes.
    ///
    /// Apply it to every pixel after multiscale expansion, not only to the
    /// component centre. PB support is not the CLEAN mask.
    ///
    /// # Errors
    ///
    /// Unit-response normalization, or non-finite arithmetic.
    pub fn apparent_to_physical(
        self,
        value: f64,
        index: usize,
        normalization: ProductNormalization,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<f64, ProductsError> {
        match normalization {
            ProductNormalization::FlatNoise => {
                let pb = self.model_primary_beam_at(index)?;
                if Self::supported(pb, policy) {
                    Self::finite(value / pb)
                } else {
                    Ok(0.0)
                }
            }
            ProductNormalization::FlatSky => Self::finite(value),
            ProductNormalization::UnitResponse => Err(ProductsError::UnsupportedNormalization),
        }
    }

    /// PB-correct a normalized plane inside the PB support and zero it
    /// outside.
    ///
    /// # Errors
    ///
    /// A non-finite result.
    ///
    /// # Panics
    ///
    /// If `values` is not one value per sensitivity pixel.
    pub fn correct_primary_beam(
        self,
        values: &[f32],
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<Vec<f32>, ProductsError> {
        assert_eq!(
            values.len(),
            self.values.len(),
            "a plane and its sensitivity have one shape"
        );
        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let pb = self.primary_beam_at(index);
                let corrected = if Self::supported(pb, policy) {
                    f64::from(*value) / pb
                } else {
                    0.0
                } as f32;
                corrected
                    .is_finite()
                    .then_some(corrected)
                    .ok_or(ProductsError::GeneratedNonfinite)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::ProductBlankingPolicy;

    fn policy(cutoff: f32) -> PrimaryBeamValidityPolicy {
        PrimaryBeamValidityPolicy::new(
            cutoff,
            ProductSupportComparison::StrictlyGreater,
            ProductBlankingPolicy::Zero,
        )
        .expect("PB policy")
    }

    #[test]
    fn psf_normalization_is_exact_and_preserves_coupled_signed_terms() {
        for amplitude in [f32::MIN_POSITIVE, 1.000_000_7, 12345.67] {
            let principal = [-0.25 * amplitude, amplitude, 0.5 * amplitude];
            let peak = psf_peak(principal.into_iter()).unwrap();
            assert_eq!(peak, amplitude);
            assert_eq!(
                principal.map(|value| normalized_psf_value(value, peak)),
                [-0.25, 1.0, 0.5]
            );
            let coupled = [-0.5 * amplitude, 0.25 * amplitude];
            assert_eq!(
                coupled.map(|value| normalized_psf_value(value, peak)),
                [-0.5, 0.25]
            );
        }
        let peak = psf_peak([0.0; 3].into_iter()).unwrap();
        assert_eq!(normalized_psf_value(0.0, peak), 0.0);
        for nonfinite in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(psf_peak([1.0, nonfinite].into_iter()).is_err());
        }
    }

    #[test]
    fn response_support_is_separate_from_model_values_and_normalization() {
        let response = MosaicSensitivity::new(&[4.0, 1.0, 0.04, 0.0, -0.04]).unwrap();
        let pb = policy(0.2);
        assert_eq!(response.validity(pb), [true, true, false, false, false]);
        assert_eq!(response.primary_beam(), [1.0, 0.5, 0.1, 0.0, 0.1]);
        assert_eq!(
            response
                .normalize(
                    &[8.0, 4.0, 0.8, 1.0, 1.0],
                    ProductNormalization::FlatNoise,
                    pb
                )
                .unwrap(),
            [2.0, 2.0, 0.0, 0.0, 0.0]
        );
        assert_eq!(
            response
                .physical_to_apparent(4.0, 1, ProductNormalization::FlatNoise, pb)
                .unwrap(),
            2.0
        );
        assert_eq!(
            response
                .apparent_to_physical(2.0, 1, ProductNormalization::FlatNoise, pb)
                .unwrap(),
            4.0
        );
        assert_eq!(
            response
                .apparent_to_physical(2.0, 2, ProductNormalization::FlatNoise, pb)
                .unwrap(),
            0.0
        );
        assert_eq!(
            response
                .apparent_to_physical(2.0, 2, ProductNormalization::FlatSky, pb)
                .unwrap(),
            2.0
        );
        assert!(matches!(
            MinorCycleImageResponse::new(ProductNormalization::UnitResponse, pb),
            Err(ProductsError::UnsupportedNormalization)
        ));
    }

    /// CASA's `divideResidualByWeight` cutoffs, pblimit·pbmax² on
    /// pbmax·√|weight| for flat noise and pblimit²·pbmax² on the weight for
    /// flat sky, are both PB > pblimit where the weight is positive.
    #[test]
    fn flat_noise_and_flat_sky_residuals_keep_casa_support() {
        let sensitivity = [4.0, 0.2, 0.4, 0.5, -0.5, 0.15, 0.17];
        let response = MosaicSensitivity::new(&sensitivity).unwrap();
        let limit = policy(0.2);
        // PB = sqrt(|sensitivity| / 4): 1, 0.224, 0.316, 0.354, 0.354, 0.194, 0.206.
        let support = response.validity(limit);
        assert_eq!(support, [true, true, true, true, true, false, true]);
        for (normalization, expected) in [
            (ProductNormalization::FlatNoise, support.clone()),
            (
                ProductNormalization::FlatSky,
                vec![true, true, true, true, false, false, true],
            ),
        ] {
            let staged = (0..sensitivity.len())
                .map(|index| {
                    response
                        .normalize_weighted_residual_sample(2.0, index, normalization, 2.0, limit)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let raw = response.normalize(&[1.0; 7], normalization, limit).unwrap();
            for (index, valid) in expected.iter().enumerate() {
                assert_eq!(
                    staged[index] != 0.0,
                    *valid,
                    "{normalization:?} staged {index}"
                );
                assert_eq!(raw[index] != 0.0, *valid, "{normalization:?} raw {index}");
                assert!(
                    (staged[index] - f64::from(raw[index])).abs() <= 1.0e-6 * staged[index].abs(),
                    "{normalization:?} {index}: {} != {}",
                    staged[index],
                    raw[index]
                );
            }
        }
        let flat_sky = (0..5)
            .map(|index| {
                response
                    .normalize_weighted_residual_sample(
                        2.0,
                        index,
                        ProductNormalization::FlatSky,
                        2.0,
                        limit,
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(flat_sky, [0.25, 5.0, 2.5, 2.0, 0.0]);
    }

    #[test]
    fn weighted_residual_support_is_not_the_published_pb_mask() {
        let response = MosaicSensitivity::new(&[4.0, 0.2, 0.4, 0.5, -0.5]).unwrap();
        let pb = policy(0.1);
        assert!(
            response
                .weighted_primary_beam(pb)
                .unwrap()
                .iter()
                .all(|value| *value > 0.0)
        );
        let flat_noise = response
            .normalize_weighted_residual_sample(2.0, 4, ProductNormalization::FlatNoise, 2.0, pb)
            .unwrap();
        assert!((flat_noise - 1.0 / 2.0_f64.sqrt()).abs() < 1.0e-7);
        assert_eq!(
            MosaicSensitivity::new(&[1.0])
                .unwrap()
                .normalize_weighted_residual_sample(
                    1.0,
                    0,
                    ProductNormalization::FlatNoise,
                    f64::from(1.0e-7_f32),
                    pb
                )
                .unwrap(),
            10_000_000.0
        );
    }
}
