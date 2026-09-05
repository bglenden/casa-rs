// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared image-response normalization and apparent/physical model coordinates.

use casa_imaging_model::{
    PrimaryBeamValidityPolicy, ProductNormalization, ProductSupportComparison,
};

/// Failure to bind or evaluate an image-plane response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ImageResponseError {
    /// A plane does not have the response's pixel count.
    #[error("image-response payload length {actual} differs from {expected}")]
    PayloadLengthMismatch {
        /// Required response-plane length.
        expected: usize,
        /// Supplied payload length.
        actual: usize,
    },
    /// A scalar operation named a pixel outside the response plane.
    #[error("image-response pixel {index} is outside {length} pixels")]
    PixelOutOfBounds {
        /// Requested flattened pixel.
        index: usize,
        /// Response-plane length.
        length: usize,
    },
    /// The sensitivity plane has no positive support.
    #[error("image response requires positive sensitivity support")]
    UnsupportedSensitivity,
    /// Direction-dependent image coordinates require flat-noise or flat-sky.
    #[error("image response requires flat-noise or flat-sky normalization")]
    UnsupportedNormalization,
    /// Input or generated arithmetic was non-finite.
    #[error("image-response arithmetic generated a non-finite value")]
    GeneratedNonfinite,
}

/// Explicit non-persistent image-coordinate law for a minor-cycle solve.
///
/// Applications bind the requested scientific normalization and PB support to
/// the solver independently of which products will be published.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MinorCycleImageResponse {
    normalization: ProductNormalization,
    policy: PrimaryBeamValidityPolicy,
}

impl MinorCycleImageResponse {
    /// Bind CASA's flat-noise or flat-sky image coordinates and PB support.
    ///
    /// # Errors
    ///
    /// Unit-response normalization has no direction-dependent model mapping.
    pub fn new(
        normalization: ProductNormalization,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<Self, ImageResponseError> {
        if !matches!(
            normalization,
            ProductNormalization::FlatNoise | ProductNormalization::FlatSky
        ) {
            return Err(ImageResponseError::UnsupportedNormalization);
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

/// Borrowed direction-dependent sensitivity supplied by the normal-state owner.
///
/// This one numeric view derives CASA's unit-peak PB, flat-noise/flat-sky
/// normalization, PB validity, and apparent/physical model conversions. It
/// neither owns a CLEAN mask nor changes the authoritative raw normal state.
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
    /// Negative FFT ringing remains in the source plane but never supplies PB
    /// support. Empty, non-finite, or wholly unsupported state is rejected.
    pub fn new(values: &'a [f64]) -> Result<Self, ImageResponseError> {
        if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
            return Err(ImageResponseError::GeneratedNonfinite);
        }
        let peak = values.iter().copied().fold(0.0_f64, f64::max);
        if peak <= 0.0 {
            return Err(ImageResponseError::UnsupportedSensitivity);
        }
        Ok(Self {
            values,
            peak,
            normal_sum_weight: 1.0,
            model_pb_scale: (peak as f32).sqrt(),
        })
    }

    /// Bind the scalar that forms CASA's published Float weight plane.
    ///
    /// Model conversion evaluates `Float(raw / sum_weight)` before its roots
    /// and support decision. The raw sensitivity and legacy product operations
    /// remain unchanged, and no normalized weight plane is allocated.
    pub fn with_normal_sum_weight(mut self, sum_weight: f64) -> Result<Self, ImageResponseError> {
        if !sum_weight.is_finite() || sum_weight <= 0.0 {
            return Err(ImageResponseError::UnsupportedSensitivity);
        }
        let scale = ((self.peak / sum_weight) as f32).sqrt();
        if !scale.is_finite() || scale <= 0.0 {
            return Err(ImageResponseError::UnsupportedSensitivity);
        }
        self.normal_sum_weight = sum_weight;
        self.model_pb_scale = scale;
        Ok(self)
    }

    fn value(self, index: usize) -> Result<f64, ImageResponseError> {
        self.values
            .get(index)
            .copied()
            .ok_or(ImageResponseError::PixelOutOfBounds {
                index,
                length: self.values.len(),
            })
    }

    fn finite(value: f64) -> Result<f64, ImageResponseError> {
        value
            .is_finite()
            .then_some(value)
            .ok_or(ImageResponseError::GeneratedNonfinite)
    }

    fn supported(pb: f64, policy: PrimaryBeamValidityPolicy) -> bool {
        match policy.comparison() {
            ProductSupportComparison::StrictlyGreater => pb > f64::from(policy.cutoff()),
        }
    }

    /// Return one unit-peak primary-beam response.
    pub fn primary_beam_at(self, index: usize) -> Result<f64, ImageResponseError> {
        Ok((self.value(index)?.max(0.0) / self.peak).sqrt())
    }

    /// Return whether a pixel is inside the separate PB support.
    pub fn valid_at(
        self,
        index: usize,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<bool, ImageResponseError> {
        Ok(Self::supported(self.primary_beam_at(index)?, policy))
    }

    fn model_primary_beam_at(self, index: usize) -> Result<f64, ImageResponseError> {
        let weight = (self.value(index)? / self.normal_sum_weight) as f32;
        let pb = weight.abs().sqrt() / self.model_pb_scale;
        Self::finite(f64::from(pb))
    }

    pub(crate) fn model_valid_at(
        self,
        index: usize,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<bool, ImageResponseError> {
        Ok(Self::supported(self.model_primary_beam_at(index)?, policy))
    }

    /// Build CASA's unit-peak PB, `sqrt(max(sensitivity,0) / peak)`.
    #[must_use]
    pub fn primary_beam(self) -> Vec<f32> {
        self.values
            .iter()
            .map(|value| (value.max(0.0) / self.peak).sqrt() as f32)
            .collect()
    }

    /// Derive PB from a published CASA Float weight plane, retaining Float roots.
    pub fn primary_beam_from_weight(weight: &[f32]) -> Result<Vec<f32>, ImageResponseError> {
        if weight.is_empty() || weight.iter().any(|value| !value.is_finite()) {
            return Err(ImageResponseError::GeneratedNonfinite);
        }
        let peak = weight.iter().copied().fold(0.0_f32, f32::max);
        if peak <= 0.0 {
            return Err(ImageResponseError::UnsupportedSensitivity);
        }
        let scale = peak.sqrt();
        Ok(weight
            .iter()
            .map(|value| value.max(0.0).sqrt() / scale)
            .collect())
    }

    /// Normalize one raw image value by sensitivity or its flat-noise envelope.
    ///
    /// This operation does not apply PB cutoff; support remains a separate law.
    pub fn normalize_sample(
        self,
        value: f64,
        index: usize,
        normalization: ProductNormalization,
    ) -> Result<f64, ImageResponseError> {
        let sensitivity = self.value(index)?;
        let denominator = match normalization {
            ProductNormalization::FlatNoise => (sensitivity * self.peak).sqrt(),
            ProductNormalization::FlatSky => sensitivity,
            ProductNormalization::UnitResponse => {
                return Err(ImageResponseError::UnsupportedNormalization);
            }
        };
        if denominator > 0.0 {
            Self::finite(value / denominator)
        } else {
            Ok(0.0)
        }
    }

    /// Normalize a complete raw plane without changing its separate PB mask.
    pub fn normalize(
        self,
        values: &[f32],
        normalization: ProductNormalization,
    ) -> Result<Vec<f32>, ImageResponseError> {
        if values.len() != self.values.len() {
            return Err(ImageResponseError::PayloadLengthMismatch {
                expected: self.values.len(),
                actual: values.len(),
            });
        }
        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let normalized =
                    self.normalize_sample(f64::from(*value), index, normalization)? as f32;
                normalized
                    .is_finite()
                    .then_some(normalized)
                    .ok_or(ImageResponseError::GeneratedNonfinite)
            })
            .collect()
    }

    /// Derive PB validity independently of CLEAN-mask and model validity.
    #[must_use]
    pub fn validity(self, policy: PrimaryBeamValidityPolicy) -> Vec<bool> {
        self.values
            .iter()
            .map(|value| Self::supported((value.max(0.0) / self.peak).sqrt(), policy))
            .collect()
    }

    /// Map a physical flat-sky model pixel to its displayed/solve coordinates.
    ///
    /// CASA flat-noise multiplies by PB and projects its support; flat-sky is
    /// the identity. Apply this before restoring-beam convolution.
    pub fn physical_to_apparent(
        self,
        value: f64,
        index: usize,
        normalization: ProductNormalization,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<f64, ImageResponseError> {
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
            ProductNormalization::UnitResponse => Err(ImageResponseError::UnsupportedNormalization),
        }
    }

    /// Map a solve-coordinate pixel to the physical model consumed by prediction.
    ///
    /// Apply to every pixel after multiscale expansion, never only to the
    /// component centre. PB support is not the CLEAN mask.
    pub fn apparent_to_physical(
        self,
        value: f64,
        index: usize,
        normalization: ProductNormalization,
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<f64, ImageResponseError> {
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
            ProductNormalization::UnitResponse => Err(ImageResponseError::UnsupportedNormalization),
        }
    }

    /// PB-correct a normalized plane on its exact PB-valid support.
    pub fn correct_primary_beam(
        self,
        values: &[f32],
        policy: PrimaryBeamValidityPolicy,
    ) -> Result<Vec<f32>, ImageResponseError> {
        if values.len() != self.values.len() {
            return Err(ImageResponseError::PayloadLengthMismatch {
                expected: self.values.len(),
                actual: values.len(),
            });
        }
        values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let pb = self.primary_beam_at(index)?;
                let corrected = if Self::supported(pb, policy) {
                    f64::from(*value) / pb
                } else {
                    0.0
                } as f32;
                corrected
                    .is_finite()
                    .then_some(corrected)
                    .ok_or(ImageResponseError::GeneratedNonfinite)
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
            ProductBlankingPolicy::ZeroAndFalseMask,
        )
        .expect("PB policy")
    }

    #[test]
    fn t51_response_support_is_separate_from_model_values_and_normalization() {
        let response = MosaicSensitivity::new(&[4.0, 1.0, 0.04, 0.0, -0.04]).unwrap();
        let pb = policy(0.2);
        assert_eq!(response.validity(pb), [true, true, false, false, false]);
        assert_eq!(
            response
                .normalize(&[8.0, 4.0, 0.8, 1.0, 1.0], ProductNormalization::FlatNoise)
                .unwrap(),
            [2.0, 2.0, 2.0, 0.0, 0.0]
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
        assert_eq!(
            MinorCycleImageResponse::new(ProductNormalization::UnitResponse, pb),
            Err(ImageResponseError::UnsupportedNormalization)
        );
    }

    #[test]
    #[ignore = "requires actual native first-three-step and nonzero-baseline scale12 model conversions"]
    fn t51_native_model_weight_conversion_matches_shared_response() {
        let root = std::path::PathBuf::from(
            std::env::var_os("CASA_RS_T51_NATIVE_MODEL_WEIGHT_FIXTURE")
                .expect("native model-weight fixture path"),
        );
        let load = |name: &str| {
            let bytes = std::fs::read(root.join(name)).expect("native Float plane");
            assert_eq!(bytes.len(), 512 * 512 * 4);
            bytes
                .chunks_exact(4)
                .map(|bytes| f64::from(f32::from_le_bytes(bytes.try_into().unwrap())))
                .collect::<Vec<_>>()
        };
        let weight = load("weight0.f32le");
        let response = MosaicSensitivity::new(&weight).unwrap();
        let pb = policy(0.0001);
        let nrms = |actual: &[f64], expected: &[f64]| {
            (actual
                .iter()
                .zip(expected)
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f64>()
                / expected.iter().map(|value| value.powi(2)).sum::<f64>())
            .sqrt()
        };
        for term in 0..2 {
            for prefix in ["native_first3", "baseline_scale12"] {
                let apparent = load(&format!("{prefix}_apparent{term}.f32le"));
                let expected_physical = load(&format!("{prefix}_physical{term}.f32le"));
                let physical = apparent
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        response
                            .apparent_to_physical(
                                *value,
                                index,
                                ProductNormalization::FlatNoise,
                                pb,
                            )
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                let expected_roundtrip = load(&format!("{prefix}_roundtrip{term}.f32le"));
                let roundtrip = physical
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        response
                            .physical_to_apparent(
                                *value,
                                index,
                                ProductNormalization::FlatNoise,
                                pb,
                            )
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                let physical_error = nrms(&physical, &expected_physical);
                let roundtrip_error = nrms(&roundtrip, &expected_roundtrip);
                eprintln!(
                    "native_model_weight {prefix} tt{term} physical_nrms={physical_error:.17e} roundtrip_nrms={roundtrip_error:.17e}"
                );
                assert!(physical_error <= 1.0e-3);
                assert!(roundtrip_error <= 1.0e-3);
            }
            let baseline = load(&format!("baseline_physical{term}.f32le"));
            let expected = load(&format!("baseline_apparent{term}.f32le"));
            let apparent = baseline
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    response
                        .physical_to_apparent(*value, index, ProductNormalization::FlatNoise, pb)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let error = nrms(&apparent, &expected);
            eprintln!("native_model_weight nonzero_baseline tt{term} apparent_nrms={error:.17e}");
            assert!(error <= 1.0e-3);
        }
    }

    #[test]
    #[ignore = "requires the actual native Float cutoff-edge model conversion capture"]
    fn t51_native_model_weight_cutoff_edges_match_shared_response() {
        let root = std::path::PathBuf::from(
            std::env::var_os("CASA_RS_T51_NATIVE_MODEL_WEIGHT_EDGES")
                .expect("native cutoff-edge fixture"),
        );
        let load = |name: &str| {
            let bytes = std::fs::read(root.join(name)).expect("native edge Float plane");
            assert_eq!(bytes.len(), 512 * 512 * 4);
            bytes
                .chunks_exact(4)
                .map(|bytes| f64::from(f32::from_le_bytes(bytes.try_into().unwrap())))
                .collect::<Vec<_>>()
        };
        let weights = load("edges_weight0.f32le");
        let raw = weights.iter().map(|value| value * 17.0).collect::<Vec<_>>();
        let response = MosaicSensitivity::new(&raw)
            .unwrap()
            .with_normal_sum_weight(17.0)
            .unwrap();
        let pb = policy(0.0001);
        let expected_support = load("edges_expected_support.f32le");
        for (index, expected) in expected_support.iter().enumerate() {
            assert_eq!(
                response.model_valid_at(index, pb).unwrap(),
                *expected != 0.0,
                "native model cutoff at pixel {index}"
            );
        }
        for term in 0..2 {
            for (input_name, output_name, divide) in [
                (
                    format!("edges_apparent{term}.f32le"),
                    format!("edges_physical{term}.f32le"),
                    true,
                ),
                (
                    format!("edges_physical_input{term}.f32le"),
                    format!("edges_apparent_from_physical{term}.f32le"),
                    false,
                ),
            ] {
                let input = load(&input_name);
                let expected = load(&output_name);
                let mut difference = 0.0;
                let mut reference = 0.0;
                for (index, (value, expected)) in input.iter().zip(&expected).enumerate() {
                    let actual = if divide {
                        response.apparent_to_physical(
                            *value,
                            index,
                            ProductNormalization::FlatNoise,
                            pb,
                        )
                    } else {
                        response.physical_to_apparent(
                            *value,
                            index,
                            ProductNormalization::FlatNoise,
                            pb,
                        )
                    }
                    .unwrap();
                    assert_eq!(
                        actual == 0.0,
                        *expected == 0.0,
                        "native projected support at pixel {index}"
                    );
                    difference += (actual - expected).powi(2);
                    reference += expected.powi(2);
                }
                let nrms = (difference / reference).sqrt();
                eprintln!("native_model_weight_edges term={term} divide={divide} nrms={nrms:.17e}");
                assert!(nrms <= 1.0e-3);
            }
        }
    }
}
