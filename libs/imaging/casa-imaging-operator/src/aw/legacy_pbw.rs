// SPDX-License-Identifier: LGPL-3.0-or-later
//! Prediction taps from the legacy nPBWProjectFT cache.

use std::sync::{Arc, Mutex};

use ndarray::ArrayD;
use num_complex::{Complex32, Complex64};

use crate::convolution::{
    CellHold, ConvolutionFunctionSet, DenseCell, ImageCorrection, KernelNormalisation,
    MuellerRouting, RowContext, TapFootprint, TapLayout,
};
use crate::error::OperatorError;
use crate::geometry::{CellLocation, GridGeometry};
use crate::sample::CfKey;

/// Correlation-ordered taps for an nPBWProjectFT prediction already
/// corrected by the associated average primary beam.
pub struct LegacyPbWConvolution {
    samples: ArrayD<Complex32>,
    sampling: u16,
    support: Vec<u16>,
    max_support: u16,
    w_scale: f64,
    reference_angle_rad: f64,
    antenna_pointing: bool,
    cache: Mutex<Vec<Option<RotatedCell>>>,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

struct RotatedCell {
    key: CfKey,
    cell: Arc<DenseCell>,
}

impl LegacyPbWConvolution {
    /// Validate a cache image before reading or publishing it.
    ///
    /// # Errors
    /// The shape does not cover the offsets needed by its sampling and support.
    pub fn validate_shape(
        shape: &[usize],
        sampling: u16,
        support: &[u16],
        correlations: usize,
    ) -> Result<(), OperatorError> {
        let [side, ny, count, planes] = *shape else {
            return Err(invalid("expected a four-axis legacy PB/W kernel"));
        };
        let maximum = support.iter().copied().max().unwrap_or(0);
        let radius = usize::from(maximum) * usize::from(sampling)
            + usize::from(CellLocation::fine_centre(sampling));
        if side != ny
            || count != correlations
            || !(1..=4).contains(&count)
            || planes != support.len()
            || !(1..=usize::from(u16::MAX)).contains(&planes)
            || sampling == 0
            || maximum > u16::MAX / 2
            || radius >= side.div_ceil(2)
        {
            return Err(invalid(
                "the legacy PB/W kernel cannot cover its fine sampling",
            ));
        }
        Ok(())
    }

    /// Bound this layout's retained cache image, correction, and rotating
    /// cells, including a former cell still borrowed by the CPU prediction.
    #[must_use]
    pub fn resident_bytes(
        kernel_side: usize,
        correlations: usize,
        sampling: u16,
        support: &[u16],
        grid: [usize; 2],
    ) -> u64 {
        let image_bytes = (kernel_side as u64)
            .saturating_pow(2)
            .saturating_mul(correlations as u64)
            .saturating_mul(support.len() as u64)
            .saturating_mul(size_of::<Complex32>() as u64);
        let rows = CellLocation::fine_rows(sampling) as u64;
        let tiles = rows * rows * correlations as u64;
        let cell = |half_support: u16| {
            let taps = 2 * u64::from(half_support) + 1;
            tiles
                .saturating_mul(
                    taps * taps * size_of::<Complex32>() as u64 + size_of::<Complex64>() as u64,
                )
                .saturating_add((size_of::<DenseCell>() + 2 * size_of::<usize>()) as u64)
        };
        let rotated_bytes = support
            .iter()
            .copied()
            .filter(|&support| support != 0)
            .fold(0_u64, |bytes, support| bytes.saturating_add(cell(support)));
        let largest = support.iter().copied().max().map_or(0, cell);
        image_bytes
            .saturating_add(rotated_bytes)
            .saturating_add(largest)
            .saturating_add(
                (support.len() as u64).saturating_mul(
                    size_of::<Option<RotatedCell>>() as u64 + size_of::<u16>() as u64,
                ),
            )
            .saturating_add(MuellerRouting::MAXIMUM_BYTES)
            .saturating_add(ImageCorrection::bytes(grid))
            .saturating_add(4096)
    }

    /// Take ownership of the cache convolution and its visibility-buffer
    /// reference angle. The Fourier model already has its beam correction.
    ///
    /// # Errors
    /// The image or correlation layout is incompatible with its support.
    pub fn new(
        samples: ArrayD<Complex32>,
        sampling: u16,
        support: Vec<u16>,
        geometry: &GridGeometry,
        grid_pols: usize,
        pol_map: &[Option<u8>],
        reference_angle_rad: f64,
    ) -> Result<Self, OperatorError> {
        Self::validate_shape(samples.shape(), sampling, &support, pol_map.len())?;
        if !(1..=4).contains(&grid_pols)
            || !reference_angle_rad.is_finite()
            || pol_map
                .iter()
                .flatten()
                .any(|&pol| usize::from(pol) >= grid_pols)
        {
            return Err(invalid("invalid legacy PB/W polarization or angle"));
        }
        let direct = (0..grid_pols)
            .map(|grid_pol| {
                pol_map
                    .iter()
                    .enumerate()
                    .map(|(pol, &target)| (target == Some(grid_pol as u8)).then_some(pol as u8))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mueller = MuellerRouting {
            adjoint: [direct.clone(), direct.clone()],
            forward: [direct.clone(), direct],
        };
        let [nx, ny] = geometry.grid_shape();
        let correction = ImageCorrection::new(vec![1.; nx], vec![1.; ny]);
        let cache = (0..support.len()).map(|_| None).collect();
        Ok(Self {
            samples,
            sampling,
            max_support: support.iter().copied().max().unwrap_or(0),
            w_scale: support.len() as f64 * geometry.image().increment_rad[0].abs(),
            support,
            reference_angle_rad,
            antenna_pointing: false,
            cache: Mutex::new(cache),
            mueller,
            correction,
        })
    }

    /// Include the antenna pointing phase in prediction and its kernel sum.
    #[must_use]
    pub fn with_antenna_pointing(mut self, enabled: bool) -> Self {
        self.antenna_pointing = enabled;
        self
    }

    /// Restore the W coordinate of a transform saved during cache generation.
    /// # Errors
    /// The scale must be finite and positive.
    pub fn with_w_scale(mut self, scale: f64) -> Result<Self, OperatorError> {
        if !scale.is_finite() || scale <= 0. {
            return Err(invalid("invalid legacy PB/W scale"));
        }
        self.w_scale = scale;
        Ok(self)
    }

    fn rotation(&self, key: CfKey) -> (f64, f64) {
        let bits = (u32::from(key.group) << 16) | u32::from(key.cube);
        let angle = f64::from(f32::from_bits(bits)) - self.reference_angle_rad;
        angle.sin_cos()
    }

    fn plane(&self, w_lambda: f64) -> Option<usize> {
        // CASA rounds the one-based square-root coordinate in Float.
        let location = (1. + (self.w_scale * w_lambda.abs()).sqrt()) as f32;
        location.is_finite().then(|| location.round() as usize - 1)
    }

    fn support(&self, key: CfKey) -> u16 {
        self.support[usize::from(key.w_plane)]
    }

    fn make_cell(&self, key: CfKey) -> DenseCell {
        let (sin, cos) = self.rotation(key);
        let side = self.samples.shape()[0] as i64;
        let correlations = self.samples.shape()[2];
        let origin = side / 2;
        let rows = CellLocation::fine_rows(self.sampling);
        let centre = i64::from(CellLocation::fine_centre(self.sampling));
        let sampling = i64::from(self.sampling);
        let half_support = i64::from(self.support(key));
        let taps = 2 * self.support(key) + 1;
        let mut data = Vec::with_capacity(rows * rows * correlations * usize::from(taps).pow(2));
        for oy in 0..rows {
            for ox in 0..rows {
                for pol in 0..correlations {
                    for ky in -half_support..=half_support {
                        let y = (ky * sampling + oy as i64 - centre) as f64;
                        for kx in -half_support..=half_support {
                            let x = (kx * sampling + ox as i64 - centre) as f64;
                            // CASA reindex rotates the *fine* coordinate, including
                            // the sample's fractional offset, before rounding.
                            let ix = origin + (cos * x + sin * y).round() as i64;
                            let iy = origin + (-sin * x + cos * y).round() as i64;
                            let value = if (0..side).contains(&ix) && (0..side).contains(&iy) {
                                self.samples
                                    [[ix as usize, iy as usize, pol, usize::from(key.w_plane)]]
                            } else {
                                Complex32::default()
                            };
                            data.push(value);
                        }
                    }
                }
            }
        }
        DenseCell::new(
            data.into_boxed_slice(),
            [taps; 2],
            self.sampling,
            correlations as u8,
        )
    }
}

impl ConvolutionFunctionSet for LegacyPbWConvolution {
    fn key(&self, row: &RowContext, _frequency_hz: f64, w_lambda: f64) -> CfKey {
        let actual = (-row.parallactic_angle_rad[0]) as f32;
        let angle = f64::from(actual) - self.reference_angle_rad;
        let (sin, cos) = angle.sin_cos();
        let plane = self
            .plane(w_lambda)
            .filter(|&index| index < self.support.len())
            .unwrap_or(0);
        let radius = f64::from(self.support[plane]) * f64::from(self.sampling)
            + f64::from(CellLocation::fine_centre(self.sampling));
        // Every rounded fine pixel stays put within this angle range.
        let bits = if (sin.abs() + (1. - cos)) * radius < 0.5 {
            (self.reference_angle_rad as f32).to_bits()
        } else {
            actual.to_bits()
        };
        CfKey {
            w_plane: plane as u16,
            ..CfKey::new((bits >> 16) as u16, bits as u16)
        }
    }

    fn taps<'s>(&'s self, key: CfKey, hold: &'s mut CellHold) -> TapLayout<'s> {
        hold.lend_keyed(key, 0, || {
            let mut cache = self.cache.lock().expect("legacy PB/W cell lock");
            let slot = &mut cache[usize::from(key.w_plane)];
            if let Some(stored) = slot.as_ref()
                && stored.key == key
            {
                return Arc::clone(&stored.cell);
            }
            *slot = None;
            let cell = Arc::new(self.make_cell(key));
            *slot = Some(RotatedCell {
                key,
                cell: Arc::clone(&cell),
            });
            cell
        })
    }

    fn gridding_oversampling(&self, _key: CfKey) -> u16 {
        self.sampling
    }

    fn gridding_half_support(&self, key: CfKey) -> [u16; 2] {
        [self.support(key); 2]
    }

    fn max_half_support(&self) -> [u16; 2] {
        [self.max_support; 2]
    }

    fn placement_half_support(&self, key: CfKey, _hold: &mut CellHold) -> [u16; 2] {
        [self.support(key); 2]
    }

    fn weight_taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        None
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn prediction_conjugates(&self, _key: CfKey, _w_positive: bool) -> bool {
        true
    }

    fn admits(&self, w_lambda: f64) -> bool {
        self.plane(w_lambda)
            .and_then(|plane| self.support.get(plane))
            .is_some_and(|&support| support != 0)
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }

    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::KernelSum
    }

    fn pointing_ramp(&self) -> bool {
        self.antenna_pointing
    }

    fn prediction_pointing_rotation(&self, key: CfKey) -> Option<(f64, f64)> {
        self.antenna_pointing.then(|| self.rotation(key))
    }

    fn tap_footprint(&self) -> TapFootprint {
        let tiles =
            (CellLocation::fine_rows(self.sampling) as u64).pow(2) * self.samples.shape()[2] as u64;
        TapFootprint {
            tap_bytes: tiles
                * (2 * u64::from(self.max_support) + 1).pow(2)
                * size_of::<Complex32>() as u64,
            norm_bytes: tiles * size_of::<Complex64>() as u64,
            cells: 1,
        }
    }
}

fn invalid(reason: &'static str) -> OperatorError {
    OperatorError::ConvolutionFunction { reason }
}
