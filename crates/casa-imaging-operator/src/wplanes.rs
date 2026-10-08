// SPDX-License-Identifier: LGPL-3.0-or-later
//! The W-projection kernel set, pinned to CASA `WPConvFunc::findConvFunction`
//! (`TransformMachines2/WPConvFunc.cc`), `WProjectFT` and `wprojgrid.f`.

use num_complex::{Complex32, Complex64};

use crate::convolution::{
    CellHold, ConvolutionFunctionSet, DenseCell, ImageCorrection, KernelNormalisation,
    MuellerRouting, RowContext, TapLayout,
};
use crate::dense::dense_cell_from_quadrant;
use crate::error::OperatorError;
use crate::fft::PlaneFft;
use crate::geometry::{GridGeometry, next_larger_even_composite};
use crate::polarization::PolarizationRouting;
use crate::sample::CfKey;
use crate::spheroidal::grdsf;

/// CASA `WPConvFunc`: fine offsets per cell when there is more than one
/// plane (`convSampling_p = 4`).
const W_OVERSAMPLING: u16 = 4;
/// CASA `WProjectFT` grid padding (`padding_p`, tclean's 1.2).
const PADDING: f64 = 1.2;
/// `WPConvFunc`: a plane's support reaches the last fine pixel whose
/// modulus exceeds this fraction of the peak-normalised plane.
const SUPPORT_THRESHOLD: f64 = 1.0e-3;

/// How many w-planes to build.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum WPlaneCount {
    /// `wprojplanes` given by the request: the largest `|w|` the planes
    /// reach is `0.25 / |Δx|` wavelengths (`WPConvFunc::findConvFunction`
    /// with `maxW_p = −1`, as tclean constructs it).
    Fixed(u32),
    /// `wprojplanes = −1`: the count follows CASA's `wStat` of the selected
    /// data at the top frequency.
    Auto {
        /// `min |w| · ν_min / c` over the selection, in wavelengths.
        min_w: f64,
        /// `max |w| · ν_max / c` over the selection, in wavelengths.
        max_w: f64,
        /// `√(mean w²) · ν_max / c` over the selection, in wavelengths.
        rms_w: f64,
    },
}

/// The W-projection kernel set: one dense complex cell per w-plane,
/// `C ⋆ FT[e^{2πi w(√(1−l²−m²)−1)}]` sampled four times per cell, scalar
/// Mueller routing and the paired `1/(grdsf · sinc)` correction.
#[derive(Clone, Debug)]
pub struct WPlanes {
    planes: Vec<DenseCell>,
    w_scale: f64,
    max_half_support: u16,
    mueller: MuellerRouting,
    correction: ImageCorrection,
}

impl WPlanes {
    /// Build the planes for `geometry`'s image on its CASA-padded grid.
    ///
    /// # Errors
    ///
    /// The plane count resolves to zero, the FFT cannot be planned, or the
    /// plane-0 kernel has no positive integral.
    pub fn new(
        geometry: &GridGeometry,
        polarization: &PolarizationRouting,
        count: WPlaneCount,
    ) -> Result<Self, OperatorError> {
        let image = geometry.image();
        let [nx, ny] = image.shape;
        let increment = image.increment_rad;
        // `WPConvFunc::findConvFunction`: the plane count and the largest
        // |w| the planes reach.
        let (planes, max_uvw) = match count {
            WPlaneCount::Fixed(planes) => (planes as usize, 0.25 / increment[0].abs()),
            WPlaneCount::Auto {
                min_w,
                max_w,
                rms_w,
            } => {
                let mean = 0.5 * (min_w + max_w);
                let max_uvw = if rms_w < mean {
                    1.05 * max_w
                } else {
                    rms_w / mean * 1.05 * max_w
                };
                let half_fov = increment[0].abs() * nx.max(ny) as f64 / 2.0;
                let planes = (max_uvw * half_fov.sin().abs()) as usize;
                (planes, max_uvw)
            }
        };
        if planes == 0 {
            return Err(OperatorError::ConvolutionFunction {
                reason: "W projection needs at least one plane",
            });
        }
        // `wScale = Float((wConvSize-1)*(wConvSize-1))/maxUVW`.
        let w_scale = f64::from(((planes - 1) * (planes - 1)) as f32) / max_uvw;
        let sampling = if planes > 1 { W_OVERSAMPLING } else { 1 };
        // `convSize = nextLargerEven(max(Int(nx*padding), Int(ny*padding)))`;
        // `CompositeNumber::nextLargerEven` returns the first even composite
        // strictly above its argument, so a padded size that is itself
        // composite (1200 → 1440) steps up (to 1458); the grid keeps 1440
        // because `FTMachine` asks for the composite above `Int(1.2 n − 0.5)`.
        // CASA keeps the raw size for one plane, which may be odd, so the
        // one-plane case takes the same even composite size here.
        let padded = ((nx as f64 * PADDING) as usize).max((ny as f64 * PADDING) as usize);
        let conv_size = next_larger_even_composite(padded + 1);
        let inner = conv_size / usize::from(sampling);
        // The screen's sky sampling: `incr · convSampling · padding·n/convSize`.
        let screen_increment = [
            increment[0] * f64::from(sampling) * PADDING * nx as f64 / conv_size as f64,
            increment[1] * f64::from(sampling) * PADDING * ny as f64 / conv_size as f64,
        ];
        let quadrant_side = conv_size / 2 - 1;
        let correction_axis = |len: usize| {
            (0..len)
                .map(|index| grdsf((index as f64 - (len / 2) as f64).abs() / (len / 2) as f64))
                .collect::<Vec<_>>()
        };
        let taper = correction_axis(inner);
        let mut fft = PlaneFft::<f64>::new([conv_size, conv_size], false)?;
        let mut screen = vec![Complex64::default(); conv_size * conv_size];
        let mut quadrants = Vec::with_capacity(planes);
        let mut peak = 0.0;
        for plane in 0..planes {
            // Plane `i` holds `w = i²/wScale`; with one plane CASA's
            // `wScale` is zero and `makeGWplane` divides zero by zero, so
            // the one-plane set takes the zero-w kernel instead.
            let w = if planes > 1 {
                (plane * plane) as f64 / w_scale
            } else {
                0.0
            };
            make_w_plane(&mut screen, conv_size, inner, screen_increment, &taper, w);
            fft.transform(&mut screen, false)?;
            let centre = conv_size / 2;
            if plane == 0 {
                peak = screen[centre * conv_size + centre].norm();
                if !peak.is_finite() || peak <= 0.0 {
                    return Err(OperatorError::ConvolutionFunction {
                        reason: "the W-projection plane-0 kernel has no peak",
                    });
                }
            }
            let mut quadrant = Vec::with_capacity(quadrant_side * quadrant_side);
            for y in 0..quadrant_side {
                for x in 0..quadrant_side {
                    let value = screen[(centre + y) * conv_size + centre + x] / peak;
                    quadrant.push(Complex32::new(value.re as f32, value.im as f32));
                }
            }
            quadrants.push(quadrant);
        }
        // Support per plane from the peak-normalised quadrant.
        let fallback = conv_size / 2 / usize::from(sampling) - 1;
        let supports = quadrants
            .iter()
            .map(|quadrant| {
                let mut support = fallback;
                for trial in (1..=conv_size / 2 - 2).rev() {
                    if f64::from(quadrant[trial].norm()) > SUPPORT_THRESHOLD
                        || f64::from(quadrant[trial * quadrant_side].norm()) > SUPPORT_THRESHOLD
                    {
                        support = (0.5 + trial as f64 / f64::from(sampling)) as usize + 1;
                        if support * usize::from(sampling) * 2 >= conv_size {
                            support = fallback;
                        }
                        break;
                    }
                }
                support
            })
            .collect::<Vec<_>>();
        // Crop every plane to the last plane's support and normalise all
        // of them by the plane-0 integral over its support.
        let last_support = *supports.last().expect("at least one plane");
        let cropped = (2 * (last_support + 2) * usize::from(sampling)).min(conv_size);
        let cropped_side = cropped / 2 - 1;
        let mut integral = 0.0;
        for iy in -(supports[0] as i64)..=supports[0] as i64 {
            for ix in -(supports[0] as i64)..=supports[0] as i64 {
                let x = ix.unsigned_abs() as usize * usize::from(sampling);
                let y = iy.unsigned_abs() as usize * usize::from(sampling);
                integral += f64::from(quadrants[0][y * quadrant_side + x].re);
            }
        }
        if !integral.is_finite() || integral <= 0.0 {
            return Err(OperatorError::ConvolutionFunction {
                reason: "the W-projection convolution function integral is not positive",
            });
        }
        let scale = (1.0 / integral) as f32;
        let cells = quadrants
            .iter()
            .zip(&supports)
            .map(|(quadrant, support)| {
                let mut cropped_quadrant = Vec::with_capacity(cropped_side * cropped_side);
                for y in 0..cropped_side {
                    for x in 0..cropped_side {
                        cropped_quadrant.push(quadrant[y * quadrant_side + x] * scale);
                    }
                }
                let half_support = u16::try_from(*support).expect("support fits u16");
                dense_cell_from_quadrant(&cropped_quadrant, cropped_side, sampling, half_support)
            })
            .collect::<Vec<_>>();
        // `WProjectFT`'s sinc of the oversampled kernel cell: `getImage`
        // tabulates it over `max(nx, ny)` for both axes and divides the
        // image by `correctX1D · sinc`; `initializeToVis` tabulates it per
        // axis and divides the model by `correctX1D / sinc`, so the model is
        // multiplied by the sinc the image is divided by (the `MosaicFT`
        // asymmetry).
        let [grid_nx, grid_ny] = geometry.grid_shape();
        let sinc = |len: usize, index: usize| {
            let x = std::f64::consts::PI * (index as f64 - (len / 2) as f64)
                / (len as f64 * f64::from(sampling));
            if index == len / 2 { 1.0 } else { x.sin() / x }
        };
        let spheroidal = |len: usize, index: usize| {
            let nu = ((index as f64 - (len / 2) as f64).abs() / (len / 2) as f64).clamp(0.0, 1.0);
            grdsf(nu)
        };
        let sinc_len = grid_nx.max(grid_ny);
        let image_correction = |len: usize| {
            (0..len)
                .map(|index| {
                    let value = spheroidal(len, index) * sinc(sinc_len, index);
                    if value.abs() > 1.0e-6 {
                        1.0 / value
                    } else {
                        0.0
                    }
                })
                .collect::<Vec<_>>()
        };
        let model_correction = |len: usize| {
            (0..len)
                .map(|index| {
                    let value = spheroidal(len, index);
                    if value.abs() > 1.0e-6 {
                        sinc(len, index) / value
                    } else {
                        0.0
                    }
                })
                .collect::<Vec<_>>()
        };
        Ok(Self {
            max_half_support: u16::try_from(last_support).expect("support fits u16"),
            planes: cells,
            w_scale,
            mueller: MuellerRouting::scalar(polarization.pol_map(), polarization.grid_pols()),
            correction: ImageCorrection::split(
                image_correction(grid_nx),
                image_correction(grid_ny),
                model_correction(grid_nx),
                model_correction(grid_ny),
            ),
        })
    }

    /// Number of planes.
    #[must_use]
    pub fn planes(&self) -> usize {
        self.planes.len()
    }

    /// CASA `wScale`: plane `i` holds `w = i² / wScale`.
    #[must_use]
    pub const fn w_scale(&self) -> f64 {
        self.w_scale
    }

    /// The plane of `w_lambda`: `nint(√(wScale · |w|))` (`wprojgrid.f`
    /// `swp`), clamped to the planes for a key; a row beyond the last plane
    /// is not gridded ([`Self::admits`]).
    #[must_use]
    pub fn plane_of(&self, w_lambda: f64) -> usize {
        self.plane_index(w_lambda).min(self.planes.len() - 1)
    }

    /// `nint(√(wScale · |w|))` before clamping.
    fn plane_index(&self, w_lambda: f64) -> usize {
        (self.w_scale * w_lambda.abs()).sqrt().round() as usize
    }

    /// Whether `wprojgrid.f` grids a row at `w_lambda`: `owp` rejects a
    /// plane index past the last plane (`loc(3) > wconvsize`).
    #[must_use]
    pub fn admits(&self, w_lambda: f64) -> bool {
        self.plane_index(w_lambda) < self.planes.len()
    }

    /// Half support of plane `plane`.
    #[must_use]
    pub fn half_support(&self, plane: usize) -> u16 {
        self.planes[plane].support[0] / 2
    }

    /// Bytes of the tap values.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.planes.iter().map(DenseCell::bytes).sum()
    }
}

impl ConvolutionFunctionSet for WPlanes {
    fn key(&self, _row: &RowContext, _freq_hz: f64, w_lambda: f64) -> CfKey {
        CfKey {
            group: u16::try_from(self.plane_of(w_lambda)).expect("plane index fits u16"),
            cube: 0,
        }
    }

    fn taps<'s>(&'s self, key: CfKey, _hold: &'s mut CellHold) -> TapLayout<'s> {
        self.planes[usize::from(key.group)].layout()
    }

    fn max_half_support(&self) -> [u16; 2] {
        [self.max_half_support; 2]
    }

    fn weight_taps<'s>(&'s self, _key: CfKey, _hold: &'s mut CellHold) -> Option<TapLayout<'s>> {
        None
    }

    fn mueller(&self) -> &MuellerRouting {
        &self.mueller
    }

    fn image_correction(&self) -> &ImageCorrection {
        &self.correction
    }

    /// `wprojgrid.f`: `norm += real(cwt)`, `sumwt += weight · norm`; the
    /// degrid does not divide.
    fn normalisation(&self) -> KernelNormalisation {
        KernelNormalisation::RealSum
    }

    fn pointing_ramp(&self) -> bool {
        false
    }

    fn admits(&self, w_lambda: f64) -> bool {
        Self::admits(self, w_lambda)
    }
}

/// `WPConvFunc::makeGWplane`: the taper times the w-screen over the inner
/// `inner × inner` pixels of a centred `conv_size × conv_size` plane, zero
/// outside; `w` in wavelengths.
fn make_w_plane(
    screen: &mut [Complex64],
    conv_size: usize,
    inner: usize,
    increment: [f64; 2],
    taper: &[f64],
    w: f64,
) {
    screen.fill(Complex64::default());
    let centre = (conv_size / 2) as i64;
    let half = (inner / 2) as i64;
    let two_pi_w = std::f64::consts::TAU * w;
    for iy in -half..half {
        let m = increment[1] * iy as f64;
        let m_sq = m * m;
        let taper_y = taper[(iy + half) as usize];
        for ix in -half..half {
            let l = increment[0] * ix as f64;
            let r_sq = l * l + m_sq;
            if r_sq < 1.0 {
                let phase = two_pi_w * ((1.0 - r_sq).sqrt() - 1.0);
                let taper_x = taper[(ix + half) as usize];
                screen[((iy + centre) * conv_size as i64 + ix + centre) as usize] =
                    Complex64::from_polar(taper_x * taper_y, phase);
            }
        }
    }
}
