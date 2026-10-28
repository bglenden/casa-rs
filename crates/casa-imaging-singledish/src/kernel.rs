// SPDX-License-Identifier: LGPL-3.0-or-later
//! The gridding kernels, as CASA's `SDGrid` tabulates them (`SDGrid::init`
//! and `SDGrid::findPBAsConvFunction`): radial functions sampled at a
//! hundredth of a pixel, out to a support in whole pixels.
//!
//! | `gridfunction` | Kernel at radius `r` pixels | Default support |
//! |---|---|---|
//! | `BOX` | 1 out to `support + 1` | 0 |
//! | `SF` | `(1 − ν²) grdsf(ν)`, `ν = r / support` | 3 |
//! | `GAUSS` | `exp(−ln 2 (r/h)²)` out to `truncate` | `⌈truncate⌉`, `truncate = 3h` |
//! | `GJINC` | the Gaussian times `2 J₁(πr/c) / (πr/c)`, out to `truncate` or the jinc's first null | `⌈truncate⌉`, or `⌈2c⌉` |
//! | `PB` | the telescope's power pattern, cut at its first value below 10⁻³ | the kept length |
//!
//! `h` is a half width at half maximum (`gwidth`, default `√ln 2` for
//! `GAUSS` and `2.52 √ln 2` for `GJINC`) and `c` the jinc's width (`jwidth`,
//! default 1.55), both in pixels. A pixel's weight is the table at its
//! radius truncated to a hundredth ([`KernelTable::taps`]).
//!
//! CASA evaluates the jinc with a rational approximation of `J₁`
//! (`grdjinc1.f`, good to about 10⁻⁸); casa-rs takes `J₁` from `libm`.

use casa_imaging_operator::grdsf;
use ndarray::ArrayView2;

use crate::SingleDishError;

/// Samples per pixel of every kernel (`convSampling`).
pub const SAMPLING: usize = 100;

/// `findPBAsConvFunction`: the PB kernel ends before its first sample
/// below this.
const PB_CUT: f32 = 1.0e-3;

/// `findPBAsConvFunction`: the PB kernel is tabulated over at least this
/// many pixels.
const PB_MINIMUM_PIXELS: usize = 128;

/// A telescope's power pattern, as the `PB` kernel samples it.
pub trait PowerPattern {
    /// The power `offset_rad` along an image's x axis from the beam's
    /// centre, at `frequency_hz` (`SDGrid::findPBAsConvFunction` applies
    /// the beam to a one-row image).
    fn power_on_axis(&self, offset_rad: f64, frequency_hz: f64) -> f32;

    /// `PBMath1D::support`: the pattern's diameter in pixels of
    /// `increment_x_rad` at `frequency_hz`, rounded down.
    fn support_pixels(&self, increment_x_rad: f64, frequency_hz: f64) -> usize;
}

/// CASA's default primary beams ([`casa_numerics::PrimaryBeam::pattern`]),
/// as the simulator and `tsdimaging` take them for the `PB` kernel.
impl PowerPattern for casa_numerics::BeamPattern {
    fn power_on_axis(&self, offset_rad: f64, frequency_hz: f64) -> f32 {
        Self::power_on_axis(self, offset_rad, frequency_hz)
    }

    fn support_pixels(&self, increment_x_rad: f64, frequency_hz: f64) -> usize {
        Self::support_pixels(self, increment_x_rad, frequency_hz)
    }
}

/// The kernel `gridfunction` names, with its parameters in pixels.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GridFunction {
    /// `BOX`: weight 1 on the pixels within `support` of the nearest one.
    Box {
        /// Half width in whole pixels (`convsupport`).
        support: usize,
    },
    /// `SF`: CASA's prolate spheroid over `support` pixels.
    Spheroidal {
        /// Radius in whole pixels (`convsupport`), at least 1.
        support: usize,
    },
    /// `GAUSS`: a Gaussian of half width `hwhm`, out to `truncate`.
    Gauss {
        /// Half width at half maximum, pixels (`gwidth`).
        hwhm: f64,
        /// Radius beyond which the kernel is zero, pixels (`truncate`).
        truncate: f32,
    },
    /// `GJINC`: a Gaussian of half width `hwhm` times a jinc of width
    /// `jwidth`, out to `truncate`, or to the jinc's first null.
    Gjinc {
        /// Half width at half maximum of the Gaussian, pixels (`gwidth`).
        hwhm: f64,
        /// Width of the jinc, pixels (`jwidth`): its argument is
        /// `π r / jwidth`.
        jwidth: f64,
        /// Radius beyond which the kernel is zero, pixels; `None` cuts it
        /// at the jinc's first null.
        truncate: Option<f32>,
    },
    /// `PB`: the telescope's power pattern ([`KernelTable::primary_beam`]).
    PrimaryBeam,
}

/// CASA's kernel parameters as the task takes them: `convsupport` in
/// pixels, and `truncate`, `gwidth` and `jwidth` in pixels, a negative or
/// zero value choosing CASA's default.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KernelParameters {
    /// `convsupport`.
    pub convsupport: i64,
    /// `truncate`, pixels.
    pub truncate: f64,
    /// `gwidth`, pixels.
    pub gwidth: f64,
    /// `jwidth`, pixels.
    pub jwidth: f64,
}

impl KernelParameters {
    /// Every parameter at CASA's default (`-1`).
    pub const DEFAULT: Self = Self {
        convsupport: -1,
        truncate: -1.0,
        gwidth: -1.0,
        jwidth: -1.0,
    };
}

impl GridFunction {
    /// `BOX` with CASA's default support, 0.
    pub const BOX: Self = Self::Box { support: 0 };

    /// `SF` with CASA's default support, 3 pixels.
    pub const SPHEROIDAL: Self = Self::Spheroidal { support: 3 };

    /// The kernel `gridfunction` names, in any case, with `parameters`
    /// resolved as `SDGrid::init` resolves them:
    /// - `BOX` and `SF` take `convsupport`, 0 and 3 when it is negative;
    /// - `GAUSS` takes `gwidth`, `√ln 2` when it is not positive, and
    ///   `truncate`, `3 gwidth` when it is negative;
    /// - `GJINC` takes `gwidth`, `2.52 √ln 2`, and `jwidth`, 1.55, when they
    ///   are not positive, and `truncate`, the jinc's first null when it is
    ///   negative.
    ///
    /// `truncate` is held in single precision, as CASA holds it.
    ///
    /// # Errors
    ///
    /// A name CASA does not know, and an `SF` support of 0, which CASA
    /// tabulates as a kernel of zeros (no pixel would be gridded).
    pub fn from_casa(name: &str, parameters: KernelParameters) -> Result<Self, SingleDishError> {
        let support = |default: usize| usize::try_from(parameters.convsupport).unwrap_or(default);
        match name.to_lowercase().as_str() {
            "box" => Ok(Self::Box {
                support: support(0),
            }),
            "sf" => match support(3) {
                0 => Err(SingleDishError::Kernel(
                    "gridfunction='SF' with convsupport=0 has no pixels to grid onto; give \
                     convsupport of at least 1, or -1 for CASA's default of 3"
                        .into(),
                )),
                support => Ok(Self::Spheroidal { support }),
            },
            "gauss" => {
                let hwhm = if parameters.gwidth > 0.0 {
                    parameters.gwidth
                } else {
                    std::f64::consts::LN_2.sqrt()
                };
                let truncate = if parameters.truncate >= 0.0 {
                    parameters.truncate
                } else {
                    3.0 * hwhm
                };
                Ok(Self::Gauss {
                    hwhm,
                    truncate: truncate as f32,
                })
            }
            "gjinc" => Ok(Self::Gjinc {
                hwhm: if parameters.gwidth > 0.0 {
                    parameters.gwidth
                } else {
                    std::f64::consts::LN_2.sqrt() * 2.52
                },
                jwidth: if parameters.jwidth > 0.0 {
                    parameters.jwidth
                } else {
                    1.55
                },
                truncate: (parameters.truncate >= 0.0).then_some(parameters.truncate as f32),
            }),
            "pb" => Ok(Self::PrimaryBeam),
            _ => Err(SingleDishError::Kernel(format!(
                "gridfunction='{name}' is not one of CASA's BOX, SF, GAUSS, GJINC or PB"
            ))),
        }
    }

    /// The support in whole pixels: `⌈truncate⌉` for `GAUSS`, and for
    /// `GJINC` `⌈truncate⌉` or `⌈2 jwidth⌉`; `None` for `PB`, whose support
    /// is the pattern's.
    #[must_use]
    pub fn support(self) -> Option<usize> {
        // `convSupport = (Int)truncate`, plus one for any fraction.
        let ceiling = |value: f32| value.max(0.0).ceil() as usize;
        match self {
            Self::Box { support } | Self::Spheroidal { support } => Some(support),
            Self::Gauss { truncate, .. } => Some(ceiling(truncate)),
            Self::Gjinc {
                jwidth, truncate, ..
            } => Some(ceiling(truncate.unwrap_or((2.0 * jwidth) as f32))),
            Self::PrimaryBeam => None,
        }
    }

    /// The bytes forming the kernel's table holds at most: for `PB`, the
    /// samples of a pattern of `pattern_pixels` ([`PowerPattern::
    /// support_pixels`]) and the table kept; for the others, the table.
    #[must_use]
    pub fn forming_bytes(self, pattern_pixels: usize) -> u64 {
        let samples = match self.support() {
            Some(support) => table_len(support),
            None => {
                let pixels = PB_MINIMUM_PIXELS.max(pattern_pixels);
                SAMPLING * pixels + table_len(pixels)
            }
        };
        (samples * size_of::<f32>()) as u64
    }
}

/// `convSize`: the samples of a kernel of `support` pixels.
const fn table_len(support: usize) -> usize {
    SAMPLING * (2 * support + 2)
}

/// One pixel a kernel reaches from a pointing, with its weight.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tap {
    /// The pixel's x index, zero-based.
    pub x: usize,
    /// The pixel's y index, zero-based.
    pub y: usize,
    /// The kernel's value at the pixel.
    pub weight: f32,
}

/// A radial kernel tabulated at a hundredth of a pixel (`convFunc`), and its
/// support in pixels.
#[derive(Debug, Clone, PartialEq)]
pub struct KernelTable {
    values: Vec<f32>,
    support: usize,
}

impl KernelTable {
    /// The table of `function`, as `SDGrid::init` forms it.
    ///
    /// # Errors
    ///
    /// `PB`, which needs the telescope's pattern
    /// ([`Self::primary_beam`]).
    pub fn new(function: GridFunction) -> Result<Self, SingleDishError> {
        let support = function.support().ok_or_else(|| {
            SingleDishError::Kernel(
                "the PB kernel is tabulated from the telescope's pattern".into(),
            )
        })?;
        let mut values = vec![0.0_f32; table_len(support)];
        let radius = |i: usize| i as f64 / SAMPLING as f64;
        match function {
            // `convFunc(i) = 1` for `i < convSize/2`.
            GridFunction::Box { .. } => values[..table_len(support) / 2].fill(1.0),
            GridFunction::Spheroidal { .. } => {
                let samples = SAMPLING * support;
                for (i, value) in values[..samples].iter_mut().enumerate() {
                    let nu = i as f64 / samples as f64;
                    *value = ((1.0 - nu * nu) * grdsf(nu)) as f32;
                }
            }
            GridFunction::Gauss { hwhm, truncate } => {
                // `itruncate = (Int)(truncate·convSampling + 0.5)`, filled
                // through `i ≤ itruncate`.
                let last = (f64::from(truncate) * SAMPLING as f64 + 0.5) as usize;
                for (i, value) in values.iter_mut().enumerate().take(last + 1) {
                    *value = gaussian(hwhm, radius(i)) as f32;
                }
            }
            GridFunction::Gjinc {
                hwhm,
                jwidth,
                truncate,
            } => match truncate {
                // Filled for `i < itruncate`.
                Some(truncate) => {
                    let end = (f64::from(truncate) * SAMPLING as f64 + 0.5) as usize;
                    for (i, value) in values.iter_mut().enumerate().take(end) {
                        *value = (gaussian(hwhm, radius(i)) * jinc(jwidth, radius(i))) as f32;
                    }
                }
                // Out to the jinc's first null.
                None => {
                    for (i, value) in values.iter_mut().enumerate() {
                        let jinc = jinc(jwidth, radius(i));
                        if jinc <= 0.0 {
                            break;
                        }
                        *value = (gaussian(hwhm, radius(i)) * jinc) as f32;
                    }
                }
            },
            GridFunction::PrimaryBeam => unreachable!("PB has no fixed support"),
        }
        Ok(Self { values, support })
    }

    /// `findPBAsConvFunction`: `pattern` along the x axis at
    /// `frequency_hz`, sampled every hundredth of a pixel of
    /// `increment_x_rad` over `max(128, PBMath1D::support)` pixels and cut
    /// before its first sample below 10⁻³. CASA keeps one sample fewer than
    /// it counts before the cut (`Slice(0, cfLen − 1)`), at least one
    /// counted, and takes the support from all it counted
    /// (`convSupport = cfLen/convSampling`).
    #[must_use]
    pub fn primary_beam(
        pattern: &impl PowerPattern,
        increment_x_rad: f64,
        frequency_hz: f64,
    ) -> Self {
        let step = increment_x_rad * (1.0 / SAMPLING as f64);
        let pixels = PB_MINIMUM_PIXELS.max(pattern.support_pixels(increment_x_rad, frequency_hz));
        let mut samples: Vec<f32> = (0..SAMPLING * pixels)
            .map(|i| pattern.power_on_axis(i as f64 * step, frequency_hz))
            .take_while(|&power| power >= PB_CUT)
            .collect();
        let counted = samples.len().max(1);
        samples.truncate(counted - 1);
        let support = counted / SAMPLING;
        let mut values = vec![0.0; table_len(support)];
        values[..samples.len()].copy_from_slice(&samples);
        Self { values, support }
    }

    /// The support in whole pixels: the kernel reaches the pixels within
    /// this many of a pointing's nearest pixel along each axis.
    #[must_use]
    pub const fn support(&self) -> usize {
        self.support
    }

    /// The table, one sample per hundredth of a pixel from the centre.
    #[must_use]
    pub fn values(&self) -> &[f32] {
        &self.values
    }

    /// The bytes the table holds.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        (self.values.len() * size_of::<f32>()) as u64
    }

    /// Append to `taps` the pixels of an `nx × ny` image the kernel reaches
    /// from a pointing at zero-based fractional pixel `pixel`, in CASA's
    /// order (y outer, x inner), and return whether the pointing's nearest
    /// pixel is on the image; a pointing whose nearest pixel is off it
    /// reaches none.
    ///
    /// As `sgridsd`, `ggridsd` and `dgridsd` (`fgridsd.f`) place it, in
    /// single precision and one-based: the nearest pixel is
    /// `loc = nint(x + 1)`, its offset in hundredths of a pixel
    /// `off = nint((loc − x − 1) · 100)`, and each pixel within `support`
    /// of `loc` along both axes takes the table at
    /// `⌊√(rx² + ry²)⌋`, `r = 100 i + off`.
    pub fn taps(&self, pixel: [f64; 2], shape: (usize, usize), taps: &mut Vec<Tap>) -> bool {
        self.for_each_tap(pixel, shape, |tap| taps.push(tap))
    }

    /// [`Self::taps`], handing each tap to `visit` in turn.
    fn for_each_tap(
        &self,
        pixel: [f64; 2],
        (nx, ny): (usize, usize),
        mut visit: impl FnMut(Tap),
    ) -> bool {
        let position = pixel.map(|value| (value + 1.0) as f32);
        let location = position.map(|value| value.round() as i64);
        let offset = [0, 1].map(|axis| {
            ((location[axis] as f32 - position[axis]) * SAMPLING as f32).round() as i64
        });
        // `ogridsd` with no support: only the nearest pixel must be on it.
        let on_image = |value: i64, size: usize| value >= 1 && value <= size as i64;
        if !on_image(location[0], nx) || !on_image(location[1], ny) {
            return false;
        }
        let support = self.support as i64;
        for iy in -support..=support {
            let y = location[1] + iy;
            if !on_image(y, ny) {
                continue;
            }
            let ry = (SAMPLING as i64 * iy + offset[1]) as f32;
            for ix in -support..=support {
                let x = location[0] + ix;
                if !on_image(x, nx) {
                    continue;
                }
                let rx = (SAMPLING as i64 * ix + offset[0]) as f32;
                let radius = (rx * rx + ry * ry).sqrt() as usize;
                visit(Tap {
                    x: (x - 1) as usize,
                    y: (y - 1) as usize,
                    weight: self.values.get(radius).copied().unwrap_or(0.0),
                });
            }
        }
        true
    }

    /// `dgridsd` for one pointing at zero-based fractional pixel `pixel` of
    /// `plane` (`[y][x]`): the kernel-weighted sum of the pixels it reaches,
    /// in single precision, and 0 for a pointing off the image. The
    /// visibility CASA predicts is this sum's conjugate.
    #[must_use]
    pub fn degrid(&self, plane: ArrayView2<'_, f32>, pixel: [f64; 2]) -> f32 {
        let (ny, nx) = plane.dim();
        let mut sum = 0.0_f32;
        self.for_each_tap(pixel, (nx, ny), |tap| {
            sum += tap.weight * plane[[tap.y, tap.x]];
        });
        sum
    }
}

/// `grdgauss`: `exp(−ln 2 (r/hwhm)²)`.
fn gaussian(hwhm: f64, radius: f64) -> f64 {
    (-std::f64::consts::LN_2 * (radius / hwhm) * (radius / hwhm)).exp()
}

/// `grdjinc1(c, r, normalize=1)`: `2 J₁(x)/x` at `x = π r / c`, 1 at the
/// centre.
fn jinc(width: f64, radius: f64) -> f64 {
    if radius == 0.0 {
        return 1.0;
    }
    let x = std::f64::consts::PI * radius / width;
    2.0 * libm::j1(x) / x
}

#[cfg(test)]
mod tests;
