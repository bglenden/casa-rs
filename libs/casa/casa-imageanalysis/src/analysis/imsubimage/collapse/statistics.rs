// SPDX-License-Identifier: LGPL-3.0-or-later

use std::cmp::Ordering;

use num_complex::{Complex32, Complex64};

use casa_images::{ImageError, ImagePixel};

/// Values reported by CASA's image collapse tool.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CollapseFunction {
    /// Sum converted to integrated brightness or flux density.
    Flux,
    /// Median absolute deviation from the median.
    Madm,
    /// Largest sample.
    Max,
    /// Mean of valid samples.
    Mean,
    /// Median of valid samples, averaging the two middle values when even.
    Median,
    /// Smallest sample.
    Min,
    /// Count of valid samples.
    Npts,
    /// Square root of the mean of the squared samples.
    Rms,
    /// Square root of the sum.
    SqrtSum,
    /// Square root of the sum divided by the number of valid samples.
    SqrtSumNpix,
    /// Square root of the sum divided by the beam area in pixels.
    SqrtSumNpixBeam,
    /// Sample standard deviation.
    Stddev,
    /// Sum of valid samples.
    Sum,
    /// Sample variance.
    Variance,
    /// Median absolute deviation scaled to estimate Gaussian standard deviation.
    Xmadm,
    /// Zero on every output pixel.
    Zero,
}

impl CollapseFunction {
    /// Parse the public function name or an unambiguous CASA abbreviation.
    ///
    /// # Errors
    /// The name is missing, unknown or the unsupported `avdev`.
    pub fn parse(name: &str) -> Result<Self, ImageError> {
        let name = name.trim().to_ascii_lowercase();
        let supported = [
            (Self::Flux, "f", "flux"),
            (Self::Madm, "mad", "madm"),
            (Self::Max, "max", "max"),
            (Self::Mean, "mea", "mean"),
            (Self::Median, "med", "median"),
            (Self::Min, "mi", "min"),
            (Self::Npts, "n", "npts"),
            (Self::Rms, "r", "rms"),
            (Self::SqrtSum, "sqrtsum", "sqrtsum"),
            (Self::SqrtSumNpix, "sqrtsum_npix", "sqrtsum_npix"),
            (
                Self::SqrtSumNpixBeam,
                "sqrtsum_npix_beam",
                "sqrtsum_npix_beam",
            ),
            (Self::Stddev, "st", "stddev"),
            (Self::Sum, "su", "sum"),
            (Self::Variance, "v", "variance"),
            (Self::Xmadm, "xm", "xmadm"),
            (Self::Zero, "z", "zero"),
        ];
        supported
            .into_iter()
            .find(|&(_, minimum, full)| name.starts_with(minimum) && full.starts_with(&name))
            .map(|(function, _, _)| function)
            .ok_or_else(|| {
                ImageError::CasaSelection(match name.as_str() {
                    "" => "Aggregate function name is not specified and it must be.".into(),
                    "avdev" => {
                        "avdev currently not supported. Let us know if you have a need for it"
                            .into()
                    }
                    _ => format!("Unknown aggregate function specified by {name}"),
                })
            })
    }

    pub(super) fn uses_median(self) -> bool {
        matches!(self, Self::Median | Self::Madm | Self::Xmadm)
    }

    pub(super) fn deviation_scale(self) -> Option<f64> {
        match self {
            Self::Madm => Some(1.0),
            Self::Xmadm => Some(1.482602218505602),
            _ => None,
        }
    }
}

/// Single-precision pixels for CASA image collapse. Complex statistics carry
/// the algebraic complex sum and variance in double precision.
pub trait CollapsePixel: ImagePixel + PartialEq {
    /// Whether variance, RMS and square roots are complex-valued.
    const COMPLEX: bool;

    /// Convert to the arithmetic precision used by the accumulators.
    fn wide(self) -> Complex64;

    /// Convert a final statistic to the image's pixel type.
    fn narrow(value: Complex64) -> Self;

    /// Scalar ordering used for extrema and medians.
    fn ordering_value(self) -> f32;
}

impl CollapsePixel for f32 {
    const COMPLEX: bool = false;

    fn wide(self) -> Complex64 {
        Complex64::new(f64::from(self), 0.0)
    }

    fn narrow(value: Complex64) -> Self {
        value.re as f32
    }

    fn ordering_value(self) -> f32 {
        self
    }
}

impl CollapsePixel for Complex32 {
    const COMPLEX: bool = true;

    fn wide(self) -> Complex64 {
        Complex64::new(f64::from(self.re), f64::from(self.im))
    }

    fn narrow(value: Complex64) -> Self {
        Self::new(value.re as f32, value.im as f32)
    }

    fn ordering_value(self) -> f32 {
        self.norm_sqr()
    }
}

#[derive(Default)]
pub(super) struct Summary<T> {
    pub(super) count: usize,
    sum: Complex64,
    squares: Complex64,
    mean: Complex64,
    dispersion: Complex64,
    extremum: Option<(usize, T)>,
}

impl<T: CollapsePixel> Summary<T> {
    pub(super) fn add(&mut self, function: CollapseFunction, sample: T, position: usize) {
        use CollapseFunction as F;
        self.count += 1;
        match function {
            F::Max | F::Min => {
                let better = if function == F::Min {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                if self.extremum.is_none_or(|(index, prior)| {
                    let order = sample.ordering_value().partial_cmp(&prior.ordering_value());
                    order == Some(better) || (order == Some(Ordering::Equal) && position < index)
                }) {
                    self.extremum = Some((position, sample));
                }
            }
            F::Rms => {
                let value = sample.wide();
                self.squares += value * value;
            }
            F::Stddev | F::Variance => {
                let value = sample.wide();
                let residual = value - self.mean;
                self.mean += residual / self.count as f64;
                self.dispersion += residual * (value - self.mean);
            }
            F::Npts | F::Zero | F::Median | F::Madm | F::Xmadm => {}
            _ => self.sum += sample.wide(),
        }
    }

    pub(super) fn finish(
        &self,
        function: CollapseFunction,
        scale: f64,
        beam_pixels: f64,
    ) -> (T, bool) {
        use CollapseFunction as F;
        let count = self.count as f64;
        let zero = Complex64::default();
        if self.count == 0 {
            return match function {
                F::Flux | F::Zero => (T::default(), true),
                F::SqrtSumNpix => (T::narrow(Complex64::new(f64::NAN, f64::NAN)), false),
                _ => (T::default(), false),
            };
        }
        let sqrt = |value: Complex64| {
            if T::COMPLEX {
                value.sqrt()
            } else {
                Complex64::new((if value.re < 0.0 { 0.0 } else { value.re }).sqrt(), 0.0)
            }
        };
        let variance = || {
            if self.count > 1 {
                self.dispersion / (count - 1.0)
            } else {
                zero
            }
        };
        let value = match function {
            F::Max | F::Min => return (self.extremum.expect("nonempty samples").1, true),
            F::Flux => self.sum * scale,
            F::Mean => self.sum / count,
            F::Npts => Complex64::new(count, 0.0),
            F::Rms => sqrt(self.squares / count),
            F::SqrtSum => sqrt(self.sum),
            F::SqrtSumNpix => sqrt(self.sum) / count,
            F::SqrtSumNpixBeam => sqrt(self.sum) / beam_pixels,
            F::Stddev => sqrt(variance()),
            F::Sum => self.sum,
            F::Variance => variance(),
            F::Zero => zero,
            F::Median | F::Madm | F::Xmadm => {
                unreachable!("median uses the order statistics of the samples")
            }
        };
        (T::narrow(value), true)
    }
}
