// SPDX-License-Identifier: LGPL-3.0-or-later
//! A component's visibility, as CASA's `SkyCompRep::visibility` forms it
//! (`components/ComponentModels`): the flux in Stokes, scaled by the spectral
//! model at the frequency, converted to the data's polarisation basis, and
//! scaled by the shape's visibility at the baseline.
//!
//! The baseline is in metres and already rotated to the component's
//! reference direction, with CASA's sign: `SimpleComponentFTMachine::get`
//! negates u and v before it rotates them. The frequency is the data's,
//! taken in the frame of the spectral model's reference frequency, as CASA
//! takes it ("the frequency is expected to be in the frame of the
//! component").
//!
//! What casa-rs does not evaluate is refused rather than approximated:
//! - a limb-darkened disk, whose visibility needs a Bessel function of real
//!   order (`gsl_sf_bessel_Jnu`);
//! - a tabular spectrum whose reference frequency is in another frame than
//!   its tabulation, which CASA converts through a frame casa-rs would have
//!   to build, and a table of fewer than two points, which CASA's linear
//!   interpolation reads out of bounds;
//! - a flux unit that is not a flux density (`Jy.km/s`, `K.rad2`), which
//!   CASA's `convertUnit(Unit("Jy"))` cannot convert either.

use casa_types::quanta::{Quantity, Unit};
use num_complex::Complex64;

use crate::component_type::Polarisation;
use crate::error::ComponentError;
use crate::shape::{ComponentShape, Ellipse, ShapeKind};
use crate::spectrum::{SpectralKind, SpectralModel, TabularSpectrum};
use crate::{Flux, SkyComponent};

/// The speed of light, m/s (casacore `C::c`).
const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

impl Flux {
    /// The flux as real Stokes I, Q, U and V in Jy: `convertUnit("Jy")`, then
    /// `FluxRep::value(Vector<Double>&)`, which converts to Stokes and keeps
    /// the real parts.
    ///
    /// # Errors
    ///
    /// [`ComponentError::FluxUnit`] when the unit is not a flux density.
    pub fn stokes_jy_values(&self) -> Result<[f64; 4], ComponentError> {
        let jy = Unit::new("Jy").expect("Jy is a unit");
        let scale = Quantity::with_unit(1.0, self.unit().clone())
            .get_value_in(&jy)
            .map_err(|_| ComponentError::FluxUnit(self.unit().name().to_string()))?;
        let stokes = to_stokes(*self.values(), self.polarisation());
        Ok(stokes.map(|value| value.re * scale))
    }
}

/// `values` in `from`'s representation, as complex Stokes I, Q, U and V
/// (`Flux::linearToStokes`, `Flux::circularToStokes`).
fn to_stokes(values: [Complex64; 4], from: Polarisation) -> [Complex64; 4] {
    let [a, b, c, d] = values;
    match from {
        Polarisation::Stokes => values,
        Polarisation::Circular => [
            (a + d) / 2.0,
            (b + c) / 2.0,
            Complex64::new((b.im - c.im) / 2.0, (c.re - b.re) / 2.0),
            (a - d) / 2.0,
        ],
        Polarisation::Linear => [
            (a + d) / 2.0,
            (a - d) / 2.0,
            (b + c) / 2.0,
            Complex64::new((b.im - c.im) / 2.0, (c.re - b.re) / 2.0),
        ],
    }
}

/// Real Stokes I, Q, U and V in `basis`'s order (`Flux::stokesToCircular`,
/// `Flux::stokesToLinear`): RR = I + V, RL = Q + iU, LR = Q − iU, LL = I − V,
/// or XX = I + Q, XY = U + iV, YX = U − iV, YY = I − Q.
#[must_use]
pub fn stokes_in(basis: Polarisation, iquv: [f64; 4]) -> [Complex64; 4] {
    let [i, q, u, v] = iquv;
    match basis {
        Polarisation::Stokes => iquv.map(|value| Complex64::new(value, 0.0)),
        Polarisation::Circular => [
            Complex64::new(i + v, 0.0),
            Complex64::new(q, u),
            Complex64::new(q, -u),
            Complex64::new(i - v, 0.0),
        ],
        Polarisation::Linear => [
            Complex64::new(i + q, 0.0),
            Complex64::new(u, v),
            Complex64::new(u, -v),
            Complex64::new(i - q, 0.0),
        ],
    }
}

impl SpectralModel {
    /// The Stokes flux at `frequency_hz` of a component whose flux at the
    /// reference frequency is `iquv`: CASA's matrix `sampleStokes`, which
    /// `SkyCompRep::visibility` uses.
    /// - Constant: `iquv` unchanged.
    /// - Spectral index: I scales as `(ν/ν₀)^α₀`, Q and U as
    ///   `(ν/ν₀)^(α₀+α₁)` and V as `(ν/ν₀)^(α₀+α₃)`, then Q and U rotate by
    ///   twice `α₂ c² (ν₀² − ν²)/(ν² ν₀²)` (a rotation measure). A single
    ///   index is `[α, 0, 0, 0]`: CASA's matrix sampler has no branch for it
    ///   and reads the missing indices out of bounds (`docs/CASA (C++)
    ///   bugs.md`), which for an unpolarised component multiplies zeros.
    /// - Power-log-polynomial: I scales as `x^(c₀ + c₁ ln x + c₂ ln²x + …)`
    ///   with `x = ν/ν₀`; Q, U and V are unchanged.
    /// - Tabular: the tabulated Stokes values, interpolated linearly in
    ///   frequency and extrapolated from the end points
    ///   (`InterpolateArray1D::linear`); the component's own flux does not
    ///   enter.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] for a tabular spectrum casa-rs does not
    /// evaluate (see the module documentation) or a reference frequency that
    /// is not positive.
    pub fn sample_stokes(
        &self,
        frequency_hz: f64,
        iquv: [f64; 4],
    ) -> Result<[f64; 4], ComponentError> {
        let nu0 = self.reference_frequency().hz();
        let positive = |spectrum: &'static str| {
            if nu0 > 0.0 {
                Ok(nu0)
            } else {
                Err(ComponentError::Spectrum {
                    spectrum,
                    reason: format!("the reference frequency {nu0} Hz is not positive"),
                })
            }
        };
        Ok(match self.kind() {
            SpectralKind::Constant => iquv,
            SpectralKind::SpectralIndex(index) => {
                let nu0 = positive("spectral index")?;
                let [a0, a1, a2, a3] =
                    index
                        .stokes_indices()
                        .unwrap_or([index.index(), 0.0, 0.0, 0.0]);
                let ratio = frequency_hz / nu0;
                let [i, q, u, v] = iquv;
                let (mut q, mut u) = (q * ratio.powf(a0 + a1), u * ratio.powf(a0 + a1));
                if a2 != 0.0 {
                    let nu = frequency_hz;
                    let two_alpha = 2.0
                        * a2
                        * SPEED_OF_LIGHT_M_PER_S
                        * SPEED_OF_LIGHT_M_PER_S
                        * (nu0 * nu0 - nu * nu)
                        / (nu * nu * nu0 * nu0);
                    let (sin, cos) = two_alpha.sin_cos();
                    (q, u) = (cos * q - sin * u, sin * q + cos * u);
                }
                [i * ratio.powf(a0), q, u, v * ratio.powf(a0 + a3)]
            }
            SpectralKind::PowerLogPolynomial(polynomial) => {
                let nu0 = positive("power-log-polynomial")?;
                let x = frequency_hz / nu0;
                let log_x = x.ln();
                let mut exponent = 0.0;
                for (power, coefficient) in polynomial.coefficients().iter().enumerate() {
                    exponent += match power {
                        0 => *coefficient,
                        1 => coefficient * log_x,
                        2 => coefficient * log_x * log_x,
                        _ => coefficient * log_x.powi(power as i32),
                    };
                }
                let [i, q, u, v] = iquv;
                [i * x.powf(exponent), q, u, v]
            }
            SpectralKind::Tabular(table) => {
                tabular_stokes(table, self.reference_frequency().refer(), frequency_hz)?
            }
        })
    }
}

/// `table`'s Stokes values at `frequency_hz` (see
/// [`SpectralModel::sample_stokes`]).
fn tabular_stokes(
    table: &TabularSpectrum,
    reference_frame: casa_types::measures::frequency::FrequencyRef,
    frequency_hz: f64,
) -> Result<[f64; 4], ComponentError> {
    let refused = |reason: String| ComponentError::Spectrum {
        spectrum: "tabular",
        reason,
    };
    if table.frame() != reference_frame {
        return Err(refused(format!(
            "a reference frequency in {reference_frame:?} and a tabulation in {:?}: casa-rs \
             does not convert the data's frequencies between them",
            table.frame()
        )));
    }
    let frequencies = table.frequencies_hz();
    if frequencies.len() < 2 {
        return Err(refused(format!(
            "{} tabulated frequencies; linear interpolation needs two",
            frequencies.len()
        )));
    }
    // `binarySearchBrackets`, then the bracketing pair, or the end pair.
    let upper = frequencies
        .partition_point(|frequency| *frequency < frequency_hz)
        .clamp(1, frequencies.len() - 1);
    let (x1, x2) = (frequencies[upper - 1], frequencies[upper]);
    let fraction = (frequency_hz - x1) / (x2 - x1);
    let (y1, y2) = (table.stokes_flux()[upper - 1], table.stokes_flux()[upper]);
    Ok(std::array::from_fn(|stokes| {
        y1[stokes] + fraction * (y2[stokes] - y1[stokes])
    }))
}

impl ComponentShape {
    /// The shape's visibility at `uvw_m` and `frequency_hz` (`visibility`
    /// of `PointShape`, `GaussianShape` and `DiskShape`), real.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Visibility`] for a limb-darkened disk.
    pub fn visibility_scale(
        &self,
        uvw_m: [f64; 3],
        frequency_hz: f64,
    ) -> Result<f64, ComponentError> {
        Ok(self
            .transform()?
            .at_baseline(uvw_m)
            .at_frequency(frequency_hz))
    }

    /// The shape's Fourier transform, prepared from its axes for evaluation
    /// at many baselines and frequencies, as `GaussianShape::updateFT` caches
    /// the Gaussian's.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Visibility`] for a limb-darkened disk.
    pub fn transform(&self) -> Result<ShapeTransform, ComponentError> {
        Ok(ShapeTransform(match self.kind() {
            ShapeKind::Point => Transform::Point,
            ShapeKind::Gaussian { axes, .. } => Transform::gaussian(axes),
            ShapeKind::Disk { axes, .. } => Transform::Disk {
                rotation: (axes.position_angle().abs() > NEAR_ABS)
                    .then(|| axes.position_angle().sin_cos()),
                minor_axis: axes.minor_axis(),
                major_axis: axes.major_axis(),
            },
            ShapeKind::LimbDarkenedDisk { .. } => {
                return Err(ComponentError::Visibility {
                    shape: "limb-darkened disk",
                    reason: "its visibility needs a Bessel function of real order".to_string(),
                });
            }
        }))
    }
}

/// A shape's Fourier transform ([`ComponentShape::transform`]), holding no
/// heap. [`Self::at_baseline`] does the work that depends on the baseline,
/// so a baseline's channels each evaluate a function of frequency alone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShapeTransform(Transform);

#[derive(Debug, Clone, Copy, PartialEq)]
enum Transform {
    Point,
    /// `GaussianShape`'s cached transform: casacore's `Gaussian2D` of unit
    /// height evaluated at `(x, y) = (−u, v) ν/c` (`Gaussian2D::eval`), as
    /// `exp(−(x′² + y′²))`. Its rotated and scaled coordinates are rows of
    /// coefficients on `(u, v)`, so that `x′ = (ν/c) (x_row · (u, v))` and
    /// likewise `y′`.
    Gaussian {
        x_row: [f64; 2],
        y_row: [f64; 2],
    },
    /// `DiskShape::visibility`: the baseline rotated by the position angle
    /// when it is not near zero, and scaled by the axes.
    Disk {
        rotation: Option<(f64, f64)>,
        minor_axis: f64,
        major_axis: f64,
    },
}

impl Transform {
    /// The Gaussian of `axes`: widths `4 ln 2/(π · minor)` and `4 ln 2/(π ·
    /// major)` and the shape's position angle plus π/2
    /// (`GaussianShape::updateFT`).
    fn gaussian(axes: &Ellipse) -> Self {
        let factor = 4.0 * std::f64::consts::LN_2 / std::f64::consts::PI;
        // `setWidth`: the larger width is the major one, held as the y
        // width, and the ratio is x over y; `setPA` keeps the angle when y is
        // the larger width, as here.
        let y_width = factor / axes.minor_axis();
        let ratio = (factor / axes.major_axis()) / y_width;
        let angle = axes.position_angle() + std::f64::consts::FRAC_PI_2;
        let (sin, cos) = angle.sin_cos();
        let fwhm_to_sigma = 1.0 / 16.0_f64.ln().sqrt();
        // x′ = (cos x + sin y)/σx and y′ = (−sin x + cos y)/σy, with x = −u
        // and y = v.
        let (x_sigma, y_sigma) = (y_width * ratio * fwhm_to_sigma, y_width * fwhm_to_sigma);
        Self::Gaussian {
            x_row: [-cos / x_sigma, sin / x_sigma],
            y_row: [sin / y_sigma, cos / y_sigma],
        }
    }
}

impl ShapeTransform {
    /// The transform at `uvw_m` (rotated to the shape's reference direction,
    /// CASA's sign), as a function of frequency.
    #[must_use]
    pub fn at_baseline(&self, uvw_m: [f64; 3]) -> BaselineTransform {
        let [u, v, _] = uvw_m;
        let wavenumber_squared = 1.0 / (SPEED_OF_LIGHT_M_PER_S * SPEED_OF_LIGHT_M_PER_S);
        BaselineTransform(match self.0 {
            Transform::Point => AtBaseline::One,
            Transform::Gaussian { x_row, y_row } => {
                let x = x_row[0] * u + x_row[1] * v;
                let y = y_row[0] * u + y_row[1] * v;
                AtBaseline::Gaussian((x * x + y * y) * wavenumber_squared)
            }
            // 1 where `u + v` is near zero, as CASA tests it (casacore
            // `near`, relative to 1e-13), and otherwise `2 J₁(r)/r` of the
            // rotated baseline scaled by the axes (the minor axis on u, the
            // major on v) and by `π ν/c`.
            Transform::Disk { .. } if near_zero(u + v) => AtBaseline::One,
            Transform::Disk {
                rotation,
                minor_axis,
                major_axis,
            } => {
                let (u, v) =
                    rotation.map_or((u, v), |(sin, cos)| (u * cos - v * sin, u * sin + v * cos));
                AtBaseline::Disk(
                    (u * minor_axis).hypot(v * major_axis) * std::f64::consts::PI
                        / SPEED_OF_LIGHT_M_PER_S,
                )
            }
        })
    }
}

/// A shape's transform at one baseline ([`ShapeTransform::at_baseline`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BaselineTransform(AtBaseline);

#[derive(Debug, Clone, Copy, PartialEq)]
enum AtBaseline {
    One,
    /// `exp(−g ν²)`.
    Gaussian(f64),
    /// `2 J₁(r)/r` with `r = R ν`.
    Disk(f64),
}

impl BaselineTransform {
    /// The shape's visibility at `frequency_hz`, real.
    #[must_use]
    pub fn at_frequency(&self, frequency_hz: f64) -> f64 {
        match self.0 {
            AtBaseline::One => 1.0,
            AtBaseline::Gaussian(per_hz_squared) => {
                (-per_hz_squared * frequency_hz * frequency_hz).exp()
            }
            AtBaseline::Disk(per_hz) => {
                let r = per_hz * frequency_hz;
                2.0 * libm::j1(r) / r
            }
        }
    }
}

/// casacore's default `nearAbs` tolerance.
const NEAR_ABS: f64 = 1.0e-13;

/// casacore `near(x, 0.0)` with its default 1e-13: relative to zero, only
/// values within `DBL_MIN` of it.
fn near_zero(x: f64) -> bool {
    x == 0.0 || x.abs() <= (1.0 + 1.0e-13) * f64::MIN_POSITIVE
}

impl SkyComponent {
    /// The component's visibility at `uvw_m` (rotated to its reference
    /// direction, CASA's sign) and `frequency_hz`, in `basis`'s order
    /// (`SkyCompRep::visibility`): the Stokes flux in Jy sampled by the
    /// spectral model, converted to `basis`, times the shape's visibility.
    ///
    /// # Errors
    ///
    /// The flux unit, spectral model or shape casa-rs does not evaluate.
    pub fn visibility(
        &self,
        uvw_m: [f64; 3],
        frequency_hz: f64,
        basis: Polarisation,
    ) -> Result<[Complex64; 4], ComponentError> {
        let flux = self.flux_in(frequency_hz, basis)?;
        let scale = self.shape().visibility_scale(uvw_m, frequency_hz)?;
        Ok(flux.map(|value| value * scale))
    }

    /// The component's flux at `frequency_hz` in `basis`'s order, in Jy: the
    /// part of [`Self::visibility`] that does not depend on the baseline.
    ///
    /// # Errors
    ///
    /// The flux unit or spectral model casa-rs does not evaluate.
    pub fn flux_in(
        &self,
        frequency_hz: f64,
        basis: Polarisation,
    ) -> Result<[Complex64; 4], ComponentError> {
        let iquv = self.flux().stokes_jy_values()?;
        let sampled = self.spectrum().sample_stokes(frequency_hz, iquv)?;
        Ok(stokes_in(basis, sampled))
    }
}

#[cfg(test)]
mod tests;
