// SPDX-License-Identifier: LGPL-3.0-or-later
//! A component's integrated flux, CASA's `Flux<Double>`.

use casa_types::quanta::Unit;
use num_complex::Complex64;

use crate::component_type::Polarisation;
use crate::error::ComponentError;

/// The flux units CASA accepts, by dimension (`FluxRep::_allowedUnits`).
const ALLOWED_FLUX_UNITS: [&str; 3] = ["Jy", "Jy.km/s", "K.rad2"];

/// The integrated flux of a component: four complex values in one unit and
/// one polarisation representation, with their errors.
///
/// This is CASA's `Flux<Double>` (`FluxRep`). The four values are, in order:
///
/// | representation | values |
/// |---|---|
/// | [`Polarisation::Stokes`] | I, Q, U, V (normally real) |
/// | [`Polarisation::Linear`] | XX, XY, YX, YY |
/// | [`Polarisation::Circular`] | RR, RL, LR, LL |
///
/// A component list stores them as written, in the `Flux` column, with the
/// unit's name in `Flux_Unit`, the representation's name in
/// `Flux_Polarisation` and the errors in `Flux_Error`. Nothing is converted
/// between representations or units on the way to or from a table.
///
/// The unit must have the dimensions of `Jy`, `Jy.km/s` or `K.rad2`, the units
/// CASA's flux accepts; its name is kept as given (`mJy` stays `mJy`).
#[derive(Debug, Clone, PartialEq)]
pub struct Flux {
    values: [Complex64; 4],
    unit: Unit,
    polarisation: Polarisation,
    errors: [Complex64; 4],
}

impl Flux {
    /// A flux with zero errors.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Unit`] when `unit` does not parse, and
    /// [`ComponentError::FluxUnit`] when its dimensions are not those of a
    /// flux CASA accepts.
    pub fn new(
        values: [Complex64; 4],
        unit: &str,
        polarisation: Polarisation,
    ) -> Result<Self, ComponentError> {
        Ok(Self {
            values,
            unit: flux_unit(unit)?,
            polarisation,
            errors: [Complex64::new(0.0, 0.0); 4],
        })
    }

    /// A Stokes flux in Jy, with zero errors: CASA's `Flux<Double>(i, q, u, v)`.
    pub fn stokes_jy(i: f64, q: f64, u: f64, v: f64) -> Self {
        Self {
            values: [i, q, u, v].map(|value| Complex64::new(value, 0.0)),
            unit: Unit::new("Jy").expect("Jy is a unit"),
            polarisation: Polarisation::Stokes,
            errors: [Complex64::new(0.0, 0.0); 4],
        }
    }

    /// This flux with its errors replaced (`FluxRep::setErrors`).
    pub fn with_errors(mut self, errors: [Complex64; 4]) -> Self {
        self.errors = errors;
        self
    }

    /// The four values in [`Self::polarisation`]'s order.
    pub fn values(&self) -> &[Complex64; 4] {
        &self.values
    }

    /// The unit of the values and errors.
    pub fn unit(&self) -> &Unit {
        &self.unit
    }

    /// The polarisation representation of the values.
    pub fn polarisation(&self) -> Polarisation {
        self.polarisation
    }

    /// The errors of the four values, in the same order and unit.
    pub fn errors(&self) -> &[Complex64; 4] {
        &self.errors
    }

    /// Replace the values, keeping the unit, representation and errors
    /// (`FluxRep::setValue`).
    pub fn set_values(&mut self, values: [Complex64; 4]) {
        self.values = values;
    }

    /// Replace the errors (`FluxRep::setErrors`).
    pub fn set_errors(&mut self, errors: [Complex64; 4]) {
        self.errors = errors;
    }

    /// Convert the complex flux values to another polarisation basis.
    ///
    /// As in CASA's `FluxRep::convertPol`, this preserves the unit and the
    /// numerical error values. Those errors may no longer describe the new
    /// basis; callers should warn when nonzero errors are present.
    pub fn convert_polarisation(&mut self, target: Polarisation) {
        if self.polarisation == target {
            return;
        }
        let [a, b, c, d] = self.values;
        let times_i = |z: Complex64| Complex64::new(-z.im, z.re);
        self.values = match (self.polarisation, target) {
            (from, Polarisation::Stokes) => crate::visibility::to_stokes(self.values, from),
            (Polarisation::Stokes, Polarisation::Linear) => {
                [a + b, c + times_i(d), c - times_i(d), a - b]
            }
            (Polarisation::Stokes, Polarisation::Circular) => {
                [a + d, b + times_i(c), b - times_i(c), a - d]
            }
            (Polarisation::Linear, Polarisation::Circular) => {
                let (ib, ic) = (times_i(b), times_i(c));
                [
                    (a - ib + ic + d) / 2.0,
                    (a + ib + ic - d) / 2.0,
                    (a - ib - ic - d) / 2.0,
                    (a + ib - ic + d) / 2.0,
                ]
            }
            (Polarisation::Circular, Polarisation::Linear) => [
                (a + b + c + d) / 2.0,
                times_i(a - b + c - d) / 2.0,
                times_i(-a - b + c + d) / 2.0,
                (a - b - c + d) / 2.0,
            ],
            _ => unreachable!("equal representations return above"),
        };
        self.polarisation = target;
    }
}

/// Parse a flux unit and check its dimensions as `FluxRep` does.
fn flux_unit(name: &str) -> Result<Unit, ComponentError> {
    let unit = Unit::new(name).map_err(|error| ComponentError::Unit {
        what: "flux",
        unit: name.to_string(),
        reason: error.to_string(),
    })?;
    let allowed = ALLOWED_FLUX_UNITS
        .iter()
        .any(|allowed| unit.conformant(&Unit::new(allowed).expect("allowed flux units parse")));
    if allowed {
        Ok(unit)
    } else {
        Err(ComponentError::FluxUnit(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flux_units_need_flux_dimensions_and_keep_their_names() {
        let flux = Flux::new([Complex64::new(1.0, 0.0); 4], "mJy", Polarisation::Linear)
            .expect("mJy is a flux unit");
        assert_eq!(flux.unit().name(), "mJy");
        assert!(
            Flux::new(
                [Complex64::new(1.0, 0.0); 4],
                "K.rad.rad",
                Polarisation::Stokes
            )
            .is_ok()
        );
        assert!(
            Flux::new(
                [Complex64::new(1.0, 0.0); 4],
                "Jy.km/s",
                Polarisation::Stokes
            )
            .is_ok()
        );
        assert_eq!(
            Flux::new([Complex64::new(1.0, 0.0); 4], "m", Polarisation::Stokes),
            Err(ComponentError::FluxUnit("m".to_string()))
        );
        assert!(matches!(
            Flux::new(
                [Complex64::new(1.0, 0.0); 4],
                "notaunit!",
                Polarisation::Stokes
            ),
            Err(ComponentError::Unit { .. })
        ));
    }

    #[test]
    fn complex_polarisation_conversion_matches_casa_and_round_trips() {
        let original = [
            Complex64::new(4.0, 2.0),
            Complex64::new(3.0, 4.0),
            Complex64::new(1.0, -2.0),
            Complex64::new(2.0, -1.0),
        ];
        let errors = [Complex64::new(0.1, 0.2); 4];
        let mut flux = Flux::new(original, "mJy", Polarisation::Linear)
            .expect("valid unit")
            .with_errors(errors);
        flux.convert_polarisation(Polarisation::Circular);
        assert_eq!(
            flux.values(),
            &[
                Complex64::new(6.0, -0.5),
                Complex64::new(0.0, 3.5),
                Complex64::new(2.0, -0.5),
                Complex64::new(0.0, 1.5),
            ]
        );
        assert_eq!(flux.unit().name(), "mJy");
        assert_eq!(flux.errors(), &errors);
        flux.convert_polarisation(Polarisation::Stokes);
        flux.convert_polarisation(Polarisation::Linear);
        assert_eq!(flux.values(), &original);
        assert_eq!(flux.errors(), &errors);
    }
}
