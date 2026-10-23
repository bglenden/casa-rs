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
/// | [`Polarisation::Stokes`] | I, Q, U, V (real; the imaginary parts are zero) |
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
}
