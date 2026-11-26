// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::quanta::{Quantity, Unit, casacore};
use casa_images::{BeamComponent, GaussianBeam, StoredGaussianBeam};
use pyo3::prelude::*;

use crate::convert::{Variant, read_quantity, value::CasaValue};

use super::PyImageTool;
use super::handle::raised;

fn quantities(argument: Option<Variant>, label: &str) -> PyResult<Vec<Quantity>> {
    let strings = match argument {
        Some(Variant::Text(value)) => {
            let separator = if value.contains(',') { ',' } else { ' ' };
            if value.contains(separator) {
                value
                    .split(separator)
                    .filter(|part| !part.is_empty() && *part != " ")
                    .map(str::to_owned)
                    .collect()
            } else {
                vec![value]
            }
        }
        Some(Variant::Texts(values)) => values,
        _ => return Err(raised(format!("Cannot understand {label} values"))),
    };
    strings
        .iter()
        .map(|value| {
            if value.contains("pix") {
                // CASA accepts this unit in the list reader. All beam
                // components that use it fail the angular-unit check.
                return Quantity::new(0.0, "pix").map_err(raised);
            }
            read_quantity(value, &format!(" In converting quantity {value}")).map_err(Into::into)
        })
        .collect()
}

fn stored(values: &[Quantity]) -> PyResult<StoredGaussianBeam> {
    let major = &values[0];
    let minor = values.get(1).unwrap_or(major);
    let zero = Quantity::new(0.0, "deg").expect("built-in unit");
    let pa = values.get(2).unwrap_or(&zero);
    let rad = Unit::new("rad").expect("built-in unit");
    for (label, quantity) in [
        ("Major axis", major),
        ("Minor axis", minor),
        ("Position angle", pa),
    ] {
        if !quantity.unit().conformant(&rad) {
            return Err(raised(format!(
                "Exception: {label} must have angular units ({} is not)..",
                quantity.unit().name()
            )));
        }
    }
    for (label, quantity) in [("Major axis", major), ("Minor axis", minor)] {
        if quantity.value() < 0.0 {
            return Err(raised(format!(
                "Exception: {label} cannot be less than zero.."
            )));
        }
    }
    if major.value() < casacore::get_conformant_value(minor, major.unit()).map_err(raised)? {
        return Err(raised(
            "Exception: Major axis must be greater or equal to minor axis.",
        ));
    }
    StoredGaussianBeam::new(major.clone(), minor.clone(), pa.clone()).map_err(raised)
}

fn radians(beam: &StoredGaussianBeam) -> PyResult<GaussianBeam> {
    let unit = Unit::new("rad").expect("built-in unit");
    let angular =
        |quantity: &Quantity| casacore::get_conformant_value(quantity, &unit).map_err(raised);
    Ok(GaussianBeam::new(
        angular(beam.component(BeamComponent::Major))?,
        angular(beam.component(BeamComponent::Minor))?,
        angular(&beam.wrapped_position_angle().map_err(raised)?)?,
    ))
}

/// CASA's tool distinguishes point sources, failures, and resolved fits.
/// The point-source limit depends on the width of the smaller beam.
fn fit_size(total: GaussianBeam, applied: GaussianBeam) -> Option<(GaussianBeam, bool)> {
    let (total_sin, total_cos) = (total.position_angle.sin(), total.position_angle.cos());
    let (beam_sin, beam_cos) = (applied.position_angle.sin(), applied.position_angle.cos());
    let alpha = (total.major * total_cos).powi(2) + (total.minor * total_sin).powi(2)
        - (applied.major * beam_cos).powi(2)
        - (applied.minor * beam_sin).powi(2);
    let beta = (total.major * total_sin).powi(2) + (total.minor * total_cos).powi(2)
        - (applied.major * beam_sin).powi(2)
        - (applied.minor * beam_cos).powi(2);
    let gamma = 2.0
        * ((total.minor.powi(2) - total.major.powi(2)) * total_sin * total_cos
            - (applied.minor.powi(2) - applied.major.powi(2)) * beam_sin * beam_cos);
    let sum = alpha + beta;
    let separation = ((alpha - beta).powi(2) + gamma.powi(2)).sqrt();
    let smallest = total.minor.min(applied.minor);
    let limit = 0.1 * smallest * smallest;
    if alpha < 0.0 || beta < 0.0 || sum < separation {
        return (0.5 * (sum - separation) < limit && alpha > -limit && beta > -limit)
            .then_some((applied, true));
    }
    let pa = if gamma.abs() + (alpha - beta).abs() == 0.0 {
        0.0
    } else {
        0.5 * (-gamma).atan2(alpha - beta)
    };
    Some((
        GaussianBeam::new(
            (0.5 * (sum + separation)).sqrt(),
            (0.5 * (sum - separation)).sqrt(),
            pa,
        ),
        false,
    ))
}

fn fit_record(beam: GaussianBeam) -> PyResult<Vec<(&'static str, CasaValue)>> {
    let angle = Quantity::new(beam.position_angle, "rad").map_err(raised)?;
    let angle = casa_images::beam::wrap_position_angle(&angle).map_err(raised)?;
    let unit = Unit::new("deg").expect("built-in unit");
    Ok(vec![
        (
            "major",
            CasaValue::quantity(beam.major_in("arcsec").map_err(raised)?, "arcsec"),
        ),
        (
            "minor",
            CasaValue::quantity(beam.minor_in("arcsec").map_err(raised)?, "arcsec"),
        ),
        (
            "pa",
            CasaValue::quantity(
                casacore::get_conformant_value(&angle, &unit).map_err(raised)?,
                "deg",
            ),
        ),
    ])
}

fn describe(beam: &StoredGaussianBeam) -> String {
    let quantity = |component| {
        let quantity = beam.component(component);
        format!(
            "{} {}",
            casacore::stream_double(quantity.value()),
            quantity.unit().name()
        )
    };
    format!(
        "major: {}, minor: {}, pa: {}",
        quantity(BeamComponent::Major),
        quantity(BeamComponent::Minor),
        quantity(BeamComponent::PositionAngle)
    )
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::unused_self)]
    #[pyo3(signature = (source=None, convolved=None), text_signature = "($self, source=[], convolved=[])")]
    fn beamforconvolvedsize(
        &self,
        py: Python<'_>,
        source: Option<Variant>,
        convolved: Option<Variant>,
    ) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            let source = quantities(source, "source")?;
            if source.len() != 3 {
                return Err(raised("Cannot understand source values"));
            }
            let convolved = quantities(convolved, "target")?;
            if convolved.len() < 3 {
                return Err(raised("Cannot understand target values"));
            }
            let source = stored(&source)?;
            let convolved = stored(&convolved)?;
            let Some((fit, false)) = fit_size(radians(&convolved)?, radians(&source)?) else {
                return Err(raised(format!(
                    "Unable to reach target resolution of {} Input source {} is probably too large.",
                    describe(&convolved), describe(&source),
                )));
            };
            Ok(CasaValue::record(fit_record(fit)?))
        })?
        .into_python(py)
    }

    #[allow(clippy::unused_self)]
    #[pyo3(signature = (source=None, beam=None), text_signature = "($self, source=[], beam=[])")]
    fn deconvolvefrombeam(
        &self,
        py: Python<'_>,
        source: Option<Variant>,
        beam: Option<Variant>,
    ) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            let source = quantities(source, "source")?;
            if !(1..=3).contains(&source.len()) {
                return Err(raised("Cannot understand source values"));
            }
            let source = stored(&source)?;
            let beam = quantities(beam, "beam")?;
            if beam.is_empty() {
                return Err(raised("Cannot understand beam values"));
            }
            let beam = stored(&beam)?;
            let result = fit_size(radians(&source)?, radians(&beam)?);
            let (fit, unresolved) = result.unwrap_or((GaussianBeam::new(0.0, 0.0, 0.0), false));
            let mut fit = fit_record(fit)?;
            fit.push(("success", CasaValue::Bool(result.is_some())));
            Ok(CasaValue::record([
                ("fit", CasaValue::record(fit)),
                ("return", CasaValue::Bool(unresolved)),
            ]))
        })?
        .into_python(py)
    }
}
