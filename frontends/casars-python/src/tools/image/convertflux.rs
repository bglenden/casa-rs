// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateSystem};
use casa_images::{BeamComponent, GaussianBeam, ImagePixelType};
use casa_types::quanta::unit_val::UnitVal;
use casa_types::quanta::{Quantity, Unit, casacore};
use pyo3::prelude::*;

use crate::convert::{Variant, quantity, value::CasaValue};
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::beam_edit::beam_for_units;
use super::beam_view::BeamView;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

struct FluxAreas {
    pixel: f64,
    beam: Option<f64>,
}

impl FluxAreas {
    fn unit(&self, name: &str) -> PyResult<Unit> {
        let area = UnitVal::ANGLE.pow(2).dim;
        let pixel = ("pixel", UnitVal::new(self.pixel, area));
        if name.contains("beam") {
            let beam = self.beam.ok_or_else(|| {
                raised(format!(
                    "No beam defined even though the image brightness units are {name}"
                ))
            })?;
            Unit::with_local_definitions(name, &[pixel, ("beam", UnitVal::new(beam, area))])
        } else {
            Unit::with_local_definitions(name, &[pixel])
        }
        .map_err(raised)
    }

    fn brightness_unit(&self, name: &str, logs: &mut Vec<LogLine>) -> PyResult<Unit> {
        let unit = self.unit(name)?;
        let dimensions = unit.val().dim * UnitVal::ANGLE.pow(2).dim;
        let jy = Unit::new("Jy").expect("built-in flux unit");
        if matches!(name, "K" | "Jy/beam.km/s") || dimensions == jy.val().dim {
            return Ok(unit);
        }
        if name.is_empty() {
            logs.push(LogLine::warning(
                "There are no image brightness units, assuming Jy/pixel",
            ));
        } else {
            logs.push(LogLine::warning(format!(
                "The image units '{name}' are not consistent with Jy when integrated over the sky.  Assuming Jy/pixel"
            )));
        }
        self.unit("Jy/pixel")
    }
}

fn checked_axis(axis: &Quantity, label: &str, radians: &Unit) -> PyResult<Quantity> {
    if !axis.unit().conformant(radians) {
        return Err(raised(format!(
            "Exception: {label} axis must have angular units ({} is not)..",
            axis.unit().name()
        )));
    }
    if axis.value() < 0.0 {
        return Err(raised(format!(
            "Exception: {label} axis cannot be less than zero.."
        )));
    }
    casacore::convert(axis, Some(radians)).map_err(raised)
}

fn flux_conversion(
    image: &ImageHandle,
    beam_view: &BeamView,
    value: Quantity,
    axes: [Quantity; 2],
    to_peak: bool,
    plane: (i64, i64),
    logs: &mut Vec<LogLine>,
) -> PyResult<CasaValue> {
    let (_, direction) = CoordinateSystem::first_direction_in_record(&image.coordinate_record())
        .map_err(raised)?
        .ok_or_else(|| {
            let name = image
                .path()
                .map_or_else(|| "Temporary_Image".into(), |path| path.to_string_lossy());
            raised(format!(
                "Exception: Image {name} does not have required coordinate Direction."
            ))
        })?;
    let radians = Unit::new("rad").expect("built-in angle unit");
    let major = checked_axis(&axes[0], "Major", &radians)?;
    let minor = checked_axis(&axes[1], "Minor", &radians)?;
    if major.value() < minor.value() {
        return Err(raised(
            "Exception: Major axis must be greater or equal to minor axis.",
        ));
    }
    let brightness =
        with_pixels!(image, storage => with_storage!(storage, image => image.units().to_owned()));
    let beam = beam_for_units(beam_view, plane.0, plane.1)?.map(|beam| {
        GaussianBeam::new(
            beam.component(BeamComponent::Major).get_si_value(),
            beam.component(BeamComponent::Minor).get_si_value(),
            0.0,
        )
    });
    let inc = direction.increment();
    let pixel = (inc[0] * inc[1]).abs();
    let mut beam = beam.filter(|beam| !beam.is_null()).map(|beam| beam.area());
    if brightness.contains("/beam") && beam.is_none() {
        // CASA's fallback beam uses the numerical coordinate increments in
        // their saved units as the full-width axes in radians.
        let (_, increments) = direction.reference_and_increment_in_world_units();
        let (major, minor) = (increments[0].abs(), increments[1].abs());
        if major < minor {
            return Err(raised(
                "Exception: Major axis must be greater or equal to minor axis.",
            ));
        }
        let fallback = GaussianBeam::new(major, minor, 0.0);
        beam = (!fallback.is_null()).then(|| fallback.area());
    }
    let areas = FluxAreas { pixel, beam };
    let jy = Unit::new("Jy").expect("built-in flux unit");
    let jykms = Unit::new("Jy.km/s").expect("built-in integrated flux unit");
    let kelvin_area = Unit::new("K.rad2").expect("built-in temperature-area unit");
    let gaussian = std::f64::consts::PI / 4.0 / 2.0_f64.ln();
    let result = if to_peak {
        let target = if value.unit().conformant(&jy) {
            &jy
        } else if value.unit().conformant(&jykms) {
            &jykms
        } else if value.unit().conformant(&kelvin_area) {
            value.unit()
        } else {
            return Err(raised(format!(
                "Cannot convert units of Flux integral ({}) to Jy",
                value.unit().name()
            )));
        };
        let flux = casacore::convert(&value, Some(target)).map_err(raised)?;
        let scaled = Quantity::with_unit(flux.value() / gaussian, flux.unit().clone());
        let scaled = casacore::div(&scaled, &major).map_err(raised)?;
        let peak = casacore::div(&scaled, &minor).map_err(raised)?;
        let brightness = areas.brightness_unit(&brightness, logs)?;
        casacore::convert(&peak, Some(&brightness)).map_err(raised)?
    } else {
        let brightness = areas.brightness_unit(value.unit().name(), logs)?;
        let integrated = areas.unit(&format!("{}.rad.rad", brightness.name()))?;
        let flux = Quantity::with_unit(
            value.value() * gaussian * major.value() * minor.value(),
            integrated,
        );
        if flux.unit().conformant(&jy) {
            casacore::convert(&flux, Some(&jy)).map_err(raised)?
        } else if flux.unit().conformant(&jykms) {
            casacore::convert(&flux, Some(&jykms)).map_err(raised)?
        } else if flux.unit().conformant(&kelvin_area) {
            flux
        } else {
            logs.push(LogLine::warning(
                "Cannot convert units of Flux integral to Jy - will assume Jy",
            ));
            Quantity::with_unit(flux.value(), jy)
        }
    };
    Ok(CasaValue::quantity(result.value(), result.unit().name()))
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (value=None, major=None, minor=None, r#type="Gaussian", topeak=true, channel=-1, polarization=-1), text_signature = "($self, value=[], major=[], minor=[], type='Gaussian', topeak=True, channel=-1, polarization=-1)")]
    fn convertflux(
        &self,
        py: Python<'_>,
        value: Option<Variant>,
        major: Option<Variant>,
        minor: Option<Variant>,
        r#type: &str,
        topeak: bool,
        channel: i64,
        polarization: i64,
    ) -> PyResult<Py<PyAny>> {
        let _ = r#type; // CASA always uses the Gaussian shape here.
        let mut logs = Vec::new();
        let result = py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(CasaValue::empty_record());
            };
            let image = attachment.lock();
            if image.kind() != ImagePixelType::Float32 {
                return Err(raised("This method only supports Float valued images"));
            }
            flux_conversion(
                &image,
                &attachment.beam_view(),
                quantity(&value.unwrap_or(Variant::Empty))?,
                [
                    quantity(&major.unwrap_or(Variant::Empty))?,
                    quantity(&minor.unwrap_or(Variant::Empty))?,
                ],
                topeak,
                (channel, polarization),
                &mut logs,
            )
        });
        emit(py, "casars.tools.image", &logs)?;
        result?.into_python(py)
    }
}
