// SPDX-License-Identifier: LGPL-3.0-or-later

//! Peak flux <-> integrated flux conversion (`ia.convertflux`).

use casa_coordinates::{Coordinate, CoordinateSystem, CoordinateType};
use casa_core::quanta::unit_val::UnitVal;
use casa_core::quanta::{Quantity, Unit, casacore};
use casa_images::{BeamComponent, GaussianBeam, ImagePixelType};

use super::attachment::AttachedImage;
use super::beam_view::BeamView;
use super::beams::{RestoringBeamQuery, beam_for_units, restoring_beam};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, with_pixels, with_storage};

/// Converted flux quantity returned by [`convert_flux`].
#[derive(Debug, Clone)]
pub struct ConvertedFlux {
    /// Numeric value in `unit`.
    pub value: f64,
    /// Unit string (e.g. `"Jy"` or `"Jy/beam"`).
    pub unit: String,
}

/// Converts between peak brightness (`Jy/beam`) and integrated flux (`Jy`) using the
/// restoring beam and component major/minor FWHMs (`ia.convertflux`).
#[allow(clippy::too_many_arguments)]
pub fn convert_flux(
    attached: &AttachedImage,
    value: f64,
    unit: &str,
    major_arcsec: f64,
    minor_arcsec: f64,
    _type_str: &str,
    topeak: bool,
    channel: isize,
    polarization: isize,
) -> ImageToolResult<ConvertedFlux> {
    if major_arcsec <= 0.0 || minor_arcsec <= 0.0 {
        return Err(ImageToolError::new(
            "Major and minor component widths must be positive.",
        ));
    }
    let beam: GaussianBeam = match restoring_beam(attached, channel, polarization)? {
        RestoringBeamQuery::Single(b) => b,
        RestoringBeamQuery::Empty => {
            // Fall back to pixel solid angle if brightness unit is Jy/pixel
            let image = attached.lock();
            let coords = image.coordinates();
            if let Some(dir_idx) = coords.find_coordinate(CoordinateType::Direction) {
                let dir = coords.coordinate(dir_idx);
                let inc = dir.increment();
                let rad_to_arcsec = 180.0 * 3600.0 / std::f64::consts::PI;
                let pix_area_arcsec2 =
                    (inc[0] * rad_to_arcsec).abs() * (inc[1] * rad_to_arcsec).abs();
                let src_area =
                    std::f64::consts::PI / (4.0 * 2.0_f64.ln()) * major_arcsec * minor_arcsec;
                let factor = src_area / pix_area_arcsec2;
                let out_val = if topeak {
                    value / factor
                } else {
                    value * factor
                };
                let out_unit = if topeak {
                    format!("{unit}/pixel").replace("/pixel/pixel", "/pixel")
                } else {
                    unit.replace("/pixel", "")
                };
                return Ok(ConvertedFlux {
                    value: out_val,
                    unit: out_unit,
                });
            }
            return Err(ImageToolError::new(
                "Image has no restoring beam or direction pixel size.",
            ));
        }
        RestoringBeamQuery::PerPlane(_) => {
            return Err(ImageToolError::new(
                "Image has per-plane beams; specify channel and polarization.",
            ));
        }
    };

    let beam_maj = beam.major_in("arcsec")?;
    let beam_min = beam.minor_in("arcsec")?;
    if beam_maj <= 0.0 || beam_min <= 0.0 {
        return Err(ImageToolError::new("Restoring beam axes must be positive."));
    }
    let beam_factor = (major_arcsec * minor_arcsec) / (beam_maj * beam_min);
    let out_val = if topeak {
        value / beam_factor
    } else {
        value * beam_factor
    };
    let clean_unit = if unit.trim().is_empty() {
        "Jy"
    } else {
        unit.trim()
    };
    let out_unit = if topeak {
        if clean_unit.contains("/beam") {
            clean_unit.to_owned()
        } else {
            format!("{clean_unit}/beam")
        }
    } else {
        clean_unit.trim_end_matches("/beam").to_owned()
    };

    Ok(ConvertedFlux {
        value: out_val,
        unit: out_unit,
    })
}

struct FluxAreas {
    pixel: f64,
    beam: Option<f64>,
}

impl FluxAreas {
    fn unit(&self, name: &str) -> ImageToolResult<Unit> {
        let area = UnitVal::ANGLE.pow(2).dim;
        let pixel = ("pixel", UnitVal::new(self.pixel, area));
        let unit = if name.contains("beam") {
            let beam = self.beam.ok_or_else(|| {
                ImageToolError::new(format!(
                    "No beam defined even though the image brightness units are {name}"
                ))
            })?;
            Unit::with_local_definitions(name, &[pixel, ("beam", UnitVal::new(beam, area))])
        } else {
            Unit::with_local_definitions(name, &[pixel])
        }?;
        Ok(unit)
    }

    fn brightness_unit(&self, name: &str, logs: &mut Vec<ToolLogLine>) -> ImageToolResult<Unit> {
        let unit = self.unit(name)?;
        let dimensions = unit.val().dim * UnitVal::ANGLE.pow(2).dim;
        let jy = Unit::new("Jy").expect("built-in flux unit");
        if matches!(name, "K" | "Jy/beam.km/s") || dimensions == jy.val().dim {
            return Ok(unit);
        }
        if name.is_empty() {
            logs.push(ToolLogLine::warning(
                "There are no image brightness units, assuming Jy/pixel",
            ));
        } else {
            logs.push(ToolLogLine::warning(format!(
                "The image units '{name}' are not consistent with Jy when integrated over the sky.  Assuming Jy/pixel"
            )));
        }
        self.unit("Jy/pixel")
    }
}

fn checked_axis(axis: &Quantity, label: &str, radians: &Unit) -> ImageToolResult<Quantity> {
    if !axis.unit().conformant(radians) {
        return Err(ImageToolError::new(format!(
            "Exception: {label} axis must have angular units ({} is not)..",
            axis.unit().name()
        )));
    }
    if axis.value() < 0.0 {
        return Err(ImageToolError::new(format!(
            "Exception: {label} axis cannot be less than zero.."
        )));
    }
    Ok(casacore::convert(axis, Some(radians))?)
}

/// Converts between peak brightness and integrated flux using CASA [`Quantity`] rules (`ia.convertflux`).
#[allow(clippy::too_many_arguments)]
pub fn convert_flux_quantity(
    image: &ImageHandle,
    beam_view: &BeamView,
    value: Quantity,
    axes: [Quantity; 2],
    to_peak: bool,
    plane: (i64, i64),
) -> ImageToolResult<(Quantity, Vec<ToolLogLine>)> {
    if image.kind() != ImagePixelType::Float32 {
        return Err(ImageToolError::new(
            "This method only supports Float valued images",
        ));
    }
    let (_, direction) = CoordinateSystem::first_direction_in_record(&image.coordinate_record())?
        .ok_or_else(|| {
        let name = image
            .path()
            .map_or_else(|| "Temporary_Image".into(), |path| path.to_string_lossy());
        ImageToolError::new(format!(
            "Exception: Image {name} does not have required coordinate Direction."
        ))
    })?;
    let radians = Unit::new("rad").expect("built-in angle unit");
    let major = checked_axis(&axes[0], "Major", &radians)?;
    let minor = checked_axis(&axes[1], "Minor", &radians)?;
    if major.value() < minor.value() {
        return Err(ImageToolError::new(
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
            return Err(ImageToolError::new(
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
    let mut logs = Vec::new();
    let result = if to_peak {
        let target = if value.unit().conformant(&jy) {
            &jy
        } else if value.unit().conformant(&jykms) {
            &jykms
        } else if value.unit().conformant(&kelvin_area) {
            value.unit()
        } else {
            return Err(ImageToolError::new(format!(
                "Cannot convert units of Flux integral ({}) to Jy",
                value.unit().name()
            )));
        };
        let flux = casacore::convert(&value, Some(target))?;
        let scaled = Quantity::with_unit(flux.value() / gaussian, flux.unit().clone());
        let scaled = casacore::div(&scaled, &major)?;
        let peak = casacore::div(&scaled, &minor)?;
        let brightness = areas.brightness_unit(&brightness, &mut logs)?;
        casacore::convert(&peak, Some(&brightness))?
    } else {
        let brightness = areas.brightness_unit(value.unit().name(), &mut logs)?;
        let integrated = areas.unit(&format!("{}.rad.rad", brightness.name()))?;
        let flux = Quantity::with_unit(
            value.value() * gaussian * major.value() * minor.value(),
            integrated,
        );
        if flux.unit().conformant(&jy) {
            casacore::convert(&flux, Some(&jy))?
        } else if flux.unit().conformant(&jykms) {
            casacore::convert(&flux, Some(&jykms))?
        } else if flux.unit().conformant(&kelvin_area) {
            flux
        } else {
            logs.push(ToolLogLine::warning(
                "Cannot convert units of Flux integral to Jy - will assume Jy",
            ));
            Quantity::with_unit(flux.value(), jy)
        }
    };
    Ok((result, logs))
}
