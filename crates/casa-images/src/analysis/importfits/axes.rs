// SPDX-License-Identifier: LGPL-3.0-or-later
//! Added singleton coordinates and axis ordering for CASA importfits.

use casa_contracts::ParameterValue;
use casa_coordinates::{
    CoordinateModel, CoordinateSystem, CoordinateType, DirectionCoordinate, Projection,
    ProjectionType, SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::{
    Quantity, Unit,
    casacore::{get_conformant_value, get_value},
    format::{ANGLE, format_angle},
};
use casa_types::{ArrayValue, RecordValue, Value};

use super::{ImportFitsRequest, invalid_type, reject};
use crate::ImageError;
use crate::fits_read::FitsToolImport;

const STANDARD: [&str; 4] = ["Right Ascension", "Declination", "Stokes", "Frequency"];

pub(super) fn apply(
    source: &mut FitsToolImport,
    request: &ImportFitsRequest,
) -> Result<Vec<usize>, ImageError> {
    if !request.defaultaxes {
        return Ok((0..source.shape().len()).collect());
    }
    let [ra, dec, frequency, stokes] = request.defaultaxesvalues.as_slice() else {
        return Err(invalid_type(
            "defaultaxesvalues",
            "When defaultaxes==True, parameter defaultaxesvalues must be provided as a list of 4 values: RA, Dec, Freq, Stokes",
        ));
    };
    // CASA probes the default FITS image before opening the selected HDU
    // and representation. It uses this probe to decide which references to set.
    let selected_default = request.whichrep == 0 && request.whichhdu <= 0;
    let default_image = if selected_default {
        None
    } else {
        Some(FitsToolImport::open(&request.fitsimage, 0, -1)?)
    };
    let probe = default_image.as_ref().unwrap_or(source).coordinates();
    let names = axis_names(probe);
    let need_direction = !(has_name(&names, STANDARD[0]) && has_name(&names, STANDARD[1]));
    let need_spectral = !has_name(&names, STANDARD[3]);
    let need_stokes = !has_name(&names, STANDARD[2]);
    for (needed, kind, name) in [
        (need_direction, CoordinateType::Direction, "direction"),
        (need_spectral, CoordinateType::Spectral, "spectral"),
        (need_stokes, CoordinateType::Stokes, "Stokes"),
    ] {
        if needed && probe.find_coordinate(kind).is_some() {
            return Err(reject(format!(
                "Non-standard {name} axes. Cannot add default axes."
            )));
        }
    }
    let add_axes = need_direction || need_spectral || need_stokes;
    let reorder = !add_axes && names != STANDARD;

    let coordinates = &mut source.coordinates().clone();
    let record = &mut source.coordinate_record;
    if add_axes {
        append_missing(record, coordinates, stokes)?;
        if need_direction {
            let axis = first_axis(coordinates, CoordinateType::Direction)?;
            for (axis, label, value) in [(axis, "RA", ra), (axis + 1, "DEC", dec)] {
                let degrees = axis_default(value, label, "deg")?;
                CoordinateSystem::set_axis_unit_in_record(record, axis, "deg")?;
                CoordinateSystem::set_reference_value_in_record(record, axis, degrees)?;
            }
        }
        if need_spectral {
            let axis = first_axis(coordinates, CoordinateType::Spectral)?;
            let hz = axis_default(frequency, "Freq", "Hz")?;
            CoordinateSystem::set_axis_unit_in_record(record, axis, "Hz")?;
            CoordinateSystem::set_reference_value_in_record(record, axis, hz)?;
            CoordinateSystem::set_rest_frequency_in_record(record, hz)?;
        }
    }
    let names = axis_names(coordinates);
    if reorder || (add_axes && names.len() == STANDARD.len()) {
        return order_standard(record, &names);
    }
    Ok((0..names.len()).collect())
}

fn axis_names(coordinates: &CoordinateSystem) -> Vec<String> {
    (0..coordinates.n_pixel_axes())
        .map(|axis| {
            let (coordinate, local) = coordinates
                .pixel_axis_owner(axis)
                .expect("dense pixel axes");
            coordinates
                .coordinate(coordinate)
                .axis_names()
                .remove(local)
        })
        .collect()
}

fn has_name(names: &[String], name: &str) -> bool {
    names.iter().any(|item| item == name)
}

fn append_missing(
    record: &mut RecordValue,
    coordinates: &mut CoordinateSystem,
    stokes: &ParameterValue,
) -> Result<(), ImageError> {
    if coordinates
        .find_coordinate(CoordinateType::Direction)
        .is_none()
    {
        let arcminute = (1.0_f64 / 60.0).to_radians();
        let direction = DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.0, 0.0],
            [-arcminute, arcminute],
            [0.0, 0.0],
        )
        .with_world_units(["'", "'"])?;
        append(record, coordinates, "direction", direction.into());
    }
    if coordinates
        .find_coordinate(CoordinateType::Spectral)
        .is_none()
    {
        // casacore CoordinateUtil::addFreqAxis uses the HI rest frequency.
        let spectral =
            SpectralCoordinate::new(FrequencyRef::LSRK, 1.415e9, 1e3, 0.0, 1.420_405_751_786e9);
        append(record, coordinates, "spectral", spectral.into());
    }
    let ParameterValue::String(stokes) = stokes else {
        return Err(invalid_type(
            "defaultaxesvalues",
            "The Stokes default value must be a string",
        ));
    };
    if !stokes.is_empty()
        && coordinates
            .find_coordinate(CoordinateType::Stokes)
            .is_none()
    {
        let stokes = StokesType::from_name(stokes).unwrap_or(StokesType::Undefined);
        let stokes = StokesCoordinate::new(vec![stokes]);
        append(record, coordinates, "stokes", stokes.into());
    }
    Ok(())
}

fn append(
    record: &mut RecordValue,
    coordinates: &mut CoordinateSystem,
    kind: &str,
    coordinate: CoordinateModel,
) {
    let index = coordinates.n_coordinates();
    coordinates.add_coordinate(coordinate);
    let appended = coordinates.to_record();
    for prefix in [kind, "pixelmap", "worldmap", "pixelreplace", "worldreplace"] {
        let field = format!("{prefix}{index}");
        record.upsert(
            &field,
            appended
                .get(&field)
                .expect("generated coordinate field")
                .clone(),
        );
    }
}

fn first_axis(coordinates: &CoordinateSystem, kind: CoordinateType) -> Result<usize, ImageError> {
    let index = coordinates
        .find_coordinate(kind)
        .ok_or_else(|| reject(format!("No {kind:?} coordinate in the selected FITS image")))?;
    coordinates
        .coordinate_pixel_axes(index)
        .expect("existing coordinate")
        .into_iter()
        .flatten()
        .next()
        .ok_or_else(|| {
            reject(format!(
                "No visible {kind:?} axis in the selected FITS image"
            ))
        })
}

fn order_standard(record: &mut RecordValue, names: &[String]) -> Result<Vec<usize>, ImageError> {
    if names == STANDARD {
        return Ok((0..names.len()).collect());
    }
    if names.len() != STANDARD.len() {
        return Err(reject(
            "Cannot transpose the selected FITS image to four standard axes",
        ));
    }
    let order = STANDARD
        .map(|name| {
            names.iter().position(|item| item == name).ok_or_else(|| {
                reject(format!(
                    "Axis {name:?} is missing from the selected FITS image"
                ))
            })
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    for index in 0..CoordinateSystem::record_coordinates(record)?.len() {
        for prefix in ["pixelmap", "worldmap"] {
            if let Some(Value::Array(ArrayValue::Int32(map))) =
                record.get_mut(&format!("{prefix}{index}"))
            {
                for axis in map {
                    *axis = order
                        .iter()
                        .position(|&i| i as i32 == *axis)
                        .ok_or_else(|| reject("Invalid FITS coordinate axis map"))?
                        as i32;
                }
            }
        }
    }
    Ok(order)
}

fn axis_default(value: &ParameterValue, label: &str, unit: &str) -> Result<f64, ImageError> {
    let invalid = || {
        reject(format!(
            "{label} default value is not a valid {unit} quantity: {value:?}"
        ))
    };
    let number = match value {
        ParameterValue::Integer(_) if unit == "Hz" => {
            return Err(reject("Unrecognized format for world coordinate"));
        }
        ParameterValue::Integer(value) => *value as f64,
        ParameterValue::Float(value) => *value,
        ParameterValue::String(text) => {
            let quantity = Quantity::read(text).map_err(|_| invalid())?;
            if unit == "Hz" && !quantity.unit().name().contains("Hz") {
                return Err(invalid_type(
                    "defaultaxesvalues",
                    format!("Freq default value is not a valid frequency quantity: {text}"),
                ));
            }
            let target = Unit::new(unit).expect("built-in CASA axis unit");
            if unit == "deg" {
                let radians = Unit::new("rad").expect("built-in angle unit");
                let seconds = Unit::new("s").expect("built-in time unit");
                if !quantity.value().is_finite()
                    || !(quantity.unit().conformant(&radians)
                        || quantity.unit().conformant(&seconds))
                {
                    return Err(invalid());
                }
                // The CASA task sends strings through qa.angle() and parses
                // the default whole-arcsecond representation.
                let angle = format_angle(get_value(&quantity, &radians), ANGLE, 0);
                let rounded = Quantity::read(&angle).map_err(|_| invalid())?;
                get_conformant_value(&rounded, &target).map_err(|_| invalid())?
            } else {
                get_conformant_value(&quantity, &target).map_err(|_| invalid())?
            }
        }
        _ => return Err(invalid()),
    };
    if !number.is_finite() {
        return Err(invalid());
    }
    Ok(number)
}
