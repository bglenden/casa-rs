// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pure-Rust coordinate defaults, validation and display formatting helpers.

use casa_coordinates::display::{
    format_latitude_dms_with_precision, format_longitude_hours_with_precision, format_stream_value,
    format_stream_value_with_precision,
};
use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, CoordinateType, ObsInfo, QualityType,
    SpectralCoordinate, StokesType, defaults,
};
use casa_core::measures::MeasFrame;
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::measures::epoch::{EpochRef, MEpoch};
use casa_core::measures::position::MPosition;
use casa_core::measures::record::{direction_to_record, epoch_to_record, position_to_record};
use casa_core::quanta::dim::{Dimension, UnitDim};
use casa_core::quanta::{Quantity, Unit, casacore};
use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::Array2;

use super::error::{ImageToolError, ImageToolResult};

/// Speed of light in meters per second.
pub const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

/// Returns the default observatory metadata used when creating new CASA images.
pub fn default_image_obs_info() -> ObsInfo {
    // ITRF position of ALMA from the casacore Observatories table.
    let (sin_lon, cos_lon) = (-1.182_546_595_504_989_2_f64).sin_cos();
    let (sin_lat, cos_lat) = (-0.399_414_986_926_273_8_f64).sin_cos();
    let radius = 6_379_946.013_264_43_f64;
    defaults::obs_info().with_telescope_position(MPosition::new_itrf(
        cos_lon * cos_lat * radius,
        sin_lon * cos_lat * radius,
        sin_lat * radius,
    ))
}

/// Returns the default spectral conversion sub-record used by CASA coordinates.
pub fn default_spectral_conversion() -> RecordValue {
    // LAST is casacore's placeholder for an unset conversion frame.
    let epoch = epoch_to_record(&MEpoch::from_mjd(0.0, EpochRef::LAST));
    RecordValue::new(vec![
        RecordField::new("epoch", Value::Record(epoch)),
        RecordField::new(
            "direction",
            Value::Record(direction_to_record(&MDirection::from_angles(
                0.0,
                std::f64::consts::FRAC_PI_2,
                DirectionRef::J2000,
            ))),
        ),
        RecordField::new(
            "position",
            Value::Record(position_to_record(&MPosition::new_itrf(0.0, 0.0, 0.0))),
        ),
        RecordField::new("system", Value::Scalar(ScalarValue::String("LSRK".into()))),
    ])
}

/// Builds the default centered coordinate record for an image of `shape`.
pub fn default_record_for_shape(shape: &[usize], linear: bool) -> ImageToolResult<RecordValue> {
    let coordinates = defaults::coordinate_system_for_shape(shape, linear)
        .with_obs_info(default_image_obs_info());
    let mut record = coordinates.to_record();
    if let Some(index) = coordinates.find_coordinate(CoordinateType::Spectral) {
        let field = format!("spectral{index}");
        if let Some(Value::Record(spectral)) = record.get_mut(&field) {
            spectral.upsert("conversion", Value::Record(default_spectral_conversion()));
        }
    }
    for index in 0..coordinates.n_coordinates() {
        if let Some(Value::Record(linear)) = record.get_mut(&format!("linear{index}")) {
            linear.remove("coordinate_type");
            linear.remove("naxes");
        }
    }
    for (axis, &length) in shape.iter().enumerate() {
        CoordinateSystem::set_reference_pixel_in_record(&mut record, axis, (length / 2) as f64)?;
    }
    Ok(record)
}

/// Splits a native coordinate record field such as `"direction0"` into its kind and index.
pub fn native_coordinate_field(name: &str) -> Option<(&str, usize)> {
    let offset = name.find(|character: char| character.is_ascii_digit())?;
    let (kind, index) = name.split_at(offset);
    let index = index.parse().ok()?;
    match kind {
        "direction" | "spectral" | "stokes" | "linear" | "tabular" | "quality" | "coordsys"
        | "coordinate" => Some((kind, index)),
        _ => None,
    }
}

/// Validates that numbered coordinate fields in `record` are contiguous and unique.
pub fn validate_coordinate_indices(record: &RecordValue) -> ImageToolResult<usize> {
    let mut indices = Vec::new();
    for field in record.fields() {
        let Some((_, index)) = native_coordinate_field(&field.name) else {
            continue;
        };
        indices.push(index);
    }
    indices.sort_unstable();
    if !indices.iter().copied().eq(0..indices.len()) {
        return Err(ImageToolError::new(
            "Coordinate record numbering must be contiguous and unique",
        ));
    }
    Ok(indices.len())
}

/// Appends a coordinate and its axis-mapping fields while keeping the existing record intact.
pub fn append_coordinate(
    coordinates: &mut CoordinateSystem,
    record: &mut RecordValue,
    coordinate: impl Into<CoordinateModel>,
) -> ImageToolResult<()> {
    let coordinate = coordinate.into();
    let index = coordinates.n_coordinates();
    let first_pixel = coordinates.n_pixel_axes() as i32;
    let first_world = coordinates.n_world_axes() as i32;
    let n_pixel = coordinate.n_pixel_axes();
    let n_world = coordinate.n_world_axes();
    let replacements = coordinate.to_world(&vec![0.0; n_pixel])?;
    let kind = coordinate.coordinate_type();
    let name = kind.record_prefix();
    let mut axis_record = coordinate.to_record();
    match kind {
        CoordinateType::Spectral => {
            axis_record.upsert("conversion", default_spectral_conversion().into());
        }
        CoordinateType::Linear => {
            axis_record.remove("coordinate_type");
            axis_record.remove("naxes");
        }
        _ => {}
    }
    record.upsert(format!("{name}{index}"), axis_record.into());
    record.upsert(
        format!("pixelmap{index}"),
        ArrayValue::from_i32_vec((first_pixel..first_pixel + n_pixel as i32).collect()).into(),
    );
    record.upsert(
        format!("worldmap{index}"),
        ArrayValue::from_i32_vec((first_world..first_world + n_world as i32).collect()).into(),
    );
    record.upsert(
        format!("pixelreplace{index}"),
        ArrayValue::from_f64_vec(vec![0.0; n_pixel]).into(),
    );
    record.upsert(
        format!("worldreplace{index}"),
        ArrayValue::from_f64_vec(replacements).into(),
    );
    coordinates.add_coordinate(coordinate);
    Ok(())
}

/// Returns `true` if any row in `matrix` is identically zero.
pub fn has_zero_row(matrix: &Array2<f64>) -> bool {
    matrix
        .rows()
        .into_iter()
        .any(|row| row.iter().all(|&value| value == 0.0))
}

/// Rejects restored linear or nested coordinate systems with singular transform matrices.
pub fn validate_restored_linear_coordinates(
    coordinates: &CoordinateSystem,
    record: &RecordValue,
) -> ImageToolResult<()> {
    if !(0..coordinates.n_coordinates()).any(|index| {
        matches!(
            coordinates.coordinate(index),
            CoordinateModel::Linear(_) | CoordinateModel::System(_)
        )
    }) {
        return Ok(());
    }
    for (index, placement) in coordinates
        .coordinates_in_record(record)?
        .into_iter()
        .enumerate()
    {
        let mapped = coordinates.coordinate(index);
        let full;
        let model = if matches!(
            mapped,
            CoordinateModel::Linear(_) | CoordinateModel::System(_)
        ) && (placement.pixel_axes.len() != mapped.n_pixel_axes()
            || placement.world_axes.len() != mapped.n_world_axes())
        {
            full = placement.model(record)?;
            &full
        } else {
            mapped
        };
        match model {
            CoordinateModel::Linear(linear) => {
                if linear.increment().contains(&0.0) || has_zero_row(linear.pc_matrix()) {
                    return Err(ImageToolError::new(
                        "wcs wcsset_error: Linear transformation matrix is singular",
                    ));
                }
            }
            CoordinateModel::System(nested) => {
                if let Some(Value::Record(nested_record)) = record.get(&placement.field) {
                    validate_restored_linear_coordinates(nested, nested_record)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Reads the stored velocity unit and Doppler convention for a spectral axis record.
pub fn stored_velocity_settings(
    record: Option<&RecordValue>,
    spectral: &SpectralCoordinate,
) -> ImageToolResult<(Unit, DopplerRef)> {
    let mut convention = spectral.velocity_convention();
    let stored_unit = if let Some(record) = record {
        if record.get("velType").is_none() {
            let legacy_code = match record.get("prefVelType") {
                Some(Value::Scalar(ScalarValue::Int32(code))) => Some(*code),
                Some(Value::Scalar(ScalarValue::Int64(code))) => i32::try_from(*code).ok(),
                _ => None,
            };
            if let Some(legacy) = legacy_code.and_then(DopplerRef::from_casacore_code) {
                convention = legacy;
            }
        }
        match record.get("velUnit").or_else(|| record.get("prefVelUnit")) {
            Some(Value::Scalar(ScalarValue::String(unit))) if !unit.is_empty() => unit.as_str(),
            _ => "km/s",
        }
    } else {
        "km/s"
    };
    let stored =
        casacore::casacore_unit(stored_unit).map_err(|e| ImageToolError::new(e.to_string()))?;
    let speed = UnitDim::basis(Dimension::Length) / UnitDim::basis(Dimension::Time);
    if stored.val().dim == speed {
        Ok((stored, convention))
    } else {
        Ok((
            Unit::new("km/s").expect("standard speed unit"),
            DopplerRef::RADIO,
        ))
    }
}

/// Formats `value` in scientific notation with fixed `precision` decimal digits.
pub fn scientific_with_precision(value: f64, precision: usize) -> String {
    let text = format!("{value:.precision$e}");
    let Some((mantissa, exponent)) = text.split_once('e') else {
        return format_stream_value_with_precision(value, precision);
    };
    let exponent: i32 = exponent.parse().expect("exponent is decimal");
    format!("{mantissa}e{exponent:+03}")
}

/// Formats a numeric coordinate value scaled relative to `increment`.
pub fn format_number(value: f64, increment: f64, scientific_style: bool) -> String {
    let precision = if scientific_style { 8 } else { 6 };
    format_number_with_precision(value, increment, scientific_style, precision)
}

/// Formats a numeric coordinate value with a base `precision` adjusted for `increment`.
pub fn format_number_with_precision(
    value: f64,
    increment: f64,
    scientific_style: bool,
    mut precision: usize,
) -> String {
    if value.is_finite() && value.fract() != 0.0 && increment.is_finite() {
        let mut needed = 1;
        let mut scale = 1.0;
        while value.abs() > scale {
            needed += 1;
            scale *= 10.0;
        }
        let increment = increment.abs();
        if increment < 1.0 {
            let mut scale = 0.1;
            while increment < scale {
                needed += 1;
                scale /= 10.0;
            }
        } else {
            let mut adjustment = 1;
            let mut scale = 1.0;
            while increment > scale {
                adjustment += 1;
                scale *= 10.0;
            }
            if adjustment < needed {
                needed -= adjustment;
            }
        }
        if needed > 5 {
            precision = needed + 1;
        }
    }
    if scientific_style {
        scientific_with_precision(value, precision)
    } else {
        format_stream_value_with_precision(value, precision)
    }
}

fn signed_angle(radians: f64) -> f64 {
    let (sin, cos) = radians.sin_cos();
    sin.atan2(cos)
}

/// Formats a direction angle in CASA's world-axis display style.
pub fn format_direction(
    frame: DirectionRef,
    axis: usize,
    radians: f64,
    absolute: bool,
    scientific_style: bool,
    precision: usize,
) -> (String, &'static str) {
    let radians = if radians == 0.0 { 0.0 } else { radians };
    let name = frame.as_str();
    let angle_frame = matches!(
        name,
        "GALACTIC"
            | "SUPERGAL"
            | "ECLIPTIC"
            | "MECLIPTIC"
            | "TECLIPTIC"
            | "AZEL"
            | "AZELSW"
            | "AZELGEO"
            | "AZELSWGEO"
    );
    let hour_axis = axis == 0 && name == "HADEC";
    if absolute && !scientific_style && !angle_frame {
        if axis != 0 {
            return (format_latitude_dms_with_precision(radians, precision), "");
        }
        if hour_axis {
            let signed = signed_angle(radians);
            let sign = if signed.is_sign_negative() { '-' } else { '+' };
            return (
                format!(
                    "{sign}{}",
                    format_longitude_hours_with_precision(signed.abs(), precision)
                ),
                "",
            );
        }
        return (
            format_longitude_hours_with_precision(radians, precision),
            "",
        );
    }
    let (number, unit) = if hour_axis {
        let hour_angle = if absolute {
            signed_angle(radians)
        } else {
            radians
        };
        let hours = hour_angle * 12.0 / std::f64::consts::PI;
        (hours, "h")
    } else if !absolute {
        (radians.to_degrees() * 3600.0, "arcsec")
    } else {
        let degrees = radians.to_degrees();
        let degrees = if axis != 0 {
            degrees
        } else if matches!(name, "ECLIPTIC" | "MECLIPTIC" | "TECLIPTIC") {
            signed_angle(radians).to_degrees()
        } else {
            degrees.rem_euclid(360.0)
        };
        (degrees, "deg")
    };
    let text = if scientific_style || !angle_frame {
        scientific_with_precision(number, precision)
    } else {
        format!("{number:.precision$}")
    };
    (text, unit)
}

fn stokes_name(value: f64) -> String {
    StokesType::from_code(value.round() as i32)
        .unwrap_or(StokesType::Undefined)
        .name()
        .into()
}

/// Formats an axis inside a nested `CoordinateSystem` model for summary output.
pub fn nested_axis_display(
    model: &CoordinateModel,
    axis: usize,
    value: f64,
    increment: f64,
) -> ImageToolResult<Option<(String, String, String)>> {
    if !matches!(model, CoordinateModel::System(_)) {
        return Ok(None);
    }
    let Some((leaf, local)) = model.world_axis_leaf(axis) else {
        return Ok(None);
    };
    Ok(match leaf {
        CoordinateModel::Direction(direction) => {
            let declared = model.declared_axis_units();
            let angle = Quantity::new(value, "rad")?.get_value_in(&Unit::new(&declared[axis])?)?;
            let local = if direction.longitude_axis().is_none() {
                1
            } else {
                local
            };
            let (increment, units) =
                format_direction(direction.direction_ref(), local, increment, false, true, 6);
            Some((format_stream_value(angle), increment, units.into()))
        }
        CoordinateModel::Stokes(_) => {
            Some((stokes_name(value), stokes_name(increment), String::new()))
        }
        CoordinateModel::Quality(_) => Some((
            QualityType::from_world(value).name().into(),
            QualityType::from_world(increment).name().into(),
            String::new(),
        )),
        _ => None,
    })
}

/// Returns the spectral formatting unit from a coordinate record or `native_unit`.
pub fn spectral_format_unit<'a>(record: Option<&'a RecordValue>, native_unit: &'a str) -> &'a str {
    match record.and_then(|record| record.get("formatUnit")) {
        Some(Value::Scalar(ScalarValue::String(unit))) if !unit.is_empty() => unit.as_str(),
        _ => native_unit,
    }
}

/// Converts a spectral coordinate value into `display_unit` (Hz, velocity, or wavelength).
pub fn spectral_display_value(
    spectral: &SpectralCoordinate,
    record: Option<&RecordValue>,
    value: f64,
    native_unit: &str,
    display_unit: &Unit,
    absolute: bool,
) -> ImageToolResult<Option<f64>> {
    let hz = Unit::new("Hz").expect("known unit");
    if display_unit.conformant(&hz) {
        return Ok(Some(
            Quantity::new(value, native_unit)?.get_value_in(display_unit)?,
        ));
    }

    let origin = spectral.value_in_hz(Coordinate::reference_value(spectral)[0])?;
    let frequency = spectral.value_in_hz(value)?;
    let frequency = if absolute {
        frequency
    } else {
        origin + frequency
    };
    let velocity = Unit::new("m/s").expect("known unit");
    let length = Unit::new("m").expect("known unit");
    let quantity = if display_unit.conformant(&velocity) {
        let rest = spectral.rest_frequency_hz()?;
        if rest <= 0.0 {
            return Ok(None);
        }
        let code = spectral_integer_field(record, "velType");
        let reference = DopplerRef::from_casacore_code(code).unwrap_or(DopplerRef::RADIO);
        let to_velocity = |frequency: f64| -> ImageToolResult<f64> {
            Ok(MDoppler::new(frequency.abs() / rest, DopplerRef::RATIO)
                .convert_to(reference, &MeasFrame::new())?
                .value()
                * SPEED_OF_LIGHT_M_S)
        };
        let mut mps = to_velocity(frequency)?;
        if !absolute {
            mps -= to_velocity(origin)?;
        }
        Quantity::new(mps, "m/s")
    } else if display_unit.conformant(&length) {
        let air = spectral_integer_field(record, "nativeType") == 5;
        let mut meters = wavelength(frequency, air);
        if !absolute {
            meters -= wavelength(origin, air);
        }
        Quantity::new(meters, "m")
    } else {
        return Err(ImageToolError::new(
            "Requested units must be consistent with km/s, m, or Hz for a SpectralCoordinate",
        ));
    }?;
    Ok(Some(quantity.get_value_in(display_unit)?))
}

/// Reads an integer field from an optional coordinate record.
pub fn spectral_integer_field(record: Option<&RecordValue>, field: &str) -> i32 {
    match record.and_then(|record| record.get(field)) {
        Some(Value::Scalar(ScalarValue::Int32(code))) => *code,
        Some(Value::Scalar(ScalarValue::Int64(code))) => i32::try_from(*code).unwrap_or_default(),
        _ => 0,
    }
}

fn wavelength(frequency: f64, air: bool) -> f64 {
    if frequency <= 0.0 {
        return f64::INFINITY;
    }
    let vacuum = SPEED_OF_LIGHT_M_S / frequency;
    if !air {
        return vacuum;
    }
    // Refractive index of air with wavelength in micrometers, used by
    // casacore's FITSSpectralUtil (Greisen et al. 2006, A&A 464, 746).
    let micrometers2 = (vacuum * 1e6).powi(2);
    let index = 1.0 + 1e-6 * (287.6155 + 1.62887 / micrometers2 + 0.01360 / micrometers2.powi(2));
    vacuum / index
}
