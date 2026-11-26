// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::SpectralCoordinate;
use casa_core::measures::{
    MeasFrame,
    doppler::{DopplerRef, MDoppler},
};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{RecordValue, ScalarValue, Value};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{format_number, scientific_with_precision};

pub(super) const SPEED_OF_LIGHT: f64 = 299_792_458.0;

pub(super) fn text(
    spectral: &SpectralCoordinate,
    record: Option<&RecordValue>,
    value: f64,
    native_unit: &str,
    absolute: bool,
    scientific: bool,
) -> PyResult<String> {
    let requested = format_unit(record, native_unit);
    let display_unit =
        Unit::new(requested).map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let Some(value) = display_value(
        spectral,
        record,
        value,
        native_unit,
        &display_unit,
        absolute,
    )?
    else {
        return Ok(format!("Fail {requested}"));
    };
    let hz = Unit::new("Hz").expect("known unit");
    let rendered = if display_unit.conformant(&hz) {
        let increment = casa_coordinates::Coordinate::increment(spectral)[0];
        let converted = display_value(
            spectral,
            record,
            increment,
            native_unit,
            &display_unit,
            false,
        )?
        .expect("frequency conversion has a value");
        format_number(value, converted, scientific)
    } else if scientific {
        scientific_with_precision(value, 8)
    } else {
        casa_coordinates::display::format_stream_value(value)
    };
    Ok(format!("{rendered} {requested}"))
}

pub(in crate::tools) fn format_unit<'a>(
    record: Option<&'a RecordValue>,
    native_unit: &'a str,
) -> &'a str {
    match record.and_then(|record| record.get("formatUnit")) {
        Some(Value::Scalar(ScalarValue::String(unit))) if !unit.is_empty() => unit.as_str(),
        _ => native_unit,
    }
}

pub(in crate::tools) fn display_value(
    spectral: &SpectralCoordinate,
    record: Option<&RecordValue>,
    value: f64,
    native_unit: &str,
    display_unit: &Unit,
    absolute: bool,
) -> PyResult<Option<f64>> {
    let hz = Unit::new("Hz").expect("known unit");
    if display_unit.conformant(&hz) {
        return Quantity::new(value, native_unit)
            .and_then(|quantity| quantity.get_value_in(display_unit))
            .map(Some)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()));
    }

    let origin = spectral
        .value_in_hz(casa_coordinates::Coordinate::reference_value(spectral)[0])
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let frequency = spectral
        .value_in_hz(value)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let frequency = if absolute {
        frequency
    } else {
        origin + frequency
    };
    let velocity = Unit::new("m/s").expect("known unit");
    let length = Unit::new("m").expect("known unit");
    let value = if display_unit.conformant(&velocity) {
        let rest = spectral
            .rest_frequency_hz()
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        if rest <= 0.0 {
            return Ok(None);
        }
        let code = integer_field(record, "velType");
        let reference = DopplerRef::from_casacore_code(code).unwrap_or(DopplerRef::RADIO);
        let to_velocity = |frequency: f64| -> PyResult<f64> {
            MDoppler::new(frequency.abs() / rest, DopplerRef::RATIO)
                .convert_to(reference, &MeasFrame::new())
                .map(|doppler| doppler.value() * SPEED_OF_LIGHT)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        };
        let mut mps = to_velocity(frequency)?;
        if !absolute {
            mps -= to_velocity(origin)?;
        }
        Quantity::new(mps, "m/s")
    } else if display_unit.conformant(&length) {
        let air = integer_field(record, "nativeType") == 5;
        let mut meters = wavelength(frequency, air);
        if !absolute {
            meters -= wavelength(origin, air);
        }
        Quantity::new(meters, "m")
    } else {
        return Err(PyRuntimeError::new_err(
            "Requested units must be consistent with km/s, m, or Hz for a SpectralCoordinate",
        ));
    }
    .and_then(|quantity| quantity.get_value_in(display_unit))
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    Ok(Some(value))
}

pub(in crate::tools) fn integer_field(record: Option<&RecordValue>, field: &str) -> i32 {
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
    let vacuum = SPEED_OF_LIGHT / frequency;
    if !air {
        return vacuum;
    }
    // Refractive index of air with wavelength in micrometers, used by
    // casacore's FITSSpectralUtil (Greisen et al. 2006, A&A 464, 746).
    let micrometers2 = (vacuum * 1e6).powi(2);
    let index = 1.0 + 1e-6 * (287.6155 + 1.62887 / micrometers2 + 0.01360 / micrometers2.powi(2));
    vacuum / index
}
