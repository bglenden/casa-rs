// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::SpectralCoordinate;
use casa_core::RecordValue;
use casa_core::quanta::Unit;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{format_number, scientific_with_precision};

pub(in crate::tools) use casa_imageanalysis::tool::coords::SPEED_OF_LIGHT_M_S as SPEED_OF_LIGHT;

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
    casa_imageanalysis::tool::coords::spectral_format_unit(record, native_unit)
}

pub(in crate::tools) fn display_value(
    spectral: &SpectralCoordinate,
    record: Option<&RecordValue>,
    value: f64,
    native_unit: &str,
    display_unit: &Unit,
    absolute: bool,
) -> PyResult<Option<f64>> {
    casa_imageanalysis::tool::coords::spectral_display_value(
        spectral,
        record,
        value,
        native_unit,
        display_unit,
        absolute,
    )
    .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}
