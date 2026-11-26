// SPDX-License-Identifier: LGPL-3.0-or-later
//! Quantity values accepted by CASA image metadata operations.

use casa_core::quanta::{Quantity, Unit};
use casa_core::{ScalarValue, Value};

use super::imhead_value::ImheadValue;
use super::projection_header::cpp_number_precision;
use crate::ImageError;

/// Parse a CASA quantity string or a scalar value-and-unit record.
pub(super) fn parse(value: &ImheadValue) -> Result<(Quantity, String), ImageError> {
    let invalid = |message: String| ImageError::InvalidMetadata(message);
    match value {
        ImheadValue::String(text) => {
            if text.contains('\0') {
                return Err(invalid(
                    "Quantity strings containing NUL are unsupported (#977)".into(),
                ));
            }
            let quantity = Quantity::read(text)
                .map_err(|_| invalid(format!("Error converting {text} to Quantity")))?;
            Ok((quantity, format!("\"{text}\"")))
        }
        ImheadValue::Record(record) => {
            let Some(Value::Scalar(ScalarValue::String(unit))) = record.get("unit") else {
                return Err(invalid("Illegal Quantity record".into()));
            };
            if unit.contains('\0') {
                return Err(invalid(
                    "Quantity units containing NUL are unsupported (#977)".into(),
                ));
            }
            let (number, ty, rendered) = match record.get("value") {
                Some(Value::Scalar(ScalarValue::Float64(value))) => {
                    (*value, "Double", cpp_number_precision(*value, 6))
                }
                Some(Value::Scalar(ScalarValue::Int32(value))) => {
                    (f64::from(*value), "Int", value.to_string())
                }
                _ => return Err(invalid("Illegal Quantity record".into())),
            };
            let unit_value = casa_core::quanta::casacore::casacore_unit(unit)
                .map_err(|error| invalid(error.to_string()))?;
            let quantity = Quantity::with_unit(number, unit_value);
            let rendered = if record.fields().len() == 2 {
                format!("  unit: String \"{unit}\"\n  value: {ty} {rendered}")
            } else {
                super::custom_values::format_record(record)
            };
            Ok((quantity, rendered))
        }
        _ => Err(invalid("Quantity must be a record or string".into())),
    }
}

/// Convert a quantity to an image axis unit, treating no unit as the axis unit.
pub(super) fn in_unit(quantity: &Quantity, unit: &str) -> Result<f64, ImageError> {
    if quantity.unit().name().is_empty() {
        return Ok(quantity.value());
    }
    let unit = Unit::new(unit).map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
    casa_core::quanta::casacore::get_conformant_value(quantity, &unit)
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
}

/// Convert as CASA's coordinate metadata setter does, including its handling
/// of nonconformant units. An empty unit means the image axis unit.
pub(super) fn in_axis_unit(quantity: &Quantity, unit: &str) -> Result<f64, ImageError> {
    if quantity.unit().name().is_empty() {
        return Ok(quantity.value());
    }
    let unit = Unit::new(unit).map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
    Ok(casa_core::quanta::casacore::get_value(quantity, &unit))
}
