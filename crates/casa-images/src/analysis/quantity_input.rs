// SPDX-License-Identifier: LGPL-3.0-or-later
//! Quantity values accepted by CASA image metadata operations.

use casa_provider_contracts::ParameterValue;
use casa_types::quanta::{Quantity, Unit};

use super::projection_header::cpp_number_precision;
use crate::ImageError;

/// Parse a CASA quantity string or a scalar value-and-unit record.
pub(super) fn parse(value: &ParameterValue) -> Result<(Quantity, String), ImageError> {
    let invalid = |message: String| ImageError::InvalidMetadata(message);
    match value {
        ParameterValue::String(text) => {
            if text.contains('\0') {
                return Err(invalid(
                    "Quantity strings containing NUL are unsupported (#977)".into(),
                ));
            }
            let quantity = Quantity::read(text)
                .map_err(|_| invalid(format!("Error converting {text} to Quantity")))?;
            Ok((quantity, format!("\"{text}\"")))
        }
        ParameterValue::Table(fields) => {
            if fields.keys().any(|key| key != "value" && key != "unit") {
                return Err(invalid(
                    "Additional quantity record fields are not implemented (#977)".into(),
                ));
            }
            let Some(ParameterValue::String(unit)) = fields.get("unit") else {
                return Err(invalid("Illegal Quantity record".into()));
            };
            if unit.contains('\0') {
                return Err(invalid(
                    "Quantity units containing NUL are unsupported (#977)".into(),
                ));
            }
            let (number, ty, rendered) = match fields.get("value") {
                Some(ParameterValue::Float(value)) => {
                    (*value, "Double", cpp_number_precision(*value, 6))
                }
                Some(ParameterValue::Integer(value)) => {
                    let value = i32::try_from(*value).map_err(|_| {
                        invalid(
                            "Quantity integer is outside the supported 32-bit range (#977)".into(),
                        )
                    })?;
                    (f64::from(value), "Int", value.to_string())
                }
                _ => return Err(invalid("Illegal Quantity record".into())),
            };
            let unit_value = casa_types::quanta::casacore::casacore_unit(unit)
                .map_err(|error| invalid(error.to_string()))?;
            let quantity = Quantity::with_unit(number, unit_value);
            Ok((
                quantity,
                format!("  unit: String \"{unit}\"\n  value: {ty} {rendered}"),
            ))
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
    casa_types::quanta::casacore::get_conformant_value(quantity, &unit)
        .map_err(|error| ImageError::InvalidMetadata(error.to_string()))
}

/// Convert as CASA's coordinate metadata setter does, including its handling
/// of nonconformant units. An empty unit means the image axis unit.
pub(super) fn in_axis_unit(quantity: &Quantity, unit: &str) -> Result<f64, ImageError> {
    if quantity.unit().name().is_empty() {
        return Ok(quantity.value());
    }
    let unit = Unit::new(unit).map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
    Ok(casa_types::quanta::casacore::get_value(quantity, &unit))
}
