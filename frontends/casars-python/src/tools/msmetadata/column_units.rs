// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::quanta::casacore::casacore_unit;
use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::{MsError, MsResult};
use casa_tables::Table;

pub(super) fn column_unit<'a>(
    table: &'a Table,
    column: &str,
    component: usize,
) -> MsResult<&'a str> {
    let units = table
        .column_keywords(column)
        .and_then(|keywords| keywords.get("QuantumUnits"));
    match units {
        Some(Value::Array(ArrayValue::String(values))) => values
            .iter()
            .nth(component)
            .or_else(|| values.first())
            .map(String::as_str),
        Some(Value::Scalar(ScalarValue::String(value))) => Some(value.as_str()),
        _ => None,
    }
    .ok_or_else(|| MsError::InvalidInput(format!("{column} has no quantum unit")))
}

pub(super) fn column_unit_factor(
    table: &Table,
    column: &str,
    component: usize,
    target: &str,
) -> MsResult<f64> {
    let source = column_unit(table, column, component)?;
    if source == target {
        return Ok(1.0);
    }
    let parse =
        |unit| casacore_unit(unit).map_err(|error| MsError::InvalidInput(error.to_string()));
    let source = parse(source)?;
    let target = parse(target)?;
    if !source.conformant(&target) {
        return Err(MsError::InvalidInput(format!(
            "{column} unit {} cannot be converted to {}",
            source.name(),
            target.name()
        )));
    }
    Ok(source.val().factor / target.val().factor)
}
