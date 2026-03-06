// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared column-keyword metadata helpers for MeasurementSet compatibility.

use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

use crate::column_def::ColumnDef;

pub(crate) fn quantum_units_for(def: &ColumnDef) -> Option<Vec<String>> {
    if def.name == "UVW" && !def.unit.is_empty() {
        return Some(vec![
            def.unit.to_string(),
            def.unit.to_string(),
            def.unit.to_string(),
        ]);
    }
    if def.unit.is_empty() {
        return None;
    }
    match def.measure_type {
        Some(casacore_tables::table_measures::MeasureType::Direction) => {
            Some(vec![def.unit.to_string(), def.unit.to_string()])
        }
        Some(casacore_tables::table_measures::MeasureType::Position) => Some(vec![
            def.unit.to_string(),
            def.unit.to_string(),
            def.unit.to_string(),
        ]),
        _ => Some(vec![def.unit.to_string()]),
    }
}

pub(crate) fn measure_type_name_for(def: &ColumnDef) -> Option<String> {
    if def.name == "UVW" {
        Some("uvw".to_string())
    } else {
        def.measure_type
            .map(|measure_type| measure_type.as_str().to_string())
    }
}

pub(crate) fn measinfo_for(def: &ColumnDef) -> Option<RecordValue> {
    let measure_type = measure_type_name_for(def)?;

    let mut fields = vec![RecordField::new(
        "type",
        Value::Scalar(ScalarValue::String(measure_type)),
    )];
    if !def.measure_ref.is_empty() {
        fields.push(RecordField::new(
            "Ref",
            Value::Scalar(ScalarValue::String(def.measure_ref.to_string())),
        ));
    }
    Some(RecordValue::new(fields))
}
