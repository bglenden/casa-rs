// SPDX-License-Identifier: LGPL-3.0-or-later
//! The native tool record produced by CASA's calibration-library grammar.

use std::collections::BTreeMap;
use std::path::Path;

use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use super::{
    CallibError, line_error, parse_bool, parse_entries, parse_int_array, read_contents,
    strip_quotes,
};

/// Parse a calibration-library file as the record returned by
/// `calibrater.parsecallibfile`. Preserve literal table names, raw
/// settings and the distinction between a string selector and an ID map.
///
/// # Errors
///
/// The file could not be read or did not conform to CASA's grammar.
pub fn load_callibrary_record(path: impl AsRef<Path>) -> Result<RecordValue, CallibError> {
    let path = path.as_ref();
    let contents = read_contents(path)?;
    let path = path.display().to_string();
    let (entries, has_statement) = parse_entries(&path, &contents, false)?;
    if !has_statement {
        return Err(line_error(
            &path,
            &contents,
            contents.lines().count() + 1,
            "no calibration library entries".into(),
        ));
    }
    let mut tables = BTreeMap::<String, (bool, Vec<RecordValue>)>::new();
    for mut entry in entries {
        let error = |reason| line_error(&path, &contents, entry.line, reason);
        let table = entry
            .values
            .remove("caltable")
            .expect("parser emits table entries");
        let calwt = entry
            .values
            .remove("calwt")
            .map(|value| parse_bool(&value))
            .transpose()
            .map_err(error)?
            .unwrap_or(true);
        let slice = parse_slice(&entry.values).map_err(error)?;
        // Later slices keep the same table-wide weight setting.
        tables
            .entry(strip_quotes(&table))
            .or_insert_with(|| (calwt, Vec::new()))
            .1
            .push(slice);
    }
    Ok(RecordValue::new(
        tables
            .into_iter()
            .map(|(table, (calwt, slices))| {
                let mut fields = slices
                    .into_iter()
                    .enumerate()
                    .map(|(index, slice)| RecordField::new(index.to_string(), Value::Record(slice)))
                    .collect::<Vec<_>>();
                fields.push(RecordField::new(
                    "calwt",
                    Value::Scalar(ScalarValue::Bool(calwt)),
                ));
                RecordField::new(table, Value::Record(ordered_record(fields)))
            })
            .collect(),
    ))
}

fn parse_slice(values: &BTreeMap<String, String>) -> Result<RecordValue, String> {
    let mut fields = Vec::new();
    for name in [
        "field", "finterp", "intent", "obs", "reach", "scan", "spw", "tinterp",
    ] {
        let value = values
            .get(name)
            .map_or_else(String::new, |value| strip_quotes(value));
        fields.push(RecordField::new(
            name,
            Value::Scalar(ScalarValue::String(value)),
        ));
    }
    for name in ["antmap", "fldmap", "obsmap", "scanmap", "spwmap"] {
        let value = match values.get(name) {
            Some(value) if !value.starts_with('[') => {
                Value::Scalar(ScalarValue::String(strip_quotes(value)))
            }
            entry => {
                let indices = entry
                    .map(|value| parse_int_array(value, name))
                    .transpose()?
                    .unwrap_or_default();
                let array = ArrayD::from_shape_vec(IxDyn(&[indices.len()]), indices)
                    .expect("one-dimensional index vector");
                Value::Array(ArrayValue::Int32(array))
            }
        };
        fields.push(RecordField::new(name, value));
    }
    Ok(ordered_record(fields))
}

fn ordered_record(mut fields: Vec<RecordField>) -> RecordValue {
    fields.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    RecordValue::new(fields)
}

/// Inspect the slice keys in a tool calibration-library record. CASA reports
/// missing keys as diagnostics, allowing the tool to return true.
///
/// # Errors
///
/// A table's value is present but cannot be read as a record.
pub fn validate_callibrary_record(record: &RecordValue) -> Result<Vec<String>, String> {
    let mut diagnostics = Vec::new();
    for table in record.fields() {
        let contents = tool_table_record(table)?;
        for field in contents.fields() {
            let Value::Record(slice) = &field.value else {
                continue;
            };
            let missing = [
                "obs", "scan", "field", "intent", "spw", "tinterp", "finterp", "obsmap", "scanmap",
                "fldmap", "spwmap", "antmap",
            ]
            .into_iter()
            .filter(|name| slice.get(name).is_none())
            .map(|name| if name == "spwmap" { "swmap" } else { name })
            .collect::<Vec<_>>();
            if !missing.is_empty() {
                diagnostics.push(format!(
                    "Caltable {} is missing the following fields: {}",
                    table.name,
                    missing.join(" ")
                ));
            }
        }
    }
    Ok(diagnostics)
}

pub(crate) fn tool_table_record(table: &RecordField) -> Result<&RecordValue, String> {
    let Value::Record(contents) = &table.value else {
        let name = cpp_record_type(&table.value);
        return Err(format!(
            "RecordRep::get_pointer - incorrect data type Record   used for field {} with type {name:<8}",
            table.name
        ));
    };
    Ok(contents)
}

fn cpp_record_type(value: &Value) -> &'static str {
    match value {
        Value::Scalar(ScalarValue::Bool(_)) => "Bool",
        Value::Scalar(ScalarValue::Int32(_) | ScalarValue::Int64(_)) => "Int",
        Value::Scalar(ScalarValue::Float32(_)) => "Float",
        Value::Scalar(ScalarValue::Float64(_)) => "Double",
        Value::Scalar(ScalarValue::String(_)) => "String",
        Value::Scalar(ScalarValue::Complex32(_)) => "Complex",
        Value::Scalar(ScalarValue::Complex64(_)) => "DComplex",
        Value::Array(ArrayValue::Int32(_) | ArrayValue::Int64(_)) => "ArrayInt",
        Value::Array(ArrayValue::Float64(_)) => "ArrayDouble",
        Value::Array(ArrayValue::String(_)) => "ArrayString",
        _ => "Unknown",
    }
}

/// Convert the slices for one tool caltable into the shared on-the-fly apply
/// requests. The tool parses a supplied Python record with its original
/// field types before constructing these requests.
pub(crate) fn tool_library_specs(
    table_name: &str,
    table: &RecordValue,
    base_dir: &Path,
) -> Result<Vec<crate::ApplyCalibrationTableSpec>, String> {
    use super::parse_callib_entry;
    use crate::ApplyInterpolationMode;

    let calwt = match table.get("calwt") {
        Some(Value::Scalar(ScalarValue::Bool(value))) => *value,
        Some(Value::Scalar(ScalarValue::Int32(value))) => *value != 0,
        Some(Value::Scalar(ScalarValue::Int64(value))) => *value != 0,
        None => false,
        Some(_) => return Err("calwt must be a boolean".into()),
    };
    table
        .fields()
        .iter()
        .filter_map(|field| match &field.value {
            Value::Record(slice) => Some(slice),
            _ => None,
        })
        .map(|slice| {
            let mut fields = BTreeMap::from([
                ("caltable".to_owned(), format!("\"{table_name}\"")),
                ("calwt".to_owned(), calwt.to_string()),
            ]);
            for field in slice.fields() {
                let entry = match (&*field.name, &field.value) {
                    (
                        "antmap" | "fldmap" | "obsmap" | "scanmap" | "spwmap",
                        Value::Array(ArrayValue::Int32(values)),
                    ) if values.is_empty() => continue,
                    (
                        "antmap" | "fldmap" | "obsmap" | "scanmap" | "spwmap",
                        Value::Array(ArrayValue::Int64(values)),
                    ) if values.is_empty() => continue,
                    (
                        "field" | "finterp" | "intent" | "obs" | "reach" | "scan" | "spw"
                        | "tinterp" | "antmap" | "fldmap" | "obsmap" | "scanmap" | "spwmap",
                        Value::Scalar(ScalarValue::String(value)),
                    ) => format!("\"{value}\""),
                    (
                        "antmap" | "fldmap" | "obsmap" | "scanmap" | "spwmap",
                        Value::Array(ArrayValue::Int32(values)),
                    ) if values.shape().iter().filter(|size| **size > 1).count() <= 1 => {
                        let ids = values.iter().map(i32::to_string).collect::<Vec<_>>();
                        format!("[{}]", ids.join(","))
                    }
                    (
                        "antmap" | "fldmap" | "obsmap" | "scanmap" | "spwmap",
                        Value::Array(ArrayValue::Int64(values)),
                    ) if values.shape().iter().filter(|size| **size > 1).count() <= 1 => {
                        let ids = values
                            .iter()
                            .copied()
                            .map(casa_provider_contracts::casa_variant_integer)
                            .map(|id| id.to_string())
                            .collect::<Vec<_>>();
                        format!("[{}]", ids.join(","))
                    }
                    (
                        key @ ("antmap" | "fldmap" | "obsmap" | "scanmap" | "spwmap"),
                        Value::Array(ArrayValue::Int32(_) | ArrayValue::Int64(_)),
                    ) => {
                        return Err(format!("invalid calibration library map shape {key}"));
                    }
                    (
                        key @ ("field" | "finterp" | "intent" | "obs" | "scan" | "spw" | "tinterp"),
                        _,
                    ) => {
                        return Err(format!("invalid calibration library field {key}"));
                    }
                    _ => continue,
                };
                fields.insert(field.name.clone(), entry);
            }
            let mut spec = parse_callib_entry(fields, base_dir)?;
            if slice.get("tinterp").is_none() {
                // A present empty time curve defaults to linear. An omitted
                // value only fails if CASA constructs an applicable context.
                spec.interp = ApplyInterpolationMode::Invalid;
            }
            Ok(spec)
        })
        .collect()
}
