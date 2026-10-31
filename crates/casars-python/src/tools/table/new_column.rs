// SPDX-License-Identifier: LGPL-3.0-or-later
//! A column description as `tb.addcols` takes it, a record of
//! `valueType`, `comment`, `ndim`, `shape`, `option`, `maxlen` and
//! `keywords` per column name (`tools/casa-compat/table_tool_oracle.py`,
//! scenario `rows_columns`, CASA 6.7.6.14).
//!
//! A column with no `ndim` holds one value per row; with one, arrays of that
//! many axes, of fixed shape when `option` has casacore's `FixedShape` flag
//! (4) and a `shape` is given, of variable shape otherwise. `option` also
//! carries `Direct` (1) and `Undefined` (2).

use casa_tables::{ColumnOptions, ColumnSchema};
use casa_types::{PrimitiveType, RecordValue};

use super::keyword_value::Incoming;
use super::state::Fail;

/// `FixedShape`, `Direct` and `Undefined` of casacore's `ColumnDesc::Option`.
const DIRECT: i64 = 1;
const UNDEFINED: i64 = 2;
const FIXED_SHAPE: i64 = 4;

/// A column to add.
pub(super) struct NewColumn {
    pub(super) schema: ColumnSchema,
    pub(super) keywords: Option<RecordValue>,
    /// Whether the description names a storage manager for the column.
    pub(super) names_manager: bool,
}

/// casacore's data type for a `valueType` name: `Some(None)` is a record
/// column.
fn value_type(name: &str) -> Option<Option<PrimitiveType>> {
    Some(Some(match name.to_ascii_lowercase().as_str() {
        "boolean" | "bool" => PrimitiveType::Bool,
        "uchar" => PrimitiveType::UInt8,
        "short" => PrimitiveType::Int16,
        "ushort" => PrimitiveType::UInt16,
        "int" => PrimitiveType::Int32,
        "uint" => PrimitiveType::UInt32,
        "int64" => PrimitiveType::Int64,
        "float" => PrimitiveType::Float32,
        "double" => PrimitiveType::Float64,
        "complex" => PrimitiveType::Complex32,
        "dcomplex" => PrimitiveType::Complex64,
        "string" => PrimitiveType::String,
        "record" => return Some(None),
        _ => return None,
    }))
}

fn failure(message: String) -> Fail {
    Fail::Runtime(message)
}

/// The column `name` that `description` describes.
pub(super) fn parse(name: &str, description: &Incoming) -> Result<NewColumn, Fail> {
    let Some(fields) = description.fields() else {
        return Err(failure(format!(
            "RecordRep::get_pointer - incorrect data type Record   used for field {name} with \
             type {}",
            description.type_label()
        )));
    };
    let get = |key: &str| {
        fields
            .iter()
            .find(|(field, _)| field == key)
            .map(|(_, value)| value)
    };
    let number = |key: &str| {
        get(key)
            .and_then(Incoming::integers)
            .and_then(<[i64]>::first)
    };
    let ndim = number("ndim").copied();
    let kind = get("valueType")
        .and_then(Incoming::text)
        .unwrap_or_default();
    let Some(data_type) = value_type(kind) else {
        let shape = if ndim.is_some() { "array" } else { "scalar" };
        return Err(failure(format!(
            "addColumns failed: Unknown data type {kind} for {shape} column {name}"
        )));
    };
    let option = number("option").copied().unwrap_or(0);
    let schema = match (data_type, ndim) {
        (None, _) => ColumnSchema::record(name),
        (Some(ty), None) => ColumnSchema::scalar(name, ty),
        (Some(ty), Some(ndim)) => {
            let shape: Vec<usize> = get("shape")
                .and_then(Incoming::integers)
                .unwrap_or_default()
                .iter()
                .filter_map(|&extent| usize::try_from(extent).ok())
                .collect();
            if option & FIXED_SHAPE != 0 && !shape.is_empty() {
                ColumnSchema::array_fixed(name, ty, shape)
            } else {
                ColumnSchema::array_variable(name, ty, usize::try_from(ndim).ok())
            }
        }
    };
    let schema = schema
        .with_options(ColumnOptions {
            direct: option & DIRECT != 0,
            undefined: option & UNDEFINED != 0,
        })
        .map_err(|error| failure(format!("addColumns failed: {error}")))?;
    let schema = match get("comment").and_then(Incoming::text) {
        Some(comment) => schema.with_comment(comment),
        None => schema,
    };
    let schema = match number("maxlen").and_then(|&length| u32::try_from(length).ok()) {
        Some(length) => schema.with_max_length(length),
        None => schema,
    };
    let keywords = get("keywords")
        .filter(|keywords| keywords.fields().is_some())
        .map(|keywords| keywords.clone().natural())
        .transpose()?
        .and_then(|value| match value {
            casa_types::Value::Record(record) => Some(record),
            _ => None,
        });
    let names_manager = ["dataManagerType", "dataManagerGroup"].iter().any(|key| {
        get(key)
            .and_then(Incoming::text)
            .is_some_and(|text| !text.is_empty())
    });
    Ok(NewColumn {
        schema,
        keywords,
        names_manager,
    })
}
