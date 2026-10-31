// SPDX-License-Identifier: LGPL-3.0-or-later
//! A column description as `tb.addcols` takes it, a port of casacore's
//! `TableProxy::makeTableDesc` and `addArrayColumnDesc`, checked against what
//! CASA 6.7.6.14 did with each case of `ADDCOLS_CASES` in
//! `tools/casa-compat/table_tool_oracle.py`.
//!
//! The description is a record of column name to a record of `valueType`
//! (required), `comment`, `option`, `maxlen`, `dataManagerType`,
//! `dataManagerGroup`, `ndim`, `shape`, `_c_order` and `keywords`. The fields
//! are read in casacore's order and the first that fails ends the call:
//!
//! * a column has `ndim` when it is an array, whatever the value's type
//!   (`asInt` then takes only an integer);
//! * a `shape` makes the array of fixed shape (`FixedShape`, 4, is added to
//!   `option`), in Fortran order unless `_c_order` reverses it; it must agree
//!   with `ndim` and have only positive extents; an `ndim` of 0 or less
//!   leaves the dimension unspecified, and a shape is then refused;
//! * `Direct` (1) and `Undefined` (2) are `option`'s other flags that the
//!   schema keeps;
//! * `_keywords_`, `_private_keywords_` and `_define_dminfo_` are not columns
//!   and are skipped; `_define_hypercolumn_` is not implemented.
//!
//! A field of the wrong type raises casacore's own message, whose type names
//! are padded to eight characters.

use casa_tables::{ColumnOptions, ColumnSchema};
use casa_types::{PrimitiveType, RecordValue, Value};

use super::keyword_value::Incoming;
use super::state::Fail;

const DIRECT: i64 = 1;
const UNDEFINED: i64 = 2;

/// A column to add.
pub(super) struct NewColumn {
    pub(super) schema: ColumnSchema,
    pub(super) keywords: Option<RecordValue>,
    /// Whether the description names a storage manager for the column.
    pub(super) names_manager: bool,
}

/// casacore's data type for a `valueType`, lower-cased: `Some(None)` is a
/// record column, which only a scalar column can be.
fn value_type(name: &str) -> Option<Option<PrimitiveType>> {
    Some(Some(match name {
        "boolean" | "bool" => PrimitiveType::Bool,
        "byte" | "uchar" => PrimitiveType::UInt8,
        "short" => PrimitiveType::Int16,
        "ushort" => PrimitiveType::UInt16,
        "integer" | "int" => PrimitiveType::Int32,
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

/// What `RecordRep::get_pointer` says of a field of the wrong type.
fn wrong_type(expected: &str, field: &str, found: &str) -> Fail {
    failure(format!(
        "RecordRep::get_pointer - incorrect data type {expected} used for field {field} with \
         type {found}"
    ))
}

/// The fields of one column's description, read as casacore reads them.
struct Fields<'a>(&'a [(String, Incoming)]);

impl Fields<'_> {
    fn get(&self, key: &str) -> Option<&Incoming> {
        self.0
            .iter()
            .find(|(field, _)| field == key)
            .map(|(_, value)| value)
    }

    /// `Record::asInt`: an integer, and nothing else.
    fn int(&self, key: &str) -> Result<Option<i64>, Fail> {
        match self.get(key) {
            None => Ok(None),
            Some(value) => match value.scalar_integer() {
                Some(integer) => Ok(Some(integer)),
                None => Err(failure("RecordInterface::asInt - invalid data type".into())),
            },
        }
    }

    /// `Record::asString`.
    fn string(&self, key: &str) -> Result<Option<String>, Fail> {
        match self.get(key) {
            None => Ok(None),
            Some(value) => match value.text() {
                Some(text) => Ok(Some(text.to_string())),
                None => Err(wrong_type("String  ", key, value.type_label())),
            },
        }
    }

    /// `Record::asBool`.
    fn flag(&self, key: &str) -> Result<bool, Fail> {
        match self.get(key) {
            None => Ok(false),
            Some(value) => value
                .scalar_flag()
                .ok_or_else(|| wrong_type("Bool    ", key, value.type_label())),
        }
    }

    /// `Record::toArrayInt64`: an integer or integers (a real is refused).
    fn integers(&self, key: &str) -> Result<Vec<i64>, Fail> {
        match self.get(key) {
            None => Ok(Vec::new()),
            Some(value) => value
                .integer_elements()
                .ok_or_else(|| wrong_type("unknown ", key, "unknown ")),
        }
    }
}

/// The column `name` that `description` describes; `None` for one of the
/// fields of a description that is not a column.
pub(super) fn parse(name: &str, description: &Incoming) -> Result<Option<NewColumn>, Fail> {
    let Some(fields) = description.fields() else {
        return Err(wrong_type("Record  ", name, description.type_label()));
    };
    let fields = Fields(fields);
    match name {
        "_keywords_" | "_private_keywords_" | "_define_dminfo_" => return Ok(None),
        "_define_hypercolumn_" => {
            return Err(Fail::Unsupported(
                "table.addcols with a hypercolumn definition is not yet implemented in casa-rs \
                 (#1028)"
                    .to_string(),
            ));
        }
        _ => {}
    }
    if fields.get("valueType").is_none() {
        return Err(failure(format!(
            "addColumns failed: No value type for column {name}"
        )));
    }
    let kind = fields
        .string("valueType")?
        .unwrap_or_default()
        .to_ascii_lowercase();
    let option = fields.int("option")?.unwrap_or(0);
    let max_length = fields.int("maxlen")?.unwrap_or(0);
    let comment = fields.string("comment")?;
    let manager_type = fields.string("dataManagerType")?;
    let manager_group = fields.string("dataManagerGroup")?;
    let ndim = fields.int("ndim")?;
    let (shape, c_order) = match ndim {
        Some(_) => (fields.integers("shape")?, fields.flag("_c_order")?),
        None => (Vec::new(), false),
    };

    let data_type = value_type(&kind);
    let schema = match (ndim, data_type) {
        (Some(_), Some(None)) | (Some(_), None) => {
            return Err(failure(format!(
                "addColumns failed: Unknown data type {kind} for array column {name}"
            )));
        }
        (Some(ndim), Some(Some(ty))) => {
            if ndim <= 0 && !shape.is_empty() {
                return Err(failure(
                    "addColumns failed: arrayColumnDesc: shape should not be given when ndim <= 0"
                        .to_string(),
                ));
            }
            if ndim > 0 && !shape.is_empty() && ndim as usize != shape.len() {
                return Err(failure(
                    "addColumns failed: arrayColumnDesc: ndim and shape mismatch".to_string(),
                ));
            }
            if shape.iter().any(|&extent| extent <= 0) {
                return Err(failure(
                    "addColumns failed: arrayColumnDesc: shape < 0".to_string(),
                ));
            }
            if shape.is_empty() {
                ColumnSchema::array_variable(
                    name,
                    ty,
                    usize::try_from(ndim).ok().filter(|&n| n > 0),
                )
            } else {
                let mut extents: Vec<usize> = shape.iter().map(|&extent| extent as usize).collect();
                if c_order {
                    extents.reverse();
                }
                ColumnSchema::array_fixed(name, ty, extents)
            }
        }
        (None, Some(Some(ty))) => ColumnSchema::scalar(name, ty),
        (None, Some(None)) => ColumnSchema::record(name),
        (None, None) => {
            return Err(failure(format!(
                "addColumns failed: Unknown data type {kind} for scalar column {name}"
            )));
        }
    };
    let schema = schema
        .with_options(ColumnOptions {
            direct: option & DIRECT != 0,
            undefined: option & UNDEFINED != 0,
        })
        .map_err(|error| failure(format!("addColumns failed: {error}")))?;
    let schema = match comment {
        Some(comment) => schema.with_comment(comment),
        None => schema,
    };
    let schema = match u32::try_from(max_length).ok().filter(|&length| length > 0) {
        Some(length) => schema.with_max_length(length),
        None => schema,
    };
    let keywords = match fields.get("keywords") {
        None => None,
        Some(keywords) if keywords.fields().is_some() => match keywords.clone().natural()? {
            Value::Record(record) => Some(record),
            _ => None,
        },
        Some(other) => return Err(wrong_type("Record  ", "keywords", other.type_label())),
    };
    let names_manager = [manager_type, manager_group]
        .iter()
        .any(|text| text.as_deref().is_some_and(|text| !text.is_empty()));
    Ok(Some(NewColumn {
        schema,
        keywords,
        names_manager,
    }))
}
