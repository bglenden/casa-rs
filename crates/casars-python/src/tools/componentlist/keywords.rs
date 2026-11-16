// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA component-list keyword reads from the attached casacore table.

use std::path::Path;

use casa_tables::{Table, TableOptions, TableWriteLock};
use casa_types::{RecordValue, Value};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::convert::value::CasaValue;
use crate::convert::{ConvertError, ConvertResult};
use crate::tools::table::{
    ComponentKeywordValue, component_keyword_published, define_component_keyword, values,
};

/// CASA defaults an omitted name to `metadata` and accepts UTF-8 bytes.
pub(super) fn argument(py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    let text: String = match value {
        None => "metadata".into(),
        Some(value) if value.is_instance_of::<PyString>() => value.extract()?,
        Some(value) if value.is_instance_of::<PyBytes>() => {
            let bytes = value.cast::<PyBytes>()?.as_bytes();
            let end = bytes
                .iter()
                .position(|&byte| byte == 0)
                .unwrap_or(bytes.len());
            PyBytes::new(py, &bytes[..end])
                .call_method1("decode", ("utf-8",))?
                .extract()?
        }
        Some(_) => return Err(PyTypeError::new_err("argument _keyword must be a string")),
    };
    Ok(text.split('\0').next().unwrap_or_default().to_owned())
}

fn open(path: &Path) -> ConvertResult<Table> {
    #[cfg(unix)]
    let result = {
        use casa_tables::{LockMode, LockOptions};
        Table::open_with_lock(
            TableOptions::new(path),
            LockOptions::new(LockMode::AutoLocking),
        )
    };
    #[cfg(not(unix))]
    let result = Table::open(TableOptions::new(path));
    result.map_err(|error| ConvertError::runtime(error.to_string()))
}

/// `hasKeyword` tests the literal top-level field name, even when it has dots.
pub(super) fn has(path: &Path, name: &str) -> ConvertResult<CasaValue> {
    let table = open(path)?;
    Ok(CasaValue::Bool(table.keywords().get(name).is_some()))
}

/// `getKeyword` resolves dotted names through records as TableProxy does.
pub(super) fn get(path: &Path, name: &str) -> ConvertResult<CasaValue> {
    let table = open(path)?;
    let value = lookup(table.keywords(), name)?;
    Ok(values::value(value, path))
}

/// Write only table metadata, reading the latest generation under the lock.
pub(super) fn put(path: &Path, name: &str, value: ComponentKeywordValue) -> ConvertResult<()> {
    let table_error = |error: casa_tables::TableError| ConvertError::runtime(error.to_string());
    let mut lock = TableWriteLock::acquire_to_reread(path, 0).map_err(table_error)?;
    let mut table = Table::open_metadata_only(TableOptions::new(path)).map_err(table_error)?;
    let mut keywords = table.keywords().clone();
    let target = target_name(&keywords, name)?;
    define_component_keyword(&mut keywords, target, value)?;
    *table.keywords_mut() = keywords;
    // Announce even a partially completed metadata write on release.
    lock.record_write();
    let saved = table
        .save_metadata_only(TableOptions::new(path))
        .map_err(table_error);
    let released = lock.release().map_err(table_error);
    // The table may have changed even if saving or publishing failed.
    component_keyword_published(path);
    saved.and(released)
}

/// TableProxy uses a generated name for each empty-name write. A dotted
/// path must already have record-valued parents; it does not create them.
fn target_name(record: &RecordValue, name: &str) -> ConvertResult<String> {
    if name.is_empty() {
        let mut candidate = "*-1".to_string();
        let mut index = 1_u64;
        while record.get(&candidate).is_some() {
            candidate = format!("*-1_{index}");
            index += 1;
        }
        return Ok(candidate);
    }
    let mut current = record;
    let parts: Vec<_> = name.split('.').collect();
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            return Err(ConvertError::runtime(format!(
                "Part of keyword name {name} is empty"
            )));
        }
        if index + 1 == parts.len() {
            break;
        }
        let found = current.get(part).ok_or_else(|| {
            ConvertError::runtime(format!("RecordInterface: field {part} is unknown"))
        })?;
        let Value::Record(inner) = found else {
            return Err(ConvertError::runtime(format!(
                "Table keyword {} does not have a record-value",
                parts[..=index].join(".")
            )));
        };
        current = inner;
    }
    Ok(name.to_string())
}

fn lookup<'a>(record: &'a RecordValue, name: &str) -> ConvertResult<&'a Value> {
    if name.is_empty() {
        return Err(ConvertError::runtime(
            "RecordInterface::asValueHolder - unknown data type",
        ));
    }
    let mut current = record;
    let parts: Vec<_> = name.split('.').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return Err(ConvertError::runtime(format!(
            "Part of keyword name {name} is empty"
        )));
    }
    for (index, part) in parts.iter().enumerate() {
        let prefix = parts[..=index].join(".");
        let value = current.get(part).ok_or_else(|| {
            ConvertError::runtime(format!("Table keyword {prefix} does not exist"))
        })?;
        if index + 1 == parts.len() {
            return Ok(value);
        }
        let Value::Record(inner) = value else {
            return Err(ConvertError::runtime(format!(
                "Table keyword {prefix} does not have a record-value"
            )));
        };
        current = inner;
    }
    unreachable!("a nonempty keyword has a part")
}
