// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA component-list keyword reads from the attached casacore table.

mod cache;
pub(super) use cache::{Shared, attach};
pub(crate) use cache::{clear_locks, flush_pending};

use std::path::Path;
use std::sync::{Arc, PoisonError};

use casa_core::{RecordValue, Value};
use casa_tables::{Table, TableOptions};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};

use crate::convert::value::CasaValue;
use crate::convert::{ConvertError, ConvertResult};
use crate::tools::table::{ComponentKeywordValue, define_component_keyword, values};

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
pub(super) fn has(shared: &Arc<Shared>, name: &str) -> ConvertResult<CasaValue> {
    cache::flush_except(shared)?;
    let mut cache = shared.cache.lock().unwrap_or_else(PoisonError::into_inner);
    cache.read(&shared.path, |table| {
        Ok(CasaValue::Bool(table.keywords().get(name).is_some()))
    })
}

/// `getKeyword` resolves dotted names through records as TableProxy does.
pub(super) fn get(shared: &Arc<Shared>, name: &str) -> ConvertResult<CasaValue> {
    cache::flush_except(shared)?;
    let mut cache = shared.cache.lock().unwrap_or_else(PoisonError::into_inner);
    cache.read(&shared.path, |table| {
        Ok(values::value(lookup(table.keywords(), name)?, &shared.path))
    })
}

/// Keep dirty keywords in the attached table under its write lock. CASA
/// likewise writes these at release rather than rewriting every put.
pub(super) fn put(
    shared: &Arc<Shared>,
    name: &str,
    value: ComponentKeywordValue,
) -> ConvertResult<()> {
    cache::flush_except(shared)?;
    let mut cache = shared.cache.lock().unwrap_or_else(PoisonError::into_inner);
    cache.begin_write()?;
    let mut next = cache.table.keywords().clone();
    let result = target_name(&next, name)
        .and_then(|target| define_component_keyword(&mut next, target, value));
    if result.is_ok() {
        *cache.table.keywords_mut() = next;
    }
    if result.is_err() {
        cache.flush(&shared.path)?;
    }
    result
}

pub(super) fn release(shared: &Arc<Shared>) -> ConvertResult<()> {
    shared
        .cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .flush(&shared.path)
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
