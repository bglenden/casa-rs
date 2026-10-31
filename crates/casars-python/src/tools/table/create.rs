// SPDX-License-Identifier: LGPL-3.0-or-later
//! Table creation, using the same descriptions as `addcols` (#1028).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use casa_tables::{Table, TableError, TableInfo, TableOptions, TableSchema};
use casa_types::Value;
use pyo3::prelude::*;

use super::copy::{
    check_target, endian_format, refuse_nested, replace_with, same_directory,
    used_by_another_process,
};
use super::keyword_value::{Data, Incoming, Wide};
use super::new_column;
use super::state::{Fail, LockRequest, LockSettings, Open, State};
use super::write_structure::default_row;
use super::{PyTableTool, integer, lock_request, record_fields, text};
use crate::convert::value::CasaValue;

struct CreateRequest {
    name: String,
    description: Vec<(String, Incoming)>,
    lock: LockRequest,
    endian: String,
    memory: bool,
    rows: i64,
    dminfo: Vec<(String, Incoming)>,
}

/// The single unnamed AipsIO group is the one layout the writer can both
/// create and update without changing the requested manager.
fn supported_dminfo(info: &[(String, Incoming)], schema: &TableSchema) -> bool {
    let [(_, Incoming::Record(manager))] = info else {
        return false;
    };
    let get = |name: &str| {
        manager
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    };
    if manager
        .iter()
        .any(|(key, _)| !["TYPE", "NAME", "COLUMNS", "SPEC", "SEQNR"].contains(&key.as_str()))
        || get("TYPE").and_then(Incoming::text) != Some("StManAipsIO")
        || get("NAME").and_then(Incoming::text) != Some("")
        || !matches!(get("SPEC"), Some(Incoming::Record(fields)) if fields.is_empty())
        || get("SEQNR").is_some_and(|value| value.scalar_integer() != Some(0))
    {
        return false;
    }
    let Some(Incoming::Wide(Wide {
        shape: Some(shape),
        data: Data::Texts(names),
    })) = get("COLUMNS")
    else {
        return false;
    };
    if shape.len() != 1 {
        return false;
    }
    let mut requested = names.clone();
    let mut actual: Vec<_> = schema
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect();
    requested.sort();
    actual.sort();
    requested == actual
}

impl State {
    /// Create a concatenated table whose parts remain in their present places.
    fn createmultitable(
        &mut self,
        output: String,
        names: Vec<String>,
        subdirectory: String,
        log: &mut Vec<crate::tools::LogLine>,
    ) -> Result<CasaValue, Fail> {
        if names.is_empty() {
            return Ok(CasaValue::Bool(false));
        }
        let mut tables = Vec::with_capacity(names.len());
        let mut paths = Vec::with_capacity(names.len());
        for name in names {
            let path = std::path::absolute(Path::new(&name))
                .map_err(|error| Fail::Runtime(error.to_string()))?;
            match Table::open(TableOptions::new(&path)) {
                Ok(table) => tables.push(table),
                Err(_) => return Ok(CasaValue::Bool(false)),
            }
            paths.push(path);
        }
        let concat = match Table::concat(tables) {
            Ok(concat) => concat,
            Err(_) => return Ok(CasaValue::Bool(false)),
        };
        let target = std::path::absolute(Path::new(&output))
            .map_err(|error| Fail::Runtime(error.to_string()))?;
        for path in &paths {
            refuse_nested(&target, path)?;
        }
        if self
            .open
            .as_ref()
            .is_some_and(|open| same_directory(&open.name, &target))
        {
            return Ok(CasaValue::Bool(false));
        }
        check_target(&target)?;
        self.refuse_open_target(&target)?;
        if used_by_another_process(&target) {
            return Err(Fail::Runtime(format!(
                "Table {} is open in another process and was not replaced",
                target.display()
            )));
        }
        if !subdirectory.is_empty() {
            drop(concat);
            return self.move_multitable(&target, &paths, Path::new(&subdirectory), log);
        }
        let mut readme = vec!["Virtual concatenation of the following tables:".to_string()];
        readme.extend(paths.iter().map(|path| format!("  {}", path.display())));
        let info = TableInfo {
            sub_type: "CONCATENATED".into(),
            readme,
            ..TableInfo::default()
        };
        replace_with(&target, |staging| {
            concat.save(TableOptions::new(staging))?;
            std::fs::write(staging.join("table.info"), info.to_string())
                .map_err(|error| Fail::Runtime(error.to_string()))?;
            Ok(())
        })?;
        Ok(CasaValue::Bool(true))
    }

    fn create(&mut self, mut request: CreateRequest) -> Result<CasaValue, Fail> {
        // CASA closes the current tool before constructing the replacement.
        self.close();
        let endian = endian_format(if request.endian.is_empty() {
            "aipsrc"
        } else {
            &request.endian
        })?;
        // A bad lock mode must not replace an existing target first.
        LockSettings::from_request(&request.lock)?;
        if request.name.is_empty() && !request.memory {
            return Err(Fail::Unsupported(
                "table.create with a disk scratch name is not yet implemented in casa-rs (#1028)"
                    .into(),
            ));
        }
        if request.name.is_empty() {
            static NEXT_SCRATCH: AtomicU64 = AtomicU64::new(0);
            loop {
                let candidate = format!(
                    "tab{}_{}",
                    std::process::id(),
                    NEXT_SCRATCH.fetch_add(1, Ordering::Relaxed)
                );
                if !Path::new(&candidate).exists() {
                    request.name = candidate;
                    break;
                }
            }
        }
        request.description.sort_by(|a, b| a.0.cmp(&b.0));
        let mut columns = Vec::new();
        let mut keywords = None;
        for (name, description) in &request.description {
            if name == "_keywords_" {
                keywords = match description.clone().natural()? {
                    Value::Record(record) => Some(record),
                    _ => return Err(Fail::Runtime("table keywords must be a record".into())),
                };
            } else if let Some(column) = new_column::parse(name, description)? {
                if column.names_manager {
                    return Err(Fail::Unsupported(
                        "table.create with a named storage manager is not yet implemented in casa-rs (#1028)".into(),
                    ));
                }
                columns.push(column);
            }
        }
        let schema = TableSchema::new(columns.iter().map(|column| column.schema.clone()).collect())
            .map_err(|error| Fail::from(TableError::from(error)))?;
        if !request.dminfo.is_empty()
            && (request.memory || !supported_dminfo(&request.dminfo, &schema))
        {
            return Err(Fail::Unsupported(
                "table.create with this dminfo is not yet implemented in casa-rs (#1028)".into(),
            ));
        }
        let mut table = if request.memory {
            Table::with_schema_memory(schema)
        } else {
            Table::with_schema(schema)
        };
        if let Some(keywords) = keywords {
            *table.keywords_mut() = keywords;
        }
        for column in columns {
            if let Some(keywords) = column.keywords {
                table.set_column_keywords(column.schema.name(), keywords);
            }
        }
        for _ in 0..request.rows.max(0) {
            let row = default_row(&table);
            table.add_row(row)?;
        }
        let target = std::path::absolute(Path::new(&request.name))
            .map_err(|error| Fail::Runtime(error.to_string()))?;
        if request.memory {
            let lock = LockSettings::from_request(&LockRequest {
                option: Some("permanent".into()),
                ..LockRequest::default()
            })?;
            self.open = Some(Open {
                table,
                name: target,
                writable: true,
                lock,
                memory_added_columns: Default::default(),
                last_modify_counter: 0,
            });
            return Ok(CasaValue::Bool(true));
        }
        check_target(&target)?;
        self.refuse_open_target(&target)?;
        if used_by_another_process(&target) {
            return Err(Fail::Runtime(format!(
                "Table {} is open in another process and was not replaced",
                target.display()
            )));
        }
        replace_with(&target, |staging| {
            table.save(TableOptions::new(staging).with_endian_format(endian))?;
            Ok(())
        })?;
        self.open = Some(Open::attach(
            &target.display().to_string(),
            &request.lock,
            true,
        )?);
        Ok(CasaValue::Bool(true))
    }
}

#[pymethods]
#[allow(clippy::too_many_arguments, non_snake_case)]
impl PyTableTool {
    /// Create a table and open it for update.
    #[pyo3(signature = (tablename = None, tabledesc = None, lockoptions = None, endianformat = None, memtype = None, nrow = None, dminfo = None), text_signature = "($self, tablename='', tabledesc={}, lockoptions={}, endianformat='', memtype='', nrow=0, dminfo={})")]
    fn create(
        &self,
        py: Python<'_>,
        tablename: Option<&Bound<'_, PyAny>>,
        tabledesc: Option<&Bound<'_, PyAny>>,
        lockoptions: Option<&Bound<'_, PyAny>>,
        endianformat: Option<&Bound<'_, PyAny>>,
        memtype: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        dminfo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let name = tablename.map_or(Ok(String::new()), |value| text(value, "tablename"))?;
        let description = tabledesc.map_or(Ok(Vec::new()), record_fields)?;
        let lock = lock_request(lockoptions)?;
        let endian = endianformat.map_or(Ok(String::new()), |value| text(value, "endianformat"))?;
        let memory = memtype.map_or(Ok(String::new()), |value| text(value, "memtype"))? == "memory";
        let rows = integer(nrow, "nrow", 0)?;
        let dminfo = dminfo.map_or(Ok(Vec::new()), record_fields)?;
        self.call(py, move |state, _| {
            state.create(CreateRequest {
                name,
                description,
                lock,
                endian,
                memory,
                rows,
                dminfo,
            })
        })
    }

    /// Create a concatenation of tables.
    #[pyo3(signature = (outputTableName, tables, subdirname), text_signature = "($self, outputTableName, tables, subdirname)")]
    fn createmultitable(
        &self,
        py: Python<'_>,
        outputTableName: &Bound<'_, PyAny>,
        tables: &Bound<'_, PyAny>,
        subdirname: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let output = text(outputTableName, "outputTableName")?;
        let tables = tables.extract::<Vec<String>>()?;
        let subdirectory = text(subdirname, "subdirname")?;
        self.call(py, move |state, log| {
            match state.createmultitable(output, tables, subdirectory, log) {
                Ok(value) => Ok(value),
                Err(_) => Ok(CasaValue::Bool(false)),
            }
        })
    }
}
