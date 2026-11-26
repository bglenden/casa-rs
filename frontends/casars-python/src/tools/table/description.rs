// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA table descriptions for saved plain tables (#1028).

use std::path::Path;

use casa_core::{RecordValue, Value};
use casa_tables::{StoredTableDescription, Table, TableOptions};
use pyo3::prelude::*;

use super::state::{Fail, State, no_table, no_table_specified};
use super::{PyTableTool, flag, values};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

fn hypercolumns(private: &RecordValue, owner: &Path) -> Result<CasaValue, Fail> {
    let mut result = Vec::new();
    for field in private.fields() {
        let Some(name) = field.name.strip_prefix("Hypercolumn_") else {
            continue;
        };
        let Value::Record(description) = &field.value else {
            return Err(Fail::Runtime(format!(
                "Invalid hypercolumn description {name}"
            )));
        };
        let mut fields = Vec::new();
        for (source, output) in [
            ("ndim", "HCndim"),
            ("data", "HCdatanames"),
            ("coord", "HCcoordnames"),
            ("id", "HCidnames"),
        ] {
            let value = description
                .get(source)
                .ok_or_else(|| Fail::Runtime(format!("Missing {source} in hypercolumn {name}")))?;
            fields.push((output, values::value(value, owner)));
        }
        result.push((name.to_string(), CasaValue::record(fields)));
    }
    Ok(CasaValue::record(result))
}

fn reference_description(root_table_ref: &Table) -> Result<StoredTableDescription, Fail> {
    let root = root_table_ref.simple_reference_root()?.ok_or_else(|| {
        Fail::Unsupported(
            "table.getdesc for this reference table is not yet implemented in casa-rs (#1028)"
                .into(),
        )
    })?;
    let root_table = Table::open(TableOptions::new(root))?;
    root_table.stored_table_description()?.ok_or_else(|| {
        Fail::Unsupported(
            "table.getdesc for this reference root is not yet implemented in casa-rs (#1028)"
                .into(),
        )
    })
}

impl State {
    fn getdesc(&self, actual: bool, log: &mut Vec<LogLine>) -> Result<CasaValue, Fail> {
        let Some(open) = self.open.as_ref() else {
            no_table(log);
            no_table_specified(log);
            return Ok(CasaValue::empty_record());
        };
        let root = open.root();
        let description = if root.table.is_reference_table()? {
            reference_description(&root.table)?
        } else {
            match root.table.stored_table_description()? {
                Some(description) => description,
                None if root.table.is_memory() => StoredTableDescription {
                    declared_managers: root
                        .table
                        .schema()
                        .into_iter()
                        .flat_map(|schema| schema.columns())
                        .map(|column| {
                            let (kind, group) = if root.memory_added_columns.contains(column.name())
                            {
                                ("MemoryStMan", "MSMTAB")
                            } else {
                                ("StandardStMan", "StandardStMan")
                            };
                            (column.name().to_string(), kind.into(), group.into())
                        })
                        .collect(),
                    private_keywords: RecordValue::default(),
                },
                None => {
                    return Err(Fail::Unsupported(
                        "table.getdesc for concatenated tables is not yet implemented in casa-rs (#1028)".into(),
                    ));
                }
            }
        };
        let mut fields = vec![
            (
                "_define_hypercolumn_".to_string(),
                hypercolumns(&description.private_keywords, &root.name)?,
            ),
            (
                "_keywords_".to_string(),
                values::record(root.table.keywords(), &root.name),
            ),
            (
                "_private_keywords_".to_string(),
                values::record(&description.private_keywords, &root.name),
            ),
        ];
        let visible_cols = open.columns.clone();
        drop(root);
        let managers: Vec<(String, String, String)> = match visible_cols {
            Some(order) => order
                .into_iter()
                .filter_map(|col| {
                    description
                        .declared_managers
                        .iter()
                        .find(|(name, _, _)| name == &col)
                        .cloned()
                })
                .collect(),
            None => description.declared_managers,
        };
        for (name, declared_type, declared_group) in managers {
            let mut column = self.getcoldesc(&name, log)?;
            if !actual && let CasaValue::Record(ref mut parts) = column {
                for (key, value) in parts {
                    match key.as_str() {
                        "dataManagerType" => *value = CasaValue::Text(declared_type.clone()),
                        "dataManagerGroup" => *value = CasaValue::Text(declared_group.clone()),
                        _ => (),
                    }
                }
            }
            fields.push((name, column));
        }
        Ok(CasaValue::record(fields))
    }
}

#[pymethods]
impl PyTableTool {
    /// Return a table description with declared or actual manager bindings.
    #[pyo3(signature = (actual = None), text_signature = "($self, actual=True)")]
    fn getdesc(&self, py: Python<'_>, actual: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let actual = flag(actual, true)?;
        self.read(py, move |state, log| state.getdesc(actual, log))
    }
}
