// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent persisted outputs of a legacy MS TaQL selection.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::MutexGuard;

use casa_mstransform::tool_sort::{check_named_output_path, write_selected_measurement_set_rows};
use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement,
    rename_without_replacing,
};
use casa_tables::taql::IndexStyle;
use casa_tables::taql::ast::{GivingClause, GivingOption};
use casa_tables::taql::eval::{ExprValue, eval_constant};
use casa_tables::{EndianFormat, Table, TableOptions, TableWriteLock};
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;

use super::legacy_query_config::query_defaults;
use super::sort::SortedDestination;
use super::{AttachedMs, runtime_error};

pub(super) struct QueryOutputOptions {
    independent: bool,
    overwrite: bool,
    endian: Option<EndianFormat>,
    storage: Option<QueryStorage>,
}

#[derive(Clone, Copy)]
enum QueryStorage {
    Separate,
    MultiFile,
    MultiHdf5,
}

impl QueryOutputOptions {
    pub(super) fn parse(giving: &GivingClause) -> PyResult<Self> {
        let mut output = Self {
            independent: false,
            overwrite: true,
            endian: None,
            storage: None,
        };
        if let Some(mode) = &giving.output_type {
            output.set_mode(mode)?;
        }
        if let Some(options) = &giving.output_options {
            let mut fields: Vec<(&str, ExprValue, &str)> = Vec::new();
            for field in options {
                let (value, data_type) = option_value(field)?;
                if let Some((_, existing, previous_type)) =
                    fields.iter_mut().find(|(name, _, _)| *name == field.name)
                {
                    if *previous_type != data_type {
                        return Err(runtime_error(format!(
                            "Incorrect TaQL output option data type for field {}",
                            field.name
                        )));
                    }
                    *existing = value;
                } else {
                    fields.push((&field.name, value, data_type));
                }
            }
            for (name, value, _) in fields {
                output.set(name, &value)?;
            }
        }
        if giving.table_name.is_empty() {
            return Err(runtime_error("TaQL output must name a MeasurementSet"));
        }
        if giving.table_name.contains("::") {
            return Err(PyNotImplementedError::new_err(
                "ms.selecttaqlold subtable GIVING is not implemented in casa-rs (#1030)",
            ));
        }
        Ok(output)
    }

    fn set_mode(&mut self, mode: &str) -> PyResult<()> {
        match mode.to_ascii_lowercase().as_str() {
            "plain" | "scratch" => (),
            "plain_local" => self.endian = Some(EndianFormat::LocalEndian),
            "plain_big" => self.endian = Some(EndianFormat::BigEndian),
            "plain_little" => self.endian = Some(EndianFormat::LittleEndian),
            "memory" => {
                return Err(PyNotImplementedError::new_err(
                    "ms.selecttaqlold GIVING AS MEMORY is not implemented in casa-rs (#1030)",
                ));
            }
            _ => {
                return Err(runtime_error(format!(
                    "{mode} is an invalid TaQL output type"
                )));
            }
        }
        self.independent = true;
        Ok(())
    }

    fn set(&mut self, name: &str, value: &ExprValue) -> PyResult<()> {
        let key = name.to_ascii_lowercase();
        match (key.as_str(), value) {
            ("type", ExprValue::String(mode)) => match mode.to_ascii_lowercase().as_str() {
                "plain" | "scratch" | "memory" => self.set_mode(mode),
                _ => Err(runtime_error(
                    "TaQL output type must be plain, scratch or memory",
                )),
            },
            ("endian", ExprValue::String(endian)) => {
                self.endian = match endian.to_ascii_lowercase().as_str() {
                    "aipsrc" => None,
                    "local" => Some(EndianFormat::LocalEndian),
                    "big" => Some(EndianFormat::BigEndian),
                    "little" => Some(EndianFormat::LittleEndian),
                    _ => return Err(runtime_error("Invalid TaQL output byte order")),
                };
                Ok(())
            }
            ("overwrite", ExprValue::Bool(overwrite)) => {
                self.overwrite = *overwrite;
                Ok(())
            }
            ("storage", ExprValue::String(storage)) => {
                self.storage = match storage.to_ascii_lowercase().as_str() {
                    "aipsrc" => None,
                    "sepfile" | "default" => Some(QueryStorage::Separate),
                    "multifile" => Some(QueryStorage::MultiFile),
                    "multihdf5" => Some(QueryStorage::MultiHdf5),
                    _ => return Err(runtime_error("Invalid TaQL output storage option")),
                };
                Ok(())
            }
            ("blocksize", ExprValue::Int(_)) => Ok(()),
            (_, ExprValue::Bool(true)) => self.set_mode(&key),
            _ => Err(runtime_error(format!("Invalid TaQL output option {name}"))),
        }
    }

    fn copy_endian(&self) -> PyResult<EndianFormat> {
        let defaults = query_defaults();
        let storage = self.storage.unwrap_or(match defaults.storage.as_deref() {
            Some("multifile") => QueryStorage::MultiFile,
            Some("multihdf5") => QueryStorage::MultiHdf5,
            _ => QueryStorage::Separate,
        });
        match storage {
            QueryStorage::Separate => Ok(self.endian.unwrap_or(defaults.endian)),
            QueryStorage::MultiFile => Err(PyNotImplementedError::new_err(
                "ms.selecttaqlold MultiFile output is not implemented in casa-rs (#1030)",
            )),
            QueryStorage::MultiHdf5 => Err(runtime_error("TaQL MultiHDF5 storage is unavailable")),
        }
    }
}

fn option_value(field: &GivingOption) -> PyResult<(ExprValue, &'static str)> {
    let value =
        eval_constant(&field.value, &field.name, IndexStyle::default()).map_err(runtime_error)?;
    let Some(data_type) = &field.data_type else {
        let data_type = match value {
            ExprValue::Bool(_) => "B",
            ExprValue::String(_) => "S",
            ExprValue::Int(_) => "I8",
            _ => {
                return Err(runtime_error(format!(
                    "Unsupported value for TaQL output option {}",
                    field.name
                )));
            }
        };
        return Ok((value, data_type));
    };
    let value = match (data_type.to_ascii_uppercase().as_str(), value) {
        ("B", value @ ExprValue::Bool(_)) => (value, "B"),
        ("S", value @ ExprValue::String(_)) => (value, "S"),
        ("I2", ExprValue::Int(value)) => (ExprValue::Int(i64::from(value as i16)), "I2"),
        ("I4", ExprValue::Int(value)) => (ExprValue::Int(i64::from(value as i32)), "I4"),
        ("I8", value @ ExprValue::Int(_)) => (value, "I8"),
        ("U1", ExprValue::Int(value)) => (ExprValue::Int(i64::from(value as u8)), "U1"),
        ("U4", ExprValue::Int(value)) => (ExprValue::Int(i64::from(value as u32)), "U4"),
        _ => {
            return Err(runtime_error(format!(
                "Invalid data type {data_type} for TaQL output option {}",
                field.name
            )));
        }
    };
    Ok(value)
}

impl AttachedMs {
    pub(super) fn save_legacy_query(
        &self,
        table: &Table,
        rows: &[usize],
        output: &Path,
        options: QueryOutputOptions,
        original: &Path,
    ) -> PyResult<Option<Self>> {
        let endian = options
            .independent
            .then(|| options.copy_endian())
            .transpose()?;
        let name = output
            .file_name()
            .ok_or_else(|| runtime_error("TaQL output must name a MeasurementSet"))?;
        let parent = output
            .parent()
            .ok_or_else(|| runtime_error("TaQL output has no parent directory"))?
            .canonicalize()
            .map_err(runtime_error)?;
        let output = parent.join(name);
        let current = self.path.as_path();
        if options.independent {
            for input in std::iter::once(current).chain((original != current).then_some(original)) {
                if input.starts_with(&output) {
                    return Err(runtime_error(
                        "cannot overwrite the attached MeasurementSet with a named copy",
                    ));
                }
                check_named_output_path(input, &output).map_err(runtime_error)?;
            }
        } else {
            for input in [original, current]
                .into_iter()
                .chain(self.sources.iter().map(|path| path.as_path()))
            {
                crate::tools::table::refuse_nested(&output, input).map_err(PyErr::from)?;
            }
        }
        crate::tools::table::check_target(&output).map_err(PyErr::from)?;
        let destination = SortedDestination::inspect(&output, "TaQL selection")?;
        if !options.overwrite && !matches!(destination, SortedDestination::Absent) {
            return Err(runtime_error("TaQL output already exists"));
        }
        let stage = tempfile::Builder::new()
            .prefix(".taql-ms-")
            .tempdir_in(&parent)
            .map_err(runtime_error)?;
        let staged = if let Some(endian) = endian {
            let staged = stage.path().join("selected.ms");
            let source_locks = self
                .read_locks
                .keys()
                .chain(self.write_locks.keys())
                .cloned()
                .collect();
            write_selected_measurement_set_rows(table, rows, &staged, endian, &source_locks)
                .map_err(runtime_error)?;
            staged
        } else {
            let staged = stage.path().to_owned();
            table
                .select_rows(rows)
                .and_then(|view| view.save(TableOptions::new(&staged)))
                .map_err(runtime_error)?;
            staged
        };
        self.working_metadata_link()
            .verify_directory()
            .map_err(runtime_error)?;
        let _stage_lock = options
            .independent
            .then(|| TableWriteLock::acquire_to_reread(&staged, 1))
            .transpose()
            .map_err(runtime_error)?;
        publish_selection(&output, &staged, stage, destination, |publication| {
            options
                .independent
                .then(|| {
                    Self::open_with_publication_guard(
                        output.clone(),
                        false,
                        false,
                        false,
                        publication,
                    )
                })
                .transpose()
        })
    }
}

fn publish_selection<T>(
    output: &Path,
    staged: &Path,
    stage: tempfile::TempDir,
    destination: SortedDestination,
    attach: impl FnOnce(&MutexGuard<'static, ()>) -> PyResult<T>,
) -> PyResult<T> {
    let publication = crate::tools::table::lock_attachment_publication();
    let had_table = destination.is_table();
    let _locks = if had_table {
        if crate::tools::table::replacement_target_is_in_use(output) {
            return Err(runtime_error("TaQL output is still open in this process"));
        }
        check_write_access(output).map_err(runtime_error)?;
        lock_table_tree_for_replacement(output).map_err(runtime_error)?
    } else {
        Vec::new()
    };
    let empty_permissions = match &destination {
        SortedDestination::Empty(metadata) => Some(metadata.permissions()),
        _ => None,
    };
    destination.publish(staged, output, "TaQL selection")?;
    match attach(&publication) {
        Ok(attached) => Ok(attached),
        Err(error) => match restore_destination(output, staged, had_table, empty_permissions) {
            Ok(()) => Err(error),
            Err(rollback) => Err(runtime_error(format!(
                "could not attach TaQL output: {error}; rollback failed: {rollback}; staging retained at {}",
                stage.keep().display()
            ))),
        },
    }
}

fn restore_destination(
    output: &Path,
    staged: &Path,
    had_table: bool,
    empty_permissions: Option<fs::Permissions>,
) -> io::Result<()> {
    if had_table {
        return exchange_directories(staged, output);
    }
    rename_without_replacing(output, staged)?;
    if let Some(permissions) = empty_permissions {
        fs::create_dir(output)?;
        fs::set_permissions(output, permissions)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use casa_tables::TableSchema;

    use super::*;

    #[test]
    fn failed_attachment_restores_the_previous_table() {
        let scratch = tempfile::tempdir().unwrap();
        let stage = tempfile::tempdir_in(scratch.path()).unwrap();
        let staged = stage.path().join("result.ms");
        let output = scratch.path().join("output.ms");
        for path in [&staged, &output] {
            let schema = TableSchema::new(Vec::new()).unwrap();
            Table::with_schema(schema)
                .save(TableOptions::new(path))
                .unwrap();
        }
        fs::write(output.join("original"), "preserve").unwrap();
        fs::write(staged.join("replacement"), "staged").unwrap();
        let destination = SortedDestination::inspect(&output, "TaQL selection").unwrap();
        let mut reached_attachment = false;
        let result = publish_selection(&output, &staged, stage, destination, |_| {
            reached_attachment = true;
            assert!(output.join("replacement").is_file());
            Err::<(), _>(runtime_error("simulated failure attaching the new view"))
        });
        assert!(reached_attachment);
        assert!(result.is_err());
        assert_eq!(
            fs::read_to_string(output.join("original")).unwrap(),
            "preserve"
        );
        assert!(!output.join("replacement").exists());
    }
}
