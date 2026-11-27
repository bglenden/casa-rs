// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent persisted outputs of a legacy MS TaQL selection.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, MutexGuard};

use casa_mstransform::tool_sort::{
    check_named_output_path, named_keyword_output_staging_parent,
    write_selected_measurement_set_rows,
};
use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement,
    rename_without_replacing,
};
use casa_tables::taql::IndexStyle;
use casa_tables::taql::ast::{GivingClause, GivingOption};
use casa_tables::taql::eval::{ExprValue, eval_constant};
use casa_tables::{EndianFormat, Table, TableOptions, TableWriteLock};
use pyo3::prelude::*;

use super::legacy_query_config::query_defaults;
use super::legacy_query_subtable::QuerySubtable;
use super::sort::SortedDestination;
use super::{AttachedMs, runtime_error};

pub(super) struct QueryOutputOptions {
    independent: bool,
    pub(super) memory: bool,
    overwrite: bool,
    endian: Option<EndianFormat>,
    storage: Option<QueryStorage>,
    block_size: i32,
}

#[derive(Clone, Copy, Default)]
struct QueryDiskFormat {
    endian: EndianFormat,
    multifile_block_size: Option<usize>,
}

pub(super) struct StagedLegacyQuery {
    output: PathBuf,
    staged: PathBuf,
    stage: tempfile::TempDir,
    destination: SortedDestination,
    subtable: Option<QuerySubtable>,
    independent: bool,
    _stage_lock: Option<TableWriteLock>,
}

pub(super) enum PublishedLegacyQuery {
    Independent(Box<AttachedMs>),
    Reference(Arc<Vec<PathBuf>>),
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
            memory: false,
            overwrite: true,
            endian: None,
            storage: None,
            block_size: -2,
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
        if output.independent && !output.memory && giving.table_name.is_empty() {
            return Err(runtime_error("TaQL output must name a MeasurementSet"));
        }
        Ok(output)
    }

    fn set_mode(&mut self, mode: &str) -> PyResult<()> {
        self.memory = false;
        match mode.to_ascii_lowercase().as_str() {
            "plain" | "scratch" => (),
            "plain_local" => self.endian = Some(EndianFormat::LocalEndian),
            "plain_big" => self.endian = Some(EndianFormat::BigEndian),
            "plain_little" => self.endian = Some(EndianFormat::LittleEndian),
            "memory" => self.memory = true,
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
            ("blocksize", ExprValue::Int(size)) => {
                self.block_size = *size as i32;
                Ok(())
            }
            (_, ExprValue::Bool(true)) => self.set_mode(&key),
            _ => Err(runtime_error(format!("Invalid TaQL output option {name}"))),
        }
    }

    fn disk_format(&self) -> PyResult<QueryDiskFormat> {
        let defaults = query_defaults();
        let storage = self.storage.unwrap_or(match defaults.storage.as_deref() {
            Some("multifile") => QueryStorage::MultiFile,
            Some("multihdf5") => QueryStorage::MultiHdf5,
            _ => QueryStorage::Separate,
        });
        let multifile_block_size = match storage {
            QueryStorage::Separate => None,
            QueryStorage::MultiFile => {
                let configured = if self.block_size <= -2 {
                    defaults.block_size
                } else {
                    self.block_size
                };
                Some(if configured > 0 {
                    configured as usize
                } else {
                    4 * 1024 * 1024
                })
            }
            QueryStorage::MultiHdf5 => {
                return Err(runtime_error("TaQL MultiHDF5 storage is unavailable"));
            }
        };
        Ok(QueryDiskFormat {
            endian: self.endian.unwrap_or(defaults.endian),
            multifile_block_size,
        })
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
    pub(super) fn stage_legacy_query(
        &self,
        table: &Table,
        rows: &[usize],
        name: &str,
        options: QueryOutputOptions,
        original: &Path,
    ) -> PyResult<Option<StagedLegacyQuery>> {
        if name.is_empty() {
            return Ok(None);
        }
        let format = options
            .independent
            .then(|| options.disk_format())
            .transpose()?
            .or_else(|| self.memory_ms.as_ref().map(|_| QueryDiskFormat::default()));
        let active = self.legacy_selection.query_parent(&self.path);
        if active.is_none() && self.memory_ms.is_some() && QuerySubtable::has_implicit_parent(name)?
        {
            return Err(runtime_error(
                "the active TaQL view has no persisted directory",
            ));
        }
        let subtable = QuerySubtable::resolve(name, active, &self.path)?;
        let output = if let Some(subtable) = &subtable {
            subtable.output.clone()
        } else {
            canonical_named_output(name)?
        };
        let parent = output
            .parent()
            .ok_or_else(|| runtime_error("TaQL output has no parent directory"))?;
        let copy_parent = self.check_query_destination(
            &output,
            original,
            options.independent,
            subtable.as_ref(),
        )?;
        crate::tools::table::check_target(&output).map_err(PyErr::from)?;
        let destination = SortedDestination::inspect(&output, "TaQL selection")?;
        if !options.overwrite && !matches!(destination, SortedDestination::Absent) {
            return Err(runtime_error("TaQL output already exists"));
        }
        let stage = tempfile::Builder::new()
            .prefix(".taql-ms-")
            .tempdir_in(if format.is_some() {
                &copy_parent
            } else {
                parent
            })
            .map_err(runtime_error)?;
        let staged = if let Some(format) = format {
            let staged = stage.path().join("selected.ms");
            let source_locks = self
                .read_locks
                .keys()
                .chain(self.write_locks.keys())
                .cloned()
                .collect();
            if let Some(memory) = &self.memory_ms {
                memory
                    .to_memory_rows(rows)
                    .and_then(|mut copy| copy.save_detached_in_stage(&staged, format.endian))
                    .map_err(runtime_error)?;
            } else {
                write_selected_measurement_set_rows(
                    table,
                    rows,
                    &staged,
                    format.endian,
                    &source_locks,
                )
                .map_err(runtime_error)?;
            }
            if let Some(block_size) = format.multifile_block_size {
                Table::pack_staged_multifile(&staged, block_size).map_err(runtime_error)?;
            }
            staged
        } else {
            let staged = stage.path().to_owned();
            table
                .select_rows(rows)
                .and_then(|view| view.save_relative_to(TableOptions::new(&staged), &output))
                .map_err(runtime_error)?;
            staged
        };
        self.working_metadata_link()
            .verify_directory()
            .map_err(runtime_error)?;
        let stage_lock = format
            .is_some()
            .then(|| TableWriteLock::acquire_to_reread(&staged, 1))
            .transpose()
            .map_err(runtime_error)?;
        Ok(Some(StagedLegacyQuery {
            output,
            staged,
            stage,
            destination,
            subtable,
            independent: options.independent,
            _stage_lock: stage_lock,
        }))
    }

    fn check_query_destination(
        &self,
        output: &Path,
        original: &Path,
        independent: bool,
        subtable: Option<&QuerySubtable>,
    ) -> PyResult<PathBuf> {
        let current = self.path.as_path();
        let active = self.legacy_selection.query_parent(current);
        let inputs = std::iter::once(current)
            .chain((original != current).then_some(original))
            .chain(active.filter(|path| *path != current && *path != original));
        let mut parent = output
            .parent()
            .expect("validated output has a parent")
            .to_owned();
        if self.memory_ms.is_some() {
            for (source, linked) in &self.memory_source_paths {
                if source.starts_with(output)
                    || (*linked && output.starts_with(source) && subtable.is_none())
                {
                    return Err(runtime_error("TaQL output overlaps a source table"));
                }
                if *linked && parent.starts_with(source) {
                    parent = source
                        .parent()
                        .ok_or_else(|| runtime_error("no staging parent outside the source table"))?
                        .to_owned();
                }
            }
            return Ok(parent);
        }
        if let Some(link) = subtable {
            for input in inputs {
                let outside =
                    named_keyword_output_staging_parent(input, output, &link.owner, &link.keyword)
                        .map_err(runtime_error)?;
                if parent.starts_with(&outside) {
                    parent = outside;
                }
            }
        } else if independent {
            for input in inputs {
                if input.starts_with(output) {
                    return Err(runtime_error(
                        "cannot overwrite the attached MeasurementSet with a named copy",
                    ));
                }
                check_named_output_path(input, output).map_err(runtime_error)?;
            }
        } else {
            for input in inputs.chain(self.sources.iter().map(|path| path.as_path())) {
                crate::tools::table::refuse_nested(output, input).map_err(PyErr::from)?;
            }
        }
        Ok(parent)
    }
}

impl StagedLegacyQuery {
    pub(super) fn publish(
        self,
        retained: &mut BTreeMap<PathBuf, TableWriteLock>,
    ) -> PyResult<PublishedLegacyQuery> {
        let mut owner_lock = None;
        let link_lock = if let Some(subtable) = &self.subtable {
            let lock = match retained.get_mut(&subtable.owner) {
                Some(lock) => lock,
                None => owner_lock.insert(
                    TableWriteLock::acquire_to_reread(&subtable.owner, 1).map_err(runtime_error)?,
                ),
            };
            subtable.verify_lock(lock)?;
            Some((subtable, lock))
        } else {
            None
        };
        publish_selection(
            &self.output,
            &self.staged,
            self.stage,
            self.destination,
            |publication| {
                let attached = self
                    .independent
                    .then(|| {
                        AttachedMs::open_with_publication_guard(
                            self.output.clone(),
                            false,
                            false,
                            false,
                            publication,
                        )
                    })
                    .transpose()?;
                if let Some((subtable, lock)) = link_lock {
                    subtable.write_link(lock)?;
                }
                Ok(match attached {
                    Some(attached) => PublishedLegacyQuery::Independent(Box::new(attached)),
                    None => PublishedLegacyQuery::Reference(
                        crate::tools::table::attach_table_paths(vec![self.output.clone()]),
                    ),
                })
            },
        )
    }
}

fn canonical_named_output(name: &str) -> PyResult<PathBuf> {
    let absolute = std::path::absolute(name).map_err(runtime_error)?;
    let name = absolute
        .file_name()
        .ok_or_else(|| runtime_error("TaQL output must name a MeasurementSet"))?;
    let parent = absolute
        .parent()
        .ok_or_else(|| runtime_error("TaQL output has no parent directory"))?
        .canonicalize()
        .map_err(runtime_error)?;
    Ok(parent.join(name))
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
