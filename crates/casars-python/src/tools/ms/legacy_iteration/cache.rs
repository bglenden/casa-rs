// SPDX-License-Identifier: LGPL-3.0-or-later
//! Persist the casacore SORTED_TABLE reference created by a writable iterator.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use casa_ms::tool_iteration::LegacyToolIteration;
use casa_tables::publication::{
    exchange_directories, lock_table_tree_for_replacement_with_held, rename_without_replacing,
};
use casa_tables::{Table, TableOptions, TablePartNames, TableWriteLock};
use casa_types::{ArrayValue, Value};
use ndarray::Array1;
use pyo3::prelude::*;

use super::super::metadata_link::{MetadataLink, compose_rows};
use super::super::sort::SortedDestination;
use super::super::visibility::VisibilityMain;
use super::super::{AttachedMs, runtime_error};

impl AttachedMs {
    pub(super) fn cache_legacy_iteration(
        &mut self,
        link: &MetadataLink,
        captured: &VisibilityMain,
        working_rows: Option<&[usize]>,
        cursor: &LegacyToolIteration,
    ) -> PyResult<()> {
        if !self.writable || self.is_multi_ms {
            return Ok(());
        }
        let projected = compose_rows(working_rows, Some(cursor.ordered_rows()));
        let projected = projected.expect("iterator rows were supplied");
        let (root, selected) = if captured.plain {
            (self.path.clone(), projected.into_owned())
        } else {
            let Some(physical) = captured
                .table
                .reference_write_projection(&projected, &[])
                .map_err(runtime_error)?
            else {
                return Ok(());
            };
            let root = physical.path.canonicalize().map_err(runtime_error)?;
            if !matches!(
                Table::immediate_part_names(&root, &root).map_err(runtime_error)?,
                TablePartNames::Plain(_)
            ) {
                return Ok(());
            }
            (root, physical.rows)
        };
        let output = root.join("SORTED_TABLE");
        if captured.plain && reusable_cache(&captured.table, &output, cursor.keys(), &selected)? {
            captured.verify_write_layout(&BTreeSet::new())?;
            return Ok(());
        }
        let mut temporary = None;
        if !self.write_locks.contains_key(&root) {
            temporary = Some(TableWriteLock::acquire_to_reread(&root, 0).map_err(runtime_error)?);
        }
        if !self
            .write_locks
            .get(&root)
            .or(temporary.as_ref())
            .is_some_and(|lock| lock.matches_table_path(&root))
        {
            return Err(runtime_error(
                "legacy iterator source or write lock was replaced",
            ));
        }
        link.verify_directory().map_err(runtime_error)?;
        captured.verify_write_layout(&BTreeSet::new())?;
        let mut main = Table::open(TableOptions::new(&root)).map_err(runtime_error)?;
        if reusable_cache(&main, &output, cursor.keys(), &selected)? {
            return Ok(());
        }
        let destination = SortedDestination::inspect(&output)?;
        let _output_locks = if destination.is_table() {
            lock_table_tree_for_replacement_with_held(&output, &self.write_locks)
                .map_err(runtime_error)?
        } else {
            Vec::new()
        };
        let version = tempfile::Builder::new()
            .prefix(".sorted-table-")
            .tempdir_in(&root)
            .map_err(runtime_error)?;
        let staged = version.path();
        main.select_rows(&selected)
            .and_then(|reference| {
                reference.save_for_destination(TableOptions::new(staged), &output)
            })
            .map_err(runtime_error)?;
        let replaced_table = destination.is_table();
        let existed = output.exists();
        destination.publish(staged, &output)?;
        main.keywords_mut().upsert(
            "SORT_COLUMNS",
            Value::Array(ArrayValue::String(
                Array1::from_vec(cursor.keys().to_vec()).into_dyn(),
            )),
        );
        main.keywords_mut()
            .upsert("SORTED_TABLE", Value::TableRef("././SORTED_TABLE".into()));
        let lock = self
            .write_locks
            .get_mut(&root)
            .or(temporary.as_mut())
            .expect("legacy iteration holds the source write lock");
        lock.record_write();
        if let Err(error) = main.save_metadata_only(TableOptions::new(&root)) {
            let restored = restore_output(staged, &output, replaced_table, existed);
            return match restored {
                Ok(()) => Err(runtime_error(error)),
                Err(rollback) => {
                    let retained = version.keep();
                    Err(runtime_error(format!(
                        "could not link SORTED_TABLE: {error}; restoring the previous output failed: {rollback}; retained {}",
                        retained.display()
                    )))
                }
            };
        }
        if let Some(temporary) = temporary {
            temporary.release().map_err(runtime_error)?;
        }
        Ok(())
    }
}

fn reusable_cache(main: &Table, output: &Path, keys: &[String], rows: &[usize]) -> PyResult<bool> {
    let Some(Value::Array(ArrayValue::String(columns))) = main.keywords().get("SORT_COLUMNS")
    else {
        return Ok(false);
    };
    let Some(Value::TableRef(link)) = main.keywords().get("SORTED_TABLE") else {
        return Ok(false);
    };
    let source = main
        .path()
        .expect("sorted cache belongs to a persisted table");
    if !columns.iter().eq(keys) || casa_tables::add_directory(link, source) != output {
        return Ok(false);
    }
    match fs::symlink_metadata(output) {
        Ok(info) if info.is_dir() => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(runtime_error(error)),
    }
    let cache = Table::open(TableOptions::new(output)).map_err(runtime_error)?;
    let ordered = cache.root_row_numbers().map_err(runtime_error)?;
    let mut remaining = rows.iter().map(|&row| row as u64).peekable();
    for row in &ordered {
        if remaining.peek() == Some(row) {
            remaining.next();
        }
    }
    Ok(remaining.next().is_none())
}

fn restore_output(
    staged: &Path,
    output: &Path,
    replaced_table: bool,
    existed: bool,
) -> std::io::Result<()> {
    if replaced_table {
        exchange_directories(staged, output)
    } else {
        rename_without_replacing(output, staged)?;
        if existed {
            fs::create_dir(output)?;
        }
        Ok(())
    }
}
