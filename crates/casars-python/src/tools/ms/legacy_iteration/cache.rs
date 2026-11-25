// SPDX-License-Identifier: LGPL-3.0-or-later
//! Persist the casacore SORTED_TABLE reference created by a writable iterator.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use casa_ms::tool_iteration::{LegacyIterationOptions, LegacyToolIteration};
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

pub(super) struct LegacySortSource {
    root: PathBuf,
    generation: Option<u32>,
}

impl AttachedMs {
    pub(super) fn legacy_sort_source(
        &self,
        captured: &VisibilityMain,
    ) -> PyResult<Option<LegacySortSource>> {
        if !self.writable || self.is_multi_ms {
            return Ok(None);
        }
        let root = if captured.plain {
            self.path.clone()
        } else {
            let Some(physical) = captured
                .table
                .reference_write_projection(&[], &[])
                .map_err(runtime_error)?
            else {
                return Ok(None);
            };
            let root = physical.path.canonicalize().map_err(runtime_error)?;
            if !matches!(
                Table::immediate_part_names(&root, &root).map_err(runtime_error)?,
                TablePartNames::Plain(_)
            ) {
                return Ok(None);
            }
            root
        };
        if captured.uses_layout_in(&root.join("SORTED_TABLE")) {
            return Ok(None);
        }
        let generation = TableWriteLock::read_modify_counter(&root).map_err(runtime_error)?;
        Ok(Some(LegacySortSource { root, generation }))
    }

    pub(super) fn cache_legacy_iteration(
        &mut self,
        link: &MetadataLink,
        captured: &VisibilityMain,
        source: LegacySortSource,
        options: LegacyIterationOptions<'_>,
        cursor: &mut LegacyToolIteration,
    ) -> PyResult<Option<PathBuf>> {
        let LegacySortSource { root, generation } = source;
        let output = root.join("SORTED_TABLE");
        if captured.plain
            && reusable_cache(&captured.table, &captured.table, &output, options, cursor)?
        {
            captured.verify_write_layout(&BTreeSet::new())?;
            if TableWriteLock::read_modify_counter(&root).map_err(runtime_error)? == generation {
                return Ok(None);
            }
        }
        let mut temporary = None;
        if !self.write_locks.contains_key(&root) {
            temporary = Some(TableWriteLock::acquire_to_reread(&root, 0).map_err(runtime_error)?);
        }
        let lock = self
            .write_locks
            .get(&root)
            .or(temporary.as_ref())
            .filter(|lock| lock.matches_table_path(&root))
            .ok_or_else(|| runtime_error("legacy iterator source or write lock was replaced"))?;
        link.verify_directory().map_err(runtime_error)?;
        captured.verify_write_layout(&BTreeSet::new())?;
        let mut main = Table::open(TableOptions::new(&root)).map_err(runtime_error)?;
        if lock.modify_counter().map_err(runtime_error)? != generation {
            let refreshed = VisibilityMain::open(&self.path)?;
            *cursor = refreshed.legacy_iteration(options)?.ok_or_else(|| {
                runtime_error("legacy iteration source changed during initialization")
            })?;
        }
        if reusable_cache(&main, &captured.table, &output, options, cursor)? {
            return Ok(None);
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
        let projected = compose_rows(options.working_rows, Some(cursor.ordered_rows()))
            .expect("iterator rows were supplied");
        let parent = if captured.plain {
            &main
        } else {
            &captured.table
        };
        parent
            .select_rows(&projected)
            .and_then(|reference| {
                reference.save_for_destination(TableOptions::new(staged), &output)
            })
            .map_err(runtime_error)?;
        captured.verify_write_layout(&BTreeSet::new())?;
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
        if let Err(error) = main.save_keywords_atomically(&root) {
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
        Ok(Some(root))
    }
}

fn reusable_cache(
    root: &Table,
    source: &Table,
    output: &Path,
    options: LegacyIterationOptions<'_>,
    cursor: &LegacyToolIteration,
) -> PyResult<bool> {
    let Some(Value::Array(ArrayValue::String(columns))) = root.keywords().get("SORT_COLUMNS")
    else {
        return Ok(false);
    };
    let Some(Value::TableRef(link)) = root.keywords().get("SORTED_TABLE") else {
        return Ok(false);
    };
    let root_path = root
        .path()
        .expect("sorted cache belongs to a persisted table");
    if !columns.iter().eq(cursor.keys()) || casa_tables::add_directory(link, root_path) != output {
        return Ok(false);
    }
    match fs::symlink_metadata(output) {
        Ok(info) if info.is_dir() => {}
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(runtime_error(error)),
    }
    let saved = Table::open(TableOptions::new(output)).map_err(runtime_error)?;
    let names = |table: &Table| {
        table
            .schema()
            .into_iter()
            .flat_map(|schema| schema.columns())
            .map(|column| column.name().to_owned())
            .collect::<Vec<_>>()
    };
    let visible = names(source);
    if visible != names(&saved) {
        return Ok(false);
    }
    let selected = compose_rows(options.working_rows, options.selected);
    let ordered = compose_rows(options.working_rows, Some(cursor.ordered_rows()))
        .expect("iterator rows were supplied");
    let input = match selected.as_deref() {
        Some(rows) => std::borrow::Cow::Borrowed(rows),
        None => std::borrow::Cow::Owned((0..source.row_count()).collect()),
    };
    let names = visible.iter().map(String::as_str).collect::<Vec<_>>();
    let Some(saved) = saved
        .reference_write_projection(&(0..saved.row_count()).collect::<Vec<_>>(), &names)
        .map_err(runtime_error)?
    else {
        return Ok(false);
    };
    if saved.path.canonicalize().map_err(runtime_error)? != root_path {
        return Ok(false);
    }
    if source.path() == Some(root_path) {
        return Ok(saved.columns == visible
            && can_reuse_rows(&input, &ordered, &saved.rows, root.row_count()));
    }
    let Some(sorted) = source
        .reference_write_projection(&ordered, &names)
        .map_err(runtime_error)?
    else {
        return Ok(false);
    };
    let Some(input) = source
        .reference_write_projection(&input, &[])
        .map_err(runtime_error)?
    else {
        return Ok(false);
    };
    Ok(sorted.columns == saved.columns
        && can_reuse_rows(&input.rows, &sorted.rows, &saved.rows, root.row_count()))
}

fn can_reuse_rows(input: &[usize], sorted: &[usize], cached: &[usize], total_rows: usize) -> bool {
    if input.len() == total_rows && sorted == cached {
        return true;
    }
    is_subsequence(input, cached) && is_subsequence(sorted, cached)
}

fn is_subsequence(rows: &[usize], cached: &[usize]) -> bool {
    let mut remaining = rows.iter().peekable();
    for row in cached {
        if remaining.peek() == Some(&row) {
            remaining.next();
        }
    }
    remaining.next().is_none()
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
