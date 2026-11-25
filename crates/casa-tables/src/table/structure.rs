// SPDX-License-Identifier: LGPL-3.0-or-later
//! Structural changes made in place on disk: the change is written to
//! `table.dat` (and, where a storage manager goes away, its files are
//! deleted); every other storage manager file is left as it is, so a
//! MeasurementSet keeps its tiled, incremental and standard managers.
//!
//! Each operation edits what `table.dat` holds, then reloads the table from
//! the disk ([`Table::resync_or_discard`]), so the handle is what the disk
//! says without the change having loaded a row, and holds nothing a release
//! of the write lock would write in full. An operation refused before it
//! wrote leaves the handle as clean as it found it. The in-memory edits of
//! [`Table::rename_column`] and [`Table::remove_column`] load every row and
//! are for a table written in full afterwards.
//!
//! # C++ equivalent
//!
//! `Table::renameColumn` and `Table::removeColumn`, which change the
//! table description and the column set and leave a storage manager that
//! keeps other columns where it is.

use std::cell::RefCell;

use super::*;
use crate::storage::StorageError;
use crate::storage::standard_stman::{dm_name_from_blob, rewrite_ssm_columns};
use crate::storage::table_control::{ColumnDescContents, TableDatContents, TableDatResult};

/// Prefix of the files of the storage manager with sequence number `seq_nr`:
/// `table.f<seq_nr>`, the `_TSM<n>` files of a tiled manager, and the `i`
/// file of a standard manager's indirect arrays. A manager whose number
/// merely starts with the same digits is another manager.
fn belongs_to_manager(file_name: &str, seq_nr: u32) -> bool {
    let Some(rest) = file_name.strip_prefix(&format!("table.f{seq_nr}")) else {
        return false;
    };
    rest.is_empty() || rest == "i" || rest.starts_with('_')
}

/// Deletes the files of the storage manager with sequence number `seq_nr`.
fn remove_manager_files(table_dir: &Path, seq_nr: u32) -> std::io::Result<()> {
    for entry in std::fs::read_dir(table_dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| belongs_to_manager(name, seq_nr))
        {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Whether a hypercolumn definition of the table description lists `name`
/// as a coordinate or an id column (the private keyword `Hypercolumn_<name>`
/// lists its `data`, `coord` and `id` columns).
fn is_id_or_coordinate_column(private_keywords: &RecordValue, name: &str) -> bool {
    private_keywords
        .fields()
        .iter()
        .filter(|field| field.name.starts_with("Hypercolumn_"))
        .filter_map(|field| match &field.value {
            Value::Record(definition) => Some(definition),
            _ => None,
        })
        .flat_map(|definition| ["coord", "id"].map(|list| definition.get(list)))
        .any(|list| match list {
            Some(Value::Array(ArrayValue::String(names))) => names.iter().any(|n| n == name),
            _ => false,
        })
}

/// Renames `old` to `new` in the hypercolumn definitions of the table
/// description, as `TableDesc::renameColumn` does through
/// `adjustHypercolumns` (casacore's own handling of coordinate and id
/// columns there is not followed: they are refused, not renamed).
fn rename_in_hypercolumns(private_keywords: &mut RecordValue, old: &str, new: &str) {
    edit_hypercolumns(private_keywords, |names| {
        for name in names {
            if name == old {
                *name = new.to_string();
            }
        }
    });
}

/// Applies `edit` to each list of column names of each hypercolumn
/// definition in `private_keywords`.
fn edit_hypercolumns(private_keywords: &mut RecordValue, edit: impl Fn(&mut Vec<String>)) {
    for field in private_keywords.fields_mut() {
        if !field.name.starts_with("Hypercolumn_") {
            continue;
        }
        let Value::Record(definition) = &mut field.value else {
            continue;
        };
        for list in definition.fields_mut() {
            let Value::Array(ArrayValue::String(array)) = &mut list.value else {
                continue;
            };
            let mut names: Vec<String> = array.iter().cloned().collect();
            edit(&mut names);
            if names.len() != array.len() || names.iter().zip(array.iter()).any(|(a, b)| a != b) {
                *array = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[names.len()]), names)
                    .expect("a vector's shape is its length");
            }
        }
    }
}

impl Table {
    /// Atomically publish table keywords while preserving column keywords,
    /// the table information file, and all row storage.
    ///
    /// Hold the table's write lock from the metadata read until publication
    /// when updating an existing table.
    ///
    /// # Errors
    /// Returns an error if the destination is not a plain table with a
    /// writable regular control file, or the new keywords cannot be saved.
    pub fn save_keywords_atomically(&self, path: &Path) -> Result<(), TableError> {
        self.save_keywords_with_sync(path, std::fs::File::sync_all)
    }

    /// Publish keywords for a cache that can be rebuilt from persisted rows.
    ///
    /// The same write-lock and atomic-replacement contract as
    /// [`Self::save_keywords_atomically`] applies. The staged descriptor
    /// is synchronized with ordinary filesystem `fsync`.
    ///
    /// # Errors
    /// Returns an error when the destination cannot be atomically updated.
    pub fn save_cache_keywords_atomically(&self, path: &Path) -> Result<(), TableError> {
        self.save_keywords_with_sync(
            path,
            crate::storage::table_keyword_write::sync_cache_keywords,
        )
    }

    fn save_keywords_with_sync(
        &self,
        path: &Path,
        sync: fn(&std::fs::File) -> std::io::Result<()>,
    ) -> Result<(), TableError> {
        crate::storage::table_keyword_write::save_keywords(
            path,
            persisted_rows::published_row_count(path),
            self.inner.keywords(),
            sync,
        )?;
        self.note_persisted_write(path);
        if self
            .source_path
            .as_deref()
            .is_some_and(|source| super::locking::same_directory(source, path))
        {
            self.inner.note_metadata_saved();
        }
        Ok(())
    }

    /// Renames column `old` to `new` in the table on disk, without loading
    /// its rows or touching a storage manager's file.
    ///
    /// Only `table.dat` changes: the column's description, and its name in
    /// the hypercolumn definitions. The name the column's storage manager
    /// knows it by (casacore's `originalName`) stays as it was. Fails if
    /// `old` is not a column or `new` already is one. The table is reloaded
    /// from the disk afterwards, so changes this handle has not saved are
    /// discarded.
    ///
    /// # C++ equivalent
    ///
    /// `Table::renameColumn`.
    pub fn rename_column_in_place(&mut self, old: &str, new: &str) -> Result<(), TableError> {
        self.edit_structure_in_place("rename_column_in_place", false, |_, dat, _| {
            let index = column_index(dat, old)?;
            if column_index(dat, new).is_ok() {
                return Err(SchemaError::DuplicateColumn(new.to_string()).into());
            }
            // The name of an id or coordinate column is also kept in the
            // hypercube values of its tiled manager's file, which an edit of
            // `table.dat` alone leaves behind: the renamed column would be
            // unreadable. (casacore's own `adjustHypercolumns` does not carry
            // these names over either.)
            if is_id_or_coordinate_column(&dat.table_desc.private_keywords, old) {
                return Err(TableError::Storage(format!(
                    "column \"{old}\" is an id or coordinate column of a hypercolumn: its name \
                     is also held in the hypercube values of its storage manager's file, which \
                     cannot be renamed in place"
                )));
            }
            dat.table_desc.columns[index].col_name = new.to_string();
            dat.column_set.columns[index].original_name = new.to_string();
            let on_file = dat
                .names_on_file
                .remove(old)
                .unwrap_or_else(|| old.to_string());
            if on_file != new {
                dat.names_on_file.insert(new.to_string(), on_file);
            }
            rename_in_hypercolumns(&mut dat.table_desc.private_keywords, old, new);
            Ok(Vec::new())
        })
    }

    /// Removes the columns `names` from the table on disk, without loading
    /// its rows. Every storage manager that held any of them must hold only
    /// them: its files are deleted, and the other managers' files are not
    /// touched. A column whose manager keeps other columns cannot be taken
    /// out in place (the manager's file holds its data), and is refused
    /// before anything changes. The table is reloaded from the disk
    /// afterwards, so changes this handle has not saved are discarded.
    ///
    /// # C++ equivalent
    ///
    /// `Table::removeColumn`.
    pub fn remove_columns_in_place(&mut self, names: &[&str]) -> Result<(), TableError> {
        self.remove_persisted_columns(names, false)
    }

    /// Remove persisted columns, allowing shared StandardStMan managers.
    /// A shared StandardStMan is rebuilt from its surviving columns while
    /// other managers keep their existing files and bindings. A compressed
    /// virtual column and its backing columns may be removed together.
    /// Shared groups whose storage managers cannot remove individual
    /// columns are refused before the table is changed.
    pub fn remove_columns_preserving_managers(&mut self, names: &[&str]) -> Result<(), TableError> {
        self.remove_persisted_columns(names, true)
    }

    fn remove_persisted_columns(
        &mut self,
        names: &[&str],
        allow_shared_standard: bool,
    ) -> Result<(), TableError> {
        self.edit_structure_in_place(
            "remove_columns_in_place",
            allow_shared_standard,
            |table, dat, path| {
                let mut indices = names
                    .iter()
                    .map(|name| column_index(dat, name))
                    .collect::<Result<Vec<_>, _>>()?;
                indices.sort_unstable();
                indices.dedup();
                let (mut gone, shared) = removal_groups(dat, &indices, allow_shared_standard)?;
                if allow_shared_standard {
                    check_virtual_dependencies(table, dat, names)?;
                    gone.extend(rewrite_standard_groups(table, dat, path, &shared)?);
                }
                detach_columns(dat, indices, names, &gone);
                Ok(gone)
            },
        )
    }

    /// Applies `edit` to the table's `table.dat` and writes it, then deletes
    /// the files of the storage managers `edit` returns the numbers of, and
    /// reloads the table from the disk. `edit` refuses before anything is
    /// written.
    fn edit_structure_in_place(
        &mut self,
        operation: &str,
        allow_persisted_virtual: bool,
        edit: impl FnOnce(&Self, &mut TableDatContents, &Path) -> Result<Vec<u32>, TableError>,
    ) -> Result<(), TableError> {
        if self.kind != TableKind::Plain {
            return Err(TableError::Storage(format!(
                "{operation} requires a plain disk-backed table"
            )));
        }
        if !self.virtual_bindings.is_empty()
            || (!allow_persisted_virtual && !self.virtual_columns.is_empty())
        {
            return Err(TableError::Storage(format!(
                "{operation} does not support virtual columns"
            )));
        }
        let path = self
            .source_path
            .clone()
            .ok_or_else(|| TableError::Storage("table has no source path".to_string()))?;
        // Every admitted write operation counts as a change of the handle,
        // and a change the disk lacks is what releasing the write lock writes
        // in full (as `StManAipsIO`, whatever the table's layout). This
        // operation writes to the disk itself, so it must leave the handle
        // as clean as it found it.
        let dirty_before = self.holds_unflushed_changes();
        let auto_unlock = self.begin_write_operation(operation)?;
        let mut wrote = false;
        let result = (|| {
            let control_path = path.join(crate::storage::TABLE_CONTROL_FILE);
            let TableDatResult::Plain(mut table_dat) =
                crate::storage::table_control::read_table_dat_dispatch(&control_path)?
            else {
                return Err(TableError::Storage(format!(
                    "{operation} only supports plain tables"
                )));
            };
            let persisted_rows = self.require_persisted_rows(&path, &table_dat)?;
            table_dat.raise_row_count(persisted_rows);
            let removed_managers = edit(self, &mut table_dat, &path)?;
            self.note_persisted_write(&path);
            wrote = true;
            crate::storage::table_control::write_table_dat(&control_path, &table_dat)?;
            for seq_nr in removed_managers {
                remove_manager_files(&path, seq_nr).map_err(StorageError::from)?;
            }
            crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(&path);
            Ok(())
        })();
        if !wrote && !dirty_before {
            // Refused before anything reached the disk.
            self.mark_flushed();
        }
        let result = if wrote {
            // The disk changed, wholly or in part: the handle takes what it
            // holds from the disk, and is clean whether or not that works.
            let reloaded = self.resync_or_discard();
            result.and(reloaded)
        } else {
            result
        };
        self.finish_write_operation(auto_unlock, result)
    }
}

type SharedStandardGroup = (u32, Vec<usize>);

fn removal_groups(
    dat: &TableDatContents,
    removed: &[usize],
    allow_shared_standard: bool,
) -> Result<(Vec<u32>, Vec<SharedStandardGroup>), TableError> {
    let mut drop_managers = Vec::new();
    let mut shared = Vec::new();
    for manager in &dat.column_set.data_managers {
        let columns = dat
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| (column.dm_seq_nr == manager.seq_nr).then_some(index));
        let (retain, discard): (Vec<_>, Vec<_>) =
            columns.partition(|index| removed.binary_search(index).is_err());
        if discard.is_empty() {
            continue;
        }
        if retain.is_empty() {
            drop_managers.push(manager.seq_nr);
        } else if allow_shared_standard && manager.type_name == "StandardStMan" {
            shared.push((manager.seq_nr, retain));
        } else {
            let name = &dat.table_desc.columns[discard[0]].col_name;
            return Err(TableError::Storage(format!(
                "column {name:?} shares storage manager {} ({}) with {} other columns",
                manager.seq_nr,
                manager.type_name,
                retain.len()
            )));
        }
    }
    Ok((drop_managers, shared))
}

fn check_virtual_dependencies(
    table: &Table,
    dat: &TableDatContents,
    removed: &[&str],
) -> Result<(), TableError> {
    for column in &dat.table_desc.columns {
        if !table.virtual_columns.contains(&column.col_name)
            || removed.contains(&column.col_name.as_str())
        {
            continue;
        }
        let depends_on_removed = column
            .keywords
            .fields()
            .iter()
            .any(|field| match &field.value {
                Value::Scalar(ScalarValue::String(value)) if field.name.starts_with('_') => {
                    removed.iter().any(|name| value.contains(name))
                }
                _ => false,
            });
        if depends_on_removed {
            return Err(TableError::Storage(format!(
                "removing columns required by virtual column {:?}",
                column.col_name
            )));
        }
    }
    Ok(())
}

fn detach_columns(dat: &mut TableDatContents, indices: Vec<usize>, names: &[&str], gone: &[u32]) {
    for index in indices.into_iter().rev() {
        dat.table_desc.columns.remove(index);
        dat.column_set.columns.remove(index);
    }
    for name in names {
        dat.names_on_file.remove(*name);
    }
    dat.column_set
        .data_managers
        .retain(|manager| !gone.contains(&manager.seq_nr));
    edit_hypercolumns(&mut dat.table_desc.private_keywords, |list| {
        list.retain(|name| !names.contains(&name.as_str()));
    });
    let keywords = &mut dat.table_desc.private_keywords;
    let emptied: Vec<_> = keywords
        .fields()
        .iter()
        .filter(|field| field.name.starts_with("Hypercolumn_"))
        .filter(|field| match &field.value {
            Value::Record(definition) => matches!(definition.get("data"),
                Some(Value::Array(array)) if array.shape().iter().product::<usize>() == 0),
            _ => false,
        })
        .map(|field| field.name.clone())
        .collect();
    for name in emptied {
        keywords.remove(&name);
    }
}

fn rewrite_standard_groups(
    table: &Table,
    dat: &mut TableDatContents,
    path: &Path,
    groups: &[SharedStandardGroup],
) -> Result<Vec<u32>, TableError> {
    let mut next_seq = dat
        .column_set
        .data_managers
        .iter()
        .map(|entry| entry.seq_nr)
        .max()
        .map_or(0, |seq| seq + 1);
    let mut new_managers = Vec::new();
    for (seq_nr, indices) in groups {
        let columns: Vec<_> = indices
            .iter()
            .map(|&index| dat.table_desc.columns[index].clone())
            .collect();
        let manager = dat
            .column_set
            .data_managers
            .iter()
            .find(|manager| manager.seq_nr == *seq_nr)
            .expect("shared StandardStMan is present");
        let name = dm_name_from_blob(&manager.data)
            .unwrap_or_else(|| columns[0].data_manager_group.clone());
        let staged = path.join(format!("table.f{next_seq}"));
        let source = path.join(format!("table.f{seq_nr}"));
        let cache = RefCell::new((usize::MAX, 0, Vec::new()));
        let result = rewrite_ssm_columns(
            &source,
            &staged,
            &columns,
            table.row_count(),
            &name,
            |column, row| {
                let mut slot = cache.borrow_mut();
                if slot.0 != column || row < slot.1 || row - slot.1 >= slot.2.len() {
                    let values = structural_cells(table, &columns[column], row)
                        .map_err(|error| StorageError::FormatMismatch(error.to_string()))?;
                    *slot = (column, row, values);
                }
                let offset = row - slot.1;
                Ok(slot.2[offset].take())
            },
        );
        let data = match result {
            Ok(data) => data,
            Err(error) => {
                for (entry, _) in &new_managers {
                    let _ = remove_manager_files(path, *entry);
                }
                let _ = remove_manager_files(path, next_seq);
                return Err(error.into());
            }
        };
        new_managers.push((
            next_seq,
            crate::storage::table_control::DataManagerEntry {
                seq_nr: next_seq,
                type_name: manager.type_name.clone(),
                data,
            },
        ));
        for &index in indices {
            dat.column_set.columns[index].dm_seq_nr = next_seq;
        }
        next_seq += 1;
    }
    dat.column_set
        .data_managers
        .extend(new_managers.into_iter().map(|(_, manager)| manager));
    dat.column_set.seq_count = next_seq.max(dat.column_set.seq_count);
    Ok(groups.iter().map(|(seq, _)| *seq).collect())
}

fn structural_cells(
    table: &Table,
    column: &ColumnDescContents,
    row: usize,
) -> Result<Vec<Option<Value>>, TableError> {
    let reader = table.column_accessor(&column.col_name)?;
    // Keep at most one array cell, or a small block of scalar cells.
    if column.is_record() {
        return Ok(vec![Some(Value::Record(reader.record_cell(row)?))]);
    }
    if column.is_array {
        return Ok(reader
            .array_cells_owned_uncached(&[row])?
            .into_iter()
            .map(|value| value.map(Value::Array))
            .collect());
    }
    let rows: Vec<_> = (row..table.row_count().min(row.saturating_add(512))).collect();
    Ok(reader
        .scalar_cells_owned_for_rows(&rows)?
        .into_iter()
        .map(|value| value.map(Value::Scalar))
        .collect())
}

/// The position of column `name` in the table description (the column set
/// lists its columns in the same order).
fn column_index(dat: &TableDatContents, name: &str) -> Result<usize, TableError> {
    dat.table_desc
        .columns
        .iter()
        .position(|column| column.col_name == name)
        .ok_or_else(|| TableError::SchemaColumnUnknown {
            column: name.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::belongs_to_manager;

    #[test]
    fn a_manager_owns_its_own_files_and_not_those_of_a_longer_number() {
        for file in ["table.f1", "table.f1i", "table.f1_TSM0", "table.f1_TSM12"] {
            assert!(belongs_to_manager(file, 1), "{file}");
        }
        for file in [
            "table.f10",
            "table.f11_TSM1",
            "table.dat",
            "table.lock",
            "table.info",
        ] {
            assert!(!belongs_to_manager(file, 1), "{file}");
        }
    }
}
