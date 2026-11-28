// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::Path;

use super::row_changes::{ReversibleEdit, StagedEdit};
use super::table_control::{
    ColumnSetStorage, DataManagerEntry, StagedTableDat, read_table_dat, read_table_dat_dispatch,
};
use super::{StorageError, TABLE_CONTROL_FILE, TableDatContents, TableDatResult};
use crate::TableError;
use crate::publication::table_manager_file_sequence;

/// Serialize the complete save in private storage. The caller holds the
/// destination's write lock through staging, publication and any rollback.
pub(crate) fn prepare(
    table: &Path,
    has_open_readers: impl FnOnce() -> bool,
    write: impl FnOnce(StagedTableDat<'_>) -> Result<(), TableError>,
) -> Result<(ReversibleEdit, bool), TableError> {
    let control = StagedTableDat::in_table_directory(table)?;
    let path = table.join(TABLE_CONTROL_FILE);
    let previous = match path.symlink_metadata() {
        Ok(_) => match read_table_dat_dispatch(&path)? {
            TableDatResult::Plain(dat) => Some(dat),
            _ => {
                return Err(StorageError::FormatMismatch(
                    "cannot overwrite a reference or concatenated table; materialize it at a new path"
                        .into(),
                ).into());
            }
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(StorageError::from(error).into()),
    };
    let original_files = if let Some(dat) = &previous {
        dat.require_separate_storage_for_write()?;
        if dat
            .column_set
            .storage
            .is_some_and(|storage| storage.kind != ColumnSetStorage::SEPARATE)
        {
            return Err(StorageError::FormatMismatch(
                "full save requires separate manager files; materialize the table at a new path"
                    .into(),
            )
            .into());
        }
        if dat
            .column_set
            .data_managers
            .iter()
            .any(|manager| manager.type_name == super::asdm_stman::TYPE)
        {
            return Err(StorageError::FormatMismatch(
                "AsdmStMan is read-only. Materialize the table at a new path.".into(),
            )
            .into());
        }
        manager_files(table, dat)?
    } else {
        BTreeSet::new()
    };

    let edit = StagedEdit::new(table)?;
    let staged = edit.stage_path();
    write(StagedTableDat::for_save_stage(
        staged,
        table,
        previous.as_ref(),
    )?)?;
    if let Some(dat) = &previous
        && has_open_readers()
        && !compatible_with_open_managers(table, staged, dat, &original_files)?
    {
        return Err(StorageError::FormatMismatch(
            "full save would change the layout of an open table; close its other handles and retry"
                .into(),
        )
        .into());
    }
    Ok((
        edit.prepare_full_publication(control, original_files)?,
        previous.is_some(),
    ))
}

/// Entries owned by the current descriptor. A file merely starting with
/// `table.f` does not belong to the table's storage managers.
fn manager_files(table: &Path, dat: &TableDatContents) -> Result<BTreeSet<OsString>, StorageError> {
    let mut files = BTreeSet::new();
    for dir_entry in fs::read_dir(table)? {
        let name = dir_entry?.file_name();
        if let Some(seq) = table_manager_file_sequence(&name)
            && dat
                .column_set
                .data_managers
                .iter()
                .any(|manager| seq == manager.seq_nr && owns_file(manager, &name))
        {
            files.insert(name);
        }
    }
    Ok(files)
}

/// Preflight all of the selected managers before flushing any of them.
pub(crate) fn check_manager_write_access(
    table: &Path,
    affected: &std::collections::HashSet<u32>,
) -> Result<(), StorageError> {
    let dat = read_table_dat(&table.join(TABLE_CONTROL_FILE))?;
    if dat.uses_multifile() {
        check_private_write_access(&table.join("table.mf"))?;
    }
    for name in manager_files(table, &dat)? {
        if table_manager_file_sequence(&name).is_some_and(|seq| affected.contains(&seq)) {
            check_private_write_access(&table.join(name))?;
        }
    }
    Ok(())
}

pub(crate) fn check_private_write_access(path: &Path) -> Result<(), StorageError> {
    crate::publication::private_table_file_metadata(path)?;
    crate::publication::check_write_access(path)?;
    Ok(())
}

fn owns_file(manager: &DataManagerEntry, name: &OsStr) -> bool {
    let base = format!("table.f{}", manager.seq_nr);
    let Some(suffix) = name.to_str().and_then(|name| name.strip_prefix(&base)) else {
        return false;
    };
    match manager.type_name.as_str() {
        "StandardStMan" | "IncrementalStMan" | "StManAipsIO" => matches!(suffix, "" | "i"),
        "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
            suffix.is_empty() || suffix.starts_with("_TSM")
        }
        _ => false,
    }
}

/// casacore resyncs the old manager objects before it reloads keywords.
/// Their manager order, column mappings and cached file geometry must
/// still be valid after the writer has released the lock.
fn compatible_with_open_managers(
    table: &Path,
    stage: &Path,
    before: &TableDatContents,
    files: &BTreeSet<OsString>,
) -> Result<bool, StorageError> {
    let after = read_table_dat(&stage.join(TABLE_CONTROL_FILE))?;
    if !same_layout(before, &after) || *files != manager_files(stage, &after)? {
        return Ok(false);
    }
    for manager in &before.column_set.data_managers {
        let sequence = manager.seq_nr;
        let same_geometry = match manager.type_name.as_str() {
            "StManAipsIO" => true,
            "StandardStMan" => {
                let spec = super::standard_stman::ssm_spec;
                spec(table, sequence)?.get("BUCKETSIZE") == spec(stage, sequence)?.get("BUCKETSIZE")
            }
            "IncrementalStMan" => {
                let spec = super::incremental_stman::ism_spec;
                spec(table, sequence)?.get("BUCKETSIZE") == spec(stage, sequence)?.get("BUCKETSIZE")
            }
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" => {
                let header = format!("table.f{sequence}");
                fs::read(table.join(&header))? == fs::read(stage.join(header))?
            }
            other => {
                // Engines such as ForwardColumn bind to the column's
                // keywords in prepare(). A keyword resync keeps that binding.
                super::is_virtual_engine(other)
                    && before.table_desc.table_keywords == after.table_desc.table_keywords
                    && before
                        .table_desc
                        .columns
                        .iter()
                        .zip(&after.table_desc.columns)
                        .zip(&before.column_set.columns)
                        .all(|((old, new), binding)| {
                            binding.dm_seq_nr != sequence || old.keywords == new.keywords
                        })
            }
        };
        if !same_geometry {
            return Ok(false);
        }
    }
    Ok(true)
}

fn same_layout(a: &TableDatContents, b: &TableDatContents) -> bool {
    a.big_endian == b.big_endian
        && a.names_on_file == b.names_on_file
        && a.table_desc.private_keywords == b.table_desc.private_keywords
        && a.table_desc.columns.len() == b.table_desc.columns.len()
        && a.column_set.columns.len() == b.column_set.columns.len()
        && a.column_set.data_managers.len() == b.column_set.data_managers.len()
        && a.column_set
            .data_managers
            .iter()
            .zip(&b.column_set.data_managers)
            .all(|(a, b)| a.type_name == b.type_name && a.seq_nr == b.seq_nr && a.data == b.data)
        && a.column_set
            .columns
            .iter()
            .zip(&b.column_set.columns)
            .all(|(a, b)| {
                a.original_name == b.original_name
                    && a.dm_seq_nr == b.dm_seq_nr
                    && a.is_array == b.is_array
            })
        && a.table_desc
            .columns
            .iter()
            .zip(&b.table_desc.columns)
            .all(|(a, b)| {
                a.col_name == b.col_name
                    && a.class_name == b.class_name
                    && a.data_type == b.data_type
                    && a.option == b.option
                    && a.nrdim == b.nrdim
                    && a.shape == b.shape
                    && a.max_length == b.max_length
                    && a.data_manager_type == b.data_manager_type
                    && a.data_manager_group == b.data_manager_group
            })
}
