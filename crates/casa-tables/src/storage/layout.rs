// SPDX-License-Identifier: LGPL-3.0-or-later
//! The data-manager layout a table has on disk, as the bindings a save keeps
//! it with.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::table_control::{TableDatResult, read_table_dat_dispatch};
use super::{StorageError, TABLE_CONTROL_FILE, TABLE_DATA_FILE_PREFIX, tiled_stman};
use crate::table::{ColumnBinding, DataManagerKind};

/// The binding of each stored column of the table at `table_path`: its data
/// manager and, for a tiled one, the default tile shape its header records.
///
/// A save with these bindings keeps every column in the kind of manager it
/// is in (ADR-0008) instead of rewriting the table with one manager (#697).
/// Columns of managers casa-rs does not write, such as virtual engines, are
/// left out and take the save's default. A table with no `table.dat` yet, or
/// one that is not a plain table, has no bindings.
///
/// # Errors
///
/// When `table.dat` or a tiled manager's header cannot be read.
pub(crate) fn persisted_column_bindings(
    table_path: &Path,
) -> Result<HashMap<String, ColumnBinding>, StorageError> {
    let control_path = table_path.join(TABLE_CONTROL_FILE);
    if !control_path.exists() {
        return Ok(HashMap::new());
    }
    let TableDatResult::Plain(table_dat) = read_table_dat_dispatch(&control_path)? else {
        return Ok(HashMap::new());
    };
    let mut bindings = HashMap::with_capacity(table_dat.column_set.columns.len());
    for column in &table_dat.column_set.columns {
        let Some(manager) = table_dat
            .column_set
            .data_managers
            .iter()
            .find(|manager| manager.seq_nr == column.dm_seq_nr)
        else {
            continue;
        };
        let Some(data_manager) = data_manager_kind(&manager.type_name) else {
            continue;
        };
        let tile_shape = if is_tiled(data_manager) {
            let shape = tiled_stman::tiled_default_tile_shape(table_path, manager.seq_nr)?;
            (!shape.is_empty() && !shape.contains(&0)).then_some(shape)
        } else {
            None
        };
        bindings.insert(
            column.original_name.clone(),
            ColumnBinding {
                data_manager,
                tile_shape,
            },
        );
    }
    Ok(bindings)
}

/// Remove the data files (`table.f*`) of the table at `table_path`, as a
/// save that rewrites the whole table in one manager does first, so that no
/// file of the old layout is left beside the new one.
///
/// # Errors
///
/// When a data file cannot be removed.
pub(crate) fn remove_data_files(table_path: &Path) -> Result<(), StorageError> {
    let Ok(entries) = fs::read_dir(table_path) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(TABLE_DATA_FILE_PREFIX)
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// The writable manager casacore names `type_name`.
fn data_manager_kind(type_name: &str) -> Option<DataManagerKind> {
    Some(match type_name {
        "StManAipsIO" => DataManagerKind::StManAipsIO,
        "StandardStMan" => DataManagerKind::StandardStMan,
        "IncrementalStMan" => DataManagerKind::IncrementalStMan,
        "TiledColumnStMan" => DataManagerKind::TiledColumnStMan,
        "TiledShapeStMan" => DataManagerKind::TiledShapeStMan,
        "TiledCellStMan" => DataManagerKind::TiledCellStMan,
        "TiledDataStMan" => DataManagerKind::TiledDataStMan,
        _ => return None,
    })
}

fn is_tiled(kind: DataManagerKind) -> bool {
    matches!(
        kind,
        DataManagerKind::TiledColumnStMan
            | DataManagerKind::TiledShapeStMan
            | DataManagerKind::TiledCellStMan
            | DataManagerKind::TiledDataStMan
    )
}
