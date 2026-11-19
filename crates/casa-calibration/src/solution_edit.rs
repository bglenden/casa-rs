// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage and publish calibration table edits while preserving the source schema.

use std::collections::BTreeSet;
use std::path::Path;

use casa_tables::{RequiredScalarColumnValues, Table, TableOptions, TableWriteLock};
use casa_types::{ScalarValue, Value};

use crate::CalibrationOutputMode;
use crate::constants::{COL_SPECTRAL_WINDOW_ID, STANDARD_SUBTABLE_KEYWORDS, TABLE_INFO_TYPE};
use crate::output::CalibrationOutput;
use crate::paths::{copy_dir_recursive, normalized, overlap};

pub(crate) fn edit_calibration_table(
    input: &Path,
    destination: &Path,
    edit: impl FnOnce(&mut Table, &Path) -> Result<(), String>,
) -> Result<(), String> {
    let input = normalized(input).map_err(error_text)?;
    let destination = normalized(destination).map_err(error_text)?;
    if input != destination && overlap(&input, &destination).map_err(error_text)? {
        return Err("the input and output calibration tables overlap".into());
    }
    let source = open_for_snapshot(&input, input != destination)?;
    if source.info().table_type != TABLE_INFO_TYPE {
        return Err("input is not a calibration table".into());
    }
    let output = CalibrationOutput::prepare(&destination, CalibrationOutputMode::Replace)
        .map_err(error_text)?;
    let _destination_lock = if input != destination && destination.exists() {
        Some(open_for_snapshot(&destination, false)?)
    } else {
        None
    };
    let reference = source.is_reference_table().map_err(error_text)?;
    if reference {
        source
            .deep_copy(TableOptions::new(&output.staging))
            .map_err(error_text)?;
    } else {
        copy_dir_recursive(&input, &output.staging).map_err(error_text)?;
    }
    let mut lock = TableWriteLock::acquire_to_reread(&output.staging, 1).map_err(error_text)?;
    let mut table = Table::open(TableOptions::new(&output.staging)).map_err(error_text)?;
    lock.record_write();
    stage_subtables(&source, &input, &output.staging, reference, &mut table)?;
    edit(&mut table, &output.staging)?;
    update_spectral_window_flags(&table, &output.staging)?;
    crate::output_merge::save_sorted(&mut table, &output.staging).map_err(error_text)?;
    drop(table);
    lock.release().map_err(error_text)?;
    output.commit().map_err(error_text)
}

#[cfg(unix)]
fn open_for_snapshot(path: &Path, read_only: bool) -> Result<Table, String> {
    Table::open_with_lock(
        TableOptions::new(path),
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLocking).read_only(read_only),
    )
    .map_err(error_text)
}

#[cfg(not(unix))]
fn open_for_snapshot(path: &Path, _read_only: bool) -> Result<Table, String> {
    Table::open(TableOptions::new(path)).map_err(error_text)
}

fn stage_subtables(
    source: &Table,
    input: &Path,
    staging: &Path,
    reference: bool,
    staged: &mut Table,
) -> Result<(), String> {
    let bases = if reference {
        source.cache_plain_table_paths().map_err(error_text)?
    } else {
        vec![input.to_path_buf()]
    };
    let base = bases
        .first()
        .ok_or("calibration table has no underlying subtable directory")?;
    for &name in STANDARD_SUBTABLE_KEYWORDS {
        let Some(link) = source.keywords().get(name).and_then(Value::as_table_ref) else {
            continue;
        };
        let origin = normalized(&base.join(link)).map_err(error_text)?;
        let target = staging.join(name);
        let copied = !reference && origin == input.join(name) && target.is_dir();
        if !copied {
            if target.is_symlink() {
                std::fs::remove_file(&target).map_err(error_text)?;
            } else if target.exists() {
                std::fs::remove_dir_all(&target).map_err(error_text)?;
            }
            copy_dir_recursive(&origin, &target).map_err(error_text)?;
        }
        staged.keywords_mut().upsert(name, Value::table_ref(name));
    }
    Ok(())
}

fn update_spectral_window_flags(table: &Table, staging: &Path) -> Result<(), String> {
    let path = staging.join("SPECTRAL_WINDOW");
    let mut spectrum = Table::open(TableOptions::new(&path)).map_err(error_text)?;
    let rows = (0..table.row_count()).collect::<Vec<_>>();
    let mut columns = table
        .required_scalar_columns_owned_for_rows(&[COL_SPECTRAL_WINDOW_ID], &rows)
        .map_err(error_text)?;
    let Some(RequiredScalarColumnValues::Int32(windows)) = columns.remove(COL_SPECTRAL_WINDOW_ID)
    else {
        return Err("invalid calibration table SPECTRAL_WINDOW_ID column".into());
    };
    let present = windows.into_iter().collect::<BTreeSet<_>>();
    let count = spectrum.row_count();
    if present.iter().any(|&id| id < 0 || id as usize >= count) {
        return Err("calibration table contains an invalid spectral window ID".into());
    }
    let mut changed = false;
    for row in 0..count {
        let expected = ScalarValue::Bool(!present.contains(&(row as i32)));
        let current = spectrum
            .cell_accessor(row, "FLAG_ROW")
            .and_then(|cell| cell.scalar())
            .map_err(error_text)?;
        if current != &expected {
            spectrum
                .cell_accessor_mut(row, "FLAG_ROW")
                .and_then(|mut cell| cell.set(Value::Scalar(expected)))
                .map_err(error_text)?;
            changed = true;
        }
    }
    if changed {
        spectrum.flush().map_err(error_text)?;
    }
    Ok(())
}

pub(crate) fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
