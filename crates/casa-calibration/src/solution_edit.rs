// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage and publish calibration table edits while preserving the source schema.

use std::path::Path;

use casa_tables::{Table, TableOptions, TableWriteLock};

use crate::CalibrationOutputMode;
use crate::output::CalibrationOutput;
use crate::paths::{copy_dir_recursive, normalized, overlap};

pub(crate) fn edit_calibration_table(
    input: &Path,
    destination: &Path,
    edit: impl FnOnce(&mut Table, &Path) -> Result<(), String>,
) -> Result<(), String> {
    stage_calibration_table_edit(input, destination, |table, path| {
        edit(table, path)?;
        crate::output_merge::save_sorted(table, path).map_err(error_text)
    })
}

pub(crate) fn stage_calibration_table_edit(
    input: &Path,
    destination: &Path,
    edit_and_save: impl FnOnce(&mut Table, &Path) -> Result<(), String>,
) -> Result<(), String> {
    let input = normalized(input).map_err(error_text)?;
    let destination = normalized(destination).map_err(error_text)?;
    if input != destination && overlap(&input, &destination).map_err(error_text)? {
        return Err("the input and output calibration tables overlap".into());
    }
    #[cfg(unix)]
    let source = Table::open_with_lock(
        TableOptions::new(&input),
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLocking).read_only(true),
    )
    .map_err(error_text)?;
    #[cfg(not(unix))]
    let source = Table::open(TableOptions::new(&input)).map_err(error_text)?;
    if source.info().table_type != crate::constants::TABLE_INFO_TYPE {
        return Err("input is not a calibration table".into());
    }
    let output = CalibrationOutput::prepare(&destination, CalibrationOutputMode::Replace)
        .map_err(error_text)?;
    copy_dir_recursive(&input, &output.staging).map_err(error_text)?;
    let mut lock = TableWriteLock::acquire_to_reread(&output.staging, 0).map_err(error_text)?;
    let mut table = Table::open(TableOptions::new(&output.staging)).map_err(error_text)?;
    edit_and_save(&mut table, &output.staging)?;
    lock.record_write();
    drop(table);
    lock.release().map_err(error_text)?;
    output.commit().map_err(error_text)
}

pub(crate) fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
