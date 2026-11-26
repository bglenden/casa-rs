// SPDX-License-Identifier: LGPL-3.0-or-later
//! Consolidate SPW-scoped metadata before sharing it among Multi-MS members.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use casa_core::{ScalarValue, Value};
use casa_tables::{Table, TableOptions};

use super::table_error;
use crate::subtables::get_i32;
use crate::transform::selection_metadata::read_subtable_rows;
use crate::transform::{MsTransformError, rewrite_selected_table_rows};

pub(super) fn consolidate(
    destination: &Path,
    members: &[PathBuf],
    groups: &[BTreeSet<i32>],
) -> Result<(), MsTransformError> {
    let master = destination.join(&members[0]);
    merge_spectral_windows(&master, destination, members, groups)?;
    for name in [
        "FEED",
        "SOURCE",
        "FREQ_OFFSET",
        "CALDEVICE",
        "SYSPOWER",
        "SYSCAL",
    ] {
        let path = master.join(name);
        if !path.exists() {
            continue;
        }
        let table =
            Table::open(TableOptions::new(&path)).map_err(|error| table_error(&path, error))?;
        let spws = (0..table.row_count())
            .map(|row| {
                get_i32(&table, row, "SPECTRAL_WINDOW_ID")
                    .map_err(|error| MsTransformError::Spectrum(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        drop(table);
        if spws.is_empty() {
            continue;
        }
        let mut rows = Vec::new();
        let mut output_ids = Vec::new();
        for windows in groups {
            let start = windows.first().copied().unwrap_or(0);
            for (row, &id) in spws.iter().enumerate() {
                if id < 0 || windows.contains(&id) {
                    rows.push(row);
                    // CASA offsets the wildcard rows from each new member,
                    // along with that member's explicit SPW references.
                    output_ids.push(if id < 0 { start + id } else { id });
                }
            }
        }
        if rows.iter().copied().eq(0..spws.len()) && output_ids == spws {
            continue;
        }
        let original = master.join(format!(".{name}.partition-original"));
        let io_error = |source: std::io::Error| MsTransformError::PrepareOutput {
            path: path.display().to_string(),
            reason: source.to_string(),
        };
        std::fs::rename(&path, &original).map_err(io_error)?;
        let mut output_ids = output_ids.into_iter();
        rewrite_selected_table_rows(&original, &path, &rows, |_, mut row| {
            let id = output_ids.next().expect("each row has a mapped SPW");
            row.upsert("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(id)));
            Ok(row)
        })?;
        std::fs::remove_dir_all(original).map_err(io_error)?;
    }
    Ok(())
}

fn merge_spectral_windows(
    master: &Path,
    destination: &Path,
    members: &[PathBuf],
    groups: &[BTreeSet<i32>],
) -> Result<(), MsTransformError> {
    let name = "SPECTRAL_WINDOW";
    let path = master.join(name);
    let error = |source| table_error(&path, source);
    let mut output = Table::open(TableOptions::new(&path)).map_err(error)?;
    for (member, ids) in members.iter().zip(groups).skip(1) {
        let source_path = destination.join(member).join(name);
        let input = Table::open(TableOptions::new(&source_path))
            .map_err(|source| table_error(&source_path, source))?;
        for &id in ids {
            let index = id as usize;
            // Each member applies Doppler and phase-centre conversions at
            // its own reference epoch. Publish the actual grid it wrote.
            let mut rows = read_subtable_rows(&input, &[index])
                .map_err(|source| table_error(&source_path, source))?;
            *output.row_accessor_mut().row_mut(index).map_err(error)? =
                rows.pop().expect("one selected spectral-window row");
        }
    }
    output
        .prepare_write()
        .save(TableOptions::new(&path))
        .map_err(error)
}
