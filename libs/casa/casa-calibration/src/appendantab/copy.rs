// SPDX-License-Identifier: LGPL-3.0-or-later
//! A private MeasurementSet copy with calibration storage owned by the copy.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::MeasurementSet;
use casa_tables::publication::copy_locked_table_tree;
use casa_tables::{add_directory, lexical_absolute};

use super::compute::message;

pub(super) struct Subtables {
    pub(super) gain: PathBuf,
    pub(super) tsys: PathBuf,
}

pub(super) fn stage(
    ms: &MeasurementSet,
    source: &Path,
    destination: &Path,
    import: bool,
) -> Result<Subtables, String> {
    #[cfg(unix)]
    let _gain = source
        .join("GAIN_CURVE")
        .is_dir()
        .then(|| retained_table(&source.join("GAIN_CURVE")))
        .transpose()?;
    copy_tree(source, destination, &mut HashSet::new())?;
    let path = |name| {
        if import {
            stage_linked(ms, source, destination, name)
        } else {
            Ok(destination.join(name))
        }
    };
    Ok(Subtables {
        gain: path("GAIN_CURVE")?,
        tsys: path("SYSCAL")?,
    })
}

fn stage_linked(
    ms: &MeasurementSet,
    source: &Path,
    destination: &Path,
    name: &str,
) -> Result<PathBuf, String> {
    let conventional = destination.join(name);
    let Some(reference) = ms
        .main_table()
        .keywords()
        .get(name)
        .and_then(casa_core::Value::as_table_ref)
    else {
        return Ok(conventional);
    };
    let referenced = lexical_absolute(&add_directory(reference, source));
    if let Ok(relative) = referenced.strip_prefix(source)
        && !relative.as_os_str().is_empty()
        && destination.join(relative).is_dir()
    {
        return Ok(destination.join(relative));
    }
    let independent = if conventional.symlink_metadata().is_ok() {
        // Preserve an unlinked conventional table when MAIN names another.
        tempfile::Builder::new()
            .prefix(".antab-subtable-")
            .tempdir_in(destination)
            .map_err(message)?
            .keep()
            .join(name)
    } else {
        conventional
    };
    copy_tree(&referenced, &independent, &mut HashSet::new())?;
    Ok(independent)
}

fn copy_tree(
    source: &Path,
    destination: &Path,
    ancestors: &mut HashSet<PathBuf>,
) -> Result<(), String> {
    let source = fs::canonicalize(source).map_err(message)?;
    if ancestors.contains(&source) {
        return Err(format!(
            "cyclic MeasurementSet link at {}",
            source.display()
        ));
    }
    #[cfg(unix)]
    let _retained = source
        .join("table.dat")
        .is_file()
        .then(|| retained_table(&source))
        .transpose()?;
    copy_locked_table_tree(&source, destination).map_err(message)?;
    own_storage(&source, destination, ancestors)
}

fn own_storage(
    source: &Path,
    destination: &Path,
    ancestors: &mut HashSet<PathBuf>,
) -> Result<(), String> {
    ancestors.insert(source.to_owned());
    for entry in fs::read_dir(destination).map_err(message)? {
        let entry = entry.map_err(message)?;
        let kind = entry.file_type().map_err(message)?;
        let original = source.join(entry.file_name());
        let copied = entry.path();
        if kind.is_symlink() {
            let resolved = fs::canonicalize(&original).map_err(message)?;
            let metadata = resolved.metadata().map_err(message)?;
            fs::remove_file(&copied).map_err(message)?;
            if metadata.is_dir() {
                copy_tree(&resolved, &copied, ancestors)?;
            } else if metadata.is_file() {
                fs::copy(&resolved, &copied).map_err(message)?;
            } else {
                return Err(format!(
                    "cannot copy MeasurementSet link {}",
                    original.display()
                ));
            }
        } else if kind.is_dir() {
            own_storage(&original, &copied, ancestors)?;
        }
    }
    ancestors.remove(source);
    Ok(())
}

#[cfg(unix)]
fn retained_table(path: &Path) -> Result<casa_tables::Table, String> {
    use casa_tables::{LockMode, LockOptions, Table, TableOptions};
    Table::open_with_lock(
        TableOptions::new(path),
        LockOptions::new(LockMode::AutoLocking),
    )
    .map_err(message)
}
