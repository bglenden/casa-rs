// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage and publish a donor-only MAIN with the reconciled subtables.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use casa_tables::publication::{
    check_write_access, exchange_directories, lock_table_tree_for_replacement,
    rename_without_replacing,
};
use casa_tables::{Table, TableOptions, TableWriteLock};
use tempfile::TempDir;

use super::{MetadataPlan, PhysicalConcatenation, main_rows, publication::io};
use crate::ms::{measurement_set_main_table_bindings, measurement_set_table_options};
use crate::tool_sort::{check_named_table_output, subtables::copy_concatenation_links};
use crate::{MeasurementSet, MsResult};

pub(super) struct NamedOutput {
    destination: PathBuf,
    workspace: TempDir,
    original: Option<Destination>,
}

struct Destination {
    identity: fs::Metadata,
    locks: Option<Vec<TableWriteLock>>,
}

impl NamedOutput {
    pub(super) fn stage(
        name: &Path,
        target: &MeasurementSet,
        donor: &MeasurementSet,
    ) -> MsResult<Self> {
        let parent = name
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = parent.canonicalize().map_err(io)?;
        let filename = name
            .file_name()
            .ok_or_else(|| io("invalid concatenation output name"))?;
        let destination = parent.join(filename);
        for table in [target.main_table(), donor.main_table()] {
            check_named_table_output(table, &destination)?;
        }
        let original = match fs::symlink_metadata(&destination) {
            Ok(identity) if identity.is_dir() => {
                let locks = if fs::read_dir(&destination).map_err(io)?.next().is_none() {
                    None
                } else if Table::open(TableOptions::new(&destination)).is_ok() {
                    check_write_access(&destination).map_err(io)?;
                    Some(lock_table_tree_for_replacement(&destination)?)
                } else {
                    return Err(io("concatenation output already exists and is not a table"));
                };
                Some(Destination { identity, locks })
            }
            Ok(_) => {
                return Err(io(
                    "concatenation output already exists and is not a directory",
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(io(error)),
        };
        let workspace = tempfile::Builder::new()
            .prefix(".casa-rs-concat-output-")
            .tempdir_in(parent)
            .map_err(io)?;
        Ok(Self {
            destination,
            workspace,
            original,
        })
    }

    pub(super) fn finish(
        self,
        attached: &MeasurementSet,
        donor: &MeasurementSet,
        plan: &MetadataPlan<'_>,
        mode: &PhysicalConcatenation<'_>,
        held: &BTreeSet<PathBuf>,
    ) -> MsResult<()> {
        let main = donor.main_table();
        let schema = main
            .schema()
            .ok_or_else(|| io("donor MAIN has no schema"))?;
        let staged = self.workspace.path().join("output.ms");
        let mut empty = Table::with_schema(schema.clone());
        empty.set_info(main.info().clone());
        copy_concatenation_links(attached.main_table(), main, &mut empty, &staged, held)?;
        empty.save_with_bindings(
            measurement_set_table_options(&staged),
            &measurement_set_main_table_bindings(main),
        )?;
        if mode.handling % 2 == 0 {
            let output = Table::open(TableOptions::new(&staged))?;
            if let Some(rows) =
                main_rows::prepare(attached, donor, &output, plan, mode.weight_scale)?
            {
                let mut guard = TableWriteLock::acquire_to_reread(&staged, 1)?;
                let mut edit = output.prepare_record_append_with_options(
                    &rows.records,
                    rows.options(),
                    &mut guard,
                )?;
                edit.publish()?;
                edit.synchronize()?;
            }
        }
        if let Some(original) = &self.original {
            let current = fs::symlink_metadata(&self.destination).map_err(io)?;
            if !same_directory(&original.identity, &current) {
                return Err(io("concatenation destination changed during staging"));
            }
            if let Some(locks) = &original.locks {
                if !locks
                    .iter()
                    .all(|lock| lock.matches_table_path(lock.path()))
                {
                    return Err(io("concatenation destination locks changed during staging"));
                }
                exchange_directories(&staged, &self.destination).map_err(io)?;
            } else {
                // This succeeds only when the destination is still empty.
                fs::rename(&staged, &self.destination).map_err(io)?;
            }
        } else {
            rename_without_replacing(&staged, &self.destination).map_err(io)?;
        }
        Ok(())
    }
}

#[cfg(unix)]
fn same_directory(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_directory(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.created().ok() == right.created().ok()
}
