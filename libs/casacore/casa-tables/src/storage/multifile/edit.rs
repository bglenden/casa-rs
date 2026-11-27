// SPDX-License-Identifier: LGPL-3.0-or-later
//! Update packed manager data under the existing table write lock.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use super::write::{member_paths, pack_members};
use super::{MultiFile, StorageError, format_error, manager_sequence, supports_multifile};
use crate::storage::DataManagerInfo;
use crate::storage::table_control::{TableDatContents, serialize_table_dat};

pub(crate) struct PackedManagerEdit {
    stage: tempfile::TempDir,
    destination: PathBuf,
    block_size: usize,
    descriptor: Box<[u8]>,
    sequences: BTreeSet<u32>,
}

impl PackedManagerEdit {
    pub(crate) fn stage(
        root: &Path,
        descriptor: &TableDatContents,
        changed: &[DataManagerInfo],
    ) -> Result<Option<Self>, StorageError> {
        if !descriptor.uses_multifile() {
            return Ok(None);
        }
        if changed
            .iter()
            .any(|group| !supports_multifile(&group.dm_type))
        {
            return Err(format_error(
                "MultiFile row edits require packed storage managers",
            ));
        }
        let sequences = descriptor
            .column_set
            .data_managers
            .iter()
            .filter(|group| supports_multifile(&group.type_name))
            .map(|group| group.seq_nr)
            .collect::<BTreeSet<_>>();
        let destination = root.join("table.mf");
        let archive = MultiFile::open(&destination)?;
        if archive.checksums.is_some() {
            return Err(format_error(
                "editing MultiFile tables with checksums is unsupported",
            ));
        }
        let stage = tempfile::Builder::new()
            .prefix(".multifile-edit-")
            .tempdir_in(root)?;
        for name in archive.entries.keys() {
            if !manager_sequence(name).is_some_and(|seq| sequences.contains(&seq)) {
                return Err(format_error(
                    "MultiFile has an unsupported member for row edits",
                ));
            }
            let mut file = archive.entry(name).expect("validated MultiFile member");
            io::copy(&mut file, &mut File::create(stage.path().join(name))?)?;
        }
        Ok(Some(Self {
            stage,
            destination,
            block_size: usize::try_from(archive.block_size)
                .map_err(|_| format_error("MultiFile block size exceeds address space"))?,
            descriptor: serialize_table_dat(descriptor)?,
            sequences,
        }))
    }

    pub(crate) fn path(&self) -> &Path {
        self.stage.path()
    }

    pub(crate) fn publish(self, descriptor: &TableDatContents) -> Result<(), StorageError> {
        if serialize_table_dat(descriptor)? != self.descriptor {
            // A normal row edit replaces one archive. A manager that needs a
            // new control record would require publishing two different files.
            return Err(format_error(
                "MultiFile row edit would change the table descriptor",
            ));
        }
        let path = self.path();
        pack_members(path, self.block_size, member_paths(path, &self.sequences)?)?;
        let written = path.join("table.mf");
        fs::set_permissions(&written, fs::metadata(&self.destination)?.permissions())?;
        fs::rename(written, self.destination)?;
        Ok(())
    }
}
