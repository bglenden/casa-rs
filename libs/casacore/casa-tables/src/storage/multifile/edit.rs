// SPDX-License-Identifier: LGPL-3.0-or-later
//! Update packed manager data under the existing table write lock.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use super::write::{member_paths, pack_members};
use super::{MultiFile, StorageError, format_error, manager_sequence, supports_multifile};
use crate::storage::DataManagerInfo;
use crate::storage::row_changes::StagedEdit;
use crate::storage::table_control::{TableDatContents, serialize_table_dat};

pub(crate) struct PackedManagerEdit<'a> {
    stage: tempfile::TempDir,
    root: &'a Path,
    block_size: usize,
    counter: u64,
    slots: Vec<String>,
    descriptor: Box<[u8]>,
    sequences: BTreeSet<u32>,
}

impl<'a> PackedManagerEdit<'a> {
    pub(crate) fn stage(
        root: &'a Path,
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
        File::options().write(true).open(&destination)?;
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
            root,
            block_size: usize::try_from(archive.block_size)
                .map_err(|_| format_error("MultiFile block size exceeds address space"))?,
            counter: archive.header_counter.wrapping_add(1),
            slots: archive.slots,
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
        let archive = StagedEdit::for_shared_manager_files(self.root)?;
        pack_members(
            archive.staged.path(),
            self.block_size,
            self.counter,
            self.ordered_members()?,
        )?;
        archive.publish()
    }

    fn ordered_members(&self) -> Result<Vec<(Option<PathBuf>, String)>, StorageError> {
        let mut files = member_paths(self.path(), &self.sequences)?
            .into_iter()
            .map(|(path, name)| (name, path))
            .collect::<BTreeMap<_, _>>();
        // MFFileIO retains its numeric slot while a CASA reader unlocks and
        // reacquires the table lock. Keep every existing slot and its name.
        let mut ordered = Vec::with_capacity(self.slots.len().max(files.len()));
        for name in &self.slots {
            let path = if name.is_empty() {
                None
            } else {
                Some(files.remove(name).ok_or_else(|| {
                    format_error("MultiFile row edit removed a logical manager file")
                })?)
            };
            ordered.push((path, name.clone()));
        }
        ordered.extend(files.into_iter().map(|(name, path)| (Some(path), name)));
        Ok(ordered)
    }
}
