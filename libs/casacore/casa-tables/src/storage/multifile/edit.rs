// SPDX-License-Identifier: LGPL-3.0-or-later
//! Update packed manager data under the existing table write lock.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use super::write::{member_paths, pack_members};
use super::{MultiFile, StorageError, format_error, manager_sequence, supports_multifile};
use crate::storage::DataManagerInfo;
use crate::storage::row_changes::StagedEdit;
use crate::storage::table_control::{TableDatContents, serialize_table_dat};

pub(crate) struct PackedManagerEdit<'a> {
    stage: tempfile::TempDir,
    root: &'a Path,
    original: MultiFile,
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
            original: archive,
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
        let stage = archive.staged.path();
        let members = self.ordered_members()?;
        if self.same_member_sizes(&members)? {
            self.stage_existing_layout(stage, &members)?;
        } else {
            // Pinned casacore resyncs its managers without reloading their
            // MultiFile block map. A changed layout needs fresh handles.
            #[cfg(unix)]
            if crate::lock::LockFile::create_or_open(self.root, false, 1.0, false)?.is_multi_used()
            {
                return Err(format_error(
                    "MultiFile row edit changes the layout while the table is open elsewhere",
                ));
            }
            pack_members(
                stage,
                usize::try_from(self.original.block_size)
                    .map_err(|_| format_error("MultiFile block size exceeds address space"))?,
                self.original.header_counter.wrapping_add(1),
                members,
            )?;
        }
        archive.publish()
    }

    fn same_member_sizes(&self, members: &[(Option<PathBuf>, String)]) -> io::Result<bool> {
        if members.len() != self.original.slots.len() {
            return Ok(false);
        }
        for (path, name) in members {
            if let Some(path) = path
                && fs::metadata(path)?.len() != self.original.entries[name].byte_count
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn stage_existing_layout(
        &self,
        stage: &Path,
        members: &[(Option<PathBuf>, String)],
    ) -> Result<(), StorageError> {
        let destination = stage.join("table.mf");
        fs::copy(self.root.join("table.mf"), &destination)?;
        let mut output = File::options().write(true).open(destination)?;
        let block_size = self.original.block_size;
        for (path, name) in members {
            let Some(path) = path else {
                continue;
            };
            let mut input = File::open(path)?;
            let entry = &self.original.entries[name];
            for run in &entry.runs {
                let bytes =
                    (entry.byte_count - run.logical_first * block_size).min(run.count * block_size);
                output.seek(SeekFrom::Start(run.first * block_size))?;
                if io::copy(&mut (&mut input).take(bytes), &mut output)? != bytes {
                    return Err(format_error("staged MultiFile member ended early"));
                }
            }
        }
        output.sync_all()?;
        Ok(())
    }

    fn ordered_members(&self) -> Result<Vec<(Option<PathBuf>, String)>, StorageError> {
        let mut files = member_paths(self.path(), &self.sequences)?
            .into_iter()
            .map(|(path, name)| (name, path))
            .collect::<BTreeMap<_, _>>();
        // MFFileIO retains its numeric slot while a CASA reader unlocks and
        // reacquires the table lock. Keep every existing slot and its name.
        let slots = &self.original.slots;
        let mut ordered = Vec::with_capacity(slots.len().max(files.len()));
        for name in slots {
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
