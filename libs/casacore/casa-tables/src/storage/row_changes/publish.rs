// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::storage::TABLE_INFO_FILE;
use crate::storage::standard_stman::{DEFAULT_SSM_BUCKET_SIZE, SSM_HEADER_SIZE, StagedRecords};
use crate::storage::table_control::{StagedTableDat, TableDatCommit};

use super::{Extension, StagedEdit, StorageError, TABLE_CONTROL_FILE, tiled_stman};

enum Update {
    Index {
        file: File,
        offset: u64,
        original: Vec<u8>,
        updated: Vec<u8>,
    },
    Extend {
        file: File,
        old_len: u64,
        new_len: u64,
    },
    Rewrite {
        file: File,
        original: File,
        updated: File,
    },
    FullRewrite(FullRewrite),
    AppendStandard {
        file: File,
        original: File,
        records: StagedRecords,
    },
    Replace {
        staged: PathBuf,
        target: PathBuf,
        original: Option<PathBuf>,
    },
    PublishControl {
        staged: PathBuf,
        target: PathBuf,
        original: Option<PathBuf>,
    },
    PublishFullControl {
        commit: Option<TableDatCommit>,
        target: PathBuf,
        original: Option<PathBuf>,
    },
    Remove {
        target: PathBuf,
        original: PathBuf,
    },
}

impl Update {
    fn apply(&mut self) -> std::io::Result<()> {
        match self {
            Self::Index {
                file,
                offset,
                updated,
                ..
            } => write_index(file, *offset, updated),
            Self::Extend {
                file,
                old_len,
                new_len,
            } => file.set_len((*new_len).max(*old_len)),
            Self::Rewrite { file, updated, .. } => rewrite(file, updated, None),
            Self::FullRewrite(change) => change.apply(),
            Self::AppendStandard { file, records, .. } => {
                file.seek(SeekFrom::Start(records.retained_end))?;
                std::io::copy(&mut records.tail, file)?;
                file.set_len(records.new_end)?;
                file.rewind()?;
                file.write_all(&records.header)
            }
            Self::Replace { staged, target, .. } | Self::PublishControl { staged, target, .. } => {
                fs::rename(staged, target)
            }
            Self::PublishFullControl { commit, .. } => commit
                .take()
                .expect("descriptor is published once")
                .publish(),
            Self::Remove { target, original } => fs::rename(target, original),
        }
    }

    fn undo(&mut self) -> std::io::Result<()> {
        match self {
            Self::Index {
                file,
                offset,
                original,
                ..
            } => write_index(file, *offset, original),
            Self::Extend { file, old_len, .. } => file.set_len(*old_len),
            Self::Rewrite { file, original, .. } => rewrite(file, original, None),
            Self::FullRewrite(change) => change.undo(),
            Self::AppendStandard {
                file,
                original,
                records,
            } => rewrite(file, original, Some(records.retained_end)),
            Self::Replace {
                target,
                original: Some(original),
                ..
            }
            | Self::PublishControl {
                target,
                original: Some(original),
                ..
            }
            | Self::PublishFullControl {
                target,
                original: Some(original),
                ..
            }
            | Self::Remove { target, original } => fs::rename(original, target),
            Self::Replace {
                target,
                original: None,
                ..
            }
            | Self::PublishFullControl {
                target,
                original: None,
                ..
            } => match fs::remove_file(target) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                result => result,
            },
            // The control file is the final update. A failed rename leaves
            // its original in place, and success commits the transaction.
            Self::PublishControl { .. } => Ok(()),
        }
    }
}

/// A full table may have one payload per tile shape. Keep its backups on
/// disk and reopen only the two files needed for one update at a time.
struct FullRewrite {
    target: PathBuf,
    original: PathBuf,
    updated: PathBuf,
    identity: fs::Metadata,
    started: bool,
}

impl FullRewrite {
    fn prepare(target: PathBuf, original: PathBuf, updated: PathBuf) -> io::Result<Self> {
        let identity = crate::publication::private_table_file_metadata(&target)?;
        let change = Self {
            target,
            original,
            updated,
            identity,
            started: false,
        };
        let mut file = change.open_target()?;
        let mut backup = File::options()
            .write(true)
            .create_new(true)
            .open(&change.original)?;
        io::copy(&mut file, &mut backup)?;
        File::open(&change.updated)?;
        Ok(change)
    }

    fn open_target(&self) -> io::Result<File> {
        let mut options = File::options();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(&self.target)?;
        let current = file.metadata()?;
        let same_file = current.is_file();
        #[cfg(unix)]
        let same_file = {
            use std::os::unix::fs::MetadataExt;
            same_file
                && current.nlink() == 1
                && current.dev() == self.identity.dev()
                && current.ino() == self.identity.ino()
        };
        if !same_file {
            return Err(self.changed_target());
        }
        Ok(file)
    }

    fn changed_target(&self) -> io::Error {
        io::Error::other(format!(
            "full save target changed during publication: {}",
            self.target.display()
        ))
    }

    fn apply(&mut self) -> io::Result<()> {
        let mut file = self.open_target()?;
        let current = file.metadata()?;
        if current.len() != self.identity.len()
            || current.modified().ok() != self.identity.modified().ok()
        {
            return Err(self.changed_target());
        }
        let mut updated = File::open(&self.updated)?;
        self.started = true;
        rewrite(&mut file, &mut updated, None)
    }

    fn undo(&mut self) -> io::Result<()> {
        if self.started {
            rewrite(
                &mut self.open_target()?,
                &mut File::open(&self.original)?,
                None,
            )?;
        }
        Ok(())
    }
}

fn rewrite(
    file: &mut File,
    source: &mut File,
    ssm_retained_end: Option<u64>,
) -> std::io::Result<()> {
    if let Some(end) = ssm_retained_end {
        // The stage changed the file header and replaced or extended its
        // tail. Leave the existing data buckets between them untouched.
        let len = source.metadata()?.len();
        file.seek(SeekFrom::Start(end))?;
        source.seek(SeekFrom::Start(end))?;
        let mut buckets = BufReader::with_capacity(DEFAULT_SSM_BUCKET_SIZE as usize, &mut *source);
        std::io::copy(&mut buckets, file)?;
        file.set_len(len)?;
        file.rewind()?;
        source.rewind()?;
        std::io::copy(&mut source.take(SSM_HEADER_SIZE), file)?;
        return Ok(());
    }
    file.seek(SeekFrom::Start(0))?;
    source.seek(SeekFrom::Start(0))?;
    let len = std::io::copy(source, file)?;
    file.set_len(len)
}

fn write_index(file: &mut File, offset: u64, data: &[u8]) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;
    file.set_len(offset + data.len() as u64)
}

fn rollback_path(staging: &Path, name: &std::ffi::OsStr) -> PathBuf {
    let mut original = OsString::from("original-");
    original.push(name);
    staging.join(original)
}

impl StagedEdit<'_> {
    /// Prepare every destination and rollback copy before changing the table.
    /// A casacore reader can keep a manager file open across lock periods.
    pub(crate) fn prepare_full_publication(
        self,
        control: StagedTableDat<'_>,
        mut original_managers: BTreeSet<OsString>,
    ) -> Result<ReversibleEdit, StorageError> {
        let staging = self.staged.path();
        let mut entries = fs::read_dir(staging)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(fs::DirEntry::file_name);
        let mut changes = Vec::new();
        let commit = control.stage(&mut File::open(staging.join(TABLE_CONTROL_FILE))?)?;
        let descriptor_backup = rollback_path(staging, TABLE_CONTROL_FILE.as_ref());
        commit.backup(&descriptor_backup)?;
        for entry in entries {
            let name = entry.file_name();
            if name == TABLE_CONTROL_FILE {
                continue;
            }
            let target = self.table.join(&name);
            let staged = entry.path();
            match target.symlink_metadata() {
                Ok(_) if original_managers.remove(&name) || name == TABLE_INFO_FILE => {
                    changes.push(Update::FullRewrite(FullRewrite::prepare(
                        target,
                        rollback_path(staging, &name),
                        staged,
                    )?));
                }
                Ok(_) => {
                    return Err(StorageError::FormatMismatch(format!(
                        "full save would replace an auxiliary entry: {}",
                        target.display()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    changes.push(Update::Replace {
                        staged,
                        target,
                        original: None,
                    });
                }
                Err(error) => return Err(error.into()),
            }
        }
        for name in original_managers {
            let target = self.table.join(&name);
            crate::publication::private_table_file_metadata(&target)?;
            File::options().write(true).open(&target)?;
            changes.push(Update::Remove {
                target,
                original: rollback_path(staging, &name),
            });
        }
        changes.push(Update::PublishFullControl {
            commit: Some(commit),
            target: self.table.join(TABLE_CONTROL_FILE),
            original: descriptor_backup.exists().then_some(descriptor_backup),
        });
        Ok(ReversibleEdit {
            table: self.table.to_path_buf(),
            staging: self.staged,
            changes,
            applied: 0,
        })
    }

    /// Publish under a write lock, with the control file last. Preflight
    /// every target before changing any and restore the previous files if
    /// an ordinary I/O failure interrupts the update.
    pub(crate) fn publish(self) -> Result<(), StorageError> {
        let mut edit = self.prepare_publication(false)?;
        if let Err(error) = edit.apply() {
            return Err(edit.restore_after(error));
        }
        Ok(())
    }

    pub(crate) fn prepare_publication(
        self,
        reversible: bool,
    ) -> Result<ReversibleEdit, StorageError> {
        let staging = self.staged.path();
        let mut files = fs::read_dir(staging)?.collect::<Result<Vec<_>, _>>()?;
        files.sort_by_key(|file| file.file_name() == TABLE_CONTROL_FILE);
        let mut changes = Vec::with_capacity(self.extensions.len() + files.len());
        for extension in self.extensions {
            match extension {
                Extension::Index {
                    path,
                    offset,
                    bytes,
                } => {
                    let mut file = File::options().read(true).write(true).open(path)?;
                    file.seek(SeekFrom::Start(offset))?;
                    let mut original = Vec::new();
                    file.read_to_end(&mut original)?;
                    changes.push(Update::Index {
                        file,
                        offset,
                        original,
                        updated: bytes,
                    });
                }
                Extension::Payload { path, len } => {
                    let file = File::options().write(true).open(path)?;
                    let old_len = file.metadata()?.len();
                    changes.push(Update::Extend {
                        file,
                        old_len,
                        new_len: len,
                    });
                }
                Extension::StandardAppend { path, records } => {
                    let file = match File::options().read(true).write(true).open(&path) {
                        Ok(file) => file,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            return Err(StorageError::MissingDataFile(path));
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let backup =
                        rollback_path(staging, path.file_name().expect("manager filename"));
                    fs::copy(&path, &backup)?;
                    changes.push(Update::AppendStandard {
                        file,
                        original: File::open(backup)?,
                        records,
                    });
                }
            }
        }
        for entry in files {
            let name = entry.file_name();
            let staged = entry.path();
            let target = self.table.join(&name);
            // casacore reopens table.dat by name and publishes its own copy
            // by rename, so readers can always open a complete descriptor.
            if name == TABLE_CONTROL_FILE {
                match File::options().write(true).open(&target) {
                    Ok(file) => fs::set_permissions(&staged, file.metadata()?.permissions())?,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                let original = if reversible {
                    let original = rollback_path(staging, &name);
                    fs::hard_link(&target, &original)?;
                    Some(original)
                } else {
                    None
                };
                changes.push(Update::PublishControl {
                    staged,
                    target,
                    original,
                });
                continue;
            }
            let retain_manager = self.preserve_file_identity;
            let backup = rollback_path(staging, &name);
            let original = match File::options()
                .read(retain_manager)
                .write(true)
                .open(&target)
            {
                Ok(file) if retain_manager => {
                    // Retained casacore readers keep manager files open and
                    // resync through those handles after the lock changes.
                    changes.push(backup_rewrite(file, &staged, &backup)?);
                    continue;
                }
                Ok(file) => {
                    fs::set_permissions(&staged, file.metadata()?.permissions())?;
                    fs::hard_link(&target, &backup)?;
                    Some(backup)
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            changes.push(Update::Replace {
                staged,
                target,
                original,
            });
        }
        Ok(ReversibleEdit {
            table: self.table.to_path_buf(),
            staging: self.staged,
            changes,
            applied: 0,
        })
    }
}

fn backup_rewrite(mut file: File, staged: &Path, backup: &Path) -> std::io::Result<Update> {
    let mut original = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(backup)?;
    std::io::copy(&mut file, &mut original)?;
    Ok(Update::Rewrite {
        file,
        original,
        updated: File::open(staged)?,
    })
}

pub(crate) struct ReversibleEdit {
    table: PathBuf,
    staging: tempfile::TempDir,
    changes: Vec<Update>,
    applied: usize,
}

#[cfg(test)]
thread_local! {
    pub(crate) static FAIL_AFTER_UPDATES: std::cell::Cell<Option<usize>> = const {
        std::cell::Cell::new(None)
    };
}

impl ReversibleEdit {
    pub(crate) fn apply(&mut self) -> Result<(), StorageError> {
        while let Some(change) = self.changes.get_mut(self.applied) {
            #[cfg(test)]
            if FAIL_AFTER_UPDATES.get() == Some(self.applied) {
                FAIL_AFTER_UPDATES.set(None);
                return Err(
                    std::io::Error::other("injected failure after live file update").into(),
                );
            }
            // A failed rename leaves both paths intact. Direct writes can
            // change bytes before surfacing an error and must be undone.
            let result = change.apply();
            if result.is_ok()
                || !matches!(
                    change,
                    Update::Replace { .. }
                        | Update::PublishControl { .. }
                        | Update::PublishFullControl { .. }
                        | Update::Remove { .. }
                )
            {
                self.applied += 1;
            }
            result?;
        }
        tiled_stman::invalidate_shared_tile_cache_for_table(&self.table);
        Ok(())
    }

    pub(crate) fn restore_after(mut self, error: StorageError) -> StorageError {
        if let Err(recovery) = self.restore() {
            return StorageError::FormatMismatch(format!(
                "table update failed: {error}; recovery failed: {recovery}; original files at {}",
                self.staging.keep().display()
            ));
        }
        error
    }

    pub(crate) fn restore(&mut self) -> Result<(), StorageError> {
        let mut saved_error = None;
        for change in self.changes[..self.applied].iter_mut().rev() {
            if let Err(error) = change.undo() {
                saved_error = Some(error);
            }
        }
        self.applied = 0;
        tiled_stman::invalidate_shared_tile_cache_for_table(&self.table);
        saved_error.map_or(Ok(()), |error| Err(error.into()))
    }

    pub(crate) fn preserve(self) -> PathBuf {
        self.staging.keep()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_control_and_manager_can_be_restored_for_a_failed_batch() {
        let table = tempfile::tempdir().unwrap();
        let mut edit = StagedEdit::new(table.path()).unwrap();
        edit.preserve_file_identity = true;
        let control = table.path().join(TABLE_CONTROL_FILE);
        let manager = table.path().join("table.f0");
        fs::write(&control, b"original descriptor").unwrap();
        fs::write(&manager, b"original manager").unwrap();
        fs::write(
            edit.staged.path().join(TABLE_CONTROL_FILE),
            b"appended descriptor",
        )
        .unwrap();
        fs::write(
            edit.staged.path().join("table.f0"),
            b"appended manager bytes",
        )
        .unwrap();

        let mut prepared = edit.prepare_publication(true).unwrap();
        prepared.apply().unwrap();
        assert_eq!(fs::read(&control).unwrap(), b"appended descriptor");
        assert_eq!(fs::read(&manager).unwrap(), b"appended manager bytes");

        prepared.restore().unwrap();
        assert_eq!(fs::read(&control).unwrap(), b"original descriptor");
        assert_eq!(fs::read(&manager).unwrap(), b"original manager");
    }

    #[test]
    fn failed_control_rename_preserves_the_descriptor_and_restores_the_manager() {
        let table = tempfile::tempdir().unwrap();
        let mut edit = StagedEdit::new(table.path()).unwrap();
        edit.preserve_file_identity = true;
        let control = table.path().join(TABLE_CONTROL_FILE);
        let manager = table.path().join("table.f0");
        fs::write(&control, b"old descriptor").unwrap();
        fs::write(&manager, b"old manager file").unwrap();
        fs::write(edit.staged.path().join("table.f0"), b"new manager file").unwrap();
        // Renaming this directory over the file fails after the manager write.
        fs::create_dir(edit.staged.path().join(TABLE_CONTROL_FILE)).unwrap();

        assert!(edit.publish().is_err());
        assert_eq!(fs::read(control).unwrap(), b"old descriptor");
        assert_eq!(fs::read(manager).unwrap(), b"old manager file");
    }

    #[test]
    fn rewrite_ssm_tail_keeps_published_buckets_and_restores_the_old_length() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let original = dir.path().join("original");
        let header = SSM_HEADER_SIZE as usize;
        let retained_end = header + 64;
        let bytes = [vec![1; header], vec![2; 64], vec![3; 32]].concat();
        fs::write(&target, &bytes).unwrap();
        fs::write(&original, &bytes).unwrap();
        for memory_limit in [128, 0] {
            let mut tail = tempfile::spooled_tempfile(memory_limit);
            tail.write_all(&[7; 96]).unwrap();
            assert_eq!(tail.is_rolled(), memory_limit == 0);
            tail.rewind().unwrap();
            let mut update = Update::AppendStandard {
                file: File::options().write(true).open(&target).unwrap(),
                original: File::open(&original).unwrap(),
                records: StagedRecords {
                    retained_end: retained_end as u64,
                    new_end: (retained_end + 96) as u64,
                    header: vec![5; header],
                    tail,
                },
            };
            update.apply().unwrap();
            assert_eq!(
                fs::read(&target).unwrap(),
                [vec![5; header], vec![2; 64], vec![7; 96]].concat()
            );
            update.undo().unwrap();
            assert_eq!(fs::read(&target).unwrap(), bytes);
        }
    }

    #[test]
    fn sparse_ssm_stage_cannot_replace_a_missing_manager() {
        let table = tempfile::tempdir().unwrap();
        let mut edit = StagedEdit::new(table.path()).unwrap();
        let name = "table.f0";
        let target = table.path().join(name);
        edit.preserve_file_identity = true;
        edit.extensions.push(Extension::StandardAppend {
            path: target.clone(),
            records: StagedRecords {
                retained_end: SSM_HEADER_SIZE,
                new_end: SSM_HEADER_SIZE,
                header: vec![],
                tail: tempfile::spooled_tempfile(0),
            },
        });
        assert!(matches!(
            edit.publish(),
            Err(StorageError::MissingDataFile(path)) if path == target
        ));
        assert!(!target.exists());
    }
}
