// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::storage::standard_stman::{DEFAULT_SSM_BUCKET_SIZE, SSM_HEADER_SIZE, StagedRecords};

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
            Self::AppendStandard { file, records, .. } => {
                file.seek(SeekFrom::Start(records.retained_end))?;
                std::io::copy(&mut records.tail, file)?;
                file.set_len(records.new_end)?;
                file.rewind()?;
                file.write_all(&records.header)
            }
            Self::Replace { staged, target, .. } | Self::PublishControl { staged, target } => {
                fs::rename(staged, target)
            }
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
            Self::AppendStandard {
                file,
                original,
                records,
            } => rewrite(file, original, Some(records.retained_end)),
            Self::Replace {
                target,
                original: Some(original),
                ..
            } => fs::rename(original, target),
            Self::Replace {
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
    /// Publish under a write lock, with the control file last. Preflight
    /// every target before changing any and restore the previous files if
    /// an ordinary I/O failure interrupts the update.
    pub(crate) fn publish(self) -> Result<(), StorageError> {
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
                changes.push(Update::PublishControl { staged, target });
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
                    fs::copy(&target, &backup)?;
                    changes.push(Update::Rewrite {
                        file,
                        original: File::open(backup)?,
                        updated: File::open(staged)?,
                    });
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
        for i in 0..changes.len() {
            if let Err(error) = changes[i].apply() {
                let mut recovery_error = None;
                for change in changes[..=i].iter_mut().rev() {
                    if let Err(error) = change.undo() {
                        recovery_error = Some(error);
                    }
                }
                return if let Some(recovery_error) = recovery_error {
                    let path = self.staged.keep();
                    Err(StorageError::FormatMismatch(format!(
                        "table update failed: {error}; recovery failed: {recovery_error};                          remaining original files at {}",
                        path.display()
                    )))
                } else {
                    Err(error.into())
                };
            }
        }
        tiled_stman::invalidate_shared_tile_cache_for_table(self.table);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
