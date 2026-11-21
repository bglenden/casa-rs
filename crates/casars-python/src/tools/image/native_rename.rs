// SPDX-License-Identifier: LGPL-3.0-or-later

//! Publication and rollback for moving complete native image tables.

use std::fs::Metadata;
use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::Arc;

use casa_tables::publication::{
    copy_locked_table_tree, exchange_directories, rename_without_replacing,
};
use casa_tables::{TableUseLock, TableWriteLock};
use pyo3::prelude::*;
use tempfile::TempDir;

use super::handle::{raised, same_file_identity};

pub(super) fn check_path_relationship(source: &Path, target: &Path, name: &str) -> PyResult<()> {
    if target.starts_with(source) {
        return Err(raised(format!(
            "Directory::move error on {} to {name}: Invalid argument",
            source.display()
        )));
    }
    if source.starts_with(target) {
        return Err(raised(format!(
            "Cannot rename image {} to {} because the destination contains the source image",
            source.display(),
            target.display()
        )));
    }
    Ok(())
}

pub(super) enum NativeTableMove {
    Renamed(Option<TempDir>),
    Copied {
        source_stage: TempDir,
        target_stage: TempDir,
        replaced: bool,
        use_lock: Arc<TableUseLock>,
        _write_lock: TableWriteLock,
    },
}

impl NativeTableMove {
    pub(super) fn use_lock(&self) -> Option<Arc<TableUseLock>> {
        match self {
            Self::Renamed(_) => None,
            Self::Copied { use_lock, .. } => Some(use_lock.clone()),
        }
    }

    pub(super) fn check_target(&self, target: &Path) -> PyResult<()> {
        if let Self::Copied { use_lock, .. } = self
            && !matches_table_entry(use_lock, target)
        {
            return Err(changed_target(target));
        }
        Ok(())
    }

    pub(super) fn finish(self, target: &Path) -> PyResult<()> {
        if let Self::Copied {
            source_stage,
            target_stage,
            _write_lock,
            ..
        } = self
        {
            let source = close_stage(source_stage, target);
            let destination = close_stage(target_stage, target);
            match (source, destination) {
                (Ok(()), result) | (result, Ok(())) => return result,
                (Err(source), Err(destination)) => {
                    return Err(raised(format!("{source}. {destination}")));
                }
            }
        }
        Ok(())
    }

    pub(super) fn restore<T>(self, source: &Path, target: &Path, error: PyErr) -> PyResult<T> {
        match self {
            Self::Renamed(backup) => restore_rename(source, target, backup, error),
            Self::Copied {
                source_stage,
                target_stage,
                replaced,
                use_lock,
                _write_lock,
                ..
            } => {
                let target_result =
                    restore_copied_target(target, &target_stage, replaced, &use_lock);
                let source_result =
                    rename_without_replacing(&source_stage.path().join("source"), source);
                if source_result.is_ok() && target_result.is_ok() {
                    return Err(error);
                }
                let staged_source = source_stage.keep();
                let staged_target = target_stage.keep();
                Err(raised(format!(
                    "{error}. Could not restore image names. Source: {source_result:?}. \
                     Destination: {target_result:?}. Inspect {} and {}",
                    staged_source.display(),
                    staged_target.display()
                )))
            }
        }
    }
}

pub(super) fn move_native_table(
    source: &Path,
    source_metadata: &Metadata,
    target: &Path,
    target_metadata: Option<&Metadata>,
    prepare_copy: impl FnOnce(&Path) -> PyResult<()>,
) -> PyResult<NativeTableMove> {
    let Some(expected) = target_metadata else {
        return match rename_without_replacing(source, target) {
            Ok(()) => Ok(NativeTableMove::Renamed(None)),
            Err(error) if error.kind() == ErrorKind::CrossesDevices => {
                move_across_devices(source, source_metadata, target, None, prepare_copy)
            }
            Err(error) => Err(raised(error)),
        };
    };
    let backup = tempdir_beside(source)?;
    if let Err(error) = exchange_directories(source, target) {
        return if error.kind() == ErrorKind::CrossesDevices {
            move_across_devices(
                source,
                source_metadata,
                target,
                target_metadata,
                prepare_copy,
            )
        } else {
            Err(raised(error))
        };
    }
    let previous = backup.path().join("previous");
    if let Err(error) = rename_without_replacing(source, &previous) {
        if let Err(restore) = exchange_directories(source, target) {
            return Err(raised(format!(
                "Could not complete image rename ({error}) or restore by exchanging {} and {} ({restore})",
                source.display(),
                target.display()
            )));
        }
        return Err(raised(error));
    }
    let moved = NativeTableMove::Renamed(Some(backup));
    if !matches_entry(expected, &previous) {
        return moved.restore(source, target, changed_target(target));
    }
    Ok(moved)
}

fn tempdir_beside(path: &Path) -> PyResult<TempDir> {
    let parent = path
        .parent()
        .ok_or_else(|| raised("Image path has no parent directory"))?;
    tempfile::tempdir_in(parent).map_err(raised)
}

fn matches_entry(expected: &Metadata, path: &Path) -> bool {
    path.symlink_metadata()
        .is_ok_and(|actual| same_file_identity(expected, &actual))
}

fn matches_table_entry(expected: &TableUseLock, path: &Path) -> bool {
    path.symlink_metadata()
        .is_ok_and(|actual| expected.matches_directory_metadata(&actual))
}

fn changed_target(target: &Path) -> PyErr {
    raised(format!(
        "The destination image entry at {} has changed",
        target.display()
    ))
}

fn move_across_devices(
    source: &Path,
    source_metadata: &Metadata,
    target: &Path,
    target_metadata: Option<&Metadata>,
    prepare_copy: impl FnOnce(&Path) -> PyResult<()>,
) -> PyResult<NativeTableMove> {
    let source_stage = tempdir_beside(source)?;
    let target_stage = tempdir_beside(target)?;
    let claimed = source_stage.path().join("source");
    let copied = target_stage.path().join("previous");
    rename_without_replacing(source, &claimed).map_err(raised)?;

    let install = || {
        if !matches_entry(source_metadata, &claimed) {
            return Err(raised(format!(
                "The source image entry at {} has changed",
                source.display()
            )));
        }
        copy_locked_table_tree(&claimed, &copied).map_err(raised)?;
        prepare_copy(&copied)?;
        let use_lock = Arc::new(TableUseLock::acquire(&copied).map_err(raised)?);
        let write_lock = TableWriteLock::acquire(&copied, 1).map_err(raised)?;
        if target_metadata.is_some() {
            exchange_directories(&copied, target).map_err(raised)?;
        } else {
            rename_without_replacing(&copied, target).map_err(raised)?;
        }
        Ok::<_, PyErr>((use_lock, write_lock))
    };
    let (use_lock, write_lock) = match install() {
        Ok(locks) => locks,
        Err(error) => {
            if let Err(restore) = rename_without_replacing(&claimed, source) {
                let recovery = source_stage.keep();
                return Err(raised(format!(
                    "{error}. Could not restore image {}: {restore}. Inspect {}",
                    source.display(),
                    recovery.display()
                )));
            }
            return Err(error);
        }
    };
    let moved = NativeTableMove::Copied {
        source_stage,
        target_stage,
        replaced: target_metadata.is_some(),
        use_lock,
        _write_lock: write_lock,
    };
    if target_metadata.is_some_and(|expected| !matches_entry(expected, &copied)) {
        return moved.restore(source, target, changed_target(target));
    }
    Ok(moved)
}

fn restore_copied_target(
    target: &Path,
    stage: &TempDir,
    replaced: bool,
    published: &TableUseLock,
) -> io::Result<()> {
    let changed = || {
        io::Error::other(format!(
            "The destination image entry at {} has changed",
            target.display()
        ))
    };
    if !matches_table_entry(published, target) {
        return Err(changed());
    }
    let withdrawn = stage.path().join("withdrawn");
    rename_without_replacing(target, &withdrawn)?;
    if !matches_table_entry(published, &withdrawn) {
        // The entry changed between preflight and withdrawal. Return it
        // without replacing any newer arrival, or leave it in the stage.
        rename_without_replacing(&withdrawn, target)?;
        return Err(changed());
    }
    if replaced {
        rename_without_replacing(&stage.path().join("previous"), target)?;
    }
    Ok(())
}

fn close_stage(stage: TempDir, target: &Path) -> PyResult<()> {
    let recovery = stage.path().to_path_buf();
    stage.close().map_err(|error| {
        raised(format!(
            "Image was moved to {} but cleanup failed: {error}. Inspect {}",
            target.display(),
            recovery.display()
        ))
    })
}

fn restore_rename<T>(
    source: &Path,
    target: &Path,
    backup: Option<tempfile::TempDir>,
    error: PyErr,
) -> PyResult<T> {
    let Some(backup) = backup else {
        rename_without_replacing(target, source).map_err(|restore| {
            raised(format!(
                "{error}. Could not restore image {}: {restore}",
                source.display()
            ))
        })?;
        return Err(error);
    };
    let previous = backup.path().join("previous");
    let restore = exchange_directories(target, &previous)
        .and_then(|()| rename_without_replacing(&previous, source));
    if let Err(restore) = restore {
        let recovery = backup.keep();
        return Err(raised(format!(
            "{error}. Could not restore image names: {restore}. Inspect {}",
            recovery.display()
        )));
    }
    Err(error)
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn changed_source_is_returned_without_copying_it() {
        Python::initialize();
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let original = root.path().join("original");
        let target = root.path().join("target");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("original"), b"1").unwrap();
        let expected = source.symlink_metadata().unwrap();
        fs::rename(&source, &original).unwrap();
        fs::create_dir(&source).unwrap();
        fs::write(source.join("peer"), b"2").unwrap();

        let error = move_across_devices(&source, &expected, &target, None, |_| {
            panic!("a changed source must never be copied")
        })
        .err()
        .expect("the source changed");

        assert!(error.to_string().contains("source image entry"));
        assert!(!target.exists());
        assert_eq!(fs::read(original.join("original")).unwrap(), b"1");
        assert_eq!(fs::read(source.join("peer")).unwrap(), b"2");
    }

    #[test]
    fn rollback_preserves_a_concurrently_replaced_destination() {
        Python::initialize();
        for overwrite in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let target = root.path().join("target");
            let peer = root.path().join("peer");
            let relocated = root.path().join("relocated");
            fs::create_dir(&source).unwrap();
            fs::write(source.join("original"), b"1").unwrap();
            if overwrite {
                fs::create_dir(&target).unwrap();
                fs::write(target.join("previous"), b"2").unwrap();
            }
            let moved = move_across_devices(
                &source,
                &source.symlink_metadata().unwrap(),
                &target,
                target.symlink_metadata().ok().as_ref(),
                |_| Ok(()),
            )
            .unwrap();
            let NativeTableMove::Copied { target_stage, .. } = &moved else {
                unreachable!()
            };
            let previous = target_stage.path().join("previous");
            fs::create_dir(&peer).unwrap();
            fs::write(peer.join("peer"), b"3").unwrap();
            fs::rename(&target, &relocated).unwrap();
            fs::rename(&peer, &target).unwrap();
            let error = moved.check_target(&target).unwrap_err();
            assert!(moved.restore::<()>(&source, &target, error).is_err());

            assert_eq!(fs::read(source.join("original")).unwrap(), b"1");
            assert_eq!(fs::read(target.join("peer")).unwrap(), b"3");
            assert_eq!(fs::read(relocated.join("original")).unwrap(), b"1");
            if overwrite {
                assert_eq!(fs::read(previous.join("previous")).unwrap(), b"2");
            }
        }
    }
}
