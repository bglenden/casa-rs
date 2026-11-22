// SPDX-License-Identifier: LGPL-3.0-or-later

//! Publication and rollback for moving complete image directories.

use std::fs::Metadata;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_tables::publication::{
    check_write_access, copy_locked_external_tree, copy_locked_table_tree, exchange_directories,
    rename_without_replacing,
};
use casa_tables::{TableUseLock, TableWriteLock};
use pyo3::prelude::*;
use tempfile::TempDir;

use super::handle::{raised, same_file_identity};
use super::lifecycle::rename_destination;

pub(super) fn native_destination(expanded: &str, name: &str) -> PyResult<PathBuf> {
    let absolute = rename_destination(expanded, name)?;
    let parent = absolute
        .parent()
        .ok_or_else(|| raised("Invalid output image path"))?;
    let directory = parent.canonicalize().map_err(|_| {
        let argument = Path::new(expanded).parent().unwrap_or(Path::new("."));
        raised(format!(
            "RegularFile::copy/move: target directory {} is not writable",
            argument.display()
        ))
    })?;
    Ok(directory.join(
        absolute
            .file_name()
            .ok_or_else(|| raised("Invalid output image name"))?,
    ))
}

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
    Renamed {
        backup: Option<(TempDir, &'static str)>,
        identity: Box<Metadata>,
    },
    Copied {
        source_stage: TempDir,
        target_stage: TempDir,
        replaced: bool,
        published: Box<CopiedDirectory>,
    },
}

pub(super) struct CopiedDirectory {
    identity: Metadata,
    use_lock: Option<Arc<TableUseLock>>,
    write_lock: Option<TableWriteLock>,
    #[cfg(unix)]
    _directory: Option<std::fs::File>,
}

impl CopiedDirectory {
    fn matches(&self, path: &Path) -> bool {
        path.symlink_metadata().is_ok_and(|actual| {
            if let Some(lease) = &self.use_lock {
                lease.matches_directory_metadata(&actual)
            } else {
                same_file_identity(&self.identity, &actual)
            }
        })
    }
}

#[derive(Clone, Copy)]
enum CopyKind {
    NativeTable,
    ReadOnlyDirectory,
    #[cfg(unix)]
    Symlink,
}

impl NativeTableMove {
    pub(super) fn release_copy_write_lock(&mut self) {
        if let Self::Copied { published, .. } = self {
            published.write_lock = None;
        }
    }

    pub(super) fn use_lock(&self) -> Option<Arc<TableUseLock>> {
        match self {
            Self::Renamed { .. } => None,
            Self::Copied { published, .. } => published.use_lock.clone(),
        }
    }

    pub(super) fn check_target(&self, target: &Path) -> PyResult<()> {
        let matches = match self {
            Self::Renamed { identity, .. } => matches_entry(identity, target),
            Self::Copied { published, .. } => published.matches(target),
        };
        if matches {
            Ok(())
        } else {
            Err(changed_target(target))
        }
    }

    pub(super) fn finish(self, target: &Path) -> PyResult<()> {
        if let Self::Copied {
            source_stage,
            target_stage,
            published,
            ..
        } = self
        {
            let source = close_stage(source_stage, target);
            let destination = close_stage(target_stage, target);
            drop(published);
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
            Self::Renamed { backup, identity } => {
                restore_rename(source, target, backup, &identity, error)
            }
            Self::Copied {
                source_stage,
                target_stage,
                replaced,
                published,
                ..
            } => {
                let target_result =
                    restore_copied_target(target, &target_stage, replaced, &published);
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
    move_image_directory(
        source,
        source_metadata,
        target,
        target_metadata,
        CopyKind::NativeTable,
        prepare_copy,
    )
}

#[cfg(unix)]
pub(super) fn move_symlink(
    source: &Path,
    metadata: &Metadata,
    target: &Path,
) -> PyResult<NativeTableMove> {
    move_image_directory(
        source,
        metadata,
        target,
        None,
        CopyKind::Symlink,
        |_| Ok(()),
    )
}

pub(super) fn move_read_only_directory(
    source: &Path,
    source_metadata: &Metadata,
    target: &Path,
    target_metadata: Option<&Metadata>,
    check_copy: impl FnOnce(&Path) -> PyResult<()>,
) -> PyResult<NativeTableMove> {
    if check_write_access(source).is_err() {
        // Linux requires write access to a moved directory when its parent
        // changes. Claim this root at a sibling name instead.
        return move_with_read_only_root(source, source_metadata, target, target_metadata);
    }
    let source_stage = tempdir_beside(source)?;
    let claimed = source_stage.path().join("source");
    rename_without_replacing(source, &claimed).map_err(raised)?;
    if !matches_entry(source_metadata, &claimed) {
        return restore_claim(
            source,
            source_stage,
            raised(format!(
                "The source image entry at {} has changed",
                source.display()
            )),
        );
    }
    let moved = if target_metadata.is_some() {
        exchange_directories(&claimed, target)
    } else {
        rename_without_replacing(&claimed, target)
    };
    match moved {
        Ok(()) => {
            let destination_changed =
                target_metadata.is_some_and(|expected| !matches_entry(expected, &claimed));
            let moved = NativeTableMove::Renamed {
                backup: target_metadata.map(|_| (source_stage, "source")),
                identity: Box::new(source_metadata.clone()),
            };
            if destination_changed {
                moved.restore(source, target, changed_target(target))
            } else {
                Ok(moved)
            }
        }
        Err(error) if error.kind() == ErrorKind::CrossesDevices => copy_claimed_directory(
            source,
            source_metadata,
            target,
            target_metadata,
            source_stage,
            CopyKind::ReadOnlyDirectory,
            check_copy,
        ),
        Err(error) => restore_claim(source, source_stage, raised(error)),
    }
}

fn move_with_read_only_root(
    source: &Path,
    identity: &Metadata,
    target: &Path,
    target_metadata: Option<&Metadata>,
) -> PyResult<NativeTableMove> {
    let claimed = sibling_slot(source)?;
    let backup = target_metadata
        .map(|_| tempdir_beside(source))
        .transpose()?;
    rename_without_replacing(source, &claimed).map_err(raised)?;
    let restore = |error| restore_named_claim(source, &claimed, error);
    if !matches_entry(identity, &claimed) {
        return restore(raised(format!(
            "The source image entry at {} has changed",
            source.display()
        )));
    }
    let moved = match &backup {
        Some(_) => exchange_directories(&claimed, target),
        None => rename_without_replacing(&claimed, target),
    };
    if let Err(error) = moved {
        let error = if error.kind() == ErrorKind::CrossesDevices {
            raised(format!(
                "Cannot move MIRIAD image {} across filesystems: the source directory is not writable",
                source.display()
            ))
        } else {
            raised(error)
        };
        return restore(error);
    }
    let changed = target_metadata.is_some_and(|expected| !matches_entry(expected, &claimed));
    let backup = if let Some(backup) = backup {
        let previous = backup.path().join("previous");
        if let Err(error) = rename_without_replacing(&claimed, &previous) {
            return Err(raised(format!(
                "Image moved to {} but the previous entry could not be staged: {error}. Inspect {}",
                target.display(),
                claimed.display()
            )));
        }
        Some((backup, "previous"))
    } else {
        None
    };
    let moved = NativeTableMove::Renamed {
        backup,
        identity: Box::new(identity.clone()),
    };
    if changed {
        moved.restore(source, target, changed_target(target))
    } else {
        Ok(moved)
    }
}

fn move_image_directory(
    source: &Path,
    source_metadata: &Metadata,
    target: &Path,
    target_metadata: Option<&Metadata>,
    kind: CopyKind,
    prepare_copy: impl FnOnce(&Path) -> PyResult<()>,
) -> PyResult<NativeTableMove> {
    let Some(expected) = target_metadata else {
        return match rename_without_replacing(source, target) {
            Ok(()) => Ok(NativeTableMove::Renamed {
                backup: None,
                identity: Box::new(source_metadata.clone()),
            }),
            Err(error) if error.kind() == ErrorKind::CrossesDevices => {
                move_across_devices(source, source_metadata, target, None, kind, prepare_copy)
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
                kind,
                prepare_copy,
            )
        } else {
            Err(raised(error))
        };
    }
    let previous = backup.path().join("previous");
    if let Err(error) = rename_without_replacing(source, &previous) {
        let restore = if matches_entry(expected, source) && matches_entry(source_metadata, target) {
            exchange_directories(source, target)
        } else {
            Err(io::Error::other(
                "an image entry changed after the exchange",
            ))
        };
        if let Err(restore) = restore {
            return Err(raised(format!(
                "Could not complete image rename ({error}) or restore by exchanging {} and {} ({restore})",
                source.display(),
                target.display()
            )));
        }
        return Err(raised(error));
    }
    let moved = NativeTableMove::Renamed {
        backup: Some((backup, "previous")),
        identity: Box::new(source_metadata.clone()),
    };
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

fn sibling_slot(path: &Path) -> PyResult<PathBuf> {
    let reservation = tempdir_beside(path)?;
    let slot = reservation.path().to_path_buf();
    reservation.close().map_err(raised)?;
    Ok(slot)
}

fn matches_entry(expected: &Metadata, path: &Path) -> bool {
    path.symlink_metadata()
        .is_ok_and(|actual| same_file_identity(expected, &actual))
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
    kind: CopyKind,
    prepare_copy: impl FnOnce(&Path) -> PyResult<()>,
) -> PyResult<NativeTableMove> {
    let source_stage = tempdir_beside(source)?;
    rename_without_replacing(source, &source_stage.path().join("source")).map_err(raised)?;
    copy_claimed_directory(
        source,
        source_metadata,
        target,
        target_metadata,
        source_stage,
        kind,
        prepare_copy,
    )
}

fn copy_claimed_directory(
    source: &Path,
    source_metadata: &Metadata,
    target: &Path,
    target_metadata: Option<&Metadata>,
    source_stage: TempDir,
    kind: CopyKind,
    prepare_copy: impl FnOnce(&Path) -> PyResult<()>,
) -> PyResult<NativeTableMove> {
    let target_stage = match tempdir_beside(target) {
        Ok(stage) => stage,
        Err(error) => return restore_claim(source, source_stage, error),
    };
    let claimed = source_stage.path().join("source");
    let copied = target_stage.path().join("previous");

    let install = || {
        if !matches_entry(source_metadata, &claimed) {
            return Err(raised(format!(
                "The source image entry at {} has changed",
                source.display()
            )));
        }
        if matches!(kind, CopyKind::ReadOnlyDirectory | CopyKind::NativeTable) {
            check_tree_removal_access(&claimed).map_err(|error| {
                let description = match kind {
                    CopyKind::ReadOnlyDirectory => "MIRIAD image",
                    _ => "native image",
                };
                raised(format!(
                    "Cannot move {description} {} across filesystems: {error}",
                    source.display()
                ))
            })?;
        }
        match kind {
            #[cfg(unix)]
            CopyKind::Symlink => {
                std::os::unix::fs::symlink(std::fs::read_link(&claimed).map_err(raised)?, &copied)
                    .map_err(raised)?
            }
            CopyKind::ReadOnlyDirectory => {
                copy_locked_external_tree(&claimed, &copied, |_| false).map_err(raised)?
            }
            CopyKind::NativeTable => copy_locked_table_tree(&claimed, &copied).map_err(raised)?,
        }
        prepare_copy(&copied)?;
        let (use_lock, write_lock) = match kind {
            CopyKind::NativeTable => (
                Some(Arc::new(TableUseLock::acquire(&copied).map_err(raised)?)),
                Some(TableWriteLock::acquire(&copied, 1).map_err(raised)?),
            ),
            _ => (None, None),
        };
        let published = CopiedDirectory {
            identity: copied.symlink_metadata().map_err(raised)?,
            use_lock,
            write_lock,
            #[cfg(unix)]
            _directory: matches!(kind, CopyKind::ReadOnlyDirectory)
                .then(|| casa_tables::publication::open_directory_identity(&copied))
                .transpose()
                .map_err(raised)?,
        };
        if target_metadata.is_some() {
            exchange_directories(&copied, target).map_err(raised)?;
        } else {
            rename_without_replacing(&copied, target).map_err(raised)?;
        }
        Ok::<_, PyErr>(published)
    };
    let published = match install() {
        Ok(locks) => locks,
        Err(error) => return restore_claim(source, source_stage, error),
    };
    let moved = NativeTableMove::Copied {
        source_stage,
        target_stage,
        replaced: target_metadata.is_some(),
        published: Box::new(published),
    };
    if target_metadata.is_some_and(|expected| !matches_entry(expected, &copied)) {
        return moved.restore(source, target, changed_target(target));
    }
    Ok(moved)
}

fn restore_claim<T>(source: &Path, stage: TempDir, error: PyErr) -> PyResult<T> {
    if let Err(restore) = rename_without_replacing(&stage.path().join("source"), source) {
        let recovery = stage.keep();
        return Err(raised(format!(
            "{error}. Could not restore image {}: {restore}. Inspect {}",
            source.display(),
            recovery.display()
        )));
    }
    Err(error)
}

fn restore_named_claim<T>(source: &Path, claimed: &Path, error: PyErr) -> PyResult<T> {
    if let Err(restore) = rename_without_replacing(claimed, source) {
        return Err(raised(format!(
            "{error}. Could not restore image {}: {restore}. Inspect {}",
            source.display(),
            claimed.display()
        )));
    }
    Err(error)
}

fn restore_copied_target(
    target: &Path,
    stage: &TempDir,
    replaced: bool,
    published: &CopiedDirectory,
) -> io::Result<()> {
    withdraw_moved_entry(target, &stage.path().join("withdrawn"), |path| {
        published.matches(path)
    })?;
    if replaced {
        rename_without_replacing(&stage.path().join("previous"), target)?;
    }
    Ok(())
}

fn withdraw_moved_entry(
    target: &Path,
    withdrawn: &Path,
    matches: impl Fn(&Path) -> bool,
) -> io::Result<()> {
    let changed = || {
        io::Error::other(format!(
            "The destination image entry at {} has changed",
            target.display()
        ))
    };
    if !matches(target) {
        return Err(changed());
    }
    rename_without_replacing(target, withdrawn)?;
    if !matches(withdrawn) {
        // The entry changed between preflight and withdrawal. Return it
        // without replacing any newer arrival, or leave it in the stage.
        rename_without_replacing(withdrawn, target)?;
        return Err(changed());
    }
    Ok(())
}

fn check_tree_removal_access(path: &Path) -> io::Result<()> {
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries = directory.read_dir()?.peekable();
        if entries.peek().is_some() {
            check_write_access(&directory)?;
        }
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            }
        }
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
    backup: Option<(TempDir, &'static str)>,
    identity: &Metadata,
    error: PyErr,
) -> PyResult<T> {
    let (stage, previous) = match backup {
        Some((stage, previous)) => (stage, Some(previous)),
        None => (
            tempdir_beside(source).map_err(|restore| {
                raised(format!(
                    "{error}. Could not stage rollback for {}: {restore}",
                    target.display()
                ))
            })?,
            None,
        ),
    };
    let withdrawn = match sibling_slot(target) {
        Ok(path) => path,
        Err(restore) => {
            let recovery = stage.keep();
            return Err(raised(format!(
                "{error}. Could not stage rollback: {restore}. Inspect {}",
                recovery.display()
            )));
        }
    };
    let restore = (|| {
        withdraw_moved_entry(target, &withdrawn, |path| matches_entry(identity, path))?;
        rename_without_replacing(&withdrawn, source)?;
        if let Some(previous) = previous {
            rename_without_replacing(&stage.path().join(previous), target)?;
        }
        Ok::<(), io::Error>(())
    })();
    if let Err(restore) = restore {
        let recovery = stage.keep();
        return Err(raised(format!(
            "{error}. Could not restore image names: {restore}. Inspect {} and {}",
            recovery.display(),
            withdrawn.display()
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

        let error = move_across_devices(
            &source,
            &expected,
            &target,
            None,
            CopyKind::NativeTable,
            |_| panic!("a changed source must never be copied"),
        )
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
                CopyKind::NativeTable,
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

    #[test]
    fn same_device_rollback_never_withdraws_a_concurrent_replacement() {
        Python::initialize();
        for overwrite in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let target = root.path().join("target");
            let relocated = root.path().join("relocated");
            fs::create_dir(&source).unwrap();
            fs::write(source.join("original"), b"1").unwrap();
            if overwrite {
                fs::create_dir(&target).unwrap();
                fs::write(target.join("previous"), b"2").unwrap();
            }
            let moved = move_read_only_directory(
                &source,
                &source.symlink_metadata().unwrap(),
                &target,
                target.symlink_metadata().ok().as_ref(),
                |_| panic!("this move stays on one filesystem"),
            )
            .unwrap();
            let NativeTableMove::Renamed { backup, .. } = &moved else {
                unreachable!()
            };
            let previous = backup
                .as_ref()
                .map(|(stage, entry)| stage.path().join(entry));
            fs::rename(&target, &relocated).unwrap();
            fs::create_dir(&target).unwrap();
            fs::write(target.join("peer"), b"3").unwrap();
            let error = moved.check_target(&target).unwrap_err();
            assert!(moved.restore::<()>(&source, &target, error).is_err());
            assert!(!source.exists());
            assert_eq!(fs::read(target.join("peer")).unwrap(), b"3");
            assert_eq!(fs::read(relocated.join("original")).unwrap(), b"1");
            if let Some(previous) = previous {
                assert_eq!(fs::read(previous.join("previous")).unwrap(), b"2");
            }
        }
    }
}
