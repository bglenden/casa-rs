// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::OsStr;
use std::fs::{self, Metadata};
use std::io::{self, ErrorKind};
use std::path::Path;

#[cfg(unix)]
use crate::lock::{LOCK_FILE_NAME, LockFile, LockOptions, read_sync_data_from_table_dir};

/// Copy a locked casacore table and its physical subtables into a private,
/// absent destination tree. Hold the source locks for the duration of the copy.
///
/// A destination table gets a fresh request list and the source's persisted
/// synchronization data. Symbolic links are copied as directory entries.
///
/// # Errors
/// Returns an error for unreadable, unsupported or uncopyable entries.
/// After failure the caller owns any partial destination.
pub fn copy_locked_table_tree(source: &Path, target: &Path) -> io::Result<()> {
    copy_tree(source, target, None, None)
}

/// Stage a locked table for replacement, sharing selected regular files at
/// its root that the staged operation will leave unchanged.
///
/// The original tree must stay locked until publication or removal of the
/// stage. The caller must select only data payloads that will retain their
/// bytes and metadata throughout staging. When a link cannot be created,
/// copy the file instead. Subtables and table locks are copied independently.
///
/// # Errors
/// Returns an error for unreadable, unsupported or uncopyable entries.
/// After failure the caller owns any partial destination.
pub fn copy_locked_table_tree_with_shared_files(
    source: &Path,
    target: &Path,
    share_file: impl Fn(&OsStr) -> bool,
) -> io::Result<()> {
    copy_tree(source, target, None, Some(&share_file))
}

/// Copy a non-table directory, retaining the table copy rules for physical
/// subtables. Copy the referents of regular-file links selected at the root.
///
/// # Errors
/// Returns an error for unreadable, unsupported or uncopyable entries.
/// After failure the caller owns any partial destination.
pub fn copy_locked_external_tree(
    source: &Path,
    target: &Path,
    follow_file_link: impl Fn(&OsStr) -> bool,
) -> io::Result<()> {
    copy_tree(source, target, Some(&follow_file_link), None)
}

type FileLinkSelection<'a> = Option<&'a dyn Fn(&OsStr) -> bool>;

fn copy_tree(
    source: &Path,
    target: &Path,
    external_root: FileLinkSelection<'_>,
    shared_root_files: FileLinkSelection<'_>,
) -> io::Result<()> {
    let metadata = source.symlink_metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "Expected a table directory",
        ));
    }
    copy_directory(source, target, &metadata, external_root, shared_root_files)
}

fn copy_directory(
    source: &Path,
    target: &Path,
    metadata: &Metadata,
    external_root: FileLinkSelection<'_>,
    shared_root_files: FileLinkSelection<'_>,
) -> io::Result<()> {
    fs::create_dir(target)?;
    #[cfg(unix)]
    let is_table =
        external_root.is_none() && source.join(crate::storage::TABLE_CONTROL_FILE).is_file();
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        #[cfg(unix)]
        if is_table && entry.file_name() == LOCK_FILE_NAME {
            copy_table_lock(source, target)?;
            continue;
        }
        let path = entry.path();
        let output = target.join(entry.file_name());
        if shared_root_files.is_some_and(|share| share(&entry.file_name()))
            && entry.file_type()?.is_file()
            && fs::hard_link(&path, &output).is_ok()
        {
            continue;
        }
        if external_root.is_some_and(|follow| follow(&entry.file_name()))
            && entry.file_type()?.is_symlink()
            && path.is_file()
        {
            copy_entry(&path.canonicalize()?, &output)?;
        } else {
            copy_entry(&path, &output)?;
        }
    }
    fs::set_permissions(target, metadata.permissions())
}

#[cfg(unix)]
fn copy_table_lock(source: &Path, target: &Path) -> io::Result<()> {
    // Opening and closing a second descriptor on a POSIX lock inode releases
    // this process's locks. The reader uses the registered descriptor instead.
    let sync = read_sync_data_from_table_dir(source)?;
    let lock = LockFile::create_or_open(
        target,
        true,
        LockOptions::default().inspection_interval,
        false,
    )?;
    if let Some(sync) = sync {
        lock.write_sync_data(&sync)?;
    }
    fs::set_permissions(
        target.join(LOCK_FILE_NAME),
        source.join(LOCK_FILE_NAME).metadata()?.permissions(),
    )
}

fn copy_entry(source: &Path, target: &Path) -> io::Result<()> {
    let metadata = source.symlink_metadata()?;
    if metadata.is_symlink() {
        #[cfg(unix)]
        return std::os::unix::fs::symlink(source.read_link()?, target);
        #[cfg(not(unix))]
        return Err(io::Error::new(
            ErrorKind::Unsupported,
            "Copying symbolic links is unsupported",
        ));
    }
    if metadata.is_dir() {
        return copy_directory(source, target, &metadata, None, None);
    }
    if metadata.is_file() {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.blocks() < metadata.len() / 512 && copy_sparse(source, target, &metadata)? {
                return Ok(());
            }
        }
        return fs::copy(source, target).map(|_| ());
    }
    Err(io::Error::new(
        ErrorKind::Unsupported,
        format!("Cannot copy table entry {}", source.display()),
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn copy_sparse(source: &Path, target: &Path, metadata: &Metadata) -> io::Result<bool> {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    let mut input = File::open(source)?;
    let mut offset = 0;
    let mut data = next_extent(&input, offset, libc::SEEK_DATA);
    if matches!(&data, Err(error) if matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOTSUP)))
    {
        return Ok(false);
    }
    let mut output = File::create_new(target)?;
    #[cfg(target_os = "macos")]
    let block_size = destination_block_size(&output)?;
    while let Some(start) = data? {
        let end = next_extent(&input, start, libc::SEEK_HOLE)?.unwrap_or(metadata.len());
        input.seek(SeekFrom::Start(start))?;
        output.seek(SeekFrom::Start(start))?;
        if io::copy(&mut (&mut input).take(end - start), &mut output)? != end - start {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "Table extent changed during copy",
            ));
        }
        #[cfg(target_os = "macos")]
        preserve_sparse_gap(&output, offset, start, block_size)?;
        offset = end;
        data = next_extent(&input, offset, libc::SEEK_DATA);
    }
    output.set_len(metadata.len())?;
    output.set_permissions(metadata.permissions())?;
    Ok(true)
}

#[cfg(target_os = "macos")]
fn destination_block_size(file: &std::fs::File) -> io::Result<u64> {
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd;

    let mut stats = MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: fstatfs writes the complete struct on success and the fd is live.
    let block_size = unsafe {
        if libc::fstatfs(file.as_raw_fd(), stats.as_mut_ptr()) != 0 {
            return Err(io::Error::last_os_error());
        }
        u64::from(stats.assume_init().f_bsize)
    };
    if block_size == 0 {
        return Err(io::Error::other("Destination filesystem has no block size"));
    }
    Ok(block_size)
}

#[cfg(target_os = "macos")]
fn preserve_sparse_gap(
    file: &std::fs::File,
    start: u64,
    end: u64,
    block_size: u64,
) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let start = start.next_multiple_of(block_size);
    let end = end / block_size * block_size;
    if start >= end {
        return Ok(());
    }
    // APFS can allocate skipped ranges when the copied file is closed.
    // Punch only ranges that the source filesystem reported as holes.
    let gap = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: start as libc::off_t,
        fp_length: (end - start) as libc::off_t,
    };
    // SAFETY: the descriptor is live and the argument uses Darwin's ABI.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &gap) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(libc::EINVAL | libc::ENOTSUP | libc::EOPNOTSUPP | libc::ENOTTY)
    ) {
        return Ok(());
    }
    Err(error)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn next_extent(file: &std::fs::File, offset: u64, whence: libc::c_int) -> io::Result<Option<u64>> {
    use std::os::fd::AsRawFd;

    // SAFETY: this is a live file descriptor and does not outlive the call.
    let result = unsafe { libc::lseek(file.as_raw_fd(), offset as libc::off_t, whence) };
    if result >= 0 {
        return Ok(Some(result as u64));
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ENXIO) {
        return Ok(None);
    }
    Err(error)
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;

    use crate::lock::{LockOutcome, LockType, SyncData};

    use super::*;

    const SOURCE_PROBE: &str = "CASA_TABLE_COPY_TEST_SOURCE";

    #[test]
    fn stage_shares_unchanged_root_payloads_only() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let stage = root.path().join("stage");
        let payload = "table.f2_TSM1";
        fs::create_dir_all(source.join("SUBTABLE")).unwrap();
        fs::write(source.join(payload), b"visibility").unwrap();
        fs::write(source.join("SUBTABLE").join(payload), b"subtable").unwrap();
        fs::write(source.join("table.dat"), b"original").unwrap();
        let mut guard = open_lock(&source);
        assert!(guard.acquire(LockType::Write, 1).unwrap().is_acquired());

        copy_locked_table_tree_with_shared_files(&source, &stage, |name| name == payload).unwrap();
        let inode = |path: &Path, file| fs::metadata(path.join(file)).unwrap().ino();
        assert_eq!(inode(&source, payload), inode(&stage, payload));
        assert_ne!(inode(&source, "table.lock"), inode(&stage, "table.lock"));
        assert_ne!(
            inode(&source.join("SUBTABLE"), payload),
            inode(&stage.join("SUBTABLE"), payload)
        );
        fs::write(stage.join("table.dat"), b"reframed").unwrap();
        assert_eq!(fs::read(source.join("table.dat")).unwrap(), b"original");
        fs::remove_dir_all(stage).unwrap();
        assert_eq!(fs::read(source.join(payload)).unwrap(), b"visibility");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn sparse_gap_rounds_inward_without_punching_adjacent_data() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("destination");
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let block = destination_block_size(&file).unwrap() * 4;
        let mut expected = vec![0; (block * 5) as usize];
        expected[block as usize] = 0x9b;
        expected[(4 * block - 1) as usize] = 0xd2;
        fs::write(&path, &expected).unwrap();

        preserve_sparse_gap(&file, block + 1, 4 * block - 1, block).unwrap();
        assert_eq!(fs::read(&path).unwrap(), expected);
        assert_eq!(
            next_extent(&file, 2 * block, libc::SEEK_HOLE).unwrap(),
            Some(2 * block)
        );
    }

    #[test]
    fn copy_preserves_source_locks_and_subtable_sync_data() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("image");
        let target = root.path().join("copy");
        fs::create_dir_all(source.join("logtable")).unwrap();
        let mut guards = Vec::new();
        for (subdir, rows) in [("", 1), ("logtable", 7)] {
            let table = source.join(subdir);
            fs::write(table.join("table.dat"), [0u8]).unwrap();
            let mut guard = open_lock(&table);
            assert!(guard.acquire(LockType::Write, 1).unwrap().is_acquired());
            let mut sync = SyncData::new();
            sync.record_write(rows, 1, true, &[true]);
            guard.write_sync_data(&sync).unwrap();
            guards.push(guard);
        }

        copy_locked_table_tree(&source, &target).unwrap();
        for (subdir, rows) in [("", 1), ("logtable", 7)] {
            let sync = read_sync_data_from_table_dir(&target.join(subdir))
                .unwrap()
                .unwrap();
            assert_eq!(sync.nrrow, rows);
        }
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "publication::copy_tree::tests::source_lock_probe",
            ])
            .env(SOURCE_PROBE, &source)
            .output()
            .unwrap();
        let output = String::from_utf8_lossy(&child.stdout);
        assert!(
            child.status.success() && output.contains("1 passed"),
            "{output}"
        );
        drop(guards);
    }

    #[test]
    fn source_lock_probe() {
        let Some(path) = std::env::var_os(SOURCE_PROBE) else {
            return;
        };
        for subdir in ["", "logtable"] {
            let mut lock = open_lock(&Path::new(&path).join(subdir));
            assert_eq!(
                lock.acquire(LockType::Write, 1).unwrap(),
                LockOutcome::HeldByAnotherProcess,
            );
        }
    }

    fn open_lock(table: &Path) -> LockFile {
        LockFile::create_or_open(table, false, 0., false).unwrap()
    }
}
