// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::{TableDatContents, serialize_table_dat};
use crate::storage::{StorageError, TABLE_CONTROL_FILE};
use crate::table_path::stored_table_dat_keywords;

const STAGED_PREFIX: &str = "table.dat_tmp.";
const RANDOM_LEN: usize = 12;

pub(crate) fn is_staged_table_dat(name: &str) -> bool {
    name.strip_prefix(STAGED_PREFIX).is_some_and(|suffix| {
        suffix.len() == RANDOM_LEN && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

/// Reserve the publication file and confirm that it can carry the live
/// descriptor's access metadata before a caller writes any manager files.
pub(crate) struct StagedTableDat<'a> {
    table: &'a Path,
    destination: PathBuf,
    staged: tempfile::NamedTempFile,
    previous: Option<AccessMetadata>,
}

impl<'a> StagedTableDat<'a> {
    pub(crate) fn for_full_save(table: &'a Path) -> Result<Self, StorageError> {
        fs::create_dir_all(table)?;
        Self::in_table_directory(table)
    }

    pub(crate) fn in_table_directory(table: &'a Path) -> Result<Self, StorageError> {
        Self::prepare(table, table.join(TABLE_CONTROL_FILE), false)
    }

    pub(crate) fn for_existing_table(table: &'a Path) -> Result<Self, StorageError> {
        if !table.exists() {
            return Err(StorageError::MissingPath(table.to_path_buf()));
        }
        Self::prepare(table, table.join(TABLE_CONTROL_FILE), true)
    }

    pub(super) fn prepare(
        table: &'a Path,
        destination: PathBuf,
        require_existing: bool,
    ) -> Result<Self, StorageError> {
        let previous = existing_access(&destination)?;
        if require_existing && previous.is_none() {
            return Err(StorageError::MissingControlFile(destination));
        }
        let mut builder = tempfile::Builder::new();
        builder.prefix(STAGED_PREFIX).rand_bytes(RANDOM_LEN);
        #[cfg(unix)]
        if previous.is_none() {
            use std::os::unix::fs::PermissionsExt;
            // Match fs::write's creation mode, including the umask or default ACL.
            builder.permissions(fs::Permissions::from_mode(0o666));
        }
        let staged = builder.tempfile_in(table)?;
        if let Some(access) = &previous {
            access.apply(staged.as_file())?;
        }
        Ok(Self {
            table,
            destination,
            staged,
            previous,
        })
    }

    pub(crate) fn table_path(&self) -> &'a Path {
        self.table
    }

    pub(crate) fn publish(mut self, contents: &TableDatContents) -> Result<(), StorageError> {
        // Backpatch object lengths in memory and store table keywords relative
        // to the same parent directory used by the previous writer.
        let contents = stored_table_dat_keywords(contents, self.table);
        self.staged.write_all(&serialize_table_dat(&contents)?)?;
        if let Some(access) = &self.previous {
            // A write can clear special mode bits on some filesystems.
            access.apply(self.staged.as_file())?;
            if existing_access(&self.destination)?.as_ref() != Some(access) {
                return Err(
                    io::Error::other("table.dat access metadata changed during save").into(),
                );
            }
            self.staged
                .persist(&self.destination)
                .map_err(|e| e.error)?;
        } else {
            self.staged
                .persist_noclobber(&self.destination)
                .map_err(|e| e.error)?;
        }
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
struct AccessMetadata {
    uid: u32,
    gid: u32,
    mode: u32,
    acl: Option<Vec<u8>>,
}

fn existing_access(path: &Path) -> io::Result<Option<AccessMetadata>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected a regular table.dat",
        ));
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        use std::os::unix::fs::OpenOptionsExt;

        // Open without truncation to check write access. A metadata lookup
        // alone would allow rename to overwrite a read-only destination.
        let file = File::options()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        Ok(Some(AccessMetadata::read(&file)?))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "cannot preserve table.dat access metadata on this platform",
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl AccessMetadata {
    fn read(file: &File) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;

        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected a regular table.dat",
            ));
        }
        // A simultaneous replacement can leave an open inode with zero links.
        if metadata.nlink() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot replace a hard-linked table.dat",
            ));
        }
        Ok(Self {
            uid: metadata.uid(),
            gid: metadata.gid(),
            mode: metadata.mode() & 0o7777,
            acl: read_acl(file)?,
        })
    }

    fn apply(&self, file: &File) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::PermissionsExt;

        let staged = Self::read(file)?;
        if *self == staged {
            return Ok(());
        }
        if self.uid != staged.uid || self.gid != staged.gid {
            let uid = if self.uid == staged.uid {
                libc::uid_t::MAX
            } else {
                self.uid
            };
            let gid = if self.gid == staged.gid {
                libc::gid_t::MAX
            } else {
                self.gid
            };
            // SAFETY: file retains the open staging descriptor.
            if unsafe { libc::fchown(file.as_raw_fd(), uid, gid) } != 0 {
                let error = io::Error::last_os_error();
                return Err(io::Error::new(
                    error.kind(),
                    format!("cannot preserve table.dat owner and group: {error}"),
                ));
            }
        }
        file.set_permissions(fs::Permissions::from_mode(self.mode))?;
        #[cfg(target_os = "linux")]
        if self.acl.is_some() || staged.acl.is_some() {
            // Setting the mode can change the ACL mask. Apply the original
            // ACL after chmod, or remove an ACL inherited only by the stage.
            set_acl(file, self.acl.as_deref())?;
        }
        if *self != Self::read(file)? {
            return Err(io::Error::other(
                "could not preserve table.dat access metadata",
            ));
        }
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl AccessMetadata {
    fn apply(&self, _file: &File) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cannot preserve table.dat access metadata on this platform",
        ))
    }
}

#[cfg(target_os = "linux")]
fn read_acl(file: &File) -> io::Result<Option<Vec<u8>>> {
    use std::os::fd::AsRawFd;

    let fd = file.as_raw_fd();
    // SAFETY: fd and the buffer are live. An empty buffer queries the size.
    let names =
        read_xattr(|buf| unsafe { libc::flistxattr(fd, buf.as_mut_ptr().cast(), buf.len()) })?;
    let mut has_posix = false;
    for name in names
        .as_deref()
        .unwrap_or_default()
        .split(|byte| *byte == 0)
    {
        if name == b"system.posix_acl_access" {
            has_posix = true;
        } else if let Some(name) = name.strip_prefix(b"system.")
            && name.windows(3).any(|part| part == b"acl")
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cannot preserve a non-POSIX table.dat ACL",
            ));
        }
    }
    if !has_posix {
        return Ok(None);
    }
    // SAFETY: fd, the attribute name and the buffer are live for the call.
    read_xattr(|buf| unsafe {
        libc::fgetxattr(
            fd,
            c"system.posix_acl_access".as_ptr(),
            buf.as_mut_ptr().cast(),
            buf.len(),
        )
    })
}

#[cfg(target_os = "linux")]
fn read_xattr(read: impl Fn(&mut [u8]) -> isize) -> io::Result<Option<Vec<u8>>> {
    let mut small = [0u8; 256];
    let size = read(&mut small);
    if size >= 0 {
        return Ok(Some(small[..size as usize].to_vec()));
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ENODATA | libc::EOPNOTSUPP) => Ok(None),
        Some(libc::ERANGE) => {
            let size = read(&mut []);
            if size < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut bytes = vec![0; size as usize];
            let size = read(&mut bytes);
            if size < 0 {
                return Err(io::Error::last_os_error());
            }
            if size as usize > bytes.len() {
                return Err(io::Error::other(
                    "table.dat access metadata changed while reading it",
                ));
            }
            bytes.truncate(size as usize);
            Ok(Some(bytes))
        }
        _ => Err(error),
    }
}

#[cfg(target_os = "linux")]
fn set_acl(file: &File, acl: Option<&[u8]>) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let fd = file.as_raw_fd();
    let name = c"system.posix_acl_access";
    // SAFETY: the staging file, name and optional value are live.
    let status = unsafe {
        match acl {
            Some(acl) => libc::fsetxattr(fd, name.as_ptr(), acl.as_ptr().cast(), acl.len(), 0),
            None => libc::fremovexattr(fd, name.as_ptr()),
        }
    };
    if status == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if acl.is_none() && error.raw_os_error() == Some(libc::ENODATA) {
        return Ok(());
    }
    Err(error)
}

#[cfg(target_os = "macos")]
fn read_acl(file: &File) -> io::Result<Option<Vec<u8>>> {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn acl_get_fd(fd: libc::c_int) -> *mut libc::c_void;
        fn acl_free(acl: *mut libc::c_void) -> libc::c_int;
    }
    // SAFETY: acl_get_fd reads the extended ACL of this live descriptor.
    let acl = unsafe { acl_get_fd(file.as_raw_fd()) };
    if !acl.is_null() {
        // SAFETY: a successful acl_get_fd allocates this ACL for the caller.
        unsafe { acl_free(acl) };
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cannot preserve a macOS extended table.dat ACL",
        ));
    }
    let error = io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(libc::ENOENT | libc::ENOTSUP | libc::EOPNOTSUPP)
    ) {
        Ok(None)
    } else {
        Err(error)
    }
}
