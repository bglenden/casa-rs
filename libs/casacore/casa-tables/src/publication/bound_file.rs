// SPDX-License-Identifier: LGPL-3.0-or-later

use std::ffi::{CStr, CString, OsStr};
use std::fs::{File, Metadata};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

fn entry_name(name: &OsStr) -> io::Result<CString> {
    if Path::new(name).file_name() != Some(name) {
        return Err(io::Error::other("expected a directory entry name"));
    }
    CString::new(name.as_bytes()).map_err(|_| io::Error::other("entry name contains NUL"))
}

fn open_entry(directory: &File, entry: &CStr, flags: i32) -> io::Result<File> {
    // The variadic mode argument uses the C integer promotion of mode_t.
    let mode: libc::c_uint = 0o600;
    // SAFETY: the descriptor and entry name stay valid. The mode argument
    // is used only when creating a new private staging file.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            entry.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            mode,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: this descriptor was opened above and has no other owner.
    let file = unsafe { File::from_raw_fd(fd) };
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("expected a regular directory entry"));
    }
    Ok(file)
}

/// Open a regular file in an already opened directory without following a link.
///
/// # Errors
/// Returns an I/O error for an invalid name, unreadable entry, or non-file.
pub fn open_regular_directory_entry(directory: &File, name: &OsStr) -> io::Result<File> {
    open_entry(directory, &entry_name(name)?, libc::O_RDONLY)
}

/// Keep a symbolic link's inode alive without opening its referent.
/// # Errors
/// Returns an I/O error for an invalid name, unreadable entry, or non-link.
pub fn open_symlink_directory_entry(directory: &File, name: &OsStr) -> io::Result<File> {
    let entry = entry_name(name)?;
    #[cfg(target_os = "linux")]
    let flags = libc::O_PATH | libc::O_NOFOLLOW;
    #[cfg(target_os = "macos")]
    let flags = libc::O_SYMLINK | libc::O_NONBLOCK;
    // SAFETY: the directory descriptor and C string remain valid for openat.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            entry.as_ptr(),
            flags | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: this descriptor was opened above and has no other owner.
    let file = unsafe { File::from_raw_fd(fd) };
    if !file.metadata()?.is_symlink() {
        return Err(io::Error::other("expected a symbolic link directory entry"));
    }
    Ok(file)
}

/// Stage a file and publish or clean it up relative to an open directory.
pub struct StagedDirectoryFile<'a> {
    directory: &'a File,
    name: CString,
    file: File,
    cleanup: Option<Metadata>,
}

impl<'a> StagedDirectoryFile<'a> {
    /// Make a private file in the captured directory.
    ///
    /// `directory_name` supplies the diagnostic path for a generated name.
    /// Creation itself uses the descriptor.
    ///
    /// # Errors
    /// Returns an I/O error if the directory or a new file cannot be opened.
    pub fn new(directory: &'a File, directory_name: &Path) -> io::Result<Self> {
        let generated = tempfile::Builder::new()
            .prefix(".imageexpr-")
            .disable_cleanup(true)
            .make_in(directory_name, |path| {
                let entry = entry_name(path.file_name().expect("generated staging name"))?;
                open_entry(
                    directory,
                    &entry,
                    libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                )
            })?;
        let name = entry_name(
            generated
                .path()
                .file_name()
                .expect("generated staging name"),
        )?;
        let (file, _) = generated.into_parts();
        let cleanup = Some(file.metadata()?);
        Ok(Self {
            directory,
            name,
            file,
            cleanup,
        })
    }

    /// The writable staging file.
    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Exchange with the expected target and retain the published file handle.
    ///
    /// If the old target changed, restore it. If another change prevents safe
    /// restoration, retain the displaced file at the staging name for recovery.
    ///
    /// # Errors
    /// Returns an I/O error if identities changed or replacement failed.
    pub fn replace(&mut self, target: &OsStr, expected: &Metadata) -> io::Result<File> {
        let target = entry_name(target)?;
        let published = self.file.try_clone()?;
        let staged = published.metadata()?;
        if !self.matches(&self.name, &staged) {
            return Err(io::Error::other("staged file changed before publication"));
        }
        let exchange = || super::exchange_paths_at(self.directory.as_raw_fd(), &self.name, &target);
        exchange()?;
        let previous = self.metadata(&self.name);
        if previous.as_ref().is_ok_and(|current| {
            same_identity(expected, current)
                && expected.len() == current.len()
                && expected.modified().ok() == current.modified().ok()
        }) {
            self.cleanup = previous.ok();
            return Ok(published);
        }
        if self.matches(&target, &staged) && exchange().is_ok() && self.matches(&self.name, &staged)
        {
            return Err(io::Error::other("target entry changed during publication"));
        }
        self.cleanup = None;
        Err(io::Error::other(format!(
            "target entry changed during publication, recovery entry {} retained",
            self.name.to_string_lossy()
        )))
    }

    fn metadata(&self, entry: &CStr) -> io::Result<Metadata> {
        open_entry(self.directory, entry, libc::O_RDONLY)?.metadata()
    }

    fn matches(&self, entry: &CStr, expected: &Metadata) -> bool {
        self.metadata(entry)
            .is_ok_and(|current| same_identity(expected, &current))
    }
}

impl Drop for StagedDirectoryFile<'_> {
    fn drop(&mut self) {
        if self
            .cleanup
            .as_ref()
            .is_some_and(|expected| self.matches(&self.name, expected))
        {
            // SAFETY: the descriptor and entry name stay valid during cleanup.
            unsafe { libc::unlinkat(self.directory.as_raw_fd(), self.name.as_ptr(), 0) };
        }
    }
}

fn same_identity(left: &Metadata, right: &Metadata) -> bool {
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use super::*;

    #[test]
    fn a_pinned_link_keeps_its_identity_after_unlink() -> io::Result<()> {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir()?;
        let directory = super::super::open_directory_identity(root.path())?;
        let entry = root.path().join("link");
        symlink("referent", &entry)?;
        let pinned = open_symlink_directory_entry(&directory, OsStr::new("link"))?;
        let before = entry.symlink_metadata()?;
        assert!(same_identity(&pinned.metadata()?, &before));
        fs::remove_file(&entry)?;
        symlink("referent", &entry)?;
        assert!(same_identity(&pinned.metadata()?, &before));
        assert!(!same_identity(
            &pinned.metadata()?,
            &entry.symlink_metadata()?
        ));
        Ok(())
    }

    #[test]
    fn replacement_preserves_a_new_target_at_the_commit_boundary() -> io::Result<()> {
        let path = tempfile::tempdir()?;
        let captured = super::super::open_directory_identity(path.path())?;
        let target = path.path().join("target");
        fs::write(&target, "old contents")?;
        let original = File::open(&target)?;
        let mut staged = StagedDirectoryFile::new(&captured, path.path())?;
        staged.file_mut().write_all(b"write attempt")?;
        let decoy = path.path().join("decoy");
        fs::write(&decoy, "replacement")?;
        fs::rename(decoy, &target)?;
        assert!(
            staged
                .replace(OsStr::new("target"), &original.metadata()?)
                .is_err()
        );
        drop(staged);
        assert_eq!(fs::read_to_string(target)?, "replacement");
        assert_eq!(path.path().read_dir()?.count(), 1);
        Ok(())
    }

    #[test]
    fn cleanup_uses_the_open_directory_after_rename() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let path = parent.path().join("original");
        let moved = parent.path().join("moved");
        fs::create_dir(&path)?;
        let captured = super::super::open_directory_identity(&path)?;
        let mut staged = StagedDirectoryFile::new(&captured, &path)?;
        staged.file_mut().write_all(b"write attempt")?;
        fs::rename(&path, &moved)?;
        fs::create_dir(&path)?;
        let decoy = path.join(OsStr::from_bytes(staged.name.to_bytes()));
        fs::write(&decoy, "unrelated contents")?;
        drop(staged);
        assert_eq!(moved.read_dir()?.count(), 0);
        assert_eq!(fs::read_to_string(decoy)?, "unrelated contents");
        Ok(())
    }
}
