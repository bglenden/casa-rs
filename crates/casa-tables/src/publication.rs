// SPDX-License-Identifier: LGPL-3.0-or-later
//! Filesystem publication of staged casacore data products.

#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::ffi::CString;
use std::path::Path;

/// Move a staged directory to a new path without replacing it on Linux or macOS.
///
/// # Errors
/// Returns an I/O error if either path is invalid or the destination exists.
#[cfg(target_os = "macos")]
pub fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let from = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let to = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("destination path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this call.
    let status = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

/// Move a staged directory to a new path without replacing it on Linux or macOS.
///
/// # Errors
/// Returns an I/O error if either path is invalid or the destination exists.
#[cfg(target_os = "linux")]
pub fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let from = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let to = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("destination path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this call.
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

/// Check that the destination is absent, then move a staged directory there.
///
/// # Errors
/// Returns an I/O error if either path is invalid or the destination exists.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn rename_without_replacing(from: &Path, to: &Path) -> std::io::Result<()> {
    if to.symlink_metadata().is_ok() {
        return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists));
    }
    std::fs::rename(from, to)
}

/// Exchange the directories at `staging` and `target` in one atomic call
/// (Linux `renameat2(RENAME_EXCHANGE)`, macOS `renamex_np(RENAME_SWAP)`):
/// afterwards each path names what the other did, and no moment exists at
/// which `target` is missing. Elsewhere it is `std::io::ErrorKind::Unsupported`.
///
/// # Errors
///
/// When either path does not exist, they are on different filesystems, or
/// the filesystem cannot exchange directories.
#[cfg(target_os = "macos")]
pub fn exchange_directories(staging: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("target path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this atomic call.
    let status = unsafe { libc::renamex_np(staging.as_ptr(), target.as_ptr(), libc::RENAME_SWAP) };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

/// Exchange the directories at `staging` and `target` in one atomic call
/// (Linux `renameat2(RENAME_EXCHANGE)`, macOS `renamex_np(RENAME_SWAP)`):
/// afterwards each path names what the other did, and no moment exists at
/// which `target` is missing. Elsewhere it is `std::io::ErrorKind::Unsupported`.
///
/// # Errors
///
/// When either path does not exist, they are on different filesystems, or
/// the filesystem cannot exchange directories.
#[cfg(target_os = "linux")]
pub fn exchange_directories(staging: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("staging path contains NUL"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("target path contains NUL"))?;
    // SAFETY: both C strings remain valid for the duration of this atomic call.
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            staging.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    (status == 0)
        .then_some(())
        .ok_or_else(std::io::Error::last_os_error)
}

/// Exchange the directories at `staging` and `target` in one atomic call
/// (Linux `renameat2(RENAME_EXCHANGE)`, macOS `renamex_np(RENAME_SWAP)`):
/// afterwards each path names what the other did, and no moment exists at
/// which `target` is missing. Elsewhere it is `std::io::ErrorKind::Unsupported`.
///
/// # Errors
///
/// When either path does not exist, they are on different filesystems, or
/// the filesystem cannot exchange directories.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn exchange_directories(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic non-empty directory exchange is unavailable",
    ))
}
