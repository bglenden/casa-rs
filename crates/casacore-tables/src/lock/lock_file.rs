// SPDX-License-Identifier: LGPL-3.0-or-later
//! Low-level lock file protocol handler.
//!
//! Handles `fcntl`-based byte-range locking, request-list I/O, and sync-data
//! I/O on a casacore `table.lock` file.
//!
//! # C++ reference
//!
//! `LockFile.cc`, `FileLocker.cc`

use std::io;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

use super::LockType;
use super::sync_data::SyncData;

/// Size of a canonical `Int` in the lock file (4 bytes, big-endian).
const SIZEINT: usize = 4;

/// Maximum number of pending request entries in the request list.
const NRREQID: usize = 32;

/// Total size of the request list header in bytes.
/// Layout: 1 count + 32 pairs of (pid, hostid), each entry is SIZEINT bytes.
const SIZEREQID: usize = (1 + 2 * NRREQID) * SIZEINT;

/// Lock file name within a table directory.
pub(crate) const LOCK_FILE_NAME: &str = "table.lock";

/// Low-level lock file protocol handler.
///
/// Owns the open file descriptor to `table.lock` and provides methods for
/// acquiring/releasing `fcntl` advisory locks, reading/writing the request
/// list, and reading/writing sync data.
///
/// C++ equivalent: `LockFile`.
#[allow(dead_code)] // fields used for AutoLocking (wave 3+)
pub(crate) struct LockFile {
    /// File descriptor for `table.lock`. -1 if no lock file.
    fd: RawFd,
    /// Whether the lock file was opened for writing.
    writable: bool,
    /// Whether to add our PID to the request list when waiting.
    add_to_list: bool,
    /// Our process ID.
    pid: i32,
    /// Host ID (always 0, matching C++ which comments out `gethostid()`).
    host_id: i32,
    /// Inspection interval in seconds for auto-locking.
    interval: f64,
    /// Time of last inspection.
    last_inspect: Instant,
    /// Counter for inspection throttling (check every 25 calls).
    inspect_count: u32,
    /// Whether permanent locking is in use (affects in-use byte range).
    perm_locking: bool,
    /// Path to the lock file (for diagnostics).
    path: PathBuf,
    /// Whether we currently hold a read lock on the main byte range.
    read_locked: bool,
    /// Whether we currently hold a write lock on the main byte range.
    write_locked: bool,
}

impl LockFile {
    /// Create or open a `table.lock` file at the given path.
    ///
    /// If `create` is true, the file is created (or truncated) with mode 0666
    /// and the request list is initialized to all zeros.
    ///
    /// An in-use read lock is acquired on the file to signal that the table
    /// is open.
    pub fn create_or_open(
        table_dir: &Path,
        create: bool,
        interval: f64,
        perm_locking: bool,
    ) -> io::Result<Self> {
        let path = table_dir.join(LOCK_FILE_NAME);
        let pid = unsafe { libc::getpid() };

        let fd = if create || !path.exists() {
            // Create with world read/write access, matching C++.
            let c_path = path_to_cstring(&path)?;
            let fd = unsafe {
                libc::open(
                    c_path.as_ptr(),
                    libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC,
                    0o666,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // Initialize the request list header to zeros.
            let zeros = [0u8; SIZEREQID];
            write_at(fd, &zeros, 0)?;
            fd
        } else {
            // Open existing, try read-write first, fall back to read-only.
            let c_path = path_to_cstring(&path)?;
            let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDWR) };
            if fd >= 0 {
                fd
            } else {
                let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                // Read-only: can't add to request list.
                return Ok(Self {
                    fd,
                    writable: false,
                    add_to_list: false,
                    pid,
                    host_id: 0,
                    interval,
                    last_inspect: Instant::now(),
                    inspect_count: 0,
                    perm_locking,
                    path,
                    read_locked: false,
                    write_locked: false,
                });
            }
        };

        let lf = Self {
            fd,
            writable: true,
            add_to_list: true,
            pid,
            host_id: 0,
            interval,
            last_inspect: Instant::now(),
            inspect_count: 0,
            perm_locking,
            path,
            read_locked: false,
            write_locked: false,
        };

        // Acquire in-use read lock (byte 1, length 1 or 2 for permanent).
        let use_len = if perm_locking { 2 } else { 1 };
        let _ = fcntl_lock(fd, libc::F_SETLK, libc::F_RDLCK.into(), 1, use_len);

        // Read any existing request list to clear stale state.
        lf.read_request_count().ok();

        Ok(lf)
    }

    /// Acquire a lock of the given type.
    ///
    /// `nattempts`: number of attempts. 0 means wait indefinitely.
    /// 1 means try once without waiting. >1 means try with 1-second
    /// sleep between attempts.
    ///
    /// Returns `true` if the lock was acquired, `false` if it failed
    /// after all attempts.
    pub fn acquire(&mut self, lock_type: LockType, nattempts: u32) -> io::Result<bool> {
        let flock_type: i32 = match lock_type {
            LockType::Read => libc::F_RDLCK.into(),
            LockType::Write => libc::F_WRLCK.into(),
        };

        // Try once without waiting.
        if fcntl_lock(self.fd, libc::F_SETLK, flock_type, 0, 1)? {
            self.set_lock_state(lock_type, true);
            return Ok(true);
        }

        if nattempts == 1 {
            return Ok(false);
        }

        // Add to request list if enabled.
        let added = if self.add_to_list && self.writable {
            self.add_request_id().ok();
            true
        } else {
            false
        };

        let result = if nattempts == 0 {
            // Wait indefinitely.
            fcntl_lock(self.fd, libc::F_SETLKW, flock_type, 0, 1)?
        } else {
            // Retry with sleep.
            let mut success = false;
            for _ in 1..nattempts {
                std::thread::sleep(std::time::Duration::from_secs(1));
                if fcntl_lock(self.fd, libc::F_SETLK, flock_type, 0, 1)? {
                    success = true;
                    break;
                }
            }
            success
        };

        if added {
            self.remove_request_id().ok();
        }

        if result {
            self.set_lock_state(lock_type, true);
        }

        self.last_inspect = Instant::now();
        self.inspect_count = 0;

        Ok(result)
    }

    /// Release the currently held lock.
    ///
    /// Returns `true` if a lock was released, `false` if no lock was held.
    pub fn release(&mut self) -> io::Result<bool> {
        if !self.read_locked && !self.write_locked {
            return Ok(false);
        }
        fcntl_lock(self.fd, libc::F_SETLK, libc::F_UNLCK.into(), 0, 1)?;
        self.read_locked = false;
        self.write_locked = false;
        Ok(true)
    }

    /// Read sync data from the lock file (after the request list header).
    ///
    /// Returns `None` if no sync data is present (infoLeng == 0).
    pub fn read_sync_data(&self) -> io::Result<Option<SyncData>> {
        // Read the info length at offset SIZEREQID.
        let mut len_buf = [0u8; SIZEINT];
        let n = read_at(self.fd, &mut len_buf, SIZEREQID as i64)?;
        if n < SIZEINT {
            return Ok(None);
        }
        let info_len = u32::from_be_bytes(len_buf) as usize;
        if info_len == 0 {
            return Ok(None);
        }

        // Read the sync payload.
        let mut payload = vec![0u8; info_len];
        let offset = (SIZEREQID + SIZEINT) as i64;
        let n = read_at(self.fd, &mut payload, offset)?;
        if n < info_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("sync data truncated: expected {info_len}, got {n}"),
            ));
        }

        SyncData::decode(&payload).map(Some)
    }

    /// Write sync data to the lock file (after the request list header).
    pub fn write_sync_data(&self, sync: &SyncData) -> io::Result<()> {
        if !self.writable {
            return Ok(());
        }
        let payload = sync.encode()?;
        let info_len = payload.len() as u32;

        // Write info length at offset SIZEREQID.
        let len_bytes = info_len.to_be_bytes();
        write_at(self.fd, &len_bytes, SIZEREQID as i64)?;

        // Write payload immediately after.
        let offset = (SIZEREQID + SIZEINT) as i64;
        write_at(self.fd, &payload, offset)?;

        // fsync to ensure data reaches disk (important for NFS).
        unsafe { libc::fsync(self.fd) };

        Ok(())
    }

    /// Check if other processes need the lock.
    ///
    /// Returns `true` if the request list has any entries, meaning another
    /// process is waiting. Throttled to check at most every 25 calls and
    /// only after the inspection interval has elapsed.
    #[allow(dead_code)] // used by AutoLocking (not yet wired up)
    pub fn inspect(&mut self, always: bool) -> io::Result<bool> {
        if !always {
            if self.interval > 0.0 && self.inspect_count < 25 {
                self.inspect_count += 1;
                return Ok(false);
            }
            self.inspect_count = 0;
            if self.interval > 0.0 && self.last_inspect.elapsed().as_secs_f64() < self.interval {
                return Ok(false);
            }
        }

        let nr = self.read_request_count()?;
        self.last_inspect = Instant::now();
        Ok(nr > 0)
    }

    /// Returns `true` if the given lock type is currently held.
    pub fn has_lock(&self, lock_type: LockType) -> bool {
        match lock_type {
            LockType::Read => self.read_locked,
            LockType::Write => self.write_locked,
        }
    }

    /// Tests if the table is opened by another process.
    ///
    /// Tries to acquire a write lock on the in-use byte; if it fails,
    /// another process has the file open.
    pub fn is_multi_used(&self) -> bool {
        // Try a non-blocking write lock on byte 1.
        !fcntl_lock(self.fd, libc::F_SETLK, libc::F_WRLCK.into(), 1, 1).unwrap_or(true)
    }

    // --- Private helpers ---

    fn set_lock_state(&mut self, lock_type: LockType, locked: bool) {
        match lock_type {
            LockType::Read => {
                self.read_locked = locked;
                if locked {
                    self.write_locked = false;
                }
            }
            LockType::Write => {
                self.write_locked = locked;
                if locked {
                    self.read_locked = false;
                }
            }
        }
    }

    /// Read the request count from the first SIZEINT bytes of the lock file.
    fn read_request_count(&self) -> io::Result<u32> {
        let mut buf = [0u8; SIZEINT];
        let n = read_at(self.fd, &mut buf, 0)?;
        if n < SIZEINT {
            return Ok(0);
        }
        Ok(i32::from_be_bytes(buf) as u32)
    }

    /// Add our PID to the request list.
    fn add_request_id(&self) -> io::Result<()> {
        let mut header = [0u8; SIZEREQID];
        let n = read_at(self.fd, &mut header, 0)?;
        if n < SIZEREQID {
            // Pad with zeros if short.
            header[n..].fill(0);
        }

        let count = i32::from_be_bytes(header[0..4].try_into().unwrap());
        let inx = count.min(NRREQID as i32 - 1) as usize;

        // Write our PID and host ID at the next slot.
        let pid_offset = (1 + 2 * inx) * SIZEINT;
        let host_offset = pid_offset + SIZEINT;
        header[pid_offset..pid_offset + 4].copy_from_slice(&self.pid.to_be_bytes());
        header[host_offset..host_offset + 4].copy_from_slice(&self.host_id.to_be_bytes());

        // Increment count.
        let new_count = (count + 1).min(NRREQID as i32);
        header[0..4].copy_from_slice(&new_count.to_be_bytes());

        write_at(self.fd, &header, 0)?;
        unsafe { libc::fsync(self.fd) };
        Ok(())
    }

    /// Remove our PID from the request list.
    fn remove_request_id(&self) -> io::Result<()> {
        let mut header = [0u8; SIZEREQID];
        let n = read_at(self.fd, &mut header, 0)?;
        if n < SIZEINT {
            return Ok(());
        }

        let count = i32::from_be_bytes(header[0..4].try_into().unwrap());
        if count <= 0 {
            return Ok(());
        }

        // Find our PID in the list and remove it.
        let mut found = false;
        for i in 0..count.min(NRREQID as i32) as usize {
            let pid_offset = (1 + 2 * i) * SIZEINT;
            let pid = i32::from_be_bytes(header[pid_offset..pid_offset + 4].try_into().unwrap());
            if pid == self.pid {
                // Shift remaining entries down.
                let remaining = count as usize - i - 1;
                if remaining > 0 {
                    let src_start = (1 + 2 * (i + 1)) * SIZEINT;
                    let dst_start = pid_offset;
                    let len = remaining * 2 * SIZEINT;
                    header.copy_within(src_start..src_start + len, dst_start);
                }
                // Clear the last slot.
                let last_offset = (1 + 2 * (count as usize - 1)) * SIZEINT;
                header[last_offset..last_offset + 2 * SIZEINT].fill(0);
                found = true;
                break;
            }
        }

        if found {
            let new_count = count - 1;
            header[0..4].copy_from_slice(&new_count.to_be_bytes());
            write_at(self.fd, &header, 0)?;
            unsafe { libc::fsync(self.fd) };
        }

        Ok(())
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        if self.fd >= 0 {
            // Release any held lock (ignore errors in Drop).
            let _ = self.release();
            unsafe { libc::close(self.fd) };
            self.fd = -1;
        }
    }
}

// --- Low-level helpers ---

/// Perform an `fcntl` lock operation.
///
/// Returns `true` if the lock was acquired/released, `false` if it would
/// block. Treats `ENOLCK` as success (NFS compatibility, matching C++).
fn fcntl_lock(fd: RawFd, cmd: i32, lock_type: i32, start: i64, len: i64) -> io::Result<bool> {
    let mut flock = libc::flock {
        l_type: lock_type as i16,
        l_whence: libc::SEEK_SET as i16,
        l_start: start,
        l_len: len,
        l_pid: 0,
    };
    let result = unsafe { libc::fcntl(fd, cmd, &mut flock) };
    if result == -1 {
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            // EAGAIN/EACCES: lock held by another process (non-blocking).
            Some(libc::EAGAIN) | Some(libc::EACCES) => Ok(false),
            // ENOLCK: NFS can't grant lock — treat as success, matching C++.
            Some(libc::ENOLCK) => Ok(true),
            _ => Err(err),
        }
    } else {
        Ok(true)
    }
}

/// Read from a file descriptor at a given offset using `pread`.
fn read_at(fd: RawFd, buf: &mut [u8], offset: i64) -> io::Result<usize> {
    let n = unsafe { libc::pread(fd, buf.as_mut_ptr().cast(), buf.len(), offset) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// Write to a file descriptor at a given offset using `pwrite`.
fn write_at(fd: RawFd, buf: &[u8], offset: i64) -> io::Result<usize> {
    let n = unsafe { libc::pwrite(fd, buf.as_ptr().cast(), buf.len(), offset) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

/// Convert a `Path` to a C string for use with `libc::open`.
fn path_to_cstring(path: &Path) -> io::Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains null byte: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn create_lock_file_writes_zeroed_header() {
        let dir = TempDir::new().unwrap();
        let lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();

        // Read the raw header from disk.
        let lock_path = dir.path().join(LOCK_FILE_NAME);
        let data = std::fs::read(&lock_path).unwrap();
        assert!(data.len() >= SIZEREQID);
        // Request count should be 0.
        assert_eq!(&data[0..4], &[0, 0, 0, 0]);
        // All request slots should be zero.
        assert!(data[..SIZEREQID].iter().all(|&b| b == 0));

        drop(lf);
    }

    #[test]
    fn acquire_release_write_lock() {
        let dir = TempDir::new().unwrap();
        let mut lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();

        assert!(!lf.has_lock(LockType::Write));
        assert!(lf.acquire(LockType::Write, 1).unwrap());
        assert!(lf.has_lock(LockType::Write));
        assert!(!lf.has_lock(LockType::Read));

        assert!(lf.release().unwrap());
        assert!(!lf.has_lock(LockType::Write));
    }

    #[test]
    fn acquire_release_read_lock() {
        let dir = TempDir::new().unwrap();
        let mut lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();

        assert!(lf.acquire(LockType::Read, 1).unwrap());
        assert!(lf.has_lock(LockType::Read));
        assert!(!lf.has_lock(LockType::Write));

        assert!(lf.release().unwrap());
        assert!(!lf.has_lock(LockType::Read));
    }

    #[test]
    fn sync_data_round_trip_through_file() {
        let dir = TempDir::new().unwrap();
        let lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();

        let sync = SyncData {
            nrrow: 42,
            nrcolumn: 2,
            modify_counter: 3,
            table_change_counter: 1,
            data_man_change_counters: vec![5, 7],
        };
        lf.write_sync_data(&sync).unwrap();

        let read_back = lf.read_sync_data().unwrap().expect("sync data present");
        assert_eq!(sync, read_back);
    }

    #[test]
    fn read_sync_data_returns_none_when_empty() {
        let dir = TempDir::new().unwrap();
        let lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();

        // Fresh lock file has no sync data.
        let result = lf.read_sync_data().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn release_returns_false_when_not_locked() {
        let dir = TempDir::new().unwrap();
        let mut lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
        assert!(!lf.release().unwrap());
    }

    #[test]
    fn inspect_throttles_calls() {
        let dir = TempDir::new().unwrap();
        let mut lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();

        // First 25 non-forced inspect calls should return false (throttled).
        for _ in 0..25 {
            assert!(!lf.inspect(false).unwrap());
        }

        // Forced inspect should always check.
        // No requests pending, so should return false.
        assert!(!lf.inspect(true).unwrap());
    }
}
