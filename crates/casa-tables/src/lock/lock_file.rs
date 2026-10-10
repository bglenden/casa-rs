// SPDX-License-Identifier: LGPL-3.0-or-later
//! Low-level lock file protocol handler.
//!
//! Handles `fcntl`-based byte-range locking, request-list I/O, and sync-data
//! I/O on a casacore `table.lock` file.

// libc::F_RDLCK et al. are i16 on macOS but i32 on Linux; the `as i32` casts
// are necessary on macOS but flagged as unnecessary on Linux.
#![allow(clippy::unnecessary_cast)]
//!
//! # One descriptor per lock file per process
//!
//! `fcntl` record locks belong to the process, not to a descriptor: two
//! descriptors on the same `table.lock` in one process never exclude each
//! other, and closing *any* descriptor on the file releases every lock the
//! process holds on it. casacore avoids both through its process-wide table
//! cache, which gives each table one `LockFile` per process. This module
//! keeps the same invariant with a process registry: every [`LockFile`]
//! handle on one lock file (identified by device and inode) shares one
//! descriptor, which is closed only when the last handle is dropped, and the
//! registry arbitrates the locks the handles hold through it:
//!
//! - a write lock is held by at most one handle in the process, and a second
//!   handle is refused it at once, without waiting, because the holder may
//!   be the thread that would wait;
//! - read locks never conflict within the process, as with casacore's shared
//!   table;
//! - the process's `fcntl` lock is the strongest lock any handle holds, so
//!   releasing one handle never drops a lock another handle still holds.
//!
//! Nothing here is persisted: the registry is process memory, and the file
//! protocol is casacore's own, so casacore and casa-rs processes exclude each
//! other through the same byte ranges.
//!
//! # Waiting for another process
//!
//! A lock another process holds is waited for as casacore's
//! `LockFile::acquire` waits: after one failed attempt the process's id is
//! added to the request list at the start of the lock file, which a casacore
//! holder using `AutoLocking` inspects and answers by releasing its lock, and
//! it is removed again once the wait ends. The request list is casacore's
//! layout: a big-endian `Int` count followed by 32 big-endian
//! `(pid, hostid)` pairs, with host id 0 as casacore writes it. casacore's
//! holder reads only the count. A wait is reported once when it starts, every
//! ten seconds while it lasts, and once more when the lock is acquired.
//!
//! # File systems without locking
//!
//! casacore counts a lock refused with `ENOLCK` ("locking over a network
//! file system is not working", NFS without `lockd`) as acquired, so tables
//! there are used without cross-process exclusion. macOS `smbfs` refuses
//! `fcntl` locks with `ENOTSUP`/`EOPNOTSUPP` for the same condition, and
//! casa-rs treats all three alike: the lock counts as acquired, read or
//! write, and one warning per lock file names the path and errno. Handles in
//! one process still exclude each other through the registry.
//!
//! # C++ reference
//!
//! `LockFile.cc`, `FileLocker.cc`, `PlainTable::tableCache`

use std::collections::{HashMap, HashSet};
use std::io;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

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

/// Interval between attempts while waiting indefinitely for another process.
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Interval between progress reports while waiting for another process.
const WAIT_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Identity of an open lock file: device and inode.
type LockFileKey = (u64, u64);

/// The process's descriptor on one `table.lock` and the locks held through it.
struct SharedLockFd {
    fd: RawFd,
    writable: bool,
    path: PathBuf,
    state: Mutex<ProcessLockState>,
}

impl SharedLockFd {
    fn state(&self) -> MutexGuard<'_, ProcessLockState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Set (or clear) an `fcntl` lock on `start..start + len` without
    /// waiting; `false` when another process holds a conflicting lock.
    fn set_lock(&self, lock_type: i32, start: i64, len: i64) -> io::Result<bool> {
        self.granted(fcntl_lock(self.fd, lock_type, start, len))
    }

    /// Whether an `fcntl` outcome grants the lock. A file system without
    /// lock support grants every lock, as casacore does for `ENOLCK`.
    fn granted(&self, outcome: io::Result<FcntlOutcome>) -> io::Result<bool> {
        Ok(match outcome? {
            FcntlOutcome::Granted => true,
            FcntlOutcome::Held => false,
            FcntlOutcome::Unsupported(errno) => {
                report_unsupported_locking(&self.path, errno);
                true
            }
        })
    }
}

/// Warn, once per lock file in this process, that its file system does not
/// support `fcntl` locking. Returns whether this call warned.
fn report_unsupported_locking(path: &Path, errno: i32) -> bool {
    static REPORTED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    let first = REPORTED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(path.to_path_buf());
    if first {
        tracing::warn!(
            path = %path.display(),
            errno,
            error = %io::Error::from_raw_os_error(errno),
            "the file system does not support table locking; the table is used without \
             cross-process locks, as casacore does when locking over a network file system \
             is not working (ENOLCK)"
        );
    }
    first
}

/// Locks the handles of one process hold on one lock file.
#[derive(Default)]
struct ProcessLockState {
    /// Handles holding a read lock.
    readers: usize,
    /// The handle holding the write lock.
    writer: Option<u64>,
    /// Length of the in-use read lock at byte 1 (2 under permanent locking).
    in_use_len: i64,
}

/// One registered lock file: its shared descriptor and the handles using it.
struct RegistryEntry {
    shared: Arc<SharedLockFd>,
    handles: usize,
    /// Descriptors opened while another process replaced the file between
    /// `stat` and `open`; closing them early would drop the process's locks.
    extra_fds: Vec<RawFd>,
}

/// Open lock files of this process. Descriptors are opened, registered and
/// closed only while this mutex is held, so no descriptor on a registered
/// file is ever closed while a lock is held through another one.
fn registry() -> MutexGuard<'static, HashMap<LockFileKey, RegistryEntry>> {
    static REGISTRY: OnceLock<Mutex<HashMap<LockFileKey, RegistryEntry>>> = OnceLock::new();
    REGISTRY
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Device and inode of `path`, or `None` when it does not exist.
fn path_key(path: &Path) -> io::Result<Option<LockFileKey>> {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some((metadata.dev(), metadata.ino()))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Device and inode of an open descriptor.
fn fd_key(fd: RawFd) -> io::Result<LockFileKey> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.st_dev as u64, stat.st_ino as u64))
}

/// Outcome of a lock request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockOutcome {
    /// This handle holds the lock.
    Acquired,
    /// Another handle in this process holds the write lock. It is never
    /// waited for, because it may belong to the waiting thread.
    HeldInProcess,
    /// Another process holds a conflicting lock, still after the attempts
    /// the request allowed.
    HeldByAnotherProcess,
}

impl LockOutcome {
    /// Whether the lock was acquired.
    pub(crate) fn is_acquired(self) -> bool {
        self == Self::Acquired
    }

    /// Why a request for `lock_type` with `nattempts` attempts was refused,
    /// worded for [`TableError::LockFailed`](crate::TableError::LockFailed).
    pub(crate) fn refusal(self, lock_type: LockType, nattempts: u32) -> String {
        match self {
            Self::Acquired => format!("the {} lock was acquired", lock_name(lock_type)),
            Self::HeldInProcess => "the write lock is held by another handle on this table in \
                                    this process; a lock held in this process is never waited for"
                .to_string(),
            Self::HeldByAnotherProcess => {
                let held = match lock_type {
                    LockType::Read => "another process holds the write lock",
                    LockType::Write => "another process holds a read or write lock",
                };
                match nattempts {
                    0 | 1 => format!("{held} (one attempt, without waiting)"),
                    attempts => format!("{held}, still after {attempts} attempts one second apart"),
                }
            }
        }
    }
}

/// The lock type as casacore's messages name it.
fn lock_name(lock_type: LockType) -> &'static str {
    match lock_type {
        LockType::Read => "read",
        LockType::Write => "write",
    }
}

/// Low-level lock file protocol handler.
///
/// One handle on a `table.lock` file. Handles on the same file in one
/// process share its descriptor through the process registry (see the
/// module documentation), and provide methods for acquiring/releasing
/// `fcntl` advisory locks, reading/writing the request list, and
/// reading/writing sync data.
///
/// C++ equivalent: `LockFile`.
#[allow(dead_code)] // fields used for AutoLocking (wave 3+)
pub(crate) struct LockFile {
    /// The process's descriptor on this lock file.
    shared: Arc<SharedLockFd>,
    /// Registry key of `shared`.
    key: LockFileKey,
    /// This handle's identity within the process.
    id: u64,
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
    /// The lock this handle holds on the main byte range.
    held: Option<LockType>,
}

impl LockFile {
    pub(crate) fn retained_heap_bytes(&self) -> usize {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            self.path.as_os_str().as_bytes().len()
        }
        #[cfg(not(unix))]
        {
            self.path.as_os_str().to_string_lossy().len()
        }
    }

    /// Create or open a `table.lock` file at the given path.
    ///
    /// If `create` is true, the file is created (or truncated) with mode 0666
    /// and the request list is initialized to all zeros. A lock file this
    /// process already has open is shared instead, and only its request list
    /// is reset.
    ///
    /// An in-use read lock is acquired on the file to signal that the table
    /// is open.
    pub fn create_or_open(
        table_dir: &Path,
        create: bool,
        interval: f64,
        perm_locking: bool,
    ) -> io::Result<Self> {
        static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);
        let path = table_dir.join(LOCK_FILE_NAME);
        let pid = unsafe { libc::getpid() };

        let mut registry = registry();
        let existing = path_key(&path)?;
        let (shared, key) = match existing.filter(|key| registry.contains_key(key)) {
            Some(key) => {
                let entry = registry.get_mut(&key).expect("registered lock file");
                entry.handles += 1;
                if create && entry.shared.writable {
                    write_at(entry.shared.fd, &[0u8; SIZEREQID], 0)?;
                }
                (Arc::clone(&entry.shared), key)
            }
            None => {
                let (fd, writable) = open_lock_file(&path, create || existing.is_none())?;
                let key = match fd_key(fd) {
                    Ok(key) => key,
                    Err(error) => {
                        unsafe { libc::close(fd) };
                        return Err(error);
                    }
                };
                if let Some(entry) = registry.get_mut(&key) {
                    entry.handles += 1;
                    entry.extra_fds.push(fd);
                    (Arc::clone(&entry.shared), key)
                } else {
                    let shared = Arc::new(SharedLockFd {
                        fd,
                        writable,
                        path: path.clone(),
                        state: Mutex::new(ProcessLockState::default()),
                    });
                    registry.insert(
                        key,
                        RegistryEntry {
                            shared: Arc::clone(&shared),
                            handles: 1,
                            extra_fds: Vec::new(),
                        },
                    );
                    (shared, key)
                }
            }
        };

        // Acquire the in-use read lock (byte 1, length 1 or 2 for permanent).
        {
            let mut state = shared.state();
            let use_len = if perm_locking { 2 } else { 1 };
            if state.in_use_len < use_len {
                let _ = shared.set_lock(libc::F_RDLCK as i32, 1, use_len);
                state.in_use_len = use_len;
            }
        }
        drop(registry);

        let lf = Self {
            add_to_list: shared.writable,
            shared,
            key,
            id: NEXT_HANDLE.fetch_add(1, Ordering::Relaxed),
            pid,
            host_id: 0,
            interval,
            last_inspect: Instant::now(),
            inspect_count: 0,
            perm_locking,
            path,
            held: None,
        };

        // Read any existing request list to clear stale state.
        lf.read_request_count().ok();

        Ok(lf)
    }

    /// Acquire a lock of the given type, as casacore's `LockFile::acquire`
    /// does.
    ///
    /// One attempt is made without waiting. When another process holds a
    /// conflicting lock and `nattempts` is not 1, this process's id is added
    /// to the lock file's request list (when the file is writable), so that a
    /// casacore holder using `AutoLocking` releases its lock at its next
    /// inspection, and the attempts continue: with `nattempts == 0` every
    /// 50 ms until the lock is acquired, however long that takes, and
    /// otherwise once a second up to `nattempts` attempts in all. The id is
    /// removed from the request list when the wait ends. A wait is logged
    /// when it starts, every ten seconds while it lasts and when it succeeds.
    ///
    /// A write lock another handle in this process holds is never waited for,
    /// whatever `nattempts` is: that handle may belong to the waiting thread.
    /// The request is answered [`LockOutcome::HeldInProcess`] at once.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when `fcntl` fails for a reason other than a held
    /// lock or missing lock support.
    pub fn acquire(&mut self, lock_type: LockType, nattempts: u32) -> io::Result<LockOutcome> {
        let first = self.try_acquire(lock_type)?;
        let result = if first == LockOutcome::HeldByAnotherProcess && nattempts != 1 {
            let added = self.add_to_list && self.shared.writable && self.add_request_id().is_ok();
            let result = self.wait_for_other_process(lock_type, nattempts);
            if added {
                self.remove_request_id().ok();
            }
            result
        } else {
            Ok(first)
        };
        self.last_inspect = Instant::now();
        self.inspect_count = 0;
        result
    }

    /// Repeat the attempt while another process holds a conflicting lock:
    /// indefinitely for `nattempts == 0`, otherwise up to `nattempts`
    /// attempts in all, one second apart.
    fn wait_for_other_process(
        &mut self,
        lock_type: LockType,
        nattempts: u32,
    ) -> io::Result<LockOutcome> {
        let table = self.path.parent().unwrap_or(&self.path).to_path_buf();
        let lock = lock_name(lock_type);
        let (interval, mut attempts_left) = if nattempts == 0 {
            tracing::warn!(
                table = %table.display(),
                lock,
                "the table is locked by another process; waiting until it is released, \
                 as casacore does"
            );
            (WAIT_POLL_INTERVAL, None)
        } else {
            tracing::warn!(
                table = %table.display(),
                lock,
                attempts = nattempts,
                "the table is locked by another process; retrying once a second"
            );
            (Duration::from_secs(1), Some(nattempts - 1))
        };
        let started = Instant::now();
        let mut next_report = started + WAIT_REPORT_INTERVAL;
        loop {
            if let Some(left) = attempts_left.as_mut() {
                if *left == 0 {
                    return Ok(LockOutcome::HeldByAnotherProcess);
                }
                *left -= 1;
            }
            std::thread::sleep(interval);
            match self.try_acquire(lock_type)? {
                LockOutcome::HeldByAnotherProcess => {}
                LockOutcome::Acquired => {
                    tracing::info!(
                        table = %table.display(),
                        lock,
                        waited_s = started.elapsed().as_secs_f64(),
                        "acquired the table lock another process held"
                    );
                    return Ok(LockOutcome::Acquired);
                }
                LockOutcome::HeldInProcess => return Ok(LockOutcome::HeldInProcess),
            }
            let now = Instant::now();
            if now >= next_report {
                tracing::info!(
                    table = %table.display(),
                    lock,
                    waited_s = now.duration_since(started).as_secs_f64(),
                    "still waiting for the table lock another process holds"
                );
                next_report = now + WAIT_REPORT_INTERVAL;
            }
        }
    }

    /// One non-blocking attempt, arbitrated against the other handles of this
    /// process before the process's `fcntl` lock is changed.
    fn try_acquire(&mut self, lock_type: LockType) -> io::Result<LockOutcome> {
        let shared = &self.shared;
        let mut state = shared.state();
        match lock_type {
            LockType::Write => {
                match state.writer {
                    Some(writer) if writer == self.id => return Ok(LockOutcome::Acquired),
                    Some(_) => return Ok(LockOutcome::HeldInProcess),
                    None => {}
                }
                if !shared.set_lock(libc::F_WRLCK as i32, 0, 1)? {
                    return Ok(LockOutcome::HeldByAnotherProcess);
                }
                if self.held == Some(LockType::Read) {
                    state.readers -= 1;
                }
                state.writer = Some(self.id);
                self.held = Some(LockType::Write);
                Ok(LockOutcome::Acquired)
            }
            LockType::Read => match self.held {
                Some(LockType::Read) => Ok(LockOutcome::Acquired),
                Some(LockType::Write) => {
                    // casacore converts a held write lock to a read lock.
                    shared.set_lock(libc::F_RDLCK as i32, 0, 1)?;
                    state.writer = None;
                    state.readers += 1;
                    self.held = Some(LockType::Read);
                    Ok(LockOutcome::Acquired)
                }
                None => {
                    // The process's lock already covers reading when another
                    // handle holds a read or write lock.
                    if state.readers == 0
                        && state.writer.is_none()
                        && !shared.set_lock(libc::F_RDLCK as i32, 0, 1)?
                    {
                        return Ok(LockOutcome::HeldByAnotherProcess);
                    }
                    state.readers += 1;
                    self.held = Some(LockType::Read);
                    Ok(LockOutcome::Acquired)
                }
            },
        }
    }

    /// Release the currently held lock.
    ///
    /// The process's `fcntl` lock drops to the strongest lock another handle
    /// in this process still holds.
    ///
    /// Returns `true` if a lock was released, `false` if no lock was held.
    pub fn release(&mut self) -> io::Result<bool> {
        let shared = &self.shared;
        let mut state = shared.state();
        let Some(held) = self.held.take() else {
            return Ok(false);
        };
        match held {
            LockType::Read => {
                state.readers -= 1;
                if state.readers == 0 && state.writer.is_none() {
                    shared.set_lock(libc::F_UNLCK as i32, 0, 1)?;
                }
            }
            LockType::Write => {
                state.writer = None;
                let remaining = if state.readers > 0 {
                    libc::F_RDLCK
                } else {
                    libc::F_UNLCK
                };
                shared.set_lock(remaining as i32, 0, 1)?;
            }
        }
        Ok(true)
    }

    /// Read sync data from the lock file (after the request list header).
    ///
    /// Returns `None` if no sync data is present (infoLeng == 0).
    pub fn read_sync_data(&self) -> io::Result<Option<SyncData>> {
        read_sync_data_from_fd(self.shared.fd)
    }

    /// Write sync data to the lock file (after the request list header).
    pub fn write_sync_data(&self, sync: &SyncData) -> io::Result<()> {
        if !self.shared.writable {
            return Ok(());
        }
        let fd = self.shared.fd;
        let payload = sync.encode()?;
        let info_len = payload.len() as u32;

        // Write info length at offset SIZEREQID.
        let len_bytes = info_len.to_be_bytes();
        write_at(fd, &len_bytes, SIZEREQID as i64)?;

        // Write payload immediately after.
        let offset = (SIZEREQID + SIZEINT) as i64;
        write_at(fd, &payload, offset)?;

        // fsync to ensure data reaches disk (important for NFS).
        unsafe { libc::fsync(fd) };

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
            // Match casacore C++ FileLocker::hasLock behavior: a write lock
            // implies read capability for this process.
            LockType::Read => self.held.is_some(),
            LockType::Write => self.held == Some(LockType::Write),
        }
    }

    /// Tests if the table is opened by another process.
    ///
    /// Tests, without acquiring it, whether a write lock on the in-use byte
    /// could be granted; if not, another process has the file open.
    ///
    /// C++ equivalent: `LockFile::isMultiUsed` (`FileLocker::canLock`).
    pub fn is_multi_used(&self) -> bool {
        !fcntl_can_lock(self.shared.fd, libc::F_WRLCK as i32, 1, 1).unwrap_or(false)
    }

    // --- Private helpers ---

    /// Read the request count from the first SIZEINT bytes of the lock file.
    fn read_request_count(&self) -> io::Result<u32> {
        let mut buf = [0u8; SIZEINT];
        let n = read_at(self.shared.fd, &mut buf, 0)?;
        if n < SIZEINT {
            return Ok(0);
        }
        Ok(i32::from_be_bytes(buf) as u32)
    }

    /// Add our PID to the request list.
    fn add_request_id(&self) -> io::Result<()> {
        let fd = self.shared.fd;
        let mut header = [0u8; SIZEREQID];
        let n = read_at(fd, &mut header, 0)?;
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

        write_at(fd, &header, 0)?;
        unsafe { libc::fsync(fd) };
        Ok(())
    }

    /// Remove our PID from the request list.
    fn remove_request_id(&self) -> io::Result<()> {
        let fd = self.shared.fd;
        let mut header = [0u8; SIZEREQID];
        let n = read_at(fd, &mut header, 0)?;
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
            write_at(fd, &header, 0)?;
            unsafe { libc::fsync(fd) };
        }

        Ok(())
    }
}

/// Open `path` read-write (creating and initializing it when `create`),
/// falling back to read-only for an existing file.
fn open_lock_file(path: &Path, create: bool) -> io::Result<(RawFd, bool)> {
    let c_path = path_to_cstring(path)?;
    if create {
        // Create with world read/write access, matching C++.
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
        if let Err(error) = write_at(fd, &[0u8; SIZEREQID], 0) {
            unsafe { libc::close(fd) };
            return Err(error);
        }
        return Ok((fd, true));
    }
    // Open existing, try read-write first, fall back to read-only.
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDWR) };
    if fd >= 0 {
        return Ok((fd, true));
    }
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // Read-only: can't add to request list.
    Ok((fd, false))
}

/// Read `TableSyncData` from an existing `table.lock` file without acquiring
/// or holding any locks.
///
/// This mirrors the information that C++ `PlainTable` consults during open
/// before deciding which row count to trust for the subsequent table-data
/// load. A lock file this process already has open is read through its
/// shared descriptor; closing a second descriptor would release the
/// process's locks on it.
pub(crate) fn read_sync_data_from_table_dir(table_dir: &Path) -> io::Result<Option<SyncData>> {
    let path = table_dir.join(LOCK_FILE_NAME);
    let registry = registry();
    let Some(key) = path_key(&path)? else {
        return Ok(None);
    };
    if let Some(entry) = registry.get(&key) {
        return read_sync_data_from_fd(entry.shared.fd);
    }

    // The process holds no lock on this file, so closing this descriptor
    // releases nothing; the registry stays locked until it is closed.
    let c_path = path_to_cstring(&path)?;
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    struct FdGuard(i32);
    impl Drop for FdGuard {
        fn drop(&mut self) {
            unsafe { libc::close(self.0) };
        }
    }
    let _guard = FdGuard(fd);
    read_sync_data_from_fd(fd)
}

/// Read the sync data that follows the request list in an open lock file.
fn read_sync_data_from_fd(fd: RawFd) -> io::Result<Option<SyncData>> {
    let mut len_buf = [0u8; SIZEINT];
    let n = read_at(fd, &mut len_buf, SIZEREQID as i64)?;
    if n < SIZEINT {
        return Ok(None);
    }
    let info_len = u32::from_be_bytes(len_buf) as usize;
    if info_len == 0 {
        return Ok(None);
    }

    let mut payload = vec![0u8; info_len];
    let offset = (SIZEREQID + SIZEINT) as i64;
    let n = read_at(fd, &mut payload, offset)?;
    if n < info_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("sync data truncated: expected {info_len}, got {n}"),
        ));
    }

    SyncData::decode(&payload).map(Some)
}

impl Drop for LockFile {
    fn drop(&mut self) {
        // Release any held lock (ignore errors in Drop).
        let _ = self.release();
        let mut registry = registry();
        let Some(entry) = registry.get_mut(&self.key) else {
            return;
        };
        entry.handles -= 1;
        if entry.handles == 0 {
            let entry = registry.remove(&self.key).expect("registered lock file");
            // Closing the last descriptor also drops the in-use lock.
            unsafe { libc::close(entry.shared.fd) };
            for fd in entry.extra_fds {
                unsafe { libc::close(fd) };
            }
        }
    }
}

// --- Low-level helpers ---

/// Outcome of one `fcntl` lock request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FcntlOutcome {
    /// The lock was set (or cleared).
    Granted,
    /// Another process holds a conflicting lock.
    Held,
    /// The file system does not support `fcntl` locking; carries the errno.
    Unsupported(i32),
}

/// Classify a failed `fcntl` lock request by its errno.
///
/// `EAGAIN`/`EACCES` mean another process holds the lock. `ENOLCK` is what
/// NFS returns when locking over the network is not working (casacore's
/// `FileLocker` counts it as acquired), and macOS `smbfs` returns
/// `ENOTSUP`/`EOPNOTSUPP` for the same condition. Any other errno is an error.
fn outcome_of_failed_lock(error: io::Error) -> io::Result<FcntlOutcome> {
    match error.raw_os_error() {
        Some(libc::EAGAIN | libc::EACCES) => Ok(FcntlOutcome::Held),
        Some(errno)
            if errno == libc::ENOLCK || errno == libc::ENOTSUP || errno == libc::EOPNOTSUPP =>
        {
            Ok(FcntlOutcome::Unsupported(errno))
        }
        _ => Err(error),
    }
}

/// Perform a non-blocking `fcntl` lock operation (`F_SETLK`).
fn fcntl_lock(fd: RawFd, lock_type: i32, start: i64, len: i64) -> io::Result<FcntlOutcome> {
    let mut flock = libc::flock {
        l_type: lock_type as i16,
        l_whence: libc::SEEK_SET as i16,
        l_start: start,
        l_len: len,
        l_pid: 0,
    };
    if unsafe { libc::fcntl(fd, libc::F_SETLK, &mut flock) } == -1 {
        outcome_of_failed_lock(io::Error::last_os_error())
    } else {
        Ok(FcntlOutcome::Granted)
    }
}

/// Test, without acquiring it, whether another process holds a lock that
/// conflicts with `lock_type` on the range.
///
/// C++ equivalent: `FileLocker::canLock` (`F_GETLK`).
fn fcntl_can_lock(fd: RawFd, lock_type: i32, start: i64, len: i64) -> io::Result<bool> {
    let mut flock = libc::flock {
        l_type: lock_type as i16,
        l_whence: libc::SEEK_SET as i16,
        l_start: start,
        l_len: len,
        l_pid: 0,
    };
    if unsafe { libc::fcntl(fd, libc::F_GETLK, &mut flock) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(flock.l_type == libc::F_UNLCK as i16)
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
        assert!(lf.acquire(LockType::Write, 1).unwrap().is_acquired());
        assert!(lf.has_lock(LockType::Write));
        assert!(lf.has_lock(LockType::Read));

        assert!(lf.release().unwrap());
        assert!(!lf.has_lock(LockType::Write));
    }

    #[test]
    fn acquire_release_read_lock() {
        let dir = TempDir::new().unwrap();
        let mut lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();

        assert!(lf.acquire(LockType::Read, 1).unwrap().is_acquired());
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

    #[test]
    fn lock_refusals_are_classified_by_errno() {
        let outcome = |errno| outcome_of_failed_lock(io::Error::from_raw_os_error(errno));
        for errno in [libc::ENOLCK, libc::ENOTSUP, libc::EOPNOTSUPP] {
            assert_eq!(outcome(errno).unwrap(), FcntlOutcome::Unsupported(errno));
        }
        for errno in [libc::EAGAIN, libc::EACCES] {
            assert_eq!(outcome(errno).unwrap(), FcntlOutcome::Held);
        }
        assert!(outcome(libc::EBADF).is_err());
    }

    /// A file system without lock support (NFS without lockd, macOS smbfs)
    /// grants every lock, as casacore does for ENOLCK, and is reported once
    /// per lock file; a lock held by another process is still refused.
    #[test]
    fn a_file_system_without_locking_grants_locks_and_warns_once() {
        let dir = TempDir::new().unwrap();
        let lock = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
        for errno in [libc::ENOTSUP, libc::EOPNOTSUPP, libc::ENOLCK] {
            assert!(
                lock.shared
                    .granted(Ok(FcntlOutcome::Unsupported(errno)))
                    .unwrap()
            );
        }
        assert!(
            !report_unsupported_locking(&lock.shared.path, libc::ENOTSUP),
            "the lock file is reported once"
        );
        assert!(!lock.shared.granted(Ok(FcntlOutcome::Held)).unwrap());
        assert!(
            lock.shared
                .granted(outcome_of_failed_lock(io::Error::from_raw_os_error(
                    libc::EBADF
                )))
                .is_err()
        );
    }

    #[test]
    fn handles_in_one_process_share_one_descriptor() {
        let dir = TempDir::new().unwrap();
        let first = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
        let second = LockFile::create_or_open(dir.path(), false, 5.0, false).unwrap();
        assert!(Arc::ptr_eq(&first.shared, &second.shared));
        assert_eq!(
            registry().get(&first.key).map(|entry| entry.handles),
            Some(2)
        );
        let key = first.key;
        drop(first);
        drop(second);
        assert!(registry().get(&key).is_none());
    }

    #[test]
    fn a_second_handle_is_refused_the_write_lock_until_the_first_releases() {
        let dir = TempDir::new().unwrap();
        let mut first = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
        let mut second = LockFile::create_or_open(dir.path(), false, 5.0, false).unwrap();

        assert!(first.acquire(LockType::Write, 1).unwrap().is_acquired());
        assert_eq!(
            second.acquire(LockType::Write, 1).unwrap(),
            LockOutcome::HeldInProcess
        );
        // Waiting on a writer in this process could wait on this very
        // thread, so neither an indefinite nor a bounded wait waits.
        let started = Instant::now();
        for nattempts in [0, 3] {
            assert_eq!(
                second.acquire(LockType::Write, nattempts).unwrap(),
                LockOutcome::HeldInProcess
            );
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        // Reads within one process never conflict.
        assert!(second.acquire(LockType::Read, 1).unwrap().is_acquired());
        assert!(second.release().unwrap());

        assert!(first.release().unwrap());
        assert!(second.acquire(LockType::Write, 1).unwrap().is_acquired());
        assert_eq!(
            first.acquire(LockType::Write, 1).unwrap(),
            LockOutcome::HeldInProcess
        );
    }

    #[test]
    fn refusals_say_who_holds_the_lock() {
        let in_process = LockOutcome::HeldInProcess.refusal(LockType::Write, 0);
        assert!(in_process.contains("another handle on this table in this process"));
        let once = LockOutcome::HeldByAnotherProcess.refusal(LockType::Write, 1);
        assert!(once.contains("another process holds a read or write lock"));
        assert!(once.contains("one attempt"));
        let bounded = LockOutcome::HeldByAnotherProcess.refusal(LockType::Read, 5);
        assert!(bounded.contains("another process holds the write lock"));
        assert!(bounded.contains("5 attempts"));
        for message in [in_process, once, bounded] {
            assert!(!message.contains("write-locked"), "{message}");
        }
    }

    /// The request list is casacore's: a big-endian count followed by
    /// big-endian `(pid, hostid)` pairs, host id 0.
    #[test]
    fn request_ids_use_the_casacore_layout() {
        let dir = TempDir::new().unwrap();
        let lf = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
        let header = || std::fs::read(dir.path().join(LOCK_FILE_NAME)).unwrap()[..12].to_vec();

        lf.add_request_id().unwrap();
        let mut expected = 1i32.to_be_bytes().to_vec();
        expected.extend(lf.pid.to_be_bytes());
        expected.extend(0i32.to_be_bytes());
        assert_eq!(header(), expected);
        assert_eq!(lf.read_request_count().unwrap(), 1);

        lf.remove_request_id().unwrap();
        assert_eq!(header(), vec![0; 12]);
        assert_eq!(lf.read_request_count().unwrap(), 0);
    }

    #[test]
    fn releasing_one_handle_keeps_the_lock_another_still_holds() {
        let dir = TempDir::new().unwrap();
        let mut reader = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
        let mut writer = LockFile::create_or_open(dir.path(), false, 5.0, false).unwrap();
        assert!(reader.acquire(LockType::Read, 1).unwrap().is_acquired());
        assert!(writer.acquire(LockType::Write, 1).unwrap().is_acquired());
        assert_eq!(writer.shared.state().readers, 1);

        assert!(writer.release().unwrap());
        assert!(reader.has_lock(LockType::Read));
        assert_eq!(reader.shared.state().readers, 1);
        assert!(reader.shared.state().writer.is_none());

        // Dropping a handle that holds nothing leaves the reader's lock and
        // the shared descriptor in place.
        drop(writer);
        assert!(registry().contains_key(&reader.key));
        assert!(reader.release().unwrap());
    }

    #[test]
    fn sync_data_peek_reads_through_a_registered_descriptor() {
        let dir = TempDir::new().unwrap();
        let mut holder = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
        assert!(holder.acquire(LockType::Write, 1).unwrap().is_acquired());
        let sync = SyncData {
            nrrow: 7,
            nrcolumn: 1,
            modify_counter: 1,
            table_change_counter: 1,
            data_man_change_counters: vec![1],
        };
        holder.write_sync_data(&sync).unwrap();
        assert_eq!(
            read_sync_data_from_table_dir(dir.path()).unwrap(),
            Some(sync)
        );
        assert!(holder.has_lock(LockType::Write));
    }
}
