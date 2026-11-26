// SPDX-License-Identifier: LGPL-3.0-or-later
//! Interruptible lock acquisition and request-list cleanup for `LockFile`.

use std::io;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{
    Acquisition, DelegatedTableWrite, LockFile, LockOutcome, LockType, NRREQID, SIZEINT, SIZEREQID,
    SharedLockFd, WAIT_REPORT_INTERVAL, delegated_write, lock_name, read_at, write_at,
};

struct RequestIdGuard {
    shared: Arc<SharedLockFd>,
    pid: i32,
    active: bool,
}

impl Drop for RequestIdGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = remove_request_id_from_fd(self.shared.fd, self.pid);
        }
    }
}

/// Remove `pid` from the request list at the head of `fd`.
pub(super) fn remove_request_id_from_fd(fd: RawFd, target_pid: i32) -> io::Result<()> {
    let mut header = [0u8; SIZEREQID];
    let n = read_at(fd, &mut header, 0)?;
    if n < SIZEINT {
        return Ok(());
    }

    let count = i32::from_be_bytes(header[0..4].try_into().unwrap());
    if count <= 0 {
        return Ok(());
    }

    let mut found = false;
    for i in 0..count.min(NRREQID as i32) as usize {
        let pid_offset = (1 + 2 * i) * SIZEINT;
        let pid = i32::from_be_bytes(header[pid_offset..pid_offset + 4].try_into().unwrap());
        if pid == target_pid {
            let remaining = count as usize - i - 1;
            if remaining > 0 {
                let src_start = (1 + 2 * (i + 1)) * SIZEINT;
                let dst_start = pid_offset;
                let len = remaining * 2 * SIZEINT;
                header.copy_within(src_start..src_start + len, dst_start);
            }
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

impl LockFile {
    /// Wait for `lock_type` under `reservations` while polling `cancelled`,
    /// registering this process's PID in `table.lock`'s request list while
    /// waiting so casacore `AutoLocking` holders release on inspection.
    pub(in crate::lock) fn acquire_with_reservation_interruptibly(
        &mut self,
        lock_type: LockType,
        reservations: Option<&[DelegatedTableWrite]>,
        cancelled: &dyn Fn() -> bool,
    ) -> io::Result<LockOutcome> {
        let shared = Arc::clone(&self.shared);
        let _acquisition = {
            let mut state = shared.state();
            if !delegated_write::permits(state.delegated_write, self, reservations) {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "a task provider is using this table",
                ));
            }
            state.acquisitions += 1;
            Acquisition(&shared)
        };

        let mut outcome = self.try_acquire(lock_type)?;
        if outcome == LockOutcome::HeldByAnotherProcess {
            let table = self.path.parent().unwrap_or(&self.path).to_path_buf();
            let lock = lock_name(lock_type);
            tracing::warn!(
                table = %table.display(),
                lock,
                "table {} is locked by another process; waiting for the {lock} lock",
                table.display()
            );
            let added = self.add_to_list && self.shared.writable && self.add_request_id().is_ok();
            let request_guard = RequestIdGuard {
                shared: Arc::clone(&self.shared),
                pid: self.pid,
                active: added,
            };
            let started = Instant::now();
            let mut last_report = Instant::now();
            while outcome == LockOutcome::HeldByAnotherProcess && !cancelled() {
                std::thread::sleep(Duration::from_millis(50));
                if last_report.elapsed() >= WAIT_REPORT_INTERVAL {
                    let waited_s = started.elapsed().as_secs_f64();
                    tracing::info!(
                        table = %table.display(),
                        lock,
                        waited_s,
                        "still waiting for the {lock} lock on table {} after {waited_s:.0} s",
                        table.display()
                    );
                    last_report = Instant::now();
                }
                if !cancelled() {
                    outcome = self.try_acquire(lock_type)?;
                }
            }
            drop(request_guard);
            if outcome.is_acquired() && !cancelled() {
                let waited_s = started.elapsed().as_secs_f64();
                tracing::info!(
                    table = %table.display(),
                    lock,
                    waited_s,
                    "acquired the {lock} lock on table {} after waiting {waited_s:.1} s",
                    table.display()
                );
            }
        }

        self.last_inspect = Instant::now();
        self.inspect_count = 0;

        if cancelled() {
            if outcome.is_acquired() {
                let _ = self.release();
            }
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "task cancelled while acquiring table write lock",
            ));
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::{Duration, Instant};

    use tempfile::TempDir;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    use super::*;
    use crate::{TableError, TableWriteLock};

    const HOLD_LOCK_DIR_ENV: &str = "CASA_RS_TEST_HOLD_WRITE_LOCK_DIR";
    const HOLD_LOCK_READY_ENV: &str = "CASA_RS_TEST_HOLD_WRITE_LOCK_READY";
    const HOLD_LOCK_RELEASE_ENV: &str = "CASA_RS_TEST_HOLD_WRITE_LOCK_RELEASE";

    #[derive(Default)]
    struct LogCapture {
        messages: Mutex<Vec<String>>,
    }

    impl LogCapture {
        fn snapshot(&self) -> Vec<String> {
            self.messages
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    struct MessageVisitor<'a>(&'a mut Option<String>);

    impl Visit for MessageVisitor<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                *self.0 = Some(format!("{value:?}"));
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "message" {
                *self.0 = Some(value.to_string());
            }
        }
    }

    struct CaptureSubscriber(Arc<LogCapture>);

    impl Subscriber for CaptureSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut message = None;
            event.record(&mut MessageVisitor(&mut message));
            if let Some(message) = message {
                self.0
                    .messages
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(message);
            }
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    fn wait_for_file(path: &Path, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if path.exists() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    #[test]
    fn hold_write_lock_for_parent() {
        let Some(dir) = std::env::var_os(HOLD_LOCK_DIR_ENV) else {
            return;
        };
        let ready = std::env::var_os(HOLD_LOCK_READY_ENV).expect("ready path");
        let release = std::env::var_os(HOLD_LOCK_RELEASE_ENV).expect("release path");

        let mut lock = LockFile::create_or_open(Path::new(&dir), false, 5.0, false).unwrap();
        assert!(lock.acquire(LockType::Write, 1).unwrap().is_acquired());
        std::fs::write(&ready, b"held").unwrap();
        let _ = wait_for_file(Path::new(&release), Duration::from_secs(15));
        let _ = lock.release();
    }

    fn spawn_write_lock_holder(dir: &Path, ready: &Path, release: &Path) -> std::process::Child {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lock::lock_file::wait::tests::hold_write_lock_for_parent",
            ])
            .env(HOLD_LOCK_DIR_ENV, dir)
            .env(HOLD_LOCK_READY_ENV, ready)
            .env(HOLD_LOCK_RELEASE_ENV, release)
            .spawn()
            .expect("spawn lock holder child");
        assert!(
            wait_for_file(ready, Duration::from_secs(10)),
            "child did not acquire write lock in time"
        );
        child
    }

    #[test]
    fn interruptible_write_lock_registers_request_id_and_removes_on_cancel() {
        let tmp = TempDir::new().unwrap();
        let table_dir = tmp.path().join("table");
        std::fs::create_dir_all(&table_dir).unwrap();
        drop(LockFile::create_or_open(&table_dir, true, 5.0, false).unwrap());

        let ready = tmp.path().join("ready");
        let release = tmp.path().join("release");
        let mut child = spawn_write_lock_holder(&table_dir, &ready, &release);

        let reservation = DelegatedTableWrite::acquire(&table_dir).unwrap();
        let reservations: Arc<[DelegatedTableWrite]> = Arc::from(vec![reservation]);
        let watcher = LockFile::create_or_open(&table_dir, false, 5.0, false).unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));

        let saw_request = std::thread::scope(|scope| {
            let cancelled_flag = Arc::clone(&cancelled);
            let watcher_ref = &watcher;
            let cancel_watcher = scope.spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline {
                    if watcher_ref.read_request_count().unwrap_or(0) >= 1 {
                        cancelled_flag.store(true, Ordering::Release);
                        return true;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                cancelled_flag.store(true, Ordering::Release);
                false
            });

            let capture = Arc::new(LogCapture::default());
            let result =
                tracing::subscriber::with_default(CaptureSubscriber(Arc::clone(&capture)), || {
                    TableWriteLock::acquire_reserved_interruptibly(
                        &table_dir,
                        &reservations,
                        &|| cancelled.load(Ordering::Acquire),
                    )
                });

            let saw_request = cancel_watcher.join().unwrap();
            assert!(
                matches!(result, Err(TableError::LockIo { .. })),
                "expected TableError::LockIo on cancel, got {result:?}"
            );
            let messages = capture.snapshot();
            assert!(
                messages
                    .iter()
                    .any(|m| m.contains("waiting for the write lock")),
                "expected wait warning in logs, got {messages:?}"
            );
            saw_request
        });

        assert!(saw_request, "waiter never wrote PID into request list");
        assert_eq!(
            watcher.read_request_count().unwrap(),
            0,
            "PID must be removed from request list after cancellation"
        );

        drop(watcher);
        drop(reservations);
        std::fs::write(&release, b"go").unwrap();
        assert!(child.wait().unwrap().success());
    }

    #[test]
    fn interruptible_write_lock_logs_under_thread_local_subscriber_and_acquires() {
        let tmp = TempDir::new().unwrap();
        let table_dir = tmp.path().join("table");
        std::fs::create_dir_all(&table_dir).unwrap();
        drop(LockFile::create_or_open(&table_dir, true, 5.0, false).unwrap());

        let ready = tmp.path().join("ready");
        let release = tmp.path().join("release");
        let mut child = spawn_write_lock_holder(&table_dir, &ready, &release);

        let reservation = DelegatedTableWrite::acquire(&table_dir).unwrap();
        let reservations: Arc<[DelegatedTableWrite]> = Arc::from(vec![reservation]);
        let watcher = LockFile::create_or_open(&table_dir, false, 5.0, false).unwrap();

        let capture = Arc::new(LogCapture::default());
        let lock = std::thread::scope(|scope| {
            let watcher_ref = &watcher;
            let release_path = release.as_path();
            let releaser = scope.spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline {
                    if watcher_ref.read_request_count().unwrap_or(0) >= 1 {
                        std::fs::write(release_path, b"go").unwrap();
                        return true;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                std::fs::write(release_path, b"go").unwrap();
                false
            });

            let lock =
                tracing::subscriber::with_default(CaptureSubscriber(Arc::clone(&capture)), || {
                    TableWriteLock::acquire_reserved_interruptibly(
                        &table_dir,
                        &reservations,
                        &|| false,
                    )
                });
            assert!(releaser.join().unwrap());
            lock.unwrap()
        });

        assert_eq!(watcher.read_request_count().unwrap(), 0);
        let messages = capture.snapshot();
        assert!(
            messages
                .iter()
                .any(|m| m.contains("waiting for the write lock")),
            "expected wait warning, got {messages:?}"
        );
        assert!(
            messages
                .iter()
                .any(|m| m.contains("acquired the write lock")),
            "expected acquired log, got {messages:?}"
        );

        lock.release().unwrap();
        drop(watcher);
        drop(reservations);
        assert!(child.wait().unwrap().success());
    }

    #[test]
    fn wait_report_thread_receives_thread_local_dispatcher() {
        let capture = Arc::new(LogCapture::default());
        tracing::subscriber::with_default(CaptureSubscriber(Arc::clone(&capture)), || {
            let dispatch = tracing::dispatcher::get_default(Clone::clone);
            let (stop, stopped) = std::sync::mpsc::channel::<()>();
            std::thread::scope(|scope| {
                scope.spawn(move || {
                    tracing::dispatcher::with_default(&dispatch, move || {
                        if let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                            stopped.recv_timeout(Duration::from_millis(5))
                        {
                            tracing::info!(
                                "still waiting for the write lock on table test after 0 s"
                            );
                        }
                    });
                });
                std::thread::sleep(Duration::from_millis(30));
                drop(stop);
            });
        });

        let messages = capture.snapshot();
        assert!(
            messages
                .iter()
                .any(|m| m.contains("still waiting for the write lock")),
            "expected thread-local subscriber to receive spawned thread log, got {messages:?}"
        );
    }
}
