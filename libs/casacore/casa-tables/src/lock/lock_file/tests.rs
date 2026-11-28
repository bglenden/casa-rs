// SPDX-License-Identifier: LGPL-3.0-or-later

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
    assert_eq!(outcome(libc::EDEADLK).unwrap(), FcntlOutcome::Deadlock);
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

/// casacore's `FileLocker::acquire(Read)` keeps a held write lock.
#[test]
fn a_read_request_keeps_a_held_write_lock() {
    let dir = TempDir::new().unwrap();
    let mut writer = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
    let mut other = LockFile::create_or_open(dir.path(), false, 5.0, false).unwrap();
    assert!(writer.acquire(LockType::Write, 1).unwrap().is_acquired());
    assert!(writer.acquire(LockType::Read, 1).unwrap().is_acquired());
    assert!(writer.has_lock(LockType::Write));
    assert_eq!(writer.shared.state().writer, Some(writer.id));
    assert_eq!(writer.shared.state().readers, 0);
    assert_eq!(
        other.acquire(LockType::Write, 1).unwrap(),
        LockOutcome::HeldInProcess
    );
    assert!(writer.release().unwrap());
    assert!(other.acquire(LockType::Write, 1).unwrap().is_acquired());
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
    let deadlock = LockOutcome::Deadlock.refusal(LockType::Write, 0);
    assert!(deadlock.contains("would deadlock with another process waiting for this table"));
    for message in [in_process, once, bounded, deadlock] {
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

#[test]
fn a_table_held_in_this_process_is_reported_before_any_other_holder() {
    let dir = TempDir::new().unwrap();
    assert_eq!(table_holder(dir.path()).unwrap(), None, "no lock file");
    let holder = LockFile::create_or_open(dir.path(), true, 5.0, false).unwrap();
    assert_eq!(
        table_holder(dir.path()).unwrap(),
        Some(TableHolder::ThisProcess)
    );
    drop(holder);
    assert_eq!(table_holder(dir.path()).unwrap(), None, "closed");
}
