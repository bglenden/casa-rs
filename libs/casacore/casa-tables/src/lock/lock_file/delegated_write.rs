// SPDX-License-Identifier: LGPL-3.0-or-later
//! Process-local delegation of a table write to a task provider.

use std::io;
use std::path::Path;
use std::sync::Arc;

use super::LockFile;

/// Reserves local data-lock acquisition for one writing task.
///
/// A writer presents the reservation when taking the table's normal kernel
/// lock. Other handles cannot acquire the local data lock while it is active.
pub struct DelegatedTableWrite(LockFile);

impl DelegatedTableWrite {
    /// Reserve a table once this process has released its data locks. Open
    /// handles can remain attached and synchronize when the reservation ends.
    ///
    /// # Errors
    /// Returns an error if the lock file cannot be opened, or if a local
    /// thread is acquiring, holding or delegating the lock.
    pub fn acquire(table_dir: &Path) -> io::Result<Self> {
        let lock = LockFile::create_or_open(table_dir, false, 0.0, false)?;
        {
            let mut state = lock.shared.state();
            if state.delegated_write.is_some()
                || state.acquisitions > 0
                || state.readers > 0
                || state.writer.is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "table lock held by this Python process or a local task is already using the table",
                ));
            }
            state.delegated_write = Some(lock.id);
        }
        Ok(Self(lock))
    }

    fn owns(&self, lock: &LockFile, owner: u64) -> bool {
        self.0.id == owner && Arc::ptr_eq(&self.0.shared, &lock.shared)
    }
}

pub(super) fn permits(
    owner: Option<u64>,
    lock: &LockFile,
    reservations: Option<&[DelegatedTableWrite]>,
) -> bool {
    owner.is_none_or(|owner| {
        reservations.is_some_and(|reservations| {
            reservations
                .iter()
                .any(|reservation| reservation.owns(lock, owner))
        })
    })
}

impl Drop for DelegatedTableWrite {
    fn drop(&mut self) {
        self.0.shared.state().delegated_write = None;
    }
}

#[cfg(test)]
mod tests {
    use super::super::LockType;
    use super::*;

    #[test]
    fn a_reservation_allows_only_its_own_writer() {
        let main = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let main_owner = DelegatedTableWrite::acquire(main.path()).unwrap();
        let other_owner = DelegatedTableWrite::acquire(other.path()).unwrap();
        let mut writer = LockFile::create_or_open(main.path(), false, 0.0, false).unwrap();

        let competing = writer.acquire(LockType::Read, 1).unwrap_err();
        assert_eq!(competing.kind(), io::ErrorKind::WouldBlock);
        let wrong_owner = writer
            .acquire_with_reservation(LockType::Write, 1, Some(std::slice::from_ref(&other_owner)))
            .unwrap_err();
        assert_eq!(wrong_owner.kind(), io::ErrorKind::WouldBlock);
        assert!(
            writer
                .acquire_with_reservation(
                    LockType::Write,
                    1,
                    Some(std::slice::from_ref(&main_owner))
                )
                .unwrap()
                .is_acquired()
        );
        assert!(writer.release().unwrap());
        drop(main_owner);
        assert!(writer.acquire(LockType::Read, 1).unwrap().is_acquired());
    }
}
