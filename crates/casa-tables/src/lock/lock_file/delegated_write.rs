// SPDX-License-Identifier: LGPL-3.0-or-later
//! Process-local delegation of a table write to a task provider.

use std::io;
use std::path::Path;

use super::LockFile;

/// Prevents local data-lock acquisition while a child process writes a table.
/// It leaves kernel lock acquisition to the child and other processes.
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
            if state.delegated_write
                || state.acquisitions > 0
                || state.readers > 0
                || state.writer.is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "table lock held by this Python process or a local task is already using the table",
                ));
            }
            state.delegated_write = true;
        }
        Ok(Self(lock))
    }
}

impl Drop for DelegatedTableWrite {
    fn drop(&mut self) {
        self.0.shared.state().delegated_write = false;
    }
}
