// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use casa_types::RecordValue;

use super::table_control::{TableDatResult, read_table_dat_dispatch, serialize_table_dat};
use super::{StorageError, TABLE_CONTROL_FILE};
use crate::publication::check_write_access;
use crate::table_path::stored_table_dat_keywords;

pub(crate) fn save_keywords(
    table: &Path,
    row_count: u64,
    keywords: &RecordValue,
    sync: fn(&File) -> io::Result<()>,
) -> Result<(), StorageError> {
    let path = table.join(TABLE_CONTROL_FILE);
    let metadata = path.symlink_metadata()?;
    if !metadata.is_file() {
        return Err(StorageError::FormatMismatch(
            "atomic keyword save needs a regular table.dat".into(),
        ));
    }
    check_write_access(&path)?;
    let TableDatResult::Plain(mut contents) = read_table_dat_dispatch(&path)? else {
        return Err(StorageError::FormatMismatch(
            "atomic keyword save is only supported for PlainTable storage".into(),
        ));
    };
    contents.raise_row_count(row_count);
    contents.table_desc.table_keywords = keywords.clone();
    let bytes = serialize_table_dat(&stored_table_dat_keywords(&contents, table))?;
    let mut staged = tempfile::NamedTempFile::new_in(table)?;
    staged.write_all(&bytes)?;
    staged.as_file().set_permissions(metadata.permissions())?;
    sync(staged.as_file())?;
    staged.persist(&path).map_err(|error| error.error)?;
    Ok(())
}

pub(crate) fn sync_cache_keywords(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;

        // File::sync_all requests F_FULLFSYNC on Apple. The sorted cache is
        // derived from published rows and uses casacore's ordinary fsync.
        loop {
            // SAFETY: the borrowed file keeps the descriptor alive.
            if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
    #[cfg(not(target_vendor = "apple"))]
    file.sync_all()
}
