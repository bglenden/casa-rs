// SPDX-License-Identifier: LGPL-3.0-or-later
//! The rows a table holds on disk, which a partial in-place save must leave
//! `table.dat` describing (#880).
//!
//! casacore rewrites `table.dat` only when a table's structure changed or
//! the table is closed (`PlainTable::putFile`). Adding rows does not count
//! as a change, so between those writes it publishes new rows in
//! `table.lock`'s sync data alone, and `table.dat` trails them; a process
//! that never closes the table, as CASA 6.7.6's simulator after a mosaic
//! predict, leaves it so. casa-rs opens such a table with the published
//! count. A partial save reads `table.dat` back from disk and writes it
//! again, so it must bring the file's counts up to the table's: casacore
//! checks every storage manager's rows against them.

use super::*;
use crate::storage::table_control::TableDatContents;

/// The rows the table at `table_dir` holds as persisted, counted as
/// [`Table::open`] counts them: the largest of `table.dat`'s two counts and
/// the count the sync data publishes.
pub(super) fn persisted_row_count(table_dir: &Path, table_dat: &TableDatContents) -> u64 {
    table_dat
        .nrrow
        .max(table_dat.column_set.nrrow)
        .max(published_row_count(table_dir))
}

/// The row count `table.lock`'s sync data publishes for the table at
/// `table_dir`, 0 when there is none to read.
pub(super) fn published_row_count(table_dir: &Path) -> u64 {
    #[cfg(unix)]
    {
        crate::lock::read_sync_data_from_table_dir(table_dir)
            .ok()
            .flatten()
            .map_or(0, |sync| sync.nrrow)
    }
    #[cfg(not(unix))]
    {
        let _ = table_dir;
        0
    }
}

impl Table {
    /// Check that a partial in-place save may write the table at
    /// `table_dir`, whose `table.dat` is `table_dat`, and return the rows the
    /// table holds there.
    ///
    /// A partial save rewrites some data managers and leaves the others, so
    /// this handle's rows must be the ones on disk: none added, inserted or
    /// removed since the table was opened or last saved in full, and as many
    /// as the disk holds. Otherwise the rewritten managers would disagree
    /// with the others, or with `table.dat`, about the rows, and casacore
    /// refuses the table. The check writes nothing, so a refused save leaves
    /// the disk as it was.
    pub(super) fn require_persisted_rows(
        &self,
        table_dir: &Path,
        table_dat: &TableDatContents,
    ) -> Result<u64, TableError> {
        if self.inner.rows_reshaped() {
            return Err(TableError::Storage(format!(
                "a partial in-place save cannot write {}: rows were added or removed since \
                 the table was opened or saved in full; save the whole table instead",
                table_dir.display()
            )));
        }
        let persisted = persisted_row_count(table_dir, table_dat);
        if self.row_count() as u64 != persisted {
            return Err(TableError::Storage(format!(
                "a partial in-place save cannot write {}: it holds {persisted} rows on disk \
                 and this handle {}; reopen the table",
                table_dir.display(),
                self.row_count()
            )));
        }
        Ok(persisted)
    }

    /// Record a successful full save to `path`: when that is the table's own
    /// directory, its rows on disk now match this handle's.
    pub(super) fn note_full_save(&self, path: &Path) {
        if self
            .source_path
            .as_deref()
            .is_some_and(|source| locking::same_directory(source, path))
        {
            self.inner.note_rows_persisted();
        }
    }
}
