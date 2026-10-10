// SPDX-License-Identifier: LGPL-3.0-or-later
//! What a table's `table.lock` publishes to other processes, compared with
//! the table as persisted.
//!
//! casacore takes a reopened table's row count from the sync data in
//! `table.lock`, refuses a lock when its column count differs from the
//! table's, and asserts that it holds one change counter per data manager
//! (`PlainTable`, `ColumnSet::resync`). These helpers read both sides so a
//! test can check that a published write describes the persisted table.
//!
//! Reading `table.lock` through a separate descriptor and closing it drops
//! every `fcntl` lock the calling process holds on that file, so call
//! [`published_table_sync`] only while the process holds no lock on the
//! table.

use std::io::Cursor;
use std::path::Path;

use casa_aipsio::AipsIo;
use casa_tables::{Table, TableOptions};

/// Size of the request list at the start of `table.lock`.
const REQUEST_LIST_BYTES: usize = (1 + 2 * 32) * 4;

/// A table's row, column and data-manager counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableShape {
    /// Rows.
    pub rows: u64,
    /// Columns.
    pub columns: usize,
    /// Data managers (per-data-manager change counters in sync data).
    pub data_managers: usize,
}

/// The sync data a table's `table.lock` publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedSync {
    /// The published shape.
    pub shape: TableShape,
    /// The modify counter.
    pub modify_counter: u32,
}

/// The sync data in `table_dir/table.lock`, `None` when there is none.
///
/// # Panics
///
/// Panics when the sync data is present but malformed.
pub fn published_table_sync(table_dir: &Path) -> Option<PublishedSync> {
    let bytes = std::fs::read(table_dir.join("table.lock")).ok()?;
    let length_at = REQUEST_LIST_BYTES;
    let length = bytes.get(length_at..length_at + 4)?;
    let length = u32::from_be_bytes(length.try_into().expect("four bytes")) as usize;
    if length == 0 {
        return None;
    }
    let payload = bytes
        .get(length_at + 4..length_at + 4 + length)
        .expect("sync payload within table.lock")
        .to_vec();
    let mut io = AipsIo::new_read_only(Cursor::new(payload));
    let version = io.getstart("sync").expect("sync object");
    let rows = if version == 2 {
        io.get_u64().expect("rows")
    } else {
        u64::from(io.get_u32().expect("rows"))
    };
    let columns = io.get_i32().expect("columns");
    let modify_counter = io.get_u32().expect("modify counter");
    let data_managers = if columns >= 0 {
        io.get_u32().expect("table change counter");
        io.getstart("Block").expect("data-manager counters");
        let counters = io.getnew_u32().expect("data-manager counters");
        io.getend().expect("end of data-manager counters");
        counters.len()
    } else {
        0
    };
    io.getend().expect("end of sync object");
    Some(PublishedSync {
        shape: TableShape {
            rows,
            columns: columns.max(0) as usize,
            data_managers,
        },
        modify_counter,
    })
}

/// The table at `table_dir` as persisted in its `table.dat`: the row count
/// from the `table.dat` header (not the sync data, which a reopened table
/// trusts), and the columns and data managers it describes.
///
/// # Panics
///
/// Panics when the table cannot be read.
pub fn persisted_table_shape(table_dir: &Path) -> TableShape {
    let file = std::fs::File::open(table_dir.join("table.dat")).expect("open table.dat");
    let mut io = AipsIo::new_read_only(file);
    let version = io.getstart("Table").expect("Table object");
    let rows = if version >= 3 {
        io.get_u64().expect("rows")
    } else {
        u64::from(io.get_u32().expect("rows"))
    };
    let table = Table::open(TableOptions::new(table_dir)).expect("open the table");
    TableShape {
        rows,
        columns: table.schema().map_or(0, |schema| schema.columns().len()),
        data_managers: table.data_manager_info().len(),
    }
}
