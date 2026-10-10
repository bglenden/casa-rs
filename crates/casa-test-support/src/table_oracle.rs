// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed facade for the casacore table oracle.

use crate::oracle_runtime::{OracleError, oracle_operation};
#[cfg(has_casacore_cpp)]
use crate::table_oracle_impl::*;
use crate::{
    BulkScalarIoBenchResult, CellSliceBenchParams, CellSliceBenchResult, CppTableFixture,
    DeepCopyBenchResult, SetAlgebraBenchResult,
};

macro_rules! table_operation {
    ($operation:expr, $body:block) => {{ oracle_operation!($operation, $body) }};
}

/// Stable Rust-facing domain facade.
pub struct TableOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl TableOracle {
    #[allow(clippy::too_many_arguments)]
    pub fn table_write(
        fixture: CppTableFixture,
        path: &std::path::Path,
    ) -> Result<(), OracleError> {
        table_operation!("table.table_write", {
            cpp_table_write_unlocked(fixture, path).map_err(|message| OracleError::CppFailure {
                operation: "table.table_write",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn table_verify(
        fixture: CppTableFixture,
        path: &std::path::Path,
    ) -> Result<(), OracleError> {
        table_operation!("table.table_verify", {
            cpp_table_verify_unlocked(fixture, path).map_err(|message| OracleError::CppFailure {
                operation: "table.table_verify",
                message,
            })
        })
    }

    /// Open the table at `path` in C++ casacore with `UserLocking`, take,
    /// release and retake an explicit read lock, and return `(rows,
    /// columns)` as the locked table reports them.
    ///
    /// Each lock synchronizes the open table with the sync data in
    /// `table.lock`, so this fails when that data does not describe the
    /// table on disk (column count, or one counter per data manager).
    pub fn lock_read_relock(path: &std::path::Path) -> Result<(u64, u32), OracleError> {
        table_operation!("table.lock_read_relock", {
            cpp_lock_read_relock(path).map_err(|message| OracleError::CppFailure {
                operation: "table.lock_read_relock",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn columns_index_time_lookups(
        path: &std::path::Path,
        key_value: i32,
        nqueries: u64,
    ) -> Result<(u64, u64), OracleError> {
        table_operation!("table.columns_index_time_lookups", {
            cpp_columns_index_time_lookups(path, key_value, nqueries).map_err(|message| {
                OracleError::CppFailure {
                    operation: "table.columns_index_time_lookups",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn vararray_bench(
        path: &std::path::Path,
        nrows: u64,
    ) -> Result<(u64, u64, u64), OracleError> {
        table_operation!("table.vararray_bench", {
            cpp_vararray_bench(path, nrows).map_err(|message| OracleError::CppFailure {
                operation: "table.vararray_bench",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_algebra_bench(
        path: &std::path::Path,
        nrows: u64,
        split_a: u64,
        split_b: u64,
    ) -> Result<SetAlgebraBenchResult, OracleError> {
        table_operation!("table.set_algebra_bench", {
            cpp_set_algebra_bench(path, nrows, split_a, split_b).map_err(|message| {
                OracleError::CppFailure {
                    operation: "table.set_algebra_bench",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn copy_rows_bench(dir: &std::path::Path, nrows: u64) -> Result<u64, OracleError> {
        table_operation!("table.copy_rows_bench", {
            cpp_copy_rows_bench(dir, nrows).map_err(|message| OracleError::CppFailure {
                operation: "table.copy_rows_bench",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn cell_slice_bench(
        path: &std::path::Path,
        params: &CellSliceBenchParams,
    ) -> Result<CellSliceBenchResult, OracleError> {
        table_operation!("table.cell_slice_bench", {
            cpp_cell_slice_bench(path, params).map_err(|message| OracleError::CppFailure {
                operation: "table.cell_slice_bench",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bulk_scalar_io_bench(
        path: &std::path::Path,
        nrows: u64,
    ) -> Result<BulkScalarIoBenchResult, OracleError> {
        table_operation!("table.bulk_scalar_io_bench", {
            cpp_bulk_scalar_io_bench(path, nrows).map_err(|message| OracleError::CppFailure {
                operation: "table.bulk_scalar_io_bench",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn deep_copy_bench(
        dir: &std::path::Path,
        nrows: u64,
    ) -> Result<DeepCopyBenchResult, OracleError> {
        table_operation!("table.deep_copy_bench", {
            cpp_deep_copy_bench(dir, nrows).map_err(|message| OracleError::CppFailure {
                operation: "table.deep_copy_bench",
                message,
            })
        })
    }
}

/// A C++ casacore reader that holds a table open between its lock periods,
/// as a casacore session holding a table open does.
///
/// The table is opened with `TableLock::UserLocking` and keeps its
/// `ScalarColumn<Int>` "id" open. Each [`read_id`](Self::read_id) takes a
/// read lock, reads `id(0)` and releases the lock without closing the table,
/// so what it read stays cached: casacore re-reads the table at the next lock
/// only when the sync data in `table.lock` says another process wrote it
/// (`PlainTable::lock`, `ColumnSet::resync`). It therefore observes whether a
/// writer announced its write, which a table opened afresh cannot.
///
/// `fcntl` locks belong to a process, so a writer whose announcement is under
/// test must run in another process than this reader; the
/// `casacore-held-table-reader` binary runs one. Keep no other casacore oracle
/// operation on the same table in the reader's process.
pub struct HeldCppTableReader {
    #[cfg(has_casacore_cpp)]
    handle: Option<CppHeldReaderHandle>,
}

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl HeldCppTableReader {
    /// Open the table at `path`, which has an Int column "id", without
    /// taking a lock.
    pub fn open(path: &std::path::Path) -> Result<Self, OracleError> {
        table_operation!("table.held_reader_open", {
            cpp_held_reader_open(path)
                .map(|handle| Self {
                    handle: Some(handle),
                })
                .map_err(|message| OracleError::CppFailure {
                    operation: "table.held_reader_open",
                    message,
                })
        })
    }

    /// Take a read lock, read `id(0)` and release the lock, keeping the
    /// table open.
    pub fn read_id(&mut self) -> Result<i32, OracleError> {
        table_operation!("table.held_reader_read_id", {
            let handle = self.handle.as_mut().expect("an open reader");
            cpp_held_reader_read_id(handle).map_err(|message| OracleError::CppFailure {
                operation: "table.held_reader_read_id",
                message,
            })
        })
    }
}

impl Drop for HeldCppTableReader {
    fn drop(&mut self) {
        #[cfg(has_casacore_cpp)]
        if let Some(handle) = self.handle.take() {
            let _guard = crate::oracle_runtime::CasacoreOracleRuntime::lock_operation(
                "table.held_reader_close",
            );
            cpp_held_reader_close(handle);
        }
    }
}
