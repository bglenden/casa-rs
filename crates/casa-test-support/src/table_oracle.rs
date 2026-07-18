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
