// SPDX-License-Identifier: LGPL-3.0-or-later
mod aipsio_oracle;
pub mod gridder_interop;
pub mod hogbom_interop;
mod image_oracle;
mod image_oracle_impl;
mod lattice_oracle;
mod lattice_oracle_impl;
pub mod measures_interop;
mod measures_oracle;
pub mod ms_interop;
mod oracle_ffi;
mod oracle_runtime;
pub mod quanta_interop;
pub mod table_interop;
pub mod table_measures_interop;
mod table_oracle;
mod table_oracle_impl;
pub mod table_quantum_interop;
pub mod taql_interop;
mod test_data;

pub use aipsio_oracle::*;
use casa_aipsio::{Complex32, Complex64};
pub use image_oracle::ImageOracle;
pub use image_oracle_impl::{
    CppImageExprBinaryOp, CppImageExprCompareOp, CppImageExprUnaryOp, CppMaskLogicalOp,
    CppRegionStatistics, CppUnsupportedRegionKind,
};
pub use lattice_oracle::LatticeOracle;
pub use lattice_oracle_impl::CppLatticeStatisticsBenchResult;
pub use oracle_runtime::OracleError;
pub use table_oracle::TableOracle;
pub use table_oracle_impl::{
    BulkScalarIoBenchResult, CellSliceBenchParams, CellSliceBenchResult, CppTableFixture,
    DeepCopyBenchResult, SetAlgebraBenchResult,
};
pub use test_data::*;

#[cfg(test)]
mod tests;
