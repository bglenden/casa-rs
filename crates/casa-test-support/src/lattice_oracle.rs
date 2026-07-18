// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed facade for the casacore lattice oracle.

use crate::CppLatticeStatisticsBenchResult;
#[cfg(has_casacore_cpp)]
use crate::lattice_oracle_impl::*;
use crate::oracle_runtime::OracleError;
#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::{CasacoreOracleRuntime, OracleDomain};

macro_rules! lattice_operation {
    ($operation:expr, $body:block) => {{
        #[cfg(has_casacore_cpp)]
        {
            CasacoreOracleRuntime::require($operation)?;
            let _guard = CasacoreOracleRuntime::lock(OracleDomain::Tables)?;
            $body
        }
        #[cfg(not(has_casacore_cpp))]
        {
            Err(OracleError::Unavailable {
                capability: $operation,
            })
        }
    }};
}

/// Stable Rust-facing domain facade.
pub struct LatticeOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl LatticeOracle {
    #[allow(clippy::too_many_arguments)]
    pub fn lattice_statistics_forced_io_bench(
        path: &std::path::Path,
        shape: &[i32],
        tile_shape: &[i32],
        cache_tiles: u64,
    ) -> Result<CppLatticeStatisticsBenchResult, OracleError> {
        lattice_operation!("lattice.lattice_statistics_forced_io_bench", {
            cpp_lattice_statistics_forced_io_bench(path, shape, tile_shape, cache_tiles).map_err(
                |message| OracleError::CppFailure {
                    operation: "lattice.lattice_statistics_forced_io_bench",
                    message,
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn lattice_statistics_forced_io_repeated_basic(
        path: &std::path::Path,
        shape: &[i32],
        tile_shape: &[i32],
        cache_tiles: u64,
        iterations: u32,
    ) -> Result<(u64, f64), OracleError> {
        lattice_operation!("lattice.lattice_statistics_forced_io_repeated_basic", {
            cpp_lattice_statistics_forced_io_repeated_basic(
                path,
                shape,
                tile_shape,
                cache_tiles,
                iterations,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "lattice.lattice_statistics_forced_io_repeated_basic",
                message,
            })
        })
    }
}
