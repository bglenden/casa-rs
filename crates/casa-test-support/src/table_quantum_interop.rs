// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed oracle facade for casacore table quantum metadata.

use crate::oracle_runtime::OracleError;
#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::{CasacoreOracleRuntime, OracleDomain};

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn table_quantum_create_cpp(path: *const std::ffi::c_char) -> i32;
    fn table_quantum_read_cpp(
        path: *const std::ffi::c_char,
        sca_fixed_out: *mut f64,
        sca_var_out: *mut f64,
        sca_var_units_out: *mut std::ffi::c_char,
        unit_buf_len: i32,
    ) -> i32;
    fn table_quantum_verify_cpp(path: *const std::ffi::c_char, ok_out: *mut i32) -> i32;
    fn table_quantum_bench_scalar_read_cpp(
        path: *const std::ffi::c_char,
        column: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns_out: *mut u64,
    ) -> i32;
    fn table_quantum_bench_array_read_cpp(
        path: *const std::ffi::c_char,
        column: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns_out: *mut u64,
    ) -> i32;
}

macro_rules! table_quantum_operation {
    ($capability:expr, $body:block) => {{
        #[cfg(has_casacore_cpp)]
        {
            let _guard = CasacoreOracleRuntime::lock(OracleDomain::Tables)?;
            $body
        }
        #[cfg(not(has_casacore_cpp))]
        {
            Err(OracleError::Unavailable {
                capability: $capability,
            })
        }
    }};
}

/// Rust-facing access to casacore table quantum operations.
pub struct TableQuantumOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl TableQuantumOracle {
    pub fn create(path: &str) -> Result<(), OracleError> {
        table_quantum_operation!("table_quantum.create", {
            let path = CasacoreOracleRuntime::c_string("table path", path)?;
            CasacoreOracleRuntime::status("table_quantum.create", unsafe {
                table_quantum_create_cpp(path.as_ptr())
            })
        })
    }

    #[allow(clippy::type_complexity)]
    pub fn read(path: &str) -> Result<(Vec<f64>, Vec<f64>, Vec<String>), OracleError> {
        table_quantum_operation!("table_quantum.read", {
            let path = CasacoreOracleRuntime::c_string("table path", path)?;
            let mut fixed = [0.0; 3];
            let mut variable = [0.0; 3];
            const UNIT_BUFFER_LENGTH: i32 = 64;
            let mut unit_buffer = vec![0; 3 * UNIT_BUFFER_LENGTH as usize];
            let status = unsafe {
                table_quantum_read_cpp(
                    path.as_ptr(),
                    fixed.as_mut_ptr(),
                    variable.as_mut_ptr(),
                    unit_buffer.as_mut_ptr().cast(),
                    UNIT_BUFFER_LENGTH,
                )
            };
            CasacoreOracleRuntime::status("table_quantum.read", status)?;
            let units = unit_buffer
                .chunks(UNIT_BUFFER_LENGTH as usize)
                .map(|bytes| CasacoreOracleRuntime::output_string("table_quantum.read", bytes))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((fixed.to_vec(), variable.to_vec(), units))
        })
    }

    pub fn verify(path: &str) -> Result<bool, OracleError> {
        table_quantum_operation!("table_quantum.verify", {
            let path = CasacoreOracleRuntime::c_string("table path", path)?;
            let mut valid = 0;
            let status = unsafe { table_quantum_verify_cpp(path.as_ptr(), &mut valid) };
            CasacoreOracleRuntime::status("table_quantum.verify", status)?;
            Ok(valid == 1)
        })
    }

    pub fn bench_scalar_read(
        path: &str,
        column: &str,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        table_quantum_operation!("table_quantum.bench_scalar_read", {
            let path = CasacoreOracleRuntime::c_string("table path", path)?;
            let column = CasacoreOracleRuntime::c_string("column", column)?;
            let mut elapsed_ns = 0;
            let status = unsafe {
                table_quantum_bench_scalar_read_cpp(
                    path.as_ptr(),
                    column.as_ptr(),
                    iterations,
                    &mut elapsed_ns,
                )
            };
            CasacoreOracleRuntime::status("table_quantum.bench_scalar_read", status)?;
            Ok(elapsed_ns)
        })
    }

    pub fn bench_array_read(path: &str, column: &str, iterations: i32) -> Result<u64, OracleError> {
        table_quantum_operation!("table_quantum.bench_array_read", {
            let path = CasacoreOracleRuntime::c_string("table path", path)?;
            let column = CasacoreOracleRuntime::c_string("column", column)?;
            let mut elapsed_ns = 0;
            let status = unsafe {
                table_quantum_bench_array_read_cpp(
                    path.as_ptr(),
                    column.as_ptr(),
                    iterations,
                    &mut elapsed_ns,
                )
            };
            CasacoreOracleRuntime::status("table_quantum.bench_array_read", status)?;
            Ok(elapsed_ns)
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(has_casacore_cpp))]
    use super::TableQuantumOracle;

    #[cfg(not(has_casacore_cpp))]
    #[test]
    fn facade_is_stable_without_cpp() {
        assert!(matches!(
            TableQuantumOracle::create("unused"),
            Err(crate::OracleError::Unavailable { .. })
        ));
    }
}
