// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed oracle facade for casacore table measure metadata.

use crate::oracle_runtime::OracleError;
#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::{CasacoreOracleRuntime, OracleDomain};

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn table_meas_create_epoch_fixed(path: *const std::ffi::c_char) -> i32;
    fn table_meas_create_epoch_var_int(path: *const std::ffi::c_char) -> i32;
    fn table_meas_create_epoch_var_str(path: *const std::ffi::c_char) -> i32;
    fn table_meas_create_direction_fixed(path: *const std::ffi::c_char) -> i32;
    fn table_meas_read_epochs(
        path: *const std::ffi::c_char,
        column: *const std::ffi::c_char,
        rows: i32,
        values_out: *mut f64,
        references_out: *mut std::ffi::c_char,
        reference_buffer_length: i32,
    ) -> i32;
    fn table_meas_read_directions(
        path: *const std::ffi::c_char,
        column: *const std::ffi::c_char,
        rows: i32,
        values_out: *mut f64,
        references_out: *mut std::ffi::c_char,
        reference_buffer_length: i32,
    ) -> i32;
    fn table_meas_verify_epochs(
        path: *const std::ffi::c_char,
        column: *const std::ffi::c_char,
        rows: i32,
        expected_mjds: *const f64,
        expected_references: *const std::ffi::c_char,
        reference_buffer_length: i32,
    ) -> i32;
    fn table_meas_verify_directions(
        path: *const std::ffi::c_char,
        column: *const std::ffi::c_char,
        rows: i32,
        expected_values: *const f64,
        expected_references: *const std::ffi::c_char,
        reference_buffer_length: i32,
    ) -> i32;
    fn table_meas_bench_epoch_read(
        path: *const std::ffi::c_char,
        column: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns_out: *mut u64,
    ) -> i32;
    fn table_meas_bench_direction_read(
        path: *const std::ffi::c_char,
        column: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns_out: *mut u64,
    ) -> i32;
}

macro_rules! table_measure_operation {
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

/// Rust-facing access to casacore table-measure operations.
pub struct TableMeasuresOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl TableMeasuresOracle {
    pub fn create_epoch_fixed(path: &str) -> Result<(), OracleError> {
        table_measure_operation!("table_measures.create_epoch_fixed", {
            create_table(
                "table_measures.create_epoch_fixed",
                path,
                table_meas_create_epoch_fixed,
            )
        })
    }

    pub fn create_epoch_variable_integer(path: &str) -> Result<(), OracleError> {
        table_measure_operation!("table_measures.create_epoch_variable_integer", {
            create_table(
                "table_measures.create_epoch_variable_integer",
                path,
                table_meas_create_epoch_var_int,
            )
        })
    }

    pub fn create_epoch_variable_string(path: &str) -> Result<(), OracleError> {
        table_measure_operation!("table_measures.create_epoch_variable_string", {
            create_table(
                "table_measures.create_epoch_variable_string",
                path,
                table_meas_create_epoch_var_str,
            )
        })
    }

    pub fn create_direction_fixed(path: &str) -> Result<(), OracleError> {
        table_measure_operation!("table_measures.create_direction_fixed", {
            create_table(
                "table_measures.create_direction_fixed",
                path,
                table_meas_create_direction_fixed,
            )
        })
    }

    pub fn read_epochs(
        path: &str,
        column: &str,
        rows: usize,
    ) -> Result<(Vec<f64>, Vec<String>), OracleError> {
        table_measure_operation!("table_measures.read_epochs", {
            read_values(
                "table_measures.read_epochs",
                path,
                column,
                rows,
                1,
                table_meas_read_epochs,
            )
        })
    }

    pub fn read_directions(
        path: &str,
        column: &str,
        rows: usize,
    ) -> Result<(Vec<f64>, Vec<String>), OracleError> {
        table_measure_operation!("table_measures.read_directions", {
            read_values(
                "table_measures.read_directions",
                path,
                column,
                rows,
                2,
                table_meas_read_directions,
            )
        })
    }

    pub fn verify_epochs(
        path: &str,
        column: &str,
        expected_mjds: &[f64],
        expected_references: &[&str],
    ) -> Result<(), OracleError> {
        table_measure_operation!("table_measures.verify_epochs", {
            if expected_mjds.len() != expected_references.len() {
                return Err(OracleError::InvalidInput {
                    context: "table measure epoch expectations",
                    message: "values and references must have matching lengths".to_owned(),
                });
            }
            let path = CasacoreOracleRuntime::c_string("table path", path)?;
            let column = CasacoreOracleRuntime::c_string("column", column)?;
            let references = pack_references(expected_references);
            CasacoreOracleRuntime::status("table_measures.verify_epochs", unsafe {
                table_meas_verify_epochs(
                    path.as_ptr(),
                    column.as_ptr(),
                    expected_mjds.len() as i32,
                    expected_mjds.as_ptr(),
                    references.as_ptr().cast(),
                    REFERENCE_BUFFER_LENGTH,
                )
            })
        })
    }

    pub fn verify_directions(
        path: &str,
        column: &str,
        expected_values: &[f64],
        expected_references: &[&str],
    ) -> Result<(), OracleError> {
        table_measure_operation!("table_measures.verify_directions", {
            if expected_values.len() != expected_references.len() * 2 {
                return Err(OracleError::InvalidInput {
                    context: "table measure direction expectations",
                    message: "each reference requires two direction values".to_owned(),
                });
            }
            let path = CasacoreOracleRuntime::c_string("table path", path)?;
            let column = CasacoreOracleRuntime::c_string("column", column)?;
            let references = pack_references(expected_references);
            CasacoreOracleRuntime::status("table_measures.verify_directions", unsafe {
                table_meas_verify_directions(
                    path.as_ptr(),
                    column.as_ptr(),
                    expected_references.len() as i32,
                    expected_values.as_ptr(),
                    references.as_ptr().cast(),
                    REFERENCE_BUFFER_LENGTH,
                )
            })
        })
    }

    pub fn bench_epoch_read(path: &str, column: &str, iterations: i32) -> Result<u64, OracleError> {
        table_measure_operation!("table_measures.bench_epoch_read", {
            bench_read(
                "table_measures.bench_epoch_read",
                path,
                column,
                iterations,
                table_meas_bench_epoch_read,
            )
        })
    }

    pub fn bench_direction_read(
        path: &str,
        column: &str,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        table_measure_operation!("table_measures.bench_direction_read", {
            bench_read(
                "table_measures.bench_direction_read",
                path,
                column,
                iterations,
                table_meas_bench_direction_read,
            )
        })
    }
}

#[cfg(has_casacore_cpp)]
const REFERENCE_BUFFER_LENGTH: i32 = 32;

#[cfg(has_casacore_cpp)]
fn create_table(
    operation: &'static str,
    path: &str,
    call: unsafe extern "C" fn(*const std::ffi::c_char) -> i32,
) -> Result<(), OracleError> {
    let path = CasacoreOracleRuntime::c_string("table path", path)?;
    CasacoreOracleRuntime::status(operation, unsafe { call(path.as_ptr()) })
}

#[cfg(has_casacore_cpp)]
fn read_values(
    operation: &'static str,
    path: &str,
    column: &str,
    rows: usize,
    values_per_row: usize,
    call: unsafe extern "C" fn(
        *const std::ffi::c_char,
        *const std::ffi::c_char,
        i32,
        *mut f64,
        *mut std::ffi::c_char,
        i32,
    ) -> i32,
) -> Result<(Vec<f64>, Vec<String>), OracleError> {
    let path = CasacoreOracleRuntime::c_string("table path", path)?;
    let column = CasacoreOracleRuntime::c_string("column", column)?;
    let mut values = vec![0.0; rows * values_per_row];
    let mut references = vec![0; rows * REFERENCE_BUFFER_LENGTH as usize];
    let status = unsafe {
        call(
            path.as_ptr(),
            column.as_ptr(),
            rows as i32,
            values.as_mut_ptr(),
            references.as_mut_ptr().cast(),
            REFERENCE_BUFFER_LENGTH,
        )
    };
    CasacoreOracleRuntime::status(operation, status)?;
    let references = references
        .chunks(REFERENCE_BUFFER_LENGTH as usize)
        .map(|bytes| CasacoreOracleRuntime::output_string(operation, bytes))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((values, references))
}

#[cfg(has_casacore_cpp)]
fn pack_references(references: &[&str]) -> Vec<u8> {
    let mut packed = vec![0; references.len() * REFERENCE_BUFFER_LENGTH as usize];
    for (index, reference) in references.iter().enumerate() {
        let start = index * REFERENCE_BUFFER_LENGTH as usize;
        let length = reference.len().min(REFERENCE_BUFFER_LENGTH as usize - 1);
        packed[start..start + length].copy_from_slice(&reference.as_bytes()[..length]);
    }
    packed
}

#[cfg(has_casacore_cpp)]
fn bench_read(
    operation: &'static str,
    path: &str,
    column: &str,
    iterations: i32,
    call: unsafe extern "C" fn(
        *const std::ffi::c_char,
        *const std::ffi::c_char,
        i32,
        *mut u64,
    ) -> i32,
) -> Result<u64, OracleError> {
    let path = CasacoreOracleRuntime::c_string("table path", path)?;
    let column = CasacoreOracleRuntime::c_string("column", column)?;
    let mut elapsed_ns = 0;
    let status = unsafe { call(path.as_ptr(), column.as_ptr(), iterations, &mut elapsed_ns) };
    CasacoreOracleRuntime::status(operation, status)?;
    Ok(elapsed_ns)
}

#[cfg(test)]
mod tests {
    #[cfg(not(has_casacore_cpp))]
    use super::TableMeasuresOracle;

    #[cfg(not(has_casacore_cpp))]
    #[test]
    fn facade_is_stable_without_cpp() {
        assert!(matches!(
            TableMeasuresOracle::create_epoch_fixed("unused"),
            Err(crate::OracleError::Unavailable { .. })
        ));
    }
}
