// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ casacore MeasurementSet interop shims.
//!
//! These helpers exercise `casacore::MeasurementSet` directly rather than the
//! lower-level generic `Table` fixture layer.

use std::path::Path;

#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;
use crate::oracle_runtime::{OracleError, oracle_operation};

/// Timings for a C++ MeasurementSet create/open/read workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsBenchResult {
    pub create_ns: u64,
    pub open_ns: u64,
    pub read_ns: u64,
}

/// Timings for reading all rows of the MAIN table from an external MeasurementSet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsMainRowsBenchResult {
    pub read_ns: u64,
    pub rows_digest: String,
}

/// Timings for opening an external MeasurementSet and then reading the full
/// MAIN-table row stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsMainOpenScanBenchResult {
    pub open_and_read_ns: u64,
    pub rows_digest: String,
}

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    #[link_name = "cpp_ms_write_basic_fixture"]
    fn ffi_cpp_ms_write_basic_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_ms_verify_basic_fixture"]
    fn ffi_cpp_ms_verify_basic_fixture(
        path: *const std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_ms_bench_create_open"]
    fn ffi_cpp_ms_bench_create_open(
        path: *const std::ffi::c_char,
        nrows: u64,
        out_create_ns: *mut u64,
        out_open_ns: *mut u64,
        out_read_ns: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_ms_digest_manifest"]
    fn ffi_cpp_ms_digest_manifest(
        path: *const std::ffi::c_char,
        out_manifest: *mut *mut std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_ms_table_row_digest"]
    fn ffi_cpp_ms_table_row_digest(
        path: *const std::ffi::c_char,
        table_label: *const std::ffi::c_char,
        row: u64,
        out_digest: *mut *mut std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_ms_table_row_field_manifest"]
    fn ffi_cpp_ms_table_row_field_manifest(
        path: *const std::ffi::c_char,
        table_label: *const std::ffi::c_char,
        row: u64,
        out_manifest: *mut *mut std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_ms_bench_main_rows"]
    fn ffi_cpp_ms_bench_main_rows(
        path: *const std::ffi::c_char,
        out_read_ns: *mut u64,
        out_digest: *mut *mut std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    #[link_name = "cpp_ms_bench_open_main_rows"]
    fn ffi_cpp_ms_bench_open_main_rows(
        path: *const std::ffi::c_char,
        out_open_and_read_ns: *mut u64,
        out_digest: *mut *mut std::ffi::c_char,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_free_error(ptr: *mut std::ffi::c_char);
}

/// Typed Rust-facing access to the casacore MeasurementSet oracle.
pub struct MeasurementSetOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl MeasurementSetOracle {
    /// Write the standard MeasurementSet interop fixture using C++ casacore.
    pub fn write_basic_fixture(path: &Path) -> Result<(), OracleError> {
        oracle_operation!("measurement_set.write_basic_fixture", {
            let c_path = CasacoreOracleRuntime::c_path("MeasurementSet path", path)?;
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe { ffi_cpp_ms_write_basic_fixture(c_path.as_ptr(), &mut error) };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "measurement_set.write_basic_fixture",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            Ok(())
        })
    }

    /// Verify the standard MeasurementSet interop fixture using C++ casacore.
    pub fn verify_basic_fixture(path: &Path) -> Result<(), OracleError> {
        oracle_operation!("measurement_set.verify_basic_fixture", {
            let c_path = CasacoreOracleRuntime::c_path("MeasurementSet path", path)?;
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe { ffi_cpp_ms_verify_basic_fixture(c_path.as_ptr(), &mut error) };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "measurement_set.verify_basic_fixture",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            Ok(())
        })
    }

    /// Benchmark C++ MeasurementSet create/open/read for the standard fixture.
    pub fn bench_create_open(path: &Path, nrows: u64) -> Result<MsBenchResult, OracleError> {
        oracle_operation!("measurement_set.bench_create_open", {
            let c_path = CasacoreOracleRuntime::c_path("MeasurementSet path", path)?;
            let mut create_ns = 0_u64;
            let mut open_ns = 0_u64;
            let mut read_ns = 0_u64;
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe {
                ffi_cpp_ms_bench_create_open(
                    c_path.as_ptr(),
                    nrows,
                    &mut create_ns,
                    &mut open_ns,
                    &mut read_ns,
                    &mut error,
                )
            };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "measurement_set.bench_create_open",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            Ok(MsBenchResult {
                create_ns,
                open_ns,
                read_ns,
            })
        })
    }

    /// Return a stable digest manifest for a MeasurementSet opened by C++ casacore.
    ///
    /// The manifest is intended for Rust↔C++ parity tests on real-world
    /// MeasurementSets. It traverses the main table, standard MS subtables, and
    /// any additional tables reachable through `TpTable` keyword references,
    /// returning compact per-table digests rather than a huge textual dump.
    pub fn digest_manifest(path: &Path) -> Result<String, OracleError> {
        oracle_operation!("measurement_set.digest_manifest", {
            let c_path = CasacoreOracleRuntime::c_path("MeasurementSet path", path)?;
            let mut manifest: *mut std::ffi::c_char = std::ptr::null_mut();
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc =
                unsafe { ffi_cpp_ms_digest_manifest(c_path.as_ptr(), &mut manifest, &mut error) };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "measurement_set.digest_manifest",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            unsafe {
                CasacoreOracleRuntime::owned_string(
                    "measurement_set.digest_manifest",
                    manifest,
                    cpp_table_free_error,
                )
            }
        })
    }

    /// Return the stable digest for a single row of a named MeasurementSet table.
    ///
    /// `table_label` uses the same naming as the MS verifier, for example
    /// `"MAIN"`, `"SOURCE"`, or `"SYSCAL"`.
    pub fn table_row_digest(
        path: &Path,
        table_label: &str,
        row: u64,
    ) -> Result<String, OracleError> {
        oracle_operation!("measurement_set.table_row_digest", {
            let c_path = CasacoreOracleRuntime::c_path("MeasurementSet path", path)?;
            let c_label =
                CasacoreOracleRuntime::c_string("MeasurementSet table label", table_label)?;
            let mut digest: *mut std::ffi::c_char = std::ptr::null_mut();
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe {
                ffi_cpp_ms_table_row_digest(
                    c_path.as_ptr(),
                    c_label.as_ptr(),
                    row,
                    &mut digest,
                    &mut error,
                )
            };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "measurement_set.table_row_digest",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            unsafe {
                CasacoreOracleRuntime::owned_string(
                    "measurement_set.table_row_digest",
                    digest,
                    cpp_table_free_error,
                )
            }
        })
    }

    /// Return a per-field digest manifest for a single row of a named MS table.
    pub fn table_row_field_manifest(
        path: &Path,
        table_label: &str,
        row: u64,
    ) -> Result<String, OracleError> {
        oracle_operation!("measurement_set.table_row_field_manifest", {
            let c_path = CasacoreOracleRuntime::c_path("MeasurementSet path", path)?;
            let c_label =
                CasacoreOracleRuntime::c_string("MeasurementSet table label", table_label)?;
            let mut manifest: *mut std::ffi::c_char = std::ptr::null_mut();
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe {
                ffi_cpp_ms_table_row_field_manifest(
                    c_path.as_ptr(),
                    c_label.as_ptr(),
                    row,
                    &mut manifest,
                    &mut error,
                )
            };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "measurement_set.table_row_field_manifest",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            unsafe {
                CasacoreOracleRuntime::owned_string(
                    "measurement_set.table_row_field_manifest",
                    manifest,
                    cpp_table_free_error,
                )
            }
        })
    }

    /// Benchmark reading all rows of the MAIN table from an external MeasurementSet.
    ///
    /// The C++ side opens the MeasurementSet, streams the full MAIN-table row digest,
    /// and returns both the elapsed time and the resulting digest.
    pub fn bench_main_rows(path: &Path) -> Result<MsMainRowsBenchResult, OracleError> {
        oracle_operation!("measurement_set.bench_main_rows", {
            let c_path = CasacoreOracleRuntime::c_path("MeasurementSet path", path)?;
            let mut read_ns = 0_u64;
            let mut digest: *mut std::ffi::c_char = std::ptr::null_mut();
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe {
                ffi_cpp_ms_bench_main_rows(c_path.as_ptr(), &mut read_ns, &mut digest, &mut error)
            };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "measurement_set.bench_main_rows",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            let rows_digest = unsafe {
                CasacoreOracleRuntime::owned_string(
                    "measurement_set.bench_main_rows",
                    digest,
                    cpp_table_free_error,
                )?
            };
            Ok(MsMainRowsBenchResult {
                read_ns,
                rows_digest,
            })
        })
    }

    /// Benchmark opening an external MeasurementSet and then reading all rows of the
    /// MAIN table on the C++ side.
    pub fn bench_open_main_rows(path: &Path) -> Result<MsMainOpenScanBenchResult, OracleError> {
        oracle_operation!("measurement_set.bench_open_main_rows", {
            let c_path = CasacoreOracleRuntime::c_path("MeasurementSet path", path)?;
            let mut open_and_read_ns = 0_u64;
            let mut digest: *mut std::ffi::c_char = std::ptr::null_mut();
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe {
                ffi_cpp_ms_bench_open_main_rows(
                    c_path.as_ptr(),
                    &mut open_and_read_ns,
                    &mut digest,
                    &mut error,
                )
            };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "measurement_set.bench_open_main_rows",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            let rows_digest = unsafe {
                CasacoreOracleRuntime::owned_string(
                    "measurement_set.bench_open_main_rows",
                    digest,
                    cpp_table_free_error,
                )?
            };
            Ok(MsMainOpenScanBenchResult {
                open_and_read_ns,
                rows_digest,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn no_cpp_backend_reports_unavailable_for_all_helpers() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        if cfg!(has_casacore_cpp) {
            MeasurementSetOracle::write_basic_fixture(path).unwrap();
            assert!(MeasurementSetOracle::verify_basic_fixture(path).is_ok());
            assert!(MeasurementSetOracle::bench_create_open(path, 4).is_ok());
            assert!(MeasurementSetOracle::digest_manifest(path).is_ok());
            assert!(MeasurementSetOracle::table_row_digest(path, "MAIN", 0).is_ok());
            assert!(MeasurementSetOracle::table_row_field_manifest(path, "MAIN", 0).is_ok());
            assert!(MeasurementSetOracle::bench_main_rows(path).is_ok());
            assert!(MeasurementSetOracle::bench_open_main_rows(path).is_ok());
        } else {
            assert!(matches!(
                MeasurementSetOracle::write_basic_fixture(path),
                Err(OracleError::Unavailable { .. })
            ));
            assert!(matches!(
                MeasurementSetOracle::verify_basic_fixture(path),
                Err(OracleError::Unavailable { .. })
            ));
            assert!(matches!(
                MeasurementSetOracle::bench_create_open(path, 4),
                Err(OracleError::Unavailable { .. })
            ));
            assert!(matches!(
                MeasurementSetOracle::digest_manifest(path),
                Err(OracleError::Unavailable { .. })
            ));
            assert!(matches!(
                MeasurementSetOracle::table_row_digest(path, "MAIN", 0),
                Err(OracleError::Unavailable { .. })
            ));
            assert!(matches!(
                MeasurementSetOracle::table_row_field_manifest(path, "MAIN", 0),
                Err(OracleError::Unavailable { .. })
            ));
            assert!(matches!(
                MeasurementSetOracle::bench_main_rows(path),
                Err(OracleError::Unavailable { .. })
            ));
            assert!(matches!(
                MeasurementSetOracle::bench_open_main_rows(path),
                Err(OracleError::Unavailable { .. })
            ));
        }
    }
}
