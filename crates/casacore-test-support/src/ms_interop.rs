// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ casacore MeasurementSet interop shims.
//!
//! These helpers exercise `casacore::MeasurementSet` directly rather than the
//! lower-level generic `Table` fixture layer.

use std::path::Path;

#[cfg(has_casacore_cpp)]
use std::ffi::CStr;

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

/// Write the standard MeasurementSet interop fixture using C++ casacore.
pub fn cpp_ms_write_basic_fixture(path: &Path) -> Result<(), String> {
    #[cfg(has_casacore_cpp)]
    {
        let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
            .map_err(|err| format!("CString: {err}"))?;
        let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
        let rc = unsafe { ffi_cpp_ms_write_basic_fixture(c_path.as_ptr(), &mut error) };
        if rc != 0 {
            return Err(take_cpp_error(error));
        }
        Ok(())
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = path;
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Verify the standard MeasurementSet interop fixture using C++ casacore.
pub fn cpp_ms_verify_basic_fixture(path: &Path) -> Result<(), String> {
    #[cfg(has_casacore_cpp)]
    {
        let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
            .map_err(|err| format!("CString: {err}"))?;
        let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
        let rc = unsafe { ffi_cpp_ms_verify_basic_fixture(c_path.as_ptr(), &mut error) };
        if rc != 0 {
            return Err(take_cpp_error(error));
        }
        Ok(())
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = path;
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Benchmark C++ MeasurementSet create/open/read for the standard fixture.
pub fn cpp_ms_bench_create_open(path: &Path, nrows: u64) -> Result<MsBenchResult, String> {
    #[cfg(has_casacore_cpp)]
    {
        let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
            .map_err(|err| format!("CString: {err}"))?;
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
        if rc != 0 {
            return Err(take_cpp_error(error));
        }
        Ok(MsBenchResult {
            create_ns,
            open_ns,
            read_ns,
        })
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = (path, nrows);
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Return a stable digest manifest for a MeasurementSet opened by C++ casacore.
///
/// The manifest is intended for Rust↔C++ parity tests on real-world
/// MeasurementSets. It traverses the main table, standard MS subtables, and
/// any additional tables reachable through `TpTable` keyword references,
/// returning compact per-table digests rather than a huge textual dump.
pub fn cpp_ms_digest_manifest(path: &Path) -> Result<String, String> {
    #[cfg(has_casacore_cpp)]
    {
        let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
            .map_err(|err| format!("CString: {err}"))?;
        let mut manifest: *mut std::ffi::c_char = std::ptr::null_mut();
        let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
        let rc = unsafe { ffi_cpp_ms_digest_manifest(c_path.as_ptr(), &mut manifest, &mut error) };
        if rc != 0 {
            return Err(take_cpp_error(error));
        }
        if manifest.is_null() {
            return Err("C++ digest manifest missing".to_string());
        }
        let result = unsafe { CStr::from_ptr(manifest) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(manifest) };
        Ok(result)
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = path;
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Return the stable digest for a single row of a named MeasurementSet table.
///
/// `table_label` uses the same naming as the MS verifier, for example
/// `"MAIN"`, `"SOURCE"`, or `"SYSCAL"`.
pub fn cpp_ms_table_row_digest(path: &Path, table_label: &str, row: u64) -> Result<String, String> {
    #[cfg(has_casacore_cpp)]
    {
        let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
            .map_err(|err| format!("CString: {err}"))?;
        let c_label =
            std::ffi::CString::new(table_label).map_err(|err| format!("CString: {err}"))?;
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
        if rc != 0 {
            return Err(take_cpp_error(error));
        }
        if digest.is_null() {
            return Err("C++ row digest missing".to_string());
        }
        let result = unsafe { CStr::from_ptr(digest) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(digest) };
        Ok(result)
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = (path, table_label, row);
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Return a per-field digest manifest for a single row of a named MS table.
pub fn cpp_ms_table_row_field_manifest(
    path: &Path,
    table_label: &str,
    row: u64,
) -> Result<String, String> {
    #[cfg(has_casacore_cpp)]
    {
        let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
            .map_err(|err| format!("CString: {err}"))?;
        let c_label =
            std::ffi::CString::new(table_label).map_err(|err| format!("CString: {err}"))?;
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
        if rc != 0 {
            return Err(take_cpp_error(error));
        }
        if manifest.is_null() {
            return Err("C++ row field manifest missing".to_string());
        }
        let result = unsafe { CStr::from_ptr(manifest) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(manifest) };
        Ok(result)
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = (path, table_label, row);
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Benchmark reading all rows of the MAIN table from an external MeasurementSet.
///
/// The C++ side opens the MeasurementSet, streams the full MAIN-table row digest,
/// and returns both the elapsed time and the resulting digest.
pub fn cpp_ms_bench_main_rows(path: &Path) -> Result<MsMainRowsBenchResult, String> {
    #[cfg(has_casacore_cpp)]
    {
        let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
            .map_err(|err| format!("CString: {err}"))?;
        let mut read_ns = 0_u64;
        let mut digest: *mut std::ffi::c_char = std::ptr::null_mut();
        let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
        let rc = unsafe {
            ffi_cpp_ms_bench_main_rows(c_path.as_ptr(), &mut read_ns, &mut digest, &mut error)
        };
        if rc != 0 {
            return Err(take_cpp_error(error));
        }
        if digest.is_null() {
            return Err("C++ MAIN rows digest missing".to_string());
        }
        let rows_digest = unsafe { CStr::from_ptr(digest) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(digest) };
        Ok(MsMainRowsBenchResult {
            read_ns,
            rows_digest,
        })
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = path;
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Benchmark opening an external MeasurementSet and then reading all rows of the
/// MAIN table on the C++ side.
pub fn cpp_ms_bench_open_main_rows(path: &Path) -> Result<MsMainOpenScanBenchResult, String> {
    #[cfg(has_casacore_cpp)]
    {
        let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
            .map_err(|err| format!("CString: {err}"))?;
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
        if rc != 0 {
            return Err(take_cpp_error(error));
        }
        if digest.is_null() {
            return Err("C++ MAIN open+scan digest missing".to_string());
        }
        let rows_digest = unsafe { CStr::from_ptr(digest) }
            .to_string_lossy()
            .to_string();
        unsafe { cpp_table_free_error(digest) };
        Ok(MsMainOpenScanBenchResult {
            open_and_read_ns,
            rows_digest,
        })
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = path;
        Err("casacore C++ backend unavailable".to_string())
    }
}

#[cfg(has_casacore_cpp)]
fn take_cpp_error(error: *mut std::ffi::c_char) -> String {
    if error.is_null() {
        return "unknown C++ error".to_string();
    }
    let message = unsafe { CStr::from_ptr(error) }
        .to_string_lossy()
        .to_string();
    unsafe { cpp_table_free_error(error) };
    message
}
