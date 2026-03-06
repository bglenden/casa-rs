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
