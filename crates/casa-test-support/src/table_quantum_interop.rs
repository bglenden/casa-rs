// SPDX-License-Identifier: LGPL-3.0-or-later
//! FFI wrappers for the C++ table quantum shim.

#[cfg(has_casacore_cpp)]
use std::ffi::CString;

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

/// Create a C++ quantum table at the given path.
#[cfg(has_casacore_cpp)]
pub fn cpp_create_quantum_table(path: &str) -> Result<(), String> {
    let c_path = CString::new(path).map_err(|e| e.to_string())?;
    let rc = unsafe { table_quantum_create_cpp(c_path.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("table_quantum_create_cpp returned {rc}"))
    }
}

/// Read scalar quantum values from a C++ or Rust quantum table.
///
/// Returns `(fixed_values, var_values, var_units)` for 3 rows.
#[cfg(has_casacore_cpp)]
#[allow(clippy::type_complexity)]
pub fn cpp_read_quantum_table(path: &str) -> Result<(Vec<f64>, Vec<f64>, Vec<String>), String> {
    let c_path = CString::new(path).map_err(|e| e.to_string())?;
    let mut sca_fixed = [0.0f64; 3];
    let mut sca_var = [0.0f64; 3];
    const UNIT_BUF: i32 = 64;
    let mut unit_buf = vec![0u8; (3 * UNIT_BUF) as usize];

    let rc = unsafe {
        table_quantum_read_cpp(
            c_path.as_ptr(),
            sca_fixed.as_mut_ptr(),
            sca_var.as_mut_ptr(),
            unit_buf.as_mut_ptr() as *mut std::ffi::c_char,
            UNIT_BUF,
        )
    };
    if rc != 0 {
        return Err(format!("table_quantum_read_cpp returned {rc}"));
    }

    let mut units = Vec::new();
    for i in 0..3 {
        let start = i * UNIT_BUF as usize;
        let slice = &unit_buf[start..start + UNIT_BUF as usize];
        let end = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
        units.push(String::from_utf8_lossy(&slice[..end]).to_string());
    }

    Ok((sca_fixed.to_vec(), sca_var.to_vec(), units))
}

/// Verify a Rust-written quantum table from C++.
#[cfg(has_casacore_cpp)]
pub fn cpp_verify_quantum_table(path: &str) -> Result<bool, String> {
    let c_path = CString::new(path).map_err(|e| e.to_string())?;
    let mut ok: i32 = 0;
    let rc = unsafe { table_quantum_verify_cpp(c_path.as_ptr(), &mut ok) };
    if rc != 0 {
        return Err(format!("table_quantum_verify_cpp returned {rc}"));
    }
    Ok(ok == 1)
}

/// Benchmark C++ scalar quantum column read.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_scalar_read(path: &str, column: &str, iterations: i32) -> Result<u64, String> {
    let c_path = CString::new(path).map_err(|e| e.to_string())?;
    let c_col = CString::new(column).map_err(|e| e.to_string())?;
    let mut ns: u64 = 0;
    let rc = unsafe {
        table_quantum_bench_scalar_read_cpp(c_path.as_ptr(), c_col.as_ptr(), iterations, &mut ns)
    };
    if rc != 0 {
        return Err(format!("bench_scalar_read returned {rc}"));
    }
    Ok(ns)
}

/// Benchmark C++ array quantum column read.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_array_read(path: &str, column: &str, iterations: i32) -> Result<u64, String> {
    let c_path = CString::new(path).map_err(|e| e.to_string())?;
    let c_col = CString::new(column).map_err(|e| e.to_string())?;
    let mut ns: u64 = 0;
    let rc = unsafe {
        table_quantum_bench_array_read_cpp(c_path.as_ptr(), c_col.as_ptr(), iterations, &mut ns)
    };
    if rc != 0 {
        return Err(format!("bench_array_read returned {rc}"));
    }
    Ok(ns)
}
