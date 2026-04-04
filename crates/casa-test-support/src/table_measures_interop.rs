// SPDX-License-Identifier: LGPL-3.0-or-later
//! FFI wrappers for the C++ table measures shim.

#[cfg(has_casacore_cpp)]
use std::ffi::CString;

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn table_meas_create_epoch_fixed(path: *const std::ffi::c_char) -> i32;
    fn table_meas_create_epoch_var_int(path: *const std::ffi::c_char) -> i32;
    fn table_meas_create_epoch_var_str(path: *const std::ffi::c_char) -> i32;
    fn table_meas_create_direction_fixed(path: *const std::ffi::c_char) -> i32;

    fn table_meas_read_epochs(
        path: *const std::ffi::c_char,
        col_name: *const std::ffi::c_char,
        nrow: i32,
        values_out: *mut f64,
        refs_out: *mut std::ffi::c_char,
        ref_buf_len: i32,
    ) -> i32;

    fn table_meas_read_directions(
        path: *const std::ffi::c_char,
        col_name: *const std::ffi::c_char,
        nrow: i32,
        values_out: *mut f64,
        refs_out: *mut std::ffi::c_char,
        ref_buf_len: i32,
    ) -> i32;

    fn table_meas_verify_epochs(
        path: *const std::ffi::c_char,
        col_name: *const std::ffi::c_char,
        nrow: i32,
        expected_mjds: *const f64,
        expected_refs: *const std::ffi::c_char,
        ref_buf_len: i32,
    ) -> i32;

    fn table_meas_verify_directions(
        path: *const std::ffi::c_char,
        col_name: *const std::ffi::c_char,
        nrow: i32,
        expected_vals: *const f64,
        expected_refs: *const std::ffi::c_char,
        ref_buf_len: i32,
    ) -> i32;

    fn table_meas_bench_epoch_read(
        path: *const std::ffi::c_char,
        col_name: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns_out: *mut u64,
    ) -> i32;

    fn table_meas_bench_direction_read(
        path: *const std::ffi::c_char,
        col_name: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns_out: *mut u64,
    ) -> i32;
}

/// Create a C++ table with a fixed-ref MEpoch column (3 rows, UTC).
#[cfg(has_casacore_cpp)]
pub fn cpp_create_epoch_fixed(path: &str) -> Result<(), String> {
    let c = CString::new(path).unwrap();
    let rc = unsafe { table_meas_create_epoch_fixed(c.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("table_meas_create_epoch_fixed: rc={rc}"))
    }
}

/// Create a C++ table with variable Int ref MEpoch column (3 rows).
#[cfg(has_casacore_cpp)]
pub fn cpp_create_epoch_var_int(path: &str) -> Result<(), String> {
    let c = CString::new(path).unwrap();
    let rc = unsafe { table_meas_create_epoch_var_int(c.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("table_meas_create_epoch_var_int: rc={rc}"))
    }
}

/// Create a C++ table with variable String ref MEpoch column (3 rows).
#[cfg(has_casacore_cpp)]
pub fn cpp_create_epoch_var_str(path: &str) -> Result<(), String> {
    let c = CString::new(path).unwrap();
    let rc = unsafe { table_meas_create_epoch_var_str(c.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("table_meas_create_epoch_var_str: rc={rc}"))
    }
}

/// Create a C++ table with a fixed-ref MDirection column (3 rows, J2000).
#[cfg(has_casacore_cpp)]
pub fn cpp_create_direction_fixed(path: &str) -> Result<(), String> {
    let c = CString::new(path).unwrap();
    let rc = unsafe { table_meas_create_direction_fixed(c.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(format!("table_meas_create_direction_fixed: rc={rc}"))
    }
}

/// Read epochs from a table using C++ ScalarMeasColumn.
///
/// Returns (mjds, refs) for `nrow` rows.
#[cfg(has_casacore_cpp)]
pub fn cpp_read_epochs(
    path: &str,
    col: &str,
    nrow: usize,
) -> Result<(Vec<f64>, Vec<String>), String> {
    let c_path = CString::new(path).unwrap();
    let c_col = CString::new(col).unwrap();
    let mut values = vec![0.0f64; nrow];
    const REF_BUF: i32 = 32;
    let mut refs_buf = vec![0u8; nrow * REF_BUF as usize];

    let rc = unsafe {
        table_meas_read_epochs(
            c_path.as_ptr(),
            c_col.as_ptr(),
            nrow as i32,
            values.as_mut_ptr(),
            refs_buf.as_mut_ptr() as *mut std::ffi::c_char,
            REF_BUF,
        )
    };

    if rc != 0 {
        return Err(format!("table_meas_read_epochs: rc={rc}"));
    }

    let refs: Vec<String> = (0..nrow)
        .map(|i| {
            let start = i * REF_BUF as usize;
            let slice = &refs_buf[start..start + REF_BUF as usize];
            let end = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
            String::from_utf8_lossy(&slice[..end]).into_owned()
        })
        .collect();

    Ok((values, refs))
}

/// Read directions from a table using C++ ScalarMeasColumn.
///
/// Returns `(values[nrow*2], refs)` for `nrow` rows.
#[cfg(has_casacore_cpp)]
pub fn cpp_read_directions(
    path: &str,
    col: &str,
    nrow: usize,
) -> Result<(Vec<f64>, Vec<String>), String> {
    let c_path = CString::new(path).unwrap();
    let c_col = CString::new(col).unwrap();
    let mut values = vec![0.0f64; nrow * 2];
    const REF_BUF: i32 = 32;
    let mut refs_buf = vec![0u8; nrow * REF_BUF as usize];

    let rc = unsafe {
        table_meas_read_directions(
            c_path.as_ptr(),
            c_col.as_ptr(),
            nrow as i32,
            values.as_mut_ptr(),
            refs_buf.as_mut_ptr() as *mut std::ffi::c_char,
            REF_BUF,
        )
    };

    if rc != 0 {
        return Err(format!("table_meas_read_directions: rc={rc}"));
    }

    let refs: Vec<String> = (0..nrow)
        .map(|i| {
            let start = i * REF_BUF as usize;
            let slice = &refs_buf[start..start + REF_BUF as usize];
            let end = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
            String::from_utf8_lossy(&slice[..end]).into_owned()
        })
        .collect();

    Ok((values, refs))
}

/// Verify a Rust-written epoch table using C++ ScalarMeasColumn.
#[cfg(has_casacore_cpp)]
pub fn cpp_verify_epochs(
    path: &str,
    col: &str,
    expected_mjds: &[f64],
    expected_refs: &[&str],
) -> Result<(), String> {
    let c_path = CString::new(path).unwrap();
    let c_col = CString::new(col).unwrap();
    let nrow = expected_mjds.len();
    const REF_BUF: i32 = 32;

    // Pack refs into fixed-size buffer
    let mut refs_buf = vec![0u8; nrow * REF_BUF as usize];
    for (i, r) in expected_refs.iter().enumerate() {
        let start = i * REF_BUF as usize;
        let bytes = r.as_bytes();
        let copy_len = bytes.len().min(REF_BUF as usize - 1);
        refs_buf[start..start + copy_len].copy_from_slice(&bytes[..copy_len]);
    }

    let rc = unsafe {
        table_meas_verify_epochs(
            c_path.as_ptr(),
            c_col.as_ptr(),
            nrow as i32,
            expected_mjds.as_ptr(),
            refs_buf.as_ptr() as *const std::ffi::c_char,
            REF_BUF,
        )
    };

    if rc == 0 {
        Ok(())
    } else {
        Err(format!("table_meas_verify_epochs: rc={rc}"))
    }
}

/// Verify a Rust-written direction table using C++ ScalarMeasColumn.
#[cfg(has_casacore_cpp)]
pub fn cpp_verify_directions(
    path: &str,
    col: &str,
    expected_vals: &[f64],
    expected_refs: &[&str],
) -> Result<(), String> {
    let c_path = CString::new(path).unwrap();
    let c_col = CString::new(col).unwrap();
    let nrow = expected_refs.len();
    const REF_BUF: i32 = 32;

    let mut refs_buf = vec![0u8; nrow * REF_BUF as usize];
    for (i, r) in expected_refs.iter().enumerate() {
        let start = i * REF_BUF as usize;
        let bytes = r.as_bytes();
        let copy_len = bytes.len().min(REF_BUF as usize - 1);
        refs_buf[start..start + copy_len].copy_from_slice(&bytes[..copy_len]);
    }

    let rc = unsafe {
        table_meas_verify_directions(
            c_path.as_ptr(),
            c_col.as_ptr(),
            nrow as i32,
            expected_vals.as_ptr(),
            refs_buf.as_ptr() as *const std::ffi::c_char,
            REF_BUF,
        )
    };

    if rc == 0 {
        Ok(())
    } else {
        Err(format!("table_meas_verify_directions: rc={rc}"))
    }
}

/// Benchmark C++ epoch measure column read throughput.
///
/// Returns elapsed nanoseconds for `iterations` full-table scans.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_epoch_read(path: &str, column: &str, iterations: i32) -> Result<u64, String> {
    let c_path = CString::new(path).unwrap();
    let c_col = CString::new(column).unwrap();
    let mut ns: u64 = 0;
    let rc = unsafe {
        table_meas_bench_epoch_read(c_path.as_ptr(), c_col.as_ptr(), iterations, &mut ns)
    };
    if rc != 0 {
        return Err(format!("table_meas_bench_epoch_read: rc={rc}"));
    }
    Ok(ns)
}

/// Benchmark C++ direction measure column read throughput.
///
/// Returns elapsed nanoseconds for `iterations` full-table scans.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_direction_read(path: &str, column: &str, iterations: i32) -> Result<u64, String> {
    let c_path = CString::new(path).unwrap();
    let c_col = CString::new(column).unwrap();
    let mut ns: u64 = 0;
    let rc = unsafe {
        table_meas_bench_direction_read(c_path.as_ptr(), c_col.as_ptr(), iterations, &mut ns)
    };
    if rc != 0 {
        return Err(format!("table_meas_bench_direction_read: rc={rc}"));
    }
    Ok(ns)
}
