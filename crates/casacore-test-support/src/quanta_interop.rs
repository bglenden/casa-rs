// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ cross-validation helpers for the quanta module.

use std::ffi::CString;

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn quanta_shim_init();
    fn quanta_shim_parse(unit_str: *const std::ffi::c_char, factor_out: *mut f64) -> i32;
    fn quanta_shim_conformant(
        unit_a: *const std::ffi::c_char,
        unit_b: *const std::ffi::c_char,
    ) -> i32;
    fn quanta_shim_qc_c(value_out: *mut f64, unit_buf: *mut u8, buf_len: i32) -> i32;
    fn quanta_shim_qc_h(value_out: *mut f64, unit_buf: *mut u8, buf_len: i32) -> i32;
    fn quanta_shim_parse_full(
        unit_str: *const std::ffi::c_char,
        factor_out: *mut f64,
        dims_out: *mut i32,
    ) -> i32;
    fn quanta_shim_qc_constant(
        name: *const std::ffi::c_char,
        value_out: *mut f64,
        unit_buf: *mut u8,
        unit_buf_len: i32,
        dims_out: *mut i32,
    ) -> i32;
    fn quanta_shim_bench_parse(
        unit_strs: *const *const std::ffi::c_char,
        count: i32,
        iterations: i32,
        elapsed_ns_out: *mut u64,
    ) -> i32;
    fn quanta_shim_bench_convert(
        value: f64,
        from_unit: *const std::ffi::c_char,
        to_unit: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns_out: *mut u64,
    ) -> i32;
}

/// Initialise the C++ unit system (idempotent).
#[cfg(has_casacore_cpp)]
pub fn init_cpp() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| unsafe { quanta_shim_init() });
}

/// Parse a unit string with C++ casacore and return the factor.
#[cfg(has_casacore_cpp)]
pub fn cpp_parse_factor(unit: &str) -> Option<f64> {
    init_cpp();
    let cstr = CString::new(unit).ok()?;
    let mut factor = 0.0f64;
    let rc = unsafe { quanta_shim_parse(cstr.as_ptr(), &mut factor) };
    if rc == 0 { Some(factor) } else { None }
}

/// Check conformance of two unit strings via C++.
#[cfg(has_casacore_cpp)]
pub fn cpp_conformant(a: &str, b: &str) -> Option<bool> {
    init_cpp();
    let ca = CString::new(a).ok()?;
    let cb = CString::new(b).ok()?;
    let rc = unsafe { quanta_shim_conformant(ca.as_ptr(), cb.as_ptr()) };
    match rc {
        1 => Some(true),
        0 => Some(false),
        _ => None,
    }
}

/// Get QC::c() from C++.
#[cfg(has_casacore_cpp)]
pub fn cpp_qc_c() -> f64 {
    init_cpp();
    let mut val = 0.0f64;
    let mut buf = [0u8; 64];
    unsafe { quanta_shim_qc_c(&mut val, buf.as_mut_ptr(), 64) };
    val
}

/// Get QC::h() from C++.
#[cfg(has_casacore_cpp)]
pub fn cpp_qc_h() -> f64 {
    init_cpp();
    let mut val = 0.0f64;
    let mut buf = [0u8; 64];
    unsafe { quanta_shim_qc_h(&mut val, buf.as_mut_ptr(), 64) };
    val
}

/// Parse a unit string with C++ casacore, returning factor and 10 dimension exponents.
#[cfg(has_casacore_cpp)]
pub fn cpp_parse_full(unit: &str) -> Option<(f64, [i32; 10])> {
    init_cpp();
    let cstr = CString::new(unit).ok()?;
    let mut factor = 0.0f64;
    let mut dims = [0i32; 10];
    let rc = unsafe { quanta_shim_parse_full(cstr.as_ptr(), &mut factor, dims.as_mut_ptr()) };
    if rc == 0 { Some((factor, dims)) } else { None }
}

/// Look up a QC constant by name from C++.
/// Returns (value, unit_string, dims).
#[cfg(has_casacore_cpp)]
pub fn cpp_qc_constant(name: &str) -> Option<(f64, String, [i32; 10])> {
    init_cpp();
    let cname = CString::new(name).ok()?;
    let mut value = 0.0f64;
    let mut unit_buf = [0u8; 128];
    let mut dims = [0i32; 10];
    let rc = unsafe {
        quanta_shim_qc_constant(
            cname.as_ptr(),
            &mut value,
            unit_buf.as_mut_ptr(),
            128,
            dims.as_mut_ptr(),
        )
    };
    if rc == 0 {
        let unit_str = std::ffi::CStr::from_bytes_until_nul(&unit_buf)
            .ok()?
            .to_str()
            .ok()?
            .to_owned();
        Some((value, unit_str, dims))
    } else {
        None
    }
}

/// Benchmark C++ unit parsing: parse the given unit strings `iterations` times each.
/// Returns total elapsed nanoseconds.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_parse(units: &[&str], iterations: i32) -> Option<u64> {
    init_cpp();
    let cstrings: Vec<CString> = units.iter().filter_map(|u| CString::new(*u).ok()).collect();
    if cstrings.len() != units.len() {
        return None;
    }
    let ptrs: Vec<*const std::ffi::c_char> = cstrings.iter().map(|c| c.as_ptr()).collect();
    let mut elapsed_ns = 0u64;
    let rc = unsafe {
        quanta_shim_bench_parse(
            ptrs.as_ptr(),
            ptrs.len() as i32,
            iterations,
            &mut elapsed_ns,
        )
    };
    if rc == 0 { Some(elapsed_ns) } else { None }
}

/// Benchmark C++ unit conversion: convert `value` from `from` to `to`, `iterations` times.
/// Returns total elapsed nanoseconds.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_convert(value: f64, from: &str, to: &str, iterations: i32) -> Option<u64> {
    init_cpp();
    let cfrom = CString::new(from).ok()?;
    let cto = CString::new(to).ok()?;
    let mut elapsed_ns = 0u64;
    let rc = unsafe {
        quanta_shim_bench_convert(
            value,
            cfrom.as_ptr(),
            cto.as_ptr(),
            iterations,
            &mut elapsed_ns,
        )
    };
    if rc == 0 { Some(elapsed_ns) } else { None }
}
