// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed C++ oracle facade for the quanta module.

use crate::oracle_runtime::{CasacoreOracleRuntime, OracleError, oracle_operation};

#[cfg(has_casacore_cpp)]
use std::ffi::CString;
#[cfg(has_casacore_cpp)]
use std::sync::Once;

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
    fn quanta_shim_mvangle_format_angle(
        radians: f64,
        second_decimals: i32,
        out_buf: *mut u8,
        out_buf_len: i32,
    ) -> i32;
    fn quanta_shim_mvangle_format_angle_dig2(
        radians: f64,
        second_decimals: i32,
        out_buf: *mut u8,
        out_buf_len: i32,
    ) -> i32;
    fn quanta_shim_mvangle_format_time(
        radians: f64,
        lower_turns: f64,
        second_decimals: i32,
        out_buf: *mut u8,
        out_buf_len: i32,
    ) -> i32;
    fn quanta_shim_mvtime_format_dmy(
        mjd_days: f64,
        second_decimals: i32,
        out_buf: *mut u8,
        out_buf_len: i32,
    ) -> i32;
    fn quanta_shim_mvtime_format_time(
        mjd_days: f64,
        second_decimals: i32,
        out_buf: *mut u8,
        out_buf_len: i32,
    ) -> i32;
    fn quanta_shim_mvtime_format_dmy_date(mjd_days: f64, out_buf: *mut u8, out_buf_len: i32)
    -> i32;
}

/// Rust-facing access to casacore's quanta implementation.
///
/// The type is always present. Builds without the C++ oracle return
/// [`OracleError::Unavailable`] from every operation.
pub struct QuantaOracle;

macro_rules! cpp_operation {
    ($capability:expr, $body:block) => {{
        oracle_operation!($capability, {
            initialize();
            $body
        })
    }};
}

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl QuantaOracle {
    pub fn available() -> bool {
        CasacoreOracleRuntime::available()
    }

    pub fn parse_factor(unit: &str) -> Result<f64, OracleError> {
        cpp_operation!("quanta.parse_factor", {
            let unit = CasacoreOracleRuntime::c_string("unit", unit)?;
            let mut factor = 0.0;
            let status = unsafe { quanta_shim_parse(unit.as_ptr(), &mut factor) };
            CasacoreOracleRuntime::status("quanta.parse_factor", status)?;
            Ok(factor)
        })
    }

    pub fn conformant(a: &str, b: &str) -> Result<bool, OracleError> {
        cpp_operation!("quanta.conformant", {
            let a = CasacoreOracleRuntime::c_string("left unit", a)?;
            let b = CasacoreOracleRuntime::c_string("right unit", b)?;
            match unsafe { quanta_shim_conformant(a.as_ptr(), b.as_ptr()) } {
                1 => Ok(true),
                0 => Ok(false),
                status => Err(OracleError::CppFailure {
                    operation: "quanta.conformant",
                    message: format!("status {status}"),
                }),
            }
        })
    }

    pub fn speed_of_light() -> Result<f64, OracleError> {
        cpp_operation!("quanta.qc.c", {
            constant_value("quanta.qc.c", quanta_shim_qc_c)
        })
    }

    pub fn planck_constant() -> Result<f64, OracleError> {
        cpp_operation!("quanta.qc.h", {
            constant_value("quanta.qc.h", quanta_shim_qc_h)
        })
    }

    pub fn parse_full(unit: &str) -> Result<(f64, [i32; 10]), OracleError> {
        cpp_operation!("quanta.parse_full", {
            let unit = CasacoreOracleRuntime::c_string("unit", unit)?;
            let mut factor = 0.0;
            let mut dimensions = [0; 10];
            let status = unsafe {
                quanta_shim_parse_full(unit.as_ptr(), &mut factor, dimensions.as_mut_ptr())
            };
            CasacoreOracleRuntime::status("quanta.parse_full", status)?;
            Ok((factor, dimensions))
        })
    }

    pub fn constant(name: &str) -> Result<(f64, String, [i32; 10]), OracleError> {
        cpp_operation!("quanta.qc.constant", {
            let name = CasacoreOracleRuntime::c_string("constant name", name)?;
            let mut value = 0.0;
            let mut unit = [0; 128];
            let mut dimensions = [0; 10];
            let status = unsafe {
                quanta_shim_qc_constant(
                    name.as_ptr(),
                    &mut value,
                    unit.as_mut_ptr(),
                    unit.len() as i32,
                    dimensions.as_mut_ptr(),
                )
            };
            CasacoreOracleRuntime::status("quanta.qc.constant", status)?;
            Ok((
                value,
                CasacoreOracleRuntime::output_string("quanta.qc.constant", &unit)?,
                dimensions,
            ))
        })
    }

    pub fn bench_parse(units: &[&str], iterations: i32) -> Result<u64, OracleError> {
        cpp_operation!("quanta.bench_parse", {
            let units: Result<Vec<CString>, _> = units
                .iter()
                .map(|unit| CasacoreOracleRuntime::c_string("unit", unit))
                .collect();
            let units = units?;
            let pointers: Vec<_> = units.iter().map(|unit| unit.as_ptr()).collect();
            let mut elapsed_ns = 0;
            let status = unsafe {
                quanta_shim_bench_parse(
                    pointers.as_ptr(),
                    pointers.len() as i32,
                    iterations,
                    &mut elapsed_ns,
                )
            };
            CasacoreOracleRuntime::status("quanta.bench_parse", status)?;
            Ok(elapsed_ns)
        })
    }

    pub fn bench_convert(
        value: f64,
        from: &str,
        to: &str,
        iterations: i32,
    ) -> Result<u64, OracleError> {
        cpp_operation!("quanta.bench_convert", {
            let from = CasacoreOracleRuntime::c_string("source unit", from)?;
            let to = CasacoreOracleRuntime::c_string("destination unit", to)?;
            let mut elapsed_ns = 0;
            let status = unsafe {
                quanta_shim_bench_convert(
                    value,
                    from.as_ptr(),
                    to.as_ptr(),
                    iterations,
                    &mut elapsed_ns,
                )
            };
            CasacoreOracleRuntime::status("quanta.bench_convert", status)?;
            Ok(elapsed_ns)
        })
    }

    pub fn format_angle(radians: f64, second_decimals: usize) -> Result<String, OracleError> {
        cpp_operation!("quanta.mvangle.angle", {
            format_string("quanta.mvangle.angle", |buffer, length| unsafe {
                quanta_shim_mvangle_format_angle(radians, second_decimals as i32, buffer, length)
            })
        })
    }

    pub fn format_angle_dig2(radians: f64, second_decimals: usize) -> Result<String, OracleError> {
        cpp_operation!("quanta.mvangle.dig2", {
            format_string("quanta.mvangle.dig2", |buffer, length| unsafe {
                quanta_shim_mvangle_format_angle_dig2(
                    radians,
                    second_decimals as i32,
                    buffer,
                    length,
                )
            })
        })
    }

    pub fn format_angle_time(
        radians: f64,
        lower_turns: f64,
        second_decimals: usize,
    ) -> Result<String, OracleError> {
        cpp_operation!("quanta.mvangle.time", {
            format_string("quanta.mvangle.time", |buffer, length| unsafe {
                quanta_shim_mvangle_format_time(
                    radians,
                    lower_turns,
                    second_decimals as i32,
                    buffer,
                    length,
                )
            })
        })
    }

    pub fn format_mjd_dmy(mjd_days: f64, second_decimals: usize) -> Result<String, OracleError> {
        cpp_operation!("quanta.mvtime.dmy", {
            format_string("quanta.mvtime.dmy", |buffer, length| unsafe {
                quanta_shim_mvtime_format_dmy(mjd_days, second_decimals as i32, buffer, length)
            })
        })
    }

    pub fn format_mjd_time(mjd_days: f64, second_decimals: usize) -> Result<String, OracleError> {
        cpp_operation!("quanta.mvtime.time", {
            format_string("quanta.mvtime.time", |buffer, length| unsafe {
                quanta_shim_mvtime_format_time(mjd_days, second_decimals as i32, buffer, length)
            })
        })
    }

    pub fn format_mjd_date(mjd_days: f64) -> Result<String, OracleError> {
        cpp_operation!("quanta.mvtime.date", {
            format_string("quanta.mvtime.date", |buffer, length| unsafe {
                quanta_shim_mvtime_format_dmy_date(mjd_days, buffer, length)
            })
        })
    }
}

#[cfg(has_casacore_cpp)]
fn initialize() {
    static INITIALIZE: Once = Once::new();
    INITIALIZE.call_once(|| unsafe { quanta_shim_init() });
}

#[cfg(has_casacore_cpp)]
fn constant_value(
    operation: &'static str,
    call: unsafe extern "C" fn(*mut f64, *mut u8, i32) -> i32,
) -> Result<f64, OracleError> {
    let mut value = 0.0;
    let mut unit = [0; 64];
    let status = unsafe { call(&mut value, unit.as_mut_ptr(), unit.len() as i32) };
    CasacoreOracleRuntime::status(operation, status)?;
    Ok(value)
}

#[cfg(has_casacore_cpp)]
fn format_string(
    operation: &'static str,
    call: impl FnOnce(*mut u8, i32) -> i32,
) -> Result<String, OracleError> {
    let mut buffer = [0; 256];
    let status = call(buffer.as_mut_ptr(), buffer.len() as i32);
    CasacoreOracleRuntime::status(operation, status)?;
    CasacoreOracleRuntime::output_string(operation, &buffer)
}

#[cfg(test)]
mod tests {
    use super::QuantaOracle;

    #[cfg(has_casacore_cpp)]
    #[test]
    fn runtime_serializes_parallel_callers() {
        let workers: Vec<_> = ["m", "km/s", "Jy"]
            .into_iter()
            .map(|unit| {
                std::thread::spawn(move || {
                    for _ in 0..8 {
                        QuantaOracle::parse_factor(unit).unwrap();
                        QuantaOracle::constant("c").unwrap();
                    }
                })
            })
            .collect();

        for worker in workers {
            worker.join().expect("parallel quanta worker should finish");
        }
    }

    #[cfg(not(has_casacore_cpp))]
    #[test]
    fn facade_remains_present_without_cpp() {
        assert!(matches!(
            QuantaOracle::parse_factor("m"),
            Err(crate::OracleError::Unavailable { .. })
        ));
    }
}
