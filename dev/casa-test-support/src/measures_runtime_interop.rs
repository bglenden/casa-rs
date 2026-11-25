// SPDX-License-Identifier: LGPL-3.0-or-later
//! FFI wrapper for the shim that reports which measures tables casacore reads.

#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn measures_shim_measures_table_identity(
        name: *const std::ffi::c_char,
        path_out: *mut std::ffi::c_char,
        path_capacity: usize,
        date_out: *mut std::ffi::c_char,
        date_capacity: usize,
    ) -> i32;
}

/// The path and `VS_DATE` of the geodetic measures table `name` that casacore
/// resolves.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_measures_table_identity(name: &str) -> Result<(String, String), String> {
    let c_name = CasacoreOracleRuntime::c_string("measures table name", name)
        .map_err(|error| error.to_string())?;
    let mut path = [0 as std::ffi::c_char; 4096];
    let mut date = [0 as std::ffi::c_char; 64];
    let rc = unsafe {
        measures_shim_measures_table_identity(
            c_name.as_ptr(),
            path.as_mut_ptr(),
            path.len(),
            date.as_mut_ptr(),
            date.len(),
        )
    };
    if rc != 0 {
        return Err(format!(
            "C++ measures_table_identity({name}) failed: rc={rc}"
        ));
    }
    let operation = "measures.measures_table_identity";
    let text = |bytes: &[std::ffi::c_char]| {
        CasacoreOracleRuntime::output_c_char_string(operation, bytes)
            .map_err(|error| error.to_string())
    };
    Ok((text(&path)?, text(&date)?))
}
