// SPDX-License-Identifier: LGPL-3.0-or-later
//! FFI wrappers for the shims over casacore's process-global measures state:
//! the measures tables it reads and its IAU model.

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

    fn measures_shim_epoch_convert_iau2000a(
        mjd_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        mjd_out: *mut f64,
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

/// Convert an epoch with casacore switched to IAU 2006/2000A; zero observatory
/// coordinates mean a frame without a position.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_epoch_convert_iau2000a(
    mjd_in: f64,
    ref_in: &str,
    ref_out: &str,
    observatory_wgs84: [f64; 3],
) -> Result<f64, String> {
    let c_in = CasacoreOracleRuntime::c_string("epoch reference", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("epoch reference", ref_out)
        .map_err(|error| error.to_string())?;
    let [lon, lat, height] = observatory_wgs84;
    let mut mjd_out = 0.0;
    let rc = unsafe {
        measures_shim_epoch_convert_iau2000a(
            mjd_in,
            c_in.as_ptr(),
            c_out.as_ptr(),
            lon,
            lat,
            height,
            &mut mjd_out,
        )
    };
    if rc == 0 {
        Ok(mjd_out)
    } else {
        Err(format!("C++ epoch_convert_iau2000a failed: rc={rc}"))
    }
}
