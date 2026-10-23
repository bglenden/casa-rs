// SPDX-License-Identifier: LGPL-3.0-or-later
//! FFI wrappers for the frame and sidereal-epoch law shims (#850).

#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn measures_shim_direction_convert_cosines(
        lon_in: f64,
        lat_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        epoch_mjd: f64,
        iau2000a: i32,
        xyz_out: *mut f64,
    ) -> i32;

    fn measures_shim_epoch_convert_model(
        mjd_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        iau2000a: i32,
        mjd_out: *mut f64,
    ) -> i32;
}

/// casacore's direction vector, unnormalized, for one conversion in a frame
/// with an optional UTC epoch (0 means none) and no position.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_direction_convert_cosines(
    lon_lat: (f64, f64),
    ref_in: &str,
    ref_out: &str,
    epoch_mjd: f64,
    iau2000a: bool,
) -> Result<[f64; 3], String> {
    let c_in = CasacoreOracleRuntime::c_string("direction reference", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("direction reference", ref_out)
        .map_err(|error| error.to_string())?;
    let mut xyz = [0.0; 3];
    let rc = unsafe {
        measures_shim_direction_convert_cosines(
            lon_lat.0,
            lon_lat.1,
            c_in.as_ptr(),
            c_out.as_ptr(),
            epoch_mjd,
            i32::from(iau2000a),
            xyz.as_mut_ptr(),
        )
    };
    if rc == 0 {
        Ok(xyz)
    } else {
        Err(format!(
            "C++ direction_convert_cosines({ref_in}->{ref_out}) failed: rc={rc}"
        ))
    }
}

/// casacore's full epoch value, day count included, for one conversion with
/// an optional WGS84 observatory (all zero means none).
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_epoch_convert_model(
    mjd_in: f64,
    ref_in: &str,
    ref_out: &str,
    observatory_wgs84: [f64; 3],
    iau2000a: bool,
) -> Result<f64, String> {
    let c_in = CasacoreOracleRuntime::c_string("epoch reference", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("epoch reference", ref_out)
        .map_err(|error| error.to_string())?;
    let [lon, lat, height] = observatory_wgs84;
    let mut mjd_out = 0.0;
    let rc = unsafe {
        measures_shim_epoch_convert_model(
            mjd_in,
            c_in.as_ptr(),
            c_out.as_ptr(),
            lon,
            lat,
            height,
            i32::from(iau2000a),
            &mut mjd_out,
        )
    };
    if rc == 0 {
        Ok(mjd_out)
    } else {
        Err(format!(
            "C++ epoch_convert_model({ref_in}->{ref_out}) failed: rc={rc}"
        ))
    }
}
