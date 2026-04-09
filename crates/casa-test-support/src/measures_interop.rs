// SPDX-License-Identifier: LGPL-3.0-or-later
//! FFI wrappers for the C++ measures shim.

#[cfg(has_casacore_cpp)]
use crate::{CasacoreGlobalStateDomain, lock_casacore_global_state};
#[cfg(has_casacore_cpp)]
use std::ffi::CString;

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn measures_shim_epoch_convert(
        mjd_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        mjd_out: *mut f64,
    ) -> i32;

    fn measures_shim_epoch_to_record(
        mjd_in: f64,
        ref_in: *const std::ffi::c_char,
        value_out: *mut f64,
        unit_out: *mut std::ffi::c_char,
        unit_buf: i32,
        refer_out: *mut std::ffi::c_char,
        refer_buf: i32,
    ) -> i32;

    fn measures_shim_position_convert(
        v0: f64,
        v1: f64,
        v2: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        out0: *mut f64,
        out1: *mut f64,
        out2: *mut f64,
    ) -> i32;

    fn measures_shim_bench_epoch_convert(
        mjd_start: f64,
        count: i32,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns: *mut u64,
    ) -> i32;

    fn measures_shim_position_to_record(
        x: f64,
        y: f64,
        z: f64,
        lon_out: *mut f64,
        lat_out: *mut f64,
        radius_out: *mut f64,
    ) -> i32;

    fn measures_shim_position_to_wgs_xyz(
        v0: f64,
        v1: f64,
        v2: f64,
        ref_in: *const std::ffi::c_char,
        out0: *mut f64,
        out1: *mut f64,
        out2: *mut f64,
    ) -> i32;

    fn measures_shim_bench_position_convert(
        x_start: f64,
        y: f64,
        z: f64,
        count: i32,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns: *mut u64,
    ) -> i32;

    fn measures_shim_doppler_convert(
        value_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        value_out: *mut f64,
    ) -> i32;

    fn measures_shim_bench_doppler_convert(
        value_start: f64,
        count: i32,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        iterations: i32,
        elapsed_ns: *mut u64,
    ) -> i32;

    fn measures_shim_direction_convert(
        lon_in: f64,
        lat_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        lon_out: *mut f64,
        lat_out: *mut f64,
    ) -> i32;

    fn measures_shim_named_direction_convert(
        source_name: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        lon_out: *mut f64,
        lat_out: *mut f64,
    ) -> i32;

    fn measures_shim_riseset(
        source_name: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        rise_out: *mut f64,
        set_out: *mut f64,
    ) -> i32;

    fn measures_shim_line_frequency(
        line_name: *const std::ffi::c_char,
        freq_out_hz: *mut f64,
    ) -> i32;

    fn measures_shim_earthmag_convert_xyz(
        x_in: f64,
        y_in: f64,
        z_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        x_out: *mut f64,
        y_out: *mut f64,
        z_out: *mut f64,
    ) -> i32;

    fn measures_shim_earthmag_convert_angles(
        lon_in: f64,
        lat_in: f64,
        length_nt: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        lon_out: *mut f64,
        lat_out: *mut f64,
    ) -> i32;

    fn measures_shim_igrf_xyz(
        ref_out: *const std::ffi::c_char,
        height_m: f64,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        x_out: *mut f64,
        y_out: *mut f64,
        z_out: *mut f64,
    ) -> i32;

    fn measures_shim_igrf_los(
        height_m: f64,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        value_out: *mut f64,
    ) -> i32;

    fn measures_shim_igrf_long(
        height_m: f64,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        value_out: *mut f64,
    ) -> i32;

    fn measures_shim_bench_direction_convert(
        lon_start: f64,
        lat: f64,
        count: i32,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        iterations: i32,
        elapsed_ns: *mut u64,
    ) -> i32;

    fn measures_shim_frequency_convert(
        freq_hz: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        freq_out: *mut f64,
    ) -> i32;

    fn measures_shim_bench_frequency_convert(
        freq_start: f64,
        count: i32,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        iterations: i32,
        elapsed_ns: *mut u64,
    ) -> i32;

    fn measures_shim_radvel_convert(
        ms_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        ms_out: *mut f64,
    ) -> i32;

    fn measures_shim_bench_radvel_convert(
        ms_start: f64,
        count: i32,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        iterations: i32,
        elapsed_ns: *mut u64,
    ) -> i32;

    fn measures_shim_frequency_convert_with_rv(
        freq_hz: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        dir_lon: f64,
        dir_lat: f64,
        dir_ref: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        rv_ms: f64,
        rv_ref: *const std::ffi::c_char,
        freq_out: *mut f64,
    ) -> i32;

    fn measures_shim_frequency_rest_with_doppler(
        freq_hz: f64,
        ref_in: *const std::ffi::c_char,
        doppler_value: f64,
        doppler_ref: *const std::ffi::c_char,
        freq_out: *mut f64,
    ) -> i32;

    fn measures_shim_frequency_shift_with_doppler(
        freq_hz: f64,
        ref_in: *const std::ffi::c_char,
        doppler_value: f64,
        doppler_ref: *const std::ffi::c_char,
        freq_out: *mut f64,
    ) -> i32;

    fn measures_shim_iau2000_precession_matrix(epoch_mjd_tt: f64, mat_out: *mut f64) -> i32;

    fn measures_shim_direction_convert_iau2000a(
        lon_in: f64,
        lat_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        epoch_mjd: f64,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        lon_out: *mut f64,
        lat_out: *mut f64,
    ) -> i32;

    fn measures_shim_earth_velocity(
        epoch_mjd_tdb: f64,
        vx: *mut f64,
        vy: *mut f64,
        vz: *mut f64,
        sun_x: *mut f64,
        sun_y: *mut f64,
        sun_z: *mut f64,
    ) -> i32;

    fn measures_shim_epoch_convert_with_frame(
        mjd_in: f64,
        ref_in: *const std::ffi::c_char,
        ref_out: *const std::ffi::c_char,
        obs_lon: f64,
        obs_lat: f64,
        obs_h: f64,
        dut1: f64,
        mjd_out: *mut f64,
    ) -> i32;

    fn measures_shim_eop_query(
        mjd: f64,
        dut1_out: *mut f64,
        xp_arcsec_out: *mut f64,
        yp_arcsec_out: *mut f64,
    ) -> i32;
}

/// Convert an epoch from one reference frame to another using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_epoch_convert(mjd_in: f64, ref_in: &str, ref_out: &str) -> Result<f64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut mjd_out: f64 = 0.0;

    let rc =
        unsafe { measures_shim_epoch_convert(mjd_in, c_in.as_ptr(), c_out.as_ptr(), &mut mjd_out) };

    if rc == 0 {
        Ok(mjd_out)
    } else {
        Err(format!("C++ epoch_convert failed: rc={rc}"))
    }
}

/// Serialize an epoch to record fields using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_epoch_to_record(mjd_in: f64, ref_in: &str) -> Result<(f64, String, String), String> {
    let c_in = CString::new(ref_in).unwrap();
    let mut value_out: f64 = 0.0;
    let mut unit_buf = [0i8; 64];
    let mut refer_buf = [0i8; 64];

    let rc = unsafe {
        measures_shim_epoch_to_record(
            mjd_in,
            c_in.as_ptr(),
            &mut value_out,
            unit_buf.as_mut_ptr(),
            64,
            refer_buf.as_mut_ptr(),
            64,
        )
    };

    if rc != 0 {
        return Err(format!("C++ epoch_to_record failed: rc={rc}"));
    }

    let unit = unsafe { std::ffi::CStr::from_ptr(unit_buf.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    let refer = unsafe { std::ffi::CStr::from_ptr(refer_buf.as_ptr()) }
        .to_string_lossy()
        .into_owned();

    Ok((value_out, unit, refer))
}

/// Convert a position between reference frames using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_position_convert(
    v0: f64,
    v1: f64,
    v2: f64,
    ref_in: &str,
    ref_out: &str,
) -> Result<(f64, f64, f64), String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut out0: f64 = 0.0;
    let mut out1: f64 = 0.0;
    let mut out2: f64 = 0.0;

    let rc = unsafe {
        measures_shim_position_convert(
            v0,
            v1,
            v2,
            c_in.as_ptr(),
            c_out.as_ptr(),
            &mut out0,
            &mut out1,
            &mut out2,
        )
    };

    if rc == 0 {
        Ok((out0, out1, out2))
    } else {
        Err(format!("C++ position_convert failed: rc={rc}"))
    }
}

/// Serialize an ITRF position to spherical record fields using C++ MVPosition.
#[cfg(has_casacore_cpp)]
pub fn cpp_position_to_record(x: f64, y: f64, z: f64) -> Result<(f64, f64, f64), String> {
    let mut lon: f64 = 0.0;
    let mut lat: f64 = 0.0;
    let mut radius: f64 = 0.0;

    let rc = unsafe { measures_shim_position_to_record(x, y, z, &mut lon, &mut lat, &mut radius) };

    if rc == 0 {
        Ok((lon, lat, radius))
    } else {
        Err(format!("C++ position_to_record failed: rc={rc}"))
    }
}

/// Convert a position to casacore's raw WGS84 XYZ TaQL representation.
#[cfg(has_casacore_cpp)]
pub fn cpp_position_to_wgs_xyz(
    v0: f64,
    v1: f64,
    v2: f64,
    ref_in: &str,
) -> Result<(f64, f64, f64), String> {
    let c_in = CString::new(ref_in).unwrap();
    let mut out0: f64 = 0.0;
    let mut out1: f64 = 0.0;
    let mut out2: f64 = 0.0;

    let rc = unsafe {
        measures_shim_position_to_wgs_xyz(
            v0,
            v1,
            v2,
            c_in.as_ptr(),
            &mut out0,
            &mut out1,
            &mut out2,
        )
    };

    if rc == 0 {
        Ok((out0, out1, out2))
    } else {
        Err(format!("C++ position_to_wgs_xyz failed: rc={rc}"))
    }
}

/// Benchmark position conversion using C++ casacore. Returns elapsed nanoseconds.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_position_convert(
    x_start: f64,
    y: f64,
    z: f64,
    count: i32,
    ref_in: &str,
    ref_out: &str,
    iterations: i32,
) -> Result<u64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut elapsed_ns: u64 = 0;

    let rc = unsafe {
        measures_shim_bench_position_convert(
            x_start,
            y,
            z,
            count,
            c_in.as_ptr(),
            c_out.as_ptr(),
            iterations,
            &mut elapsed_ns,
        )
    };

    if rc == 0 {
        Ok(elapsed_ns)
    } else {
        Err(format!("C++ bench_position_convert failed: rc={rc}"))
    }
}

/// Benchmark epoch conversion using C++ casacore. Returns elapsed nanoseconds.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_epoch_convert(
    mjd_start: f64,
    count: i32,
    ref_in: &str,
    ref_out: &str,
    iterations: i32,
) -> Result<u64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut elapsed_ns: u64 = 0;

    let rc = unsafe {
        measures_shim_bench_epoch_convert(
            mjd_start,
            count,
            c_in.as_ptr(),
            c_out.as_ptr(),
            iterations,
            &mut elapsed_ns,
        )
    };

    if rc == 0 {
        Ok(elapsed_ns)
    } else {
        Err(format!("C++ bench_epoch_convert failed: rc={rc}"))
    }
}

/// Convert a Doppler value between conventions using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_doppler_convert(value_in: f64, ref_in: &str, ref_out: &str) -> Result<f64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut value_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_doppler_convert(value_in, c_in.as_ptr(), c_out.as_ptr(), &mut value_out)
    };

    if rc == 0 {
        Ok(value_out)
    } else {
        Err(format!("C++ doppler_convert failed: rc={rc}"))
    }
}

/// Benchmark Doppler conversion using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_bench_doppler_convert(
    value_start: f64,
    count: i32,
    ref_in: &str,
    ref_out: &str,
    iterations: i32,
) -> Result<u64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut elapsed_ns: u64 = 0;

    let rc = unsafe {
        measures_shim_bench_doppler_convert(
            value_start,
            count,
            c_in.as_ptr(),
            c_out.as_ptr(),
            iterations,
            &mut elapsed_ns,
        )
    };

    if rc == 0 {
        Ok(elapsed_ns)
    } else {
        Err(format!("C++ bench_doppler_convert failed: rc={rc}"))
    }
}

/// Convert a direction between reference frames using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_direction_convert(
    lon_in: f64,
    lat_in: f64,
    ref_in: &str,
    ref_out: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<(f64, f64), String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut lon_out: f64 = 0.0;
    let mut lat_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_direction_convert(
            lon_in,
            lat_in,
            c_in.as_ptr(),
            c_out.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            &mut lon_out,
            &mut lat_out,
        )
    };

    if rc == 0 {
        Ok((lon_out, lat_out))
    } else {
        Err(format!("C++ direction_convert failed: rc={rc}"))
    }
}

/// Convert a named source direction to a target frame using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_named_direction_convert(
    source_name: &str,
    ref_out: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<(f64, f64), String> {
    let c_name = CString::new(source_name).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut lon_out: f64 = 0.0;
    let mut lat_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_named_direction_convert(
            c_name.as_ptr(),
            c_out.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            &mut lon_out,
            &mut lat_out,
        )
    };

    if rc == 0 {
        Ok((lon_out, lat_out))
    } else {
        Err(format!("C++ named_direction_convert failed: rc={rc}"))
    }
}

/// Compute rise and set UTC MJDs for a named source using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_riseset(
    source_name: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<(f64, f64), String> {
    let c_name = CString::new(source_name).unwrap();
    let mut rise_out: f64 = 0.0;
    let mut set_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_riseset(
            c_name.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            &mut rise_out,
            &mut set_out,
        )
    };

    if rc == 0 {
        Ok((rise_out, set_out))
    } else {
        Err(format!("C++ riseset failed: rc={rc}"))
    }
}

/// Resolve a named spectral line to its rest frequency using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_line_frequency(line_name: &str) -> Result<f64, String> {
    let c_name = CString::new(line_name).unwrap();
    let mut freq_out_hz = 0.0;

    let rc = unsafe { measures_shim_line_frequency(c_name.as_ptr(), &mut freq_out_hz) };

    if rc == 0 {
        Ok(freq_out_hz)
    } else {
        Err(format!("C++ line_frequency failed: rc={rc}"))
    }
}

/// Convert an explicit Earth-magnetic vector between frames using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_earthmag_convert_xyz(
    xyz_in: [f64; 3],
    ref_in: &str,
    ref_out: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<[f64; 3], String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut xyz_out = [0.0f64; 3];

    let rc = unsafe {
        measures_shim_earthmag_convert_xyz(
            xyz_in[0],
            xyz_in[1],
            xyz_in[2],
            c_in.as_ptr(),
            c_out.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            &mut xyz_out[0],
            &mut xyz_out[1],
            &mut xyz_out[2],
        )
    };

    if rc == 0 {
        Ok(xyz_out)
    } else {
        Err(format!("C++ earthmag_convert_xyz failed: rc={rc}"))
    }
}

/// Convert an explicit Earth-magnetic angle/length value between frames using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_earthmag_convert_angles(
    lon_in: f64,
    lat_in: f64,
    length_nt: f64,
    ref_in: &str,
    ref_out: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<(f64, f64), String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut lon_out = 0.0f64;
    let mut lat_out = 0.0f64;

    let rc = unsafe {
        measures_shim_earthmag_convert_angles(
            lon_in,
            lat_in,
            length_nt,
            c_in.as_ptr(),
            c_out.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            &mut lon_out,
            &mut lat_out,
        )
    };

    if rc == 0 {
        Ok((lon_out, lat_out))
    } else {
        Err(format!("C++ earthmag_convert_angles failed: rc={rc}"))
    }
}

/// Evaluate an IGRF helper using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_igrf_value(
    mode: &str,
    ref_out: Option<&str>,
    height_m: f64,
    dir_lon: f64,
    dir_lat: f64,
    dir_ref: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<Vec<f64>, String> {
    let c_dir_ref = CString::new(dir_ref).unwrap();

    match mode {
        "xyz" => {
            let c_out = CString::new(ref_out.unwrap_or("ITRF")).unwrap();
            let mut xyz_out = [0.0f64; 3];
            let rc = unsafe {
                measures_shim_igrf_xyz(
                    c_out.as_ptr(),
                    height_m,
                    dir_lon,
                    dir_lat,
                    c_dir_ref.as_ptr(),
                    epoch_mjd,
                    obs_lon,
                    obs_lat,
                    obs_h,
                    &mut xyz_out[0],
                    &mut xyz_out[1],
                    &mut xyz_out[2],
                )
            };
            if rc == 0 {
                Ok(xyz_out.to_vec())
            } else {
                Err(format!("C++ igrf_xyz failed: rc={rc}"))
            }
        }
        "los" => {
            let mut value_out = 0.0f64;
            let rc = unsafe {
                measures_shim_igrf_los(
                    height_m,
                    dir_lon,
                    dir_lat,
                    c_dir_ref.as_ptr(),
                    epoch_mjd,
                    obs_lon,
                    obs_lat,
                    obs_h,
                    &mut value_out,
                )
            };
            if rc == 0 {
                Ok(vec![value_out])
            } else {
                Err(format!("C++ igrf_los failed: rc={rc}"))
            }
        }
        "long" => {
            let mut value_out = 0.0f64;
            let rc = unsafe {
                measures_shim_igrf_long(
                    height_m,
                    dir_lon,
                    dir_lat,
                    c_dir_ref.as_ptr(),
                    epoch_mjd,
                    obs_lon,
                    obs_lat,
                    obs_h,
                    &mut value_out,
                )
            };
            if rc == 0 {
                Ok(vec![value_out])
            } else {
                Err(format!("C++ igrf_long failed: rc={rc}"))
            }
        }
        other => Err(format!("unknown IGRF mode: {other}")),
    }
}

/// Get C++ casacore's IAU 2000 bias-precession matrix at a given TT MJD epoch.
#[cfg(has_casacore_cpp)]
pub fn cpp_iau2000_precession_matrix(epoch_mjd_tt: f64) -> Result<[[f64; 3]; 3], String> {
    let _guard = lock_casacore_global_state(CasacoreGlobalStateDomain::MeasuresIau2000A);
    let mut mat = [0.0f64; 9];
    let rc = unsafe { measures_shim_iau2000_precession_matrix(epoch_mjd_tt, mat.as_mut_ptr()) };
    if rc == 0 {
        Ok([
            [mat[0], mat[1], mat[2]],
            [mat[3], mat[4], mat[5]],
            [mat[6], mat[7], mat[8]],
        ])
    } else {
        Err(format!("C++ iau2000_precession_matrix failed: rc={rc}"))
    }
}

/// Convert a direction using C++ casacore with IAU 2006/2000A nutation/precession.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_direction_convert_iau2000a(
    lon_in: f64,
    lat_in: f64,
    ref_in: &str,
    ref_out: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<(f64, f64), String> {
    let _guard = lock_casacore_global_state(CasacoreGlobalStateDomain::MeasuresIau2000A);
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut lon_out: f64 = 0.0;
    let mut lat_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_direction_convert_iau2000a(
            lon_in,
            lat_in,
            c_in.as_ptr(),
            c_out.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            &mut lon_out,
            &mut lat_out,
        )
    };

    if rc == 0 {
        Ok((lon_out, lat_out))
    } else {
        Err(format!("C++ direction_convert_iau2000a failed: rc={rc}"))
    }
}

/// Get the Earth aberration velocity and Sun position from C++ casacore.
/// Returns ((vx, vy, vz), (sun_x, sun_y, sun_z)).
/// Velocity is in units of c. Sun position is in AU.
#[cfg(has_casacore_cpp)]
pub fn cpp_earth_velocity(epoch_mjd_tdb: f64) -> Result<([f64; 3], [f64; 3]), String> {
    let (mut vx, mut vy, mut vz) = (0.0, 0.0, 0.0);
    let (mut sx, mut sy, mut sz) = (0.0, 0.0, 0.0);
    let rc = unsafe {
        measures_shim_earth_velocity(
            epoch_mjd_tdb,
            &mut vx,
            &mut vy,
            &mut vz,
            &mut sx,
            &mut sy,
            &mut sz,
        )
    };
    if rc == 0 {
        Ok(([vx, vy, vz], [sx, sy, sz]))
    } else {
        Err(format!("C++ earth_velocity failed: rc={rc}"))
    }
}

/// Benchmark direction conversion using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_bench_direction_convert(
    lon_start: f64,
    lat: f64,
    count: i32,
    ref_in: &str,
    ref_out: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
    iterations: i32,
) -> Result<u64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut elapsed_ns: u64 = 0;

    let rc = unsafe {
        measures_shim_bench_direction_convert(
            lon_start,
            lat,
            count,
            c_in.as_ptr(),
            c_out.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            iterations,
            &mut elapsed_ns,
        )
    };

    if rc == 0 {
        Ok(elapsed_ns)
    } else {
        Err(format!("C++ bench_direction_convert failed: rc={rc}"))
    }
}

/// Convert a frequency between reference frames using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_frequency_convert(
    freq_hz: f64,
    ref_in: &str,
    ref_out: &str,
    dir_lon: f64,
    dir_lat: f64,
    dir_ref: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<f64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let c_dir = CString::new(dir_ref).unwrap();
    let mut freq_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_frequency_convert(
            freq_hz,
            c_in.as_ptr(),
            c_out.as_ptr(),
            dir_lon,
            dir_lat,
            c_dir.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            &mut freq_out,
        )
    };

    if rc == 0 {
        Ok(freq_out)
    } else {
        Err(format!("C++ frequency_convert failed: rc={rc}"))
    }
}

/// Benchmark frequency conversion using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_bench_frequency_convert(
    freq_start: f64,
    count: i32,
    ref_in: &str,
    ref_out: &str,
    dir_lon: f64,
    dir_lat: f64,
    dir_ref: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
    iterations: i32,
) -> Result<u64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let c_dir = CString::new(dir_ref).unwrap();
    let mut elapsed_ns: u64 = 0;

    let rc = unsafe {
        measures_shim_bench_frequency_convert(
            freq_start,
            count,
            c_in.as_ptr(),
            c_out.as_ptr(),
            dir_lon,
            dir_lat,
            c_dir.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            iterations,
            &mut elapsed_ns,
        )
    };

    if rc == 0 {
        Ok(elapsed_ns)
    } else {
        Err(format!("C++ bench_frequency_convert failed: rc={rc}"))
    }
}

/// Convert a radial velocity between reference frames using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_radvel_convert(
    ms_in: f64,
    ref_in: &str,
    ref_out: &str,
    dir_lon: f64,
    dir_lat: f64,
    dir_ref: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<f64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let c_dir = CString::new(dir_ref).unwrap();
    let mut ms_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_radvel_convert(
            ms_in,
            c_in.as_ptr(),
            c_out.as_ptr(),
            dir_lon,
            dir_lat,
            c_dir.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            &mut ms_out,
        )
    };

    if rc == 0 {
        Ok(ms_out)
    } else {
        Err(format!("C++ radvel_convert failed: rc={rc}"))
    }
}

/// Benchmark radial velocity conversion using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_bench_radvel_convert(
    ms_start: f64,
    count: i32,
    ref_in: &str,
    ref_out: &str,
    dir_lon: f64,
    dir_lat: f64,
    dir_ref: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
    iterations: i32,
) -> Result<u64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let c_dir = CString::new(dir_ref).unwrap();
    let mut elapsed_ns: u64 = 0;

    let rc = unsafe {
        measures_shim_bench_radvel_convert(
            ms_start,
            count,
            c_in.as_ptr(),
            c_out.as_ptr(),
            dir_lon,
            dir_lat,
            c_dir.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            iterations,
            &mut elapsed_ns,
        )
    };

    if rc == 0 {
        Ok(elapsed_ns)
    } else {
        Err(format!("C++ bench_radvel_convert failed: rc={rc}"))
    }
}

/// Convert a frequency with radial velocity in the frame using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_frequency_convert_with_rv(
    freq_hz: f64,
    ref_in: &str,
    ref_out: &str,
    dir_lon: f64,
    dir_lat: f64,
    dir_ref: &str,
    epoch_mjd: f64,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
    rv_ms: f64,
    rv_ref: &str,
) -> Result<f64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let c_dir = CString::new(dir_ref).unwrap();
    let c_rv = CString::new(rv_ref).unwrap();
    let mut freq_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_frequency_convert_with_rv(
            freq_hz,
            c_in.as_ptr(),
            c_out.as_ptr(),
            dir_lon,
            dir_lat,
            c_dir.as_ptr(),
            epoch_mjd,
            obs_lon,
            obs_lat,
            obs_h,
            rv_ms,
            c_rv.as_ptr(),
            &mut freq_out,
        )
    };

    if rc == 0 {
        Ok(freq_out)
    } else {
        Err(format!("C++ frequency_convert_with_rv failed: rc={rc}"))
    }
}

/// Convert an observed frequency to a rest frequency using a Doppler value.
#[cfg(has_casacore_cpp)]
pub fn cpp_frequency_rest_with_doppler(
    freq_hz: f64,
    ref_in: &str,
    doppler_value: f64,
    doppler_ref: &str,
) -> Result<f64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_doppler = CString::new(doppler_ref).unwrap();
    let mut freq_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_frequency_rest_with_doppler(
            freq_hz,
            c_in.as_ptr(),
            doppler_value,
            c_doppler.as_ptr(),
            &mut freq_out,
        )
    };

    if rc == 0 {
        Ok(freq_out)
    } else {
        Err(format!("C++ frequency_rest_with_doppler failed: rc={rc}"))
    }
}

/// Shift a rest frequency using a Doppler value.
#[cfg(has_casacore_cpp)]
pub fn cpp_frequency_shift_with_doppler(
    freq_hz: f64,
    ref_in: &str,
    doppler_value: f64,
    doppler_ref: &str,
) -> Result<f64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_doppler = CString::new(doppler_ref).unwrap();
    let mut freq_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_frequency_shift_with_doppler(
            freq_hz,
            c_in.as_ptr(),
            doppler_value,
            c_doppler.as_ptr(),
            &mut freq_out,
        )
    };

    if rc == 0 {
        Ok(freq_out)
    } else {
        Err(format!("C++ frequency_shift_with_doppler failed: rc={rc}"))
    }
}

/// Convert an epoch with position and dUT1 in the frame using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub fn cpp_epoch_convert_with_frame(
    mjd_in: f64,
    ref_in: &str,
    ref_out: &str,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
    dut1: f64,
) -> Result<f64, String> {
    let c_in = CString::new(ref_in).unwrap();
    let c_out = CString::new(ref_out).unwrap();
    let mut mjd_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_epoch_convert_with_frame(
            mjd_in,
            c_in.as_ptr(),
            c_out.as_ptr(),
            obs_lon,
            obs_lat,
            obs_h,
            dut1,
            &mut mjd_out,
        )
    };

    if rc == 0 {
        Ok(mjd_out)
    } else {
        Err(format!("C++ epoch_convert_with_frame failed: rc={rc}"))
    }
}

/// Query EOP data (dUT1, polar motion) from C++ casacore's IERS tables.
///
/// Returns `(dut1_seconds, xp_arcsec, yp_arcsec)`.
#[cfg(has_casacore_cpp)]
pub fn cpp_eop_query(mjd: f64) -> Result<(f64, f64, f64), String> {
    let mut dut1: f64 = 0.0;
    let mut xp: f64 = 0.0;
    let mut yp: f64 = 0.0;

    let rc = unsafe { measures_shim_eop_query(mjd, &mut dut1, &mut xp, &mut yp) };

    if rc == 0 {
        Ok((dut1, xp, yp))
    } else {
        Err(format!("C++ eop_query failed: rc={rc}"))
    }
}
