// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Convert a frequency between reference frames using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn cpp_frequency_convert(
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
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_dir = CasacoreOracleRuntime::c_string("measures input", dir_ref)
        .map_err(|error| error.to_string())?;
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

/// Convert a frequency using the same persistent-converter pattern used by
/// CASA `VLAFiller` when converting channel grids.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn cpp_frequency_convert_via_model(
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
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_dir = CasacoreOracleRuntime::c_string("measures input", dir_ref)
        .map_err(|error| error.to_string())?;
    let mut freq_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_frequency_convert_via_model(
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
        Err(format!("C++ frequency_convert_via_model failed: rc={rc}"))
    }
}

/// Convert a frequency using a converter model that is created before the frame
/// is updated, mirroring the lifecycle of CASA `VLAFiller::itsFreqCtr`.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn cpp_frequency_convert_via_mutated_model(
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
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_dir = CasacoreOracleRuntime::c_string("measures input", dir_ref)
        .map_err(|error| error.to_string())?;
    let mut freq_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_frequency_convert_via_mutated_model(
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
        Err(format!(
            "C++ frequency_convert_via_mutated_model failed: rc={rc}"
        ))
    }
}

/// Convert a frequency using CASA `MSUtil::getFreqRangeInSpw`'s persistent
/// converter and mutable-frame lifecycle.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn cpp_frequency_convert_via_reset_frame(
    freq_hz: f64,
    ref_in: &str,
    ref_out: &str,
    initial_dir_lon: f64,
    initial_dir_lat: f64,
    initial_epoch_mjd: f64,
    target_dir_lon: f64,
    target_dir_lat: f64,
    target_epoch_mjd: f64,
    dir_ref: &str,
    obs_lon: f64,
    obs_lat: f64,
    obs_h: f64,
) -> Result<f64, String> {
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_dir = CasacoreOracleRuntime::c_string("measures input", dir_ref)
        .map_err(|error| error.to_string())?;
    let mut freq_out = 0.0;
    let rc = unsafe {
        measures_shim_frequency_convert_via_reset_frame(
            freq_hz,
            c_in.as_ptr(),
            c_out.as_ptr(),
            initial_dir_lon,
            initial_dir_lat,
            initial_epoch_mjd,
            target_dir_lon,
            target_dir_lat,
            target_epoch_mjd,
            c_dir.as_ptr(),
            obs_lon,
            obs_lat,
            obs_h,
            &mut freq_out,
        )
    };
    if rc == 0 {
        Ok(freq_out)
    } else {
        Err(format!(
            "C++ frequency_convert_via_reset_frame failed: rc={rc}"
        ))
    }
}

/// Convert a frequency between reference frames using distinct source and
/// target measures frames in C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn cpp_frequency_convert_between_frames(
    freq_hz: f64,
    ref_in: &str,
    ref_out: &str,
    src_dir_lon: f64,
    src_dir_lat: f64,
    src_dir_ref: &str,
    src_epoch_mjd: f64,
    src_obs_lon: f64,
    src_obs_lat: f64,
    src_obs_h: f64,
    dst_dir_lon: f64,
    dst_dir_lat: f64,
    dst_dir_ref: &str,
    dst_epoch_mjd: f64,
    dst_obs_lon: f64,
    dst_obs_lat: f64,
    dst_obs_h: f64,
) -> Result<f64, String> {
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_src_dir = CasacoreOracleRuntime::c_string("measures input", src_dir_ref)
        .map_err(|error| error.to_string())?;
    let c_dst_dir = CasacoreOracleRuntime::c_string("measures input", dst_dir_ref)
        .map_err(|error| error.to_string())?;
    let mut freq_out: f64 = 0.0;

    let rc = unsafe {
        measures_shim_frequency_convert_between_frames(
            freq_hz,
            c_in.as_ptr(),
            c_out.as_ptr(),
            src_dir_lon,
            src_dir_lat,
            c_src_dir.as_ptr(),
            src_epoch_mjd,
            src_obs_lon,
            src_obs_lat,
            src_obs_h,
            dst_dir_lon,
            dst_dir_lat,
            c_dst_dir.as_ptr(),
            dst_epoch_mjd,
            dst_obs_lon,
            dst_obs_lat,
            dst_obs_h,
            &mut freq_out,
        )
    };

    if rc == 0 {
        Ok(freq_out)
    } else {
        Err(format!(
            "C++ frequency_convert_between_frames failed: rc={rc}"
        ))
    }
}

/// Benchmark frequency conversion using C++ casacore.
#[cfg(has_casacore_cpp)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn cpp_bench_frequency_convert(
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
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_dir = CasacoreOracleRuntime::c_string("measures input", dir_ref)
        .map_err(|error| error.to_string())?;
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
pub(crate) fn cpp_radvel_convert(
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
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_dir = CasacoreOracleRuntime::c_string("measures input", dir_ref)
        .map_err(|error| error.to_string())?;
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
pub(crate) fn cpp_bench_radvel_convert(
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
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_dir = CasacoreOracleRuntime::c_string("measures input", dir_ref)
        .map_err(|error| error.to_string())?;
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
pub(crate) fn cpp_frequency_convert_with_rv(
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
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_out = CasacoreOracleRuntime::c_string("measures input", ref_out)
        .map_err(|error| error.to_string())?;
    let c_dir = CasacoreOracleRuntime::c_string("measures input", dir_ref)
        .map_err(|error| error.to_string())?;
    let c_rv = CasacoreOracleRuntime::c_string("measures input", rv_ref)
        .map_err(|error| error.to_string())?;
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
pub(crate) fn cpp_frequency_rest_with_doppler(
    freq_hz: f64,
    ref_in: &str,
    doppler_value: f64,
    doppler_ref: &str,
) -> Result<f64, String> {
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_doppler = CasacoreOracleRuntime::c_string("measures input", doppler_ref)
        .map_err(|error| error.to_string())?;
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
pub(crate) fn cpp_frequency_shift_with_doppler(
    freq_hz: f64,
    ref_in: &str,
    doppler_value: f64,
    doppler_ref: &str,
) -> Result<f64, String> {
    let c_in = CasacoreOracleRuntime::c_string("measures input", ref_in)
        .map_err(|error| error.to_string())?;
    let c_doppler = CasacoreOracleRuntime::c_string("measures input", doppler_ref)
        .map_err(|error| error.to_string())?;
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
