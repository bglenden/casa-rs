// SPDX-License-Identifier: LGPL-3.0-or-later
//! Private casacore C++ oracle implementation.

#[cfg(has_casacore_cpp)]
use crate::oracle_ffi::cpp_table_free_error;
#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;

/// Result of the C++ forced-I/O lattice statistics benchmark.
pub struct CppLatticeStatisticsBenchResult {
    pub basic_ns: u64,
    pub order_ns: u64,
    pub mean: Vec<f64>,
    pub sigma: Vec<f64>,
    pub median: Vec<f64>,
    pub q1: Vec<f64>,
    pub q3: Vec<f64>,
}

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    fn cpp_lattice_stats_float_forced_io(
        path: *const std::ffi::c_char,
        shape: *const i32,
        ndim: i32,
        tile_shape: *const i32,
        tile_ndim: i32,
        cache_tiles: u64,
        mean_out: *mut f64,
        sigma_out: *mut f64,
        median_out: *mut f64,
        q1_out: *mut f64,
        q3_out: *mut f64,
        output_len: i64,
        basic_ns_out: *mut u64,
        order_ns_out: *mut u64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_lattice_stats_float_forced_io_repeated_basic(
        path: *const std::ffi::c_char,
        shape: *const i32,
        ndim: i32,
        tile_shape: *const i32,
        tile_ndim: i32,
        cache_tiles: u64,
        iterations: u32,
        total_ns_out: *mut u64,
        checksum_out: *mut f64,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
}

/// Run the C++ forced-I/O paged-lattice statistics benchmark.
///
/// The shim creates a C++ `PagedArray<Float>` at `path`, fills it with the
/// deterministic ramp `x + y*nx + z*(nx*ny)`, constrains the tile cache to
/// `cache_tiles`, temp-closes it, and then times:
/// - basic stats: `NPTS`, `MEAN`, `SIGMA`
/// - order stats: `MEDIAN`, `Q1`, `Q3`
///
/// The benchmark currently expects a 3-D shape and uses `axes=[0,1]`, so the
/// returned vectors have length `shape[2]`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_lattice_statistics_forced_io_bench(
    path: &std::path::Path,
    shape: &[i32],
    tile_shape: &[i32],
    cache_tiles: u64,
) -> Result<CppLatticeStatisticsBenchResult, String> {
    if shape.len() != tile_shape.len() {
        return Err(format!(
            "shape/tile ndim mismatch: {} vs {}",
            shape.len(),
            tile_shape.len()
        ));
    }
    let output_len = shape
        .last()
        .copied()
        .ok_or_else(|| "shape must not be empty".to_string())? as usize;

    let c_path =
        CasacoreOracleRuntime::c_path("lattice path", path).map_err(|error| error.to_string())?;

    let mut basic_ns = 0u64;
    let mut order_ns = 0u64;
    let mut mean = vec![0.0f64; output_len];
    let mut sigma = vec![0.0f64; output_len];
    let mut median = vec![0.0f64; output_len];
    let mut q1 = vec![0.0f64; output_len];
    let mut q3 = vec![0.0f64; output_len];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        cpp_lattice_stats_float_forced_io(
            c_path.as_ptr(),
            shape.as_ptr(),
            shape.len() as i32,
            tile_shape.as_ptr(),
            tile_shape.len() as i32,
            cache_tiles,
            mean.as_mut_ptr(),
            sigma.as_mut_ptr(),
            median.as_mut_ptr(),
            q1.as_mut_ptr(),
            q3.as_mut_ptr(),
            output_len as i64,
            &mut basic_ns,
            &mut order_ns,
            &mut error,
        )
    };

    if rc != 0 {
        return Err(unsafe {
            CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error)
        });
    }

    Ok(CppLatticeStatisticsBenchResult {
        basic_ns,
        order_ns,
        mean,
        sigma,
        median,
        q1,
        q3,
    })
}

/// Run only the C++ basic-family forced-I/O lattice-statistics workload
/// repeatedly on one prepared paged lattice.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_lattice_statistics_forced_io_repeated_basic(
    path: &std::path::Path,
    shape: &[i32],
    tile_shape: &[i32],
    cache_tiles: u64,
    iterations: u32,
) -> Result<(u64, f64), String> {
    if shape.len() != tile_shape.len() {
        return Err(format!(
            "shape/tile ndim mismatch: {} vs {}",
            shape.len(),
            tile_shape.len()
        ));
    }
    let c_path =
        CasacoreOracleRuntime::c_path("lattice path", path).map_err(|error| error.to_string())?;
    let mut total_ns = 0u64;
    let mut checksum = 0.0f64;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        cpp_lattice_stats_float_forced_io_repeated_basic(
            c_path.as_ptr(),
            shape.as_ptr(),
            shape.len() as i32,
            tile_shape.as_ptr(),
            tile_shape.len() as i32,
            cache_tiles,
            iterations,
            &mut total_ns,
            &mut checksum,
            &mut error,
        )
    };

    if rc == 0 {
        Ok((total_ns, checksum))
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}
