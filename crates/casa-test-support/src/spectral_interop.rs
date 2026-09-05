// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ casacore `InterpolateArray1D` spectral coefficient oracle.

#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;
use crate::oracle_runtime::{OracleError, oracle_operation};

/// CASA/casacore one-dimensional spectral interpolation method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SpectralInterpolationMethod {
    /// Select the nearest input coordinate.
    Nearest = 0,
    /// Interpolate between the bracketing pair.
    Linear = 1,
    /// Use CASA/casacore four-point polynomial interpolation.
    Cubic = 2,
}

/// Exact CASA/casacore coefficient and edge-validity result.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralInterpolationOracleResult {
    /// One coefficient for each input coordinate, in source order.
    pub coefficients: Vec<f64>,
    /// Whether the requested coordinate is inside the source coordinate range.
    pub valid: bool,
}

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    #[link_name = "cpp_spectral_interpolation_coefficients"]
    fn ffi_cpp_spectral_interpolation_coefficients(
        input_coordinates: *const f64,
        input_count: i32,
        output_coordinate: f64,
        method: i32,
        coefficients_out: *mut f64,
        valid_out: *mut u8,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
    fn cpp_table_free_error(ptr: *mut std::ffi::c_char);
}

/// Typed access to CASA/casacore spectral interpolation behavior.
pub struct SpectralInterpolationOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl SpectralInterpolationOracle {
    /// Interpolate basis vectors to expose the exact CASA/casacore coefficients.
    pub fn coefficients(
        input_coordinates: &[f64],
        output_coordinate: f64,
        method: SpectralInterpolationMethod,
    ) -> Result<SpectralInterpolationOracleResult, OracleError> {
        oracle_operation!("spectral.interpolation_coefficients", {
            let input_count =
                i32::try_from(input_coordinates.len()).map_err(|_| OracleError::InvalidInput {
                    context: "spectral input coordinates",
                    message: "coordinate count exceeds i32".to_owned(),
                })?;
            if input_count == 0 || input_coordinates.iter().any(|value| !value.is_finite()) {
                return Err(OracleError::InvalidInput {
                    context: "spectral input coordinates",
                    message: "coordinates must be finite and nonempty".to_owned(),
                });
            }
            let mut coefficients = vec![0.0; input_coordinates.len()];
            let mut valid = 0_u8;
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe {
                ffi_cpp_spectral_interpolation_coefficients(
                    input_coordinates.as_ptr(),
                    input_count,
                    output_coordinate,
                    method as i32,
                    coefficients.as_mut_ptr(),
                    &mut valid,
                    &mut error,
                )
            };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "spectral.interpolation_coefficients",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            Ok(SpectralInterpolationOracleResult {
                coefficients,
                valid: valid != 0,
            })
        })
    }
}
