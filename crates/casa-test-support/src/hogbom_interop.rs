// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ casacore `hclean` interop helpers.

#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;
use crate::oracle_runtime::{OracleError, oracle_operation};

/// Result of running one casacore `hclean` minor-cycle call on a single plane.
#[derive(Debug, Clone, PartialEq)]
pub struct HogbomMinorCycle2d {
    /// CASA-reported iteration count returned by `hclean`.
    pub iterdone: usize,
    /// Peak absolute residual after the minor-cycle call.
    pub peak_residual_jy_per_beam: f32,
    /// Updated model plane in canonical x-major `(x, y)` order.
    pub model: Vec<f32>,
    /// Updated residual plane in canonical x-major `(x, y)` order.
    pub residual: Vec<f32>,
}

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    #[link_name = "cpp_hogbom_clean_minor_cycle_2d"]
    fn ffi_cpp_hogbom_clean_minor_cycle_2d(
        nx: i32,
        ny: i32,
        gain: f32,
        threshold: f32,
        cycle_niter: i32,
        psf_in: *const f32,
        residual_in: *const f32,
        model_out: *mut f32,
        residual_out: *mut f32,
        max_len: i32,
        iterdone_out: *mut i32,
        peak_out: *mut f32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_table_free_error(ptr: *mut std::ffi::c_char);
}

/// Typed Rust-facing access to casacore's Hogbom oracle.
pub struct HogbomOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl HogbomOracle {
    /// Run one casacore `hclean` minor-cycle call on a single residual/PSF plane.
    ///
    /// Rust imaging planes are x-major and may place the PSF peak anywhere.
    /// The Fortran oracle consumes column-major arrays and treats
    /// `[nx / 2, ny / 2]` as the PSF reference pixel. This bridge transcodes
    /// every plane and translates the PSF, residual, and returned model by
    /// the same periodic image offset. The inverse translation restores the
    /// caller's coordinates; production coordinates are never changed to
    /// accommodate the test oracle.
    pub fn clean_minor_cycle_2d(
        psf: &[f32],
        residual: &[f32],
        shape: [usize; 2],
        gain: f32,
        threshold: f32,
        cycle_niter: usize,
    ) -> Result<HogbomMinorCycle2d, OracleError> {
        oracle_operation!("hogbom.clean_minor_cycle_2d", {
            let [nx, ny] = shape;
            if psf.len() != nx * ny || residual.len() != nx * ny {
                return Err(OracleError::InvalidInput {
                    context: "hogbom planes",
                    message: format!(
                        "expected {} pixels for shape {:?}, got psf={} residual={}",
                        nx * ny,
                        shape,
                        psf.len(),
                        residual.len()
                    ),
                });
            }

            let mut psf_peak = None::<(usize, f32)>;
            for (index, value) in psf.iter().copied().enumerate() {
                let magnitude = value.abs();
                if value.is_finite() && psf_peak.is_none_or(|(_, best)| magnitude > best) {
                    psf_peak = Some((index, magnitude));
                }
            }
            let (psf_peak, magnitude) = psf_peak.ok_or_else(|| OracleError::InvalidInput {
                context: "hogbom PSF plane",
                message: "PSF has no finite peak".to_owned(),
            })?;
            if magnitude == 0.0 {
                return Err(OracleError::InvalidInput {
                    context: "hogbom PSF plane",
                    message: "PSF peak is zero".to_owned(),
                });
            }
            let psf_peak = [psf_peak / ny, psf_peak % ny];
            let psf = pack_fortran_centered(psf, [nx, ny], psf_peak);
            let residual = pack_fortran_centered(residual, [nx, ny], psf_peak);
            let mut model_out = vec![0.0f32; nx * ny];
            let mut residual_out = vec![0.0f32; nx * ny];
            let mut iterdone = 0i32;
            let mut peak = 0.0f32;
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe {
                ffi_cpp_hogbom_clean_minor_cycle_2d(
                    nx as i32,
                    ny as i32,
                    gain,
                    threshold,
                    cycle_niter as i32,
                    psf.as_ptr(),
                    residual.as_ptr(),
                    model_out.as_mut_ptr(),
                    residual_out.as_mut_ptr(),
                    (nx * ny) as i32,
                    &mut iterdone,
                    &mut peak,
                    &mut error,
                )
            };
            unsafe {
                CasacoreOracleRuntime::cpp_status(
                    "hogbom.clean_minor_cycle_2d",
                    rc,
                    error,
                    cpp_table_free_error,
                )?;
            }
            Ok(HogbomMinorCycle2d {
                iterdone: iterdone as usize,
                peak_residual_jy_per_beam: peak,
                model: unpack_fortran_centered(&model_out, [nx, ny], psf_peak),
                residual: unpack_fortran_centered(&residual_out, [nx, ny], psf_peak),
            })
        })
    }
}

#[cfg(has_casacore_cpp)]
fn pack_fortran_centered(input: &[f32], shape: [usize; 2], peak: [usize; 2]) -> Vec<f32> {
    let [nx, ny] = shape;
    let centre = [nx / 2, ny / 2];
    let mut packed = vec![0.0; nx * ny];
    for source_x in 0..nx {
        let oracle_x = (source_x + centre[0] + nx - peak[0]) % nx;
        for source_y in 0..ny {
            let oracle_y = (source_y + centre[1] + ny - peak[1]) % ny;
            packed[oracle_x + nx * oracle_y] = input[source_x * ny + source_y];
        }
    }
    packed
}

#[cfg(has_casacore_cpp)]
fn unpack_fortran_centered(input: &[f32], shape: [usize; 2], peak: [usize; 2]) -> Vec<f32> {
    let [nx, ny] = shape;
    let centre = [nx / 2, ny / 2];
    let mut unpacked = vec![0.0; nx * ny];
    for source_x in 0..nx {
        let oracle_x = (source_x + centre[0] + nx - peak[0]) % nx;
        for source_y in 0..ny {
            let oracle_y = (source_y + centre[1] + ny - peak[1]) % ny;
            unpacked[source_x * ny + source_y] = input[oracle_x + nx * oracle_y];
        }
    }
    unpacked
}
