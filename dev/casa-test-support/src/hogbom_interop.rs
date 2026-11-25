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
        mask_in: *const u8,
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
    /// The Fortran oracle consumes column-major, unit-peak planes and treats
    /// `[nx / 2, ny / 2]` as the PSF reference pixel. This bridge normalizes
    /// the working planes and embeds them in a zero-padded canvas, translating
    /// the PSF peak to that reference pixel without wrapping finite support.
    /// Returned model and residual planes are cropped back into the caller's
    /// canonical coordinates; production coordinates are never changed to
    /// accommodate the test oracle.
    pub fn clean_minor_cycle_2d(
        psf: &[f32],
        residual: &[f32],
        shape: [usize; 2],
        gain: f32,
        threshold: f32,
        cycle_niter: usize,
    ) -> Result<HogbomMinorCycle2d, OracleError> {
        Self::clean_minor_cycle_2d_with_mask(
            psf,
            residual,
            shape,
            None,
            gain,
            threshold,
            cycle_niter,
        )
    }

    /// Run one casacore `hclean` call with an explicit x-major CLEAN mask.
    pub fn clean_minor_cycle_2d_masked(
        psf: &[f32],
        residual: &[f32],
        shape: [usize; 2],
        mask: &[bool],
        gain: f32,
        threshold: f32,
        cycle_niter: usize,
    ) -> Result<HogbomMinorCycle2d, OracleError> {
        Self::clean_minor_cycle_2d_with_mask(
            psf,
            residual,
            shape,
            Some(mask),
            gain,
            threshold,
            cycle_niter,
        )
    }

    fn clean_minor_cycle_2d_with_mask(
        psf: &[f32],
        residual: &[f32],
        shape: [usize; 2],
        mask: Option<&[bool]>,
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
            if mask.is_some_and(|mask| mask.len() != nx * ny) {
                return Err(OracleError::InvalidInput {
                    context: "hogbom mask",
                    message: format!("expected {} mask pixels for shape {shape:?}", nx * ny),
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
            let padded_shape = [
                nx.checked_mul(2).ok_or_else(|| OracleError::InvalidInput {
                    context: "hogbom plane shape",
                    message: "padded width overflow".to_owned(),
                })?,
                ny.checked_mul(2).ok_or_else(|| OracleError::InvalidInput {
                    context: "hogbom plane shape",
                    message: "padded height overflow".to_owned(),
                })?,
            ];
            let offset = [nx - psf_peak[0], ny - psf_peak[1]];
            let normalized_psf = psf
                .iter()
                .map(|value| *value / magnitude)
                .collect::<Vec<_>>();
            let normalized_residual = residual
                .iter()
                .map(|value| *value / magnitude)
                .collect::<Vec<_>>();
            let psf = pack_fortran_padded(&normalized_psf, [nx, ny], padded_shape, offset);
            let residual =
                pack_fortran_padded(&normalized_residual, [nx, ny], padded_shape, offset);
            let mask = mask.map(|mask| {
                let values = mask
                    .iter()
                    .map(|value| u8::from(*value))
                    .collect::<Vec<_>>();
                pack_fortran_padded(&values, [nx, ny], padded_shape, offset)
            });
            let padded_cells = padded_shape[0] * padded_shape[1];
            let mut model_out = vec![0.0f32; padded_cells];
            let mut residual_out = vec![0.0f32; padded_cells];
            let mut iterdone = 0i32;
            let mut peak = 0.0f32;
            let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
            let rc = unsafe {
                ffi_cpp_hogbom_clean_minor_cycle_2d(
                    padded_shape[0] as i32,
                    padded_shape[1] as i32,
                    gain,
                    threshold / magnitude,
                    cycle_niter as i32,
                    psf.as_ptr(),
                    residual.as_ptr(),
                    mask.as_ref().map_or(std::ptr::null(), Vec::as_ptr),
                    model_out.as_mut_ptr(),
                    residual_out.as_mut_ptr(),
                    padded_cells as i32,
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
                peak_residual_jy_per_beam: peak * magnitude,
                model: unpack_fortran_padded(&model_out, [nx, ny], padded_shape, offset),
                residual: unpack_fortran_padded(&residual_out, [nx, ny], padded_shape, offset)
                    .into_iter()
                    .map(|value| value * magnitude)
                    .collect(),
            })
        })
    }
}

#[cfg(has_casacore_cpp)]
fn pack_fortran_padded<T: Copy + Default>(
    input: &[T],
    shape: [usize; 2],
    padded_shape: [usize; 2],
    offset: [usize; 2],
) -> Vec<T> {
    let [nx, ny] = shape;
    let [padded_nx, padded_ny] = padded_shape;
    let mut packed = vec![T::default(); padded_nx * padded_ny];
    for source_x in 0..nx {
        for source_y in 0..ny {
            let oracle_x = source_x + offset[0];
            let oracle_y = source_y + offset[1];
            packed[oracle_x + padded_nx * oracle_y] = input[source_x * ny + source_y];
        }
    }
    packed
}

#[cfg(has_casacore_cpp)]
fn unpack_fortran_padded(
    input: &[f32],
    shape: [usize; 2],
    padded_shape: [usize; 2],
    offset: [usize; 2],
) -> Vec<f32> {
    let [nx, ny] = shape;
    let [padded_nx, _] = padded_shape;
    let mut unpacked = vec![0.0; nx * ny];
    for source_x in 0..nx {
        for source_y in 0..ny {
            let oracle_x = source_x + offset[0];
            let oracle_y = source_y + offset[1];
            unpacked[source_x * ny + source_y] = input[oracle_x + padded_nx * oracle_y];
        }
    }
    unpacked
}
