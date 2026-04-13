// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ casacore `hclean` interop helpers.

/// Result of running one casacore `hclean` minor-cycle call on a single plane.
#[derive(Debug, Clone, PartialEq)]
pub struct HogbomMinorCycle2d {
    /// CASA-reported iteration count returned by `hclean`.
    pub iterdone: usize,
    /// Peak absolute residual after the minor-cycle call.
    pub peak_residual_jy_per_beam: f32,
    /// Updated model plane in ndarray-compatible `(x, y)` order.
    pub model: Vec<f32>,
    /// Updated residual plane in ndarray-compatible `(x, y)` order.
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

/// Run one casacore `hclean` minor-cycle call on a single residual/PSF plane.
pub fn cpp_hogbom_clean_minor_cycle_2d(
    psf: &[f32],
    residual: &[f32],
    shape: [usize; 2],
    gain: f32,
    threshold: f32,
    cycle_niter: usize,
) -> Result<HogbomMinorCycle2d, String> {
    #[cfg(has_casacore_cpp)]
    {
        let [nx, ny] = shape;
        if psf.len() != nx * ny || residual.len() != nx * ny {
            return Err(format!(
                "hogbom interop expects {} pixels for shape {:?}, got psf={} residual={}",
                nx * ny,
                shape,
                psf.len(),
                residual.len()
            ));
        }

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
        if rc != 0 {
            let message = if error.is_null() {
                "casacore hogbom shim failed".to_string()
            } else {
                let message = unsafe { std::ffi::CStr::from_ptr(error) }
                    .to_string_lossy()
                    .to_string();
                unsafe { cpp_table_free_error(error) };
                message
            };
            return Err(message);
        }
        Ok(HogbomMinorCycle2d {
            iterdone: iterdone as usize,
            peak_residual_jy_per_beam: peak,
            model: model_out,
            residual: residual_out,
        })
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = (psf, residual, shape, gain, threshold, cycle_niter);
        Err("casacore C++ backend unavailable".to_string())
    }
}
