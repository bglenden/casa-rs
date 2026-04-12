// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ casacore `ConvolveGridder` interop helpers.

/// One nonzero cell written by C++ `ConvolveGridder::grid()` for a unit sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GridderSampleCell {
    /// Grid x index.
    pub x: usize,
    /// Grid y index.
    pub y: usize,
    /// Real part of the gridded value.
    pub re: f32,
    /// Imaginary part of the gridded value.
    pub im: f32,
}

/// Result of gridding a single unit sample with C++ `ConvolveGridder`.
#[derive(Debug, Clone, PartialEq)]
pub struct GridderSamplePatch {
    /// Rounded grid location from `Gridder::location()`.
    pub location: [i32; 2],
    /// Continuous grid position from `Gridder::position()`.
    pub grid_position: [f64; 2],
    /// Convolution support radius.
    pub support: i32,
    /// Kernel oversampling factor.
    pub sampling: i32,
    /// Nonzero cells written into the grid.
    pub cells: Vec<GridderSampleCell>,
}

/// Result of making a corrected dirty image with C++ casacore gridding helpers.
#[derive(Debug, Clone, PartialEq)]
pub struct GridderImage2d {
    /// Output image shape `[nx, ny]`.
    pub image_shape: [usize; 2],
    /// Real-valued image plane stored in ndarray-compatible `(x, y)` iteration order.
    pub pixels: Vec<f32>,
}

#[cfg(has_casacore_cpp)]
unsafe extern "C" {
    #[link_name = "cpp_convolve_gridder_grid_unit_sample_2d"]
    fn ffi_cpp_convolve_gridder_grid_unit_sample_2d(
        nx: i32,
        ny: i32,
        scale_x: f64,
        scale_y: f64,
        offset_x: f64,
        offset_y: f64,
        u: f64,
        v: f64,
        loc_out: *mut i32,
        gpos_out: *mut f64,
        support_out: *mut i32,
        sampling_out: *mut i32,
        x_out: *mut i32,
        y_out: *mut i32,
        value_re_out: *mut f32,
        value_im_out: *mut f32,
        max_points: i32,
        count_out: *mut i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    fn cpp_table_free_error(ptr: *mut std::ffi::c_char);

    #[link_name = "cpp_convolve_gridder_correction_row_2d"]
    fn ffi_cpp_convolve_gridder_correction_row_2d(
        nx: i32,
        ny: i32,
        scale_x: f64,
        scale_y: f64,
        offset_x: f64,
        offset_y: f64,
        locy: i32,
        factor_out: *mut f32,
        max_len: i32,
        nread_out: *mut i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;

    #[link_name = "cpp_convolve_gridder_make_dirty_image_2d"]
    fn ffi_cpp_convolve_gridder_make_dirty_image_2d(
        grid_nx: i32,
        grid_ny: i32,
        image_nx: i32,
        image_ny: i32,
        scale_x: f64,
        scale_y: f64,
        offset_x: f64,
        offset_y: f64,
        u_out: *const f64,
        v_out: *const f64,
        vis_re_out: *const f32,
        vis_im_out: *const f32,
        weight_out: *const f32,
        gridable_out: *const u8,
        nsamples: i32,
        image_out: *mut f32,
        max_image_len: i32,
        out_error: *mut *mut std::ffi::c_char,
    ) -> i32;
}

/// Grid a single unit sample with C++ casacore `ConvolveGridder`.
pub fn cpp_convolve_gridder_grid_unit_sample_2d(
    grid_shape: [usize; 2],
    scale: [f64; 2],
    offset: [f64; 2],
    uv_lambda: [f64; 2],
) -> Result<GridderSamplePatch, String> {
    #[cfg(has_casacore_cpp)]
    {
        let mut location = [0_i32; 2];
        let mut grid_position = [0.0_f64; 2];
        let mut support = 0_i32;
        let mut sampling = 0_i32;
        let max_points = 64_i32;
        let mut x = vec![0_i32; max_points as usize];
        let mut y = vec![0_i32; max_points as usize];
        let mut re = vec![0.0_f32; max_points as usize];
        let mut im = vec![0.0_f32; max_points as usize];
        let mut count = 0_i32;
        let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
        let rc = unsafe {
            ffi_cpp_convolve_gridder_grid_unit_sample_2d(
                grid_shape[0] as i32,
                grid_shape[1] as i32,
                scale[0],
                scale[1],
                offset[0],
                offset[1],
                uv_lambda[0],
                uv_lambda[1],
                location.as_mut_ptr(),
                grid_position.as_mut_ptr(),
                &mut support,
                &mut sampling,
                x.as_mut_ptr(),
                y.as_mut_ptr(),
                re.as_mut_ptr(),
                im.as_mut_ptr(),
                max_points,
                &mut count,
                &mut error,
            )
        };
        if rc != 0 {
            let message = if error.is_null() {
                "casacore gridder shim failed".to_string()
            } else {
                let message = unsafe { std::ffi::CStr::from_ptr(error) }
                    .to_string_lossy()
                    .to_string();
                unsafe { cpp_table_free_error(error) };
                message
            };
            return Err(message);
        }
        Ok(GridderSamplePatch {
            location,
            grid_position,
            support,
            sampling,
            cells: (0..count as usize)
                .map(|index| GridderSampleCell {
                    x: x[index] as usize,
                    y: y[index] as usize,
                    re: re[index],
                    im: im[index],
                })
                .collect(),
        })
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = (grid_shape, scale, offset, uv_lambda);
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Return a 1D correction row from C++ casacore `ConvolveGridder::correctX1D()`.
pub fn cpp_convolve_gridder_correction_row_2d(
    grid_shape: [usize; 2],
    scale: [f64; 2],
    offset: [f64; 2],
    locy: usize,
) -> Result<Vec<f32>, String> {
    #[cfg(has_casacore_cpp)]
    {
        let mut factor = vec![0.0_f32; grid_shape[0]];
        let mut nread = 0_i32;
        let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
        let rc = unsafe {
            ffi_cpp_convolve_gridder_correction_row_2d(
                grid_shape[0] as i32,
                grid_shape[1] as i32,
                scale[0],
                scale[1],
                offset[0],
                offset[1],
                locy as i32,
                factor.as_mut_ptr(),
                factor.len() as i32,
                &mut nread,
                &mut error,
            )
        };
        if rc != 0 {
            let message = if error.is_null() {
                "casacore correction-row shim failed".to_string()
            } else {
                let message = unsafe { std::ffi::CStr::from_ptr(error) }
                    .to_string_lossy()
                    .to_string();
                unsafe { cpp_table_free_error(error) };
                message
            };
            return Err(message);
        }
        factor.truncate(nread as usize);
        Ok(factor)
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = (grid_shape, scale, offset, locy);
        Err("casacore C++ backend unavailable".to_string())
    }
}

/// Make a corrected dirty image with C++ casacore `ConvolveGridder` + `LatticeFFT`.
#[allow(clippy::too_many_arguments)]
pub fn cpp_convolve_gridder_make_dirty_image_2d(
    grid_shape: [usize; 2],
    image_shape: [usize; 2],
    scale: [f64; 2],
    offset: [f64; 2],
    u_lambda: &[f64],
    v_lambda: &[f64],
    visibility_re: &[f32],
    visibility_im: &[f32],
    weight: &[f32],
    gridable: &[bool],
) -> Result<GridderImage2d, String> {
    let len = u_lambda.len();
    if v_lambda.len() != len
        || visibility_re.len() != len
        || visibility_im.len() != len
        || weight.len() != len
        || gridable.len() != len
    {
        return Err("dirty-image inputs must have matching lengths".to_string());
    }
    #[cfg(has_casacore_cpp)]
    {
        let mut image = vec![0.0_f32; image_shape[0] * image_shape[1]];
        let gridable_bytes = gridable
            .iter()
            .map(|value| u8::from(*value))
            .collect::<Vec<_>>();
        let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
        let rc = unsafe {
            ffi_cpp_convolve_gridder_make_dirty_image_2d(
                grid_shape[0] as i32,
                grid_shape[1] as i32,
                image_shape[0] as i32,
                image_shape[1] as i32,
                scale[0],
                scale[1],
                offset[0],
                offset[1],
                u_lambda.as_ptr(),
                v_lambda.as_ptr(),
                visibility_re.as_ptr(),
                visibility_im.as_ptr(),
                weight.as_ptr(),
                gridable_bytes.as_ptr(),
                len as i32,
                image.as_mut_ptr(),
                image.len() as i32,
                &mut error,
            )
        };
        if rc != 0 {
            let message = if error.is_null() {
                "casacore dirty-image shim failed".to_string()
            } else {
                let message = unsafe { std::ffi::CStr::from_ptr(error) }
                    .to_string_lossy()
                    .to_string();
                unsafe { cpp_table_free_error(error) };
                message
            };
            return Err(message);
        }
        Ok(GridderImage2d {
            image_shape,
            pixels: image,
        })
    }
    #[cfg(not(has_casacore_cpp))]
    {
        let _ = (
            grid_shape,
            image_shape,
            scale,
            offset,
            u_lambda,
            v_lambda,
            visibility_re,
            visibility_im,
            weight,
            gridable,
        );
        Err("casacore C++ backend unavailable".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_cpp_backend_reports_unavailable_and_validates_lengths() {
        if cfg!(has_casacore_cpp) {
            assert!(cpp_convolve_gridder_grid_unit_sample_2d(
                [16, 16],
                [1.0, 1.0],
                [0.0, 0.0],
                [0.1, 0.2],
            )
            .is_err());
            assert!(cpp_convolve_gridder_correction_row_2d([16, 16], [1.0, 1.0], [0.0, 0.0], 3)
                .is_ok());
            assert_eq!(
                cpp_convolve_gridder_make_dirty_image_2d(
                    [16, 16],
                    [8, 8],
                    [1.0, 1.0],
                    [0.0, 0.0],
                    &[0.0, 1.0],
                    &[0.0],
                    &[1.0, 1.0],
                    &[0.0, 0.0],
                    &[1.0, 1.0],
                    &[true, true],
                ),
                Err("dirty-image inputs must have matching lengths".to_string())
            );
            assert!(cpp_convolve_gridder_make_dirty_image_2d(
                [16, 16],
                [8, 8],
                [1.0, 1.0],
                [0.0, 0.0],
                &[0.0, 1.0],
                &[0.0, 1.0],
                &[1.0, 1.0],
                &[0.0, 0.0],
                &[1.0, 1.0],
                &[true, true],
            )
            .is_err());
        } else {
            assert_eq!(
                cpp_convolve_gridder_grid_unit_sample_2d(
                    [16, 16],
                    [1.0, 1.0],
                    [0.0, 0.0],
                    [0.1, 0.2],
                ),
                Err("casacore C++ backend unavailable".to_string())
            );
            assert_eq!(
                cpp_convolve_gridder_correction_row_2d([16, 16], [1.0, 1.0], [0.0, 0.0], 3),
                Err("casacore C++ backend unavailable".to_string())
            );
            assert_eq!(
                cpp_convolve_gridder_make_dirty_image_2d(
                    [16, 16],
                    [8, 8],
                    [1.0, 1.0],
                    [0.0, 0.0],
                    &[0.0, 1.0],
                    &[0.0],
                    &[1.0, 1.0],
                    &[0.0, 0.0],
                    &[1.0, 1.0],
                    &[true, true],
                ),
                Err("dirty-image inputs must have matching lengths".to_string())
            );
            assert_eq!(
                cpp_convolve_gridder_make_dirty_image_2d(
                    [16, 16],
                    [8, 8],
                    [1.0, 1.0],
                    [0.0, 0.0],
                    &[0.0, 1.0],
                    &[0.0, 1.0],
                    &[1.0, 1.0],
                    &[0.0, 0.0],
                    &[1.0, 1.0],
                    &[true, true],
                ),
                Err("casacore C++ backend unavailable".to_string())
            );
        }
    }
}
