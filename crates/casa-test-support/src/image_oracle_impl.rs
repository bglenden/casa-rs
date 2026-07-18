// SPDX-License-Identifier: LGPL-3.0-or-later
//! Private casacore C++ oracle implementation.

#[cfg(has_casacore_cpp)]
use crate::oracle_ffi::*;
#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::CasacoreOracleRuntime;
#[cfg(has_casacore_cpp)]
use crate::{Complex32, Complex64};

// ---------------------------------------------------------------------------
// PagedImage interop helpers
// ---------------------------------------------------------------------------

/// Creates a C++ `PagedImage<Float>` with the given shape, data, and units.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_create_image(
    path: &std::path::Path,
    shape: &[i32],
    data: &[f32],
    units: &str,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_units =
        CasacoreOracleRuntime::c_string("image units", units).map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_create_pagedimage_float(
            c_path.as_ptr(),
            shape.as_ptr(),
            shape.len() as i32,
            data.as_ptr(),
            data.len() as i64,
            c_units.as_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Creates a C++ `PagedImage<Float>` with an explicit tile shape.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_create_image_tiled(
    path: &std::path::Path,
    shape: &[i32],
    tile_shape: &[i32],
    data: &[f32],
    units: &str,
) -> Result<(), String> {
    if shape.len() != tile_shape.len() {
        return Err(format!(
            "shape/tile ndim mismatch: {} vs {}",
            shape.len(),
            tile_shape.len()
        ));
    }
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_units =
        CasacoreOracleRuntime::c_string("image units", units).map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_create_pagedimage_float_tiled(
            c_path.as_ptr(),
            shape.as_ptr(),
            tile_shape.as_ptr(),
            shape.len() as i32,
            data.as_ptr(),
            data.len() as i64,
            c_units.as_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads all pixel data from a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_data(
    path: &std::path::Path,
    max_size: usize,
) -> Result<Vec<f32>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut data = vec![0.0f32; max_size];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_float(
            c_path.as_ptr(),
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data)
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Creates a C++ `PagedImage<Double>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_create_image_f64(
    path: &std::path::Path,
    shape: &[i32],
    data: &[f64],
    units: &str,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_units =
        CasacoreOracleRuntime::c_string("image units", units).map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_create_pagedimage_double(
            c_path.as_ptr(),
            shape.as_ptr(),
            shape.len() as i32,
            data.as_ptr(),
            data.len() as i64,
            c_units.as_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads all pixel data from a C++ `PagedImage<Double>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_data_f64(
    path: &std::path::Path,
    max_size: usize,
) -> Result<Vec<f64>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut data = vec![0.0f64; max_size];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_double(
            c_path.as_ptr(),
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data)
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Creates a C++ `PagedImage<Complex>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_create_image_complex32(
    path: &std::path::Path,
    shape: &[i32],
    data: &[Complex32],
    units: &str,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_units =
        CasacoreOracleRuntime::c_string("image units", units).map_err(|error| error.to_string())?;
    let flat: Vec<f32> = data.iter().flat_map(|v| [v.re, v.im]).collect();
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_create_pagedimage_complex32(
            c_path.as_ptr(),
            shape.as_ptr(),
            shape.len() as i32,
            flat.as_ptr(),
            data.len() as i64,
            c_units.as_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads all pixel data from a C++ `PagedImage<Complex>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_data_complex32(
    path: &std::path::Path,
    max_size: usize,
) -> Result<Vec<Complex32>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut flat = vec![0.0f32; max_size * 2];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_complex32(
            c_path.as_ptr(),
            flat.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        Ok((0..nread as usize)
            .map(|i| Complex32::new(flat[2 * i], flat[2 * i + 1]))
            .collect())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Creates a C++ `PagedImage<DComplex>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_create_image_complex64(
    path: &std::path::Path,
    shape: &[i32],
    data: &[Complex64],
    units: &str,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_units =
        CasacoreOracleRuntime::c_string("image units", units).map_err(|error| error.to_string())?;
    let flat: Vec<f64> = data.iter().flat_map(|v| [v.re, v.im]).collect();
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_create_pagedimage_complex64(
            c_path.as_ptr(),
            shape.as_ptr(),
            shape.len() as i32,
            flat.as_ptr(),
            data.len() as i64,
            c_units.as_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads all pixel data from a C++ `PagedImage<DComplex>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_data_complex64(
    path: &std::path::Path,
    max_size: usize,
) -> Result<Vec<Complex64>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut flat = vec![0.0f64; max_size * 2];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_complex64(
            c_path.as_ptr(),
            flat.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        Ok((0..nread as usize)
            .map(|i| Complex64::new(flat[2 * i], flat[2 * i + 1]))
            .collect())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads the shape of a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_shape(path: &std::path::Path) -> Result<Vec<i32>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut shape = vec![0i32; 8];
    let mut ndim: i32 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_shape(
            c_path.as_ptr(),
            shape.as_mut_ptr(),
            8,
            &mut ndim,
            &mut error,
        )
    };
    if rc == 0 {
        shape.truncate(ndim as usize);
        Ok(shape)
    } else {
        let msg = unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) };
        Err(msg)
    }
}

/// Reads the units string from a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_units(path: &std::path::Path) -> Result<String, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut buf = vec![0i8; 256];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_units(
            c_path.as_ptr(),
            buf.as_mut_ptr() as *mut std::ffi::c_char,
            256,
            &mut error,
        )
    };
    if rc == 0 {
        CasacoreOracleRuntime::output_c_char_string("image.read_units", &buf)
            .map_err(|error| error.to_string())
    } else {
        let msg = unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) };
        Err(msg)
    }
}

/// Creates a C++ `TempImage<Float>`, fills metadata, and materializes it to disk.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_create_temp_image_materialized(
    path: &std::path::Path,
    shape: &[i32],
    data: &[f32],
    units: &str,
    object_name: &str,
    image_type: &str,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_units =
        CasacoreOracleRuntime::c_string("image units", units).map_err(|error| error.to_string())?;
    let c_object = CasacoreOracleRuntime::c_string("image object name", object_name)
        .map_err(|error| error.to_string())?;
    let c_image_type = CasacoreOracleRuntime::c_string("image type", image_type)
        .map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_create_tempimage_float_materialized(
            c_path.as_ptr(),
            shape.as_ptr(),
            shape.len() as i32,
            data.as_ptr(),
            data.len() as i64,
            c_units.as_ptr(),
            c_object.as_ptr(),
            c_image_type.as_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads the number of coordinates from a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_coordinate_count(path: &std::path::Path) -> Result<i32, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut count: i32 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc =
        unsafe { cpp_read_pagedimage_coordinate_count(c_path.as_ptr(), &mut count, &mut error) };
    if rc == 0 {
        Ok(count)
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads the default mask name from a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_default_mask_name(path: &std::path::Path) -> Result<String, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut buf = vec![0i8; 256];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_default_mask_name(
            c_path.as_ptr(),
            buf.as_mut_ptr() as *mut std::ffi::c_char,
            256,
            &mut error,
        )
    };
    if rc == 0 {
        CasacoreOracleRuntime::output_c_char_string("image.read_default_mask_name", &buf)
            .map_err(|error| error.to_string())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads the default pixel-mask contents from a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_default_mask(
    path: &std::path::Path,
    max_size: usize,
) -> Result<Vec<bool>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut data = vec![0u8; max_size];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_default_mask(
            c_path.as_ptr(),
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data.into_iter().map(|value| value != 0).collect())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Writes a native WCPolygon saved region using the C++ casacore implementation.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_write_polygon_region(
    path: &std::path::Path,
    region_name: &str,
    x: &[f64],
    y: &[f64],
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_name = CasacoreOracleRuntime::c_string("image region or mask name", region_name)
        .map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        ffi_cpp_write_polygon_region(
            c_path.as_ptr(),
            c_name.as_ptr(),
            x.as_ptr(),
            y.as_ptr(),
            x.len() as i32,
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Writes a native WCUnion-of-polygons saved region using the C++ casacore implementation.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_write_union_region(
    path: &std::path::Path,
    region_name: &str,
    x1: &[f64],
    y1: &[f64],
    x2: &[f64],
    y2: &[f64],
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_name = CasacoreOracleRuntime::c_string("image region or mask name", region_name)
        .map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        ffi_cpp_write_union_region(
            c_path.as_ptr(),
            c_name.as_ptr(),
            x1.as_ptr(),
            y1.as_ptr(),
            x1.len() as i32,
            x2.as_ptr(),
            y2.as_ptr(),
            x2.len() as i32,
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Writes an unsupported WCBox saved region using the C++ casacore implementation.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_write_box_region(
    path: &std::path::Path,
    region_name: &str,
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_name = CasacoreOracleRuntime::c_string("image region or mask name", region_name)
        .map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        ffi_cpp_write_box_region(c_path.as_ptr(), c_name.as_ptr(), x0, y0, x1, y1, &mut error)
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads the native casacore class name for a saved region.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_region_class(
    path: &std::path::Path,
    region_name: &str,
) -> Result<String, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_name = CasacoreOracleRuntime::c_string("image region or mask name", region_name)
        .map_err(|error| error.to_string())?;
    let mut buf = vec![0i8; 256];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        ffi_cpp_read_region_class(
            c_path.as_ptr(),
            c_name.as_ptr(),
            buf.as_mut_ptr() as *mut std::ffi::c_char,
            256,
            &mut error,
        )
    };
    if rc == 0 {
        CasacoreOracleRuntime::output_c_char_string("image.read_region_class", &buf)
            .map_err(|error| error.to_string())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads all saved region names visible to C++ casacore.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_region_names(path: &std::path::Path) -> Result<Vec<String>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut buf = vec![0i8; 4096];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        ffi_cpp_read_region_names(
            c_path.as_ptr(),
            buf.as_mut_ptr() as *mut std::ffi::c_char,
            4096,
            &mut error,
        )
    };
    if rc == 0 {
        let joined = CasacoreOracleRuntime::output_c_char_string("image.read_region_names", &buf)
            .map_err(|error| error.to_string())?;
        if joined.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(joined.split(',').map(|name| name.to_string()).collect())
        }
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CppUnsupportedRegionKind {
    Ellipsoid,
    Intersection,
    Difference,
    Complement,
    Concatenation,
    Extension,
    LelMask,
    LcBox,
}

#[cfg(has_casacore_cpp)]
impl CppUnsupportedRegionKind {
    fn ffi_code(self) -> i32 {
        match self {
            Self::Ellipsoid => 0,
            Self::Intersection => 1,
            Self::Difference => 2,
            Self::Complement => 3,
            Self::Concatenation => 4,
            Self::Extension => 5,
            Self::LelMask => 6,
            Self::LcBox => 7,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CppRegionStatistics {
    pub pixel_count: usize,
    pub sum: f64,
    pub mean: f64,
    pub median: f64,
    pub rms: f64,
    pub sigma: f64,
    pub min: f64,
    pub max: f64,
}

#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_write_unsupported_region(
    path: &std::path::Path,
    region_name: &str,
    kind: CppUnsupportedRegionKind,
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_name = CasacoreOracleRuntime::c_string("image region or mask name", region_name)
        .map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        ffi_cpp_write_unsupported_region(
            c_path.as_ptr(),
            c_name.as_ptr(),
            kind.ffi_code(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_region_statistics(
    path: &std::path::Path,
    region_name: &str,
) -> Result<CppRegionStatistics, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_name = CasacoreOracleRuntime::c_string("image region or mask name", region_name)
        .map_err(|error| error.to_string())?;
    let mut stats = [0.0f64; 8];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        ffi_cpp_read_region_statistics(
            c_path.as_ptr(),
            c_name.as_ptr(),
            stats.as_mut_ptr(),
            stats.len() as i32,
            &mut error,
        )
    };
    if rc == 0 {
        Ok(CppRegionStatistics {
            pixel_count: stats[0] as usize,
            sum: stats[1],
            mean: stats[2],
            median: stats[3],
            rms: stats[4],
            sigma: stats[5],
            min: stats[6],
            max: stats[7],
        })
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Writes a named default pixel mask using the C++ casacore implementation.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_write_default_mask(
    path: &std::path::Path,
    mask_name: &str,
    data: &[bool],
) -> Result<(), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let c_name = CasacoreOracleRuntime::c_string("image region or mask name", mask_name)
        .map_err(|error| error.to_string())?;
    let bytes = data
        .iter()
        .map(|value| u8::from(*value))
        .collect::<Vec<_>>();
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        ffi_cpp_write_default_mask(
            c_path.as_ptr(),
            c_name.as_ptr(),
            bytes.as_ptr(),
            bytes.len() as i64,
            &mut error,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads the image-info object name from a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_info_object_name(path: &std::path::Path) -> Result<String, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut buf = vec![0i8; 256];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_imageinfo_object_name(
            c_path.as_ptr(),
            buf.as_mut_ptr() as *mut std::ffi::c_char,
            256,
            &mut error,
        )
    };
    if rc == 0 {
        CasacoreOracleRuntime::output_c_char_string("image.read_info_object_name", &buf)
            .map_err(|error| error.to_string())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads the image-info type from a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_info_type(path: &std::path::Path) -> Result<String, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut buf = vec![0i8; 256];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_imageinfo_type(
            c_path.as_ptr(),
            buf.as_mut_ptr() as *mut std::ffi::c_char,
            256,
            &mut error,
        )
    };
    if rc == 0 {
        CasacoreOracleRuntime::output_c_char_string("image.read_info_type", &buf)
            .map_err(|error| error.to_string())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Reads a sub-cube slice from a C++ `PagedImage<Float>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_read_image_slice(
    path: &std::path::Path,
    start: &[i32],
    length: &[i32],
) -> Result<Vec<f32>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let max_size: i64 = length.iter().map(|&l| l as i64).product();
    let mut data = vec![0.0f32; max_size as usize];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_read_pagedimage_slice(
            c_path.as_ptr(),
            start.as_ptr(),
            length.as_ptr(),
            start.len() as i32,
            data.as_mut_ptr(),
            max_size,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data)
    } else {
        let msg = unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) };
        Err(msg)
    }
}

/// Evaluates a unary `ImageExpr<Float>` in C++ and returns the materialized pixels.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_eval_image_expr_unary(
    path: &std::path::Path,
    op: CppImageExprUnaryOp,
    max_size: usize,
) -> Result<Vec<f32>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut data = vec![0.0f32; max_size];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_eval_pagedimage_float_unary_expr(
            c_path.as_ptr(),
            op as i32,
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data)
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Evaluates a binary `ImageExpr<Float>` in C++ and returns the materialized pixels.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_eval_image_expr_binary(
    lhs_path: &std::path::Path,
    rhs_path: &std::path::Path,
    op: CppImageExprBinaryOp,
    max_size: usize,
) -> Result<Vec<f32>, String> {
    let c_lhs = CasacoreOracleRuntime::c_path("left image path", lhs_path)
        .map_err(|error| error.to_string())?;
    let c_rhs = CasacoreOracleRuntime::c_path("right image path", rhs_path)
        .map_err(|error| error.to_string())?;
    let mut data = vec![0.0f32; max_size];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_eval_pagedimage_float_binary_expr(
            c_lhs.as_ptr(),
            c_rhs.as_ptr(),
            op as i32,
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data)
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Evaluates an image/scalar `ImageExpr<Float>` in C++ and returns the materialized pixels.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_eval_image_expr_scalar(
    path: &std::path::Path,
    scalar: f32,
    op: CppImageExprBinaryOp,
    max_size: usize,
) -> Result<Vec<f32>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut data = vec![0.0f32; max_size];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_eval_pagedimage_float_scalar_expr(
            c_path.as_ptr(),
            scalar,
            op as i32,
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data)
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Evaluates a representative comparison/logical `ImageExpr<Bool>` in C++.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_eval_image_mask_range(
    path: &std::path::Path,
    lower_cmp: CppImageExprCompareOp,
    lower: f32,
    logical_op: CppMaskLogicalOp,
    upper_cmp: CppImageExprCompareOp,
    upper: f32,
    max_size: usize,
) -> Result<Vec<bool>, String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut data = vec![0u8; max_size];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_eval_pagedimage_float_range_mask_expr(
            c_path.as_ptr(),
            lower_cmp as i32,
            lower,
            logical_op as i32,
            upper_cmp as i32,
            upper,
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data.into_iter().map(|value| value != 0).collect())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Evaluates the fixed Wave 11c closeout expression in C++ and returns a slice.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_eval_image_expr_closeout_slice(
    path: &std::path::Path,
    start: &[i32],
    length: &[i32],
) -> Result<Vec<f32>, String> {
    assert_eq!(start.len(), length.len(), "start/length ndim mismatch");
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let max_size = length
        .iter()
        .fold(1usize, |acc, &dim| acc.saturating_mul(dim as usize));
    let mut data = vec![0.0f32; max_size];
    let mut nread: i64 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_eval_pagedimage_float_closeout_expr_slice(
            c_path.as_ptr(),
            start.as_ptr(),
            length.as_ptr(),
            start.len() as i32,
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        Ok(data)
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Unary operators supported by the C++ image-expression shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CppImageExprUnaryOp {
    Negate = 0,
    Exp = 1,
    Sin = 2,
    Cos = 3,
    Tan = 4,
    Asin = 5,
    Acos = 6,
    Atan = 7,
    Sinh = 8,
    Cosh = 9,
    Tanh = 10,
    Log = 11,
    Log10 = 12,
    Sqrt = 13,
    Abs = 14,
    Ceil = 15,
    Floor = 16,
    Round = 17,
    Sign = 18,
    Conj = 19,
}

/// Binary operators supported by the C++ image-expression shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CppImageExprBinaryOp {
    Add = 0,
    Multiply = 1,
    Subtract = 2,
    Divide = 3,
    Pow = 4,
    Fmod = 5,
    Atan2 = 6,
    Min = 7,
    Max = 8,
}

/// Comparison operators supported by the C++ image-expression shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CppImageExprCompareOp {
    GreaterThan = 0,
    LessThan = 1,
    GreaterEqual = 2,
    LessEqual = 3,
    Equal = 4,
    NotEqual = 5,
}

/// Logical operators supported by the C++ mask-expression shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CppMaskLogicalOp {
    And = 0,
    Or = 1,
}

/// Evaluate a LEL expression string using the C++ `ImageExprParse::command`
/// parser and return the result as a flat (Fortran-order) `Vec<f32>`.
///
/// The expression string may reference on-disk images by their filesystem path.
/// The `max_size` parameter is the maximum number of output elements.
///
/// Returns the data, shape, and ndim on success.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_eval_lel_expr(
    expr: &str,
    max_size: usize,
) -> Result<(Vec<f32>, Vec<i32>), String> {
    let c_expr = CasacoreOracleRuntime::c_string("image expression", expr)
        .map_err(|error| error.to_string())?;
    let mut data = vec![0.0f32; max_size];
    let mut nread: i64 = 0;
    let mut shape = vec![0i32; 8];
    let mut ndim: i32 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_eval_lel_expr_float(
            c_expr.as_ptr(),
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            shape.as_mut_ptr(),
            8,
            &mut ndim,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        shape.truncate(ndim as usize);
        Ok((data, shape))
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_profile_lel_scalar_expr(expr: &str, passes: usize) -> Result<[f64; 3], String> {
    let c_expr = CasacoreOracleRuntime::c_string("image expression", expr)
        .map_err(|error| error.to_string())?;
    let mut timings = [0.0f64; 3];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_profile_lel_scalar_expr_float(
            c_expr.as_ptr(),
            passes as i32,
            timings.as_mut_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok(timings)
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Evaluate a boolean LEL expression string using C++ `ImageExprParse::command`
/// and return the result as a flat (Fortran-order) `Vec<bool>`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_eval_lel_expr_mask(
    expr: &str,
    max_size: usize,
) -> Result<(Vec<bool>, Vec<i32>), String> {
    let c_expr = CasacoreOracleRuntime::c_string("image expression", expr)
        .map_err(|error| error.to_string())?;
    let mut data = vec![0u8; max_size];
    let mut nread: i64 = 0;
    let mut shape = vec![0i32; 8];
    let mut ndim: i32 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_eval_lel_expr_bool(
            c_expr.as_ptr(),
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            shape.as_mut_ptr(),
            8,
            &mut ndim,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        shape.truncate(ndim as usize);
        let bools: Vec<bool> = data.into_iter().map(|v| v != 0).collect();
        Ok((bools, shape))
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Save a LEL expression as an `.imgexpr` file using C++.
///
/// The C++ parser evaluates the expression and then `ImageExpr<Float>::save()`
/// writes the `imageexpr.json` file.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_save_lel_expr_file(
    expr: &str,
    save_path: &std::path::Path,
) -> Result<(), String> {
    let c_expr = CasacoreOracleRuntime::c_string("image expression", expr)
        .map_err(|error| error.to_string())?;
    let c_path = CasacoreOracleRuntime::c_path("image save path", save_path)
        .map_err(|error| error.to_string())?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe { cpp_save_lel_expr(c_expr.as_ptr(), c_path.as_ptr(), &mut error) };
    if rc == 0 {
        Ok(())
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Open an `.imgexpr` file using C++ and return the pixel data.
///
/// Uses `ImageOpener::openImageExpr()` to open the file and reads all pixels
/// as `f32`.
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_open_lel_expr_file(
    path: &std::path::Path,
    max_size: usize,
) -> Result<(Vec<f32>, Vec<i32>), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut data = vec![0.0f32; max_size];
    let mut nread: i64 = 0;
    let mut shape = vec![0i32; 8];
    let mut ndim: i32 = 0;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_open_lel_expr_float(
            c_path.as_ptr(),
            data.as_mut_ptr(),
            max_size as i64,
            &mut nread,
            shape.as_mut_ptr(),
            8,
            &mut ndim,
            &mut error,
        )
    };
    if rc == 0 {
        data.truncate(nread as usize);
        shape.truncate(ndim as usize);
        Ok((data, shape))
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Benchmarks C++ `PagedImage<Float>` plane-by-plane I/O.
///
/// Creates a 3D image with the given shape and tile shape, writes all z-planes
/// sequentially with a unique pixel pattern, then reopens and reads them back.
///
/// Returns `(create_ms, write_ms, read_ms)`.
/// Benchmarks plane-by-plane image I/O using C++ casacore.
///
/// `max_cache_mib`: when > 0, limits the C++ tile cache to this many MiB
/// via `ROTiledStManAccessor`. Pass 0 for unlimited (default behaviour).
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_bench_image_plane_by_plane(
    path: &std::path::Path,
    shape: &[i32],
    tile: &[i32],
    max_cache_mib: i32,
) -> Result<(f64, f64, f64), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut timings = [0.0f64; 3];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_bench_plane_by_plane(
            c_path.as_ptr(),
            shape.as_ptr(),
            tile.as_ptr(),
            shape.len() as i32,
            max_cache_mib,
            timings.as_mut_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok((timings[0], timings[1], timings[2]))
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Returns `(create_ms, write_ms, read_ms)`.
/// Benchmarks spectrum-by-spectrum (1,1,nz) image I/O using C++ casacore.
///
/// `max_cache_mib`: when > 0, limits the C++ tile cache to this many MiB
/// via `ROTiledStManAccessor`. Pass 0 for unlimited (default behaviour).
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_bench_image_spectrum_by_spectrum(
    path: &std::path::Path,
    shape: &[i32],
    tile: &[i32],
    max_cache_mib: i32,
) -> Result<(f64, f64, f64), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut timings = [0.0f64; 3];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_bench_spectrum_by_spectrum(
            c_path.as_ptr(),
            shape.as_ptr(),
            tile.as_ptr(),
            shape.len() as i32,
            max_cache_mib,
            timings.as_mut_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok((timings[0], timings[1], timings[2]))
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}

/// Returns `(create_ms, write_ms, read_ms)`.
/// Benchmarks plane-by-plane Complex32 image I/O using C++ casacore.
///
/// `max_cache_mib`: when > 0, limits the C++ tile cache to this many MiB
/// via `ROTiledStManAccessor`. Pass 0 for unlimited (default behaviour).
#[cfg(has_casacore_cpp)]
pub(crate) fn cpp_bench_image_plane_by_plane_complex(
    path: &std::path::Path,
    shape: &[i32],
    tile: &[i32],
    max_cache_mib: i32,
) -> Result<(f64, f64, f64), String> {
    let c_path =
        CasacoreOracleRuntime::c_path("image path", path).map_err(|error| error.to_string())?;
    let mut timings = [0.0f64; 3];
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();
    let rc = unsafe {
        cpp_bench_plane_by_plane_complex(
            c_path.as_ptr(),
            shape.as_ptr(),
            tile.as_ptr(),
            shape.len() as i32,
            max_cache_mib,
            timings.as_mut_ptr(),
            &mut error,
        )
    };
    if rc == 0 {
        Ok((timings[0], timings[1], timings[2]))
    } else {
        Err(unsafe { CasacoreOracleRuntime::cpp_error_message(error, cpp_table_free_error) })
    }
}
