// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed facade for the casacore image oracle.

#[cfg(has_casacore_cpp)]
use crate::image_oracle_impl::*;
use crate::oracle_runtime::OracleError;
#[cfg(has_casacore_cpp)]
use crate::oracle_runtime::{CasacoreOracleRuntime, OracleDomain};
use crate::{
    Complex32, Complex64, CppImageExprBinaryOp, CppImageExprCompareOp, CppImageExprUnaryOp,
    CppMaskLogicalOp, CppRegionStatistics, CppUnsupportedRegionKind,
};

macro_rules! image_operation {
    ($operation:expr, $body:block) => {{
        #[cfg(has_casacore_cpp)]
        {
            CasacoreOracleRuntime::require($operation)?;
            let _guard = CasacoreOracleRuntime::lock(OracleDomain::Imaging)?;
            $body
        }
        #[cfg(not(has_casacore_cpp))]
        {
            Err(OracleError::Unavailable {
                capability: $operation,
            })
        }
    }};
}

/// Stable Rust-facing domain facade.
pub struct ImageOracle;

#[cfg_attr(not(has_casacore_cpp), allow(unused_variables))]
impl ImageOracle {
    #[allow(clippy::too_many_arguments)]
    pub fn create_image(
        path: &std::path::Path,
        shape: &[i32],
        data: &[f32],
        units: &str,
    ) -> Result<(), OracleError> {
        image_operation!("image.create_image", {
            cpp_create_image(path, shape, data, units).map_err(|message| OracleError::CppFailure {
                operation: "image.create_image",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_image_tiled(
        path: &std::path::Path,
        shape: &[i32],
        tile_shape: &[i32],
        data: &[f32],
        units: &str,
    ) -> Result<(), OracleError> {
        image_operation!("image.create_image_tiled", {
            cpp_create_image_tiled(path, shape, tile_shape, data, units).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.create_image_tiled",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_data(
        path: &std::path::Path,
        max_size: usize,
    ) -> Result<Vec<f32>, OracleError> {
        image_operation!("image.read_image_data", {
            cpp_read_image_data(path, max_size).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_data",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_image_f64(
        path: &std::path::Path,
        shape: &[i32],
        data: &[f64],
        units: &str,
    ) -> Result<(), OracleError> {
        image_operation!("image.create_image_f64", {
            cpp_create_image_f64(path, shape, data, units).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.create_image_f64",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_data_f64(
        path: &std::path::Path,
        max_size: usize,
    ) -> Result<Vec<f64>, OracleError> {
        image_operation!("image.read_image_data_f64", {
            cpp_read_image_data_f64(path, max_size).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_data_f64",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_image_complex32(
        path: &std::path::Path,
        shape: &[i32],
        data: &[Complex32],
        units: &str,
    ) -> Result<(), OracleError> {
        image_operation!("image.create_image_complex32", {
            cpp_create_image_complex32(path, shape, data, units).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.create_image_complex32",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_data_complex32(
        path: &std::path::Path,
        max_size: usize,
    ) -> Result<Vec<Complex32>, OracleError> {
        image_operation!("image.read_image_data_complex32", {
            cpp_read_image_data_complex32(path, max_size).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.read_image_data_complex32",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_image_complex64(
        path: &std::path::Path,
        shape: &[i32],
        data: &[Complex64],
        units: &str,
    ) -> Result<(), OracleError> {
        image_operation!("image.create_image_complex64", {
            cpp_create_image_complex64(path, shape, data, units).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.create_image_complex64",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_data_complex64(
        path: &std::path::Path,
        max_size: usize,
    ) -> Result<Vec<Complex64>, OracleError> {
        image_operation!("image.read_image_data_complex64", {
            cpp_read_image_data_complex64(path, max_size).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.read_image_data_complex64",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_shape(path: &std::path::Path) -> Result<Vec<i32>, OracleError> {
        image_operation!("image.read_image_shape", {
            cpp_read_image_shape(path).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_shape",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_units(path: &std::path::Path) -> Result<String, OracleError> {
        image_operation!("image.read_image_units", {
            cpp_read_image_units(path).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_units",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_temp_image_materialized(
        path: &std::path::Path,
        shape: &[i32],
        data: &[f32],
        units: &str,
        object_name: &str,
        image_type: &str,
    ) -> Result<(), OracleError> {
        image_operation!("image.create_temp_image_materialized", {
            cpp_create_temp_image_materialized(path, shape, data, units, object_name, image_type)
                .map_err(|message| OracleError::CppFailure {
                    operation: "image.create_temp_image_materialized",
                    message,
                })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_coordinate_count(path: &std::path::Path) -> Result<i32, OracleError> {
        image_operation!("image.read_image_coordinate_count", {
            cpp_read_image_coordinate_count(path).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_coordinate_count",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_default_mask_name(path: &std::path::Path) -> Result<String, OracleError> {
        image_operation!("image.read_image_default_mask_name", {
            cpp_read_image_default_mask_name(path).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_default_mask_name",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_default_mask(
        path: &std::path::Path,
        max_size: usize,
    ) -> Result<Vec<bool>, OracleError> {
        image_operation!("image.read_image_default_mask", {
            cpp_read_image_default_mask(path, max_size).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_default_mask",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_polygon_region(
        path: &std::path::Path,
        region_name: &str,
        x: &[f64],
        y: &[f64],
    ) -> Result<(), OracleError> {
        image_operation!("image.write_polygon_region", {
            cpp_write_polygon_region(path, region_name, x, y).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.write_polygon_region",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_union_region(
        path: &std::path::Path,
        region_name: &str,
        x1: &[f64],
        y1: &[f64],
        x2: &[f64],
        y2: &[f64],
    ) -> Result<(), OracleError> {
        image_operation!("image.write_union_region", {
            cpp_write_union_region(path, region_name, x1, y1, x2, y2).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.write_union_region",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_box_region(
        path: &std::path::Path,
        region_name: &str,
        x0: f64,
        y0: f64,
        x1: f64,
        y1: f64,
    ) -> Result<(), OracleError> {
        image_operation!("image.write_box_region", {
            cpp_write_box_region(path, region_name, x0, y0, x1, y1).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.write_box_region",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_region_class(
        path: &std::path::Path,
        region_name: &str,
    ) -> Result<String, OracleError> {
        image_operation!("image.read_region_class", {
            cpp_read_region_class(path, region_name).map_err(|message| OracleError::CppFailure {
                operation: "image.read_region_class",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_region_names(path: &std::path::Path) -> Result<Vec<String>, OracleError> {
        image_operation!("image.read_region_names", {
            cpp_read_region_names(path).map_err(|message| OracleError::CppFailure {
                operation: "image.read_region_names",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_unsupported_region(
        path: &std::path::Path,
        region_name: &str,
        kind: CppUnsupportedRegionKind,
    ) -> Result<(), OracleError> {
        image_operation!("image.write_unsupported_region", {
            cpp_write_unsupported_region(path, region_name, kind).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.write_unsupported_region",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_region_statistics(
        path: &std::path::Path,
        region_name: &str,
    ) -> Result<CppRegionStatistics, OracleError> {
        image_operation!("image.read_region_statistics", {
            cpp_read_region_statistics(path, region_name).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.read_region_statistics",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_default_mask(
        path: &std::path::Path,
        mask_name: &str,
        data: &[bool],
    ) -> Result<(), OracleError> {
        image_operation!("image.write_default_mask", {
            cpp_write_default_mask(path, mask_name, data).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.write_default_mask",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_info_object_name(path: &std::path::Path) -> Result<String, OracleError> {
        image_operation!("image.read_image_info_object_name", {
            cpp_read_image_info_object_name(path).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_info_object_name",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_info_type(path: &std::path::Path) -> Result<String, OracleError> {
        image_operation!("image.read_image_info_type", {
            cpp_read_image_info_type(path).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_info_type",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn read_image_slice(
        path: &std::path::Path,
        start: &[i32],
        length: &[i32],
    ) -> Result<Vec<f32>, OracleError> {
        image_operation!("image.read_image_slice", {
            cpp_read_image_slice(path, start, length).map_err(|message| OracleError::CppFailure {
                operation: "image.read_image_slice",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn eval_image_expr_unary(
        path: &std::path::Path,
        op: CppImageExprUnaryOp,
        max_size: usize,
    ) -> Result<Vec<f32>, OracleError> {
        image_operation!("image.eval_image_expr_unary", {
            cpp_eval_image_expr_unary(path, op, max_size).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.eval_image_expr_unary",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn eval_image_expr_binary(
        lhs_path: &std::path::Path,
        rhs_path: &std::path::Path,
        op: CppImageExprBinaryOp,
        max_size: usize,
    ) -> Result<Vec<f32>, OracleError> {
        image_operation!("image.eval_image_expr_binary", {
            cpp_eval_image_expr_binary(lhs_path, rhs_path, op, max_size).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.eval_image_expr_binary",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn eval_image_expr_scalar(
        path: &std::path::Path,
        scalar: f32,
        op: CppImageExprBinaryOp,
        max_size: usize,
    ) -> Result<Vec<f32>, OracleError> {
        image_operation!("image.eval_image_expr_scalar", {
            cpp_eval_image_expr_scalar(path, scalar, op, max_size).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.eval_image_expr_scalar",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn eval_image_mask_range(
        path: &std::path::Path,
        lower_cmp: CppImageExprCompareOp,
        lower: f32,
        logical_op: CppMaskLogicalOp,
        upper_cmp: CppImageExprCompareOp,
        upper: f32,
        max_size: usize,
    ) -> Result<Vec<bool>, OracleError> {
        image_operation!("image.eval_image_mask_range", {
            cpp_eval_image_mask_range(
                path, lower_cmp, lower, logical_op, upper_cmp, upper, max_size,
            )
            .map_err(|message| OracleError::CppFailure {
                operation: "image.eval_image_mask_range",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn eval_image_expr_closeout_slice(
        path: &std::path::Path,
        start: &[i32],
        length: &[i32],
    ) -> Result<Vec<f32>, OracleError> {
        image_operation!("image.eval_image_expr_closeout_slice", {
            cpp_eval_image_expr_closeout_slice(path, start, length).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.eval_image_expr_closeout_slice",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn eval_lel_expr(expr: &str, max_size: usize) -> Result<(Vec<f32>, Vec<i32>), OracleError> {
        image_operation!("image.eval_lel_expr", {
            cpp_eval_lel_expr(expr, max_size).map_err(|message| OracleError::CppFailure {
                operation: "image.eval_lel_expr",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn profile_lel_scalar_expr(expr: &str, passes: usize) -> Result<[f64; 3], OracleError> {
        image_operation!("image.profile_lel_scalar_expr", {
            cpp_profile_lel_scalar_expr(expr, passes).map_err(|message| OracleError::CppFailure {
                operation: "image.profile_lel_scalar_expr",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn eval_lel_expr_mask(
        expr: &str,
        max_size: usize,
    ) -> Result<(Vec<bool>, Vec<i32>), OracleError> {
        image_operation!("image.eval_lel_expr_mask", {
            cpp_eval_lel_expr_mask(expr, max_size).map_err(|message| OracleError::CppFailure {
                operation: "image.eval_lel_expr_mask",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn save_lel_expr_file(expr: &str, save_path: &std::path::Path) -> Result<(), OracleError> {
        image_operation!("image.save_lel_expr_file", {
            cpp_save_lel_expr_file(expr, save_path).map_err(|message| OracleError::CppFailure {
                operation: "image.save_lel_expr_file",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_lel_expr_file(
        path: &std::path::Path,
        max_size: usize,
    ) -> Result<(Vec<f32>, Vec<i32>), OracleError> {
        image_operation!("image.open_lel_expr_file", {
            cpp_open_lel_expr_file(path, max_size).map_err(|message| OracleError::CppFailure {
                operation: "image.open_lel_expr_file",
                message,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_image_plane_by_plane(
        path: &std::path::Path,
        shape: &[i32],
        tile: &[i32],
        max_cache_mib: i32,
    ) -> Result<(f64, f64, f64), OracleError> {
        image_operation!("image.bench_image_plane_by_plane", {
            cpp_bench_image_plane_by_plane(path, shape, tile, max_cache_mib).map_err(|message| {
                OracleError::CppFailure {
                    operation: "image.bench_image_plane_by_plane",
                    message,
                }
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_image_spectrum_by_spectrum(
        path: &std::path::Path,
        shape: &[i32],
        tile: &[i32],
        max_cache_mib: i32,
    ) -> Result<(f64, f64, f64), OracleError> {
        image_operation!("image.bench_image_spectrum_by_spectrum", {
            cpp_bench_image_spectrum_by_spectrum(path, shape, tile, max_cache_mib).map_err(
                |message| OracleError::CppFailure {
                    operation: "image.bench_image_spectrum_by_spectrum",
                    message,
                },
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bench_image_plane_by_plane_complex(
        path: &std::path::Path,
        shape: &[i32],
        tile: &[i32],
        max_cache_mib: i32,
    ) -> Result<(f64, f64, f64), OracleError> {
        image_operation!("image.bench_image_plane_by_plane_complex", {
            cpp_bench_image_plane_by_plane_complex(path, shape, tile, max_cache_mib).map_err(
                |message| OracleError::CppFailure {
                    operation: "image.bench_image_plane_by_plane_complex",
                    message,
                },
            )
        })
    }
}
