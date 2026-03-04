// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scaled column engines — virtual columns that apply a linear transform.
//!
//! This module provides a single Rust implementation ([`ScaledColumnEngine`])
//! that handles two distinct C++ casacore on-disk formats:
//!
//! ## ScaledArrayEngine (C++ `ScaledArrayEngine<VT, ST>`)
//!
//! Computes `virtual = stored * scale + offset` with real-valued scale/offset.
//! Stored and virtual arrays have identical shapes. Used for numeric (non-complex)
//! type pairs like `<Double, Int>` and `<Float, UChar>`.
//!
//! Keywords: `_ScaledArrayEngine_*` and `_BaseMappedArrayEngine_Name`.
//! DM type: `"ScaledArrayEngine<double  ,Int     >"`.
//!
//! ## ScaledComplexData (C++ `ScaledComplexData<VT, ST>`)
//!
//! Stores complex values with a prepended dimension of 2 for real/imaginary parts.
//! Scale and offset are complex-valued, applied per-component:
//! - `re_virtual = re_stored * scale.re + offset.re`
//! - `im_virtual = im_stored * scale.im + offset.im`
//!
//! Virtual shape `[N]` → stored shape `[2, N]` on disk.
//!
//! Keywords: `_ScaledComplexData_*` and `_BaseMappedArrayEngine_Name`.
//! DM type: `"ScaledComplexData<Complex ,Short   >"`.
//!
//! C++ needs two separate template classes because `ScaledArrayEngine<Complex, Short>`
//! fails to compile (template bug: `if (offset == 0)` compares complex with int).
//! Rust handles both in one struct via runtime dispatch on the [`ScaledVariant`].

use casacore_types::{
    Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, Axis, IxDyn};

use super::StorageError;
use super::table_control::{ColumnDescContents, PlainColumnEntry};
use super::virtual_engine::{VirtualColumnEngine, VirtualContext};

// -- Keyword name constants --------------------------------------------------

// Shared: stored column name.
const KW_STORED_COL: &str = "_BaseMappedArrayEngine_Name";

// ScaledArrayEngine keywords (real-valued scale/offset).
const KW_SA_FIXED_SCALE: &str = "_ScaledArrayEngine_FixedScale";
const KW_SA_SCALE: &str = "_ScaledArrayEngine_Scale";
const KW_SA_FIXED_OFFSET: &str = "_ScaledArrayEngine_FixedOffset";
const KW_SA_OFFSET: &str = "_ScaledArrayEngine_Offset";
const KW_SA_SCALE_NAME: &str = "_ScaledArrayEngine_ScaleName";
const KW_SA_OFFSET_NAME: &str = "_ScaledArrayEngine_OffsetName";

// ScaledComplexData keywords (complex-valued scale/offset).
const KW_SC_FIXED_SCALE: &str = "_ScaledComplexData_FixedScale";
const KW_SC_SCALE: &str = "_ScaledComplexData_Scale";
const KW_SC_FIXED_OFFSET: &str = "_ScaledComplexData_FixedOffset";
const KW_SC_OFFSET: &str = "_ScaledComplexData_Offset";
const KW_SC_SCALE_NAME: &str = "_ScaledComplexData_ScaleName";
const KW_SC_OFFSET_NAME: &str = "_ScaledComplexData_OffsetName";

// -- Variant enum ------------------------------------------------------------

/// Which C++ on-disk format this engine instance handles.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ScaledVariant {
    /// C++ `ScaledArrayEngine<VT, ST>`: 1-to-1 shape, real scale/offset.
    Array,
    /// C++ `ScaledComplexData<VT, ST>`: stored shape prepends dim 2, complex scale/offset.
    ComplexData,
}

// -- Engine struct -----------------------------------------------------------

/// Unified virtual column engine for scaled array and scaled complex data.
///
/// Handles both `ScaledArrayEngine` and `ScaledComplexData` C++ on-disk formats
/// via runtime dispatch on [`ScaledVariant`].
///
/// # C++ equivalents
///
/// - `ScaledArrayEngine<VT, ST>` in `casacore/tables/DataMan/ScaledArrayEngine.h`
/// - `ScaledComplexData<VT, ST>` in `casacore/tables/DataMan/ScaledComplexData.h`
#[derive(Debug)]
pub(crate) struct ScaledColumnEngine {
    pub variant: ScaledVariant,
}

impl VirtualColumnEngine for ScaledColumnEngine {
    fn type_name(&self) -> &str {
        match self.variant {
            ScaledVariant::Array => "ScaledArrayEngine",
            ScaledVariant::ComplexData => "ScaledComplexData",
        }
    }

    fn materialize(
        &self,
        ctx: &VirtualContext,
        bound_cols: &[(usize, &PlainColumnEntry)],
        rows: &mut [RecordValue],
    ) -> Result<(), StorageError> {
        match self.variant {
            ScaledVariant::Array => materialize_array(ctx, bound_cols, rows),
            ScaledVariant::ComplexData => materialize_complex_data(ctx, bound_cols, rows),
        }
    }
}

// -- ScaledArrayEngine materialization ---------------------------------------

/// Materialize columns for the `ScaledArrayEngine` variant.
fn materialize_array(
    ctx: &VirtualContext,
    bound_cols: &[(usize, &PlainColumnEntry)],
    rows: &mut [RecordValue],
) -> Result<(), StorageError> {
    for &(desc_idx, _pc) in bound_cols {
        let col_desc = &ctx.col_descs[desc_idx];
        let col_name = &col_desc.col_name;
        let kw = &col_desc.keywords;
        let target_type = col_desc.primitive_type;

        let stored_col_name = get_string_keyword(kw, KW_STORED_COL, col_name)?;

        // Scale configuration (real-valued).
        let fixed_scale = get_bool_keyword(kw, KW_SA_FIXED_SCALE).unwrap_or(true);
        let base_scale = if fixed_scale {
            get_f64_keyword(kw, KW_SA_SCALE).unwrap_or(1.0)
        } else {
            1.0
        };
        let scale_col_name = if !fixed_scale {
            get_string_keyword(kw, KW_SA_SCALE_NAME, col_name).ok()
        } else {
            None
        };

        // Offset configuration (real-valued).
        let fixed_offset = get_bool_keyword(kw, KW_SA_FIXED_OFFSET).unwrap_or(true);
        let base_offset = if fixed_offset {
            get_f64_keyword(kw, KW_SA_OFFSET).unwrap_or(0.0)
        } else {
            0.0
        };
        let offset_col_name = if !fixed_offset {
            get_string_keyword(kw, KW_SA_OFFSET_NAME, col_name).ok()
        } else {
            None
        };

        for (row_idx, row) in rows.iter_mut().enumerate() {
            let scale = if let Some(ref sc_name) = scale_col_name {
                get_scalar_f64_from_row(&ctx.rows[row_idx], sc_name).unwrap_or(base_scale)
            } else {
                base_scale
            };
            let offset = if let Some(ref off_name) = offset_col_name {
                get_scalar_f64_from_row(&ctx.rows[row_idx], off_name).unwrap_or(base_offset)
            } else {
                base_offset
            };

            let stored_value = ctx.rows[row_idx].get(&stored_col_name);
            let virtual_value = match stored_value {
                Some(v) => apply_real_scale_offset(v, scale, offset, target_type)?,
                None => {
                    return Err(StorageError::FormatMismatch(format!(
                        "ScaledArrayEngine: stored column '{stored_col_name}' \
                         not found in row {row_idx}"
                    )));
                }
            };
            row.push(RecordField::new(col_name.clone(), virtual_value));
        }
    }
    Ok(())
}

/// Apply real-valued `virtual = stored * scale + offset`, producing Float32 or Float64.
fn apply_real_scale_offset(
    stored: &Value,
    scale: f64,
    offset: f64,
    target_type: PrimitiveType,
) -> Result<Value, StorageError> {
    match stored {
        Value::Scalar(sv) => {
            let v = scalar_to_f64(sv)?;
            let scaled = v * scale + offset;
            let out = match target_type {
                PrimitiveType::Float32 => ScalarValue::Float32(scaled as f32),
                _ => ScalarValue::Float64(scaled),
            };
            Ok(Value::Scalar(out))
        }
        Value::Array(av) => {
            let arr = array_to_f64(av)?;
            let scaled = arr.mapv(|x| x * scale + offset);
            let out = match target_type {
                PrimitiveType::Float32 => {
                    casacore_types::ArrayValue::Float32(scaled.mapv(|x| x as f32))
                }
                _ => casacore_types::ArrayValue::Float64(scaled),
            };
            Ok(Value::Array(out))
        }
        Value::Record(_) => Err(StorageError::FormatMismatch(
            "ScaledArrayEngine: cannot scale a Record value".to_string(),
        )),
    }
}

// -- ScaledComplexData materialization ---------------------------------------

/// Materialize columns for the `ScaledComplexData` variant.
///
/// Stored arrays have shape `[2, ...]` where the first axis holds `[real, imag]`.
/// Scale and offset are complex-valued; scaling is applied per-component.
fn materialize_complex_data(
    ctx: &VirtualContext,
    bound_cols: &[(usize, &PlainColumnEntry)],
    rows: &mut [RecordValue],
) -> Result<(), StorageError> {
    for &(desc_idx, _pc) in bound_cols {
        let col_desc = &ctx.col_descs[desc_idx];
        let col_name = &col_desc.col_name;
        let kw = &col_desc.keywords;
        let target_type = col_desc.primitive_type;

        let stored_col_name = get_string_keyword(kw, KW_STORED_COL, col_name)?;

        // Scale configuration (complex-valued).
        let fixed_scale = get_bool_keyword(kw, KW_SC_FIXED_SCALE).unwrap_or(true);
        let base_scale = if fixed_scale {
            get_complex64_keyword(kw, KW_SC_SCALE).unwrap_or(Complex64::new(1.0, 1.0))
        } else {
            Complex64::new(1.0, 1.0)
        };
        let scale_col_name = if !fixed_scale {
            get_string_keyword(kw, KW_SC_SCALE_NAME, col_name).ok()
        } else {
            None
        };

        // Offset configuration (complex-valued).
        let fixed_offset = get_bool_keyword(kw, KW_SC_FIXED_OFFSET).unwrap_or(true);
        let base_offset = if fixed_offset {
            get_complex64_keyword(kw, KW_SC_OFFSET).unwrap_or(Complex64::new(0.0, 0.0))
        } else {
            Complex64::new(0.0, 0.0)
        };
        let offset_col_name = if !fixed_offset {
            get_string_keyword(kw, KW_SC_OFFSET_NAME, col_name).ok()
        } else {
            None
        };

        for (row_idx, row) in rows.iter_mut().enumerate() {
            let scale = if let Some(ref sc_name) = scale_col_name {
                get_complex64_from_row(&ctx.rows[row_idx], sc_name).unwrap_or(base_scale)
            } else {
                base_scale
            };
            let offset = if let Some(ref off_name) = offset_col_name {
                get_complex64_from_row(&ctx.rows[row_idx], off_name).unwrap_or(base_offset)
            } else {
                base_offset
            };

            let stored_value = ctx.rows[row_idx].get(&stored_col_name);
            let virtual_value = match stored_value {
                Some(v) => apply_complex_scale_offset(v, scale, offset, target_type)?,
                None => {
                    return Err(StorageError::FormatMismatch(format!(
                        "ScaledComplexData: stored column '{stored_col_name}' \
                         not found in row {row_idx}"
                    )));
                }
            };
            row.push(RecordField::new(col_name.clone(), virtual_value));
        }
    }
    Ok(())
}

/// Apply complex-valued scale/offset to a stored array with prepended dimension of 2.
///
/// Stored shape: `[2, d0, d1, ...]` → virtual shape: `[d0, d1, ...]`.
/// For each element pair `(re_stored, im_stored)`:
/// - `re_virtual = re_stored * scale.re + offset.re`
/// - `im_virtual = im_stored * scale.im + offset.im`
fn apply_complex_scale_offset(
    stored: &Value,
    scale: Complex64,
    offset: Complex64,
    target_type: PrimitiveType,
) -> Result<Value, StorageError> {
    let stored_arr = match stored {
        Value::Array(av) => array_to_f64(av)?,
        _ => {
            return Err(StorageError::FormatMismatch(
                "ScaledComplexData: expected array value".to_string(),
            ));
        }
    };

    // Stored shape must have first dimension == 2.
    let stored_shape = stored_arr.shape();
    if stored_shape.is_empty() || stored_shape[0] != 2 {
        return Err(StorageError::FormatMismatch(format!(
            "ScaledComplexData: stored array first dimension must be 2, got {:?}",
            stored_shape
        )));
    }

    // Virtual shape is the stored shape minus the first dimension.
    let virtual_shape: Vec<usize> = stored_shape[1..].to_vec();
    let n_elements: usize = virtual_shape.iter().product();

    // Split the stored [2, ...] array into real and imaginary views using
    // index_axis.  This correctly handles both C-order and Fortran-order
    // memory layouts — unlike flat iteration which depends on traversal order.
    let re_view = stored_arr.index_axis(Axis(0), 0); // shape [d0, d1, ...]
    let im_view = stored_arr.index_axis(Axis(0), 1); // shape [d0, d1, ...]

    match target_type {
        PrimitiveType::Complex32 => {
            let mut complex_data = Vec::with_capacity(n_elements);
            for (&re_stored, &im_stored) in re_view.iter().zip(im_view.iter()) {
                let re = re_stored * scale.re + offset.re;
                let im = im_stored * scale.im + offset.im;
                complex_data.push(Complex32::new(re as f32, im as f32));
            }
            let arr = ArrayD::from_shape_vec(IxDyn(&virtual_shape), complex_data).map_err(|e| {
                StorageError::FormatMismatch(format!("ScaledComplexData reshape: {e}"))
            })?;
            Ok(Value::Array(casacore_types::ArrayValue::Complex32(arr)))
        }
        PrimitiveType::Complex64 => {
            let mut complex_data = Vec::with_capacity(n_elements);
            for (&re_stored, &im_stored) in re_view.iter().zip(im_view.iter()) {
                let re = re_stored * scale.re + offset.re;
                let im = im_stored * scale.im + offset.im;
                complex_data.push(Complex64::new(re, im));
            }
            let arr = ArrayD::from_shape_vec(IxDyn(&virtual_shape), complex_data).map_err(|e| {
                StorageError::FormatMismatch(format!("ScaledComplexData reshape: {e}"))
            })?;
            Ok(Value::Array(casacore_types::ArrayValue::Complex64(arr)))
        }
        _ => Err(StorageError::FormatMismatch(format!(
            "ScaledComplexData: unsupported virtual type {target_type:?}, expected Complex32/Complex64"
        ))),
    }
}

// -- Shared helpers ----------------------------------------------------------

/// Convert a scalar value to f64.
fn scalar_to_f64(sv: &ScalarValue) -> Result<f64, StorageError> {
    match sv {
        ScalarValue::Int16(v) => Ok(*v as f64),
        ScalarValue::Int32(v) => Ok(*v as f64),
        ScalarValue::Int64(v) => Ok(*v as f64),
        ScalarValue::UInt8(v) => Ok(*v as f64),
        ScalarValue::UInt16(v) => Ok(*v as f64),
        ScalarValue::UInt32(v) => Ok(*v as f64),
        ScalarValue::Float32(v) => Ok(*v as f64),
        ScalarValue::Float64(v) => Ok(*v),
        other => Err(StorageError::FormatMismatch(format!(
            "ScaledColumnEngine: unsupported stored scalar type: {:?}",
            other
        ))),
    }
}

/// Convert an array value to `ArrayD<f64>`.
fn array_to_f64(av: &casacore_types::ArrayValue) -> Result<ArrayD<f64>, StorageError> {
    use casacore_types::ArrayValue;
    match av {
        ArrayValue::Int16(a) => Ok(a.mapv(|x| x as f64)),
        ArrayValue::Int32(a) => Ok(a.mapv(|x| x as f64)),
        ArrayValue::Int64(a) => Ok(a.mapv(|x| x as f64)),
        ArrayValue::UInt8(a) => Ok(a.mapv(|x| x as f64)),
        ArrayValue::UInt16(a) => Ok(a.mapv(|x| x as f64)),
        ArrayValue::UInt32(a) => Ok(a.mapv(|x| x as f64)),
        ArrayValue::Float32(a) => Ok(a.mapv(|x| x as f64)),
        ArrayValue::Float64(a) => Ok(a.clone()),
        other => Err(StorageError::FormatMismatch(format!(
            "ScaledColumnEngine: unsupported stored array type: {:?}",
            std::mem::discriminant(other)
        ))),
    }
}

/// Read a scalar f64 from a row by column name, promoting integer types.
fn get_scalar_f64_from_row(row: &RecordValue, col_name: &str) -> Option<f64> {
    match row.get(col_name)? {
        Value::Scalar(sv) => scalar_to_f64(sv).ok(),
        _ => None,
    }
}

/// Read a Complex64 scalar from a row by column name.
fn get_complex64_from_row(row: &RecordValue, col_name: &str) -> Option<Complex64> {
    match row.get(col_name)? {
        Value::Scalar(ScalarValue::Complex64(c)) => Some(*c),
        Value::Scalar(ScalarValue::Complex32(c)) => Some(Complex64::new(c.re as f64, c.im as f64)),
        _ => None,
    }
}

/// Extract a string keyword value.
fn get_string_keyword(kw: &RecordValue, key: &str, col_name: &str) -> Result<String, StorageError> {
    match kw.get(key) {
        Some(Value::Scalar(ScalarValue::String(s))) => Ok(s.clone()),
        Some(_) => Err(StorageError::FormatMismatch(format!(
            "column '{col_name}': keyword '{key}' is not a string"
        ))),
        None => Err(StorageError::FormatMismatch(format!(
            "column '{col_name}': missing keyword '{key}'"
        ))),
    }
}

/// Extract a bool keyword value.
fn get_bool_keyword(kw: &RecordValue, key: &str) -> Option<bool> {
    match kw.get(key)? {
        Value::Scalar(ScalarValue::Bool(b)) => Some(*b),
        _ => None,
    }
}

/// Extract an f64 keyword value.
fn get_f64_keyword(kw: &RecordValue, key: &str) -> Option<f64> {
    match kw.get(key)? {
        Value::Scalar(ScalarValue::Float64(v)) => Some(*v),
        Value::Scalar(ScalarValue::Float32(v)) => Some(*v as f64),
        _ => None,
    }
}

/// Extract a Complex64 keyword value.
fn get_complex64_keyword(kw: &RecordValue, key: &str) -> Option<Complex64> {
    match kw.get(key)? {
        Value::Scalar(ScalarValue::Complex64(c)) => Some(*c),
        Value::Scalar(ScalarValue::Complex32(c)) => Some(Complex64::new(c.re as f64, c.im as f64)),
        _ => None,
    }
}

/// Resolve the virtual column descriptor for a bound column.
#[allow(dead_code)]
pub(crate) fn virtual_col_desc<'a>(
    col_descs: &'a [ColumnDescContents],
    col_name: &str,
) -> Option<&'a ColumnDescContents> {
    col_descs.iter().find(|c| c.col_name == col_name)
}
