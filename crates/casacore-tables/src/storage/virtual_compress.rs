// SPDX-License-Identifier: LGPL-3.0-or-later
//! Lossy compression virtual column engines.
//!
//! This module provides virtual column engines that decompress stored integer
//! columns into floating-point or complex values using FITS-style linear
//! scaling.
//!
//! # CompressFloat
//!
//! Lossy Float→Short compression: `virtual[i] = stored * scale + offset`.
//! Short value -32768 is the NaN sentinel (mapped to `f64::NAN` on read).
//! Used in many archived radio astronomy datasets.
//!
//! On-disk keywords (on the virtual Float64 column):
//! - `_CompressFloat_Scale` (Float32)
//! - `_CompressFloat_Offset` (Float32)
//! - `_CompressFloat_ScaleName` (String) — per-row scale column name
//! - `_CompressFloat_OffsetName` (String) — per-row offset column name
//! - `_CompressFloat_Fixed` (Bool) — true if scale/offset are fixed
//! - `_CompressFloat_AutoScale` (Bool)
//! - `_BaseMappedArrayEngine_StoredColumnName` (String)
//!
//! DM type string: `"CompressFloat"`.
//!
//! # CompressComplex / CompressComplexSD
//!
//! Lossy Complex→Int compression for visibility data. Each stored Int
//! packs real (upper 16 bits) and imaginary (lower 16 bits) components.
//!
//! DM type strings: `"CompressComplex"`, `"CompressComplexSD"`.
//!
//! # C++ equivalents
//!
//! - `CompressFloat` in `casacore/tables/DataMan/CompressFloat.h`
//! - `CompressComplex` in `casacore/tables/DataMan/CompressComplex.h`

use casacore_types::{
    ArrayValue, Complex32, Complex64, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, IxDyn};

use super::StorageError;
use super::table_control::PlainColumnEntry;
use super::virtual_engine::{VirtualColumnEngine, VirtualContext};

// -- Keyword name constants --------------------------------------------------

// CompressFloat keywords.
const KW_CF_STORED_COL: &str = "_BaseMappedArrayEngine_StoredColumnName";
const KW_CF_SCALE: &str = "_CompressFloat_Scale";
const KW_CF_OFFSET: &str = "_CompressFloat_Offset";
const KW_CF_SCALE_NAME: &str = "_CompressFloat_ScaleName";
const KW_CF_OFFSET_NAME: &str = "_CompressFloat_OffsetName";
const KW_CF_FIXED: &str = "_CompressFloat_Fixed";

// CompressComplex keywords.
const KW_CC_STORED_COL: &str = "_BaseMappedArrayEngine_StoredColumnName";
const KW_CC_SCALE: &str = "_CompressComplex_Scale";
const KW_CC_OFFSET: &str = "_CompressComplex_Offset";
const KW_CC_SCALE_NAME: &str = "_CompressComplex_ScaleName";
const KW_CC_OFFSET_NAME: &str = "_CompressComplex_OffsetName";
const KW_CC_FIXED: &str = "_CompressComplex_Fixed";
const KW_CC_TYPE: &str = "_CompressComplex_Type";

/// NaN sentinel for CompressFloat (Short -32768).
const NAN_SENTINEL_SHORT: i16 = i16::MIN;

// ==========================================================================
// CompressFloat
// ==========================================================================

/// Virtual column engine for lossy Float→Short FITS-style compression.
///
/// Decompresses stored Int16 arrays to Float32/Float64 using:
/// `virtual[i] = (stored == -32768) ? NaN : stored * scale + offset`
///
/// # C++ equivalent
///
/// `CompressFloat` in `casacore/tables/DataMan/CompressFloat.h`.
#[derive(Debug)]
pub(crate) struct CompressFloatEngine;

impl VirtualColumnEngine for CompressFloatEngine {
    fn type_name(&self) -> &str {
        "CompressFloat"
    }

    fn materialize(
        &self,
        ctx: &VirtualContext,
        bound_cols: &[(usize, &PlainColumnEntry)],
        rows: &mut [RecordValue],
    ) -> Result<(), StorageError> {
        for &(desc_idx, _pc) in bound_cols {
            let col_desc = &ctx.col_descs[desc_idx];
            let col_name = &col_desc.col_name;
            let kw = &col_desc.keywords;
            let target_type = col_desc.require_primitive_type()?;

            let stored_col_name = get_string_keyword(kw, KW_CF_STORED_COL, col_name)?;

            // Scale/offset configuration.
            let fixed = get_bool_keyword(kw, KW_CF_FIXED).unwrap_or(true);
            let base_scale = get_f32_keyword(kw, KW_CF_SCALE).unwrap_or(1.0) as f64;
            let base_offset = get_f32_keyword(kw, KW_CF_OFFSET).unwrap_or(0.0) as f64;
            let scale_col_name = if !fixed {
                get_string_keyword(kw, KW_CF_SCALE_NAME, col_name).ok()
            } else {
                None
            };
            let offset_col_name = if !fixed {
                get_string_keyword(kw, KW_CF_OFFSET_NAME, col_name).ok()
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
                    Some(v) => decompress_float(v, scale, offset, target_type)?,
                    None => {
                        return Err(StorageError::FormatMismatch(format!(
                            "CompressFloat: stored column '{stored_col_name}' \
                             not found in row {row_idx}"
                        )));
                    }
                };
                row.push(RecordField::new(col_name.clone(), virtual_value));
            }
        }
        Ok(())
    }
}

/// Decompress stored Int16 values to Float using FITS-style linear scaling.
/// Short -32768 maps to NaN.
fn decompress_float(
    stored: &Value,
    scale: f64,
    offset: f64,
    target_type: casacore_types::PrimitiveType,
) -> Result<Value, StorageError> {
    match stored {
        Value::Scalar(ScalarValue::Int16(v)) => {
            let result = if *v == NAN_SENTINEL_SHORT {
                f64::NAN
            } else {
                *v as f64 * scale + offset
            };
            let out = match target_type {
                casacore_types::PrimitiveType::Float32 => ScalarValue::Float32(result as f32),
                _ => ScalarValue::Float64(result),
            };
            Ok(Value::Scalar(out))
        }
        Value::Array(ArrayValue::Int16(arr)) => {
            let decompressed = arr.mapv(|v| {
                if v == NAN_SENTINEL_SHORT {
                    f64::NAN
                } else {
                    v as f64 * scale + offset
                }
            });
            let out = match target_type {
                casacore_types::PrimitiveType::Float32 => {
                    ArrayValue::Float32(decompressed.mapv(|x| x as f32))
                }
                _ => ArrayValue::Float64(decompressed),
            };
            Ok(Value::Array(out))
        }
        _ => Err(StorageError::FormatMismatch(
            "CompressFloat: expected Int16 stored value".to_string(),
        )),
    }
}

// ==========================================================================
// CompressComplex / CompressComplexSD
// ==========================================================================

/// Which CompressComplex variant is in use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompressComplexVariant {
    /// Standard: upper 16 bits = real, lower 16 bits = imag.
    Standard,
    /// Single-dish: if LSB==0, imag is zero and real gets full 31-bit precision;
    /// if LSB==1, real gets 16 bits and imag gets 15 bits.
    SingleDish,
}

/// Virtual column engine for lossy Complex→Int compression.
///
/// # CompressComplex transform
///
/// Each stored Int packs real (upper 16 bits) and imaginary (lower 16 bits):
/// ```text
/// r  = stored_val / 65536
/// im = stored_val - r * 65536
/// virtual = Complex(r * scale + offset, im * scale + offset)
/// ```
/// If `r == -32768`, the result is `NaN + NaN*i`.
///
/// # CompressComplexSD transform (single-dish optimization)
///
/// ```text
/// If LSB == 0: imag is zero, real = (stored >> 1) with 31-bit precision
/// If LSB == 1: real = upper 16 bits, imag = middle 15 bits
/// ```
///
/// # C++ equivalents
///
/// - `CompressComplex` in `casacore/tables/DataMan/CompressComplex.h`
/// - `CompressComplexSD` in the same header.
#[derive(Debug)]
pub(crate) struct CompressComplexEngine {
    pub variant: CompressComplexVariant,
}

impl VirtualColumnEngine for CompressComplexEngine {
    fn type_name(&self) -> &str {
        match self.variant {
            CompressComplexVariant::Standard => "CompressComplex",
            CompressComplexVariant::SingleDish => "CompressComplexSD",
        }
    }

    fn materialize(
        &self,
        ctx: &VirtualContext,
        bound_cols: &[(usize, &PlainColumnEntry)],
        rows: &mut [RecordValue],
    ) -> Result<(), StorageError> {
        for &(desc_idx, _pc) in bound_cols {
            let col_desc = &ctx.col_descs[desc_idx];
            let col_name = &col_desc.col_name;
            let kw = &col_desc.keywords;
            let target_type = col_desc.require_primitive_type()?;

            let stored_col_name = get_string_keyword(kw, KW_CC_STORED_COL, col_name)?;

            let fixed = get_bool_keyword(kw, KW_CC_FIXED).unwrap_or(true);
            let base_scale = get_f32_keyword(kw, KW_CC_SCALE).unwrap_or(1.0) as f64;
            let base_offset = get_f32_keyword(kw, KW_CC_OFFSET).unwrap_or(0.0) as f64;
            let scale_col_name = if !fixed {
                get_string_keyword(kw, KW_CC_SCALE_NAME, col_name).ok()
            } else {
                None
            };
            let offset_col_name = if !fixed {
                get_string_keyword(kw, KW_CC_OFFSET_NAME, col_name).ok()
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
                    Some(v) => match self.variant {
                        CompressComplexVariant::Standard => {
                            decompress_complex(v, scale, offset, target_type)?
                        }
                        CompressComplexVariant::SingleDish => {
                            decompress_complex_sd(v, scale, offset, target_type)?
                        }
                    },
                    None => {
                        return Err(StorageError::FormatMismatch(format!(
                            "CompressComplex: stored column '{stored_col_name}' \
                             not found in row {row_idx}"
                        )));
                    }
                };
                row.push(RecordField::new(col_name.clone(), virtual_value));
            }
        }
        Ok(())
    }
}

/// Decompress standard CompressComplex: Int32 → Complex.
///
/// Upper 16 bits = real, lower 16 bits = imag.
/// `r = stored / 65536; im = stored - r * 65536`
/// NaN sentinel: r == -32768.
fn decompress_complex(
    stored: &Value,
    scale: f64,
    offset: f64,
    target_type: casacore_types::PrimitiveType,
) -> Result<Value, StorageError> {
    match stored {
        Value::Scalar(ScalarValue::Int32(v)) => {
            let c = unpack_complex_standard(*v, scale, offset);
            let out = to_complex_scalar(c, target_type);
            Ok(Value::Scalar(out))
        }
        Value::Array(ArrayValue::Int32(arr)) => {
            let out = unpack_complex_array_standard(arr, scale, offset, target_type)?;
            Ok(Value::Array(out))
        }
        _ => Err(StorageError::FormatMismatch(
            "CompressComplex: expected Int32 stored value".to_string(),
        )),
    }
}

/// Unpack a single Int32 to Complex64 using standard packing.
///
/// Uses i64 arithmetic to avoid overflow when stored == i32::MIN.
fn unpack_complex_standard(stored: i32, scale: f64, offset: f64) -> Complex64 {
    let s = stored as i64;
    let r = if s >= 0 {
        s / 65536
    } else {
        // C++ integer division truncates toward zero; this mirrors that for
        // negative values, giving the "floor division" result.
        (s - 65535) / 65536
    };
    let im = s - r * 65536;
    if r as i16 == NAN_SENTINEL_SHORT {
        Complex64::new(f64::NAN, f64::NAN)
    } else {
        Complex64::new(r as f64 * scale + offset, im as f64 * scale + offset)
    }
}

/// Unpack a standard CompressComplex Int32 array to complex array.
fn unpack_complex_array_standard(
    arr: &ArrayD<i32>,
    scale: f64,
    offset: f64,
    target_type: casacore_types::PrimitiveType,
) -> Result<ArrayValue, StorageError> {
    let complexes: Vec<Complex64> = arr
        .iter()
        .map(|&v| unpack_complex_standard(v, scale, offset))
        .collect();
    let shape = arr.shape().to_vec();
    match target_type {
        casacore_types::PrimitiveType::Complex32 => {
            let c32: Vec<Complex32> = complexes
                .iter()
                .map(|c| Complex32::new(c.re as f32, c.im as f32))
                .collect();
            let out = ArrayD::from_shape_vec(IxDyn(&shape), c32).map_err(|e| {
                StorageError::FormatMismatch(format!("CompressComplex reshape: {e}"))
            })?;
            Ok(ArrayValue::Complex32(out))
        }
        _ => {
            let out = ArrayD::from_shape_vec(IxDyn(&shape), complexes).map_err(|e| {
                StorageError::FormatMismatch(format!("CompressComplex reshape: {e}"))
            })?;
            Ok(ArrayValue::Complex64(out))
        }
    }
}

/// Decompress CompressComplexSD: Int32 → Complex (single-dish optimization).
///
/// If LSB == 0: imag is zero, real = stored >> 1 (31-bit precision).
/// If LSB == 1: real = upper 16 bits, imag = bits [1..16] (15-bit).
fn decompress_complex_sd(
    stored: &Value,
    scale: f64,
    offset: f64,
    target_type: casacore_types::PrimitiveType,
) -> Result<Value, StorageError> {
    match stored {
        Value::Scalar(ScalarValue::Int32(v)) => {
            let c = unpack_complex_sd(*v, scale, offset);
            let out = to_complex_scalar(c, target_type);
            Ok(Value::Scalar(out))
        }
        Value::Array(ArrayValue::Int32(arr)) => {
            let complexes: Vec<Complex64> = arr
                .iter()
                .map(|&v| unpack_complex_sd(v, scale, offset))
                .collect();
            let shape = arr.shape().to_vec();
            let out = match target_type {
                casacore_types::PrimitiveType::Complex32 => {
                    let c32: Vec<Complex32> = complexes
                        .iter()
                        .map(|c| Complex32::new(c.re as f32, c.im as f32))
                        .collect();
                    ArrayValue::Complex32(ArrayD::from_shape_vec(IxDyn(&shape), c32).map_err(
                        |e| StorageError::FormatMismatch(format!("CompressComplexSD reshape: {e}")),
                    )?)
                }
                _ => ArrayValue::Complex64(
                    ArrayD::from_shape_vec(IxDyn(&shape), complexes).map_err(|e| {
                        StorageError::FormatMismatch(format!("CompressComplexSD reshape: {e}"))
                    })?,
                ),
            };
            Ok(Value::Array(out))
        }
        _ => Err(StorageError::FormatMismatch(
            "CompressComplexSD: expected Int32 stored value".to_string(),
        )),
    }
}

/// Unpack a single Int32 to Complex64 using single-dish packing.
fn unpack_complex_sd(stored: i32, scale: f64, offset: f64) -> Complex64 {
    if stored & 1 == 0 {
        // LSB == 0: imag is zero, real gets full 31-bit precision.
        let real_raw = stored >> 1;
        Complex64::new(real_raw as f64 * scale + offset, 0.0)
    } else {
        // LSB == 1: real = upper 16 bits, imag = bits [1..16].
        let real_raw = stored >> 16;
        let imag_raw = (stored & 0xFFFE) >> 1; // bits [1..15], 15-bit signed
        // Sign-extend 15-bit value.
        let imag_raw = if imag_raw & 0x4000 != 0 {
            imag_raw | !0x7FFF
        } else {
            imag_raw
        };
        Complex64::new(
            real_raw as f64 * scale + offset,
            imag_raw as f64 * scale + offset,
        )
    }
}

/// Convert Complex64 to appropriate scalar value based on target type.
fn to_complex_scalar(c: Complex64, target_type: casacore_types::PrimitiveType) -> ScalarValue {
    match target_type {
        casacore_types::PrimitiveType::Complex32 => {
            ScalarValue::Complex32(Complex32::new(c.re as f32, c.im as f32))
        }
        _ => ScalarValue::Complex64(c),
    }
}

// -- Shared keyword helpers --------------------------------------------------

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

/// Extract an f32 keyword value (CompressFloat stores scale/offset as Float32).
fn get_f32_keyword(kw: &RecordValue, key: &str) -> Option<f32> {
    match kw.get(key)? {
        Value::Scalar(ScalarValue::Float32(v)) => Some(*v),
        Value::Scalar(ScalarValue::Float64(v)) => Some(*v as f32),
        _ => None,
    }
}

/// Read a scalar f64 from a row by column name, promoting numeric types.
fn get_scalar_f64_from_row(row: &RecordValue, col_name: &str) -> Option<f64> {
    match row.get(col_name)? {
        Value::Scalar(ScalarValue::Float32(v)) => Some(*v as f64),
        Value::Scalar(ScalarValue::Float64(v)) => Some(*v),
        Value::Scalar(ScalarValue::Int16(v)) => Some(*v as f64),
        Value::Scalar(ScalarValue::Int32(v)) => Some(*v as f64),
        _ => None,
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use casacore_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    fn make_context<'a>(
        col_descs: &'a [super::super::table_control::ColumnDescContents],
        rows: &'a [RecordValue],
        nrrow: usize,
    ) -> VirtualContext<'a> {
        VirtualContext {
            col_descs,
            rows,
            table_path: std::path::Path::new("/tmp/test_compress"),
            nrrow,
        }
    }

    fn make_col_desc(
        col_name: &str,
        primitive_type: PrimitiveType,
        keywords: RecordValue,
        dm_type: &str,
    ) -> super::super::table_control::ColumnDescContents {
        use super::super::data_type::CasacoreDataType;
        super::super::table_control::ColumnDescContents {
            class_name: String::new(),
            col_name: col_name.to_string(),
            comment: String::new(),
            data_manager_type: dm_type.to_string(),
            data_manager_group: dm_type.to_string(),
            data_type: CasacoreDataType::TpFloat,
            option: 0,
            nrdim: -1,
            shape: Vec::new(),
            max_length: 0,
            keywords,
            is_array: true,
            primitive_type: Some(primitive_type),
        }
    }

    fn make_plain_col(seq: u32) -> super::super::table_control::PlainColumnEntry {
        super::super::table_control::PlainColumnEntry {
            original_name: String::new(),
            dm_seq_nr: seq,
            is_array: true,
        }
    }

    // -- CompressFloat tests -------------------------------------------------

    #[test]
    fn test_compress_float_fixed_scale() {
        // scale=0.01, offset=100.0
        // stored=[100, -100, 0, 32767, -32768(NaN)]
        // expected=[101.0, 99.0, 100.0, 427.67, NaN]
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CF_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("DATA_STORED".to_string())),
        ));
        kw.push(RecordField::new(
            KW_CF_SCALE.to_string(),
            Value::Scalar(ScalarValue::Float32(0.01)),
        ));
        kw.push(RecordField::new(
            KW_CF_OFFSET.to_string(),
            Value::Scalar(ScalarValue::Float32(100.0)),
        ));
        kw.push(RecordField::new(
            KW_CF_FIXED.to_string(),
            Value::Scalar(ScalarValue::Bool(true)),
        ));

        let col_desc = make_col_desc("DATA", PrimitiveType::Float64, kw, "CompressFloat");
        let pc = make_plain_col(1);

        let stored_arr =
            ArrayD::from_shape_vec(ndarray::IxDyn(&[5]), vec![100i16, -100, 0, 32767, -32768])
                .unwrap();
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "DATA_STORED".to_string(),
            Value::Array(ArrayValue::Int16(stored_arr)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 1);
        CompressFloatEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        match rows[0].get("DATA").unwrap() {
            Value::Array(ArrayValue::Float64(arr)) => {
                let vals = arr.as_slice().unwrap();
                assert!((vals[0] - 101.0).abs() < 1e-6);
                assert!((vals[1] - 99.0).abs() < 1e-6);
                assert!((vals[2] - 100.0).abs() < 1e-6);
                assert!((vals[3] - 427.67).abs() < 0.01);
                assert!(vals[4].is_nan());
            }
            other => panic!("expected Float64 array, got {other:?}"),
        }
    }

    #[test]
    fn test_compress_float_nan_sentinel() {
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CF_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("S".to_string())),
        ));

        let col_desc = make_col_desc("V", PrimitiveType::Float64, kw, "CompressFloat");
        let pc = make_plain_col(1);

        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "S".to_string(),
            Value::Scalar(ScalarValue::Int16(-32768)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 1);
        CompressFloatEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        match rows[0].get("V").unwrap() {
            Value::Scalar(ScalarValue::Float64(v)) => assert!(v.is_nan()),
            other => panic!("expected Float64 scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_compress_float_per_row_scale() {
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CF_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("S".to_string())),
        ));
        kw.push(RecordField::new(
            KW_CF_FIXED.to_string(),
            Value::Scalar(ScalarValue::Bool(false)),
        ));
        kw.push(RecordField::new(
            KW_CF_SCALE_NAME.to_string(),
            Value::Scalar(ScalarValue::String("SCALE_COL".to_string())),
        ));
        kw.push(RecordField::new(
            KW_CF_OFFSET_NAME.to_string(),
            Value::Scalar(ScalarValue::String("OFFSET_COL".to_string())),
        ));

        let col_desc = make_col_desc("V", PrimitiveType::Float64, kw, "CompressFloat");
        let pc = make_plain_col(1);

        // Row 0: scale=2.0, offset=10.0, stored=5 → 2*5+10=20
        // Row 1: scale=0.5, offset=0.0, stored=10 → 0.5*10+0=5
        let stored_rows: Vec<RecordValue> = vec![
            {
                let mut r = RecordValue::default();
                r.push(RecordField::new(
                    "S".to_string(),
                    Value::Scalar(ScalarValue::Int16(5)),
                ));
                r.push(RecordField::new(
                    "SCALE_COL".to_string(),
                    Value::Scalar(ScalarValue::Float32(2.0)),
                ));
                r.push(RecordField::new(
                    "OFFSET_COL".to_string(),
                    Value::Scalar(ScalarValue::Float32(10.0)),
                ));
                r
            },
            {
                let mut r = RecordValue::default();
                r.push(RecordField::new(
                    "S".to_string(),
                    Value::Scalar(ScalarValue::Int16(10)),
                ));
                r.push(RecordField::new(
                    "SCALE_COL".to_string(),
                    Value::Scalar(ScalarValue::Float32(0.5)),
                ));
                r.push(RecordField::new(
                    "OFFSET_COL".to_string(),
                    Value::Scalar(ScalarValue::Float32(0.0)),
                ));
                r
            },
        ];

        let mut rows: Vec<RecordValue> = vec![RecordValue::default(), RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 2);
        CompressFloatEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        match rows[0].get("V").unwrap() {
            Value::Scalar(ScalarValue::Float64(v)) => assert!((v - 20.0).abs() < 1e-6),
            other => panic!("row 0: expected Float64, got {other:?}"),
        }
        match rows[1].get("V").unwrap() {
            Value::Scalar(ScalarValue::Float64(v)) => assert!((v - 5.0).abs() < 1e-6),
            other => panic!("row 1: expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn test_compress_float_to_float32() {
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CF_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("S".to_string())),
        ));
        kw.push(RecordField::new(
            KW_CF_SCALE.to_string(),
            Value::Scalar(ScalarValue::Float32(1.0)),
        ));

        let col_desc = make_col_desc("V", PrimitiveType::Float32, kw, "CompressFloat");
        let pc = make_plain_col(1);

        let stored_arr = ArrayD::from_shape_vec(ndarray::IxDyn(&[2]), vec![100i16, 200]).unwrap();
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "S".to_string(),
            Value::Array(ArrayValue::Int16(stored_arr)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 1);
        CompressFloatEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        match rows[0].get("V").unwrap() {
            Value::Array(ArrayValue::Float32(arr)) => {
                assert_eq!(arr.as_slice().unwrap(), &[100.0f32, 200.0]);
            }
            other => panic!("expected Float32 array, got {other:?}"),
        }
    }

    // -- CompressComplex tests -----------------------------------------------

    #[test]
    fn test_compress_complex_standard() {
        // stored = 1 * 65536 + 2 = 65538, scale=1.0, offset=0.0
        // → Complex(1.0, 2.0)
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CC_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("S".to_string())),
        ));
        kw.push(RecordField::new(
            KW_CC_SCALE.to_string(),
            Value::Scalar(ScalarValue::Float32(1.0)),
        ));
        kw.push(RecordField::new(
            KW_CC_OFFSET.to_string(),
            Value::Scalar(ScalarValue::Float32(0.0)),
        ));

        let col_desc = make_col_desc("VIS", PrimitiveType::Complex64, kw, "CompressComplex");
        let pc = make_plain_col(1);

        // Packed value: real=1, imag=2 → 1*65536 + 2
        let stored_val = 65538;
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "S".to_string(),
            Value::Scalar(ScalarValue::Int32(stored_val)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 1);
        let engine = CompressComplexEngine {
            variant: CompressComplexVariant::Standard,
        };
        engine.materialize(&ctx, &[(0, &pc)], &mut rows).unwrap();

        match rows[0].get("VIS").unwrap() {
            Value::Scalar(ScalarValue::Complex64(c)) => {
                assert!((c.re - 1.0).abs() < 1e-6);
                assert!((c.im - 2.0).abs() < 1e-6);
            }
            other => panic!("expected Complex64 scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_compress_complex_nan_sentinel() {
        // stored with r == -32768 should produce NaN
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CC_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("S".to_string())),
        ));

        let col_desc = make_col_desc("VIS", PrimitiveType::Complex64, kw, "CompressComplex");
        let pc = make_plain_col(1);

        // Construct stored value where r = -32768.
        // -32768 * 65536 overflows i32, so use i32::MIN which is -2147483648.
        // r = (-2147483648 - 65535) / 65536 = -2147549183 / 65536 = -32769 for negative path...
        // Actually, let's just use the wrapping arithmetic: i32::MIN = -32768 * 65536 in two's complement.
        let stored_val = i32::MIN; // -2147483648, which is -32768 * 65536 in two's complement
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "S".to_string(),
            Value::Scalar(ScalarValue::Int32(stored_val)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 1);
        let engine = CompressComplexEngine {
            variant: CompressComplexVariant::Standard,
        };
        engine.materialize(&ctx, &[(0, &pc)], &mut rows).unwrap();

        match rows[0].get("VIS").unwrap() {
            Value::Scalar(ScalarValue::Complex64(c)) => {
                assert!(c.re.is_nan());
                assert!(c.im.is_nan());
            }
            other => panic!("expected Complex64 NaN, got {other:?}"),
        }
    }

    #[test]
    fn test_compress_complex_sd_zero_imag() {
        // LSB == 0: imag is zero, real = stored >> 1
        // stored = 200 (even, so LSB=0), real_raw = 100
        // scale=1.0, offset=0.0 → Complex(100.0, 0.0)
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CC_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("S".to_string())),
        ));
        kw.push(RecordField::new(
            KW_CC_SCALE.to_string(),
            Value::Scalar(ScalarValue::Float32(1.0)),
        ));
        kw.push(RecordField::new(
            KW_CC_OFFSET.to_string(),
            Value::Scalar(ScalarValue::Float32(0.0)),
        ));

        let col_desc = make_col_desc("VIS", PrimitiveType::Complex64, kw, "CompressComplexSD");
        let pc = make_plain_col(1);

        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "S".to_string(),
            Value::Scalar(ScalarValue::Int32(200)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 1);
        let engine = CompressComplexEngine {
            variant: CompressComplexVariant::SingleDish,
        };
        engine.materialize(&ctx, &[(0, &pc)], &mut rows).unwrap();

        match rows[0].get("VIS").unwrap() {
            Value::Scalar(ScalarValue::Complex64(c)) => {
                assert!((c.re - 100.0).abs() < 1e-6);
                assert!(c.im.abs() < 1e-6);
            }
            other => panic!("expected Complex64, got {other:?}"),
        }
    }

    #[test]
    fn test_compress_complex_sd_with_imag() {
        // LSB == 1: real = upper 16 bits, imag = bits [1..16] (15-bit signed)
        // Let's construct: real=3, imag=5
        // stored = (3 << 16) | (5 << 1) | 1 = 196608 + 10 + 1 = 196619
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CC_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("S".to_string())),
        ));
        kw.push(RecordField::new(
            KW_CC_SCALE.to_string(),
            Value::Scalar(ScalarValue::Float32(1.0)),
        ));
        kw.push(RecordField::new(
            KW_CC_OFFSET.to_string(),
            Value::Scalar(ScalarValue::Float32(0.0)),
        ));

        let col_desc = make_col_desc("VIS", PrimitiveType::Complex64, kw, "CompressComplexSD");
        let pc = make_plain_col(1);

        let stored = (3 << 16) | (5 << 1) | 1;
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "S".to_string(),
            Value::Scalar(ScalarValue::Int32(stored)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 1);
        let engine = CompressComplexEngine {
            variant: CompressComplexVariant::SingleDish,
        };
        engine.materialize(&ctx, &[(0, &pc)], &mut rows).unwrap();

        match rows[0].get("VIS").unwrap() {
            Value::Scalar(ScalarValue::Complex64(c)) => {
                assert!((c.re - 3.0).abs() < 1e-6, "re={}", c.re);
                assert!((c.im - 5.0).abs() < 1e-6, "im={}", c.im);
            }
            other => panic!("expected Complex64, got {other:?}"),
        }
    }

    #[test]
    fn test_compress_complex_array() {
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_CC_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("S".to_string())),
        ));
        kw.push(RecordField::new(
            KW_CC_SCALE.to_string(),
            Value::Scalar(ScalarValue::Float32(0.5)),
        ));
        kw.push(RecordField::new(
            KW_CC_OFFSET.to_string(),
            Value::Scalar(ScalarValue::Float32(0.0)),
        ));

        let col_desc = make_col_desc("VIS", PrimitiveType::Complex64, kw, "CompressComplex");
        let pc = make_plain_col(1);

        // Two values: (2, 4) and (6, 8)
        let v1 = 2 * 65536 + 4;
        let v2 = 6 * 65536 + 8;
        let stored_arr = ArrayD::from_shape_vec(ndarray::IxDyn(&[2]), vec![v1, v2]).unwrap();
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "S".to_string(),
            Value::Array(ArrayValue::Int32(stored_arr)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc];
        let ctx = make_context(&descs, &stored_rows, 1);
        let engine = CompressComplexEngine {
            variant: CompressComplexVariant::Standard,
        };
        engine.materialize(&ctx, &[(0, &pc)], &mut rows).unwrap();

        match rows[0].get("VIS").unwrap() {
            Value::Array(ArrayValue::Complex64(arr)) => {
                let vals = arr.as_slice().unwrap();
                assert!((vals[0].re - 1.0).abs() < 1e-6); // 2 * 0.5
                assert!((vals[0].im - 2.0).abs() < 1e-6); // 4 * 0.5
                assert!((vals[1].re - 3.0).abs() < 1e-6); // 6 * 0.5
                assert!((vals[1].im - 4.0).abs() < 1e-6); // 8 * 0.5
            }
            other => panic!("expected Complex64 array, got {other:?}"),
        }
    }
}
