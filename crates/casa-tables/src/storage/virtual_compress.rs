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

use casa_types::{ArrayValue, Complex32, Complex64, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis, IxDyn, Slice};
use std::ops::Range;

use super::StorageError;
use super::table_control::PlainColumnEntry;
use super::virtual_engine::{VirtualColumnEngine, VirtualContext};

// -- Keyword name constants --------------------------------------------------

// CASA persists the base engine's source column as Name. Older casa-rs
// tables used StoredColumnName for the same link.
const KW_MAPPED_SOURCE: &str = "_BaseMappedArrayEngine_Name";

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
const KW_CC_AUTO_SCALE: &str = "_CompressComplex_AutoScale";
const KW_CC_TYPE: &str = "_CompressComplex_Type";

pub(crate) fn automatic_complex_keywords(stored: &str, scale: &str, offset: &str) -> RecordValue {
    let mut keywords = RecordValue::default();
    for (key, name) in [
        (KW_MAPPED_SOURCE, stored),
        (KW_CC_SCALE_NAME, scale),
        (KW_CC_OFFSET_NAME, offset),
        (KW_CC_TYPE, "CompressComplex"),
    ] {
        keywords.upsert(key, Value::Scalar(ScalarValue::String(name.to_owned())));
    }
    for (key, flag) in [(KW_CC_FIXED, false), (KW_CC_AUTO_SCALE, true)] {
        keywords.upsert(key, Value::Scalar(ScalarValue::Bool(flag)));
    }
    for key in [KW_CC_SCALE, KW_CC_OFFSET] {
        keywords.upsert(key, Value::Scalar(ScalarValue::Float32(0.)));
    }
    keywords
}

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

            let stored_col_name = get_string_keyword(kw, KW_MAPPED_SOURCE, col_name)
                .or_else(|_| get_string_keyword(kw, KW_CF_STORED_COL, col_name))?;

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
    target_type: casa_types::PrimitiveType,
) -> Result<Value, StorageError> {
    match stored {
        Value::Scalar(ScalarValue::Int16(v)) => {
            let result = if *v == NAN_SENTINEL_SHORT {
                f64::NAN
            } else {
                *v as f64 * scale + offset
            };
            let out = match target_type {
                casa_types::PrimitiveType::Float32 => ScalarValue::Float32(result as f32),
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
                casa_types::PrimitiveType::Float32 => {
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

/// The physical columns and scale policy used for a stored complex array.
#[derive(Debug, Clone)]
pub struct ComplexCompression {
    /// The physical Int32 array column.
    pub stored: String,
    /// The origin of scale and offset for each array cell.
    pub scale: ComplexScale,
    variant: CompressComplexVariant,
    casa_layout: bool,
}

/// Fixed or per-row scale and offset for a CASA complex array.
#[derive(Debug, Clone)]
pub enum ComplexScale {
    /// One scale and offset held in the column keywords.
    Fixed {
        /// Scale of one integer step.
        scale: f32,
        /// Floating-point value of a zero integer.
        offset: f32,
    },
    /// Scalar columns hold scale and offset for each row.
    PerRow {
        /// Scale column name.
        scale: String,
        /// Offset column name.
        offset: String,
        /// Whether writes choose a new scale when needed.
        automatic: bool,
    },
}

impl ComplexCompression {
    /// CASA's automatically scaled CompressComplex layout for a new complex
    /// array column, with separate packed, scale and offset columns.
    pub fn automatic_standard(column: &str) -> Self {
        Self {
            stored: format!("{column}_COMPRESSED"),
            scale: ComplexScale::PerRow {
                scale: format!("{column}_SCALE"),
                offset: format!("{column}_OFFSET"),
                automatic: true,
            },
            variant: CompressComplexVariant::Standard,
            casa_layout: true,
        }
    }

    pub(crate) fn read(
        keywords: &RecordValue,
        column: &str,
        manager: &str,
    ) -> Result<Self, StorageError> {
        let stored = get_string_keyword(keywords, KW_MAPPED_SOURCE, column)
            .or_else(|_| get_string_keyword(keywords, KW_CC_STORED_COL, column))?;
        let scale = if get_bool_keyword(keywords, KW_CC_FIXED).unwrap_or(true) {
            ComplexScale::Fixed {
                scale: get_f32_keyword(keywords, KW_CC_SCALE).unwrap_or(1.0),
                offset: get_f32_keyword(keywords, KW_CC_OFFSET).unwrap_or(0.0),
            }
        } else {
            ComplexScale::PerRow {
                scale: get_string_keyword(keywords, KW_CC_SCALE_NAME, column)?,
                offset: get_string_keyword(keywords, KW_CC_OFFSET_NAME, column)?,
                automatic: get_bool_keyword(keywords, KW_CC_AUTO_SCALE).unwrap_or(false),
            }
        };
        let variant = if manager == "CompressComplexSD" {
            CompressComplexVariant::SingleDish
        } else {
            CompressComplexVariant::Standard
        };
        Ok(Self {
            stored,
            scale,
            variant,
            casa_layout: keywords.get(KW_MAPPED_SOURCE).is_some(),
        })
    }

    pub(crate) fn decode_cell(
        &self,
        value: ArrayValue,
        scale: f32,
        offset: f32,
        target_type: casa_types::PrimitiveType,
    ) -> Result<ArrayValue, StorageError> {
        if self.casa_layout
            && let ArrayValue::Int32(array) = &value
        {
            return Ok(decode_casa_complex(
                array,
                self.variant,
                scale,
                offset,
                target_type,
            ));
        }
        let value = Value::Array(value);
        let decoded = match self.variant {
            CompressComplexVariant::Standard => {
                decompress_complex(&value, scale.into(), offset.into(), target_type)
            }
            CompressComplexVariant::SingleDish => {
                decompress_complex_sd(&value, scale.into(), offset.into(), target_type)
            }
        }?;
        match decoded {
            Value::Array(array) => Ok(array),
            _ => Err(StorageError::FormatMismatch(
                "compressed array decoded as a scalar".into(),
            )),
        }
    }

    /// Whether a write may change the per-row scale and offset columns.
    pub fn is_automatic(&self) -> bool {
        matches!(
            self.scale,
            ComplexScale::PerRow {
                automatic: true,
                ..
            }
        )
    }

    /// Verify that writes would use the same on-disk codec as reads.
    pub fn check_writable(&self) -> Result<(), crate::TableError> {
        if self.casa_layout {
            return Ok(());
        }
        Err(crate::TableError::Storage(
            "legacy compressed complex columns are read-only".into(),
        ))
    }

    /// Pack a complete visibility cell, or replace its selected channels.
    /// For a partial write, `values` holds the updated full cell and `packed`
    /// holds its previous encoding. An unchanged scale preserves every
    /// packed sample outside the selected channels.
    pub fn encode_visibility_cell(
        &self,
        values: &ArrayD<Complex32>,
        packed: Option<ArrayD<i32>>,
        previous: (f32, f32),
        channels: Option<Range<usize>>,
    ) -> Result<(ArrayD<i32>, (f32, f32)), crate::TableError> {
        self.check_writable()?;
        if values.ndim() != 2
            || channels
                .as_ref()
                .is_some_and(|range| range.start > range.end || range.end > values.shape()[1])
        {
            return Err(crate::TableError::Storage(
                "invalid compressed visibility channel slice".into(),
            ));
        }
        let ranges = channels.filter(|range| range.start != 0 || range.end != values.shape()[1]);
        let Some(channels) = ranges else {
            let scale = if self.is_automatic() {
                scale_offset(self.range(values.iter()))
            } else {
                previous
            };
            return Ok((values.mapv(|value| self.pack_value(value, scale)), scale));
        };
        let mut packed = packed
            .filter(|encoded| encoded.shape() == values.shape())
            .ok_or_else(|| {
                crate::TableError::Storage(
                    "a partial compressed write needs the original cell".into(),
                )
            })?;
        let slice = values.slice_axis(Axis(1), Slice::from(channels.clone()));
        let bounds = self.range(slice.iter());
        let (scale, offset) = previous;
        let lower = offset - scale * 65_534.0 / 2.0;
        let upper = offset + scale * 65_534.0 / 2.0;
        if self.is_automatic() && scale == 0.0 {
            if bounds.is_none() {
                return Ok((packed, previous));
            }
            let scale = scale_offset(bounds);
            packed.fill(i32::MIN);
            packed
                .slice_axis_mut(Axis(1), Slice::from(channels))
                .assign(&slice.mapv(|value| self.pack_value(value, scale)));
            return Ok((packed, scale));
        }
        if let Some((min, max)) =
            bounds.filter(|&(min, max)| self.is_automatic() && (min < lower || max > upper))
        {
            let scale = scale_offset(Some((lower.min(min), upper.max(max))));
            return Ok((values.mapv(|value| self.pack_value(value, scale)), scale));
        }
        packed
            .slice_axis_mut(Axis(1), Slice::from(channels))
            .assign(&slice.mapv(|value| self.pack_value(value, previous)));
        Ok((packed, previous))
    }

    /// Pack a visibility cell for a new row. A row-specific codec needs
    /// initialized scale and offset values even when later writes use them
    /// unchanged.
    pub fn encode_new_visibility_cell(
        &self,
        values: &ArrayD<Complex32>,
    ) -> Result<(ArrayD<i32>, (f32, f32)), crate::TableError> {
        let scale = match &self.scale {
            ComplexScale::Fixed { scale, offset } => (*scale, *offset),
            ComplexScale::PerRow {
                automatic: true, ..
            } => (0.0, 0.0),
            ComplexScale::PerRow {
                automatic: false, ..
            } => scale_offset(self.range(values.iter())),
        };
        self.encode_visibility_cell(values, None, scale, None)
    }

    fn range<'a>(&self, values: impl Iterator<Item = &'a Complex32>) -> Option<(f32, f32)> {
        values
            .filter(|value| value.re.is_finite() && value.im.is_finite())
            .map(|value| {
                let imag = if matches!(self.variant, CompressComplexVariant::SingleDish)
                    && value.im == 0.0
                {
                    value.re
                } else {
                    value.im
                };
                (value.re.min(imag), value.re.max(imag))
            })
            .reduce(|(min, max), (other_min, other_max)| (min.min(other_min), max.max(other_max)))
    }

    fn pack_value(&self, value: Complex32, (scale, offset): (f32, f32)) -> i32 {
        if !value.re.is_finite() || !value.im.is_finite() {
            return i32::MIN;
        }
        let real = quantize(value.re, offset, scale, -32_767, 32_767);
        match self.variant {
            CompressComplexVariant::Standard => {
                real * 65_536 + quantize(value.im, offset, scale, -32_767, 32_767)
            }
            CompressComplexVariant::SingleDish if value.im == 0.0 => {
                quantize(
                    value.re,
                    offset,
                    scale / 32_768.0,
                    -(1 << 30),
                    (1 << 30) - 1,
                ) << 1
            }
            CompressComplexVariant::SingleDish => {
                real * 65_536 + 2 * quantize(value.im, offset, scale * 2.0, -16_384, 16_383) + 1
            }
        }
    }
}

fn decode_casa_complex(
    stored: &ArrayD<i32>,
    variant: CompressComplexVariant,
    scale: f32,
    offset: f32,
    target_type: casa_types::PrimitiveType,
) -> ArrayValue {
    let decoded = stored.mapv(|packed| {
        if matches!(variant, CompressComplexVariant::SingleDish) && packed & 1 == 0 {
            return Complex32::new((packed >> 1) as f32 * (scale / 32_768.0) + offset, 0.0);
        }
        let mut real = packed / 65_536;
        if real == -32_768 {
            return Complex32::new(f32::NAN, f32::NAN);
        }
        let mut imag = packed - real * 65_536;
        if imag < -32_768 {
            real -= 1;
            imag += 65_536;
        } else if imag >= 32_768 {
            real += 1;
            imag -= 65_536;
        }
        let imag = match variant {
            CompressComplexVariant::Standard => imag as f32 * scale + offset,
            CompressComplexVariant::SingleDish => (imag >> 1) as f32 * (scale * 2.0) + offset,
        };
        Complex32::new(real as f32 * scale + offset, imag)
    });
    match target_type {
        casa_types::PrimitiveType::Complex64 => ArrayValue::Complex64(
            decoded.mapv(|value| Complex64::new(value.re.into(), value.im.into())),
        ),
        _ => ArrayValue::Complex32(decoded),
    }
}

fn scale_offset(bounds: Option<(f32, f32)>) -> (f32, f32) {
    let Some((min, max)) = bounds else {
        return (0.0, 0.0);
    };
    let scale = if min == max {
        1.0
    } else {
        (max - min) / 65_534.0
    };
    (scale, (min + max) / 2.0)
}

fn quantize(value: f32, offset: f32, scale: f32, lower: i32, upper: i32) -> i32 {
    (((value - offset) / scale).round() as i32).clamp(lower, upper)
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

            let stored_col_name = get_string_keyword(kw, KW_MAPPED_SOURCE, col_name)
                .or_else(|_| get_string_keyword(kw, KW_CC_STORED_COL, col_name))?;

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
                    Some(Value::Array(ArrayValue::Int32(array)))
                        if kw.get(KW_MAPPED_SOURCE).is_some() =>
                    {
                        Value::Array(decode_casa_complex(
                            array,
                            self.variant,
                            scale as f32,
                            offset as f32,
                            target_type,
                        ))
                    }
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
    target_type: casa_types::PrimitiveType,
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
    target_type: casa_types::PrimitiveType,
) -> Result<ArrayValue, StorageError> {
    let complexes: Vec<Complex64> = arr
        .iter()
        .map(|&v| unpack_complex_standard(v, scale, offset))
        .collect();
    let shape = arr.shape().to_vec();
    match target_type {
        casa_types::PrimitiveType::Complex32 => {
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
    target_type: casa_types::PrimitiveType,
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
                casa_types::PrimitiveType::Complex32 => {
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
fn to_complex_scalar(c: Complex64, target_type: casa_types::PrimitiveType) -> ScalarValue {
    match target_type {
        casa_types::PrimitiveType::Complex32 => {
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
    use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
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
    fn refuses_writing_the_legacy_complex_layout() {
        let keywords = RecordValue::new(vec![RecordField::new(
            KW_CC_STORED_COL.to_owned(),
            Value::Scalar(ScalarValue::String("STORED".to_owned())),
        )]);
        for variant in ["CompressComplex", "CompressComplexSD"] {
            let codec = ComplexCompression::read(&keywords, "DATA", variant).unwrap();
            let values = ndarray::Array::from_elem(IxDyn(&[1, 1]), Complex32::new(1.0, -1.0));
            assert!(codec.check_writable().is_err());
            assert!(
                codec
                    .encode_visibility_cell(&values, None, (1.0, 0.0), None)
                    .is_err()
            );
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
