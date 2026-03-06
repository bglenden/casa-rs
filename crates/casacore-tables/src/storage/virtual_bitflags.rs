// SPDX-License-Identifier: LGPL-3.0-or-later
//! BitFlagsEngine — virtual Bool columns derived from integer bit-flag columns.
//!
//! Maps stored integer columns (UInt8, Int16, Int32) to Bool arrays by applying
//! a configurable bitmask: `virtual_bool[i] = (stored_int[i] & read_mask) != 0`.
//!
//! Nearly every MeasurementSet uses this engine for FLAG columns, where the
//! underlying FLAG_ROW or BIT_FLAGS column stores per-correlation bit flags
//! and the FLAG column presents a boolean view for downstream processing.
//!
//! # On-disk keywords (on the virtual Bool column)
//!
//! - `_BitFlagsEngine_ReadMask` (UInt32) — bitmask for read direction
//! - `_BitFlagsEngine_WriteMask` (UInt32) — bitmask for write direction
//! - `_BitFlagsEngine_ReadMaskKeys` (String array) — symbolic mask names
//! - `_BitFlagsEngine_WriteMaskKeys` (String array) — symbolic mask names
//! - `_BaseMappedArrayEngine_StoredColumnName` (String) — stored column name
//!
//! # DM type strings
//!
//! `"BitFlagsEngine<uChar"`, `"BitFlagsEngine<Short"`, `"BitFlagsEngine<Int"`
//! (note: no closing `>` — this is a C++ quirk).
//!
//! # C++ equivalent
//!
//! `BitFlagsEngine<uChar|Short|Int>` in
//! `casacore/tables/DataMan/BitFlagsEngine.h`.

use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

use super::StorageError;
use super::table_control::PlainColumnEntry;
use super::virtual_engine::{VirtualColumnEngine, VirtualContext};

// -- Keyword name constants --------------------------------------------------

/// Keyword storing the name of the underlying integer column.
const KW_STORED_COL: &str = "_BaseMappedArrayEngine_StoredColumnName";

/// Keyword storing the read-direction bitmask (UInt32).
const KW_READ_MASK: &str = "_BitFlagsEngine_ReadMask";

// -- Engine struct -----------------------------------------------------------

/// Virtual column engine that maps integer bit-flag columns to Bool arrays.
///
/// For each element in the stored integer column, the virtual Bool value is
/// `(stored & read_mask) != 0`. If the read mask keyword is absent, all bits
/// are considered (mask = `0xFFFF_FFFF`).
///
/// # C++ equivalent
///
/// `BitFlagsEngine<uChar|Short|Int>` in
/// `casacore/tables/DataMan/BitFlagsEngine.h`.
#[derive(Debug)]
pub(crate) struct BitFlagsEngine;

impl VirtualColumnEngine for BitFlagsEngine {
    fn type_name(&self) -> &str {
        "BitFlagsEngine"
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

            // Read stored column name from keywords.
            let stored_col_name = get_string_keyword(kw, KW_STORED_COL, col_name)?;

            // Read the bitmask (default: all bits set).
            let read_mask = get_uint32_keyword(kw, KW_READ_MASK).unwrap_or(0xFFFF_FFFF);

            for (row_idx, row) in rows.iter_mut().enumerate() {
                let stored_value = ctx.rows[row_idx].get(&stored_col_name);
                let virtual_value = match stored_value {
                    Some(v) => apply_bitmask(v, read_mask)?,
                    None => {
                        return Err(StorageError::FormatMismatch(format!(
                            "BitFlagsEngine: stored column '{stored_col_name}' \
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

// -- Transform logic ---------------------------------------------------------

/// Apply the bitmask transform: `(stored & mask) != 0`.
///
/// Handles both scalar and array stored values of integer types
/// (UInt8, Int16, Int32, UInt16, UInt32, Int64).
fn apply_bitmask(stored: &Value, mask: u32) -> Result<Value, StorageError> {
    match stored {
        Value::Scalar(sv) => {
            let bits = scalar_to_u32(sv)?;
            Ok(Value::Scalar(ScalarValue::Bool((bits & mask) != 0)))
        }
        Value::Array(av) => {
            let bool_arr = array_to_bool(av, mask)?;
            Ok(Value::Array(ArrayValue::Bool(bool_arr)))
        }
        Value::TableRef(_) => Err(StorageError::FormatMismatch(
            "BitFlagsEngine: cannot apply bitmask to a TableRef value".to_string(),
        )),
        Value::Record(_) => Err(StorageError::FormatMismatch(
            "BitFlagsEngine: cannot apply bitmask to a Record value".to_string(),
        )),
    }
}

/// Convert a scalar integer value to u32 for bitmask operations.
fn scalar_to_u32(sv: &ScalarValue) -> Result<u32, StorageError> {
    match sv {
        ScalarValue::UInt8(v) => Ok(*v as u32),
        ScalarValue::Int16(v) => Ok(*v as u32),
        ScalarValue::Int32(v) => Ok(*v as u32),
        ScalarValue::UInt16(v) => Ok(*v as u32),
        ScalarValue::UInt32(v) => Ok(*v),
        ScalarValue::Int64(v) => Ok(*v as u32),
        other => Err(StorageError::FormatMismatch(format!(
            "BitFlagsEngine: unsupported stored scalar type: {other:?}"
        ))),
    }
}

/// Convert an integer array to a Bool array via bitmask.
fn array_to_bool(av: &ArrayValue, mask: u32) -> Result<ArrayD<bool>, StorageError> {
    match av {
        ArrayValue::UInt8(a) => Ok(a.mapv(|x| (x as u32 & mask) != 0)),
        ArrayValue::Int16(a) => Ok(a.mapv(|x| (x as u32 & mask) != 0)),
        ArrayValue::Int32(a) => Ok(a.mapv(|x| (x as u32 & mask) != 0)),
        ArrayValue::UInt16(a) => Ok(a.mapv(|x| (x as u32 & mask) != 0)),
        ArrayValue::UInt32(a) => Ok(a.mapv(|x| (x & mask) != 0)),
        ArrayValue::Int64(a) => Ok(a.mapv(|x| (x as u32 & mask) != 0)),
        other => Err(StorageError::FormatMismatch(format!(
            "BitFlagsEngine: unsupported stored array type: {:?}",
            std::mem::discriminant(other)
        ))),
    }
}

// -- Keyword helpers ---------------------------------------------------------

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

/// Extract a UInt32 keyword value, supporting promotion from smaller int types.
fn get_uint32_keyword(kw: &RecordValue, key: &str) -> Option<u32> {
    match kw.get(key)? {
        Value::Scalar(ScalarValue::UInt32(v)) => Some(*v),
        Value::Scalar(ScalarValue::Int32(v)) => Some(*v as u32),
        Value::Scalar(ScalarValue::UInt8(v)) => Some(*v as u32),
        Value::Scalar(ScalarValue::Int16(v)) => Some(*v as u32),
        Value::Scalar(ScalarValue::UInt16(v)) => Some(*v as u32),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    /// Build a minimal VirtualContext for testing.
    fn make_context<'a>(
        col_descs: &'a [super::super::table_control::ColumnDescContents],
        rows: &'a [RecordValue],
        nrrow: usize,
    ) -> VirtualContext<'a> {
        VirtualContext {
            col_descs,
            rows,
            table_path: std::path::Path::new("/tmp/test_bitflags"),
            nrrow,
        }
    }

    fn make_col_desc(
        col_name: &str,
        keywords: RecordValue,
    ) -> super::super::table_control::ColumnDescContents {
        use super::super::data_type::CasacoreDataType;
        super::super::table_control::ColumnDescContents {
            class_name: String::new(),
            col_name: col_name.to_string(),
            comment: String::new(),
            data_manager_type: "BitFlagsEngine<uChar".to_string(),
            data_manager_group: "BitFlagsEngine".to_string(),
            data_type: CasacoreDataType::TpBool,
            option: 0,
            nrdim: -1,
            shape: Vec::new(),
            max_length: 0,
            keywords,
            is_array: true,
            primitive_type: Some(casacore_types::PrimitiveType::Bool),
        }
    }

    fn make_plain_col(seq: u32) -> super::super::table_control::PlainColumnEntry {
        super::super::table_control::PlainColumnEntry {
            original_name: String::new(),
            dm_seq_nr: seq,
            is_array: true,
        }
    }

    #[test]
    fn test_bitflags_uint8_array() {
        // Stored column: UInt8 array [0x01, 0x02, 0x04, 0x08]
        // Read mask: 0x05 (bits 0 and 2)
        // Expected: [true, false, true, false]
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("BIT_FLAGS".to_string())),
        ));
        kw.push(RecordField::new(
            KW_READ_MASK.to_string(),
            Value::Scalar(ScalarValue::UInt32(0x05)),
        ));

        let col_desc = make_col_desc("FLAG", kw);
        let pc = make_plain_col(1);

        let stored_arr =
            ArrayD::from_shape_vec(ndarray::IxDyn(&[4]), vec![0x01u8, 0x02, 0x04, 0x08]).unwrap();
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "BIT_FLAGS".to_string(),
            Value::Array(ArrayValue::UInt8(stored_arr)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc.clone()];
        let ctx = make_context(&descs, &stored_rows, 1);
        let engine = BitFlagsEngine;
        engine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .expect("materialize should succeed");

        let flag_val = rows[0].get("FLAG").expect("FLAG column missing");
        match flag_val {
            Value::Array(ArrayValue::Bool(arr)) => {
                assert_eq!(arr.as_slice().unwrap(), &[true, false, true, false]);
            }
            other => panic!("expected Bool array, got {other:?}"),
        }
    }

    #[test]
    fn test_bitflags_int16_scalar() {
        // Stored: Int16 scalar = 0x0003 (bits 0 and 1 set)
        // Mask: 0x02 (bit 1 only)
        // Expected: true
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("FLAGS_INT".to_string())),
        ));
        kw.push(RecordField::new(
            KW_READ_MASK.to_string(),
            Value::Scalar(ScalarValue::UInt32(0x02)),
        ));

        let col_desc = make_col_desc("FLAG", kw);
        let pc = make_plain_col(1);

        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "FLAGS_INT".to_string(),
            Value::Scalar(ScalarValue::Int16(0x0003)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc.clone()];
        let ctx = make_context(&descs, &stored_rows, 1);
        BitFlagsEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        match rows[0].get("FLAG").unwrap() {
            Value::Scalar(ScalarValue::Bool(b)) => assert!(*b),
            other => panic!("expected Bool scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_bitflags_int32_no_match() {
        // Stored: Int32 scalar = 0x0010
        // Mask: 0x000F
        // Expected: false (no overlap)
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("FLAGS32".to_string())),
        ));
        kw.push(RecordField::new(
            KW_READ_MASK.to_string(),
            Value::Scalar(ScalarValue::UInt32(0x000F)),
        ));

        let col_desc = make_col_desc("FLAG", kw);
        let pc = make_plain_col(1);

        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "FLAGS32".to_string(),
            Value::Scalar(ScalarValue::Int32(0x0010)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc.clone()];
        let ctx = make_context(&descs, &stored_rows, 1);
        BitFlagsEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        match rows[0].get("FLAG").unwrap() {
            Value::Scalar(ScalarValue::Bool(b)) => assert!(!*b),
            other => panic!("expected Bool scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_bitflags_default_mask_all_bits() {
        // No _BitFlagsEngine_ReadMask keyword → default 0xFFFFFFFF
        // Any non-zero stored value should produce true.
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("FLAGS".to_string())),
        ));

        let col_desc = make_col_desc("FLAG", kw);
        let pc = make_plain_col(1);

        let stored_arr = ArrayD::from_shape_vec(ndarray::IxDyn(&[3]), vec![0u8, 1, 255]).unwrap();
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "FLAGS".to_string(),
            Value::Array(ArrayValue::UInt8(stored_arr)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc.clone()];
        let ctx = make_context(&descs, &stored_rows, 1);
        BitFlagsEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        match rows[0].get("FLAG").unwrap() {
            Value::Array(ArrayValue::Bool(arr)) => {
                assert_eq!(arr.as_slice().unwrap(), &[false, true, true]);
            }
            other => panic!("expected Bool array, got {other:?}"),
        }
    }

    #[test]
    fn test_bitflags_multiple_rows() {
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("BF".to_string())),
        ));
        kw.push(RecordField::new(
            KW_READ_MASK.to_string(),
            Value::Scalar(ScalarValue::UInt32(0x01)),
        ));

        let col_desc = make_col_desc("FLAG", kw);
        let pc = make_plain_col(1);

        let stored_rows: Vec<RecordValue> = (0..3)
            .map(|i| {
                let mut r = RecordValue::default();
                r.push(RecordField::new(
                    "BF".to_string(),
                    Value::Scalar(ScalarValue::UInt8(i as u8)),
                ));
                r
            })
            .collect();

        let mut rows: Vec<RecordValue> = (0..3).map(|_| RecordValue::default()).collect();

        let descs = [col_desc.clone()];
        let ctx = make_context(&descs, &stored_rows, 3);
        BitFlagsEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        // Row 0: stored=0, 0&1=0 → false
        // Row 1: stored=1, 1&1=1 → true
        // Row 2: stored=2, 2&1=0 → false
        let expected = [false, true, false];
        for (i, exp) in expected.iter().enumerate() {
            match rows[i].get("FLAG").unwrap() {
                Value::Scalar(ScalarValue::Bool(b)) => assert_eq!(b, exp, "row {i}"),
                other => panic!("row {i}: expected Bool scalar, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_bitflags_uint32_array() {
        let mut kw = RecordValue::default();
        kw.push(RecordField::new(
            KW_STORED_COL.to_string(),
            Value::Scalar(ScalarValue::String("BF32".to_string())),
        ));
        kw.push(RecordField::new(
            KW_READ_MASK.to_string(),
            Value::Scalar(ScalarValue::UInt32(0xFF00)),
        ));

        let col_desc = make_col_desc("FLAG", kw);
        let pc = make_plain_col(1);

        let stored_arr = ArrayD::from_shape_vec(
            ndarray::IxDyn(&[4]),
            vec![0x00FFu32, 0x0100, 0xFF00, 0x0000],
        )
        .unwrap();
        let mut stored_row = RecordValue::default();
        stored_row.push(RecordField::new(
            "BF32".to_string(),
            Value::Array(ArrayValue::UInt32(stored_arr)),
        ));

        let stored_rows = vec![stored_row];
        let mut rows = vec![RecordValue::default()];

        let descs = [col_desc.clone()];
        let ctx = make_context(&descs, &stored_rows, 1);
        BitFlagsEngine
            .materialize(&ctx, &[(0, &pc)], &mut rows)
            .unwrap();

        match rows[0].get("FLAG").unwrap() {
            Value::Array(ArrayValue::Bool(arr)) => {
                assert_eq!(arr.as_slice().unwrap(), &[false, true, true, false]);
            }
            other => panic!("expected Bool array, got {other:?}"),
        }
    }
}
