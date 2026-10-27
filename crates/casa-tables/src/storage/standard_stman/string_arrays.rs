// SPDX-License-Identifier: LGPL-3.0-or-later
//! The string-array cells of a StandardStMan file being read: the bytes a
//! cell's string buckets hold, decoded as casacore's `SSMStringHandler`
//! writes them.

use casa_types::{ArrayValue, Value};
use ndarray::ShapeBuilder;

use super::StorageError;

/// One string-array cell as casacore's `SSMStringHandler::get` reads it
/// from `block`, the cell's bytes. A column of fixed shape stores only its
/// strings, each a length and its bytes; any other column stores first its
/// shape (its dimension count, each extent) and a filled flag, the strings
/// following only when the flag is set. A 0-dimensional shape is
/// casacore's empty `IPosition`, an array of no elements, read as `[0]`.
pub(super) fn decode(block: &[u8], fixed_shape: Option<&[i32]>) -> Result<Value, StorageError> {
    let (shape, mut pos, filled_flag) = if let Some(shape) = fixed_shape {
        (
            shape.iter().map(|&dim| dim as usize).collect::<Vec<_>>(),
            0usize,
            true,
        )
    } else {
        if block.len() < 8 {
            return Err(StorageError::FormatMismatch(
                "SSM string array block too short for shape header".to_string(),
            ));
        }
        let ndim = u32::from_be_bytes([block[0], block[1], block[2], block[3]]) as usize;
        let shape_bytes = 4 + ndim * 4 + 4;
        if block.len() < shape_bytes {
            return Err(StorageError::FormatMismatch(
                "SSM string array block too short for declared shape".to_string(),
            ));
        }

        let mut shape = Vec::with_capacity(ndim.max(1));
        let mut pos = 4usize;
        for _ in 0..ndim {
            let dim =
                i32::from_be_bytes([block[pos], block[pos + 1], block[pos + 2], block[pos + 3]]);
            if dim < 0 {
                return Err(StorageError::FormatMismatch(
                    "SSM string array shape contains negative extent".to_string(),
                ));
            }
            shape.push(dim as usize);
            pos += 4;
        }
        if ndim == 0 {
            shape.push(0);
        }
        let filled_flag =
            i32::from_be_bytes([block[pos], block[pos + 1], block[pos + 2], block[pos + 3]]) != 0;
        (shape, pos + 4, filled_flag)
    };

    let nrelem = shape.iter().try_fold(1usize, |acc, &dim| {
        acc.checked_mul(dim).ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "SSM string array shape product overflow for shape {:?}",
                shape
            ))
        })
    })?;
    let mut values = Vec::with_capacity(nrelem);
    if filled_flag {
        for _ in 0..nrelem {
            if pos + 4 > block.len() {
                return Err(StorageError::FormatMismatch(
                    "SSM string array: block too short for element length prefix".to_string(),
                ));
            }
            let slen =
                u32::from_be_bytes([block[pos], block[pos + 1], block[pos + 2], block[pos + 3]])
                    as usize;
            pos += 4;
            if pos + slen > block.len() {
                return Err(StorageError::FormatMismatch(
                    "SSM string array: block too short for element payload".to_string(),
                ));
            }
            let s = String::from_utf8(block[pos..pos + slen].to_vec())
                .map_err(|e| StorageError::FormatMismatch(format!("invalid UTF-8: {e}")))?;
            pos += slen;
            values.push(s);
        }
    } else {
        values.resize(nrelem, String::new());
    }

    let arr = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape).f(), values)
        .map_err(|e| StorageError::FormatMismatch(format!("array shape: {e}")))?;
    Ok(Value::Array(ArrayValue::String(arr)))
}

#[cfg(test)]
mod tests {
    use casa_types::{ArrayValue, Value};

    use super::decode;

    fn int(value: i32) -> [u8; 4] {
        value.to_be_bytes()
    }

    /// A cell's bytes: a shape header (dimension count, extents, filled
    /// flag), when given, then each string's length and bytes.
    fn cell(header: Option<(&[i32], i32)>, strings: &[&str]) -> Vec<u8> {
        let mut bytes = Vec::new();
        if let Some((shape, filled)) = header {
            bytes.extend(int(shape.len() as i32));
            for dim in shape {
                bytes.extend(int(*dim));
            }
            bytes.extend(int(filled));
        }
        for string in strings {
            bytes.extend(int(string.len() as i32));
            bytes.extend(string.as_bytes());
        }
        bytes
    }

    fn strings(value: &Value) -> (Vec<usize>, Vec<String>) {
        let Value::Array(ArrayValue::String(array)) = value else {
            panic!("a string array: {value:?}");
        };
        (array.shape().to_vec(), array.t().iter().cloned().collect())
    }

    /// casacore writes an empty `IPosition` cell, filled, as eight bytes:
    /// no dimensions, then the flag, and no strings (`alma/multiobs.ms`'s
    /// SOURCE `TRANSITION`, #923). It is an array of no elements, not one.
    #[test]
    fn a_zero_dimensional_cell_holds_no_strings() {
        for filled in [1, 0] {
            let bytes = cell(Some((&[], filled)), &[]);
            assert_eq!(bytes.len(), 8);
            assert_eq!(
                strings(&decode(&bytes, None).expect("decode")),
                (vec![0], vec![]),
                "filled {filled}"
            );
        }
    }

    /// A shaped cell, filled or not, and a fixed-shape column's strings
    /// without a header, in casacore's Fortran order.
    #[test]
    fn shaped_cells_decode_as_casacore_writes_them() {
        let filled = cell(Some((&[2, 1], 1)), &["ab", "c"]);
        assert_eq!(
            strings(&decode(&filled, None).expect("filled")),
            (vec![2, 1], vec!["ab".to_string(), "c".to_string()])
        );
        let empty = cell(Some((&[3], 0)), &[]);
        assert_eq!(
            strings(&decode(&empty, None).expect("unfilled")),
            (vec![3], vec![String::new(); 3])
        );
        let fixed = cell(None, &["x", "yz"]);
        assert_eq!(
            strings(&decode(&fixed, Some(&[2])).expect("fixed")),
            (vec![2], vec!["x".to_string(), "yz".to_string()])
        );
        assert!(decode(&cell(Some((&[2], 1)), &["ab"]), None).is_err());
    }
}
