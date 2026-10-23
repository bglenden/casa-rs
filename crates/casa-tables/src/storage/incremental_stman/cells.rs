// SPDX-License-Identifier: LGPL-3.0-or-later
//! The bytes of one IncrementalStMan cell, a scalar or a fixed-shape array,
//! as casacore's `ISMColumn` writes and reads them.
//!
//! A cell of `nrelem` elements is its elements in canonical form, in storage
//! (Fortran) order ([`cell_elements`](super::super::cell_elements)): Bools
//! bit-packed from the cell's first byte (`Conversion::boolToBit`), other
//! fixed-width types at their canonical size in the file's byte order, and
//! strings as `ISMColumn::fromString` lays them out. A scalar is a cell of
//! one element, so a one-element array is stored exactly as a scalar of its
//! type.

use std::iter::repeat_n;

use casa_types::{ArrayValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

pub(super) use super::super::cell_elements::Cell;
use super::super::cell_elements::{Canonical, Element, encode_fixed_cell, for_each_element};
use super::{CasacoreDataType, ColumnRawData, StorageError, ism_element_size, read_ism_string};

/// The canonical bytes of `cell` in a column of `nrelem` elements of type
/// `data_type`; the elements it does not give are zeros or empty strings.
pub(super) fn encode_cell(
    cell: Cell<'_>,
    data_type: CasacoreDataType,
    nrelem: usize,
    big_endian: bool,
) -> Vec<u8> {
    match data_type {
        CasacoreDataType::TpBool => {
            let mut bytes = vec![0u8; nrelem.div_ceil(8)];
            for_each_element::<bool>(cell, nrelem, |index, &element| {
                if element {
                    bytes[index / 8] |= 1 << (index % 8);
                }
            });
            bytes
        }
        CasacoreDataType::TpString => encode_strings(cell, nrelem, big_endian),
        _ => {
            let mut bytes = vec![0u8; ism_element_size(data_type) * nrelem];
            encode_fixed_cell(&mut bytes, cell, data_type, nrelem, big_endian);
            bytes
        }
    }
}

/// Strings as `ISMColumn::fromString` writes them: a length that includes
/// itself, then each string. One string is its bytes after the length
/// (`[4 + len][bytes]`, the layout of a scalar); several are each preceded
/// by their own length.
fn encode_strings(cell: Cell<'_>, nrelem: usize, big_endian: bool) -> Vec<u8> {
    let length = |length: usize| {
        let length = length as u32;
        if big_endian {
            length.to_be_bytes()
        } else {
            length.to_le_bytes()
        }
    };
    if nrelem == 1 {
        let mut string = "";
        for_each_element::<String>(cell, 1, |_, element| string = element.as_str());
        let mut bytes = Vec::with_capacity(4 + string.len());
        bytes.extend_from_slice(&length(4 + string.len()));
        bytes.extend_from_slice(string.as_bytes());
        return bytes;
    }
    let mut strings = vec![""; nrelem];
    for_each_element::<String>(cell, nrelem, |index, element| {
        strings[index] = element.as_str();
    });
    let total = 4 + strings.iter().map(|string| 4 + string.len()).sum::<usize>();
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&length(total));
    for string in strings {
        bytes.extend_from_slice(&length(string.len()));
        bytes.extend_from_slice(string.as_bytes());
    }
    bytes
}

/// The fixed-shape array cell of `nrelem` elements of type `data_type` and
/// shape `shape` at the start of `data`.
pub(super) fn decode_array_cell(
    data: &[u8],
    data_type: CasacoreDataType,
    nrelem: usize,
    shape: &[i32],
    big_endian: bool,
) -> Result<Value, StorageError> {
    let shape = shape.iter().map(|&dim| dim as usize).collect::<Vec<_>>();
    fn shaped<T>(shape: &[usize], elements: Vec<T>) -> Result<ArrayD<T>, StorageError> {
        ArrayD::from_shape_vec(IxDyn(shape).f(), elements)
            .map_err(|err| StorageError::FormatMismatch(format!("array shape: {err}")))
    }
    fn fixed<T: Canonical>(data: &[u8], nrelem: usize, big_endian: bool) -> Vec<T> {
        data[..nrelem * T::BYTES]
            .chunks_exact(T::BYTES)
            .map(|bytes| T::get(bytes, big_endian))
            .collect()
    }
    let array = match data_type {
        CasacoreDataType::TpBool => ArrayValue::Bool(shaped(
            &shape,
            (0..nrelem)
                .map(|index| (data[index / 8] >> (index % 8)) & 1 != 0)
                .collect(),
        )?),
        CasacoreDataType::TpString => {
            ArrayValue::String(shaped(&shape, read_ism_string(data, nrelem, big_endian)?)?)
        }
        CasacoreDataType::TpUChar => {
            ArrayValue::UInt8(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpShort => {
            ArrayValue::Int16(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpUShort => {
            ArrayValue::UInt16(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpInt => {
            ArrayValue::Int32(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpUInt => {
            ArrayValue::UInt32(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpInt64 => {
            ArrayValue::Int64(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpFloat => {
            ArrayValue::Float32(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpDouble => {
            ArrayValue::Float64(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpComplex => {
            ArrayValue::Complex32(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        CasacoreDataType::TpDComplex => {
            ArrayValue::Complex64(shaped(&shape, fixed(data, nrelem, big_endian))?)
        }
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported ISM array type: {data_type:?}"
            )));
        }
    };
    Ok(Value::Array(array))
}

/// Array cells of `nrelem` elements of type `data_type`, flattened in
/// Fortran order, a mistyped or non-array cell as `nrelem` zeros or empty
/// strings.
pub(super) fn flatten_array_cells(
    values: &[Value],
    data_type: CasacoreDataType,
    nrelem: usize,
) -> Result<ColumnRawData, StorageError> {
    fn flatten<T: Element + Clone + Default>(values: &[Value], nrelem: usize) -> Vec<T> {
        let mut flat = Vec::with_capacity(values.len() * nrelem);
        for value in values {
            if let Value::Array(array) = value
                && let Some(array) = T::of_array(array)
            {
                flat.extend(array.t().iter().cloned());
            } else {
                flat.extend(repeat_n(T::default(), nrelem));
            }
        }
        flat
    }
    Ok(match data_type {
        CasacoreDataType::TpBool => ColumnRawData::Bool(flatten(values, nrelem)),
        CasacoreDataType::TpUChar => ColumnRawData::UInt8(flatten(values, nrelem)),
        CasacoreDataType::TpShort => ColumnRawData::Int16(flatten(values, nrelem)),
        CasacoreDataType::TpUShort => ColumnRawData::UInt16(flatten(values, nrelem)),
        CasacoreDataType::TpInt => ColumnRawData::Int32(flatten(values, nrelem)),
        CasacoreDataType::TpUInt => ColumnRawData::UInt32(flatten(values, nrelem)),
        CasacoreDataType::TpInt64 => ColumnRawData::Int64(flatten(values, nrelem)),
        CasacoreDataType::TpFloat => ColumnRawData::Float32(flatten(values, nrelem)),
        CasacoreDataType::TpDouble => ColumnRawData::Float64(flatten(values, nrelem)),
        CasacoreDataType::TpComplex => ColumnRawData::Complex32(flatten(values, nrelem)),
        CasacoreDataType::TpDComplex => ColumnRawData::Complex64(flatten(values, nrelem)),
        CasacoreDataType::TpString => ColumnRawData::String(flatten(values, nrelem)),
        _ => {
            return Err(StorageError::FormatMismatch(format!(
                "unsupported ISM array type: {data_type:?}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    //! A one-element array is stored as a scalar of its type; arrays keep
    //! every element, in storage order, for every element type.

    use casa_types::{Complex32, Complex64, ScalarValue};

    use super::*;

    /// A scalar of each element type and the matching data type.
    fn scalars() -> Vec<(ScalarValue, CasacoreDataType)> {
        vec![
            (ScalarValue::Bool(true), CasacoreDataType::TpBool),
            (ScalarValue::UInt8(200), CasacoreDataType::TpUChar),
            (ScalarValue::Int16(-300), CasacoreDataType::TpShort),
            (ScalarValue::UInt16(60_000), CasacoreDataType::TpUShort),
            (ScalarValue::Int32(-70_000), CasacoreDataType::TpInt),
            (ScalarValue::UInt32(4_000_000_000), CasacoreDataType::TpUInt),
            (ScalarValue::Int64(-1 << 40), CasacoreDataType::TpInt64),
            (ScalarValue::Float32(2.5), CasacoreDataType::TpFloat),
            (ScalarValue::Float64(7.0), CasacoreDataType::TpDouble),
            (
                ScalarValue::Complex32(Complex32::new(1.5, -2.0)),
                CasacoreDataType::TpComplex,
            ),
            (
                ScalarValue::Complex64(Complex64::new(-3.0, 0.25)),
                CasacoreDataType::TpDComplex,
            ),
            (
                ScalarValue::String("seven".to_string()),
                CasacoreDataType::TpString,
            ),
        ]
    }

    /// The one-element array holding `scalar`.
    fn singleton(scalar: &ScalarValue) -> ArrayValue {
        fn one<T: Clone>(value: &T) -> ArrayD<T> {
            ArrayD::from_elem(IxDyn(&[1]), value.clone())
        }
        match scalar {
            ScalarValue::Bool(value) => ArrayValue::Bool(one(value)),
            ScalarValue::UInt8(value) => ArrayValue::UInt8(one(value)),
            ScalarValue::Int16(value) => ArrayValue::Int16(one(value)),
            ScalarValue::UInt16(value) => ArrayValue::UInt16(one(value)),
            ScalarValue::Int32(value) => ArrayValue::Int32(one(value)),
            ScalarValue::UInt32(value) => ArrayValue::UInt32(one(value)),
            ScalarValue::Int64(value) => ArrayValue::Int64(one(value)),
            ScalarValue::Float32(value) => ArrayValue::Float32(one(value)),
            ScalarValue::Float64(value) => ArrayValue::Float64(one(value)),
            ScalarValue::Complex32(value) => ArrayValue::Complex32(one(value)),
            ScalarValue::Complex64(value) => ArrayValue::Complex64(one(value)),
            ScalarValue::String(value) => ArrayValue::String(one(value)),
        }
    }

    #[test]
    fn a_one_element_array_encodes_as_its_scalar() {
        for big_endian in [false, true] {
            for (scalar, data_type) in scalars() {
                let array = singleton(&scalar);
                let from_scalar = encode_cell(Cell::Scalar(&scalar), data_type, 1, big_endian);
                let from_array = encode_cell(Cell::Array(&array), data_type, 1, big_endian);
                assert_eq!(from_array, from_scalar, "{data_type:?}");
                assert_ne!(
                    from_array,
                    encode_cell(Cell::Undefined, data_type, 1, big_endian),
                    "{data_type:?} keeps its value"
                );
                let decoded =
                    decode_array_cell(&from_array, data_type, 1, &[1], big_endian).unwrap();
                assert_eq!(decoded, Value::Array(array), "{data_type:?}");
            }
        }
    }

    #[test]
    fn one_string_takes_the_scalar_layout() {
        let array = ArrayValue::String(ArrayD::from_elem(IxDyn(&[1]), "seven".to_string()));
        let mut expected = 9u32.to_le_bytes().to_vec();
        expected.extend_from_slice(b"seven");
        assert_eq!(
            encode_cell(Cell::Array(&array), CasacoreDataType::TpString, 1, false),
            expected
        );
    }

    #[test]
    fn arrays_keep_their_elements_in_storage_order() {
        let values = (0..6).map(|i| i * 3 + 1).collect::<Vec<i32>>();
        let ints = ArrayD::from_shape_vec(IxDyn(&[2, 3]), values).unwrap();
        let strings = ints.mapv(|value| format!("s{value}"));
        let flags = ints.mapv(|value| value % 2 == 0);
        for big_endian in [false, true] {
            for (array, data_type) in [
                (ArrayValue::Int32(ints.clone()), CasacoreDataType::TpInt),
                (
                    ArrayValue::String(strings.clone()),
                    CasacoreDataType::TpString,
                ),
                (ArrayValue::Bool(flags.clone()), CasacoreDataType::TpBool),
                (
                    ArrayValue::Int16(ints.mapv(|value| value as i16)),
                    CasacoreDataType::TpShort,
                ),
                (
                    ArrayValue::UInt16(ints.mapv(|value| value as u16)),
                    CasacoreDataType::TpUShort,
                ),
                (
                    ArrayValue::UInt32(ints.mapv(|value| value as u32)),
                    CasacoreDataType::TpUInt,
                ),
                (
                    ArrayValue::Int64(ints.mapv(i64::from)),
                    CasacoreDataType::TpInt64,
                ),
                (
                    ArrayValue::UInt8(ints.mapv(|value| value as u8)),
                    CasacoreDataType::TpUChar,
                ),
            ] {
                let encoded = encode_cell(Cell::Array(&array), data_type, 6, big_endian);
                let decoded =
                    decode_array_cell(&encoded, data_type, 6, &[2, 3], big_endian).unwrap();
                assert_eq!(decoded, Value::Array(array), "{data_type:?}");
            }
        }
        // Storage order is Fortran order: the first element, then the one
        // below it.
        let encoded = encode_cell(
            Cell::Array(&ArrayValue::Int32(ints)),
            CasacoreDataType::TpInt,
            6,
            false,
        );
        assert_eq!(i32::from_le_bytes(encoded[4..8].try_into().unwrap()), 10);
    }
}
