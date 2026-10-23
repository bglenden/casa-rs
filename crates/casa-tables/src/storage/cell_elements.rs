// SPDX-License-Identifier: LGPL-3.0-or-later
//! The elements of one cell, a scalar or a fixed-shape array, in storage
//! order, and their canonical encoding: what casacore's `StandardStMan` and
//! `IncrementalStMan` store for a cell kept in their own data.
//!
//! A cell of `nrelem` elements is its elements in storage (Fortran) order,
//! each fixed-width element at its canonical size in the file's byte order
//! (`CanonicalConversion` or `LECanonicalConversion`). A scalar is a cell of
//! one element, so a one-element array is stored exactly as a scalar of its
//! type. How Bools are packed and strings laid out is the storage manager's
//! own; each takes the elements from here.

use casa_types::{ArrayValue, Complex32, Complex64, ScalarValue, Value};
use ndarray::ArrayD;

use super::data_type::CasacoreDataType;

/// One cell as the encoders take it.
#[derive(Clone, Copy)]
pub(super) enum Cell<'a> {
    Scalar(&'a ScalarValue),
    Array(&'a ArrayValue),
    /// No value: the cell encodes as zeros and empty strings.
    Undefined,
}

impl<'a> From<Option<&'a Value>> for Cell<'a> {
    fn from(value: Option<&'a Value>) -> Self {
        match value {
            Some(Value::Scalar(value)) => Self::Scalar(value),
            Some(Value::Array(value)) => Self::Array(value),
            _ => Self::Undefined,
        }
    }
}

impl<'a> From<Option<&'a ScalarValue>> for Cell<'a> {
    fn from(value: Option<&'a ScalarValue>) -> Self {
        value.map_or(Self::Undefined, Self::Scalar)
    }
}

/// An element type of cells: where its elements are in a scalar or an
/// array of the type.
pub(super) trait Element: Sized {
    fn of_scalar(value: &ScalarValue) -> Option<&Self>;
    fn of_array(value: &ArrayValue) -> Option<&ArrayD<Self>>;
}

/// A fixed-width element type: its canonical size and encoding.
pub(super) trait Canonical: Element + Copy {
    const BYTES: usize;
    fn put(self, dst: &mut [u8], big_endian: bool);
    fn get(src: &[u8], big_endian: bool) -> Self;
}

macro_rules! element {
    ($type:ty, $variant:ident) => {
        impl Element for $type {
            fn of_scalar(value: &ScalarValue) -> Option<&Self> {
                match value {
                    ScalarValue::$variant(value) => Some(value),
                    _ => None,
                }
            }

            fn of_array(value: &ArrayValue) -> Option<&ArrayD<Self>> {
                match value {
                    ArrayValue::$variant(value) => Some(value),
                    _ => None,
                }
            }
        }
    };
}

/// A fixed-width element type encoded by the primitive's own byte
/// conversions.
macro_rules! primitive {
    ($type:ty, $variant:ident) => {
        element!($type, $variant);

        impl Canonical for $type {
            const BYTES: usize = size_of::<$type>();

            fn put(self, dst: &mut [u8], big_endian: bool) {
                let bytes = if big_endian {
                    self.to_be_bytes()
                } else {
                    self.to_le_bytes()
                };
                dst[..Self::BYTES].copy_from_slice(&bytes);
            }

            fn get(src: &[u8], big_endian: bool) -> Self {
                let bytes = src[..Self::BYTES].try_into().expect("element bytes");
                if big_endian {
                    Self::from_be_bytes(bytes)
                } else {
                    Self::from_le_bytes(bytes)
                }
            }
        }
    };
}

/// A complex element type: its real part, then its imaginary part.
macro_rules! complex {
    ($type:ty, $variant:ident, $part:ty) => {
        element!($type, $variant);

        impl Canonical for $type {
            const BYTES: usize = 2 * size_of::<$part>();

            fn put(self, dst: &mut [u8], big_endian: bool) {
                self.re.put(dst, big_endian);
                self.im.put(&mut dst[size_of::<$part>()..], big_endian);
            }

            fn get(src: &[u8], big_endian: bool) -> Self {
                Self::new(
                    <$part>::get(src, big_endian),
                    <$part>::get(&src[size_of::<$part>()..], big_endian),
                )
            }
        }
    };
}

element!(bool, Bool);
element!(String, String);
primitive!(u8, UInt8);
primitive!(i16, Int16);
primitive!(u16, UInt16);
primitive!(i32, Int32);
primitive!(u32, UInt32);
primitive!(i64, Int64);
primitive!(f32, Float32);
primitive!(f64, Float64);
complex!(Complex32, Complex32, f32);
complex!(Complex64, Complex64, f64);

/// Call `put(index, element)` for the first `nrelem` elements of `cell` of
/// type `T`, in storage order: a scalar is the element of a one-element
/// cell, an array gives its elements in Fortran order, and an undefined or
/// mistyped cell gives none.
pub(super) fn for_each_element<'a, T: Element + 'a>(
    cell: Cell<'a>,
    nrelem: usize,
    mut put: impl FnMut(usize, &'a T),
) {
    match cell {
        Cell::Scalar(value) if nrelem == 1 => {
            if let Some(element) = T::of_scalar(value) {
                put(0, element);
            }
        }
        Cell::Array(value) => {
            if let Some(array) = T::of_array(value) {
                for (index, element) in array.t().into_iter().take(nrelem).enumerate() {
                    put(index, element);
                }
            }
        }
        _ => {}
    }
}

/// The `nrelem` Bools of `cell` in storage order; the elements it does not
/// give are `false`.
pub(super) fn cell_bools(cell: Cell<'_>, nrelem: usize) -> Vec<bool> {
    let mut bools = vec![false; nrelem];
    for_each_element::<bool>(cell, nrelem, |index, &element| bools[index] = element);
    bools
}

/// Write the canonical bytes of the `nrelem` elements of type `T` of `cell`
/// to the start of `dst`; the elements it does not give are zeros.
pub(super) fn encode_elements<T: Canonical>(
    dst: &mut [u8],
    cell: Cell<'_>,
    nrelem: usize,
    big_endian: bool,
) {
    dst[..T::BYTES * nrelem].fill(0);
    for_each_element::<T>(cell, nrelem, |index, &element| {
        element.put(&mut dst[index * T::BYTES..], big_endian);
    });
}

/// Write the canonical bytes of `cell`, `nrelem` elements of the
/// fixed-width type `data_type`, to the start of `dst`: as many bytes as the
/// elements take, the elements it does not give as zeros. Returns whether
/// `data_type` is a fixed-width type; Bools, strings and other types write
/// nothing.
pub(super) fn encode_fixed_cell(
    dst: &mut [u8],
    cell: Cell<'_>,
    data_type: CasacoreDataType,
    nrelem: usize,
    big_endian: bool,
) -> bool {
    match data_type {
        CasacoreDataType::TpUChar => encode_elements::<u8>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpShort => encode_elements::<i16>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpUShort => encode_elements::<u16>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpInt => encode_elements::<i32>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpUInt => encode_elements::<u32>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpInt64 => encode_elements::<i64>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpFloat => encode_elements::<f32>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpDouble => encode_elements::<f64>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpComplex => encode_elements::<Complex32>(dst, cell, nrelem, big_endian),
        CasacoreDataType::TpDComplex => encode_elements::<Complex64>(dst, cell, nrelem, big_endian),
        _ => return false,
    }
    true
}

#[cfg(test)]
mod tests {
    //! A one-element array encodes as its scalar; an array keeps every
    //! element, in storage order, for every fixed-width type.

    use ndarray::{IxDyn, ShapeBuilder};

    use super::*;

    /// The one-element array holding the fixed-width `scalar`.
    fn singleton(scalar: &ScalarValue) -> ArrayValue {
        fn one<T: Clone>(value: &T) -> ArrayD<T> {
            ArrayD::from_elem(IxDyn(&[1]), value.clone())
        }
        match scalar {
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
            other => panic!("not a fixed-width numeric scalar: {other:?}"),
        }
    }

    #[test]
    fn a_one_element_array_encodes_as_its_scalar() {
        let cases = [
            (ScalarValue::UInt8(200), CasacoreDataType::TpUChar, 1),
            (ScalarValue::Int16(-300), CasacoreDataType::TpShort, 2),
            (ScalarValue::UInt16(60_000), CasacoreDataType::TpUShort, 2),
            (ScalarValue::Int32(-70_000), CasacoreDataType::TpInt, 4),
            (
                ScalarValue::UInt32(4_000_000_000),
                CasacoreDataType::TpUInt,
                4,
            ),
            (ScalarValue::Int64(-1 << 40), CasacoreDataType::TpInt64, 8),
            (ScalarValue::Float32(2.5), CasacoreDataType::TpFloat, 4),
            (ScalarValue::Float64(7.0), CasacoreDataType::TpDouble, 8),
            (
                ScalarValue::Complex32(Complex32::new(1.5, -2.0)),
                CasacoreDataType::TpComplex,
                8,
            ),
            (
                ScalarValue::Complex64(Complex64::new(-3.0, 0.25)),
                CasacoreDataType::TpDComplex,
                16,
            ),
        ];
        for big_endian in [false, true] {
            for (scalar, data_type, bytes) in &cases {
                let array = singleton(scalar);
                let (mut from_scalar, mut from_array) = (vec![0xff; *bytes], vec![0xff; *bytes]);
                assert!(encode_fixed_cell(
                    &mut from_scalar,
                    Cell::Scalar(scalar),
                    *data_type,
                    1,
                    big_endian
                ));
                encode_fixed_cell(
                    &mut from_array,
                    Cell::Array(&array),
                    *data_type,
                    1,
                    big_endian,
                );
                assert_eq!(from_array, from_scalar, "{data_type:?}");
                assert!(from_array.iter().any(|&byte| byte != 0), "{data_type:?}");
            }
        }
    }

    #[test]
    fn arrays_keep_their_elements_in_storage_order() {
        let values = (0..6).map(|i| (i * 3 + 1) as i16).collect::<Vec<_>>();
        let array =
            ArrayValue::Int16(ArrayD::from_shape_vec(IxDyn(&[2, 3]), values).expect("shape"));
        let mut encoded = vec![0xff; 12];
        encode_fixed_cell(
            &mut encoded,
            Cell::Array(&array),
            CasacoreDataType::TpShort,
            6,
            true,
        );
        // Fortran order: element (1, 0), 10, follows element (0, 0), 1.
        let elements = encoded
            .as_chunks::<2>()
            .0
            .iter()
            .map(|bytes| i16::from_be_bytes(*bytes))
            .collect::<Vec<_>>();
        assert_eq!(elements, [1, 10, 4, 13, 7, 16]);

        let flags = ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![true, false, false, true])
            .expect("shape");
        assert_eq!(
            cell_bools(Cell::Array(&ArrayValue::Bool(flags)), 4),
            [true, false, false, true]
        );
        assert_eq!(cell_bools(Cell::Undefined, 3), [false; 3]);
    }

    #[test]
    fn an_undefined_cell_overwrites_the_cell_with_zeros() {
        let mut encoded = vec![0xff; 8];
        encode_fixed_cell(
            &mut encoded,
            Cell::Undefined,
            CasacoreDataType::TpFloat,
            2,
            false,
        );
        assert_eq!(encoded, [0; 8]);
    }
}
