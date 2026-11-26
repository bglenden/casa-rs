// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sparse tile patches can borrow cells already in numeric tile order.

use super::{
    ArrayD, ArrayValue, CasacoreDataType, Cow, StorageError, TilePixel, encode_tiled_array,
    is_fortran_contiguous, tile_values_as_bytes,
};

pub(super) fn encode_row(
    value: &ArrayValue,
    big_endian: bool,
) -> Result<(Cow<'_, [u8]>, CasacoreDataType), StorageError> {
    if big_endian == cfg!(target_endian = "big") {
        let borrowed = match value {
            ArrayValue::Complex32(array) => {
                fortran_bytes(array).map(|bytes| (bytes, CasacoreDataType::TpComplex))
            }
            ArrayValue::Float32(array) => {
                fortran_bytes(array).map(|bytes| (bytes, CasacoreDataType::TpFloat))
            }
            _ => None,
        };
        if let Some((bytes, data_type)) = borrowed {
            return Ok((Cow::Borrowed(bytes), data_type));
        }
    }
    encode_tiled_array(value, big_endian).map(|(bytes, data_type)| (Cow::Owned(bytes), data_type))
}

fn fortran_bytes<T: TilePixel>(array: &ArrayD<T>) -> Option<&[u8]> {
    if !is_fortran_contiguous(array.shape(), array.strides()) {
        return None;
    }
    array.as_slice_memory_order().map(tile_values_as_bytes)
}

#[cfg(test)]
mod tests {
    use casa_core::Complex32;
    use ndarray::{IxDyn, ShapeBuilder};

    use super::*;

    #[test]
    fn sparse_numeric_cells_preserve_layout_and_endianness() {
        let shape = IxDyn(&[2, 3]);
        let floats = vec![
            1.25,
            f32::from_bits(0x7fc0_1234),
            -0.0,
            4.0,
            -2.5,
            f32::INFINITY,
        ];
        let complex = floats
            .iter()
            .map(|&real| Complex32::new(real, -real))
            .collect::<Vec<_>>();
        let float_row_major = ArrayD::from_shape_vec(shape.clone(), floats.clone()).unwrap();
        let complex_row_major = ArrayD::from_shape_vec(shape.clone(), complex.clone()).unwrap();
        let float_fortran = ArrayD::from_shape_vec(shape.clone().f(), floats).unwrap();
        let complex_fortran = ArrayD::from_shape_vec(shape.f(), complex).unwrap();

        for (value, fortran) in [
            (ArrayValue::Float32(float_row_major), false),
            (ArrayValue::Complex32(complex_row_major), false),
            (ArrayValue::Float32(float_fortran), true),
            (ArrayValue::Complex32(complex_fortran), true),
        ] {
            for big_endian in [false, true] {
                let (reference, reference_type) = encode_tiled_array(&value, big_endian).unwrap();
                let (encoded, actual_type) = encode_row(&value, big_endian).unwrap();
                assert_eq!(encoded, reference);
                assert_eq!(actual_type, reference_type);
                assert_eq!(
                    matches!(encoded, Cow::Borrowed(_)),
                    fortran && big_endian == cfg!(target_endian = "big")
                );
            }
        }
    }
}
