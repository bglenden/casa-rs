// SPDX-License-Identifier: LGPL-3.0-or-later
//! Decode fixed-shape cells into reusable typed buffers, without an ndarray.

use super::*;
use casa_core::{Complex32, Complex64};

pub(crate) trait CanonicalArrayElement: Copy + Default {
    const BITS: usize;
    fn read(
        file: &mut impl Read,
        order: ArrayFileByteOrder,
        values: &mut [Self],
    ) -> Result<(), StorageError>;
}

impl CanonicalArrayElement for bool {
    const BITS: usize = 1;
    fn read(
        file: &mut impl Read,
        _: ArrayFileByteOrder,
        values: &mut [Self],
    ) -> Result<(), StorageError> {
        for chunk in values.chunks_mut(8) {
            let mut byte = [0];
            file.read_exact(&mut byte)?;
            for (bit, value) in chunk.iter_mut().enumerate() {
                *value = byte[0] & (1 << bit) != 0;
            }
        }
        Ok(())
    }
}

macro_rules! real {
    ($type:ty, $bits:expr, $read:ident) => {
        impl CanonicalArrayElement for $type {
            const BITS: usize = $bits;
            fn read(
                file: &mut impl Read,
                order: ArrayFileByteOrder,
                values: &mut [Self],
            ) -> Result<(), StorageError> {
                for value in values {
                    *value = $read(file, order)?;
                }
                Ok(())
            }
        }
    };
}
real!(f32, 32, read_f32);
real!(f64, 64, read_f64);

macro_rules! complex {
    ($type:ty, $bits:expr, $read:ident) => {
        impl CanonicalArrayElement for $type {
            const BITS: usize = $bits;
            fn read(
                file: &mut impl Read,
                order: ArrayFileByteOrder,
                values: &mut [Self],
            ) -> Result<(), StorageError> {
                for value in values {
                    *value = Self::new($read(file, order)?, $read(file, order)?);
                }
                Ok(())
            }
        }
    };
}
complex!(Complex32, 64, read_f32);
complex!(Complex64, 128, read_f64);

impl StManArrayFileReader {
    /// Append one whole cell after checking its stored shape. A different or
    /// undefined shape returns false before allocation or payload decoding.
    /// Failed I/O invalidates the cursor as the general array reader does.
    pub(crate) fn append_fixed_shape<T: CanonicalArrayElement>(
        &mut self,
        offset: i64,
        expected: &[usize],
        shape: &mut Vec<usize>,
        values: &mut Vec<T>,
    ) -> Result<bool, StorageError> {
        let count = expected
            .iter()
            .try_fold(1usize, |size, &dim| size.checked_mul(dim))
            .ok_or_else(|| StorageError::FormatMismatch("fixed array size overflow".into()))?;
        let bytes = count
            .checked_mul(T::BITS)
            .ok_or_else(|| {
                StorageError::FormatMismatch("fixed array payload size overflow".into())
            })?
            .div_ceil(8);
        if !self.read_shape_into(offset, shape)? || shape.as_slice() != expected {
            return Ok(false);
        }
        let start = values.len();
        values.try_reserve(count).map_err(|error| {
            StorageError::FormatMismatch(format!("cannot allocate fixed array payload: {error}"))
        })?;
        values.resize(start + count, T::default());
        let position = std::mem::replace(&mut self.pos, u64::MAX);
        T::read(&mut self.file, self.bo, &mut values[start..])?;
        self.pos = position + bytes as u64;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncated_typed_payload_does_not_poison_the_next_read() {
        for big_endian in [false, true] {
            for version in [0, 1] {
                let root = tempfile::tempdir().unwrap();
                let path = root.path().join("typed.arr");
                let value = Value::Array(ArrayValue::from_bool_vec(vec![true, false]));
                let mut writer = StManArrayFileWriter::create(&path, big_endian, version).unwrap();
                let first = writer
                    .write_array(&value, CasacoreDataType::TpBool)
                    .unwrap();
                let second = writer
                    .write_array(&value, CasacoreDataType::TpBool)
                    .unwrap();
                writer.finish().unwrap();
                drop(writer);
                let mut reader = StManArrayFileReader::open(&path, big_endian).unwrap();
                let mut shape = Vec::new();
                assert!(
                    reader
                        .append_fixed_shape(first, &[2], &mut shape, &mut Vec::<Complex64>::new())
                        .is_err()
                );
                let mut found = Vec::<bool>::new();
                assert!(
                    reader
                        .append_fixed_shape(second, &[2], &mut shape, &mut found)
                        .unwrap()
                );
                assert_eq!(found, [true, false]);
            }
        }
    }
}
