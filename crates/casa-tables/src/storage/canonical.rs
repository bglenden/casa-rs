// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical (big-endian) byte conversion for StandardStMan bucket data.
//!
//! SSM stores scalar data directly in bucket byte arrays using canonical
//! (big-endian) byte order, not via AipsIO framing. Booleans are bit-packed.

use super::data_type::CasacoreDataType;

/// Returns the canonical storage size for one element of the given type.
///
/// Returns `(bytes, bits)`. For most types `bits == bytes * 8`.
/// For Bool, `bytes == 0, bits == 1` (bit-packed).
/// For String (variable-length), returns the 3-Int reference size (12 bytes).
pub(crate) fn canonical_element_size(dt: CasacoreDataType) -> (usize, usize) {
    match dt {
        CasacoreDataType::TpBool => (0, 1),
        CasacoreDataType::TpUChar | CasacoreDataType::TpChar => (1, 8),
        CasacoreDataType::TpShort | CasacoreDataType::TpUShort => (2, 16),
        CasacoreDataType::TpInt | CasacoreDataType::TpUInt | CasacoreDataType::TpFloat => (4, 32),
        CasacoreDataType::TpDouble | CasacoreDataType::TpInt64 | CasacoreDataType::TpComplex => {
            (8, 64)
        }
        CasacoreDataType::TpDComplex => (16, 128),
        // Variable-length string: 3 canonical Ints (bucketNr, offset, length)
        CasacoreDataType::TpString => (12, 96),
        _ => (0, 0),
    }
}

// ---------------------------------------------------------------------------
// Big-endian scalar reads
// ---------------------------------------------------------------------------

pub(crate) fn read_i16_be(src: &[u8]) -> i16 {
    i16::from_be_bytes([src[0], src[1]])
}

pub(crate) fn read_u16_be(src: &[u8]) -> u16 {
    u16::from_be_bytes([src[0], src[1]])
}

pub(crate) fn read_i32_be(src: &[u8]) -> i32 {
    i32::from_be_bytes([src[0], src[1], src[2], src[3]])
}

pub(crate) fn read_u32_be(src: &[u8]) -> u32 {
    u32::from_be_bytes([src[0], src[1], src[2], src[3]])
}

pub(crate) fn read_i64_be(src: &[u8]) -> i64 {
    i64::from_be_bytes([
        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
    ])
}

pub(crate) fn read_f32_be(src: &[u8]) -> f32 {
    f32::from_be_bytes([src[0], src[1], src[2], src[3]])
}

pub(crate) fn read_f64_be(src: &[u8]) -> f64 {
    f64::from_be_bytes([
        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
    ])
}

// ---------------------------------------------------------------------------
// Big-endian scalar writes
// ---------------------------------------------------------------------------

pub(crate) fn write_i16_be(dst: &mut [u8], val: i16) {
    dst[..2].copy_from_slice(&val.to_be_bytes());
}

pub(crate) fn write_u16_be(dst: &mut [u8], val: u16) {
    dst[..2].copy_from_slice(&val.to_be_bytes());
}

pub(crate) fn write_i32_be(dst: &mut [u8], val: i32) {
    dst[..4].copy_from_slice(&val.to_be_bytes());
}

pub(crate) fn write_u32_be(dst: &mut [u8], val: u32) {
    dst[..4].copy_from_slice(&val.to_be_bytes());
}

pub(crate) fn write_i64_be(dst: &mut [u8], val: i64) {
    dst[..8].copy_from_slice(&val.to_be_bytes());
}

pub(crate) fn write_f32_be(dst: &mut [u8], val: f32) {
    dst[..4].copy_from_slice(&val.to_be_bytes());
}

pub(crate) fn write_f64_be(dst: &mut [u8], val: f64) {
    dst[..8].copy_from_slice(&val.to_be_bytes());
}

// ---------------------------------------------------------------------------
// Little-endian scalar writes
// ---------------------------------------------------------------------------

pub(crate) fn write_i16_le(dst: &mut [u8], val: i16) {
    dst[..2].copy_from_slice(&val.to_le_bytes());
}

pub(crate) fn write_u16_le(dst: &mut [u8], val: u16) {
    dst[..2].copy_from_slice(&val.to_le_bytes());
}

pub(crate) fn write_i32_le(dst: &mut [u8], val: i32) {
    dst[..4].copy_from_slice(&val.to_le_bytes());
}

pub(crate) fn write_u32_le(dst: &mut [u8], val: u32) {
    dst[..4].copy_from_slice(&val.to_le_bytes());
}

pub(crate) fn write_i64_le(dst: &mut [u8], val: i64) {
    dst[..8].copy_from_slice(&val.to_le_bytes());
}

pub(crate) fn write_f32_le(dst: &mut [u8], val: f32) {
    dst[..4].copy_from_slice(&val.to_le_bytes());
}

pub(crate) fn write_f64_le(dst: &mut [u8], val: f64) {
    dst[..8].copy_from_slice(&val.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Bool bit-packing
// ---------------------------------------------------------------------------

/// Unpack `count` boolean values from `src` starting at `bit_offset`.
///
/// Uses LSB-first bit order within each byte, matching C++ casacore's
/// `Conversion::bitToBool`.
pub(crate) fn read_bool_bits(src: &[u8], bit_offset: usize, count: usize) -> Vec<bool> {
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let bit = bit_offset + i;
        let byte_idx = bit / 8;
        let bit_idx = bit % 8;
        result.push((src[byte_idx] >> bit_idx) & 1 != 0);
    }
    result
}

/// Pack boolean values into `dst` starting at `bit_offset`.
///
/// Uses LSB-first bit order within each byte, matching C++ casacore's
/// `Conversion::boolToBit`.
pub(crate) fn write_bool_bits(dst: &mut [u8], bit_offset: usize, values: &[bool]) {
    for (i, &val) in values.iter().enumerate() {
        let bit = bit_offset + i;
        let byte_idx = bit / 8;
        let bit_idx = bit % 8;
        if val {
            dst[byte_idx] |= 1 << bit_idx;
        } else {
            dst[byte_idx] &= !(1 << bit_idx);
        }
    }
}

// ---------------------------------------------------------------------------
// Little-endian scalar reads
// ---------------------------------------------------------------------------

pub(crate) fn read_i16_le(src: &[u8]) -> i16 {
    i16::from_le_bytes([src[0], src[1]])
}

pub(crate) fn read_u16_le(src: &[u8]) -> u16 {
    u16::from_le_bytes([src[0], src[1]])
}

pub(crate) fn read_i32_le(src: &[u8]) -> i32 {
    i32::from_le_bytes([src[0], src[1], src[2], src[3]])
}

pub(crate) fn read_u32_le(src: &[u8]) -> u32 {
    u32::from_le_bytes([src[0], src[1], src[2], src[3]])
}

pub(crate) fn read_f32_le(src: &[u8]) -> f32 {
    f32::from_le_bytes([src[0], src[1], src[2], src[3]])
}

pub(crate) fn read_i64_le(src: &[u8]) -> i64 {
    i64::from_le_bytes([
        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
    ])
}

pub(crate) fn read_f64_le(src: &[u8]) -> f64 {
    f64::from_le_bytes([
        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
    ])
}

// ---------------------------------------------------------------------------
// Bulk slice reads (big-endian)
// ---------------------------------------------------------------------------

pub(crate) fn read_i16_slice_be(src: &[u8], count: usize) -> Vec<i16> {
    (0..count).map(|i| read_i16_be(&src[i * 2..])).collect()
}

pub(crate) fn read_u16_slice_be(src: &[u8], count: usize) -> Vec<u16> {
    (0..count).map(|i| read_u16_be(&src[i * 2..])).collect()
}

pub(crate) fn read_i32_slice_be(src: &[u8], count: usize) -> Vec<i32> {
    (0..count).map(|i| read_i32_be(&src[i * 4..])).collect()
}

pub(crate) fn read_u32_slice_be(src: &[u8], count: usize) -> Vec<u32> {
    (0..count).map(|i| read_u32_be(&src[i * 4..])).collect()
}

pub(crate) fn read_i64_slice_be(src: &[u8], count: usize) -> Vec<i64> {
    (0..count).map(|i| read_i64_be(&src[i * 8..])).collect()
}

pub(crate) fn read_f32_slice_be(src: &[u8], count: usize) -> Vec<f32> {
    (0..count).map(|i| read_f32_be(&src[i * 4..])).collect()
}

pub(crate) fn read_f64_slice_be(src: &[u8], count: usize) -> Vec<f64> {
    (0..count).map(|i| read_f64_be(&src[i * 8..])).collect()
}

// ---------------------------------------------------------------------------
// Bulk slice reads (little-endian)
// ---------------------------------------------------------------------------

pub(crate) fn read_i16_slice_le(src: &[u8], count: usize) -> Vec<i16> {
    (0..count).map(|i| read_i16_le(&src[i * 2..])).collect()
}

pub(crate) fn read_u16_slice_le(src: &[u8], count: usize) -> Vec<u16> {
    (0..count).map(|i| read_u16_le(&src[i * 2..])).collect()
}

pub(crate) fn read_i32_slice_le(src: &[u8], count: usize) -> Vec<i32> {
    (0..count).map(|i| read_i32_le(&src[i * 4..])).collect()
}

pub(crate) fn read_u32_slice_le(src: &[u8], count: usize) -> Vec<u32> {
    (0..count).map(|i| read_u32_le(&src[i * 4..])).collect()
}

pub(crate) fn read_i64_slice_le(src: &[u8], count: usize) -> Vec<i64> {
    (0..count).map(|i| read_i64_le(&src[i * 8..])).collect()
}

pub(crate) fn read_f32_slice_le(src: &[u8], count: usize) -> Vec<f32> {
    (0..count).map(|i| read_f32_le(&src[i * 4..])).collect()
}

pub(crate) fn read_f64_slice_le(src: &[u8], count: usize) -> Vec<f64> {
    (0..count).map(|i| read_f64_le(&src[i * 8..])).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_element_sizes_match_casacore() {
        assert_eq!(canonical_element_size(CasacoreDataType::TpBool), (0, 1));
        assert_eq!(canonical_element_size(CasacoreDataType::TpUChar), (1, 8));
        assert_eq!(canonical_element_size(CasacoreDataType::TpShort), (2, 16));
        assert_eq!(canonical_element_size(CasacoreDataType::TpInt), (4, 32));
        assert_eq!(canonical_element_size(CasacoreDataType::TpFloat), (4, 32));
        assert_eq!(canonical_element_size(CasacoreDataType::TpDouble), (8, 64));
        assert_eq!(canonical_element_size(CasacoreDataType::TpInt64), (8, 64));
        assert_eq!(canonical_element_size(CasacoreDataType::TpComplex), (8, 64));
        assert_eq!(
            canonical_element_size(CasacoreDataType::TpDComplex),
            (16, 128)
        );
        assert_eq!(canonical_element_size(CasacoreDataType::TpString), (12, 96));
    }

    #[test]
    fn i32_round_trip() {
        let mut buf = [0u8; 4];
        write_i32_be(&mut buf, -42);
        assert_eq!(read_i32_be(&buf), -42);
    }

    #[test]
    fn f64_round_trip() {
        let mut buf = [0u8; 8];
        write_f64_be(&mut buf, std::f64::consts::PI);
        assert_eq!(read_f64_be(&buf), std::f64::consts::PI);
    }

    #[test]
    fn bool_bit_packing_round_trip() {
        let values = vec![true, false, true, true, false, false, true, false, true];
        let byte_count = values.len().div_ceil(8);
        let mut buf = vec![0u8; byte_count];
        write_bool_bits(&mut buf, 0, &values);
        let read_back = read_bool_bits(&buf, 0, values.len());
        assert_eq!(read_back, values);
    }

    #[test]
    fn bool_bit_packing_with_offset() {
        let mut buf = vec![0u8; 2];
        let values = vec![true, false, true];
        write_bool_bits(&mut buf, 3, &values);
        let read_back = read_bool_bits(&buf, 3, 3);
        assert_eq!(read_back, values);
    }

    #[test]
    fn bulk_i32_slice_read() {
        let data: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x01, // 1
            0xFF, 0xFF, 0xFF, 0xFF, // -1
            0x00, 0x00, 0x00, 0x2A, // 42
        ];
        assert_eq!(read_i32_slice_be(&data, 3), vec![1, -1, 42]);
    }

    #[test]
    fn bulk_f64_slice_read() {
        let mut data = vec![0u8; 16];
        write_f64_be(&mut data[0..8], 1.5);
        write_f64_be(&mut data[8..16], -99.5);
        assert_eq!(read_f64_slice_be(&data, 2), vec![1.5, -99.5]);
    }
}
