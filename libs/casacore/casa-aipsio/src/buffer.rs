// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded, in-memory AipsIO object codec.

use std::io::Cursor;

use crate::{AipsIo, AipsIoObjectError, AipsIoObjectResult, ByteOrder, primitive_codec};

const MAGIC_VALUE: u32 = 0xbebebebe;
const MIN_OBJECT_LENGTH: u32 = 12;

fn validate_root_frame(data: &[u8], byte_order: ByteOrder) -> Result<u32, String> {
    if data.len() < 16 {
        return Err(format!("need at least 16 bytes, have {}", data.len()));
    }
    let magic =
        primitive_codec::decode_u32(data[0..4].try_into().expect("fixed-size slice"), byte_order);
    if magic != MAGIC_VALUE {
        return Err(format!("invalid magic 0x{magic:08x}"));
    }

    let declared =
        primitive_codec::decode_u32(data[4..8].try_into().expect("fixed-size slice"), byte_order);
    if declared < MIN_OBJECT_LENGTH {
        return Err(format!(
            "object length {declared} is shorter than its header"
        ));
    }
    let object_end = 4usize
        .checked_add(declared as usize)
        .ok_or_else(|| format!("object length {declared} overflows address space"))?;
    if object_end > data.len() {
        return Err(format!(
            "object length {declared} exceeds {} available bytes",
            data.len().saturating_sub(4)
        ));
    }

    let type_len = primitive_codec::decode_u32(
        data[8..12].try_into().expect("fixed-size slice"),
        byte_order,
    );
    if type_len == 0 {
        return Err("object type is empty".to_string());
    }
    let type_end = 12usize
        .checked_add(type_len as usize)
        .ok_or_else(|| format!("object type length {type_len} overflows address space"))?;
    let version_end = type_end
        .checked_add(4)
        .ok_or_else(|| "object version position overflows address space".to_string())?;
    if version_end > object_end {
        return Err(format!(
            "object type length {type_len} exceeds declared object boundary"
        ));
    }
    std::str::from_utf8(&data[12..type_end])
        .map_err(|error| format!("object type is not UTF-8: {error}"))?;
    Ok(declared)
}

/// Detect a complete root object's byte order from its framing structure.
///
/// Detection never guesses: if both byte orders form valid frames, the input
/// is rejected as ambiguous and the caller must supply external format context.
pub fn detect_aipsio_byte_order(data: &[u8]) -> AipsIoObjectResult<ByteOrder> {
    let big = validate_root_frame(data, ByteOrder::BigEndian);
    let little = validate_root_frame(data, ByteOrder::LittleEndian);
    match (big, little) {
        (Ok(_), Err(_)) => Ok(ByteOrder::BigEndian),
        (Err(_), Ok(_)) => Ok(ByteOrder::LittleEndian),
        (Ok(big_endian_length), Ok(little_endian_length)) => {
            Err(AipsIoObjectError::AmbiguousByteOrder {
                big_endian_length,
                little_endian_length,
            })
        }
        (Err(big), Err(little)) => Err(AipsIoObjectError::InvalidFrame(format!(
            "big-endian: {big}; little-endian: {little}"
        ))),
    }
}

/// In-memory adapter over the canonical framed [`AipsIo`] reader.
///
/// Construction takes one bounded copy of the supplied metadata buffer. All
/// object framing, primitive decoding, nesting, and boundary checks then run
/// through the same implementation used by stream-backed AipsIO.
pub struct AipsIoSliceReader {
    io: AipsIo,
}

impl AipsIoSliceReader {
    /// Construct a reader using byte order supplied by the containing format.
    pub fn new(data: &[u8], byte_order: ByteOrder) -> AipsIoObjectResult<Self> {
        validate_root_frame(data, byte_order).map_err(AipsIoObjectError::InvalidFrame)?;
        Ok(Self {
            io: AipsIo::new_read_only_with_order(Cursor::new(data.to_vec()), byte_order),
        })
    }

    /// Construct a reader after unambiguous structural byte-order detection.
    pub fn from_detected(data: &[u8]) -> AipsIoObjectResult<Self> {
        Self::new(data, detect_aipsio_byte_order(data)?)
    }

    /// Return the configured byte order.
    pub fn byte_order(&self) -> ByteOrder {
        self.io.byte_order()
    }

    /// Begin a type-checked object and return its persisted version.
    pub fn getstart(&mut self, expected_type: &str) -> AipsIoObjectResult<u32> {
        self.io.getstart(expected_type)
    }

    /// Finish the current object, requiring exact consumption of its frame.
    pub fn getend(&mut self) -> AipsIoObjectResult<u32> {
        self.io.getend()
    }

    /// Read a canonical boolean.
    pub fn read_bool(&mut self) -> AipsIoObjectResult<bool> {
        self.io.get_bool()
    }

    /// Read a canonical unsigned byte.
    pub fn read_u8(&mut self) -> AipsIoObjectResult<u8> {
        self.io.get_u8()
    }

    /// Read a canonical unsigned 32-bit integer.
    pub fn read_u32(&mut self) -> AipsIoObjectResult<u32> {
        self.io.get_u32()
    }

    /// Read a canonical signed 32-bit integer.
    pub fn read_i32(&mut self) -> AipsIoObjectResult<i32> {
        self.io.get_i32()
    }

    /// Read a canonical unsigned 64-bit integer.
    pub fn read_u64(&mut self) -> AipsIoObjectResult<u64> {
        self.io.get_u64()
    }

    /// Read a length-prefixed UTF-8 string.
    pub fn read_string(&mut self) -> AipsIoObjectResult<String> {
        self.io.get_string()
    }

    /// Read a nested `Block<uInt>` object.
    pub fn read_block_u32(&mut self) -> AipsIoObjectResult<Vec<u32>> {
        self.getstart("Block")?;
        let count = self.read_u32()? as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.read_u32()?);
        }
        self.getend()?;
        Ok(values)
    }

    /// Read a nested `Block<Int64>` object.
    pub fn read_block_u64(&mut self) -> AipsIoObjectResult<Vec<u64>> {
        self.getstart("Block")?;
        let count = self.read_u32()? as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.read_u64()?);
        }
        self.getend()?;
        Ok(values)
    }
}

/// Prepared in-memory writer for canonical AipsIO object frames.
pub struct AipsIoBufferWriter {
    io: AipsIo,
}

impl AipsIoBufferWriter {
    /// Create an empty writer in the byte order required by the containing format.
    pub fn new(byte_order: ByteOrder) -> Self {
        Self {
            io: AipsIo::new_write_only_with_order(Cursor::new(Vec::new()), byte_order),
        }
    }

    /// Consume the writer and return the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.io
            .into_inner_typed::<Cursor<Vec<u8>>>()
            .expect("buffer writer owns its cursor")
            .into_inner()
    }

    /// Begin an object, adding top-level magic only at nesting level zero.
    pub fn putstart(&mut self, type_name: &str, version: u32) -> AipsIoObjectResult<()> {
        self.io.putstart(type_name, version).map(|_| ())
    }

    /// Finish the current object and backpatch its declared byte length.
    pub fn putend(&mut self) -> AipsIoObjectResult<u32> {
        self.io.putend()
    }

    /// Write a canonical boolean.
    pub fn put_bool(&mut self, value: bool) {
        self.io.put_bool(value).expect("writing to Vec cannot fail");
    }

    /// Write an unsigned byte.
    pub fn put_u8(&mut self, value: u8) {
        self.io.put_u8(value).expect("writing to Vec cannot fail");
    }

    /// Write a canonical unsigned 32-bit integer.
    pub fn put_u32(&mut self, value: u32) {
        self.io.put_u32(value).expect("writing to Vec cannot fail");
    }

    /// Write a canonical signed 32-bit integer.
    pub fn put_i32(&mut self, value: i32) {
        self.io.put_i32(value).expect("writing to Vec cannot fail");
    }

    /// Write a canonical unsigned 64-bit integer.
    pub fn put_u64(&mut self, value: u64) {
        self.io.put_u64(value).expect("writing to Vec cannot fail");
    }

    /// Write a length-prefixed UTF-8 string.
    pub fn put_string(&mut self, value: &str) -> AipsIoObjectResult<()> {
        self.io.put_string(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_round_trip_in_both_byte_orders() {
        for order in [ByteOrder::BigEndian, ByteOrder::LittleEndian] {
            let mut writer = AipsIoBufferWriter::new(order);
            writer.putstart("Outer", 2).unwrap();
            writer.put_i32(-7);
            writer.putstart("Block", 1).unwrap();
            writer.put_u32(2);
            writer.put_u64(11);
            writer.put_u64(13);
            writer.putend().unwrap();
            writer.putend().unwrap();
            let bytes = writer.into_bytes();

            assert_eq!(detect_aipsio_byte_order(&bytes).unwrap(), order);
            let mut reader = AipsIoSliceReader::from_detected(&bytes).unwrap();
            assert_eq!(reader.getstart("Outer").unwrap(), 2);
            assert_eq!(reader.read_i32().unwrap(), -7);
            assert_eq!(reader.read_block_u64().unwrap(), vec![11, 13]);
            reader.getend().unwrap();
        }
    }

    #[test]
    fn rejects_truncated_and_incompletely_consumed_frames() {
        let mut writer = AipsIoBufferWriter::new(ByteOrder::BigEndian);
        writer.putstart("Object", 1).unwrap();
        writer.put_u32(42);
        writer.putend().unwrap();
        let bytes = writer.into_bytes();

        assert!(matches!(
            AipsIoSliceReader::from_detected(&bytes[..bytes.len() - 1]),
            Err(AipsIoObjectError::InvalidFrame(_))
        ));
        let mut reader = AipsIoSliceReader::from_detected(&bytes).unwrap();
        reader.getstart("Object").unwrap();
        assert!(matches!(
            reader.getend(),
            Err(AipsIoObjectError::IncompleteObjectRead)
        ));
    }

    #[test]
    fn rejects_structurally_ambiguous_byte_order() {
        let declared = 0x0001_0100_u32;
        let mut bytes = vec![b'a'; 4 + declared as usize];
        bytes[0..4].copy_from_slice(&MAGIC_VALUE.to_be_bytes());
        bytes[4..8].copy_from_slice(&[0, 1, 1, 0]);
        bytes[8..12].copy_from_slice(&[0, 0, 1, 0]);

        assert!(matches!(
            detect_aipsio_byte_order(&bytes),
            Err(AipsIoObjectError::AmbiguousByteOrder { .. })
        ));
    }
}
