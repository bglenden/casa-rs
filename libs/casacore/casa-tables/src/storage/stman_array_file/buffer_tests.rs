// SPDX-License-Identifier: LGPL-3.0-or-later
//! Buffer cursor, byte compatibility and recovery regressions.

use super::*;
use tempfile::TempDir;

#[test]
fn empty_string_records_keep_buffers_and_canonical_offsets() {
    for big_endian in [false, true] {
        for version in [0, 1] {
            let root = TempDir::new().unwrap();
            let path = root.path().join("empty-strings.arr");
            let text = Value::Array(ArrayValue::from_string_vec(vec![String::new()]));
            let number = Value::Array(ArrayValue::from_f32_vec(vec![2.]));
            let mut writer = StManArrayFileWriter::create(&path, big_endian, version).unwrap();
            let first = writer
                .write_array(&text, CasacoreDataType::TpString)
                .unwrap();
            // A stream-position query/seek would have flushed this buffer.
            assert!(!writer.file.buffer().is_empty());
            assert_eq!(
                writer.sequential_end,
                Some(if version == 0 { 28 } else { 32 })
            );
            let second = writer
                .write_array(&number, CasacoreDataType::TpFloat)
                .unwrap();
            assert_eq!((first, second), (16, 32));
            writer.finish().unwrap();
            drop(writer);
            let mut reader = StManArrayFileReader::open(&path, big_endian).unwrap();
            assert_eq!(
                reader
                    .read_array_at(first, CasacoreDataType::TpString)
                    .unwrap(),
                Some(text)
            );
            assert_eq!(reader.pos, if version == 0 { 28 } else { 32 });
            assert!(!reader.file.buffer().is_empty());
            assert_eq!(
                reader
                    .read_array_at(second, CasacoreDataType::TpFloat)
                    .unwrap(),
                Some(number)
            );
            // In v0 the four padding bytes can contain a separately stored
            // zero-length string. A nonzero offset must still invalidate the
            // cursor, despite the resulting value also being an empty string.
            if version == 0 {
                let mut file = OpenOptions::new().write(true).open(&path).unwrap();
                file.seek(SeekFrom::Start(24)).unwrap();
                write_u32(
                    &mut file,
                    28,
                    if big_endian {
                        ArrayFileByteOrder::Big
                    } else {
                        ArrayFileByteOrder::Little
                    },
                )
                .unwrap();
                drop(file);
                let mut reader = StManArrayFileReader::open(&path, big_endian).unwrap();
                reader
                    .read_array_at(first, CasacoreDataType::TpString)
                    .unwrap();
                assert_eq!(reader.pos, u64::MAX);
                assert_eq!(
                    reader
                        .read_array_at(second, CasacoreDataType::TpFloat)
                        .unwrap(),
                    Some(Value::Array(ArrayValue::from_f32_vec(vec![2.])))
                );
            }
        }
    }
}

#[test]
fn malformed_shape_does_not_poison_the_next_aligned_read() {
    for big_endian in [false, true] {
        for version in [0, 1] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("recover.arr");
            let value = Value::Array(ArrayValue::from_bool_vec(vec![true]));
            let mut writer = StManArrayFileWriter::create(&path, big_endian, version).unwrap();
            let first = writer
                .write_array(&value, CasacoreDataType::TpBool)
                .unwrap();
            let second = writer
                .write_array(&value, CasacoreDataType::TpBool)
                .unwrap();
            writer.finish().unwrap();
            drop(writer);
            let bo = if big_endian {
                ArrayFileByteOrder::Big
            } else {
                ArrayFileByteOrder::Little
            };
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(64)).unwrap();
            if version > 0 {
                write_u32(&mut file, 1, bo).unwrap();
            }
            write_u32(&mut file, 1, bo).unwrap();
            write_i32(&mut file, -1, bo).unwrap();
            let mut reader = StManArrayFileReader::open(&path, big_endian).unwrap();
            assert_eq!(
                reader
                    .read_array_at(first, CasacoreDataType::TpBool)
                    .unwrap(),
                Some(value.clone())
            );
            assert!(reader.read_shape_at(64).is_err());
            assert_eq!(
                reader
                    .read_array_at(second, CasacoreDataType::TpBool)
                    .unwrap(),
                Some(value)
            );
        }
    }
}

#[test]
fn truncated_payload_does_not_poison_the_next_aligned_read() {
    for big_endian in [false, true] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("payload.arr");
        let value = Value::Array(ArrayValue::from_bool_vec(vec![true, false]));
        let mut writer = StManArrayFileWriter::create(&path, big_endian, 1).unwrap();
        let first = writer
            .write_array(&value, CasacoreDataType::TpBool)
            .unwrap();
        let second = writer
            .write_array(&value, CasacoreDataType::TpBool)
            .unwrap();
        writer.finish().unwrap();
        drop(writer);
        let mut reader = StManArrayFileReader::open(&path, big_endian).unwrap();
        // A corrupted dtype requests more payload than this file contains.
        assert!(
            reader
                .read_array_at(first, CasacoreDataType::TpDComplex)
                .is_err()
        );
        assert_eq!(
            reader
                .read_array_at(second, CasacoreDataType::TpBool)
                .unwrap(),
            Some(value)
        );
    }
}

#[test]
fn reopening_after_string_payload_appends_without_overwriting_existing_records() {
    for big_endian in [false, true] {
        for version in [0, 1] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("append.arr");
            let text = Value::Array(ArrayValue::from_string_vec(vec![
                "alpha".into(),
                String::new(),
            ]));
            let number = Value::Array(ArrayValue::from_f64_vec(vec![3.]));
            let mut writer = StManArrayFileWriter::create(&path, big_endian, version).unwrap();
            let first = writer
                .write_array(&text, CasacoreDataType::TpString)
                .unwrap();
            writer.finish().unwrap();
            drop(writer);
            let mut writer = StManArrayFileWriter::open_append(&path, big_endian).unwrap();
            let second = writer
                .write_array(&number, CasacoreDataType::TpDouble)
                .unwrap();
            writer.finish().unwrap();
            drop(writer);
            let mut reader = StManArrayFileReader::open(&path, big_endian).unwrap();
            assert_eq!(
                reader
                    .read_array_at(first, CasacoreDataType::TpString)
                    .unwrap(),
                Some(text)
            );
            assert_eq!(
                reader
                    .read_array_at(second, CasacoreDataType::TpDouble)
                    .unwrap(),
                Some(number)
            );
        }
    }
}

#[test]
fn aligned_numeric_records_keep_canonical_bytes_across_header_flush() {
    // Canonical v0 records at 16, 32 and 48: one float, three packed
    // booleans, one double. Padding is zero, as in a newly extended file.
    for (big_endian, hex) in [
        (
            false,
            "00000000 4000000000000000 00000000 01000000 01000000 00000040 00000000 01000000 03000000 05 00000000000000 01000000 01000000 0000000000000840",
        ),
        (
            true,
            "00000000 0000000000000040 00000000 00000001 00000001 40000000 00000000 00000001 00000003 05 00000000000000 00000001 00000001 4008000000000000",
        ),
    ] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("canonical.arr");
        let values = [
            (
                Value::Array(ArrayValue::from_f32_vec(vec![2.])),
                CasacoreDataType::TpFloat,
            ),
            (
                Value::Array(ArrayValue::from_bool_vec(vec![true, false, true])),
                CasacoreDataType::TpBool,
            ),
            (
                Value::Array(ArrayValue::from_f64_vec(vec![3.])),
                CasacoreDataType::TpDouble,
            ),
        ];
        let mut writer = StManArrayFileWriter::create(&path, big_endian, 0).unwrap();
        for (index, (value, dtype)) in values.iter().enumerate() {
            assert_eq!(
                writer.write_array(value, *dtype).unwrap(),
                [16, 32, 48][index]
            );
            if index == 0 {
                writer.finish().unwrap();
            }
        }
        writer.finish().unwrap();
        let expected = hex
            .split_whitespace()
            .flat_map(|word| {
                word.as_bytes()
                    .chunks(2)
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            })
            .collect::<Vec<_>>();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        let mut reader = StManArrayFileReader::open(&path, big_endian).unwrap();
        for ((value, dtype), offset) in values.iter().zip([16, 32, 48]) {
            assert_eq!(
                reader.read_array_at(offset, *dtype).unwrap().as_ref(),
                Some(value)
            );
        }
    }
}

#[test]
fn buffered_alignment_preserves_string_indirection_and_refcount_versions() {
    for big_endian in [false, true] {
        for version in [0, 1] {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("mixed.arr");
            let values = [
                (
                    Value::Array(ArrayValue::from_f32_vec(vec![2.5])),
                    CasacoreDataType::TpFloat,
                ),
                (
                    Value::Array(ArrayValue::from_string_vec(vec![
                        String::new(),
                        "alpha".into(),
                    ])),
                    CasacoreDataType::TpString,
                ),
                (
                    Value::Array(ArrayValue::from_complex32_vec(vec![
                        casa_core::Complex32::new(2., 3.),
                    ])),
                    CasacoreDataType::TpComplex,
                ),
                (
                    Value::Array(ArrayValue::from_bool_vec(vec![true, false, true])),
                    CasacoreDataType::TpBool,
                ),
            ];
            let mut writer = StManArrayFileWriter::create(&path, big_endian, version).unwrap();
            let offsets = values
                .iter()
                .map(|(value, dtype)| writer.write_array(value, *dtype).unwrap())
                .collect::<Vec<_>>();
            writer.finish().unwrap();
            let mut reader = StManArrayFileReader::open(&path, big_endian).unwrap();
            for ((value, dtype), offset) in values.iter().zip(offsets) {
                assert_eq!(
                    reader.read_array_at(offset, *dtype).unwrap().as_ref(),
                    Some(value)
                );
            }
        }
    }
}
