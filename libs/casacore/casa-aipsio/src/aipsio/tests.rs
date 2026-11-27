// SPDX-License-Identifier: LGPL-3.0-or-later

use super::{AipsIo, AipsIoObjectError, AipsOpenOption, MAGIC_VALUE};
use crate::{ArrayValue, Complex32, Complex64, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};
use std::io::Cursor;
use std::path::PathBuf;

#[test]
fn scalar_put_get_methods_round_trip() {
    let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
    writer.putstart("scalars", 7).expect("putstart");
    writer.put_bool(true).expect("put bool");
    writer.put_i8(-3).expect("put i8");
    writer.put_u8(250).expect("put u8");
    writer.put_i16(-1234).expect("put i16");
    writer.put_u16(54321).expect("put u16");
    writer.put_i32(-1_234_567).expect("put i32");
    writer.put_u32(3_456_789).expect("put u32");
    writer.put_i64(-9_876_543_210).expect("put i64");
    writer.put_u64(9_876_543_210).expect("put u64");
    writer.put_f32(3.25).expect("put f32");
    writer.put_f64(-10.5).expect("put f64");
    writer
        .put_complex32(Complex32::new(1.5, -2.5))
        .expect("put complex32");
    writer
        .put_complex64(Complex64::new(10.25, -0.75))
        .expect("put complex64");
    writer.put_string("alpha").expect("put string");
    writer.putend().expect("putend");

    let bytes = writer
        .into_inner_typed::<Cursor<Vec<u8>>>()
        .expect("writer should be cursor")
        .into_inner();

    let mut reader = AipsIo::new_read_only(Cursor::new(bytes));
    assert_eq!(reader.get_next_type().expect("get type"), "scalars");
    assert_eq!(reader.getstart("scalars").expect("getstart"), 7);
    assert!(reader.get_bool().expect("get bool"));
    assert_eq!(reader.get_i8().expect("get i8"), -3);
    assert_eq!(reader.get_u8().expect("get u8"), 250);
    assert_eq!(reader.get_i16().expect("get i16"), -1234);
    assert_eq!(reader.get_u16().expect("get u16"), 54321);
    assert_eq!(reader.get_i32().expect("get i32"), -1_234_567);
    assert_eq!(reader.get_u32().expect("get u32"), 3_456_789);
    assert_eq!(reader.get_i64().expect("get i64"), -9_876_543_210);
    assert_eq!(reader.get_u64().expect("get u64"), 9_876_543_210);
    assert_eq!(reader.get_f32().expect("get f32"), 3.25);
    assert_eq!(reader.get_f64().expect("get f64"), -10.5);
    assert_eq!(
        reader.get_complex32().expect("get complex32"),
        Complex32::new(1.5, -2.5)
    );
    assert_eq!(
        reader.get_complex64().expect("get complex64"),
        Complex64::new(10.25, -0.75)
    );
    assert_eq!(reader.get_string().expect("get string"), "alpha");
    reader.getend().expect("getend");
}

#[test]
fn array_put_get_into_and_getnew_methods_round_trip() {
    let bools = vec![true, false, true, true, false, false, true, false, true];
    let i8s = vec![-5_i8, 0, 7];
    let u8s = vec![1_u8, 200, 255];
    let i16s = vec![-300_i16, 0, 1200];
    let u16s = vec![1_u16, 65535, 32];
    let i32s = vec![-1_000_000_i32, 1, 2];
    let u32s = vec![0_u32, 1_000_000, 4_000_000_000_u32];
    let i64s = vec![-10_i64, 0, 123_456_789_012];
    let u64s = vec![0_u64, 9_999_999_999, 42];
    let f32s = vec![1.25_f32, -2.5, 3.75];
    let f64s = vec![1.25_f64, -2.5, 3.75];
    let c32s = vec![Complex32::new(1.0, -1.0), Complex32::new(2.5, -3.5)];
    let c64s = vec![Complex64::new(1.0, -1.0), Complex64::new(2.5, -3.5)];
    let strings = vec!["aa".to_string(), "".to_string(), "xyz".to_string()];

    let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
    writer.putstart("into", 1).expect("putstart into");
    writer
        .put_bool_slice(&bools, false)
        .expect("put bool slice");
    writer.put_i8_slice(&i8s, false).expect("put i8 slice");
    writer.put_u8_slice(&u8s, false).expect("put u8 slice");
    writer.put_i16_slice(&i16s, false).expect("put i16 slice");
    writer.put_u16_slice(&u16s, false).expect("put u16 slice");
    writer.put_i32_slice(&i32s, false).expect("put i32 slice");
    writer.put_u32_slice(&u32s, false).expect("put u32 slice");
    writer.put_i64_slice(&i64s, false).expect("put i64 slice");
    writer.put_u64_slice(&u64s, false).expect("put u64 slice");
    writer.put_f32_slice(&f32s, false).expect("put f32 slice");
    writer.put_f64_slice(&f64s, false).expect("put f64 slice");
    writer
        .put_complex32_slice(&c32s, false)
        .expect("put complex32 slice");
    writer
        .put_complex64_slice(&c64s, false)
        .expect("put complex64 slice");
    writer
        .put_string_slice(&strings, false)
        .expect("put string slice");
    writer.putend().expect("putend into");

    writer.putstart("getnew", 1).expect("putstart getnew");
    writer.put_bool_slice(&bools, true).expect("put bool slice");
    writer.put_i8_slice(&i8s, true).expect("put i8 slice");
    writer.put_u8_slice(&u8s, true).expect("put u8 slice");
    writer.put_i16_slice(&i16s, true).expect("put i16 slice");
    writer.put_u16_slice(&u16s, true).expect("put u16 slice");
    writer.put_i32_slice(&i32s, true).expect("put i32 slice");
    writer.put_u32_slice(&u32s, true).expect("put u32 slice");
    writer.put_i64_slice(&i64s, true).expect("put i64 slice");
    writer.put_u64_slice(&u64s, true).expect("put u64 slice");
    writer.put_f32_slice(&f32s, true).expect("put f32 slice");
    writer.put_f64_slice(&f64s, true).expect("put f64 slice");
    writer
        .put_complex32_slice(&c32s, true)
        .expect("put complex32 slice");
    writer
        .put_complex64_slice(&c64s, true)
        .expect("put complex64 slice");
    writer
        .put_string_slice(&strings, true)
        .expect("put string slice");
    writer.putend().expect("putend getnew");

    let bytes = writer
        .into_inner_typed::<Cursor<Vec<u8>>>()
        .expect("writer should be cursor")
        .into_inner();

    let mut reader = AipsIo::new_read_only(Cursor::new(bytes));

    assert_eq!(reader.getstart("into").expect("getstart into"), 1);
    let mut bools_out = vec![false; bools.len()];
    reader.get_bool_into(&mut bools_out).expect("get bool into");
    assert_eq!(bools_out, bools);

    let mut i8s_out = vec![0_i8; i8s.len()];
    reader.get_i8_into(&mut i8s_out).expect("get i8 into");
    assert_eq!(i8s_out, i8s);
    let mut u8s_out = vec![0_u8; u8s.len()];
    reader.get_u8_into(&mut u8s_out).expect("get u8 into");
    assert_eq!(u8s_out, u8s);
    let mut i16s_out = vec![0_i16; i16s.len()];
    reader.get_i16_into(&mut i16s_out).expect("get i16 into");
    assert_eq!(i16s_out, i16s);
    let mut u16s_out = vec![0_u16; u16s.len()];
    reader.get_u16_into(&mut u16s_out).expect("get u16 into");
    assert_eq!(u16s_out, u16s);
    let mut i32s_out = vec![0_i32; i32s.len()];
    reader.get_i32_into(&mut i32s_out).expect("get i32 into");
    assert_eq!(i32s_out, i32s);
    let mut u32s_out = vec![0_u32; u32s.len()];
    reader.get_u32_into(&mut u32s_out).expect("get u32 into");
    assert_eq!(u32s_out, u32s);
    let mut i64s_out = vec![0_i64; i64s.len()];
    reader.get_i64_into(&mut i64s_out).expect("get i64 into");
    assert_eq!(i64s_out, i64s);
    let mut u64s_out = vec![0_u64; u64s.len()];
    reader.get_u64_into(&mut u64s_out).expect("get u64 into");
    assert_eq!(u64s_out, u64s);
    let mut f32s_out = vec![0_f32; f32s.len()];
    reader.get_f32_into(&mut f32s_out).expect("get f32 into");
    assert_eq!(f32s_out, f32s);
    let mut f64s_out = vec![0_f64; f64s.len()];
    reader.get_f64_into(&mut f64s_out).expect("get f64 into");
    assert_eq!(f64s_out, f64s);
    let mut c32s_out = vec![Complex32::new(0.0, 0.0); c32s.len()];
    reader
        .get_complex32_into(&mut c32s_out)
        .expect("get complex32 into");
    assert_eq!(c32s_out, c32s);
    let mut c64s_out = vec![Complex64::new(0.0, 0.0); c64s.len()];
    reader
        .get_complex64_into(&mut c64s_out)
        .expect("get complex64 into");
    assert_eq!(c64s_out, c64s);
    let mut strings_out = vec![String::new(); strings.len()];
    reader
        .get_string_into(&mut strings_out)
        .expect("get string into");
    assert_eq!(strings_out, strings);
    reader.getend().expect("getend into");

    assert_eq!(reader.getstart("getnew").expect("getstart getnew"), 1);
    assert_eq!(reader.getnew_bool().expect("getnew bool"), bools);
    assert_eq!(reader.getnew_i8().expect("getnew i8"), i8s);
    assert_eq!(reader.getnew_u8().expect("getnew u8"), u8s);
    assert_eq!(reader.getnew_i16().expect("getnew i16"), i16s);
    assert_eq!(reader.getnew_u16().expect("getnew u16"), u16s);
    assert_eq!(reader.getnew_i32().expect("getnew i32"), i32s);
    assert_eq!(reader.getnew_u32().expect("getnew u32"), u32s);
    assert_eq!(reader.getnew_i64().expect("getnew i64"), i64s);
    assert_eq!(reader.getnew_u64().expect("getnew u64"), u64s);
    assert_eq!(reader.getnew_f32().expect("getnew f32"), f32s);
    assert_eq!(reader.getnew_f64().expect("getnew f64"), f64s);
    assert_eq!(reader.getnew_complex32().expect("getnew complex32"), c32s);
    assert_eq!(reader.getnew_complex64().expect("getnew complex64"), c64s);
    assert_eq!(reader.getnew_string().expect("getnew string"), strings);
    reader.getend().expect("getend getnew");
}

#[test]
fn nested_object_framing_round_trip() {
    let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
    writer.putstart("outer", 1).expect("putstart outer");
    writer.put_i32(11).expect("put outer pre");
    writer.putstart("inner", 2).expect("putstart inner");
    writer.put_string("nested").expect("put inner");
    writer.putend().expect("putend inner");
    writer.put_i32(22).expect("put outer post");
    writer.putend().expect("putend outer");

    let bytes = writer
        .into_inner_typed::<Cursor<Vec<u8>>>()
        .expect("writer should be cursor")
        .into_inner();
    let mut reader = AipsIo::new_read_only(Cursor::new(bytes));

    assert_eq!(reader.get_next_type().expect("next outer"), "outer");
    assert_eq!(reader.get_next_type().expect("next outer cached"), "outer");
    assert_eq!(reader.getstart("outer").expect("getstart outer"), 1);
    assert_eq!(reader.get_i32().expect("outer pre"), 11);
    assert_eq!(reader.get_next_type().expect("next inner"), "inner");
    assert_eq!(reader.get_next_type().expect("next inner cached"), "inner");
    assert_eq!(reader.getstart("inner").expect("getstart inner"), 2);
    assert_eq!(reader.get_string().expect("inner payload"), "nested");
    reader.getend().expect("getend inner");
    assert_eq!(reader.get_i32().expect("outer post"), 22);
    reader.getend().expect("getend outer");
}

#[test]
fn put_get_value_round_trip_for_record_with_nested_values() {
    let matrix =
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![1_i32, 2_i32, 3_i32, 4_i32]).expect("shape");
    let cube = ArrayD::from_shape_vec(
        IxDyn(&[2, 1, 3]),
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
        ],
    )
    .expect("shape");

    let value = Value::Record(RecordValue::new(vec![
        RecordField::new("flag", Value::Scalar(ScalarValue::Bool(true))),
        RecordField::new("matrix", Value::Array(ArrayValue::Int32(matrix))),
        RecordField::new(
            "nested",
            Value::Record(RecordValue::new(vec![
                RecordField::new("name", Value::Scalar(ScalarValue::String("alpha".into()))),
                RecordField::new("cube", Value::Array(ArrayValue::String(cube))),
            ])),
        ),
    ]));

    let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
    writer.putstart("value", 1).expect("putstart");
    writer.put_value(&value).expect("put value");
    writer.putend().expect("putend");

    let bytes = writer
        .into_inner_typed::<Cursor<Vec<u8>>>()
        .expect("writer should be cursor")
        .into_inner();

    let mut reader = AipsIo::new_read_only(Cursor::new(bytes));
    assert_eq!(reader.get_next_type().expect("type"), "value");
    assert_eq!(reader.getstart("value").expect("start"), 1);
    let decoded = reader.get_value().expect("get value");
    assert_eq!(decoded, value);
    reader.getend().expect("getend");
}

/// A Fortran-layout array (as tables read them) and a strided view
/// round-trip by logical element, not by memory order.
#[test]
fn put_get_value_keeps_logical_elements_whatever_the_layout() {
    use ndarray::{ShapeBuilder, s};
    let at = |index: IxDyn| (index[0] * 10 + index[1]) as f64;
    let fortran = ArrayD::from_shape_fn(IxDyn(&[2, 3]).f(), at);
    let strided = ArrayD::from_shape_fn(IxDyn(&[2, 6]), at)
        .slice_move(s![.., ..;2])
        .into_dyn();
    for array in [fortran, strided] {
        let value = Value::Array(ArrayValue::Float64(array));
        let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
        writer.putstart("value", 1).expect("putstart");
        writer.put_value(&value).expect("put value");
        writer.putend().expect("putend");
        let bytes = writer
            .into_inner_typed::<Cursor<Vec<u8>>>()
            .expect("writer should be cursor")
            .into_inner();
        let mut reader = AipsIo::new_read_only(Cursor::new(bytes));
        assert_eq!(reader.getstart("value").expect("start"), 1);
        assert_eq!(reader.get_value().expect("get value"), value);
        reader.getend().expect("getend");
    }
}

#[test]
fn get_value_rejects_unknown_value_kind_tag() {
    let mut writer = AipsIo::new_read_write(Cursor::new(Vec::<u8>::new()));
    writer.putstart("value", 1).expect("putstart");
    writer.put_u8(99).expect("write invalid kind tag");
    writer.putend().expect("putend");

    let bytes = writer
        .into_inner_typed::<Cursor<Vec<u8>>>()
        .expect("writer should be cursor")
        .into_inner();
    let mut reader = AipsIo::new_read_only(Cursor::new(bytes));
    assert_eq!(reader.getstart("value").expect("start"), 1);

    let err = reader
        .get_value()
        .expect_err("unknown kind tags should fail");
    assert!(matches!(err, AipsIoObjectError::InvalidValueKindTag(99)));
}

#[test]
fn old_mode_rejects_putstart() {
    let path = temp_file("old_mode_rejects_putstart.data");
    {
        let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create temp file");
        io.close().expect("close");
    }

    let mut io = AipsIo::open(&path, AipsOpenOption::Old).expect("open old");
    let err = io
        .putstart("abc", 1)
        .expect_err("old mode should be read-only");
    assert!(matches!(err, AipsIoObjectError::PutStartUnavailable));
    let _ = std::fs::remove_file(path);
}

#[test]
fn new_no_replace_errors_if_file_exists() {
    let path = temp_file("new_no_replace_exists.data");
    {
        let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create file");
        io.close().expect("close");
    }
    let result = AipsIo::open(&path, AipsOpenOption::NewNoReplace);
    assert!(matches!(result, Err(AipsIoObjectError::Io(_))));
    let _ = std::fs::remove_file(path);
}

#[test]
fn delete_mode_removes_file_on_close() {
    let path = temp_file("delete_mode_removes_file.data");
    {
        let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create file");
        io.close().expect("close");
    }
    assert!(path.exists());

    let mut io = AipsIo::open(&path, AipsOpenOption::Delete).expect("open delete");
    io.close().expect("close delete");
    assert!(!path.exists());
}

#[test]
fn append_mode_appends_new_object() {
    let path = temp_file("append_mode_appends.data");

    {
        let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create file");
        io.putstart("obj", 1).expect("putstart");
        io.put_i32(11).expect("put value");
        io.putend().expect("putend");
        io.close().expect("close");
    }

    {
        let mut io = AipsIo::open(&path, AipsOpenOption::Append).expect("append open");
        io.putstart("obj", 1).expect("putstart");
        io.put_i32(22).expect("put value");
        io.putend().expect("putend");
        io.close().expect("close");
    }

    {
        let mut io = AipsIo::open(&path, AipsOpenOption::Old).expect("read open");
        assert_eq!(io.getstart("obj").expect("getstart first"), 1);
        assert_eq!(io.get_i32().expect("get first"), 11);
        io.getend().expect("getend first");

        assert_eq!(io.getstart("obj").expect("getstart second"), 1);
        assert_eq!(io.get_i32().expect("get second"), 22);
        io.getend().expect("getend second");
        io.close().expect("close");
    }

    let _ = std::fs::remove_file(path);
}

#[test]
fn append_mode_backpatches_length_in_place() {
    let path = temp_file("append_mode_backpatches_length.data");

    {
        let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create file");
        io.putstart("obj", 1).expect("putstart");
        io.put_i32(11).expect("put value");
        io.putend().expect("putend");
        io.close().expect("close");
    }

    {
        let mut io = AipsIo::open(&path, AipsOpenOption::Append).expect("append open");
        io.putstart("obj", 1).expect("putstart");
        io.put_i32(22).expect("put value");
        io.putend().expect("putend");
        io.close().expect("close");
    }

    let (second_obj_magic_offset, second_obj_len, end_pos_after_second) = {
        let mut io = AipsIo::open(&path, AipsOpenOption::Old).expect("read open");
        assert_eq!(io.getstart("obj").expect("getstart first"), 1);
        assert_eq!(io.get_i32().expect("get first"), 11);
        io.getend().expect("getend first");

        let second_obj_magic_offset = io.getpos().expect("offset before second object");
        assert_eq!(io.getstart("obj").expect("getstart second"), 1);
        assert_eq!(io.get_i32().expect("get second"), 22);
        let second_obj_len = io.getend().expect("getend second");
        let end_pos_after_second = io.getpos().expect("position after second object");
        io.close().expect("close");

        (
            second_obj_magic_offset,
            second_obj_len,
            end_pos_after_second,
        )
    };

    let raw = std::fs::read(&path).expect("read file bytes");
    let offset = second_obj_magic_offset as usize;
    let magic_field = u32::from_be_bytes(
        raw[offset..offset + 4]
            .try_into()
            .expect("magic field slice"),
    );
    assert_eq!(
        magic_field, MAGIC_VALUE,
        "each top-level object starts with magic"
    );

    let len_field = u32::from_be_bytes(
        raw[offset + 4..offset + 8]
            .try_into()
            .expect("length field slice"),
    );
    assert_ne!(
        len_field, MAGIC_VALUE,
        "append writes must backpatch object length in place"
    );
    assert_eq!(
        len_field, second_obj_len,
        "backpatched length must match encoded object size"
    );

    let file_len = std::fs::metadata(&path).expect("file metadata").len();
    assert_eq!(
        file_len, end_pos_after_second,
        "append writes should not leave trailing bytes from failed backpatch"
    );

    let _ = std::fs::remove_file(path);
}

#[test]
fn close_marks_stream_as_not_open() {
    let path = temp_file("close_marks_not_open.data");
    let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("open");
    io.close().expect("close");
    let err = io.getpos().expect_err("closed stream should reject getpos");
    assert!(matches!(err, AipsIoObjectError::NotOpen));
    let _ = std::fs::remove_file(path);
}

fn temp_file(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    path.push(format!("casa-rs-{name}.{}.{}", std::process::id(), nanos));
    path
}
