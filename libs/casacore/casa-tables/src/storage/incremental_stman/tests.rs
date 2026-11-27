// SPDX-License-Identifier: LGPL-3.0-or-later

use super::super::canonical::write_i32_be;
use super::*;

#[test]
fn ism_element_sizes() {
    assert_eq!(ism_element_size(CasacoreDataType::TpBool), 1);
    assert_eq!(ism_element_size(CasacoreDataType::TpUChar), 1);
    assert_eq!(ism_element_size(CasacoreDataType::TpShort), 2);
    assert_eq!(ism_element_size(CasacoreDataType::TpInt), 4);
    assert_eq!(ism_element_size(CasacoreDataType::TpFloat), 4);
    assert_eq!(ism_element_size(CasacoreDataType::TpDouble), 8);
    assert_eq!(ism_element_size(CasacoreDataType::TpInt64), 8);
    assert_eq!(ism_element_size(CasacoreDataType::TpComplex), 8);
    assert_eq!(ism_element_size(CasacoreDataType::TpDComplex), 16);
    assert_eq!(ism_element_size(CasacoreDataType::TpString), 0);
}

#[test]
fn get_interval_basic() {
    let col_idx = IsmBucketColIndex {
        n_values: 3,
        row_nrs: vec![0, 3, 7],
        offsets: vec![0, 4, 8],
    };
    // Row 0 → interval 0
    assert_eq!(get_interval(&col_idx, 0), 0);
    // Row 2 → interval 0 (still in [0, 3))
    assert_eq!(get_interval(&col_idx, 2), 0);
    // Row 3 → interval 1 (at boundary)
    assert_eq!(get_interval(&col_idx, 3), 1);
    // Row 5 → interval 1 (in [3, 7))
    assert_eq!(get_interval(&col_idx, 5), 1);
    // Row 7 → interval 2 (at boundary)
    assert_eq!(get_interval(&col_idx, 7), 2);
    // Row 10 → interval 2 (past last, stays at last)
    assert_eq!(get_interval(&col_idx, 10), 2);
}

#[test]
fn get_interval_single_entry() {
    let col_idx = IsmBucketColIndex {
        n_values: 1,
        row_nrs: vec![0],
        offsets: vec![0],
    };
    assert_eq!(get_interval(&col_idx, 0), 0);
    assert_eq!(get_interval(&col_idx, 5), 0);
}

#[test]
fn parse_header_be() {
    // Synthesize a BE header
    let io_order = ByteOrder::BigEndian;
    let mut buf = AipsIoBufferWriter::new(io_order);
    buf.putstart("IncrementalStMan", 4).unwrap();
    buf.put_u32(32768); // bucket_size
    buf.put_u32(10); // nr_buckets
    buf.put_u32(1); // persCacheSize
    buf.put_u32(0); // uniqnr
    buf.put_u32(0); // nFreeBucket
    buf.put_i32(-1); // firstFreeBucket
    buf.putend().unwrap();
    let mut header = buf.into_bytes();
    header.resize(512, 0);

    // Parse it
    let mut cursor = std::io::Cursor::new(header);
    let hdr = parse_ism_header(&mut cursor).unwrap();
    assert_eq!(hdr.bucket_size, 32768);
    assert_eq!(hdr.nr_buckets, 10);
    assert!(hdr.big_endian);
}

#[test]
fn parse_header_le() {
    let io_order = ByteOrder::LittleEndian;
    let mut buf = AipsIoBufferWriter::new(io_order);
    buf.putstart("IncrementalStMan", 5).unwrap();
    buf.put_bool(false); // LE
    buf.put_u32(16384); // bucket_size
    buf.put_u32(5); // nr_buckets
    buf.put_u32(1); // persCacheSize
    buf.put_u32(0); // uniqnr
    buf.put_u32(0); // nFreeBucket
    buf.put_i32(-1); // firstFreeBucket
    buf.putend().unwrap();
    let mut header = buf.into_bytes();
    header.resize(512, 0);

    let mut cursor = std::io::Cursor::new(header);
    let hdr = parse_ism_header(&mut cursor).unwrap();
    assert_eq!(hdr.bucket_size, 16384);
    assert_eq!(hdr.nr_buckets, 5);
    assert!(!hdr.big_endian);
}

#[test]
fn string_encoding_round_trip() {
    for big_endian in [true, false] {
        let val = ScalarValue::String("hello".to_string());
        let encoded = encode_cell(Some(&val).into(), CasacoreDataType::TpString, 1, big_endian);
        let decoded = read_ism_string(&encoded, 1, big_endian).unwrap();
        assert_eq!(decoded, vec!["hello"]);
    }
}

#[test]
fn string_encoding_empty() {
    for big_endian in [true, false] {
        let val = ScalarValue::String(String::new());
        let encoded = encode_cell(Some(&val).into(), CasacoreDataType::TpString, 1, big_endian);
        let decoded = read_ism_string(&encoded, 1, big_endian).unwrap();
        assert_eq!(decoded, vec![""]);
    }
}

#[test]
fn bool_encoding_round_trip() {
    for big_endian in [true, false] {
        let val_true = ScalarValue::Bool(true);
        let val_false = ScalarValue::Bool(false);
        let enc_true = encode_cell(
            Some(&val_true).into(),
            CasacoreDataType::TpBool,
            1,
            big_endian,
        );
        let enc_false = encode_cell(
            Some(&val_false).into(),
            CasacoreDataType::TpBool,
            1,
            big_endian,
        );
        assert_eq!(enc_true.len(), 1);
        assert_eq!(enc_false.len(), 1);
        assert_eq!(enc_true[0], 1);
        assert_eq!(enc_false[0], 0);
    }
}

#[test]
fn bucket_round_trip() {
    // Build a bucket for 5 rows of i32 column: [1, 1, 1, 2, 2]
    let big_endian = true;
    let bucket_size = 256u32;
    let ncol = 1;

    // Data area: value 1 at offset 0, value 2 at offset 4
    let mut data_area = vec![0u8; 8];
    write_i32_be(&mut data_area[0..], 1);
    write_i32_be(&mut data_area[4..], 2);

    // Per-column index: 2 entries
    // row_nrs = [0, 3], offsets = [0, 4]
    let n_values = 2u32;
    let mut index_area = Vec::new();
    index_area.extend_from_slice(&n_values.to_be_bytes());
    index_area.extend_from_slice(&0u32.to_be_bytes()); // row_nr 0
    index_area.extend_from_slice(&3u32.to_be_bytes()); // row_nr 3
    index_area.extend_from_slice(&0u32.to_be_bytes()); // offset 0
    index_area.extend_from_slice(&4u32.to_be_bytes()); // offset 4

    let mut raw = vec![0u8; bucket_size as usize];
    let index_start = bucket_size as usize - index_area.len();

    // Write index_offset at byte 0
    raw[0..4].copy_from_slice(&(index_start as u32).to_be_bytes());
    // Write data area at byte 4
    raw[4..4 + data_area.len()].copy_from_slice(&data_area);
    // Write index at end
    raw[index_start..index_start + index_area.len()].copy_from_slice(&index_area);

    // Parse the bucket
    let bucket = parse_ism_bucket(&raw, ncol, big_endian).unwrap();
    assert_eq!(bucket.col_indices.len(), 1);
    assert_eq!(bucket.col_indices[0].n_values, 2);
    assert_eq!(bucket.col_indices[0].row_nrs, vec![0, 3]);
    assert_eq!(bucket.col_indices[0].offsets, vec![0, 4]);

    // Verify all 5 rows read back correctly
    for rel_row in 0..5u32 {
        let k = get_interval(&bucket.col_indices[0], rel_row);
        let offset = bucket.col_indices[0].offsets[k] as usize;
        let val =
            read_scalar_at(&bucket.data, offset, CasacoreDataType::TpInt, big_endian).unwrap();
        let expected = if rel_row < 3 { 1 } else { 2 };
        assert_eq!(
            val,
            casa_core::Value::Scalar(casa_core::ScalarValue::Int32(expected)),
            "row {rel_row}"
        );
    }
}

#[test]
fn scalar_column_writer_matches_row_writer_for_scalar_columns() {
    use casa_core::{RecordField, RecordValue, ScalarValue, Value};
    use tempfile::TempDir;

    let schema = vec![
        ColumnDescContents {
            class_name: "ScalarColumnDesc".to_string(),
            col_name: "flag_row".to_string(),
            comment: String::new(),
            data_manager_type: String::new(),
            data_manager_group: String::new(),
            shape: vec![],
            data_type: CasacoreDataType::TpBool,
            option: 0,
            nrdim: 0,
            max_length: 0,
            keywords: RecordValue::default(),
            is_array: false,
            primitive_type: Some(casa_core::PrimitiveType::Bool),
        },
        ColumnDescContents {
            class_name: "ScalarColumnDesc".to_string(),
            col_name: "scan".to_string(),
            comment: String::new(),
            data_manager_type: String::new(),
            data_manager_group: String::new(),
            shape: vec![],
            data_type: CasacoreDataType::TpInt,
            option: 0,
            nrdim: 0,
            max_length: 0,
            keywords: RecordValue::default(),
            is_array: false,
            primitive_type: Some(casa_core::PrimitiveType::Int32),
        },
    ];

    let rows = vec![
        RecordValue::new(vec![
            RecordField::new("flag_row", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("scan", Value::Scalar(ScalarValue::Int32(1))),
        ]),
        RecordValue::new(vec![
            RecordField::new("flag_row", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("scan", Value::Scalar(ScalarValue::Int32(1))),
        ]),
        RecordValue::new(vec![
            RecordField::new("flag_row", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("scan", Value::Scalar(ScalarValue::Int32(2))),
        ]),
        RecordValue::new(vec![
            RecordField::new("flag_row", Value::Scalar(ScalarValue::Bool(true))),
            RecordField::new("scan", Value::Scalar(ScalarValue::Int32(2))),
        ]),
    ];

    let flag_values = vec![
        Some(ScalarValue::Bool(false)),
        Some(ScalarValue::Bool(false)),
        Some(ScalarValue::Bool(true)),
        Some(ScalarValue::Bool(true)),
    ];
    let scan_values = vec![
        Some(ScalarValue::Int32(1)),
        Some(ScalarValue::Int32(1)),
        Some(ScalarValue::Int32(2)),
        Some(ScalarValue::Int32(2)),
    ];
    let scalar_columns = vec![flag_values.as_slice(), scan_values.as_slice()];

    for big_endian in [true, false] {
        let dir = TempDir::new().expect("tempdir");
        let row_path = dir.path().join(if big_endian {
            "row_be.ism"
        } else {
            "row_le.ism"
        });
        let scalar_path = dir.path().join(if big_endian {
            "scalar_be.ism"
        } else {
            "scalar_le.ism"
        });

        let row_dm = write_ism_file(&row_path, &schema, &rows, big_endian).expect("row writer");
        let scalar_dm =
            write_ism_file_scalar_columns(&scalar_path, &schema, &scalar_columns, big_endian)
                .expect("scalar writer");

        assert_eq!(row_dm, scalar_dm);
        assert_eq!(
            std::fs::read(&row_path).expect("read row file"),
            std::fs::read(&scalar_path).expect("read scalar file")
        );
    }
}

#[test]
fn selected_scalar_reader_ignores_array_siblings_owned_by_same_manager() {
    use casa_core::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use tempfile::TempDir;

    let scalar_desc = ColumnDescContents {
        class_name: "ScalarColumnDesc".to_string(),
        col_name: "FIELD_ID".to_string(),
        comment: String::new(),
        data_manager_type: "IncrementalStMan".to_string(),
        data_manager_group: "ISMData".to_string(),
        data_type: CasacoreDataType::TpInt,
        option: 0,
        nrdim: 0,
        shape: vec![],
        max_length: 0,
        keywords: RecordValue::default(),
        is_array: false,
        primitive_type: Some(PrimitiveType::Int32),
    };
    let physical_sibling_desc = ColumnDescContents {
        col_name: "UVW".to_string(),
        ..scalar_desc.clone()
    };
    let rows = [0, 0, 2, 2]
        .into_iter()
        .map(|field_id| {
            RecordValue::new(vec![
                RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(field_id))),
                RecordField::new("UVW", Value::Scalar(ScalarValue::Int32(17))),
            ])
        })
        .collect::<Vec<_>>();
    let dir = TempDir::new().expect("tempdir");
    let file_path = dir.path().join("mixed-manager.ism");
    let dm_blob = write_ism_file(
        &file_path,
        &[scalar_desc.clone(), physical_sibling_desc.clone()],
        &rows,
        false,
    )
    .expect("write ISM fixture");

    let array_sibling_desc = ColumnDescContents {
        class_name: "ArrayColumnDesc<double>".to_string(),
        data_type: CasacoreDataType::TpArrayDouble,
        nrdim: 1,
        shape: vec![3],
        is_array: true,
        primitive_type: Some(PrimitiveType::Float64),
        ..physical_sibling_desc
    };
    let selected = read_ism_scalar_columns_rows_uncached(
        &file_path,
        &dm_blob,
        &[&scalar_desc, &array_sibling_desc],
        &[3, 0, 2],
        false,
    )
    .expect("read scalar column beside array sibling");

    assert_eq!(
        selected[0],
        vec![
            Some(ScalarValue::Int32(2)),
            Some(ScalarValue::Int32(0)),
            Some(ScalarValue::Int32(2)),
        ]
    );
    assert_eq!(selected[1], vec![None, None, None]);
}
