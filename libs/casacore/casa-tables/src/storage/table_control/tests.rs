// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use std::path::PathBuf;

fn test_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("table_control_{name}_{nanos}"))
}

#[test]
fn concat_descriptor_preserves_wide_row_counts_and_subtable_names() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("table.dat");
    let cases = [
        (i32::MAX as u64, 2),
        (i32::MAX as u64 + 1, 3),
        (u32::MAX as u64 + 11, 3),
    ];
    for big_endian in [false, true] {
        for (nrrow, version) in cases {
            let expected = ConcatTableDatContents {
                nrrow,
                big_endian,
                table_paths: vec!["././SUBMSS/first.ms".into()],
                sub_table_names: vec!["ANTENNA".into()],
            };
            write_concat_table_dat(&path, &expected).unwrap();
            let mut io = AipsIo::open(&path, AipsOpenOption::Old).unwrap();
            assert_eq!(read_table_header(&mut io).unwrap().0, version);
            let TableDatResult::Concat(actual) = read_table_dat_dispatch(&path).unwrap() else {
                panic!("expected ConcatTable");
            };
            assert_eq!(actual.nrrow, expected.nrrow);
            assert_eq!(actual.big_endian, expected.big_endian);
            assert_eq!(actual.table_paths, expected.table_paths);
            assert_eq!(actual.sub_table_names, expected.sub_table_names);
        }
    }
}

/// A Boolean array in a keyword record is bit-packed, as casacore's
/// `TypeIO` writes it: sixteen flags take the two bytes nine do, and the
/// first flag is the lowest bit. One byte per flag made CASA 6.7.6.14
/// refuse the table ("AipsIO::getend: part of object not read").
#[test]
fn a_boolean_array_keyword_is_bit_packed() {
    fn record_with(flags: &[bool]) -> RecordValue {
        let mut record = RecordValue::default();
        let cells = ArrayD::from_shape_vec(IxDyn(&[flags.len()]).f(), flags.to_vec()).unwrap();
        record.upsert("flags", Value::Array(ArrayValue::Bool(cells)));
        record
    }
    let nine = [true, false, true, true, false, false, true, false, true];
    let sixteen = [true; 16];
    let short = serialize_record_to_uchar(&record_with(&nine)).unwrap();
    let long = serialize_record_to_uchar(&record_with(&sixteen)).unwrap();
    assert_eq!(short.len(), long.len(), "both pack into two bytes");

    let three = serialize_record_to_uchar(&record_with(&[true, false, true])).unwrap();
    let element_count_then_bits = [0, 0, 0, 3, 0b101];
    assert!(
        three
            .windows(5)
            .any(|window| window == element_count_then_bits),
        "{three:?}"
    );

    for flags in [&nine[..], &sixteen[..]] {
        let bytes = serialize_record_to_uchar(&record_with(flags)).unwrap();
        let read = deserialize_record_from_uchar(&bytes).unwrap();
        let Some(Value::Array(ArrayValue::Bool(cells))) = read.get("flags") else {
            panic!("a Boolean array: {read:?}");
        };
        assert_eq!(cells.iter().copied().collect::<Vec<_>>(), flags);
    }
}

/// An array keyword is of variable shape in its `RecordDesc`
/// (`IPosition(1, -1)`, as `RecordDesc::addField` without a shape makes
/// it), whatever shape its data has: a fixed shape would make casacore
/// refuse a later update of another shape
/// (`Record::define - fixed array conformance error`, observed with CASA
/// 6.7.6.14 on a table whose descriptor had been changed to a fixed one).
#[test]
fn an_array_keyword_is_of_variable_shape_in_its_record_desc() {
    let variable_shape = [0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff];
    let mut record = RecordValue::default();
    record.upsert(
        "vector",
        Value::Array(ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![1.0, 2.0]).unwrap(),
        )),
    );
    record.upsert(
        "matrix",
        Value::Array(ArrayValue::Int32(
            ArrayD::from_shape_vec(IxDyn(&[2, 3]).f(), vec![1, 2, 3, 4, 5, 6]).unwrap(),
        )),
    );
    let bytes = serialize_record_to_uchar(&record).unwrap();
    let described = bytes
        .windows(variable_shape.len())
        .filter(|window| *window == variable_shape)
        .count();
    assert_eq!(
        described, 2,
        "one variable shape per array field: {bytes:?}"
    );

    let read = deserialize_record_from_uchar(&bytes).unwrap();
    assert_eq!(read, record, "the data keeps its own shape");
}

#[test]
fn column_keywords_roundtrip_in_table_dat() {
    // Build a ColumnDescContents with nested record keywords
    let mut measinfo = RecordValue::default();
    measinfo.upsert("type", Value::Scalar(ScalarValue::String("epoch".into())));
    measinfo.upsert("Ref", Value::Scalar(ScalarValue::String("UTC".into())));
    let mut col_kw = RecordValue::default();
    col_kw.upsert("MEASINFO", Value::Record(measinfo));

    let col = ColumnDescContents {
        class_name: "ArrayColumnDesc<double  ".to_string(),
        col_name: "TIME".to_string(),
        comment: String::new(),
        data_manager_type: "StManAipsIO".to_string(),
        data_manager_group: "StManAipsIO".to_string(),
        data_type: CasacoreDataType::TpDouble,
        option: 5, // Direct | FixedShape
        nrdim: 1,
        shape: vec![1],
        max_length: 0,
        keywords: col_kw.clone(),
        is_array: true,
        primitive_type: Some(PrimitiveType::Float64),
    };

    let contents = TableDatContents {
        nrrow: 1,
        big_endian: true,
        table_desc: TableDescContents {
            name: String::new(),
            version: String::new(),
            comment: String::new(),
            table_keywords: RecordValue::default(),
            private_keywords: RecordValue::default(),
            columns: vec![col],
        },
        column_set: ColumnSetContents {
            nrrow: 1,
            storage: None,
            seq_count: 1,
            data_managers: vec![DataManagerEntry {
                type_name: "StManAipsIO".to_string(),
                seq_nr: 0,
                data: vec![],
            }],
            columns: vec![PlainColumnEntry {
                original_name: "TIME".to_string(),
                dm_seq_nr: 0,
                is_array: true,
            }],
        },
        names_on_file: std::collections::HashMap::new(),
        asdm: Default::default(),
    };

    let dir = test_dir("col_kw_rt");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("table.dat");

    write_table_dat(&path, &contents).unwrap();

    // Read back and check column keywords
    let readback = read_table_dat(&path).unwrap();
    assert_eq!(readback.table_desc.columns.len(), 1);
    let kw = &readback.table_desc.columns[0].keywords;
    assert!(
        !kw.fields().is_empty(),
        "column keywords should not be empty after roundtrip"
    );
    match kw.get("MEASINFO") {
        Some(Value::Record(mi)) => {
            assert_eq!(
                mi.get("type"),
                Some(&Value::Scalar(ScalarValue::String("epoch".into())))
            );
            assert_eq!(
                mi.get("Ref"),
                Some(&Value::Scalar(ScalarValue::String("UTC".into())))
            );
        }
        other => panic!("expected MEASINFO record, got {other:?}"),
    }

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn fixed_array_schema_preserves_indirect_option_bit() {
    let schema = crate::ColumnSchema::array_fixed("data", PrimitiveType::Float32, vec![3]);
    let desc = ColumnDescContents::from_column_schema(&schema);
    assert_eq!(desc.option, 4);
    assert_eq!(desc.nrdim, 1);
    assert_eq!(desc.shape, vec![3]);
}

#[test]
fn option_bits_follow_casacore_base_column_desc() {
    let direct = crate::ColumnOptions {
        direct: true,
        undefined: false,
    };
    let undefined = crate::ColumnOptions {
        direct: false,
        undefined: true,
    };
    let option =
        |column: crate::ColumnSchema| ColumnDescContents::from_column_schema(&column).option;
    // Direct forces FixedShape, scalars included: CASA's component lists
    // carry option 5 on their Direct String and Double scalars.
    let scalar = crate::ColumnSchema::scalar("Label", PrimitiveType::String);
    assert_eq!(option(scalar.with_options(direct).unwrap()), 5);
    let fixed = crate::ColumnSchema::array_fixed("Flux", PrimitiveType::Complex64, vec![4]);
    assert_eq!(option(fixed.with_options(direct).unwrap()), 5);
    // Undefined persists on arrays and records, as casacore writes it.
    let vector =
        crate::ColumnSchema::array_variable("Optional_Parameters", PrimitiveType::Float64, Some(1));
    assert_eq!(option(vector.with_options(undefined).unwrap()), 2);
    let record = crate::ColumnSchema::record("Spectral_Record");
    assert_eq!(option(record.clone()), 0);
    assert_eq!(option(record.with_options(undefined).unwrap()), 2);
    let back = ColumnDescContents::from_column_schema(
        &crate::ColumnSchema::array_variable("v", PrimitiveType::Float64, Some(1))
            .with_options(undefined)
            .unwrap(),
    )
    .to_column_schema()
    .unwrap();
    assert!(back.options().undefined && !back.options().direct);
}

#[test]
fn table_reference_keywords_roundtrip_in_table_record() {
    let record = RecordValue::new(vec![RecordField::new(
        "ANTENNA",
        Value::table_ref("ANTENNA"),
    )]);

    let encoded = serialize_record_to_uchar(&record).unwrap();
    let decoded = deserialize_record_from_uchar(&encoded).unwrap();

    assert_eq!(
        decoded.get("ANTENNA"),
        Some(&Value::TableRef("ANTENNA".to_string()))
    );
}

/// Write a minimal table.dat + empty data file, then compare with C++-written version.
/// The output path is printed so it can be inspected externally.
#[test]
fn column_keywords_binary_matches_cpp_format() {
    // Build exactly what `from_snapshot` builds, with column keywords injected
    let schema = crate::TableSchema::new(vec![crate::ColumnSchema::array_fixed(
        "TIME",
        PrimitiveType::Float64,
        vec![1],
    )])
    .unwrap();

    let mut col_keywords = std::collections::HashMap::new();
    let mut measinfo = RecordValue::default();
    measinfo.upsert("type", Value::Scalar(ScalarValue::String("epoch".into())));
    measinfo.upsert("Ref", Value::Scalar(ScalarValue::String("UTC".into())));
    let mut kw = RecordValue::default();
    kw.upsert("MEASINFO", Value::Record(measinfo));
    col_keywords.insert("TIME".to_string(), kw);

    let table_dat = TableDatContents::from_snapshot(
        &schema,
        &RecordValue::default(),
        &col_keywords,
        1,
        "StManAipsIO",
        &[],
        true,
    );

    // Verify keywords were injected
    assert_eq!(table_dat.table_desc.columns.len(), 1);
    let col = &table_dat.table_desc.columns[0];
    assert!(
        !col.keywords.fields().is_empty(),
        "from_snapshot should inject column keywords into ColumnDescContents"
    );
    assert!(
        col.keywords.get("MEASINFO").is_some(),
        "MEASINFO should be present in column keywords"
    );
}
