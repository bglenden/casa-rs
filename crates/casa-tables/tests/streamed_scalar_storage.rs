// SPDX-License-Identifier: LGPL-3.0-or-later
//! Streamed scalar construction sources write the same StandardStMan and
//! IncrementalStMan files as the same values held in memory.
//!
//! A streamed source of the column's type is encoded a block of cells at a
//! time; an in-memory source goes cell by cell through each manager's value
//! encoder. The two must agree byte for byte, in either byte order, with
//! undefined cells and across block and bucket boundaries.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use casa_tables::{
    ColumnBinding, ColumnOverrides, ColumnSchema, DataManagerKind, EndianFormat,
    StreamedScalarType, StreamingScalarColumnWriter, Table, TableOptions, TableSchema,
};
use casa_types::{PrimitiveType, ScalarValue, Value};

/// More rows than three of the IncrementalStMan writer's encoding blocks.
const ROWS: usize = 3 * 4096 + 123;

struct Column {
    name: &'static str,
    primitive: PrimitiveType,
    streamed: StreamedScalarType,
    value: fn(usize) -> Option<ScalarValue>,
}

const COLUMNS: [Column; 5] = [
    Column {
        name: "ANTENNA1",
        primitive: PrimitiveType::Int32,
        streamed: StreamedScalarType::Int32,
        value: |row| Some(ScalarValue::Int32((row % 43) as i32 - 7)),
    },
    Column {
        name: "FLAG_ROW",
        primitive: PrimitiveType::Bool,
        streamed: StreamedScalarType::Bool,
        value: |row| (row % 13 != 0).then_some(ScalarValue::Bool(row % 3 == 0)),
    },
    Column {
        name: "GAIN",
        primitive: PrimitiveType::Float32,
        streamed: StreamedScalarType::Float32,
        value: |row| (row % 17 != 5).then(|| ScalarValue::Float32((row / 7) as f32 * -0.25)),
    },
    Column {
        name: "SCAN_NUMBER",
        primitive: PrimitiveType::Int32,
        streamed: StreamedScalarType::Int32,
        value: |row| Some(ScalarValue::Int32((row / 351) as i32)),
    },
    Column {
        name: "TIME",
        primitive: PrimitiveType::Float64,
        streamed: StreamedScalarType::Float64,
        value: |row| (row % 1000 != 999).then_some(ScalarValue::Float64(4.0e9 + (row / 50) as f64)),
    },
];

fn save(path: &Path, manager: DataManagerKind, endian: EndianFormat, streamed: bool) {
    let schema = TableSchema::new(
        COLUMNS
            .iter()
            .map(|column| ColumnSchema::scalar(column.name, column.primitive))
            .collect(),
    )
    .expect("schema");
    let mut table = Table::with_schema(schema);
    fs::create_dir_all(path).expect("table directory");
    let mut overrides = ColumnOverrides::for_row_count(ROWS);
    let mut bindings = HashMap::new();
    for column in &COLUMNS {
        if streamed {
            let mut writer = StreamingScalarColumnWriter::create(
                path.join(format!(".{}.scalar.tmp", column.name)),
                ROWS,
                column.streamed,
            )
            .expect("construction source");
            for row in 0..ROWS {
                writer.push((column.value)(row)).expect("push");
            }
            overrides.insert_streamed_scalar(column.name, writer.finish().expect("finish"));
        } else {
            // In-memory overrides need every cell; an undefined streamed
            // cell is stored as zero.
            let zero = match column.streamed {
                StreamedScalarType::Bool => ScalarValue::Bool(false),
                StreamedScalarType::Int32 => ScalarValue::Int32(0),
                StreamedScalarType::Float32 => ScalarValue::Float32(0.0),
                StreamedScalarType::Float64 => ScalarValue::Float64(0.0),
            };
            let values = (0..ROWS)
                .map(|row| Some(Value::Scalar((column.value)(row).unwrap_or(zero.clone()))))
                .collect();
            overrides.insert_values(column.name, values);
        }
        bindings.insert(
            column.name.to_string(),
            ColumnBinding {
                data_manager: manager,
                tile_shape: None,
            },
        );
    }
    table
        .prepare_write()
        .save_with_column_overrides(
            TableOptions::new(path)
                .with_data_manager(manager)
                .with_endian_format(endian),
            &bindings,
            &overrides,
        )
        .expect("save");
    for column in &COLUMNS {
        let _ = fs::remove_file(path.join(format!(".{}.scalar.tmp", column.name)));
    }
}

/// Every file of a table, by name.
fn files(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = fs::read_dir(path)
        .expect("table directory")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.is_file())
        .map(|file| {
            let name = file.file_name().unwrap().to_string_lossy().into_owned();
            (name, fs::read(&file).expect("read file"))
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[test]
fn streamed_sources_write_the_files_of_in_memory_values() {
    let root = tempfile::tempdir().expect("tempdir");
    for manager in [
        DataManagerKind::StandardStMan,
        DataManagerKind::IncrementalStMan,
    ] {
        for endian in [EndianFormat::LittleEndian, EndianFormat::BigEndian] {
            let case = format!("{manager:?}-{endian:?}");
            let in_memory = root.path().join(format!("{case}-values"));
            let streamed = root.path().join(format!("{case}-streamed"));
            save(&in_memory, manager, endian, false);
            save(&streamed, manager, endian, true);
            let expected = files(&in_memory);
            assert!(
                expected.iter().any(|(name, _)| name.starts_with("table.f")),
                "{case}: no data file in {expected:?}",
                expected = expected.iter().map(|(name, _)| name).collect::<Vec<_>>()
            );
            assert!(files(&streamed) == expected, "{case}: files differ");

            let reopened = Table::open(TableOptions::new(&streamed)).expect("reopen");
            for column in &COLUMNS {
                let stored = reopened
                    .column_accessor(column.name)
                    .and_then(|cells| cells.scalar_cells_owned())
                    .expect("read column");
                assert_eq!(stored.len(), ROWS);
                for (row, stored) in stored.into_iter().enumerate() {
                    if let Some(expected) = (column.value)(row) {
                        assert_eq!(stored, Some(expected), "{case} {} row {row}", column.name);
                    }
                }
            }
        }
    }
}
