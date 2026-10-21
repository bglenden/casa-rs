// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generated array columns write the files of the same cells held as rows.
//!
//! A table saved from [`GeneratedArrayColumn`]s and [`GeneratedScalarColumn`]s,
//! per-row closures or runs, without one row in memory, must be byte for
//! byte the table saved from row records: StandardStMan with scalars,
//! strings, direct and indirect fixed-shape arrays and variable-shape arrays
//! with undefined cells, and IncrementalStMan with scalars and fixed-shape
//! arrays, in either byte order and across bucket boundaries. Saves that
//! cannot be written from generated cells are refused.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use casa_tables::{
    ColumnBinding, ColumnOptions, ColumnOverrides, ColumnSchema, DataManagerKind, EndianFormat,
    GeneratedArrayColumn, GeneratedArrayValueRun, GeneratedScalarColumn, GeneratedScalarValueRun,
    Table, TableError, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;

/// Rows spanning several buckets of either manager.
const ROWS: usize = 2_345;

/// Rows a run lasts in the run-structured columns.
const RUN_ROWS: usize = 37;

fn array<T: Clone>(shape: &[usize], values: Vec<T>) -> ArrayD<T> {
    ArrayD::from_shape_vec(shape.to_vec(), values).expect("shape matches")
}

/// How a column's cells are given to the generated save.
#[derive(Clone, Copy, PartialEq)]
enum Generated {
    PerRow,
    Runs,
}

/// One column of a test table: its schema, cell `row`, and whether its
/// cells come in runs of [`RUN_ROWS`] (so it can be given as runs).
struct Column {
    schema: fn() -> ColumnSchema,
    cell: fn(usize) -> Option<Value>,
    runs: bool,
}

fn direct() -> ColumnOptions {
    ColumnOptions {
        direct: true,
        undefined: false,
    }
}

/// The run a run-structured cell of `row` belongs to.
fn run(row: usize) -> usize {
    row / RUN_ROWS
}

/// StandardStMan columns: POINTING's, then the other kinds the manager
/// stores.
fn standard_columns() -> Vec<Column> {
    vec![
        Column {
            schema: || ColumnSchema::scalar("ANTENNA_ID", PrimitiveType::Int32),
            cell: |row| Some(Value::Scalar(ScalarValue::Int32((row % 7) as i32))),
            runs: false,
        },
        Column {
            schema: || ColumnSchema::array_variable("DIRECTION", PrimitiveType::Float64, Some(2)),
            cell: |row| {
                let run = run(row) as f64;
                Some(Value::Array(ArrayValue::Float64(array(
                    &[2, 1],
                    vec![0.25 * run, -0.5 + run / 64.0],
                ))))
            },
            runs: true,
        },
        Column {
            schema: || ColumnSchema::scalar("INTERVAL", PrimitiveType::Float64),
            cell: |row| Some(Value::Scalar(ScalarValue::Float64(1.0 + run(row) as f64))),
            runs: true,
        },
        Column {
            schema: || ColumnSchema::scalar("NAME", PrimitiveType::String),
            cell: |_| Some(Value::Scalar(ScalarValue::String(String::new()))),
            runs: true,
        },
        Column {
            schema: || ColumnSchema::scalar("NUM_POLY", PrimitiveType::Int32),
            cell: |_| Some(Value::Scalar(ScalarValue::Int32(0))),
            runs: true,
        },
        Column {
            schema: || ColumnSchema::array_variable("TARGET", PrimitiveType::Float64, Some(2)),
            cell: |row| {
                let run = run(row) as f64;
                Some(Value::Array(ArrayValue::Float64(array(
                    &[2, 1],
                    vec![1.0 - run, run * 0.125],
                ))))
            },
            runs: true,
        },
        Column {
            schema: || ColumnSchema::scalar("TIME", PrimitiveType::Float64),
            cell: |row| {
                Some(Value::Scalar(ScalarValue::Float64(
                    4.0e9 + row as f64 / 3.0,
                )))
            },
            runs: false,
        },
        Column {
            schema: || ColumnSchema::scalar("TRACKING", PrimitiveType::Bool),
            cell: |row| Some(Value::Scalar(ScalarValue::Bool(run(row) % 3 != 1))),
            runs: true,
        },
        Column {
            schema: || ColumnSchema::scalar("LONG_NAME", PrimitiveType::String),
            cell: |row| {
                Some(Value::Scalar(ScalarValue::String(format!(
                    "a name longer than eight bytes {}",
                    run(row)
                ))))
            },
            runs: true,
        },
        Column {
            schema: || ColumnSchema::array_variable("COUNTS", PrimitiveType::Int32, None),
            cell: |row| {
                let count = row % 4;
                (row % 9 != 4).then(|| {
                    Value::Array(ArrayValue::Int32(array(
                        &[count],
                        (0..count as i32).map(|i| i - row as i32).collect(),
                    )))
                })
            },
            runs: false,
        },
        Column {
            schema: || ColumnSchema::array_variable("FLAGS", PrimitiveType::Bool, Some(1)),
            cell: |row| {
                (row % 5 != 3).then(|| {
                    Value::Array(ArrayValue::Bool(array(
                        &[row % 3 + 1],
                        (0..=row % 3).map(|i| (i + row) % 2 == 0).collect(),
                    )))
                })
            },
            runs: false,
        },
        Column {
            schema: || ColumnSchema::array_variable("LABELS", PrimitiveType::String, Some(1)),
            cell: |row| {
                (row % 6 != 1).then(|| {
                    Value::Array(ArrayValue::String(array(
                        &[2],
                        vec![format!("label {}", run(row)), String::new()],
                    )))
                })
            },
            runs: false,
        },
        Column {
            schema: || ColumnSchema::array_fixed("OFFSET", PrimitiveType::Float64, vec![2]),
            cell: |row| {
                Some(Value::Array(ArrayValue::Float64(array(
                    &[2],
                    vec![run(row) as f64, -(run(row) as f64)],
                ))))
            },
            runs: true,
        },
        Column {
            schema: || {
                ColumnSchema::array_fixed("POSITION", PrimitiveType::Float64, vec![3, 2])
                    .with_options(direct())
                    .expect("direct")
            },
            cell: |row| {
                Some(Value::Array(ArrayValue::Float64(array(
                    &[3, 2],
                    (0..6).map(|i| (row * 6 + i) as f64 * 0.5).collect(),
                ))))
            },
            runs: false,
        },
        Column {
            schema: || {
                ColumnSchema::array_fixed("RECEPTORS", PrimitiveType::String, vec![2])
                    .with_options(direct())
                    .expect("direct")
            },
            cell: |row| {
                Some(Value::Array(ArrayValue::String(array(
                    &[2],
                    vec!["X".to_string(), format!("receptor {}", run(row))],
                ))))
            },
            runs: true,
        },
    ]
}

/// IncrementalStMan columns: scalars and fixed-shape arrays.
fn incremental_columns() -> Vec<Column> {
    vec![
        Column {
            schema: || ColumnSchema::scalar("ANTENNA_ID", PrimitiveType::Int32),
            cell: |row| Some(Value::Scalar(ScalarValue::Int32((row % 7) as i32))),
            runs: false,
        },
        Column {
            schema: || ColumnSchema::scalar("TIME", PrimitiveType::Float64),
            cell: |row| Some(Value::Scalar(ScalarValue::Float64(4.0e9 + run(row) as f64))),
            runs: true,
        },
        Column {
            schema: || ColumnSchema::scalar("NAME", PrimitiveType::String),
            cell: |row| {
                Some(Value::Scalar(ScalarValue::String(format!(
                    "n{}",
                    run(row) % 5
                ))))
            },
            runs: true,
        },
        Column {
            schema: || ColumnSchema::array_fixed("DIRECTION", PrimitiveType::Float64, vec![2, 1]),
            cell: |row| {
                Some(Value::Array(ArrayValue::Float64(array(
                    &[2, 1],
                    vec![run(row) as f64, 0.5],
                ))))
            },
            runs: true,
        },
        Column {
            schema: || ColumnSchema::array_fixed("CORR", PrimitiveType::Int32, vec![4]),
            cell: |row| {
                let base = (row % 3) as i32;
                Some(Value::Array(ArrayValue::Int32(array(
                    &[4],
                    vec![base, base + 1, 5, 8],
                ))))
            },
            runs: false,
        },
        Column {
            schema: || ColumnSchema::array_fixed("FLAGS", PrimitiveType::Bool, vec![3]),
            cell: |row| {
                Some(Value::Array(ArrayValue::Bool(array(
                    &[3],
                    vec![run(row).is_multiple_of(2), true, run(row).is_multiple_of(3)],
                ))))
            },
            runs: true,
        },
        Column {
            schema: || ColumnSchema::array_fixed("RECEPTORS", PrimitiveType::String, vec![2]),
            cell: |row| {
                Some(Value::Array(ArrayValue::String(array(
                    &[2],
                    vec!["R".to_string(), format!("L{}", run(row) % 4)],
                ))))
            },
            runs: true,
        },
    ]
}

fn schema(columns: &[Column]) -> TableSchema {
    TableSchema::new(columns.iter().map(|column| (column.schema)()).collect()).expect("schema")
}

fn options(path: &Path, manager: DataManagerKind, endian: EndianFormat) -> TableOptions {
    TableOptions::new(path)
        .with_data_manager(manager)
        .with_endian_format(endian)
}

/// Save `rows` rows as row records, the way a subtable is saved.
fn save_rows(
    path: &Path,
    columns: &[Column],
    rows: usize,
    manager: DataManagerKind,
    endian: EndianFormat,
) {
    let mut table = Table::with_schema(schema(columns));
    for row in 0..rows {
        let fields = columns
            .iter()
            .filter_map(|column| {
                (column.cell)(row).map(|value| RecordField::new((column.schema)().name(), value))
            })
            .collect();
        table.add_row(RecordValue::new(fields)).expect("add row");
    }
    table
        .prepare_write()
        .save(options(path, manager, endian))
        .expect("save rows");
}

/// The runs of a run-structured column.
fn runs_of(column: &Column, rows: usize) -> Vec<(usize, Option<Value>)> {
    (0..rows)
        .step_by(RUN_ROWS)
        .map(|start| (start, (column.cell)(start)))
        .collect()
}

/// The generated override of `column` over `rows` rows.
fn insert_generated(
    overrides: &mut ColumnOverrides,
    column: &Column,
    rows: usize,
    generated: Generated,
) {
    let schema = (column.schema)();
    let cell = column.cell;
    let name = schema.name().to_string();
    let as_runs = generated == Generated::Runs && column.runs;
    let is_array = matches!(schema.column_type(), casa_tables::ColumnType::Array(_));
    if is_array {
        let values = if as_runs {
            let runs = runs_of(column, rows)
                .into_iter()
                .map(|(start, value)| {
                    GeneratedArrayValueRun::new(
                        start,
                        value.map(|value| match value {
                            Value::Array(array) => array,
                            other => panic!("array cell {other:?}"),
                        }),
                    )
                })
                .collect();
            GeneratedArrayColumn::from_array_runs(rows, runs).expect("array runs")
        } else {
            GeneratedArrayColumn::new(rows, move |row| match cell(row) {
                Some(Value::Array(array)) => Some(array),
                _ => None,
            })
        };
        overrides.insert_generated_array(name, values);
    } else {
        let values = if as_runs {
            let runs = runs_of(column, rows)
                .into_iter()
                .map(|(start, value)| {
                    GeneratedScalarValueRun::new(
                        start,
                        value.map(|value| match value {
                            Value::Scalar(scalar) => scalar,
                            other => panic!("scalar cell {other:?}"),
                        }),
                    )
                })
                .collect();
            GeneratedScalarColumn::from_scalar_runs(rows, runs).expect("scalar runs")
        } else {
            GeneratedScalarColumn::new(rows, move |row| match cell(row) {
                Some(Value::Scalar(scalar)) => Some(scalar),
                _ => None,
            })
        };
        overrides.insert_generated_scalar(name, values);
    }
}

/// Save `rows` rows of generated cells from an empty table.
fn save_generated(
    path: &Path,
    columns: &[Column],
    rows: usize,
    manager: DataManagerKind,
    endian: EndianFormat,
    generated: Generated,
) {
    let mut table = Table::with_schema(schema(columns));
    let mut overrides = ColumnOverrides::for_row_count(rows);
    for column in columns {
        insert_generated(&mut overrides, column, rows, generated);
    }
    table
        .prepare_write()
        .save_with_column_overrides(options(path, manager, endian), &HashMap::new(), &overrides)
        .expect("save generated");
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

fn assert_same_files(expected: &Path, found: &Path, case: &str) {
    let (expected, found) = (files(expected), files(found));
    let names = |files: &[(String, Vec<u8>)]| {
        files
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&expected), names(&found), "{case}: file names");
    for ((name, expected), (_, found)) in expected.iter().zip(&found) {
        assert!(expected == found, "{case}: {name} differs");
    }
}

/// The reopened table holds every cell as given.
fn assert_cells(path: &Path, columns: &[Column], rows: usize, case: &str) {
    let table = Table::open(TableOptions::new(path)).expect("reopen");
    assert_eq!(table.row_count(), rows, "{case}");
    for column in columns {
        let name = (column.schema)().name().to_string();
        let cells = table
            .column_accessor(&name)
            .and_then(|cells| cells.cells())
            .expect("column cells");
        for (row, stored) in cells.into_iter().enumerate() {
            match (column.cell)(row) {
                Some(expected) => {
                    assert_eq!(stored, Some(&expected), "{case}: {name} row {row}");
                }
                None => assert!(
                    !table.is_cell_defined(row, &name).expect("cell"),
                    "{case}: {name} row {row} is undefined"
                ),
            }
        }
    }
}

#[test]
fn generated_standard_columns_write_the_files_of_row_records() {
    let root = tempfile::tempdir().expect("tempdir");
    let columns = standard_columns();
    for rows in [0, 20, ROWS] {
        for endian in [EndianFormat::LittleEndian, EndianFormat::BigEndian] {
            let case = format!("StandardStMan {rows} rows {endian:?}");
            let manager = DataManagerKind::StandardStMan;
            let reference = root.path().join(format!("{rows}-{endian:?}-rows"));
            save_rows(&reference, &columns, rows, manager, endian);
            for generated in [Generated::PerRow, Generated::Runs] {
                let path = root
                    .path()
                    .join(format!("{rows}-{endian:?}-{}", generated as u8));
                save_generated(&path, &columns, rows, manager, endian, generated);
                assert_same_files(&reference, &path, &case);
            }
            assert_cells(&reference, &columns, rows, &case);
        }
    }
}

/// Save `rows` rows with every column but `generated` (an array column) as
/// materialized override values.
///
/// A group holding a generated array column is written from its column
/// sources, so this is the column-source writer's materialized write.
/// IncrementalStMan's row writer, which writes groups of row records, lays
/// buckets out differently once a value changes at a bucket's first row (it
/// keeps a copy of the previous value there), for scalars as for arrays.
fn save_values(
    path: &Path,
    columns: &[Column],
    rows: usize,
    manager: DataManagerKind,
    endian: EndianFormat,
    generated: &Column,
) {
    let mut table = Table::with_schema(schema(columns));
    let mut overrides = ColumnOverrides::for_row_count(rows);
    for column in columns {
        overrides.insert_values(
            (column.schema)().name(),
            (0..rows).map(column.cell).collect(),
        );
    }
    insert_generated(&mut overrides, generated, rows, Generated::PerRow);
    table
        .prepare_write()
        .save_with_column_overrides(options(path, manager, endian), &HashMap::new(), &overrides)
        .expect("save values");
}

#[test]
fn generated_incremental_columns_write_the_files_of_materialized_cells() {
    let root = tempfile::tempdir().expect("tempdir");
    let columns = incremental_columns();
    let manager = DataManagerKind::IncrementalStMan;
    for rows in [0, 20, ROWS] {
        for endian in [EndianFormat::LittleEndian, EndianFormat::BigEndian] {
            let case = format!("IncrementalStMan {rows} rows {endian:?}");
            let generated = root.path().join(format!("{rows}-{endian:?}-per-row"));
            save_generated(
                &generated,
                &columns,
                rows,
                manager,
                endian,
                Generated::PerRow,
            );
            assert_cells(&generated, &columns, rows, &case);
            let runs = root.path().join(format!("{rows}-{endian:?}-runs"));
            save_generated(&runs, &columns, rows, manager, endian, Generated::Runs);
            assert_same_files(&generated, &runs, &case);
            // Each array column is materialized in all but one reference.
            let arrays = columns
                .iter()
                .filter(|column| (column.schema)().data_type().is_some())
                .filter(|column| {
                    matches!(
                        (column.schema)().column_type(),
                        casa_tables::ColumnType::Array(_)
                    )
                });
            for (index, kept) in arrays.enumerate() {
                let reference = root
                    .path()
                    .join(format!("{rows}-{endian:?}-values-{index}"));
                save_values(&reference, &columns, rows, manager, endian, kept);
                assert_same_files(&reference, &generated, &case);
            }
            if rows <= 20 {
                // In one bucket, the row writer agrees.
                let rows_path = root.path().join(format!("{rows}-{endian:?}-rows"));
                save_rows(&rows_path, &columns, rows, manager, endian);
                assert_same_files(&rows_path, &generated, &case);
            }
        }
    }
}

fn pointing_table() -> Table {
    Table::with_schema(
        TableSchema::new(vec![
            ColumnSchema::scalar("ANTENNA_ID", PrimitiveType::Int32),
            ColumnSchema::array_variable("DIRECTION", PrimitiveType::Float64, Some(2)),
        ])
        .expect("schema"),
    )
}

fn direction_cells(rows: usize) -> GeneratedArrayColumn {
    GeneratedArrayColumn::constant(
        rows,
        Some(ArrayValue::Float64(array(&[2, 1], vec![0.5, 0.25]))),
    )
}

fn save_pointing(
    path: &Path,
    manager: DataManagerKind,
    bindings: &HashMap<String, ColumnBinding>,
    overrides: &ColumnOverrides,
) -> Result<(), TableError> {
    pointing_table().prepare_write().save_with_column_overrides(
        TableOptions::new(path).with_data_manager(manager),
        bindings,
        overrides,
    )
}

fn refusal(result: Result<(), TableError>) -> String {
    result.expect_err("refused").to_string()
}

#[test]
fn saves_generated_cells_cannot_take_are_refused() {
    let root = tempfile::tempdir().expect("tempdir");
    let rows = 10;
    let mut complete = ColumnOverrides::for_row_count(rows);
    complete.insert_generated_scalar(
        "ANTENNA_ID",
        GeneratedScalarColumn::new(rows, |row| Some(ScalarValue::Int32(row as i32))),
    );
    complete.insert_generated_array("DIRECTION", direction_cells(rows));

    let message = refusal(save_pointing(
        &root.path().join("aipsio"),
        DataManagerKind::StManAipsIO,
        &HashMap::new(),
        &complete,
    ));
    assert!(
        message.contains("needs StandardStMan or IncrementalStMan"),
        "{message}"
    );

    let tiled = HashMap::from([(
        "DIRECTION".to_string(),
        ColumnBinding {
            data_manager: DataManagerKind::TiledShapeStMan,
            tile_shape: None,
        },
    )]);
    let message = refusal(save_pointing(
        &root.path().join("tiled"),
        DataManagerKind::StandardStMan,
        &tiled,
        &complete,
    ));
    assert!(message.contains("TiledShapeStMan"), "{message}");

    let mut partial = ColumnOverrides::for_row_count(rows);
    partial.insert_generated_array("DIRECTION", direction_cells(rows));
    let message = refusal(save_pointing(
        &root.path().join("partial"),
        DataManagerKind::StandardStMan,
        &HashMap::new(),
        &partial,
    ));
    assert!(
        message.contains("ANTENNA_ID, which has no override"),
        "{message}"
    );

    let message = refusal(save_pointing(
        &root.path().join("incremental"),
        DataManagerKind::IncrementalStMan,
        &HashMap::new(),
        &complete,
    ));
    assert!(
        message.contains("IncrementalStMan does not write variable-shape array column DIRECTION"),
        "{message}"
    );

    let mut misshapen = complete.clone();
    misshapen.insert_generated_array(
        "DIRECTION",
        GeneratedArrayColumn::new(rows, |row| {
            Some(ArrayValue::Float64(array(
                &[row % 2 + 1],
                vec![0.0; row % 2 + 1],
            )))
        }),
    );
    let message = refusal(save_pointing(
        &root.path().join("misshapen"),
        DataManagerKind::StandardStMan,
        &HashMap::new(),
        &misshapen,
    ));
    assert!(
        message.contains("generated array column DIRECTION row 0: 1 axes in a column of 2"),
        "{message}"
    );

    let mut mistyped = complete.clone();
    mistyped.insert_generated_array(
        "DIRECTION",
        GeneratedArrayColumn::constant(rows, Some(ArrayValue::Int32(array(&[2, 1], vec![1, 2])))),
    );
    assert!(matches!(
        save_pointing(
            &root.path().join("mistyped"),
            DataManagerKind::StandardStMan,
            &HashMap::new(),
            &mistyped,
        ),
        Err(TableError::PrimitiveTypeMismatch { row_index: 0, .. })
    ));
}

#[test]
fn generated_array_runs_validate_and_resolve_cells() {
    let cell = |value: f64| Some(ArrayValue::Float64(array(&[2], vec![value, -value])));
    let column = GeneratedArrayColumn::from_array_runs(
        10,
        vec![
            GeneratedArrayValueRun::new(0, cell(1.0)),
            GeneratedArrayValueRun::new(4, cell(2.0)),
            GeneratedArrayValueRun::new(8, None),
        ],
    )
    .expect("valid runs");
    assert_eq!(column.row_count(), 10);
    assert_eq!(column.value(0), cell(1.0));
    assert_eq!(column.value(3), cell(1.0));
    assert_eq!(column.value(4), cell(2.0));
    assert_eq!(column.value(7), cell(2.0));
    assert_eq!(column.value(8), None);
    assert_eq!(column.value(10), None);
    assert_eq!(
        GeneratedArrayColumn::constant(3, cell(5.0)).value(2),
        cell(5.0)
    );
    assert_eq!(
        GeneratedArrayColumn::new(3, move |row| cell(row as f64)).value(2),
        cell(2.0)
    );

    for (rows, starts, problem) in [
        (2, vec![1], "must start at row 0"),
        (2, vec![0, 0], "strictly increasing"),
        (2, vec![0, 2], "beyond row count 2"),
        (0, vec![0], "for zero rows must be empty"),
        (2, vec![], "must start at row 0"),
    ] {
        let runs = starts
            .into_iter()
            .map(|start| GeneratedArrayValueRun::new(start, cell(0.0)))
            .collect();
        match GeneratedArrayColumn::from_array_runs(rows, runs) {
            Err(TableError::InvalidGeneratedArrayRuns { message }) => {
                assert!(message.contains(problem), "{message}");
            }
            other => panic!("{problem}: {other:?}"),
        }
    }
    assert!(GeneratedArrayColumn::from_array_runs(0, Vec::new()).is_ok());
}
