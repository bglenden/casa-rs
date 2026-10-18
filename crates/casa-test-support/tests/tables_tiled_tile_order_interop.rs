// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "cpp-interop-tests")]
//! Tile-order interop for TiledShapeStMan, TiledCellStMan and
//! TiledColumnStMan (#755).
//!
//! casacore lays a tile out over a hypercolumn's data columns by
//! descending pixel size, Bool last and ties in descending position in the
//! hypercolumn definition or, without one, in binding order. Each fixture
//! binds columns of mixed sizes in an order that is not the tile's: CR,
//! casa-rs reads what C++ wrote; RC, C++ reads what casa-rs patched in place
//! or rewrote.

use std::path::{Path, PathBuf};

use casa_tables::{DataManagerKind, EndianFormat, Table, TableOptions};
use casa_test_support::{CppTableFixture, TableOracle, casacore_oracle_available};
use casa_types::{ArrayValue, Complex32, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

const PATCHED_ROWS: [usize; 2] = [1, 3];
const MIXED_ROWS: usize = 4;
const MIXED_COLUMNS: [&str; 4] = ["flag", "data", "weight", "sigma"];
const NODEF_ROWS: usize = 3;
const NODEF_COLUMNS: [&str; 4] = ["count", "flag", "value", "amp"];

/// A column-major cell of `shape` whose element `i` is `value(i)`.
fn cell<T>(shape: &[usize], value: impl Fn(usize) -> T) -> ArrayD<T> {
    let count = shape.iter().product();
    ArrayD::from_shape_vec(IxDyn(shape).f(), (0..count).map(value).collect()).expect("cell shape")
}

/// The shape-mixed fixture's cell of `column` in `row`; `patched` negates
/// data and inverts flag in [`PATCHED_ROWS`].
fn mixed_cell(column: &str, row: usize, patched: bool) -> ArrayValue {
    let shape: &[usize] = if row < 2 { &[2, 3] } else { &[3, 2] };
    let changed = patched && PATCHED_ROWS.contains(&row);
    let r = row as f32;
    match column {
        "data" => ArrayValue::Complex32(cell(shape, |i| {
            let value = Complex32::new(10.0 * r + i as f32, -(i as f32));
            if changed { -value } else { value }
        })),
        "weight" => ArrayValue::Float32(cell(shape, |i| 0.5 * (r + 1.0) + i as f32)),
        "sigma" => ArrayValue::Float32(cell(shape, |i| 100.0 + 10.0 * r + i as f32)),
        "flag" => ArrayValue::Bool(cell(shape, |i| ((i + row) % 2 == 1) != changed)),
        other => panic!("no shape-mixed column {other}"),
    }
}

/// The no-definition fixture's cell of `column` in `row`.
fn nodef_cell(column: &str, row: usize) -> ArrayValue {
    let shape = [2, 3];
    match column {
        "count" => ArrayValue::Int32(cell(&shape, |i| (1000 * row + i) as i32)),
        "flag" => ArrayValue::Bool(cell(&shape, |i| i % 3 == row)),
        "value" => ArrayValue::Float64(cell(&shape, |i| 0.25 + row as f64 + 10.0 * i as f64)),
        "amp" => ArrayValue::Float32(cell(&shape, |i| -1.5 * (i as f32 + 1.0) - row as f32)),
        other => panic!("no column {other}"),
    }
}

fn cpp_table(dir: &Path, writer: CppTableFixture) -> PathBuf {
    let path = dir.join(format!("{writer:?}.tbl"));
    TableOracle::table_write(writer, &path).expect("C++ writes the fixture");
    path
}

/// Every cell of `columns` in `rows` rows reads as `expected`, lazily and
/// in a whole-table load.
fn assert_cells(
    path: &Path,
    rows: usize,
    columns: &[&str],
    expected: impl Fn(&str, usize) -> ArrayValue,
) {
    let table = Table::open(TableOptions::new(path)).expect("casa-rs opens the table");
    assert_eq!(table.row_count(), rows);
    for &column in columns {
        let cells = table.column_accessor(column).expect("column");
        for row in 0..rows {
            assert_eq!(
                cells.array_cell(row).expect("lazy cell"),
                &expected(column, row),
                "{column} row {row}"
            );
        }
    }
    let whole = Table::open(TableOptions::new(path)).expect("casa-rs opens the table");
    for (row, record) in whole.rows().expect("all rows").iter().enumerate() {
        for &column in columns {
            assert_eq!(
                record.get(column),
                Some(&Value::Array(expected(column, row))),
                "whole-table {column} row {row}"
            );
        }
    }
}

#[test]
fn cr_mixed_size_columns_read_as_casacore_lays_them_out() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    for writer in [
        CppTableFixture::TiledShapeMixed,
        CppTableFixture::TiledShapeMixedBigEndian,
        CppTableFixture::TiledCellMixed,
        CppTableFixture::TiledCellMixedBigEndian,
    ] {
        let path = cpp_table(dir.path(), writer);
        TableOracle::table_verify(writer, &path).expect("CC: C++ reads its own table");
        assert_cells(&path, MIXED_ROWS, &MIXED_COLUMNS, |column, row| {
            mixed_cell(column, row, false)
        });
    }
    for writer in [
        CppTableFixture::TiledColumnNoDef,
        CppTableFixture::TiledColumnNoDefBigEndian,
    ] {
        let path = cpp_table(dir.path(), writer);
        TableOracle::table_verify(writer, &path).expect("CC: C++ reads its own table");
        assert_cells(&path, NODEF_ROWS, &NODEF_COLUMNS, nodef_cell);
    }
}

/// casa-rs patches data and flag in rows 1 and 3 in place; C++ reads the
/// patched cells and every other cell unchanged.
#[test]
fn rc_casacore_reads_mixed_size_columns_casa_rs_patched() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    for writer in [
        CppTableFixture::TiledShapeMixed,
        CppTableFixture::TiledShapeMixedBigEndian,
        CppTableFixture::TiledCellMixed,
        CppTableFixture::TiledCellMixedBigEndian,
    ] {
        let path = cpp_table(dir.path(), writer);
        let mut table = Table::open(TableOptions::new(&path)).expect("casa-rs opens the table");
        let mut row = table
            .row_accessor_mut()
            .prepare(&["data", "flag"])
            .expect("prepare data and flag");
        for patched_row in PATCHED_ROWS {
            row.seek(patched_row).expect("seek row");
            row.set_value_at(0, Value::Array(mixed_cell("data", patched_row, true)))
                .expect("set data");
            row.set_value_at(1, Value::Array(mixed_cell("flag", patched_row, true)))
                .expect("set flag");
        }
        row.flush().expect("flush rows");
        drop(row);
        table
            .prepare_write()
            .save_selected_rows(&["data", "flag"], &PATCHED_ROWS)
            .expect("casa-rs patches the rows in place");
        drop(table);
        TableOracle::table_verify(CppTableFixture::TiledShapeMixedPatched, &path)
            .expect("RC: C++ reads the patched cells");
        assert_cells(&path, MIXED_ROWS, &MIXED_COLUMNS, |column, row| {
            mixed_cell(column, row, true)
        });
    }
}

/// casa-rs rewrites each C++ table under the same manager in each byte
/// order; C++ reads every cell of the rewrite.
#[test]
fn rc_casacore_reads_mixed_size_columns_casa_rs_wrote() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let cases = [
        (
            CppTableFixture::TiledShapeMixed,
            DataManagerKind::TiledShapeStMan,
            vec![2, 3, 2],
        ),
        (
            CppTableFixture::TiledCellMixed,
            DataManagerKind::TiledCellStMan,
            vec![2, 2],
        ),
        (
            CppTableFixture::TiledColumnNoDef,
            DataManagerKind::TiledColumnStMan,
            vec![2, 3, 2],
        ),
    ];
    for (writer, manager, tile_shape) in cases {
        let source = cpp_table(dir.path(), writer);
        let table = Table::open(TableOptions::new(&source)).expect("casa-rs opens the table");
        for format in [EndianFormat::LittleEndian, EndianFormat::BigEndian] {
            let rewritten = dir
                .path()
                .join(format!("{writer:?}-{format:?}-rewritten.tbl"));
            table
                .save(
                    TableOptions::new(&rewritten)
                        .with_data_manager(manager)
                        .with_tile_shape(tile_shape.clone())
                        .with_endian_format(format),
                )
                .expect("casa-rs rewrites the table");
            TableOracle::table_verify(writer, &rewritten)
                .unwrap_or_else(|error| panic!("RC {writer:?} {format:?}: {error}"));
        }
    }
}

/// #764 review: an empty TiledColumnStMan is casacore's either way. CR:
/// casa-rs opens casacore's empty table and reads the first row C++ then
/// adds. RC: casa-rs empties the C++ table and rewrites it in each byte
/// order; C++ opens it with each column's shape and its one empty
/// hypercube, and adds the first row, which casa-rs reads back.
#[test]
fn an_empty_tiled_column_manager_is_casacores_both_ways() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let cases = [
        (
            CppTableFixture::TiledColumnNoDefEmpty,
            EndianFormat::LittleEndian,
        ),
        (
            CppTableFixture::TiledColumnNoDefEmptyBigEndian,
            EndianFormat::BigEndian,
        ),
    ];
    let source = cpp_table(dir.path(), CppTableFixture::TiledColumnNoDef);
    for (empty, format) in cases {
        let cpp = cpp_table(dir.path(), empty);
        let table = Table::open(TableOptions::new(&cpp)).expect("CR: casa-rs opens it");
        assert_eq!(table.row_count(), 0);
        assert!(table.rows().expect("CR: no rows").is_empty());
        drop(table);
        TableOracle::table_verify(empty, &cpp).expect("CC: C++ adds the first row");
        assert_cells(&cpp, 1, &NODEF_COLUMNS, nodef_cell);

        let mut table = Table::open(TableOptions::new(&source)).expect("casa-rs opens the table");
        table.remove_rows(&[0, 1, 2]).expect("remove every row");
        let rewritten = dir.path().join(format!("empty-{format:?}.tbl"));
        table
            .save(
                TableOptions::new(&rewritten)
                    .with_data_manager(DataManagerKind::TiledColumnStMan)
                    .with_tile_shape(vec![2, 3, 2])
                    .with_endian_format(format),
            )
            .expect("casa-rs writes the empty table");
        TableOracle::table_verify(empty, &rewritten)
            .unwrap_or_else(|error| panic!("RC {format:?}: {error}"));
        assert_cells(&rewritten, 1, &NODEF_COLUMNS, nodef_cell);
    }
}
