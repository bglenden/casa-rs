// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tile-order laws on tables casacore wrote (#755).
//!
//! `tests/data/tiled_tile_order/shape_mixed/{little,big}` hold the
//! `CppTableFixture::TiledShapeMixed` table of `casa-test-support` as
//! casacore 3.7.1 wrote it in each byte order: a TiledShapeStMan
//! hypercolumn `TiledMixed` defined as data \[flag, sigma, data, weight\]
//! and bound flag (Bool), data (Complex), weight, sigma (Float), so its tile
//! is data, weight, sigma, flag. `column_nodef/{little,big}` hold
//! `CppTableFixture::TiledColumnNoDef`: a TiledColumnStMan without a
//! definition, bound count (Int), flag (Bool), value (Double), amp (Float),
//! so its tile is value, amp, count, flag. Regenerate them with those
//! fixtures' writers when a fixture changes.

use std::path::{Path, PathBuf};

use casa_types::{ArrayValue, Complex32, Value};
use ndarray::ShapeBuilder;
use tempfile::tempdir;

use super::*;
use crate::{DataManagerKind, EndianFormat, SelectedArray2DCells, Table, TableOptions};

const ENDIANS: [&str; 2] = ["little", "big"];
const PATCHED_ROWS: [usize; 2] = [1, 3];
const MIXED_ROWS: usize = 4;
const MIXED_COLUMNS: [&str; 4] = ["flag", "data", "weight", "sigma"];
const NODEF_ROWS: usize = 3;
const NODEF_COLUMNS: [&str; 4] = ["count", "flag", "value", "amp"];

/// The fixture table at `name` under `tests/data/tiled_tile_order`.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/tiled_tile_order")
        .join(name)
}

/// A writable copy in `dir` of the fixture table at `name`.
fn copy_fixture(name: &str, dir: &Path) -> PathBuf {
    let table = dir.join(format!("{}.tbl", name.replace('/', "-")));
    std::fs::create_dir_all(&table).expect("create table dir");
    for entry in std::fs::read_dir(fixture(name)).expect("fixture dir") {
        let entry = entry.expect("fixture entry");
        std::fs::copy(entry.path(), table.join(entry.file_name())).expect("copy fixture file");
    }
    table
}

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

/// Every cell of `columns` in `rows` rows of the table at `table_path` is
/// `expected`'s, read lazily and in a whole-table load.
fn assert_cells(
    table_path: &Path,
    rows: usize,
    columns: &[&str],
    expected: impl Fn(&str, usize) -> ArrayValue,
) {
    let table = Table::open(TableOptions::new(table_path)).expect("open table");
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
    let whole = Table::open(TableOptions::new(table_path)).expect("open table");
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

/// #755: casacore lays a TiledShapeStMan tile out over the definition's
/// data columns by descending pixel size, Bool last and ties in descending
/// definition position, and a TiledColumnStMan without a definition the
/// same way over its bound columns.
#[test]
fn mixed_size_columns_read_as_casacore_lays_them_out() {
    for endian in ENDIANS {
        assert_cells(
            &fixture(&format!("shape_mixed/{endian}")),
            MIXED_ROWS,
            &MIXED_COLUMNS,
            |column, row| mixed_cell(column, row, false),
        );
        assert_cells(
            &fixture(&format!("column_nodef/{endian}")),
            NODEF_ROWS,
            &NODEF_COLUMNS,
            nodef_cell,
        );
    }
}

/// The typed channel-range reads take the same layout: Complex cells by
/// copy, Bool cells by the decode callback.
#[test]
fn typed_reads_of_mixed_size_columns_take_casacores_layout() {
    for endian in ENDIANS {
        let table = Table::open(TableOptions::new(fixture(&format!("shape_mixed/{endian}"))))
            .expect("open table");
        // Rows 0 and 1 have [2, 3] cells; channels 1 and 2 of axis 1, packed
        // [channel][row][axis0], at column-major index axis0 + 2·channel.
        let cells = || {
            (1..3).flat_map(|channel| {
                (0..2).flat_map(move |row| (0..2).map(move |axis0| (row, axis0 + 2 * channel)))
            })
        };
        let read = |column: &str| {
            table
                .column_accessor(column)
                .expect("column")
                .array_cells_2d_channel_range_typed_uncached(&[0, 1], 1, 2)
                .expect("typed read")
                .expect("defined cells")
        };
        let SelectedArray2DCells::Complex32(data) = read("data") else {
            panic!("{endian}: data reads as Complex32");
        };
        let expected = cells()
            .map(|(row, i)| Complex32::new(10.0 * row as f32 + i as f32, -(i as f32)))
            .collect::<Vec<_>>();
        assert_eq!(data.values(), expected, "{endian} data");
        let SelectedArray2DCells::Bool(flag) = read("flag") else {
            panic!("{endian}: flag reads as Bool");
        };
        let expected = cells()
            .map(|(row, i)| (i + row) % 2 == 1)
            .collect::<Vec<_>>();
        assert_eq!(flag.values(), expected, "{endian} flag");
    }
}

/// Patching two of a hypercolumn's mixed-size columns in place writes
/// their bytes where casacore laid them out and no other column's, and
/// leaves the header as casacore wrote it.
#[test]
fn patching_mixed_size_columns_leaves_the_others_untouched() {
    let dir = tempdir().expect("tempdir");
    for endian in ENDIANS {
        let name = format!("shape_mixed/{endian}");
        let table_path = copy_fixture(&name, dir.path());
        let mut table = Table::open(TableOptions::new(&table_path)).expect("open table");
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
            .expect("patch rows in place");
        drop(table);
        assert_cells(&table_path, MIXED_ROWS, &MIXED_COLUMNS, |column, row| {
            mixed_cell(column, row, true)
        });
        assert_eq!(
            std::fs::read(table_path.join("table.f0")).expect("patched header"),
            std::fs::read(fixture(&name).join("table.f0")).expect("fixture header"),
            "{endian}: the header is as casacore wrote it"
        );
    }
}

/// casa-rs's writer lays a TiledColumnStMan tile out as casacore does:
/// rewriting casacore's no-definition table with its tile shape gives
/// casacore's tiles byte for byte, and the rewrite reads back.
#[test]
fn casa_rs_writes_casacores_tiles_byte_for_byte() {
    let dir = tempdir().expect("tempdir");
    for (endian, format) in [
        ("little", EndianFormat::LittleEndian),
        ("big", EndianFormat::BigEndian),
    ] {
        let source = fixture(&format!("column_nodef/{endian}"));
        let table = Table::open(TableOptions::new(&source)).expect("open table");
        let rewritten = dir.path().join(format!("rewritten-{endian}.tbl"));
        table
            .save(
                TableOptions::new(&rewritten)
                    .with_data_manager(DataManagerKind::TiledColumnStMan)
                    .with_tile_shape(vec![2, 3, 2])
                    .with_endian_format(format),
            )
            .expect("rewrite the table");
        assert_cells(&rewritten, NODEF_ROWS, &NODEF_COLUMNS, nodef_cell);
        assert_eq!(
            std::fs::read(rewritten.join("table.f0_TSM0")).expect("rewritten tiles"),
            std::fs::read(source.join("table.f0_TSM0")).expect("casacore's tiles"),
            "{endian}: casa-rs's tiles are casacore's"
        );
    }
}

/// A manager casa-rs wrote with no rows lists every bound column's type,
/// as casacore's header does; one earlier casa-rs wrote without them still
/// reads.
#[test]
fn an_empty_tiled_manager_lists_its_column_types() {
    let dir = tempdir().expect("tempdir");
    let mut table =
        Table::open(TableOptions::new(fixture("column_nodef/little"))).expect("open table");
    let empty = dir.path().join("empty.tbl");
    table.remove_rows(&[0, 1, 2]).expect("remove every row");
    table
        .save(
            TableOptions::new(&empty)
                .with_data_manager(DataManagerKind::TiledColumnStMan)
                .with_tile_shape(vec![2, 3, 2]),
        )
        .expect("save the empty table");
    let (_, header) = read_tiled_header(&empty.join("table.f0")).expect("empty header");
    assert_eq!(header.col_data_types.len(), NODEF_COLUMNS.len());
    assert_eq!(
        Table::open(TableOptions::new(&empty))
            .expect("open the empty table")
            .row_count(),
        0
    );

    // The form earlier casa-rs wrote: no column types and no hypercube.
    let legacy = TiledStManHeader {
        col_data_types: Vec::new(),
        cubes: Vec::new(),
        ..header
    };
    write_tiled_header(
        &empty.join("table.f0"),
        &TiledVariant::Column {
            default_tile_shape: Vec::new(),
        },
        &legacy,
    )
    .expect("write a legacy empty header");
    assert_eq!(
        Table::open(TableOptions::new(&empty))
            .expect("open a legacy empty table")
            .rows()
            .expect("no rows")
            .len(),
        0
    );
}
