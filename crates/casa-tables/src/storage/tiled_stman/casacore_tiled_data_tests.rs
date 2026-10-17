// SPDX-License-Identifier: LGPL-3.0-or-later
//! `TiledDataStMan` laws on tables casacore wrote (#730).
//!
//! `tests/data/tiled_data_stman/{little,big}` hold the
//! `CppTableFixture::TiledDataStMan` table of `casa-test-support`, as
//! casacore 3.7.1 wrote it in each byte order: a Float32 `data` column and
//! an Int32 id column `id` in two hypercubes, id 10 with \[2,3\] cells
//! (rows 0-1) and id 20 with \[3,2\] cells (rows 2-4); cell `row` holds
//! `100*row + i` at column-major index `i`.
//!
//! `multi/{little,big}` hold `CppTableFixture::TiledDataStManMulti`:
//! data columns of equal pixel size whose hypercolumn order differs from
//! the table description's (#756 review), and `scalar` holds
//! `CppTableFixture::TiledDataStManScalar`, a scalar data column over two
//! row axes. Regenerate them with those fixtures' writers when a fixture
//! changes.

use std::path::{Path, PathBuf};

use casa_types::{ArrayValue, ScalarValue, Value};
use ndarray::ShapeBuilder;
use tempfile::tempdir;

use super::*;
use crate::{Table, TableOptions};

const ENDIANS: [&str; 2] = ["little", "big"];
const FILES: [&str; 4] = ["table.dat", "table.f0", "table.f0_TSM0", "table.info"];
const ROWS: usize = 5;
const PATCHED_ROWS: [usize; 2] = [1, 3];

/// The fixture table at `name` under `tests/data/tiled_data_stman`.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/tiled_data_stman")
        .join(name)
}

/// A writable copy in `dir` of the fixture table at `name`.
fn copy_fixture(name: &str, dir: &Path) -> PathBuf {
    let table = dir.join(format!("{}.tbl", name.replace('/', "-")));
    std::fs::create_dir_all(&table).expect("create table dir");
    for file in FILES {
        std::fs::copy(fixture(name).join(file), table.join(file)).expect("copy fixture file");
    }
    table
}

fn cell(row: usize, patched: bool) -> ArrayValue {
    let shape = if row < 2 { [2, 3] } else { [3, 2] };
    let sign = if patched && PATCHED_ROWS.contains(&row) {
        -1.0
    } else {
        1.0
    };
    let values = (0..6)
        .map(|index| sign * (100.0 * row as f32 + index as f32))
        .collect();
    ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&shape).f(), values).expect("cell shape"))
}

fn id(row: usize) -> Value {
    Value::Scalar(ScalarValue::Int32(if row < 2 { 10 } else { 20 }))
}

fn assert_cells(table_path: &Path, patched: bool) {
    let table = Table::open(TableOptions::new(table_path)).expect("open TiledDataStMan table");
    assert_eq!(table.row_count(), ROWS);
    let data = table.column_accessor("data").expect("data column");
    let ids = table.column_accessor("id").expect("id column");
    for row in 0..ROWS {
        assert_eq!(
            data.array_cell(row).expect("data cell"),
            &cell(row, patched),
            "data row {row}"
        );
        assert_eq!(
            ids.get(row).expect("id cell"),
            Some(&id(row)),
            "id row {row}"
        );
    }
}

#[test]
fn casacore_tiled_data_headers_read_and_rewrite_byte_for_byte() {
    for endian in ENDIANS {
        let header_path = fixture(endian).join("table.f0");
        let (variant, header) = read_tiled_header(&header_path).expect("read header");
        let TiledVariant::Data {
            nrrow_last,
            ref row_map,
            ref cube_map,
            ref pos_map,
        } = variant
        else {
            panic!("{endian}: expected a TiledDataStMan header, got {variant:?}");
        };
        assert_eq!(nrrow_last, ROWS as u64, "{endian}");
        assert_eq!(row_map, &[0, 2], "{endian}");
        assert_eq!(cube_map, &[0, 1], "{endian}");
        assert_eq!(pos_map, &[0, 0], "{endian}");
        assert_eq!(header.big_endian, endian == "big");
        let cube_ids: Vec<_> = header
            .cubes
            .iter()
            .map(|cube| cube.values.get("id").cloned())
            .collect();
        assert_eq!(cube_ids, [Some(id(0)), Some(id(2))], "{endian}");

        let written = serialize_tiled_header(&variant, &header).expect("serialize header");
        let original = std::fs::read(&header_path).expect("read header bytes");
        assert_eq!(&*written, original.as_slice(), "{endian}: rewritten header");
    }
}

#[test]
fn a_version_1_tiled_data_header_reads_its_uint_row_map() {
    let (_, header) =
        read_tiled_header(&fixture("little").join("table.f0")).expect("read fixture header");
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("table.f0");
    let mut io = AipsIo::open(&path, AipsOpenOption::New).expect("create header");
    io.putstart("TiledDataStMan", 1).expect("putstart");
    write_tiled_stman_base(&mut io, &header).expect("base header");
    io.put_u32(5).expect("nrrowLast");
    io.put_u32(2).expect("nused");
    write_block_u32(&mut io, &[0, 2]).expect("row map");
    write_block_u32(&mut io, &[0, 1]).expect("cube map");
    write_block_u32(&mut io, &[0, 0]).expect("position map");
    io.putend().expect("putend");
    io.close().expect("close header");

    let (variant, read) = read_tiled_header(&path).expect("read version 1 header");
    let TiledVariant::Data {
        nrrow_last,
        row_map,
        cube_map,
        pos_map,
    } = variant
    else {
        panic!("expected a TiledDataStMan header");
    };
    assert_eq!(
        (nrrow_last, row_map, cube_map, pos_map),
        (5, vec![0, 2], vec![0, 1], vec![0, 0])
    );
    assert_eq!(read.cubes.len(), 2);
}

#[test]
fn a_casacore_tiled_data_table_reads_and_patches_rows_in_place() {
    let dir = tempdir().expect("tempdir");
    for endian in ENDIANS {
        let table_path = copy_fixture(endian, dir.path());
        assert_cells(&table_path, false);

        let mut table = Table::open(TableOptions::new(&table_path)).expect("open table");
        let mut row = table
            .row_accessor_mut()
            .prepare(&["data"])
            .expect("prepare data row");
        for patched_row in PATCHED_ROWS {
            row.seek(patched_row).expect("seek row");
            row.set_value_at(0, Value::Array(cell(patched_row, true)))
                .expect("set cell");
        }
        row.flush().expect("flush rows");
        drop(row);
        table
            .prepare_write()
            .save_selected_rows(&["data"], &PATCHED_ROWS)
            .expect("patch TiledDataStMan rows in place");
        drop(table);

        assert_cells(&table_path, true);
        assert_eq!(
            std::fs::read(table_path.join("table.f0")).expect("patched header"),
            std::fs::read(fixture(endian).join("table.f0")).expect("fixture header"),
            "{endian}: an in-place patch leaves the header as casacore wrote it"
        );
    }
}

const MULTI_ROWS: usize = 4;
const MULTI_COLUMNS: [&str; 4] = ["a", "b", "c", "d"];

/// The multi fixture's cell of `column` in `row`; `patched` negates `a` in
/// [`PATCHED_ROWS`].
fn multi_cell(column: &str, row: usize, patched: bool) -> ArrayValue {
    let shape = IxDyn(if row < 2 { &[2, 3] } else { &[3, 2] }).f();
    let base = 100.0 * row as f32;
    let index = (0..6).map(|index| index as f32);
    match column {
        "a" => {
            let sign = if patched && PATCHED_ROWS.contains(&row) {
                -1.0
            } else {
                1.0
            };
            let values = index.map(|i| sign * (1000.0 + base + i)).collect();
            ArrayValue::Float32(ArrayD::from_shape_vec(shape, values).expect("a"))
        }
        "b" => ArrayValue::Float32(
            ArrayD::from_shape_vec(shape, index.map(|i| 2000.0 + base + i).collect()).expect("b"),
        ),
        "c" => ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                shape,
                index
                    .map(|i| casa_types::Complex32::new(3000.0 + base + i, -(i + 1.0)))
                    .collect(),
            )
            .expect("c"),
        ),
        "d" => ArrayValue::Int32(
            ArrayD::from_shape_vec(
                shape,
                index.map(|i| 4000 + 100 * row as i32 + i as i32).collect(),
            )
            .expect("d"),
        ),
        other => panic!("no multi column {other}"),
    }
}

fn assert_multi_cells(table_path: &Path, patched: bool) {
    let table = Table::open(TableOptions::new(table_path)).expect("open multi table");
    assert_eq!(table.row_count(), MULTI_ROWS);
    for column in MULTI_COLUMNS {
        let cells = table.column_accessor(column).expect("data column");
        for row in 0..MULTI_ROWS {
            assert_eq!(
                cells.array_cell(row).expect("lazy cell"),
                &multi_cell(column, row, patched),
                "{column} row {row}"
            );
        }
    }
    let ids = table.column_accessor("id").expect("id column");
    for row in 0..MULTI_ROWS {
        assert_eq!(
            ids.get(row).expect("id cell"),
            Some(&id(row)),
            "id row {row}"
        );
    }
    // The whole-table load decodes every column at once, by the same layout.
    let whole = Table::open(TableOptions::new(table_path)).expect("open multi table");
    for (row, record) in whole.rows().expect("all rows").iter().enumerate() {
        for column in MULTI_COLUMNS {
            assert_eq!(
                record.get(column),
                Some(&Value::Array(multi_cell(column, row, patched))),
                "whole-table {column} row {row}"
            );
        }
        assert_eq!(record.get("id"), Some(&id(row)), "whole-table id row {row}");
    }
}

/// Fable and Astra on #756: casacore lays equal-size data columns out in
/// descending hypercolumn position (here c, d, a, b), and a stray field `a`
/// in a values record does not make `a` an id column.
#[test]
fn equal_size_data_columns_read_as_casacore_lays_them_out() {
    for endian in ENDIANS {
        assert_multi_cells(&fixture(&format!("multi/{endian}")), false);
    }
}

/// Astra on #756: patching one data column writes that column's bytes and
/// no other column's.
#[test]
fn patching_one_data_column_leaves_the_others_untouched() {
    let dir = tempdir().expect("tempdir");
    for endian in ENDIANS {
        let name = format!("multi/{endian}");
        let table_path = copy_fixture(&name, dir.path());
        let mut table = Table::open(TableOptions::new(&table_path)).expect("open table");
        let mut row = table.row_accessor_mut().prepare(&["a"]).expect("prepare a");
        for patched_row in PATCHED_ROWS {
            row.seek(patched_row).expect("seek row");
            row.set_value_at(0, Value::Array(multi_cell("a", patched_row, true)))
                .expect("set a");
        }
        row.flush().expect("flush rows");
        drop(row);
        table
            .prepare_write()
            .save_selected_rows(&["a"], &PATCHED_ROWS)
            .expect("patch a in place");
        drop(table);
        assert_multi_cells(&table_path, true);
        assert_eq!(
            std::fs::read(table_path.join("table.f0")).expect("patched header"),
            std::fs::read(fixture(&name).join("table.f0")).expect("fixture header"),
            "{endian}: the header is as casacore wrote it"
        );
    }
}

/// Astra on #756: casacore spreads a scalar data column's rows over every
/// hypercube axis, so casa-rs refuses the column rather than misread it.
#[test]
fn a_scalar_data_column_over_two_row_axes_is_refused_not_misread() {
    let refusal = match Table::open(TableOptions::new(fixture("scalar"))) {
        Err(error) => error.to_string(),
        Ok(table) => match table
            .column_accessor("s")
            .and_then(|s| s.get(0).map(|cell| cell.cloned()))
        {
            Err(error) => error.to_string(),
            Ok(cell) => panic!("the scalar column read as {cell:?}"),
        },
    };
    assert!(refusal.contains("scalar"), "{refusal}");
}
