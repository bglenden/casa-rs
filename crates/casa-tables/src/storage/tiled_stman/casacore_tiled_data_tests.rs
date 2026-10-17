// SPDX-License-Identifier: LGPL-3.0-or-later
//! `TiledDataStMan` laws on tables casacore wrote (#730).
//!
//! `tests/data/tiled_data_stman/{little,big}` hold the
//! `CppTableFixture::TiledDataStMan` table of `casa-test-support`, as
//! casacore 3.7.1 wrote it in each byte order: a Float32 `data` column and
//! an Int32 id column `id` in two hypercubes, id 10 with \[2,3\] cells
//! (rows 0-1) and id 20 with \[3,2\] cells (rows 2-4); cell `row` holds
//! `100*row + i` at column-major index `i`. Regenerate them with that
//! fixture's writers when the fixture changes.

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

fn fixture(endian: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/tiled_data_stman")
        .join(endian)
}

/// A writable copy of the fixture table in `dir`.
fn copy_fixture(endian: &str, dir: &Path) -> PathBuf {
    let table = dir.join(format!("{endian}.tbl"));
    std::fs::create_dir_all(&table).expect("create table dir");
    for file in FILES {
        std::fs::copy(fixture(endian).join(file), table.join(file)).expect("copy fixture file");
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
