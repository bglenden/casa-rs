// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "cpp-interop-tests")]
//! TiledDataStMan interop (#730).
//!
//! casacore writes a table whose hypercolumn has an id column and two
//! hypercubes with different cell shapes. casa-rs has no TiledDataStMan
//! writer of its own, so the matrix is CR and RC: casa-rs reads the C++
//! table, and C++ reads it after casa-rs has patched rows in place.

use std::path::{Path, PathBuf};

use casa_tables::{Table, TableOptions};
use casa_test_support::{CppTableFixture, TableOracle, casacore_oracle_available};
use casa_types::{ArrayValue, ScalarValue, Value};
use ndarray::ShapeBuilder;

const ROWS: usize = 5;
const PATCHED_ROWS: [usize; 2] = [1, 3];

/// The fixture's cell in `row`: `100*row + i` at column-major index `i`,
/// negated when `patched` and `row` is one of [`PATCHED_ROWS`].
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
    ArrayValue::Float32(
        ndarray::Array::from_shape_vec(ndarray::IxDyn(&shape).f(), values).expect("cell shape"),
    )
}

fn id(row: usize) -> Value {
    Value::Scalar(ScalarValue::Int32(if row < 2 { 10 } else { 20 }))
}

/// The fixture's little- and big-endian writers.
const WRITERS: [CppTableFixture; 2] = [
    CppTableFixture::TiledDataStMan,
    CppTableFixture::TiledDataStManBigEndian,
];

fn cpp_table(dir: &Path, writer: CppTableFixture) -> PathBuf {
    let path = dir.join(format!("{writer:?}.tbl"));
    TableOracle::table_write(writer, &path).expect("C++ writes the TiledDataStMan fixture");
    path
}

fn assert_cells(path: &Path, patched: bool) {
    let table = Table::open(TableOptions::new(path)).expect("casa-rs opens the table");
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
fn cc_tiled_data_stman_fixture_verifies() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    for writer in WRITERS {
        let path = cpp_table(dir.path(), writer);
        TableOracle::table_verify(writer, &path).expect("C++ reads its own TiledDataStMan table");
    }
}

#[test]
fn cr_casa_rs_reads_both_hypercubes_and_their_ids() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    for writer in WRITERS {
        assert_cells(&cpp_table(dir.path(), writer), false);
    }
}

#[test]
fn rc_casacore_reads_rows_casa_rs_patched_in_place() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    for writer in WRITERS {
        patch_and_verify(&cpp_table(dir.path(), writer));
    }
}

fn patch_and_verify(path: &Path) {
    let mut table = Table::open(TableOptions::new(path)).expect("casa-rs opens the table");
    let mut row = table
        .row_accessor_mut()
        .prepare(&["data"])
        .expect("prepare data row");
    for patched_row in PATCHED_ROWS {
        row.seek(patched_row).expect("seek patched row");
        row.set_value_at(0, Value::Array(cell(patched_row, true)))
            .expect("set patched cell");
    }
    row.flush().expect("flush patched rows");
    drop(row);
    table
        .prepare_write()
        .save_selected_rows(&["data"], &PATCHED_ROWS)
        .expect("casa-rs patches the TiledDataStMan rows in place");
    drop(table);

    TableOracle::table_verify(CppTableFixture::TiledDataStManPatched, path)
        .expect("C++ reads the rows casa-rs patched, and the rows it left");
    assert_cells(path, true);
}
