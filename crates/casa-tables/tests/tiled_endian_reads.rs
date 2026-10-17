// SPDX-License-Identifier: LGPL-3.0-or-later
//! A lazily read tiled cell is the cell written, in either byte order.
//!
//! Lazy array reads go through the shared tile cache, which holds a tile's
//! pixels in the host's byte order. A big-endian table once decoded those
//! pixels as big-endian a second time and read every cell byte-swapped
//! (found with #730's big-endian casacore fixture). Cells here span several
//! tiles, so both the one-tile and the assembled-cell decodes run.

use casa_tables::{
    ColumnOptions, ColumnSchema, DataManagerKind, EndianFormat, Table, TableOptions, TableSchema,
};
use casa_types::{ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

const ROWS: usize = 3;
const CELL: [usize; 2] = [4, 3];

fn float_cell(row: usize) -> ArrayValue {
    let values = (0..12).map(|index| 100.0 * row as f64 + index as f64 + 0.25);
    ArrayValue::Float64(
        ArrayD::from_shape_vec(IxDyn(&CELL).f(), values.collect()).expect("cell shape"),
    )
}

fn complex_cell(row: usize) -> ArrayValue {
    let values = (0..12).map(|index| Complex32::new(row as f32 + 0.5, index as f32 - 3.0));
    ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&CELL).f(), values.collect()).expect("cell shape"),
    )
}

fn schema(manager: DataManagerKind, primitive: PrimitiveType) -> TableSchema {
    let column = if manager == DataManagerKind::TiledColumnStMan {
        ColumnSchema::array_fixed("data", primitive, CELL.to_vec())
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .expect("fixed-shape column")
    } else {
        ColumnSchema::array_variable("data", primitive, Some(CELL.len()))
    };
    TableSchema::new(vec![column]).expect("schema")
}

/// Two-by-two cell tiles, so each [4,3] cell spans six tiles; a
/// TiledCellStMan's hypercube is one cell and has no row axis.
fn tile_shape(manager: DataManagerKind) -> Vec<usize> {
    if manager == DataManagerKind::TiledCellStMan {
        vec![2, 2]
    } else {
        vec![2, 2, 1]
    }
}

fn assert_lazy_reads_return_written_cells(cell: fn(usize) -> ArrayValue, primitive: PrimitiveType) {
    let dir = tempfile::tempdir().expect("tempdir");
    for manager in [
        DataManagerKind::TiledColumnStMan,
        DataManagerKind::TiledShapeStMan,
        DataManagerKind::TiledCellStMan,
    ] {
        for endian in [EndianFormat::BigEndian, EndianFormat::LittleEndian] {
            let path = dir.path().join(format!("{manager:?}-{endian:?}.tbl"));
            let mut table = Table::with_schema(schema(manager, primitive));
            for row in 0..ROWS {
                table
                    .add_row(RecordValue::new(vec![RecordField::new(
                        "data",
                        Value::Array(cell(row)),
                    )]))
                    .expect("add row");
            }
            table
                .save(
                    TableOptions::new(&path)
                        .with_data_manager(manager)
                        .with_endian_format(endian)
                        .with_tile_shape(tile_shape(manager)),
                )
                .expect("save tiled table");

            let reopened = Table::open(TableOptions::new(&path)).expect("reopen");
            let data = reopened.column_accessor("data").expect("data column");
            for row in 0..ROWS {
                assert_eq!(
                    data.array_cell(row).expect("lazy cell"),
                    &cell(row),
                    "{manager:?} {endian:?} row {row}"
                );
            }
        }
    }
}

#[test]
fn lazy_float_reads_of_tiled_tables_return_the_written_cells_in_either_byte_order() {
    assert_lazy_reads_return_written_cells(float_cell, PrimitiveType::Float64);
}

#[test]
fn lazy_complex_reads_of_tiled_tables_return_the_written_cells_in_either_byte_order() {
    assert_lazy_reads_return_written_cells(complex_cell, PrimitiveType::Complex32);
}
