// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed selected reads of cells whose axis 0 lies in several tiles.
//!
//! casacore lets a tile hold part of a cell's axis 0 (CASA's ALMA filler
//! tiles `WEIGHT` and `SIGMA` `[1, 131072]`). Each law writes cells whose
//! every element is distinct, tiled one, two or three elements along axis 0
//! (the last tile padding the axis), along with the usual tile that spans
//! it, and reads selected rows (out of order, repeated, in a last partial
//! row tile) through every typed destination, in both byte orders.

use casa_types::{ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use tempfile::tempdir;

use super::*;
use crate::{
    ColumnSchema, DataManagerKind, EndianFormat, SelectedArray1DCells, SelectedArray2DCells, Table,
    TableOptions, TableSchema,
};

const ENDIANS: [EndianFormat; 2] = [EndianFormat::BigEndian, EndianFormat::LittleEndian];
const PRIMITIVES: [PrimitiveType; 4] = [
    PrimitiveType::Complex32,
    PrimitiveType::Float32,
    PrimitiveType::Float64,
    PrimitiveType::Bool,
];

/// The value code of `row`'s element `index` (`[axis 0]` or
/// `[axis 0, channel]`): distinct for every element of every row.
fn code(row: usize, index: &[usize]) -> usize {
    row * 1000 + index.get(1).copied().unwrap_or(0) * 10 + index[0]
}

/// A column-major cell of `shape` and type `primitive` holding `row`'s
/// codes.
fn cell(primitive: PrimitiveType, shape: &[usize], row: usize) -> ArrayValue {
    let mut codes = Vec::new();
    let channels = shape.get(1).copied().unwrap_or(1);
    for channel in 0..channels {
        for axis0 in 0..shape[0] {
            codes.push(code(row, &[axis0, channel]));
        }
    }
    let shape = IxDyn(shape).f();
    match primitive {
        PrimitiveType::Complex32 => ArrayValue::Complex32(
            ArrayD::from_shape_vec(shape, codes.into_iter().map(complex).collect()).expect("cell"),
        ),
        PrimitiveType::Float32 => ArrayValue::Float32(
            ArrayD::from_shape_vec(shape, codes.into_iter().map(|c| c as f32).collect())
                .expect("cell"),
        ),
        PrimitiveType::Float64 => ArrayValue::Float64(
            ArrayD::from_shape_vec(shape, codes.into_iter().map(|c| c as f64).collect())
                .expect("cell"),
        ),
        PrimitiveType::Bool => ArrayValue::Bool(
            ArrayD::from_shape_vec(shape, codes.into_iter().map(flag).collect()).expect("cell"),
        ),
        _ => unreachable!(),
    }
}

fn complex(code: usize) -> Complex32 {
    Complex32::new(code as f32, -(code as f32))
}

fn flag(code: usize) -> bool {
    code.is_multiple_of(3)
}

/// Column `name_of(primitive)` per primitive.
fn name_of(primitive: PrimitiveType) -> &'static str {
    match primitive {
        PrimitiveType::Complex32 => "C",
        PrimitiveType::Float32 => "F",
        PrimitiveType::Float64 => "D",
        PrimitiveType::Bool => "B",
        _ => unreachable!(),
    }
}

/// A table of `rows` rows with a column of every primitive of cell
/// `shape`, saved by `manager` with `tile_shape` in byte order `endian`
/// and reopened lazily.
fn tiled_table(
    dir: &Path,
    shape: &[usize],
    rows: usize,
    manager: DataManagerKind,
    tile_shape: &[usize],
    endian: EndianFormat,
) -> Table {
    let schema = TableSchema::new(
        PRIMITIVES
            .iter()
            .map(|&primitive| {
                ColumnSchema::array_fixed(name_of(primitive), primitive, shape.to_vec())
            })
            .collect(),
    )
    .expect("schema");
    let mut table = Table::with_schema(schema);
    for row in 0..rows {
        table
            .add_row(RecordValue::new(
                PRIMITIVES
                    .iter()
                    .map(|&primitive| {
                        RecordField::new(
                            name_of(primitive),
                            Value::Array(cell(primitive, shape, row)),
                        )
                    })
                    .collect(),
            ))
            .expect("row");
    }
    let root = dir.join(format!("split_{tile_shape:?}_{endian:?}.table"));
    std::fs::create_dir_all(&root).expect("directory");
    table
        .save(
            TableOptions::new(&root)
                .with_data_manager(manager)
                .with_tile_shape(tile_shape.to_vec())
                .with_endian_format(endian),
        )
        .expect("save");
    let (_, header) = read_tiled_header(&root.join("table.f0")).expect("header");
    assert!(
        header
            .cubes
            .iter()
            .all(|cube| cube.cube_shape.is_empty() || cube.tile_shape == tile_shape),
        "tiled {tile_shape:?}"
    );
    Table::open(TableOptions::new(&root)).expect("open")
}

/// Assert that `values` (`code -> value` per primitive) hold the codes
/// `expected` gives for each output index.
fn assert_codes<T: PartialEq + std::fmt::Debug>(
    values: &[T],
    expected: &[usize],
    value: impl Fn(usize) -> T,
    context: &str,
) {
    assert_eq!(values.len(), expected.len(), "{context}");
    for (index, (actual, &code)) in values.iter().zip(expected).enumerate() {
        assert_eq!(*actual, value(code), "{context}: output {index}");
    }
}

/// 1-D cells of four elements, tiled one, three, four (spanning) or eight
/// (padding) elements along axis 0, by both managers that read them:
/// every selected cell is gathered whole, through the allocating and the
/// filling reads.
#[test]
fn one_dimensional_cells_are_gathered_from_their_axis0_tiles() {
    let rows = [22, 0, 5, 5, 13, 21, 3];
    let expected: Vec<usize> = rows
        .iter()
        .flat_map(|&row| (0..4).map(move |axis0| code(row, &[axis0])))
        .collect();
    for manager in [
        DataManagerKind::TiledShapeStMan,
        DataManagerKind::TiledColumnStMan,
    ] {
        for endian in ENDIANS {
            for tile_shape in [[1, 4], [3, 4], [4, 4], [8, 4], [1, 1]] {
                let dir = tempdir().expect("tempdir");
                let table = tiled_table(dir.path(), &[4], 23, manager, &tile_shape, endian);
                let context = format!("{manager:?} {endian:?} {tile_shape:?}");
                let read = |column| {
                    table
                        .get_array_cells_1d_typed_uncached(column, &rows)
                        .expect("read")
                };
                let SelectedArray1DCells::Complex32(c) = read("C") else {
                    panic!("Complex32")
                };
                assert_codes(c.values(), &expected, complex, &context);
                let SelectedArray1DCells::Float64(d) = read("D") else {
                    panic!("Float64")
                };
                assert_codes(d.values(), &expected, |c| c as f64, &context);
                let SelectedArray1DCells::Bool(b) = read("B") else {
                    panic!("Bool")
                };
                assert_codes(b.values(), &expected, flag, &context);

                let mut floats = vec![f32::NAN; 3];
                let shape = table
                    .fill_array_cells_1d_typed_uncached(
                        "F",
                        &rows,
                        SelectedArray1DCellsMut::Float32(&mut floats),
                    )
                    .expect("fill");
                assert_eq!((shape.row_count, shape.axis0_count), (rows.len(), 4));
                assert_codes(&floats, &expected, |c| c as f32, &context);
                let mut flags = Vec::new();
                table
                    .fill_array_cells_1d_typed_uncached(
                        "B",
                        &rows,
                        SelectedArray1DCellsMut::Bool(&mut flags),
                    )
                    .expect("fill");
                assert_codes(&flags, &expected, flag, &context);
            }
        }
    }
}

/// The codes a 2-D read of `rows`' channels `start..start + count` of
/// three-element axis 0 delivers, by output index.
fn expected_2d(rows: &[usize], start: usize, count: usize, row_major: bool) -> Vec<usize> {
    let mut expected = vec![0; rows.len() * count * 3];
    for (index, &row) in rows.iter().enumerate() {
        for channel in start..start + count {
            for axis0 in 0..3 {
                let sample = if row_major {
                    index * count + channel - start
                } else {
                    (channel - start) * rows.len() + index
                };
                expected[sample * 3 + axis0] = code(row, &[axis0, channel]);
            }
        }
    }
    expected
}

/// Read `rows`' channels `start..start + count` of every column of `table`
/// through every typed destination and the allocating read, and check
/// every element.
fn assert_2d_reads(table: &Table, rows: &[usize], start: usize, count: usize, context: &str) {
    for row_major in [false, true] {
        let expected = expected_2d(rows, start, count, row_major);
        let context = format!("{context} {start}+{count} row-major {row_major}");
        let (mut data, mut floats, mut flags) = (Vec::new(), Vec::new(), Vec::new());
        for primitive in [
            PrimitiveType::Complex32,
            PrimitiveType::Float32,
            PrimitiveType::Bool,
        ] {
            let destination = match (primitive, row_major) {
                (PrimitiveType::Complex32, false) => SelectedArray2DCellsMut::Complex32(&mut data),
                (PrimitiveType::Complex32, true) => {
                    SelectedArray2DCellsMut::RowChannelComplex32(&mut data)
                }
                (PrimitiveType::Float32, false) => SelectedArray2DCellsMut::Float32(&mut floats),
                (PrimitiveType::Float32, true) => {
                    SelectedArray2DCellsMut::RowChannelFloat32(&mut floats)
                }
                (PrimitiveType::Bool, false) => SelectedArray2DCellsMut::Bool(&mut flags),
                (PrimitiveType::Bool, true) => SelectedArray2DCellsMut::RowChannelBool(&mut flags),
                _ => unreachable!(),
            };
            let shape = table
                .fill_array_cells_2d_channel_range_typed_uncached(
                    name_of(primitive),
                    rows,
                    start,
                    count,
                    destination,
                )
                .expect("fill")
                .expect("defined cells");
            assert_eq!(
                (shape.row_count, shape.axis0_count, shape.channel_count),
                (rows.len(), 3, count)
            );
        }
        assert_codes(&data, &expected, complex, &context);
        assert_codes(&floats, &expected, |c| c as f32, &context);
        assert_codes(&flags, &expected, flag, &context);
        if row_major {
            continue;
        }
        let mut doubles = Vec::new();
        table
            .fill_array_cells_2d_channel_range_typed_uncached(
                "D",
                rows,
                start,
                count,
                SelectedArray2DCellsMut::Float64(&mut doubles),
            )
            .expect("fill")
            .expect("defined cells");
        assert_codes(&doubles, &expected, |c| c as f64, &context);
        let read = |column| {
            table
                .get_array_cells_2d_channel_range_typed_uncached(column, rows, start, count)
                .expect("read")
                .expect("defined cells")
        };
        let SelectedArray2DCells::Complex32(c) = read("C") else {
            panic!("Complex32")
        };
        assert_codes(c.values(), &expected, complex, &context);
        let SelectedArray2DCells::Bool(b) = read("B") else {
            panic!("Bool")
        };
        assert_codes(b.values(), &expected, flag, &context);
    }
}

/// 2-D cells of three correlations and six channels, tiled one or two
/// correlations (the last tile padding the axis), all three, or four
/// (padding), with channel tiles that split the selected ranges, and
/// one-pixel tiles whose booleans stay packed: every selected channel range
/// is gathered whole, through the shared cache for a few rows and streamed
/// for many.
#[test]
fn two_dimensional_cells_are_gathered_from_their_axis0_tiles() {
    let few = [3, 2, 2];
    let many: Vec<usize> = (0..40).rev().step_by(3).chain([39, 0]).collect();
    for endian in ENDIANS {
        for tile_shape in [[1, 2, 2], [2, 4, 2], [3, 2, 2], [4, 4, 2], [1, 1, 1]] {
            let dir = tempdir().expect("tempdir");
            let table = tiled_table(
                dir.path(),
                &[3, 6],
                40,
                DataManagerKind::TiledShapeStMan,
                &tile_shape,
                endian,
            );
            for rows in [&few[..], &many[..]] {
                for (start, count) in [(0, 6), (1, 3), (5, 1), (2, 2)] {
                    let context = format!("{endian:?} {tile_shape:?} {} rows", rows.len());
                    assert_2d_reads(&table, rows, start, count, &context);
                }
            }
        }
    }
}

/// A streamed read takes each row tile's selected channel tiles, every
/// tile along axis 0 of each, as one run of consecutive buckets: one read
/// a row tile, of exactly the tiles it needs.
#[test]
fn a_streamed_read_takes_a_row_tile_split_along_axis0_as_one_run() {
    for primitive in [PrimitiveType::Float32, PrimitiveType::Bool] {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "DATA",
            primitive,
            vec![4, 6],
        )])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        for row in 0..56 {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "DATA",
                    Value::Array(cell(primitive, &[4, 6], row)),
                )]))
                .expect("row");
        }
        let dir = tempdir().expect("tempdir");
        let root = dir.path().join("streamed_split.table");
        std::fs::create_dir_all(&root).expect("directory");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::TiledShapeStMan)
                    .with_tile_shape(vec![1, 2, 2]),
            )
            .expect("save");
        let table = Table::open(TableOptions::new(&root)).expect("open");
        // Fourteen row tiles, nonadjacent; channels 1..5 lie in channel
        // tiles 0 to 2, each four tiles along axis 0.
        let mut rows: Vec<usize> = (0..56).step_by(4).rev().collect();
        rows.push(0);
        let (mut floats, mut flags) = (Vec::new(), Vec::new());
        let destination = match primitive {
            PrimitiveType::Float32 => SelectedArray2DCellsMut::Float32(&mut floats),
            _ => SelectedArray2DCellsMut::Bool(&mut flags),
        };
        STREAMED_READ_COUNTS.with(|counts| counts.set((0, 0)));
        table
            .fill_array_cells_2d_channel_range_typed_uncached("DATA", &rows, 1, 4, destination)
            .expect("fill")
            .expect("defined cells");
        let (calls, bytes) = STREAMED_READ_COUNTS.with(|counts| counts.get());
        // A tile holds 1 x 2 x 2 pixels: 16 bytes of Float32, one byte of
        // packed Bool.
        let tile_bytes = if primitive == PrimitiveType::Bool {
            1
        } else {
            16
        };
        assert_eq!(calls, 14, "{primitive:?}: one read a row tile");
        assert_eq!(
            bytes,
            14 * 3 * 4 * tile_bytes,
            "{primitive:?}: only the tiles needed"
        );
        let mut expected = vec![0; rows.len() * 4 * 4];
        for (index, &row) in rows.iter().enumerate() {
            for channel in 1..5 {
                for axis0 in 0..4 {
                    expected[((channel - 1) * rows.len() + index) * 4 + axis0] =
                        code(row, &[axis0, channel]);
                }
            }
        }
        match primitive {
            PrimitiveType::Float32 => assert_codes(&floats, &expected, |c| c as f32, "streamed"),
            _ => assert_codes(&flags, &expected, flag, "streamed"),
        }
    }
}
