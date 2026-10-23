// SPDX-License-Identifier: LGPL-3.0-or-later
//! IncrementalStMan array columns casacore keeps in its array file (#852,
//! ISMIndColumn reading).
//!
//! casacore's IncrementalStMan stores an array column without the Direct
//! option, of fixed shape or not, as an `ISMIndColumn`: its bucket values
//! are the offsets of its cells in `table.f0i`. casa-rs once read a
//! fixed-shape one's offsets as its cells, and panicked.
//!
//! `tests/data/ism_indirect_arrays/{little,big}` hold casa-test-support's
//! `CppTableFixture::IsmIndirectArrays` and `IsmIndirectArraysBigEndian`
//! tables as casacore 3.7.1 wrote them: arrays of every element type in 3
//! rows, of fixed shapes `[1]`, `[3]` and `[2, 3]` without the Direct option
//! (`Float64_2x3_ind`), and of variable shape and rank 1 (`Float64_var`,
//! whose cell in row `row` has `row + 1` elements, and `Float64_undef`, the
//! same with row 0 never written, so undefined). Element `index`, in
//! storage (Fortran) order, of row `row` is made from `7 + 10 row + index`.
//! Regenerate them with those fixtures' writers when the fixture changes.

use std::path::{Path, PathBuf};

use casa_tables::{ColumnType, Table, TableOptions};
use casa_types::{ArrayValue, Complex32, Complex64, PrimitiveType, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

const ROWS: usize = 3;
const FILES: [&str; 4] = ["table.dat", "table.f0", "table.f0i", "table.info"];
const TYPES: [PrimitiveType; 12] = [
    PrimitiveType::Bool,
    PrimitiveType::UInt8,
    PrimitiveType::Int16,
    PrimitiveType::UInt16,
    PrimitiveType::Int32,
    PrimitiveType::UInt32,
    PrimitiveType::Int64,
    PrimitiveType::Float32,
    PrimitiveType::Float64,
    PrimitiveType::Complex32,
    PrimitiveType::Complex64,
    PrimitiveType::String,
];
const SHAPES: [(&str, &[usize]); 3] = [("1", &[1]), ("3", &[3]), ("2x3", &[2, 3])];

/// A copy in `dir` of the fixture table in byte order `endian`, so that
/// opening it leaves the checked-in files alone.
fn fixture_copy(endian: &str, dir: &Path) -> PathBuf {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/ism_indirect_arrays")
        .join(endian);
    let table = dir.join(endian);
    std::fs::create_dir_all(&table).expect("create table dir");
    for file in FILES {
        std::fs::copy(source.join(file), table.join(file)).expect("copy fixture file");
    }
    table
}

/// The cell of shape `shape` of a column of `data_type` in `row`.
fn cell(data_type: PrimitiveType, shape: &[usize], row: usize) -> ArrayValue {
    fn fortran<T>(shape: &[usize], values: impl Iterator<Item = T>) -> ArrayD<T> {
        ArrayD::from_shape_vec(IxDyn(shape).f(), values.collect()).expect("shape matches")
    }
    let elements = 0..shape.iter().product::<usize>();
    let base = move |index: usize| (7 + 10 * row + index) as i64;
    match data_type {
        PrimitiveType::Bool => ArrayValue::Bool(fortran(
            shape,
            elements.map(|i| (row + i).is_multiple_of(2)),
        )),
        PrimitiveType::UInt8 => ArrayValue::UInt8(fortran(shape, elements.map(|i| base(i) as u8))),
        PrimitiveType::Int16 => {
            ArrayValue::Int16(fortran(shape, elements.map(|i| -base(i) as i16)))
        }
        PrimitiveType::UInt16 => {
            ArrayValue::UInt16(fortran(shape, elements.map(|i| (65_535 - base(i)) as u16)))
        }
        PrimitiveType::Int32 => {
            ArrayValue::Int32(fortran(shape, elements.map(|i| (-1000 * base(i)) as i32)))
        }
        PrimitiveType::UInt32 => ArrayValue::UInt32(fortran(
            shape,
            elements.map(|i| (4_294_967_295 - base(i)) as u32),
        )),
        PrimitiveType::Int64 => {
            ArrayValue::Int64(fortran(shape, elements.map(|i| -(base(i) << 40))))
        }
        PrimitiveType::Float32 => {
            ArrayValue::Float32(fortran(shape, elements.map(|i| base(i) as f32 + 0.25)))
        }
        PrimitiveType::Float64 => {
            ArrayValue::Float64(fortran(shape, elements.map(|i| base(i) as f64)))
        }
        PrimitiveType::Complex32 => ArrayValue::Complex32(fortran(
            shape,
            elements.map(|i| Complex32::new(base(i) as f32, -0.5)),
        )),
        PrimitiveType::Complex64 => ArrayValue::Complex64(fortran(
            shape,
            elements.map(|i| Complex64::new(base(i) as f64, 0.125)),
        )),
        PrimitiveType::String => {
            ArrayValue::String(fortran(shape, elements.map(|i| format!("r{row}e{i}"))))
        }
    }
}

/// Each column of the fixture by name, with its cell in each row, `None`
/// where it is undefined.
fn columns() -> Vec<(String, Vec<Option<ArrayValue>>)> {
    let mut columns = Vec::new();
    for data_type in TYPES {
        for (name, shape) in SHAPES {
            columns.push((
                format!("{data_type:?}_{name}_ind"),
                (0..ROWS)
                    .map(|row| Some(cell(data_type, shape, row)))
                    .collect(),
            ));
        }
        let variable = |row: usize| cell(data_type, &[row + 1], row);
        columns.push((
            format!("{data_type:?}_var"),
            (0..ROWS).map(|row| Some(variable(row))).collect(),
        ));
        columns.push((
            format!("{data_type:?}_undef"),
            (0..ROWS)
                .map(|row| (row > 0).then(|| variable(row)))
                .collect(),
        ));
    }
    columns
}

#[test]
fn incremental_indirect_arrays_casacore_wrote_read_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    for endian in ["little", "big"] {
        let table = Table::open(TableOptions::new(fixture_copy(endian, dir.path())))
            .expect("open the casacore table");
        assert_eq!(table.row_count(), ROWS, "{endian}");
        let managers = table.data_manager_info();
        assert!(
            managers
                .iter()
                .all(|manager| manager.dm_type == "IncrementalStMan"),
            "{endian}: {managers:?}"
        );
        for (name, cells) in columns() {
            let schema = table.schema().and_then(|schema| schema.column(&name));
            let schema = schema.unwrap_or_else(|| panic!("{endian}: column {name}"));
            assert!(
                matches!(schema.column_type(), ColumnType::Array(_)) && !schema.options().direct,
                "{endian}: {name} is an indirect array column"
            );
            // A column read, then every cell of the loaded rows.
            let column = table.column_accessor(&name).expect("column");
            let read = column.array_cells_owned(&[0, 1, 2]).expect("read cells");
            for (row, expected) in cells.iter().enumerate() {
                assert_eq!(&read[row], expected, "{endian}: {name} row {row}");
                let cell = table.cell_accessor(row, &name).expect("cell");
                assert_eq!(
                    cell.value().expect("cell value"),
                    expected.clone().map(Value::Array).as_ref(),
                    "{endian}: {name} row {row}"
                );
                assert_eq!(
                    cell.is_defined(),
                    Ok(expected.is_some()),
                    "{endian}: {name} row {row} definedness"
                );
            }
        }
    }
}
