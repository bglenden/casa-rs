// SPDX-License-Identifier: LGPL-3.0-or-later
//! The every-type array fixtures of the table cross matrix.
//!
//! Fixed-shape arrays of every element type, of shapes `[1]`, `[3]` and
//! `[2, 3]`, in [`ELEMENT_ROWS`] rows, as
//! `casacore_cpp_table_element_arrays.cpp` makes them. A column is named
//! `<type>_<shape>` (`Float64_2x3`) when it is stored directly and
//! `<type>_<shape>_ind` when it is stored indirectly, in its storage
//! manager's array file. Element `index`, in storage (Fortran) order, of row
//! `row` is made from `7 + 10 row + index`, so `Float64_1` holds `[7.0]` in
//! row 0.

use std::collections::HashMap;
use std::path::Path;

use casa_tables::{
    ColumnOptions, ColumnOverrides, ColumnSchema, DataManagerKind, EndianFormat,
    GeneratedArrayColumn, GeneratedArrayValueRun, Table, TableError, TableOptions, TableSchema,
};
use casa_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, Value,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::CppTableFixture;
use crate::table_interop::TableFixture;

/// The element types of the every-type fixtures.
pub const ELEMENT_TYPES: [PrimitiveType; 12] = [
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

/// The cell shapes of the every-type fixtures, with the names their columns
/// take.
pub const ELEMENT_SHAPES: [(&str, &[usize]); 3] = [("1", &[1]), ("3", &[3]), ("2x3", &[2, 3])];

/// Rows of the every-type fixtures.
pub const ELEMENT_ROWS: usize = 3;

/// How a column of an every-type fixture stores its cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElementStorage {
    /// In the storage manager's own data (the Direct option).
    Direct,
    /// In the storage manager's array file (`table.fNi`).
    Indirect,
}

impl ElementStorage {
    /// The suffix of the column names.
    fn suffix(self) -> &'static str {
        match self {
            Self::Direct => "",
            Self::Indirect => "_ind",
        }
    }

    /// The column options.
    fn options(self) -> ColumnOptions {
        ColumnOptions {
            direct: self == Self::Direct,
            undefined: false,
        }
    }
}

/// One column of an every-type fixture.
#[derive(Debug, Clone)]
pub struct ElementColumn {
    /// The column name, such as `Float64_2x3` or `Float64_2x3_ind`.
    pub name: String,
    /// The element type.
    pub data_type: PrimitiveType,
    /// The cell shape.
    pub shape: &'static [usize],
    /// How the column stores its cells.
    pub storage: ElementStorage,
}

impl ElementColumn {
    /// The column's schema.
    pub fn schema(&self) -> ColumnSchema {
        ColumnSchema::array_fixed(self.name.as_str(), self.data_type, self.shape.to_vec())
            .with_options(self.storage.options())
            .expect("fixed-shape array options")
    }

    /// The column's cell in `row`.
    pub fn cell(&self, row: usize) -> ArrayValue {
        element_array(self.data_type, self.shape, row)
    }
}

/// The columns of an every-type fixture: every element type at every shape,
/// for each of `storages` in turn.
pub fn element_columns(storages: &[ElementStorage]) -> Vec<ElementColumn> {
    storages
        .iter()
        .flat_map(|&storage| {
            ELEMENT_TYPES.into_iter().flat_map(move |data_type| {
                ELEMENT_SHAPES
                    .into_iter()
                    .map(move |(name, shape)| ElementColumn {
                        name: format!("{data_type:?}_{name}{}", storage.suffix()),
                        data_type,
                        shape,
                        storage,
                    })
            })
        })
        .collect()
}

/// The cell of an every-type column of type `data_type` and shape `shape`
/// in `row`.
pub fn element_array(data_type: PrimitiveType, shape: &[usize], row: usize) -> ArrayValue {
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

/// The schema of the every-type columns of `storages`.
pub fn element_arrays_schema(storages: &[ElementStorage]) -> TableSchema {
    TableSchema::new(
        element_columns(storages)
            .iter()
            .map(ElementColumn::schema)
            .collect(),
    )
    .expect("schema")
}

/// Save at `path` an every-type table of the columns of `storages`, stored
/// with `manager` in `endian` byte order, from generated array columns:
/// per-row closures, or with `runs` one run per row.
///
/// # Errors
///
/// The save's error.
pub fn save_generated_element_arrays(
    path: &Path,
    storages: &[ElementStorage],
    manager: DataManagerKind,
    endian: EndianFormat,
    runs: bool,
) -> Result<(), TableError> {
    let mut overrides = ColumnOverrides::for_row_count(ELEMENT_ROWS);
    for column in element_columns(storages) {
        let generated = if runs {
            let runs = (0..ELEMENT_ROWS)
                .map(|row| GeneratedArrayValueRun::new(row, Some(column.cell(row))))
                .collect();
            GeneratedArrayColumn::from_array_runs(ELEMENT_ROWS, runs)?
        } else {
            let column = column.clone();
            GeneratedArrayColumn::new(ELEMENT_ROWS, move |row| Some(column.cell(row)))
        };
        overrides.insert_generated_array(column.name, generated);
    }
    Table::with_schema(element_arrays_schema(storages))
        .prepare_write()
        .save_with_column_overrides(
            TableOptions::new(path)
                .with_data_manager(manager)
                .with_endian_format(endian),
            &HashMap::new(),
            &overrides,
        )
}

/// The every-type fixture of the columns of `storages`, which `cpp_fixture`
/// writes and verifies in casacore.
pub fn element_arrays_fixture(
    storages: &[ElementStorage],
    cpp_fixture: CppTableFixture,
) -> TableFixture {
    let columns = element_columns(storages);
    let rows = (0..ELEMENT_ROWS)
        .map(|row| {
            RecordValue::new(
                columns
                    .iter()
                    .map(|column| {
                        RecordField::new(column.name.as_str(), Value::Array(column.cell(row)))
                    })
                    .collect(),
            )
        })
        .collect();
    TableFixture {
        schema: element_arrays_schema(storages),
        rows,
        table_keywords: RecordValue::default(),
        column_keywords: vec![],
        cpp_fixture: Some(cpp_fixture),
        tile_shape: None,
        column_bindings: Vec::new(),
    }
}
