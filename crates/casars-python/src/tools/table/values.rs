// SPDX-License-Identifier: LGPL-3.0-or-later
//! A table's cells and keywords as casatools hands them to Python.
//!
//! casatools promotes on the way out: integer cells and columns become
//! `int64`, real ones `float64`, complex ones `complex128`, as probed in
//! CASA 6.7.6.14 (`tools/casa-compat/table_tool_oracle.py`). Arrays keep
//! casacore's axis order, and a column's rows come last
//! ([`stack_rows`]). A table keyword is the path of its table, written
//! `Table: <absolute path>`.

use std::path::Path;

use casa_tables::add_directory;
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use num_complex::Complex64;

use super::state::Fail;
use crate::convert::value::CasaValue;

/// `array`'s values in Fortran order, converted, with its shape.
fn fortran<T: Copy, U>(array: &ArrayD<T>, convert: impl Fn(T) -> U) -> (Vec<usize>, Vec<U>) {
    (
        array.shape().to_vec(),
        array.t().iter().map(|value| convert(*value)).collect(),
    )
}

fn complex32(value: num_complex::Complex32) -> Complex64 {
    Complex64::new(f64::from(value.re), f64::from(value.im))
}

/// An array cell as casatools returns it.
pub(super) fn array(array: &ArrayValue) -> CasaValue {
    let ints = |(shape, values)| CasaValue::Ints { shape, values };
    let floats = |(shape, values)| CasaValue::Floats { shape, values };
    let complexes = |(shape, values)| CasaValue::Complexes { shape, values };
    match array {
        ArrayValue::Bool(values) => {
            let (shape, values) = fortran(values, |flag| flag);
            CasaValue::Bools { shape, values }
        }
        ArrayValue::UInt8(values) => ints(fortran(values, i64::from)),
        ArrayValue::UInt16(values) => ints(fortran(values, i64::from)),
        ArrayValue::UInt32(values) => ints(fortran(values, i64::from)),
        ArrayValue::Int16(values) => ints(fortran(values, i64::from)),
        ArrayValue::Int32(values) => ints(fortran(values, i64::from)),
        ArrayValue::Int64(values) => ints(fortran(values, |value| value)),
        ArrayValue::Float32(values) => floats(fortran(values, f64::from)),
        ArrayValue::Float64(values) => floats(fortran(values, |value| value)),
        ArrayValue::Complex32(values) => complexes(fortran(values, complex32)),
        ArrayValue::Complex64(values) => complexes(fortran(values, |value| value)),
        ArrayValue::String(values) => CasaValue::TextArray {
            shape: values.shape().to_vec(),
            values: values.t().iter().cloned().collect(),
        },
    }
}

/// A cell or keyword value; `owner` is the table directory a table keyword
/// is relative to.
pub(super) fn value(value: &Value, owner: &Path) -> CasaValue {
    match value {
        Value::Scalar(scalar) => CasaValue::from_scalar(scalar),
        Value::Array(cells) => array(cells),
        Value::Record(fields) => record(fields, owner),
        Value::TableRef(stored) => {
            let stored = stored.strip_prefix("Table: ").unwrap_or(stored);
            CasaValue::Text(format!(
                "Table: {}",
                add_directory(stored, owner).to_string_lossy()
            ))
        }
    }
}

/// A record as a `dict` whose keys are sorted, as casatools sorts them.
pub(super) fn record(fields: &RecordValue, owner: &Path) -> CasaValue {
    CasaValue::record(
        fields
            .fields()
            .iter()
            .map(|field| (field.name.clone(), value(&field.value, owner))),
    )
}

/// A column of no rows: an empty array of the column's type, whatever its
/// cells' shape.
pub(super) fn empty_column(data_type: Option<PrimitiveType>) -> CasaValue {
    let shape = vec![0];
    match data_type {
        Some(PrimitiveType::String) => CasaValue::TextArray {
            shape,
            values: Vec::new(),
        },
        Some(PrimitiveType::Complex32 | PrimitiveType::Complex64) => CasaValue::Complexes {
            shape,
            values: Vec::new(),
        },
        Some(PrimitiveType::Float32 | PrimitiveType::Float64) => CasaValue::Floats {
            shape,
            values: Vec::new(),
        },
        Some(PrimitiveType::Bool) | None => CasaValue::Bools {
            shape,
            values: Vec::new(),
        },
        Some(_) => CasaValue::Ints {
            shape,
            values: Vec::new(),
        },
    }
}

/// The scalar cells of a column as one array, as `getcol` returns them.
pub(super) fn scalar_column(cells: Vec<ScalarValue>) -> CasaValue {
    let rows = vec![cells.len()];
    match cells.first() {
        None => CasaValue::Bools {
            shape: vec![0],
            values: Vec::new(),
        },
        Some(ScalarValue::Bool(_)) => CasaValue::Bools {
            shape: rows,
            values: cells
                .iter()
                .map(|cell| matches!(cell, ScalarValue::Bool(true)))
                .collect(),
        },
        Some(ScalarValue::String(_)) => CasaValue::TextArray {
            shape: rows,
            values: cells.iter().map(string_of).collect(),
        },
        Some(ScalarValue::Complex32(_) | ScalarValue::Complex64(_)) => CasaValue::Complexes {
            shape: rows,
            values: cells
                .iter()
                .map(|cell| match CasaValue::from_scalar(cell) {
                    CasaValue::Complex(value) => value,
                    _ => Complex64::new(0.0, 0.0),
                })
                .collect(),
        },
        Some(
            ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_),
        ) => CasaValue::Ints {
            shape: rows,
            values: cells
                .iter()
                .map(|cell| match CasaValue::from_scalar(cell) {
                    CasaValue::Int(value) => value,
                    _ => 0,
                })
                .collect(),
        },
        Some(ScalarValue::Float32(_) | ScalarValue::Float64(_)) => CasaValue::Floats {
            shape: rows,
            values: cells
                .iter()
                .map(|cell| match CasaValue::from_scalar(cell) {
                    CasaValue::Float(value) => value,
                    _ => 0.0,
                })
                .collect(),
        },
    }
}

fn string_of(cell: &ScalarValue) -> String {
    match cell {
        ScalarValue::String(text) => text.clone(),
        _ => String::new(),
    }
}

/// The array cells of a column stacked with the rows last, as `getcol`
/// returns them: row `r` of cells of shape `[a, b]` is `result[:, :, r]`.
/// Cells of different shapes are casacore's "shapes not conforming".
pub(super) fn stack_rows(cells: &[CasaValue]) -> Result<CasaValue, Fail> {
    fn shape_of(cell: &CasaValue) -> Option<&[usize]> {
        match cell {
            CasaValue::Ints { shape, .. }
            | CasaValue::Bools { shape, .. }
            | CasaValue::TextArray { shape, .. }
            | CasaValue::Floats { shape, .. }
            | CasaValue::Complexes { shape, .. } => Some(shape),
            _ => None,
        }
    }
    let Some(first) = cells.first().and_then(shape_of) else {
        return Err(Fail::Runtime(
            "Table DataManager error: Internal error: StManIndArray::get/put shapes not conforming"
                .into(),
        ));
    };
    if cells.iter().any(|cell| shape_of(cell) != Some(first)) {
        return Err(Fail::Runtime(
            "Table DataManager error: Internal error: StManIndArray::get/put shapes not conforming"
                .into(),
        ));
    }
    let mut shape = first.to_vec();
    shape.push(cells.len());
    macro_rules! stacked {
        ($variant:ident) => {{
            let mut values = Vec::new();
            for cell in cells {
                if let CasaValue::$variant { values: row, .. } = cell {
                    values.extend(row.iter().cloned());
                }
            }
            CasaValue::$variant { shape, values }
        }};
    }
    Ok(match &cells[0] {
        CasaValue::Ints { .. } => stacked!(Ints),
        CasaValue::Bools { .. } => stacked!(Bools),
        CasaValue::TextArray { .. } => stacked!(TextArray),
        CasaValue::Complexes { .. } => stacked!(Complexes),
        _ => stacked!(Floats),
    })
}
