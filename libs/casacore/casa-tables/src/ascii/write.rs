// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use casa_core::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};

use super::AsciiError;
use crate::{ArrayShapeContract, ColumnSchema, ColumnType, Table};

fn descriptor(column: &ColumnSchema) -> String {
    use PrimitiveType::*;
    let mut result = match column.data_type() {
        Some(Bool) => "B",
        Some(UInt8 | UInt16 | Int16) => "S",
        Some(Int32 | UInt32 | Int64) => "I",
        Some(Float32) => "R",
        Some(Float64) => "D",
        Some(Complex32) => "X",
        Some(Complex64) => "DX",
        Some(String) => "A",
        None => "REC",
    }
    .to_owned();
    if let ColumnType::Array(shape) = column.column_type() {
        result.push('[');
        if let ArrayShapeContract::Fixed { shape } = shape {
            result.push_str(
                &shape
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        result.push(']');
    }
    result
}

/// Write CASA's two header lines and text rows in the supplied order.
///
/// A separate header file is used only if provided and different from the
/// data file. Fixed array dimensions appear in the descriptor, and both
/// fixed and variable array cells are written in first-axis-fastest order.
/// This is CASA's bracketed export, not the flat-array import format.
pub fn write_ascii(
    table: &Table,
    rows: impl IntoIterator<Item = usize>,
    columns: &[&ColumnSchema],
    data: &Path,
    header: Option<&Path>,
    separator: &str,
) -> Result<(), AsciiError> {
    let separator = if separator.is_empty() { " " } else { separator };
    let err = |path: &Path, source| AsciiError::Io {
        path: path.into(),
        source,
    };
    let mut out = BufWriter::new(File::create(data).map_err(|error| err(data, error))?);
    if let Some(header) = header.filter(|header| *header != data) {
        let mut header_out =
            BufWriter::new(File::create(header).map_err(|error| err(header, error))?);
        write_header(&mut header_out, columns, separator)
            .and_then(|()| header_out.flush())
            .map_err(|error| err(header, error))?;
    } else {
        write_header(&mut out, columns, separator).map_err(|error| err(data, error))?;
    }
    let accessors = columns
        .iter()
        .map(|column| table.column_accessor(column.name()))
        .collect::<Result<Vec<_>, _>>()?;
    for row in rows {
        for (i, (column, accessor)) in columns.iter().zip(&accessors).enumerate() {
            if i > 0 {
                out.write_all(separator.as_bytes())
                    .map_err(|error| err(data, error))?;
            }
            let missing = || {
                AsciiError::Format(format!(
                    "Table DataManager error: Invalid operation: no value in row {row} of column {}",
                    column.name()
                ))
            };
            let result = match column.column_type() {
                ColumnType::Record => write_record(&mut out, &accessor.record_cell(row)?),
                ColumnType::Scalar => {
                    let value = accessor
                        .scalar_cells_owned_for_rows(&[row])?
                        .pop()
                        .flatten()
                        .ok_or_else(missing)?;
                    write_scalar(&mut out, &value, None)
                }
                ColumnType::Array(_) => {
                    let array = accessor
                        .array_cells_owned_uncached(&[row])?
                        .pop()
                        .flatten()
                        .ok_or_else(missing)?;
                    write_array(&mut out, &array, separator)
                }
            };
            result.map_err(|error| err(data, error))?;
        }
        out.write_all(b"\n").map_err(|error| err(data, error))?;
    }
    out.flush().map_err(|error| err(data, error))
}

fn write_header(
    out: &mut impl Write,
    columns: &[&ColumnSchema],
    separator: &str,
) -> io::Result<()> {
    let names = columns
        .iter()
        .map(|column| column.name())
        .collect::<Vec<_>>();
    let types = columns
        .iter()
        .map(|column| descriptor(column))
        .collect::<Vec<_>>();
    writeln!(out, "{}", names.join(separator))?;
    writeln!(out, "{}", types.join(separator))
}

// C++ defaultfloat uses significant digits and chooses between fixed and
// scientific notation by the rounded decimal exponent, like C's %g.
fn real(value: f64, precision: usize) -> String {
    if !value.is_finite() {
        return match (value.is_nan(), value.is_sign_negative()) {
            (true, true) => "-nan",
            (true, false) => "nan",
            (false, true) => "-inf",
            (false, false) => "inf",
        }
        .into();
    }
    let sci = format!("{:.*e}", precision - 1, value);
    let (mantissa, exp) = sci.split_once('e').expect("scientific notation");
    let exp: i32 = exp.parse().expect("decimal exponent");
    let trim = |num: String| {
        if num.contains('.') {
            num.trim_end_matches('0').trim_end_matches('.').to_owned()
        } else {
            num
        }
    };
    if exp < -4 || exp >= precision as i32 {
        format!("{}e{exp:+03}", trim(mantissa.into()))
    } else {
        trim(format!(
            "{:.*}",
            (precision as i32 - 1 - exp) as usize,
            value
        ))
    }
}

fn write_scalar(
    out: &mut impl Write,
    value: &ScalarValue,
    precision: Option<usize>,
) -> io::Result<()> {
    use ScalarValue::*;
    match value {
        Bool(v) => write!(out, "{}", u8::from(*v)),
        UInt8(v) => write!(out, "{v}"),
        UInt16(v) => write!(out, "{v}"),
        UInt32(v) => write!(out, "{v}"),
        Int16(v) => write!(out, "{v}"),
        Int32(v) => write!(out, "{v}"),
        Int64(v) => write!(out, "{v}"),
        Float32(v) => write!(out, "{}", real((*v).into(), precision.unwrap_or(9))),
        Float64(v) => write!(out, "{}", real(*v, precision.unwrap_or(18))),
        Complex32(v) => write!(
            out,
            "({},{})",
            real(v.re.into(), precision.unwrap_or(9)),
            real(v.im.into(), precision.unwrap_or(9))
        ),
        Complex64(v) => write!(
            out,
            "({},{})",
            real(v.re, precision.unwrap_or(18)),
            real(v.im, precision.unwrap_or(18))
        ),
        String(v) => write!(out, "\"{v}\""),
    }
}

fn write_array(out: &mut impl Write, array: &ArrayValue, separator: &str) -> io::Result<()> {
    macro_rules! write_elements {
        ($array:expr, $func:expr) => {
            bracketed(
                out,
                $array.shape(),
                $array.view().reversed_axes().iter(),
                separator,
                $func,
            )
        };
    }
    match array {
        ArrayValue::Bool(v) => write_elements!(v, |out, v| write!(out, "{}", u8::from(*v))),
        ArrayValue::UInt8(v) => write_elements!(v, |out, v| write!(out, "{v}")),
        ArrayValue::UInt16(v) => write_elements!(v, |out, v| write!(out, "{v}")),
        ArrayValue::UInt32(v) => write_elements!(v, |out, v| write!(out, "{v}")),
        ArrayValue::Int16(v) => write_elements!(v, |out, v| write!(out, "{v}")),
        ArrayValue::Int32(v) => write_elements!(v, |out, v| write!(out, "{v}")),
        ArrayValue::Int64(v) => write_elements!(v, |out, v| write!(out, "{v}")),
        ArrayValue::Float32(v) => {
            write_elements!(v, |out, v: &f32| write!(out, "{}", real((*v).into(), 9)))
        }
        ArrayValue::Float64(v) => {
            write_elements!(v, |out, v: &f64| write!(out, "{}", real(*v, 18)))
        }
        ArrayValue::Complex32(v) => write_elements!(v, |out, v: &num_complex::Complex32| write!(
            out,
            "({},{})",
            real(v.re.into(), 9),
            real(v.im.into(), 9)
        )),
        ArrayValue::Complex64(v) => write_elements!(v, |out, v: &num_complex::Complex64| write!(
            out,
            "({},{})",
            real(v.re, 18),
            real(v.im, 18)
        )),
        ArrayValue::String(v) => write_elements!(v, |out, v| write!(out, "\"{v}\"")),
    }
}

fn bracketed<W: Write, T>(
    out: &mut W,
    shape: &[usize],
    elements: impl Iterator<Item = T>,
    separator: &str,
    mut element: impl FnMut(&mut W, T) -> io::Result<()>,
) -> io::Result<()> {
    let mut position = vec![0; shape.len()];
    let mut openings = shape.len();
    for value in elements {
        for _ in 0..openings {
            out.write_all(b"[")?;
        }
        element(out, value)?;
        openings = 0;
        for (pos, size) in position.iter_mut().zip(shape) {
            *pos += 1;
            if *pos < *size {
                break;
            }
            out.write_all(b"]")?;
            *pos = 0;
            openings += 1;
        }
        if openings < shape.len() {
            out.write_all(separator.as_bytes())?;
        }
    }
    Ok(())
}

fn record_type(kind: PrimitiveType) -> &'static str {
    use PrimitiveType::*;
    match kind {
        Bool => "Bool",
        UInt8 => "uChar",
        UInt16 => "uShort",
        UInt32 => "uInt",
        Int16 => "Short",
        Int32 => "Int",
        Int64 => "Int64",
        Float32 => "Float",
        Float64 => "Double",
        Complex32 => "Complex",
        Complex64 => "DComplex",
        String => "String",
    }
}

fn write_record(out: &mut impl Write, record: &RecordValue) -> io::Result<()> {
    out.write_all(b"{")?;
    write_fields(out, record, "  ")?;
    out.write_all(b"}")
}

fn write_fields(out: &mut impl Write, record: &RecordValue, indent: &str) -> io::Result<()> {
    for field in record.fields() {
        write!(out, "{indent}{}: ", field.name)?;
        match &field.value {
            Value::Scalar(value) => {
                write!(out, "{} ", record_type(value.primitive_type()))?;
                write_scalar(out, value, Some(6))?;
            }
            Value::Array(value) => write_record_array(out, value, indent)?,
            Value::Record(value) => {
                writeln!(out, "{{")?;
                write_fields(out, value, &format!("{indent}  "))?;
                write!(out, "{indent}}}")?;
            }
            Value::TableRef(name) => write!(out, "Table {name}")?,
        }
        writeln!(out)?;
    }
    Ok(())
}

fn write_record_array<W: Write>(out: &mut W, array: &ArrayValue, indent: &str) -> io::Result<()> {
    write!(
        out,
        "{} array with shape {:?}",
        record_type(array.primitive_type()),
        array.shape()
    )?;
    let len = if array.len() > 26 {
        out.write_all(b", first values:")?;
        25
    } else {
        array.len()
    };
    write!(out, "\n{indent}  [")?;
    macro_rules! values {
        ($array:expr, $print:expr) => {{
            for (i, v) in $array.view().reversed_axes().iter().take(len).enumerate() {
                if i > 0 {
                    out.write_all(b", ")?;
                }
                ($print)(out, v)?;
            }
        }};
    }
    match array {
        ArrayValue::Bool(v) => values!(v, |out: &mut W, v: &bool| write!(out, "{}", u8::from(*v))),
        ArrayValue::UInt8(v) => values!(v, |out: &mut W, v| write!(out, "{v}")),
        ArrayValue::UInt16(v) => values!(v, |out: &mut W, v| write!(out, "{v}")),
        ArrayValue::UInt32(v) => values!(v, |out: &mut W, v| write!(out, "{v}")),
        ArrayValue::Int16(v) => values!(v, |out: &mut W, v| write!(out, "{v}")),
        ArrayValue::Int32(v) => values!(v, |out: &mut W, v| write!(out, "{v}")),
        ArrayValue::Int64(v) => values!(v, |out: &mut W, v| write!(out, "{v}")),
        ArrayValue::Float32(v) => values!(v, |out: &mut W, v: &f32| write!(
            out,
            "{}",
            real((*v).into(), 6)
        )),
        ArrayValue::Float64(v) => values!(v, |out: &mut W, v: &f64| write!(out, "{}", real(*v, 6))),
        ArrayValue::Complex32(v) => values!(v, |out: &mut W, v: &num_complex::Complex32| write!(
            out,
            "({},{})",
            real(v.re.into(), 6),
            real(v.im.into(), 6)
        )),
        ArrayValue::Complex64(v) => values!(v, |out: &mut W, v: &num_complex::Complex64| write!(
            out,
            "({},{})",
            real(v.re, 6),
            real(v.im, 6)
        )),
        ArrayValue::String(v) => values!(v, |out: &mut W, v| write!(out, "{v}")),
    }
    out.write_all(b"]")
}
