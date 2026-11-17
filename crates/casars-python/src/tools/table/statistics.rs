// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA table-column statistics (#1028).

use casa_tables::{ArrayShapeContract, ColumnType};
use casa_types::{ArrayValue, PrimitiveType, ScalarValue};
use pyo3::prelude::*;

use super::state::{Fail, State, no_table_specified};
use super::{PyTableTool, text};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

fn median(sorted: &[f64]) -> f64 {
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn record(values: &[f64]) -> Result<CasaValue, Fail> {
    if values.is_empty() {
        return Err(Fail::Runtime("All selected rows are flagged.".into()));
    }
    let count = values.len();
    let has_nan = values.iter().any(|v| v.is_nan());
    let sum: f64 = values.iter().copied().sum();
    let sumsq: f64 = values.iter().map(|v| v * v).sum();
    let mean = sum / count as f64;
    let rms = (sumsq / count as f64).sqrt();
    let variance = if count <= 1 {
        0.0
    } else if values.iter().all(|v| v.is_finite()) {
        // Welford two-pass keeps finite variance numerically accurate.
        let mut w_mean = 0.0;
        let mut m2 = 0.0;
        for (index, &value) in values.iter().enumerate() {
            let old = w_mean;
            w_mean += (value - w_mean) / (index + 1) as f64;
            m2 += (value - old) * (value - w_mean);
        }
        m2 / (count - 1) as f64
    } else {
        (sumsq - sum * sum / count as f64) / (count - 1) as f64
    };
    let stddev = if variance.is_nan() {
        f64::NAN
    } else {
        variance.max(0.0).sqrt()
    };
    let (min_val, max_val, med_val, mad_val, quartile_val) = if has_nan {
        let mut min_finite = None;
        let mut max_finite = None;
        for &value in values {
            if !value.is_nan() {
                min_finite = Some(min_finite.map_or(value, |cur: f64| cur.min(value)));
                max_finite = Some(max_finite.map_or(value, |cur: f64| cur.max(value)));
            }
        }
        (
            min_finite.unwrap_or(f64::NAN),
            max_finite.unwrap_or(f64::NAN),
            f64::NAN,
            f64::NAN,
            f64::NAN,
        )
    } else {
        let mut sorted: Vec<f64> = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        let med = median(&sorted);
        let q1 = sorted[count.div_ceil(4) - 1];
        let q3 = sorted[count - count / 4 - 1];
        let mut deviations: Vec<f64> = sorted.iter().map(|v| (v - med).abs()).collect();
        deviations.sort_by(f64::total_cmp);
        (
            sorted[0],
            sorted[count - 1],
            med,
            median(&deviations),
            q3 - q1,
        )
    };
    Ok(CasaValue::record([
        ("min", CasaValue::Float(min_val)),
        ("max", CasaValue::Float(max_val)),
        ("sum", CasaValue::Float(sum)),
        ("sumsq", CasaValue::Float(sumsq)),
        ("mean", CasaValue::Float(mean)),
        ("var", CasaValue::Float(variance)),
        ("stddev", CasaValue::Float(stddev)),
        ("rms", CasaValue::Float(rms)),
        ("median", CasaValue::Float(med_val)),
        ("medabsdevmed", CasaValue::Float(mad_val)),
        ("quartile", CasaValue::Float(quartile_val)),
        ("npts", CasaValue::Float(count as f64)),
    ]))
}

fn type_name(ty: Option<PrimitiveType>) -> &'static str {
    match ty {
        Some(PrimitiveType::Bool) => "Bool",
        Some(PrimitiveType::UInt8) => "uChar",
        Some(PrimitiveType::UInt16) => "uShort",
        Some(PrimitiveType::UInt32) => "uInt",
        Some(PrimitiveType::Int16) => "Short",
        Some(PrimitiveType::Int32) => "Int",
        Some(PrimitiveType::Int64) => "Int64",
        Some(PrimitiveType::Float32) => "float",
        Some(PrimitiveType::Float64) => "double",
        Some(PrimitiveType::Complex32) => "Complex",
        Some(PrimitiveType::Complex64) => "DComplex",
        Some(PrimitiveType::String) => "String",
        None => "unknown",
    }
}

fn complex(value: num_complex::Complex32, mode: &str) -> f64 {
    let re = f64::from(value.re);
    let im = f64::from(value.im);
    match mode {
        "amp" | "amplitude" => re.hypot(im),
        "phase" => im.atan2(re),
        "imag" | "imaginary" => im,
        "real" => re,
        _ => unreachable!(),
    }
}

fn scalar(value: &ScalarValue, mode: &str) -> Result<f64, Fail> {
    Ok(match value {
        ScalarValue::Bool(value) => f64::from(u8::from(*value)),
        ScalarValue::Int32(value) => f64::from(*value),
        ScalarValue::Float32(value) => f64::from(*value),
        ScalarValue::Float64(value) => *value,
        ScalarValue::Complex32(value) => complex(*value, mode),
        _ => return Err(Fail::Runtime("unsupported statistics cell".into())),
    })
}

fn array(value: &ArrayValue, mode: &str) -> Result<(Vec<usize>, Vec<f64>), Fail> {
    macro_rules! values {
        ($a:expr, $convert:expr) => {
            ($a.shape().to_vec(), $a.t().iter().map($convert).collect())
        };
    }
    Ok(match value {
        ArrayValue::Bool(a) => values!(a, |v: &bool| f64::from(u8::from(*v))),
        ArrayValue::Int32(a) => values!(a, |v: &i32| f64::from(*v)),
        ArrayValue::Float32(a) => values!(a, |v: &f32| f64::from(*v)),
        ArrayValue::Float64(a) => values!(a, |v: &f64| *v),
        ArrayValue::Complex32(a) => values!(a, |v: &num_complex::Complex32| complex(*v, mode)),
        _ => return Err(Fail::Runtime("unsupported statistics array".into())),
    })
}

// CASA's getColumn uses the first cell's shape and zero-fills a smaller cell.
// Preserve Fortran-order indexing when a shorter axis is not the last axis.
fn pad_cell(values: Vec<f64>, shape: &[usize], target: &[usize]) -> Result<Vec<f64>, Fail> {
    if shape == target {
        return Ok(values);
    }
    if shape.len() != target.len() || shape.iter().zip(target).any(|(a, b)| a > b) {
        return Err(Fail::Unsupported("table.statistics on cells larger than the first cell is not yet implemented in casa-rs (#1028)".into()));
    }
    let size = target
        .iter()
        .try_fold(1usize, |n, d| n.checked_mul(*d))
        .ok_or_else(|| Fail::Runtime("table.statistics array shape overflow".into()))?;
    let mut padded = vec![0.0; size];
    for (index, value) in values.into_iter().enumerate() {
        let mut remainder = index;
        let mut offset = 0;
        let mut stride = 1;
        for (&source, &destination) in shape.iter().zip(target) {
            offset += (remainder % source) * stride;
            remainder /= source;
            stride *= destination;
        }
        padded[offset] = value;
    }
    Ok(padded)
}

impl State {
    fn statistics(
        &self,
        column: &str,
        mode: &str,
        log: &mut Vec<LogLine>,
    ) -> Result<CasaValue, Fail> {
        let Some(open) = self.open.as_ref() else {
            no_table_specified(log);
            return Ok(CasaValue::empty_record());
        };
        let root = open.root();
        let nrows = open
            .row_map
            .as_ref()
            .map_or_else(|| root.table.row_count(), Vec::len);
        if nrows == 0 {
            return Err(Fail::Runtime("Table has zero rows, cannot continue".into()));
        }
        if !open.column_allowed(column) {
            return Err(Fail::Runtime(format!("Table column {column} is unknown")));
        }
        let desc = root
            .table
            .schema()
            .and_then(|schema| schema.column(column))
            .ok_or_else(|| Fail::Runtime(format!("Table column {column} is unknown")))?;
        let rank = match desc.column_type() {
            ColumnType::Scalar | ColumnType::Record => None,
            ColumnType::Array(ArrayShapeContract::Fixed { shape }) => Some(shape.len()),
            ColumnType::Array(ArrayShapeContract::Variable { ndim }) => *ndim,
        };
        let is_array = matches!(desc.column_type(), ColumnType::Array(_));
        let ty = desc.data_type();
        let label = format!(
            "{}{} {}",
            rank.filter(|r| *r > 0)
                .map_or_else(String::new, |r| format!("{r}-dimensional ")),
            type_name(ty),
            if is_array { "array" } else { "scalar" }
        );
        log.push(LogLine::info(format!(
            "Compute statistics on {label} column {column}{}...",
            if mode.is_empty() {
                String::new()
            } else {
                format!(", use {mode}")
            }
        )));
        let supported = matches!(
            ty,
            Some(
                PrimitiveType::Bool
                    | PrimitiveType::Int32
                    | PrimitiveType::Float32
                    | PrimitiveType::Float64
                    | PrimitiveType::Complex32
            )
        ) && (!is_array
            || rank != Some(1)
            || matches!(ty, Some(PrimitiveType::Float32 | PrimitiveType::Float64)));
        if !supported {
            return Err(Fail::Runtime(format!(
                "Sorry, no support for {label} columns"
            )));
        }
        if ty == Some(PrimitiveType::Complex32)
            && !matches!(
                mode,
                "amp" | "amplitude" | "phase" | "imag" | "imaginary" | "real"
            )
        {
            return Err(Fail::Runtime(format!(
                "complex_value must be amp, amplitude, phase, imag, imaginary or real, is {mode}"
            )));
        }
        let access = root.table.row_accessor();
        let mut groups: Vec<Vec<f64>> = Vec::new();
        let mut shape = None;
        for view_row in 0..nrows {
            let row = open.translate_row(view_row);
            if is_array {
                let (next_shape, values) = array(access.array_cell(row, column)?, mode)?;
                let target = shape.get_or_insert_with(|| next_shape.clone());
                let values = pad_cell(values, &next_shape, target)?;
                if rank == Some(1) {
                    if groups.is_empty() {
                        groups.resize_with(values.len(), Vec::new);
                    }
                    for (group, value) in groups.iter_mut().zip(values) {
                        group.push(value);
                    }
                } else {
                    if groups.is_empty() {
                        groups.push(Vec::new());
                    }
                    groups[0].extend(values);
                }
            } else {
                if groups.is_empty() {
                    groups.push(Vec::new());
                }
                groups[0].push(scalar(access.scalar_cell(row, column)?, mode)?);
            }
        }
        let mut result = Vec::new();
        for (index, values) in groups.iter().enumerate() {
            let name = if is_array && rank == Some(1) {
                format!("{column}_{index}")
            } else {
                column.to_string()
            };
            result.push((name, record(values)?));
        }
        Ok(CasaValue::record(result))
    }
}

#[pymethods]
impl PyTableTool {
    /// Compute CASA statistics for a table column.
    #[pyo3(signature = (column, complex_value = None), text_signature = "($self, column, complex_value='')")]
    fn statistics(
        &self,
        py: Python<'_>,
        column: &Bound<'_, PyAny>,
        complex_value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(column, "column")?;
        let mode = complex_value.map_or(Ok(String::new()), |value| text(value, "complex_value"))?;
        self.read(py, move |state, log| state.statistics(&column, &mode, log))
    }
}
