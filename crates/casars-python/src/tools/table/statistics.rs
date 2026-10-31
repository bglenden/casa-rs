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

fn record(values: &[f32]) -> Result<CasaValue, Fail> {
    if values.is_empty() {
        return Err(Fail::Runtime("All selected rows are flagged.".into()));
    }
    if values.iter().any(|v| !v.is_finite()) {
        return Err(Fail::Unsupported(
            "table.statistics with nonfinite values is not yet implemented in casa-rs (#1028)"
                .into(),
        ));
    }
    let (mut sum, mut sumsq, mut mean, mut variance) = (0.0, 0.0, 0.0, 0.0);
    for (index, &raw) in values.iter().enumerate() {
        let value = f64::from(raw);
        sum += value;
        sumsq += value * value;
        let old = mean;
        mean += (value - mean) / (index + 1) as f64;
        variance += (value - old) * (value - mean);
    }
    let count = values.len();
    let mut sorted: Vec<f64> = values.iter().copied().map(f64::from).collect();
    sorted.sort_by(f64::total_cmp);
    let med = median(&sorted);
    let q1 = sorted[count.div_ceil(4) - 1];
    let q3 = sorted[count - count / 4 - 1];
    let mut deviations: Vec<f64> = sorted.iter().map(|v| (v - med).abs()).collect();
    deviations.sort_by(f64::total_cmp);
    let variance = if count > 1 {
        variance / (count - 1) as f64
    } else {
        0.0
    };
    Ok(CasaValue::record([
        ("min", CasaValue::Float(sorted[0])),
        ("max", CasaValue::Float(sorted[count - 1])),
        ("sum", CasaValue::Float(sum)),
        ("sumsq", CasaValue::Float(sumsq)),
        ("mean", CasaValue::Float(sum / count as f64)),
        ("var", CasaValue::Float(variance)),
        ("stddev", CasaValue::Float(variance.sqrt())),
        ("rms", CasaValue::Float((sumsq / count as f64).sqrt())),
        ("median", CasaValue::Float(med)),
        ("medabsdevmed", CasaValue::Float(median(&deviations))),
        ("quartile", CasaValue::Float(q3 - q1)),
        ("npts", CasaValue::Float(count as f64)),
    ]))
}

fn type_name(ty: Option<PrimitiveType>, array: bool) -> &'static str {
    match (ty, array) {
        (Some(PrimitiveType::Bool), _) => "Bool",
        (Some(PrimitiveType::Int32), _) => "Int",
        (Some(PrimitiveType::Int64), _) => "Int64",
        (Some(PrimitiveType::Float32), true) => "float",
        (Some(PrimitiveType::Float32), false) => "Float",
        (Some(PrimitiveType::Float64), true) => "double",
        (Some(PrimitiveType::Float64), false) => "Double",
        (Some(PrimitiveType::Complex32), _) => "Complex",
        (Some(PrimitiveType::Complex64), _) => "DComplex",
        (Some(PrimitiveType::String), _) => "String",
        _ => "unknown",
    }
}

fn complex(value: num_complex::Complex32, mode: &str) -> f32 {
    match mode {
        "amp" | "amplitude" => value.norm(),
        "phase" => value.arg(),
        "imag" | "imaginary" => value.im,
        "real" => value.re,
        _ => unreachable!(),
    }
}

fn scalar(value: &ScalarValue, mode: &str) -> Result<f32, Fail> {
    Ok(match value {
        ScalarValue::Bool(value) => u8::from(*value) as f32,
        ScalarValue::Int32(value) => *value as f32,
        ScalarValue::Float32(value) => *value,
        ScalarValue::Float64(value) => *value as f32,
        ScalarValue::Complex32(value) => complex(*value, mode),
        _ => return Err(Fail::Runtime("unsupported statistics cell".into())),
    })
}

fn array(value: &ArrayValue, mode: &str) -> Result<(Vec<usize>, Vec<f32>), Fail> {
    macro_rules! values {
        ($a:expr, $convert:expr) => {
            ($a.shape().to_vec(), $a.t().iter().map($convert).collect())
        };
    }
    Ok(match value {
        ArrayValue::Bool(a) => values!(a, |v: &bool| u8::from(*v) as f32),
        ArrayValue::Int32(a) => values!(a, |v: &i32| *v as f32),
        ArrayValue::Float32(a) => values!(a, |v: &f32| *v),
        ArrayValue::Float64(a) => values!(a, |v: &f64| *v as f32),
        ArrayValue::Complex32(a) => values!(a, |v: &num_complex::Complex32| complex(*v, mode)),
        _ => return Err(Fail::Runtime("unsupported statistics array".into())),
    })
}

// CASA's getColumn uses the first cell's shape and zero-fills a smaller cell.
// Preserve Fortran-order indexing when a shorter axis is not the last axis.
fn pad_cell(values: Vec<f32>, shape: &[usize], target: &[usize]) -> Result<Vec<f32>, Fail> {
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
        if open.table.row_count() == 0 {
            return Err(Fail::Runtime("Table has zero rows, cannot continue".into()));
        }
        let desc = open
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
            type_name(ty, is_array),
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
        let access = open.table.row_accessor();
        let mut groups: Vec<Vec<f32>> = Vec::new();
        let mut shape = None;
        for row in 0..open.table.row_count() {
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
