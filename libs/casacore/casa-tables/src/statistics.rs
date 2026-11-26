// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column statistics shared by CASA's table tool and the calstat task.

use std::collections::BTreeMap;

use casa_core::{ArrayValue, PrimitiveType, ScalarValue};

use crate::{ArrayShapeContract, ColumnType, Table, TableColumn, TableError};

/// CASA statistics indexed by column name, then statistic name. One-dimensional
/// arrays use one entry per element, indexed as `COLUMN_0`, `COLUMN_1`, and so on.
pub type ColumnStatistics = BTreeMap<String, BTreeMap<&'static str, f64>>;

/// Failure computing the statistics for a table column.
#[derive(Debug, thiserror::Error)]
pub enum ColumnStatisticsError {
    /// A table-access error.
    #[error(transparent)]
    Table(#[from] TableError),
    /// CASA rejects the selected column, cells, or projection.
    #[error("{0}")]
    Invalid(String),
    /// The current native reader cannot reproduce this storage behavior.
    #[error("{0}")]
    Unsupported(String),
}

fn median(sorted: &[f64]) -> f64 {
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn record(values: &[f64]) -> Result<BTreeMap<&'static str, f64>, ColumnStatisticsError> {
    if values.is_empty() {
        return Err(ColumnStatisticsError::Invalid(
            "All selected rows are flagged.".into(),
        ));
    }
    let count = values.len();
    let has_nan = values.iter().any(|v| v.is_nan());
    let sum: f64 = values.iter().copied().sum();
    // CASA's lattice squares each float sample before accumulating as doubles.
    let sumsq: f64 = values
        .iter()
        .map(|v| f64::from((*v as f32) * (*v as f32)))
        .sum();
    let mean = sum / count as f64;
    let rms = (sumsq / count as f64).sqrt();
    let variance = if count <= 1 {
        0.0
    } else if values.iter().all(|v| v.is_finite()) {
        // Welford's recurrence avoids cancellation in the finite variance.
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
    Ok(BTreeMap::from([
        ("min", min_val),
        ("max", max_val),
        ("sum", sum),
        ("sumsq", sumsq),
        ("mean", mean),
        ("var", variance),
        ("stddev", stddev),
        ("rms", rms),
        ("median", med_val),
        ("medabsdevmed", mad_val),
        ("quartile", quartile_val),
        ("npts", count as f64),
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
    let re = value.re;
    let im = value.im;
    f64::from(match mode {
        "amp" | "amplitude" => re.hypot(im),
        "phase" => im.atan2(re),
        "imag" | "imaginary" => im,
        "real" => re,
        _ => unreachable!(),
    })
}

fn scalar(value: &ScalarValue, mode: &str) -> Result<f64, ColumnStatisticsError> {
    Ok(match value {
        ScalarValue::Bool(value) => f64::from(u8::from(*value)),
        ScalarValue::Int32(value) => f64::from(*value as f32),
        ScalarValue::Float32(value) => f64::from(*value),
        ScalarValue::Float64(value) => f64::from(*value as f32),
        ScalarValue::Complex32(value) => complex(*value, mode),
        _ => {
            return Err(ColumnStatisticsError::Invalid(
                "unsupported statistics cell".into(),
            ));
        }
    })
}

fn array(value: &ArrayValue, mode: &str) -> Result<(Vec<usize>, Vec<f64>), ColumnStatisticsError> {
    macro_rules! values {
        ($a:expr, $convert:expr) => {
            ($a.shape().to_vec(), $a.t().iter().map($convert).collect())
        };
    }
    Ok(match value {
        ArrayValue::Bool(a) => values!(a, |v: &bool| f64::from(u8::from(*v))),
        ArrayValue::Int32(a) => values!(a, |v: &i32| f64::from(*v as f32)),
        ArrayValue::Float32(a) => values!(a, |v: &f32| f64::from(*v)),
        ArrayValue::Float64(a) => values!(a, |v: &f64| f64::from(*v as f32)),
        ArrayValue::Complex32(a) => values!(a, |v: &num_complex::Complex32| complex(*v, mode)),
        _ => {
            return Err(ColumnStatisticsError::Invalid(
                "unsupported statistics array".into(),
            ));
        }
    })
}

// CASA's getColumn uses the first cell's shape and zero-fills a smaller cell.
// Preserve Fortran-order indexing when a shorter axis is not the last axis.
fn pad_cell(
    values: Vec<f64>,
    shape: &[usize],
    target: &[usize],
) -> Result<Vec<f64>, ColumnStatisticsError> {
    if shape == target {
        return Ok(values);
    }
    if shape.len() != target.len() || shape.iter().zip(target).any(|(a, b)| a > b) {
        return Err(ColumnStatisticsError::Unsupported("table.statistics on cells larger than the first cell is not yet implemented in casa-rs (#1028)".into()));
    }
    let size = target
        .iter()
        .try_fold(1usize, |n, d| n.checked_mul(*d))
        .ok_or_else(|| {
            ColumnStatisticsError::Invalid("table.statistics array shape overflow".into())
        })?;
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

/// Compute the same unmasked column statistics as CASA's table.statistics.
///
/// Row selections may be supplied in view order. CASA projects all samples
/// to single precision, including double and integer columns, before
/// computing the double-precision aggregate statistics. Complex columns
/// require a projection of amplitude, phase, real part or imaginary part.
///
/// # Errors
/// Rejects empty tables, unknown or unsupported columns, invalid complex
/// projections and storage failures while reading a selected cell.
pub fn column_statistics(
    table: &Table,
    rows: Option<&[usize]>,
    column: &str,
    mode: &str,
    mut log: impl FnMut(String),
) -> Result<ColumnStatistics, ColumnStatisticsError> {
    let nrows = rows.map_or_else(|| table.row_count(), <[usize]>::len);
    if nrows == 0 {
        return Err(ColumnStatisticsError::Invalid(
            "Table has zero rows, cannot continue".into(),
        ));
    }
    let desc = table
        .schema()
        .and_then(|schema| schema.column(column))
        .ok_or_else(|| {
            ColumnStatisticsError::Invalid(format!("Table column {column} is unknown"))
        })?;
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
    log(format!(
        "Compute statistics on {label} column {column}{}...",
        if mode.is_empty() {
            String::new()
        } else {
            format!(", use {mode}")
        }
    ));
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
        return Err(ColumnStatisticsError::Invalid(format!(
            "Sorry, no support for {label} columns"
        )));
    }
    if ty == Some(PrimitiveType::Complex32)
        && !matches!(
            mode,
            "amp" | "amplitude" | "phase" | "imag" | "imaginary" | "real"
        )
    {
        return Err(ColumnStatisticsError::Invalid(format!(
            "complex_value must be amp, amplitude, phase, imag, imaginary or real, is {mode}"
        )));
    }
    let access = table.column_accessor(column)?;
    let indexed = is_array && rank == Some(1);
    let groups = if is_array {
        let manager = table
            .data_manager_info()
            .iter()
            .find(|manager| manager.columns.iter().any(|name| name == column));
        let standard = match manager {
            Some(manager) => manager.dm_type == "StandardStMan",
            None => {
                table.reference_column_manager_type(column)?.as_deref() == Some("StandardStMan")
            }
        };
        let mismatch_error = if standard {
            Some("Table DataManager error: Internal error: StManIndArray::get/put shapes not conforming".to_owned())
        } else if rows.is_some() || table.is_reference_table()? {
            Some(format!(
                "Table DataManager error: getArrayColumnCells shape mismatch in column {column}"
            ))
        } else {
            None
        };
        array_samples(
            &access,
            column,
            rows,
            nrows,
            mode,
            indexed,
            mismatch_error.as_deref(),
        )?
    } else {
        let cells = match rows {
            Some(rows) => access.scalar_cells_owned_for_rows(rows),
            None => access.scalar_cells_owned(),
        }?;
        let values = cells.into_iter().enumerate().map(|(index, cell)| {
            let row = rows.map_or(index, |rows| rows[index]);
            scalar(&cell.ok_or_else(|| missing_cell(row, column))?, mode)
        });
        vec![values.collect::<Result<Vec<_>, _>>()?]
    };
    let mut result = BTreeMap::new();
    for (index, values) in groups.iter().enumerate() {
        let name = if indexed {
            format!("{column}_{index}")
        } else {
            column.to_string()
        };
        result.insert(name, record(values)?);
    }
    Ok(result)
}

fn array_samples(
    access: &TableColumn<'_>,
    column: &str,
    rows: Option<&[usize]>,
    nrows: usize,
    mode: &str,
    indexed: bool,
    mismatch_error: Option<&str>,
) -> Result<Vec<Vec<f64>>, ColumnStatisticsError> {
    // Batch storage reads instead of asking the data manager to reopen and
    // locate the array for every row. Keep the decoded cells in one batch.
    const ROW_BATCH: usize = 4096;
    let mut groups: Vec<Vec<f64>> = Vec::new();
    let mut shape = None;
    for start in (0..nrows).step_by(ROW_BATCH) {
        let end = (start + ROW_BATCH).min(nrows);
        let contiguous;
        let selected = match rows {
            Some(rows) => &rows[start..end],
            None => {
                contiguous = (start..end).collect::<Vec<_>>();
                &contiguous
            }
        };
        let arrays = access.array_cells_owned_uncached(selected)?;
        for (&row, cell) in selected.iter().zip(arrays) {
            let cell = cell.ok_or_else(|| missing_cell(row, column))?;
            let (next_shape, values) = array(&cell, mode)?;
            let target = shape.get_or_insert_with(|| {
                let streams = if indexed { values.len() } else { 1 };
                groups.resize_with(streams, Vec::new);
                next_shape.clone()
            });
            if target.contains(&0) {
                if indexed {
                    return Ok(Vec::new());
                }
                return Err(ColumnStatisticsError::Invalid(
                    "All selected rows are flagged.".into(),
                ));
            }
            if *target != next_shape
                && let Some(message) = mismatch_error
            {
                return Err(ColumnStatisticsError::Invalid(message.to_owned()));
            }
            let values = pad_cell(values, &next_shape, target)?;
            if indexed {
                for (group, value) in groups.iter_mut().zip(values) {
                    group.push(value);
                }
            } else {
                groups[0].extend(values);
            }
        }
    }
    Ok(groups)
}

fn missing_cell(row_index: usize, column: &str) -> TableError {
    TableError::ColumnNotFound {
        row_index,
        column: column.to_owned(),
    }
}
