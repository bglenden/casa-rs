// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed records for the original VisibilityIterator's current buffer.

use std::collections::BTreeSet;

use casa_core::{ArrayValue, RecordValue, Value};
use casa_tables::{PlainReferenceRead, RequiredScalarColumnValues, Table};
use ndarray::{Axis, Ix2};

use super::{VisibilityRead, VisibilityReadOptions, input};
use crate::error::MsResult;
use crate::tool_iteration::Vi1ToolIteration;

/// Read a VI1 buffer with row IDs relative to its captured MeasurementSet.
///
/// # Errors
/// Returns an error for an unsupported field or a missing or invalid column.
pub fn read_vi1_visibility(
    main: &Table,
    members: &[Table],
    reference: Option<&PlainReferenceRead>,
    cursor: &Vi1ToolIteration,
    items: &[String],
) -> MsResult<RecordValue> {
    let names = items
        .iter()
        .map(|item| item.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let mut record = RecordValue::default();
    if names.is_empty() {
        return Ok(record);
    }
    let rows = cursor.rows();
    let mut reader = VisibilityRead::new(
        main,
        members,
        reference,
        rows,
        rows.first().copied(),
        &names,
        VisibilityReadOptions::default(),
    )?;
    let has_float = main
        .schema()
        .is_some_and(|schema| schema.column("FLOAT_DATA").is_some());
    for requested in items {
        let name = requested.to_ascii_lowercase();
        if record.get(&name).is_some() {
            continue;
        }
        let value = match name.as_str() {
            "data" if has_float => {
                reader.observed_float(&reader.raw_matrix_cells("FLOAT_DATA", false)?)?
            }
            "data" | "model_data" | "corrected_data" | "flag" => {
                reader.raw_matrix_cells(&name.to_ascii_uppercase(), name != "flag")?
            }
            "antenna1" | "antenna2" | "time" => {
                read_scalar(&mut reader, &name.to_ascii_uppercase())?
            }
            "rows" => ArrayValue::from_i64_vec(cursor.row_numbers()?),
            "weight" => parallel_hand_weights(&reader)?,
            _ => {
                return Err(input(format!(
                    "ngetdata: Unsupported item requrested ({requested})"
                )));
            }
        };
        record.upsert(name, Value::Array(value));
    }
    Ok(record)
}

fn read_scalar(reader: &mut VisibilityRead<'_>, name: &str) -> MsResult<ArrayValue> {
    match reader.scalar_columns.remove(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(ArrayValue::from_i32_vec(values)),
        Some(RequiredScalarColumnValues::Float64(values)) => Ok(ArrayValue::from_f64_vec(values)),
        _ => Err(input(format!("invalid VI1 scalar column {name}"))),
    }
}

fn parallel_hand_weights(reader: &VisibilityRead<'_>) -> MsResult<ArrayValue> {
    let ArrayValue::Float32(weights) = reader.raw_vector_cells("WEIGHT")? else {
        return Err(input("WEIGHT must contain Float values"));
    };
    let weights = weights
        .into_dimensionality::<Ix2>()
        .map_err(|_| input("WEIGHT must contain a correlation vector per row"))?;
    let count = weights.nrows();
    if count == 0 {
        return Err(input("WEIGHT has no correlations"));
    }
    let values = weights
        .axis_iter(Axis(1))
        .map(|row| (row[0] + row[count - 1]) / 2.0)
        .collect();
    Ok(ArrayValue::from_f32_vec(values))
}
