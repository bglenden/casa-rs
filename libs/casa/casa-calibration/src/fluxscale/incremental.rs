// SPDX-License-Identifier: LGPL-3.0-or-later
//! `setSpecify`/`specify` emits a constant G/T correction for every antenna.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use casa_core::{ArrayValue, Complex32, RecordValue, ScalarValue, Value};
use casa_tables::{Table, TableOptions};
use ndarray::ArrayD;

use super::{FluxScaleError, FluxScaleEstimate, GainSampleRow, selection::SampleMasks};

/// `createMemCalTable` starts again from the MS metadata, collapsing every
/// window to its mean frequency and signed aggregate widths, including unused
/// windows. Solved gain tables can contain different RESOLUTION/bandwidths.
pub(super) fn copy_metadata(source: &Path, stage: &Path) -> Result<(), FluxScaleError> {
    let failed =
        |error: casa_tables::TableError| crate::output_merge::invalid(stage, error.to_string());
    let main = Table::open_metadata_only(TableOptions::new(source)).map_err(failed)?;
    let owner = crate::paths::subtable_owner(&main, source)
        .map_err(|error| crate::output_merge::invalid(source, error.to_string()))?;
    for name in ["ANTENNA", "FIELD", "OBSERVATION", "SPECTRAL_WINDOW"] {
        let reference = match main.keywords().get(name) {
            Some(Value::TableRef(value) | Value::Scalar(ScalarValue::String(value))) => Some(value),
            _ => None,
        };
        let path = reference
            .map(|value| casa_tables::add_directory(value, &owner))
            .filter(|path| path.exists())
            .unwrap_or_else(|| source.join(name));
        let table = Table::open(TableOptions::new(path)).map_err(failed)?;
        table
            .save(TableOptions::new(stage.join(name)))
            .map_err(failed)?;
    }
    let path = stage.join("SPECTRAL_WINDOW");
    let mut table = Table::open(TableOptions::new(&path)).map_err(failed)?;
    for row in 0..table.row_count() {
        let mut width = 0.;
        for column in ["CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"] {
            let ArrayValue::Float64(values) = table
                .cell_accessor(row, column)
                .and_then(|cell| cell.array())
                .map_err(failed)?
            else {
                return Err(crate::output_merge::invalid(
                    &path,
                    format!("{column} is not Float64"),
                )
                .into());
            };
            if values.is_empty() {
                continue;
            }
            let sum = values.iter().sum::<f64>();
            let value = if column == "CHAN_FREQ" {
                sum / values.len() as f64
            } else {
                sum
            };
            if column == "CHAN_WIDTH" {
                width = value;
            }
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut cell| cell.set(Value::Array(ArrayValue::from_f64_vec(vec![value]))))
                .map_err(failed)?;
        }
        table
            .cell_accessor_mut(row, "NUM_CHAN")
            .and_then(|mut cell| cell.set(Value::Scalar(ScalarValue::Int32(1))))
            .map_err(failed)?;
        table
            .cell_accessor_mut(row, "TOTAL_BANDWIDTH")
            .and_then(|mut cell| cell.set(Value::Scalar(ScalarValue::Float64(width.abs()))))
            .map_err(failed)?;
    }
    table.save(TableOptions::new(path)).map_err(failed)?;
    Ok(())
}

pub(super) fn populate(
    table: &mut Table,
    root: &Path,
    rows: &[GainSampleRow],
    estimates: &HashMap<(i32, i32), FluxScaleEstimate>,
    masks: &SampleMasks,
) -> Result<(), FluxScaleError> {
    let invalid =
        |error: casa_tables::TableError| crate::output_merge::invalid(root, error.to_string());
    let shape = rows[0].gains.raw_dim();
    let time = reference_time(rows, masks);
    let antennas = Table::open(TableOptions::new(root.join("ANTENNA")))
        .map_err(invalid)?
        .row_count();
    let windows = Table::open(TableOptions::new(root.join("SPECTRAL_WINDOW")))
        .map_err(invalid)?
        .row_count();
    let fields = rows.iter().map(|row| row.field_id).collect::<BTreeSet<_>>();
    for spw in 0..windows {
        for &field in &fields {
            let factor = estimates
                .get(&(field, spw as i32))
                .map_or(1., |estimate| estimate.correction_factor);
            for antenna in 0..antennas {
                let mut row = RecordValue::new(Vec::new());
                for (column, value) in [
                    ("FIELD_ID", field),
                    ("SPECTRAL_WINDOW_ID", spw as i32),
                    ("ANTENNA1", antenna as i32),
                    ("ANTENNA2", -1),
                    ("SCAN_NUMBER", -1),
                    ("OBSERVATION_ID", 0),
                    ("ARRAY_ID", 0),
                ] {
                    if table
                        .schema()
                        .is_some_and(|schema| schema.contains_column(column))
                    {
                        row.upsert(column, Value::Scalar(ScalarValue::Int32(value)));
                    }
                }
                for (column, value) in [("TIME", time), ("INTERVAL", 0.), ("TIME_EXTRA_PREC", 0.)] {
                    if table
                        .schema()
                        .is_some_and(|schema| schema.contains_column(column))
                    {
                        row.upsert(column, Value::Scalar(ScalarValue::Float64(value)));
                    }
                }
                row.upsert(
                    "CPARAM",
                    Value::Array(ArrayValue::Complex32(ArrayD::from_elem(
                        shape.clone(),
                        Complex32::new(factor, 0.),
                    ))),
                );
                row.upsert(
                    "FLAG",
                    Value::Array(ArrayValue::Bool(ArrayD::from_elem(shape.clone(), false))),
                );
                for column in ["PARAMERR", "SNR"] {
                    row.upsert(
                        column,
                        Value::Array(ArrayValue::Float32(ArrayD::zeros(shape.clone()))),
                    );
                }
                table.add_row(row).map_err(invalid)?;
            }
        }
    }
    Ok(())
}

fn reference_time(rows: &[GainSampleRow], masks: &SampleMasks) -> f64 {
    let mut selected = rows
        .iter()
        .zip(&masks.logical)
        .filter_map(|(row, selected)| selected.then_some(row))
        .collect::<Vec<_>>();
    selected.sort_by(|a, b| {
        a.spw_id
            .cmp(&b.spw_id)
            .then(a.time_seconds.total_cmp(&b.time_seconds))
            .then(a.field_id.cmp(&b.field_id))
            .then(a.antenna_id.cmp(&b.antenna_id))
    });
    let (mut previous_field, mut time) = (-1, 0.);
    for row in selected {
        // Upstream assigns its whole mgreft vector on every increasing FIELD_ID.
        if row.field_id > previous_field {
            time = row.time_seconds;
        }
        previous_field = row.field_id;
    }
    time
}
