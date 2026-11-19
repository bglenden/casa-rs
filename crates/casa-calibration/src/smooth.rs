// SPDX-License-Identifier: LGPL-3.0-or-later
//! Smooth complex gain solutions in amplitude and tracked phase.

use std::collections::BTreeMap;
use std::path::Path;

use casa_ms::MeasurementSet;
use casa_tables::{RequiredScalarColumnValues, Table, TableOptions, TableWriteLock};
use casa_types::{ArrayValue, Complex32, Value};

use crate::CalibrationOutputMode;
use crate::output::CalibrationOutput;
use crate::paths::{copy_dir_recursive, normalized, overlap};

#[derive(Clone, Copy)]
pub(crate) enum Smoother {
    Mean,
    Median,
}

pub(crate) fn smooth_calibration_table(
    ms_path: &Path,
    input: &Path,
    destination: &Path,
    field: &str,
    smoother: Smoother,
    seconds: f64,
    _ratesmooth: bool,
) -> Result<(), String> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err("smoothtime must be a positive duration in seconds".into());
    }
    let ms = MeasurementSet::open(ms_path).map_err(error_text)?;
    let fields = ms.resolve_field_selector(field).map_err(error_text)?;
    let input = normalized(input).map_err(error_text)?;
    let destination = normalized(destination).map_err(error_text)?;
    if input != destination && overlap(&input, &destination).map_err(error_text)? {
        return Err("the input and output calibration tables overlap".into());
    }
    let source = Table::open(TableOptions::new(&input)).map_err(error_text)?;
    if source.info().table_type != crate::constants::TABLE_INFO_TYPE {
        return Err("input is not a calibration table".into());
    }
    if source.info().sub_type == "Fringe Jones" {
        return Err("fringe rate smoothing is tracked in #1525".into());
    }
    let parameter_column = match source.schema() {
        Some(schema) if schema.contains_column("CPARAM") => "CPARAM",
        Some(schema) if schema.contains_column("FPARAM") => "FPARAM",
        _ => return Err("input table has no supported calibration parameter column".into()),
    };
    let output = CalibrationOutput::prepare(&destination, CalibrationOutputMode::Replace)
        .map_err(error_text)?;
    copy_dir_recursive(&input, &output.staging).map_err(error_text)?;
    let mut lock = TableWriteLock::acquire_to_reread(&output.staging, 0).map_err(error_text)?;
    let mut table = Table::open(TableOptions::new(&output.staging)).map_err(error_text)?;
    let groups = solution_groups(&table, &fields)?;
    for rows in groups.values().filter(|rows| rows.len() > 1) {
        smooth_group(
            &mut table,
            parameter_column,
            rows,
            smoother,
            seconds / 2.0,
            &mut lock,
        )?;
    }
    drop(table);
    lock.release().map_err(error_text)?;
    output.commit().map_err(error_text)
}

fn error_text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

type SolutionGroups = BTreeMap<[i32; 4], Vec<(usize, f64)>>;

fn solution_groups(table: &Table, fields: &[i32]) -> Result<SolutionGroups, String> {
    let rows = (0..table.row_count()).collect::<Vec<_>>();
    let mut columns = table
        .required_scalar_columns_owned_for_rows(
            &[
                "SPECTRAL_WINDOW_ID",
                "FIELD_ID",
                "ANTENNA1",
                "ANTENNA2",
                "TIME",
            ],
            &rows,
        )
        .map_err(|error| error.to_string())?;
    let Some(RequiredScalarColumnValues::Float64(times)) = columns.remove("TIME") else {
        return Err("invalid calibration table TIME column".into());
    };
    let mut integer = |name| match columns.remove(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(format!("invalid calibration table {name} column")),
    };
    let spws = integer("SPECTRAL_WINDOW_ID")?;
    let field_ids = integer("FIELD_ID")?;
    let antenna1 = integer("ANTENNA1")?;
    let antenna2 = integer("ANTENNA2")?;
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for (row, time) in times.into_iter().enumerate() {
        let field = field_ids[row];
        if fields.is_empty() || fields.binary_search(&field).is_ok() {
            groups
                .entry([spws[row], field, antenna1[row], antenna2[row]])
                .or_default()
                .push((row, time));
        }
    }
    Ok(groups)
}

fn smooth_group(
    table: &mut Table,
    column: &str,
    rows: &[(usize, f64)],
    smoother: Smoother,
    half_width: f64,
    lock: &mut TableWriteLock,
) -> Result<(), String> {
    let row_ids = rows.iter().map(|&(row, _)| row).collect::<Vec<_>>();
    let read = |name| {
        table
            .column_accessor(name)
            .and_then(|column| column.array_cells_owned_uncached(&row_ids))
            .map_err(|error| error.to_string())
    };
    let parameters = read(column)?;
    let mut flags = read("FLAG")?
        .into_iter()
        .map(|cell| match cell {
            Some(ArrayValue::Bool(flags)) => Ok(flags),
            _ => Err("calibration table contains an undefined FLAG cell".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut parameters = parameters
        .into_iter()
        .map(|cell| {
            cell.ok_or_else(|| format!("calibration table contains an undefined {column} cell"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let shape = flags[0].shape().to_vec();
    if shape.len() != 2
        || flags.iter().any(|cell| cell.shape() != shape)
        || parameters.iter().any(|cell| cell.shape() != shape)
    {
        return Err(
            "calibration solution and flag cell shapes disagree within a time series".into(),
        );
    }
    let mut by_time = (0..rows.len()).collect::<Vec<_>>();
    by_time.sort_by(|&a, &b| rows[a].1.total_cmp(&rows[b].1));
    let windows = rows
        .iter()
        .map(|&(_, time)| {
            let first = by_time.partition_point(|&row| rows[row].1 <= time - half_width);
            let end = by_time.partition_point(|&row| rows[row].1 <= time + half_width);
            &by_time[first..end]
        })
        .collect::<Vec<_>>();
    for parameter in 0..shape[0] {
        for channel in 0..shape[1] {
            let cell = [parameter, channel];
            let valid = flags.iter().map(|row| !row[cell]).collect::<Vec<_>>();
            if column == "CPARAM" {
                smooth_complex(&mut parameters, cell, &valid, &windows, smoother)?;
            } else {
                smooth_real(&mut parameters, cell, &valid, &windows, smoother)?;
            }
            for (row, window) in flags.iter_mut().zip(&windows) {
                row[cell] = !window.iter().any(|&sample| valid[sample]);
            }
        }
    }
    let columns = [column, "FLAG"];
    let mut writer = table
        .row_accessor_mut()
        .prepare(&columns)
        .map_err(|error| error.to_string())?;
    for ((&row, parameters), flags) in row_ids.iter().zip(parameters).zip(flags) {
        writer.seek(row).map_err(|error| error.to_string())?;
        writer
            .set_value_at(0, Value::Array(parameters))
            .map_err(|error| error.to_string())?;
        writer
            .set_value_at(1, Value::Array(ArrayValue::Bool(flags)))
            .map_err(|error| error.to_string())?;
    }
    writer.flush().map_err(|error| error.to_string())?;
    drop(writer);
    lock.record_write();
    table
        .prepare_write()
        .save_selected_rows(&columns, &row_ids)
        .map_err(|error| error.to_string())?;
    table.discard_persisted_cell_updates(&columns, &row_ids);
    Ok(())
}

fn smooth_complex(
    parameters: &mut [ArrayValue],
    cell: [usize; 2],
    valid: &[bool],
    windows: &[&[usize]],
    smoother: Smoother,
) -> Result<(), String> {
    let mut amplitudes = Vec::with_capacity(parameters.len());
    let mut phases = Vec::with_capacity(parameters.len());
    for parameter in &*parameters {
        let ArrayValue::Complex32(values) = parameter else {
            return Err("CPARAM must contain complex32 calibration solutions".into());
        };
        let value = values[cell];
        amplitudes.push(value.norm());
        let mut phase = value.arg();
        if let Some(&previous) = phases.last() {
            let pi = std::f64::consts::PI;
            while f64::from(phase) > f64::from(previous) + pi {
                phase = (f64::from(phase) - 2.0 * pi) as f32;
            }
            while f64::from(phase) < f64::from(previous) - pi {
                phase = (f64::from(phase) + 2.0 * pi) as f32;
            }
        }
        phases.push(phase);
    }
    let smoothed_amplitudes = smooth_series(&amplitudes, valid, windows, smoother);
    let smoothed_phases = smooth_series(&phases, valid, windows, smoother);
    for (row, (amplitude, phase)) in parameters
        .iter_mut()
        .zip(smoothed_amplitudes.into_iter().zip(smoothed_phases))
    {
        let ArrayValue::Complex32(values) = row else {
            unreachable!()
        };
        values[cell] = Complex32::from_polar(amplitude, phase);
    }
    Ok(())
}

fn smooth_real(
    parameters: &mut [ArrayValue],
    cell: [usize; 2],
    valid: &[bool],
    windows: &[&[usize]],
    smoother: Smoother,
) -> Result<(), String> {
    let values = parameters
        .iter()
        .map(|row| match row {
            ArrayValue::Float32(values) => Ok(values[cell]),
            _ => Err("FPARAM must contain float32 calibration solutions".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (row, value) in parameters
        .iter_mut()
        .zip(smooth_series(&values, valid, windows, smoother))
    {
        let ArrayValue::Float32(solution) = row else {
            unreachable!()
        };
        solution[cell] = value;
    }
    Ok(())
}

fn smooth_series(
    values: &[f32],
    valid: &[bool],
    windows: &[&[usize]],
    smoother: Smoother,
) -> Vec<f32> {
    let mut samples = Vec::new();
    windows
        .iter()
        .zip(values)
        .map(|(window, &original)| {
            samples.clear();
            samples.extend(
                window
                    .iter()
                    .filter(|&&row| valid[row])
                    .map(|&row| values[row]),
            );
            if samples.is_empty() {
                return original;
            }
            match smoother {
                Smoother::Mean => samples.iter().sum::<f32>() / samples.len() as f32,
                Smoother::Median => {
                    samples.sort_unstable_by(f32::total_cmp);
                    (samples[samples.len() / 2] + samples[(samples.len() - 1) / 2]) / 2.0
                }
            }
        })
        .collect()
}
