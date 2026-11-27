// SPDX-License-Identifier: LGPL-3.0-or-later
//! Smooth calibration solutions over centered windows.

mod fringe;

use std::collections::BTreeMap;
use std::path::Path;

use casa_core::{ArrayValue, Complex32, Value};
use casa_ms::MeasurementSet;
use casa_tables::{RequiredScalarColumnValues, Table, TableOptions};

use crate::solution_edit::{edit_calibration_table, error_text};

#[derive(Clone, Copy)]
/// Reduction applied to calibration solutions in each smoothing time window.
pub enum Smoother {
    /// Arithmetic mean of usable samples in the window.
    Mean,
    /// Median of usable samples in the window.
    Median,
}

impl Smoother {
    /// Resolves the CASA smoothing token, returning None for unsupported modes.
    pub fn from_name(mode: &str) -> Option<Self> {
        match mode {
            "mean" => Some(Self::Mean),
            "median" => Some(Self::Median),
            _ => None,
        }
    }
}

/// Smooths calibration solutions over a time interval in seconds for the selected fields and
/// publishes the result.
pub fn smooth_calibration_table(
    ms_path: &Path,
    input: &Path,
    destination: &Path,
    field: &str,
    smoother: Smoother,
    seconds: f64,
    ratesmooth: bool,
) -> Result<(), String> {
    if seconds.is_nan() || seconds <= 0.0 {
        return Err("smoothtime must be a positive duration in seconds".into());
    }
    let ms = MeasurementSet::open(ms_path).map_err(error_text)?;
    let fields = ms.resolve_field_selector(field).map_err(error_text)?;
    edit_calibration_table(input, destination, |table, path| {
        smooth_table(table, path, &fields, smoother, seconds / 2.0, ratesmooth)
    })
}

fn smooth_table(
    table: &mut Table,
    path: &Path,
    fields: &[i32],
    smoother: Smoother,
    half_width: f64,
    ratesmooth: bool,
) -> Result<(), String> {
    let subtype = table.info().sub_type.trim().to_ascii_uppercase();
    if !matches!(
        subtype.as_str(),
        "G JONES"
            | "T JONES"
            | "B JONES"
            | "ACCOR JONES"
            | "TF JONES"
            | "K JONES"
            | "KCROSS JONES"
            | "FRINGE JONES"
    ) {
        return Err(format!(
            "calibration type {} does not support smoothing",
            table.info().sub_type
        ));
    }
    let parameter_column = match table.schema() {
        Some(schema) if schema.contains_column("CPARAM") => "CPARAM",
        Some(schema) if schema.contains_column("FPARAM") => "FPARAM",
        _ => return Err("input table has no supported calibration parameter column".into()),
    };
    let groups = solution_groups(table, fields)?;
    let spectrum = if subtype == "FRINGE JONES" {
        Some(Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW"))).map_err(error_text)?)
    } else {
        None
    };
    for (key, rows) in groups.iter().filter(|(_, rows)| rows.len() > 1) {
        let fringe = if let Some(spectrum) = &spectrum {
            let window = usize::try_from(key[0]).map_err(error_text)?;
            let spectral_window = casa_ms::subtables::MsSpectralWindow::new(spectrum);
            let frequency = *spectral_window
                .chan_freq(window)
                .map_err(error_text)?
                .first()
                .ok_or_else(|| {
                    "fringe solution spectral window has no channel frequencies".to_string()
                })?;
            Some(FringePrediction {
                frequency,
                ratesmooth,
            })
        } else {
            None
        };
        smooth_group(table, parameter_column, rows, smoother, half_width, fringe)?;
    }
    Ok(())
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

struct FringePrediction {
    frequency: f64,
    ratesmooth: bool,
}

fn smooth_group(
    table: &mut Table,
    column: &str,
    rows: &[(usize, f64)],
    smoother: Smoother,
    half_width: f64,
    fringe: Option<FringePrediction>,
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
    if let Some(fringe) = fringe {
        fringe::apply(
            &mut parameters,
            &mut flags,
            rows,
            &windows,
            smoother,
            fringe.frequency,
            fringe.ratesmooth,
        )?;
    } else {
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
    writer.flush().map_err(error_text)
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
