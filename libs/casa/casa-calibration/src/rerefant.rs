// SPDX-License-Identifier: LGPL-3.0-or-later
//! Re-reference a calibration table across solution intervals.

use std::path::Path;

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_tables::{RequiredScalarColumnValues, Table};
use ndarray::ArrayD;
use num_complex::Complex32;

use crate::solution_edit::{edit_calibration_table, error_text};

#[derive(Clone, Copy)]
enum ReferenceKind {
    Phase,
    Leakage,
    Fringe,
}

#[derive(Clone, Copy)]
struct SolutionIndex {
    row: usize,
    window: i32,
    time: f64,
    antenna: i32,
}

struct Solution {
    index: SolutionIndex,
    flags: ArrayD<bool>,
    parameters: ArrayValue,
}

pub fn rereference_calibration_table(
    ms_path: &Path,
    input: &Path,
    destination: &Path,
    flexible: bool,
    selector: &str,
) -> Result<(), String> {
    let ms = MeasurementSet::open(ms_path).map_err(error_text)?;
    let preferred = ms
        .resolve_reference_antennas(selector)
        .map_err(error_text)?;
    let Some(&primary) = preferred.first() else {
        return Err("no reference antenna specified".into());
    };
    let mut choices = vec![i32::try_from(primary).map_err(error_text)?];
    if flexible {
        let antenna = ms.antenna().map_err(error_text)?;
        let center = antenna
            .position(*preferred.last().unwrap())
            .map_err(error_text)?;
        let mut nearby = (0..antenna.row_count())
            .filter(|id| !preferred.contains(id))
            .map(|id| {
                let position = antenna.position(id).map_err(error_text)?;
                let distance = position
                    .into_iter()
                    .zip(center)
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f64>();
                Ok((id, distance))
            })
            .collect::<Result<Vec<_>, String>>()?;
        nearby.sort_by(|a, b| a.1.total_cmp(&b.1));
        // The second entry retains the last reference, ahead of the fallback list.
        choices.push(choices[0]);
        for id in preferred
            .into_iter()
            .skip(1)
            .chain(nearby.into_iter().map(|(id, _)| id))
        {
            choices.push(i32::try_from(id).map_err(error_text)?);
        }
    }
    edit_calibration_table(input, destination, |table, _| {
        edit_references(table, &mut choices)
    })
}

fn solution_indices(table: &Table) -> Result<Vec<SolutionIndex>, String> {
    let rows = (0..table.row_count()).collect::<Vec<_>>();
    let mut columns = table
        .required_scalar_columns_owned_for_rows(&["SPECTRAL_WINDOW_ID", "TIME", "ANTENNA1"], &rows)
        .map_err(error_text)?;
    let Some(RequiredScalarColumnValues::Float64(times)) = columns.remove("TIME") else {
        return Err("invalid calibration table TIME column".into());
    };
    let mut integer = |name| match columns.remove(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(format!("invalid calibration table {name} column")),
    };
    let windows = integer("SPECTRAL_WINDOW_ID")?;
    let antennas = integer("ANTENNA1")?;
    let mut indices = rows
        .into_iter()
        .map(|row| SolutionIndex {
            row,
            window: windows[row],
            time: times[row],
            antenna: antennas[row],
        })
        .collect::<Vec<_>>();
    indices.sort_by(|a, b| {
        a.window
            .cmp(&b.window)
            .then_with(|| a.time.total_cmp(&b.time))
    });
    Ok(indices)
}

fn edit_references(table: &mut Table, choices: &mut [i32]) -> Result<(), String> {
    let subtype = table.info().sub_type.trim().to_ascii_uppercase();
    let (kind, column) = match subtype.as_str() {
        "G JONES" | "B JONES" | "T JONES" | "TF JONES" => (ReferenceKind::Phase, "CPARAM"),
        "D JONES" | "DF JONES" | "DLLS JONES" | "DFLLS JONES" => (ReferenceKind::Leakage, "CPARAM"),
        "FRINGE JONES" => (ReferenceKind::Fringe, "FPARAM"),
        _ => {
            return Err(format!(
                "calibration type {} does not support re-referencing",
                table.info().sub_type
            ));
        }
    };
    let indices = solution_indices(table)?;
    for window in indices.chunk_by(|a, b| a.window == b.window) {
        let rows = window.iter().map(|index| index.row).collect::<Vec<_>>();
        let read = |name| {
            table
                .column_accessor(name)
                .and_then(|column| column.array_cells_owned_uncached(&rows))
                .map_err(error_text)
        };
        let mut solutions = window
            .iter()
            .copied()
            .zip(read("FLAG")?.into_iter().zip(read(column)?))
            .map(|(index, (flags, parameters))| match (flags, parameters) {
                (Some(ArrayValue::Bool(flags)), Some(parameters))
                    if flags.shape() == parameters.shape() =>
                {
                    Ok(Solution {
                        index,
                        flags,
                        parameters,
                    })
                }
                _ => {
                    Err("undefined or inconsistent calibration solution and FLAG cells".to_string())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let shape = solutions[0].flags.shape().to_vec();
        if shape.len() != 2
            || shape.contains(&0)
            || solutions.iter().any(|row| row.flags.shape() != shape)
        {
            return Err("calibration cells disagree within a spectral window".into());
        }
        let parameters_per_pol = match kind {
            ReferenceKind::Fringe if shape[0] == 8 => 4,
            ReferenceKind::Leakage if shape[0] == 2 => 1,
            ReferenceKind::Phase if shape[0] <= 2 => 1,
            _ => return Err("unexpected calibration parameter count for re-referencing".into()),
        };
        let second_polarization = shape[0] > parameters_per_pol
            && solutions
                .iter()
                .any(|row| has_polarization(row, parameters_per_pol, parameters_per_pol));
        let mut previous: Option<&[Solution]> = None;
        let mut writer = table
            .row_accessor_mut()
            .prepare(&[column, "FLAG", "ANTENNA2"])
            .map_err(error_text)?;
        for interval in solutions.chunk_by_mut(|a, b| a.index.time == b.index.time) {
            let from = previous.unwrap_or(interval);
            let chosen = choices.iter().find_map(|&antenna| {
                let acceptable = |rows: &[Solution]| {
                    let index = rows.iter().position(|row| row.index.antenna == antenna)?;
                    let row = &rows[index];
                    (has_polarization(row, 0, parameters_per_pol)
                        && (!second_polarization
                            || has_polarization(row, parameters_per_pol, parameters_per_pol)))
                    .then_some(index)
                };
                Some((antenna, acceptable(from)?, acceptable(interval)?))
            });
            if let Some((antenna, prior_row, current_row)) = chosen {
                let current = &interval[current_row];
                let prior = previous.map(|rows| &rows[prior_row]);
                let correction = reference_correction(kind, &shape, current, prior)?;
                if choices.len() > 1 {
                    choices[1] = antenna;
                }
                for row in interval.iter_mut() {
                    apply_correction(kind, &mut row.parameters, &correction)?;
                    writer.seek(row.index.row).map_err(error_text)?;
                    writer
                        .set_value_at(0, Value::Array(row.parameters.clone()))
                        .map_err(error_text)?;
                    writer
                        .set_value_at(2, Value::Scalar(ScalarValue::Int32(antenna)))
                        .map_err(error_text)?;
                }
                previous = Some(interval);
            } else {
                for row in interval {
                    row.flags.fill(true);
                    writer.seek(row.index.row).map_err(error_text)?;
                    writer
                        .set_value_at(1, Value::Array(ArrayValue::Bool(row.flags.clone())))
                        .map_err(error_text)?;
                }
            }
        }
        writer.flush().map_err(error_text)?;
    }
    Ok(())
}

fn has_polarization(row: &Solution, first: usize, count: usize) -> bool {
    let channels = row.flags.shape()[1];
    (first..first + count)
        .any(|parameter| (0..channels).any(|channel| !row.flags[[parameter, channel]]))
}

fn reference_correction(
    kind: ReferenceKind,
    shape: &[usize],
    current: &Solution,
    prior: Option<&Solution>,
) -> Result<ArrayValue, String> {
    let conditioned = |row: &Solution| match (&row.parameters, kind) {
        (ArrayValue::Float32(data), ReferenceKind::Fringe) => Ok(ArrayValue::Float32(data.clone())),
        (ArrayValue::Complex32(data), ReferenceKind::Phase) => {
            let mut data = data.clone();
            for (value, &flagged) in data.iter_mut().zip(&row.flags) {
                let amplitude = value.norm();
                *value = if flagged || amplitude < f32::EPSILON {
                    Complex32::new(1.0, 0.0)
                } else {
                    *value / amplitude
                };
            }
            Ok(ArrayValue::Complex32(data))
        }
        (ArrayValue::Complex32(data), ReferenceKind::Leakage) => {
            let mut data = data.clone();
            for channel in 0..shape[1] {
                data[[1, channel]] = -data[[0, channel]].conj();
            }
            Ok(ArrayValue::Complex32(data))
        }
        _ => Err("calibration parameter data type does not match its table type".to_string()),
    };
    let mut correction = conditioned(current)?;
    if let Some(previous) = prior {
        apply_correction(kind, &mut correction, &conditioned(previous)?)?;
    }
    Ok(correction)
}

fn apply_correction(
    kind: ReferenceKind,
    data: &mut ArrayValue,
    correction: &ArrayValue,
) -> Result<(), String> {
    match (data, correction, kind) {
        (ArrayValue::Complex32(data), ArrayValue::Complex32(offset), ReferenceKind::Phase) => {
            ndarray::Zip::from(data)
                .and(offset)
                .for_each(|value, &factor| *value /= factor);
        }
        (ArrayValue::Complex32(data), ArrayValue::Complex32(offset), ReferenceKind::Leakage) => {
            ndarray::Zip::from(data)
                .and(offset)
                .for_each(|value, &offset| *value -= offset);
        }
        (ArrayValue::Float32(data), ArrayValue::Float32(offset), ReferenceKind::Fringe) => {
            ndarray::Zip::from(data)
                .and(offset)
                .for_each(|value, &offset| *value -= offset);
        }
        _ => return Err("calibration parameter data type does not match its table type".into()),
    }
    Ok(())
}
