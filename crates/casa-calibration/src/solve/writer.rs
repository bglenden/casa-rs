// SPDX-License-Identifier: LGPL-3.0-or-later
//! On-disk caltable writing for limited `gaincal`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{GainSolveError, GainSolveReport, GainSolveRequest};
use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_ARRAY_ID, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_FPARAM,
    COL_INTERVAL, COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR,
    COL_SPECTRAL_WINDOW_ID, COL_TIME, COL_TIME_EXTRA_PREC, COL_WEIGHT,
};
use crate::solve::SolvedGains;
use crate::solve::grouping::{SelectedSolveRow, selected_channels};
use crate::writer::{CalibrationTableDescriptor, CalibrationTableWriter};

pub(crate) fn write_gain_caltable(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    solved: &SolvedGains,
    mode: crate::CalibrationOutputMode,
) -> Result<GainSolveReport, GainSolveError> {
    let name = ms
        .path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<in-memory>".to_string());
    write_gain_caltable_named(ms, name, request, solved, mode)
}

/// [`write_gain_caltable`], recording `measurement_set_name` as the table's
/// `MSName`: the store the subtables of `ms` describe, when `ms` holds
/// them in memory.
pub(crate) fn write_gain_caltable_named(
    ms: &MeasurementSet,
    measurement_set_name: String,
    request: &GainSolveRequest,
    solved: &SolvedGains,
    mode: crate::CalibrationOutputMode,
) -> Result<GainSolveReport, GainSolveError> {
    write_gain_table(
        ms,
        measurement_set_name,
        request,
        solved,
        mode,
        request.gain_type.vis_cal(),
        GainTableSpectrum::Collapsed,
    )
}

pub(super) fn write_new_style_gain_caltable(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    solved: &SolvedGains,
    mode: crate::CalibrationOutputMode,
    subtype: &'static str,
) -> Result<GainSolveReport, GainSolveError> {
    let name = ms.path().map_or_else(
        || "<in-memory>".to_owned(),
        |path| path.display().to_string(),
    );
    write_gain_table(
        ms,
        name,
        request,
        solved,
        mode,
        subtype,
        GainTableSpectrum::Collapsed,
    )
}

pub(super) struct GainChannelGrid<'a> {
    pub(super) bin_widths: &'a BTreeMap<i32, usize>,
    pub(super) selected_rows: &'a [SelectedSolveRow],
}

pub(super) fn write_tf_caltable(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    solved: &SolvedGains,
    mode: crate::CalibrationOutputMode,
    channels: GainChannelGrid<'_>,
) -> Result<GainSolveReport, GainSolveError> {
    let name = ms.path().map_or_else(
        || "<in-memory>".to_owned(),
        |path| path.display().to_string(),
    );
    write_gain_table(
        ms,
        name,
        request,
        solved,
        mode,
        "Tf Jones",
        GainTableSpectrum::Channelized(channels),
    )
}

enum GainTableSpectrum<'a> {
    Collapsed,
    Channelized(GainChannelGrid<'a>),
    LegacyMueller(Option<GainChannelGrid<'a>>),
}

pub(super) fn write_baseline_caltable(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    solved: &SolvedGains,
    mode: crate::CalibrationOutputMode,
    subtype: &'static str,
    channels: Option<GainChannelGrid<'_>>,
    legacy: bool,
) -> Result<GainSolveReport, GainSolveError> {
    let name = ms.path().map_or_else(
        || "<in-memory>".to_owned(),
        |path| path.display().to_string(),
    );
    let spectrum = if legacy {
        GainTableSpectrum::LegacyMueller(channels)
    } else {
        channels.map_or(GainTableSpectrum::Collapsed, GainTableSpectrum::Channelized)
    };
    write_gain_table(ms, name, request, solved, mode, subtype, spectrum)
}

fn write_gain_table(
    ms: &MeasurementSet,
    measurement_set_name: String,
    request: &GainSolveRequest,
    solved: &SolvedGains,
    mode: crate::CalibrationOutputMode,
    subtype: &'static str,
    spectrum: GainTableSpectrum<'_>,
) -> Result<GainSolveReport, GainSolveError> {
    let _write_phase = crate::timing::Phase::start("gain_write");
    let rows = &solved.rows;
    if rows.is_empty() {
        return Ok(gain_report(request, solved));
    }
    let (channels, legacy_mueller) = match spectrum {
        GainTableSpectrum::Collapsed => (None, false),
        GainTableSpectrum::Channelized(grid) => (Some(grid), false),
        GainTableSpectrum::LegacyMueller(grid) => (grid, true),
    };
    let delay = request.gain_type.is_delay();
    let legacy_gain_metadata = !delay
        && !matches!(
            subtype,
            "Accor Jones" | "M Mueller" | "Mf Mueller" | "A Mueller"
        );
    let (parameter_column, parameter_type) = if delay {
        (COL_FPARAM, casa_types::PrimitiveType::Float32)
    } else {
        (COL_CPARAM, casa_types::PrimitiveType::Complex32)
    };
    let mut columns = vec![
        ColumnSchema::scalar(COL_TIME, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_types::PrimitiveType::Int32),
    ];
    if legacy_gain_metadata {
        columns.extend([
            ColumnSchema::scalar(COL_ARRAY_ID, casa_types::PrimitiveType::Int32),
            ColumnSchema::scalar(COL_TIME_EXTRA_PREC, casa_types::PrimitiveType::Float64),
        ]);
    }
    columns.extend([
        ColumnSchema::array_variable(parameter_column, parameter_type, Some(2)),
        ColumnSchema::array_variable(COL_PARAMERR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_FLAG, casa_types::PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable(COL_SNR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_WEIGHT, casa_types::PrimitiveType::Float32, Some(2)),
    ]);
    let schema = TableSchema::new(columns).expect("valid gain caltable schema");
    let mut writer = CalibrationTableWriter::with_mode(
        ms,
        CalibrationTableDescriptor {
            output: &request.output_table,
            schema,
            subtype,
            parameter_type: Some(if delay { "Float" } else { "Complex" }),
            measurement_set_name,
            include_polarization_basis: true,
            time_extra_precision_column: legacy_gain_metadata.then_some(COL_TIME_EXTRA_PREC),
        },
        mode,
    )?;

    for row in rows {
        let receptors = if channels.is_some() {
            request.gain_type.polarizations()
        } else {
            row.gains.len()
        };
        let shape = IxDyn(&[receptors, row.gains.len() / receptors]).f();
        let parameters = if delay {
            ArrayValue::Float32(
                ArrayD::from_shape_vec(
                    shape.clone(),
                    row.gains.iter().map(|gain| gain.re).collect(),
                )
                .expect("delay vector should reshape to receptor x channel"),
            )
        } else {
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(shape.clone(), row.gains.clone())
                    .expect("gain vector should reshape to receptor x channel"),
            )
        };
        let mut fields = vec![
            RecordField::new(
                COL_TIME,
                Value::Scalar(ScalarValue::Float64(row.time_seconds)),
            ),
            RecordField::new(
                COL_FIELD_ID,
                Value::Scalar(ScalarValue::Int32(row.field_id)),
            ),
            RecordField::new(
                COL_SPECTRAL_WINDOW_ID,
                Value::Scalar(ScalarValue::Int32(row.spw_id)),
            ),
            RecordField::new(
                COL_ANTENNA1,
                Value::Scalar(ScalarValue::Int32(row.antenna_id)),
            ),
            RecordField::new(
                COL_ANTENNA2,
                Value::Scalar(ScalarValue::Int32(row.refant_id)),
            ),
            // CASA writes every solution's INTERVAL as 0 (`SolvableVisCal::keepNCT`).
            RecordField::new(COL_INTERVAL, Value::Scalar(ScalarValue::Float64(0.0))),
            RecordField::new(
                COL_SCAN_NUMBER,
                Value::Scalar(ScalarValue::Int32(row.scan_number)),
            ),
            RecordField::new(
                COL_OBSERVATION_ID,
                Value::Scalar(ScalarValue::Int32(row.observation_id)),
            ),
            RecordField::new(parameter_column, Value::Array(parameters)),
            RecordField::new(
                COL_PARAMERR,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape.clone(), row.param_errors.clone())
                        .expect("paramerr vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_FLAG,
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape.clone(), row.flags.clone())
                        .expect("flag vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_SNR,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, row.snrs.clone())
                        .expect("snr vector should reshape to receptor x channel"),
                )),
            ),
        ];
        if legacy_gain_metadata {
            fields.push(RecordField::new(
                COL_ARRAY_ID,
                Value::Scalar(ScalarValue::Int32(0)),
            ));
            fields.push(RecordField::new(
                COL_TIME_EXTRA_PREC,
                Value::Scalar(ScalarValue::Float64(0.0)),
            ));
            fields.push(RecordField::new(
                COL_WEIGHT,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[row.gains.len(), 1]).f(), row.weights.clone())
                        .expect("weight vector should reshape to receptor x channel"),
                )),
            ));
        }
        writer.append(RecordValue::new(fields))?;
    }

    writer.copy_subtables(&[
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
    ])?;
    if let Some(channels) = channels {
        writer.copy_subtables(&[(SubtableId::SpectralWindow, "SPECTRAL_WINDOW")])?;
        let path = writer.output_path().join("SPECTRAL_WINDOW");
        if legacy_mueller {
            write_legacy_mueller_channels(&path, solved)?;
        } else {
            let mut windows = solved.attempted_spw_ids.clone();
            if subtype == "Tf Jones" {
                windows.extend(solved.rows.iter().map(|row| row.spw_id));
                if request.combine.spectral_windows {
                    windows.extend(channels.selected_rows.iter().map(|row| row.data_spw_id));
                }
            }
            crate::bandpass::write_solution_spectral_windows(
                &path,
                &windows,
                channels.bin_widths,
                channels.selected_rows,
                request.combine.spectral_windows,
            )
            .map_err(|error| {
                if subtype == "Tf Jones" {
                    GainSolveError::FrequencyScalar(error.to_string())
                } else {
                    GainSolveError::Baseline(error.to_string())
                }
            })?;
        }
    } else {
        write_gain_spectral_window_subtable(
            ms,
            writer.output_path(),
            solved,
            request.combine.spectral_windows,
            legacy_mueller,
        )?;
    }
    writer.save_main()?;

    Ok(gain_report(request, solved))
}

fn gain_report(request: &GainSolveRequest, solved: &SolvedGains) -> GainSolveReport {
    let rows = &solved.rows;
    let field_ids = rows.iter().map(|row| row.field_id).collect::<BTreeSet<_>>();
    let spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    GainSolveReport {
        output_table: request.output_table.clone(),
        gain_type: request.gain_type,
        refant_antenna_id: solved.refant_id,
        field_ids: field_ids.iter().copied().collect(),
        spectral_window_ids: spw_ids.iter().copied().collect(),
        solution_row_count: rows.len(),
        selected_data: solved.selected_data.clone(),
        solve_stats: solved.solve_stats.clone(),
    }
}

fn write_legacy_mueller_channels(path: &Path, solved: &SolvedGains) -> Result<(), GainSolveError> {
    let failed = |source| GainSolveError::CopySubtable {
        subtable: "SPECTRAL_WINDOW".to_string(),
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut table = Table::open(TableOptions::new(path)).map_err(failed)?;
    for row in 0..table.row_count() {
        let window = row as i32;
        let attempted = solved.attempted_spw_ids.contains(&window);
        let mut cells = vec![("FLAG_ROW", Value::Scalar(ScalarValue::Bool(!attempted)))];
        let widths = get_f64_array(&table, row, "CHAN_WIDTH")?;
        if !widths.is_empty() {
            let (selected, _) = crate::solve::grouping::polynomial::channel_selection(
                solved.channel_selections.get(&window),
                widths.len(),
            )?;
            let total_bandwidth = selected
                .iter()
                .map(|&channel| widths[channel])
                .sum::<f64>()
                .abs();
            for column in ["CHAN_FREQ", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION"] {
                let values = get_f64_array(&table, row, column)?;
                if values.len() != widths.len() {
                    return Err(GainSolveError::UnsupportedParameterShape {
                        path: format!("SPECTRAL_WINDOW row {row} {column}"),
                        shape: vec![values.len(), widths.len()],
                    });
                }
                let values = selected
                    .iter()
                    .map(|&channel| values[channel])
                    .collect::<Vec<_>>();
                cells.push((column, Value::Array(f64_array(&values))));
            }
            cells.extend([
                (
                    "NUM_CHAN",
                    Value::Scalar(ScalarValue::Int32(selected.len() as i32)),
                ),
                (
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(total_bandwidth)),
                ),
            ]);
        }
        for (column, value) in cells {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut cell| cell.set(value))
                .map_err(failed)?;
        }
    }
    table.save(TableOptions::new(path)).map_err(failed)
}

/// Write the table's `SPECTRAL_WINDOW` as CASA's `gaincal` writes a G or T
/// table's: the MeasurementSet's rows, each collapsed to the one channel a
/// solution holds ([`CollapsedWindow`]), the windows without solution rows
/// flagged (`NewCalTable::flagAbsentSpws`).
fn write_gain_spectral_window_subtable(
    ms: &MeasurementSet,
    output_root: &Path,
    solved: &SolvedGains,
    combine_spectral_windows: bool,
    legacy_mueller: bool,
) -> Result<(), GainSolveError> {
    let destination = output_root.join("SPECTRAL_WINDOW");
    let failed = |source| GainSolveError::CopySubtable {
        subtable: "SPECTRAL_WINDOW".to_string(),
        path: output_root.display().to_string(),
        source: Box::new(source),
    };
    ms.subtable(SubtableId::SpectralWindow)
        .expect("required subtable available")
        .save(TableOptions::new(&destination))
        .map_err(failed)?;
    let mut table = Table::open(TableOptions::new(&destination)).map_err(failed)?;
    let combined_window =
        if combine_spectral_windows && solved.selected_data.spectral_windows.len() > 1 {
            Some((
                solved.selected_data.spectral_windows[0],
                if legacy_mueller {
                    legacy_combined_window_metadata(&table, &solved.selected_data.spectral_windows)?
                } else {
                    combined_window_metadata(&table, solved)?
                },
            ))
        } else {
            None
        };

    let with_rows = solved
        .rows
        .iter()
        .map(|row| row.spw_id)
        .collect::<BTreeSet<_>>();
    if let Some(missing) = with_rows
        .iter()
        .chain(&solved.attempted_spw_ids)
        .find(|spw_id| usize::try_from(**spw_id).map_or(true, |row| row >= table.row_count()))
    {
        return Err(GainSolveError::UnsupportedParameterShape {
            path: format!("SPECTRAL_WINDOW rows, which lack the solved window {missing}"),
            shape: vec![table.row_count()],
        });
    }
    for row in 0..table.row_count() {
        let spw_id = i32::try_from(row).ok();
        let has_rows = spw_id.is_some_and(|spw_id| with_rows.contains(&spw_id));
        let attempted =
            has_rows || spw_id.is_some_and(|spw_id| solved.attempted_spw_ids.contains(&spw_id));
        let frequencies = get_f64_array(&table, row, "CHAN_FREQ")?;
        let widths = get_f64_array(&table, row, "CHAN_WIDTH")?;
        if frequencies.len() != widths.len() {
            return Err(GainSolveError::UnsupportedParameterShape {
                path: format!("SPECTRAL_WINDOW row {row} CHAN_FREQ and CHAN_WIDTH"),
                shape: vec![frequencies.len(), widths.len()],
            });
        }
        let collapsed =
            match combined_window {
                Some((reference, window)) if spw_id == Some(reference) => Some(window),
                _ if legacy_mueller => Some(legacy_collapsed_window(&table, row)?),
                None if attempted && !legacy_mueller => {
                    let selection = solved.channel_selections.get(&(row as i32));
                    let channels = selected_channels(selection, frequencies.len())?;
                    let window = CollapsedWindow::solved(&frequencies, &widths, channels)
                        .ok_or_else(|| GainSolveError::UnsupportedParameterShape {
                            path: format!(
                                "SPECTRAL_WINDOW row {row}, of which the solve selects no channel"
                            ),
                            shape: vec![frequencies.len()],
                        })?;
                    Some(window)
                }
                _ => CollapsedWindow::unsolved(
                    &frequencies,
                    &widths,
                    &get_f64_array(&table, row, "EFFECTIVE_BW")?,
                    &get_f64_array(&table, row, "RESOLUTION")?,
                ),
            };
        let mut cells = vec![("FLAG_ROW", Value::Scalar(ScalarValue::Bool(!has_rows)))];
        if let Some(window) = collapsed {
            cells.extend([
                ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(1))),
                ("CHAN_FREQ", Value::Array(f64_array(&[window.frequency]))),
                ("CHAN_WIDTH", Value::Array(f64_array(&[window.width]))),
                (
                    "EFFECTIVE_BW",
                    Value::Array(f64_array(&[window.effective_bw])),
                ),
                ("RESOLUTION", Value::Array(f64_array(&[window.resolution]))),
            ]);
            if let Some(total) = window.total_bandwidth {
                cells.push((
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(total)),
                ));
            }
        }
        for (column, value) in cells {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut cell| cell.set(value))
                .map_err(failed)?;
        }
    }

    table
        .save(TableOptions::new(&destination))
        .map_err(failed)?;

    Ok(())
}

pub(super) fn combined_window_frequency(
    ms: &MeasurementSet,
    solved: &SolvedGains,
) -> Result<f64, GainSolveError> {
    let spectral_windows = ms
        .subtable(SubtableId::SpectralWindow)
        .expect("required subtable available");
    Ok(combined_window_metadata(spectral_windows, solved)?.frequency)
}

fn combined_window_metadata(
    table: &Table,
    solved: &SolvedGains,
) -> Result<CollapsedWindow, GainSolveError> {
    let mut weighted_width = 0.0;
    let mut weighted_frequency = 0.0;
    let mut bandwidth = 0.0;
    for &spw_id in &solved.selected_data.spectral_windows {
        let row = spw_id as usize;
        let frequencies = get_f64_array(table, row, "CHAN_FREQ")?;
        let widths = get_f64_array(table, row, "CHAN_WIDTH")?;
        let channels =
            selected_channels(solved.channel_selections.get(&spw_id), frequencies.len())?;
        let window = CollapsedWindow::solved(&frequencies, &widths, channels).ok_or_else(|| {
            GainSolveError::UnsupportedParameterShape {
                path: format!("SPECTRAL_WINDOW row {row} in a combined solve"),
                shape: vec![frequencies.len(), widths.len()],
            }
        })?;
        weighted_frequency += window.frequency * window.effective_bw;
        weighted_width += window.width * window.effective_bw;
        bandwidth += window.effective_bw;
    }
    let frequency = weighted_frequency / bandwidth;
    Ok(CollapsedWindow {
        frequency,
        width: weighted_width / bandwidth,
        effective_bw: bandwidth,
        resolution: frequency,
        total_bandwidth: Some(frequency),
    })
}

fn legacy_collapsed_window(table: &Table, row: usize) -> Result<CollapsedWindow, GainSolveError> {
    let frequencies = get_f64_array(table, row, "CHAN_FREQ")?;
    let mut window = CollapsedWindow::unsolved(
        &frequencies,
        &get_f64_array(table, row, "CHAN_WIDTH")?,
        &get_f64_array(table, row, "EFFECTIVE_BW")?,
        &get_f64_array(table, row, "RESOLUTION")?,
    )
    .ok_or_else(|| GainSolveError::UnsupportedParameterShape {
        path: format!("SPECTRAL_WINDOW row {row} without channels"),
        shape: vec![frequencies.len()],
    })?;
    window.total_bandwidth = Some(window.width.abs());
    Ok(window)
}

fn legacy_combined_window_metadata(
    table: &Table,
    windows: &[i32],
) -> Result<CollapsedWindow, GainSolveError> {
    let mut reference = legacy_collapsed_window(table, windows[0] as usize)?;
    let mut weighted_frequency = reference.frequency * reference.effective_bw;
    for &window in &windows[1..] {
        let next = legacy_collapsed_window(table, window as usize)?;
        weighted_frequency += next.frequency * next.effective_bw;
        reference.effective_bw += next.effective_bw;
    }
    reference.frequency = weighted_frequency / reference.effective_bw;
    Ok(reference)
}

/// The one channel of a window's row in a G or T gain table: what CASA's
/// `gaincal` writes, measured with CASA 6.7.5 (#917).
#[derive(Clone, Copy, Debug, PartialEq)]
struct CollapsedWindow {
    frequency: f64,
    width: f64,
    effective_bw: f64,
    resolution: f64,
    /// `None` keeps the MeasurementSet's.
    total_bandwidth: Option<f64>,
}

impl CollapsedWindow {
    /// A window a solve reached, over `channels` of `frequencies` and
    /// `widths` (as many of each): the arithmetic mean of their
    /// frequencies, unweighted whatever their widths, and the sum of their
    /// widths' magnitudes. CASA's solve averages and sums them before a G or
    /// T solve (`ChannelAverageTVI`, over the channels in ascending order;
    /// `VisibilityIteratorImpl2` holds widths positive), and
    /// `SolvableVisCal::setCTFrequencies` writes the sum as `CHAN_WIDTH` and
    /// `EFFECTIVE_BW` and the frequency as `RESOLUTION` and
    /// `TOTAL_BANDWIDTH`. casa-rs writes those two as CASA does, though they
    /// are meant to be widths: CASA appends to a table only if they match
    /// its own (`docs/CASA (C++) bugs.md`). `None` when `channels` is empty
    /// or names a channel the window lacks.
    fn solved(frequencies: &[f64], widths: &[f64], mut channels: Vec<usize>) -> Option<Self> {
        if channels.is_empty() || channels.iter().any(|channel| *channel >= widths.len()) {
            return None;
        }
        channels.sort_unstable();
        let frequency = channels
            .iter()
            .map(|channel| frequencies[*channel])
            .sum::<f64>()
            / channels.len() as f64;
        let width = channels
            .iter()
            .map(|channel| widths[*channel].abs())
            .sum::<f64>();
        Some(Self {
            frequency,
            width,
            effective_bw: width,
            resolution: frequency,
            total_bandwidth: Some(frequency),
        })
    }

    /// A window no solve reached, as `SolvableVisCal::createMemCalTable2`
    /// collapses every window of a table whose parameters do not depend on
    /// frequency: the mean of all its frequencies and the sums of its
    /// `CHAN_WIDTH`, `EFFECTIVE_BW` and `RESOLUTION`, signs kept, its
    /// `TOTAL_BANDWIDTH` the MeasurementSet's. `None` for a window without
    /// channels, left as it is.
    fn unsolved(
        frequencies: &[f64],
        widths: &[f64],
        effective_bw: &[f64],
        resolution: &[f64],
    ) -> Option<Self> {
        (!frequencies.is_empty()).then(|| Self {
            frequency: frequencies.iter().sum::<f64>() / frequencies.len() as f64,
            width: widths.iter().sum(),
            effective_bw: effective_bw.iter().sum(),
            resolution: resolution.iter().sum(),
            total_bandwidth: None,
        })
    }
}

fn get_f64_array(table: &Table, row: usize, column: &str) -> Result<Vec<f64>, GainSolveError> {
    match table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(MsError::from)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: column.to_string(),
            source,
        })? {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        other => Err(GainSolveError::UnsupportedParameterShape {
            path: format!("{column} array"),
            shape: other.shape().to_vec(),
        }),
    }
}

fn f64_array(values: &[f64]) -> ArrayValue {
    ArrayValue::Float64(
        ArrayD::from_shape_vec(IxDyn(&[values.len()]).f(), values.to_vec())
            .expect("f64 vector should reshape to 1-D array"),
    )
}

#[cfg(test)]
mod tests {
    use super::CollapsedWindow;

    /// `alma/multiobs.ms` (casatestdata): window 1, four channels of
    /// decreasing frequency, each −117.1875 MHz wide.
    const LOWER: [f64; 4] = [
        217_281_281_250.0,
        217_164_093_750.0,
        217_046_906_250.0,
        216_929_718_750.0,
    ];
    const LOWER_WIDTH: f64 = -117_187_500.0;

    /// What CASA 6.7.5's `gaincal` (G, `calmode='ap'`) wrote for that
    /// window: solved, at the arithmetic mean of its selected channels
    /// with their widths' summed magnitude, the mean again as resolution
    /// and total bandwidth; without solutions (another window solved), at
    /// the mean of all its channels with each column's signed sum.
    #[test]
    fn windows_collapse_as_casa_gaincal_wrote_them() {
        let widths = [LOWER_WIDTH; 4];
        let magnitudes = [-LOWER_WIDTH; 4];
        for (channels, frequency, width) in [
            (vec![0, 1, 2, 3], 217_105_500_000.0, 468_750_000.0),
            (vec![1, 2], 217_105_500_000.0, 234_375_000.0),
        ] {
            assert_eq!(
                CollapsedWindow::solved(&LOWER, &widths, channels.clone()),
                Some(CollapsedWindow {
                    frequency,
                    width,
                    effective_bw: width,
                    resolution: frequency,
                    total_bandwidth: Some(frequency),
                }),
                "{channels:?}"
            );
        }
        assert_eq!(
            CollapsedWindow::unsolved(&LOWER, &widths, &magnitudes, &magnitudes),
            Some(CollapsedWindow {
                frequency: 217_105_500_000.0,
                width: -468_750_000.0,
                effective_bw: 468_750_000.0,
                resolution: 468_750_000.0,
                total_bandwidth: None,
            })
        );
    }

    /// The mean is unweighted whatever the widths: CASA's frequency for
    /// ngc5921 with its widths rewritten to grow threefold across the
    /// window stayed the plain mean, 130 kHz from the width-weighted one.
    /// It sums the channels in ascending order, whatever order the
    /// selection names them in.
    #[test]
    fn the_mean_ignores_the_widths_and_the_selections_order() {
        let frequencies = [1.0e9, 1.001e9, 1.003e9];
        let widths = [1.0e6, 3.0e6, 1.0e6];
        let window = CollapsedWindow::solved(&frequencies, &widths, vec![0, 1]).expect("solved");
        assert_eq!((window.frequency, window.width), (1.0005e9, 4.0e6));
        assert_eq!(
            CollapsedWindow::solved(&frequencies, &widths, vec![2, 0, 1]),
            CollapsedWindow::solved(&frequencies, &widths, vec![0, 1, 2])
        );
    }

    /// No channels, or a channel the window lacks, collapse to nothing.
    #[test]
    fn a_selection_outside_the_window_is_refused() {
        assert_eq!(CollapsedWindow::solved(&[1.0e9], &[1.0e6], vec![]), None);
        assert_eq!(CollapsedWindow::solved(&[1.0e9], &[1.0e6], vec![1]), None);
        assert_eq!(CollapsedWindow::unsolved(&[], &[], &[], &[]), None);
    }
}
