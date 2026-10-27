// SPDX-License-Identifier: LGPL-3.0-or-later
//! On-disk caltable writing for limited `gaincal`.

use std::collections::BTreeSet;
use std::path::Path;

use casa_ms::MsError;
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{GainSolveError, GainSolveReport, GainSolveRequest};
use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_ARRAY_ID, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
    COL_TIME_EXTRA_PREC, COL_WEIGHT,
};
use crate::solve::grouping::selected_channels;
use crate::solve::kernel::SolutionRow;
use crate::writer::{CalibrationTableDescriptor, CalibrationTableWriter};

pub(crate) fn write_gain_caltable(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    refant_id: i32,
    rows: &[SolutionRow],
) -> Result<GainSolveReport, GainSolveError> {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casa_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ARRAY_ID, casa_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_TIME_EXTRA_PREC, casa_types::PrimitiveType::Float64),
        ColumnSchema::array_variable(COL_CPARAM, casa_types::PrimitiveType::Complex32, Some(2)),
        ColumnSchema::array_variable(COL_PARAMERR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_FLAG, casa_types::PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable(COL_SNR, casa_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_WEIGHT, casa_types::PrimitiveType::Float32, Some(2)),
    ])
    .expect("valid gain caltable schema");
    let mut writer = CalibrationTableWriter::create(
        ms,
        CalibrationTableDescriptor {
            output: &request.output_table,
            schema,
            subtype: request.gain_type.vis_cal(),
            parameter_type: Some("Complex"),
            measurement_set_name: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            include_polarization_basis: true,
            time_extra_precision_column: Some(COL_TIME_EXTRA_PREC),
        },
    )?;

    for row in rows {
        writer.append(RecordValue::new(vec![
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
            RecordField::new(COL_ARRAY_ID, Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                COL_TIME_EXTRA_PREC,
                Value::Scalar(ScalarValue::Float64(0.0)),
            ),
            RecordField::new(
                COL_CPARAM,
                Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(IxDyn(&[row.gains.len(), 1]).f(), row.gains.clone())
                        .expect("gain vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_PARAMERR,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        IxDyn(&[row.gains.len(), 1]).f(),
                        row.param_errors.clone(),
                    )
                    .expect("paramerr vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_FLAG,
                Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(IxDyn(&[row.flags.len(), 1]).f(), row.flags.clone())
                        .expect("flag vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_SNR,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[row.gains.len(), 1]).f(), row.snrs.clone())
                        .expect("snr vector should reshape to receptor x channel"),
                )),
            ),
            RecordField::new(
                COL_WEIGHT,
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[row.gains.len(), 1]).f(), row.weights.clone())
                        .expect("weight vector should reshape to receptor x channel"),
                )),
            ),
        ]))?;
    }

    writer.copy_subtables(&[
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
    ])?;
    write_gain_spectral_window_subtable(ms, writer.output_path(), request, rows)?;
    writer.save_main()?;

    let field_ids = rows.iter().map(|row| row.field_id).collect::<BTreeSet<_>>();
    let spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    Ok(GainSolveReport {
        output_table: request.output_table.clone(),
        gain_type: request.gain_type,
        refant_antenna_id: refant_id,
        field_ids: field_ids.iter().copied().collect(),
        spectral_window_ids: spw_ids.iter().copied().collect(),
        solution_row_count: rows.len(),
    })
}

/// Write the table's `SPECTRAL_WINDOW` as CASA's `gaincal` writes a G or T
/// table's: the MeasurementSet's rows, each collapsed to the one channel a
/// solution holds ([`CollapsedWindow`]), the windows without solutions
/// flagged.
fn write_gain_spectral_window_subtable(
    ms: &MeasurementSet,
    output_root: &Path,
    request: &GainSolveRequest,
    rows: &[SolutionRow],
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

    let solved_spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    for row in 0..table.row_count() {
        let frequencies = get_f64_array(&table, row, "CHAN_FREQ")?;
        let widths = get_f64_array(&table, row, "CHAN_WIDTH")?;
        let solved = i32::try_from(row).is_ok_and(|spw_id| solved_spw_ids.contains(&spw_id));
        let collapsed = if solved {
            let selection = request.selection.channel_selection_for_spw(row as i32);
            let channels = selected_channels(selection.as_ref(), frequencies.len())?;
            let window =
                CollapsedWindow::solved(&frequencies, &widths, &channels).ok_or_else(|| {
                    GainSolveError::UnsupportedParameterShape {
                        path: format!("SPECTRAL_WINDOW row {row} selected channels"),
                        shape: vec![frequencies.len(), widths.len()],
                    }
                })?;
            Some(window)
        } else {
            CollapsedWindow::unsolved(
                &frequencies,
                &widths,
                &get_f64_array(&table, row, "EFFECTIVE_BW")?,
                &get_f64_array(&table, row, "RESOLUTION")?,
            )
        };
        let mut cells = vec![("FLAG_ROW", Value::Scalar(ScalarValue::Bool(!solved)))];
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

/// The one channel of a window's row in a G or T gain table: what CASA's
/// `gaincal` writes, measured with CASA 6.7.5 (#917), except where noted.
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
    /// A window solved over `channels`: the arithmetic mean of their
    /// frequencies, unweighted whatever their widths, and the sum of their
    /// widths' magnitudes, which CASA's solve averages and sums before a G
    /// or T solve (`ChannelAverageTVI`; `VisibilityIteratorImpl2` holds
    /// widths positive) and `SolvableVisCal::setCTFrequencies` writes, the
    /// sum as `CHAN_WIDTH` and `EFFECTIVE_BW`. CASA also writes the
    /// frequency as `RESOLUTION` and `TOTAL_BANDWIDTH`; casa-rs writes the
    /// summed width there (`docs/CASA (C++) bugs.md`). `None` when
    /// `channels` is empty or outside the window.
    fn solved(frequencies: &[f64], widths: &[f64], channels: &[usize]) -> Option<Self> {
        if channels.is_empty() || channels.iter().any(|channel| *channel >= widths.len()) {
            return None;
        }
        let frequency = channels
            .iter()
            .map(|channel| frequencies.get(*channel))
            .sum::<Option<f64>>()?
            / channels.len() as f64;
        let width = channels
            .iter()
            .map(|channel| widths[*channel].abs())
            .sum::<f64>();
        Some(Self {
            frequency,
            width,
            effective_bw: width,
            resolution: width,
            total_bandwidth: Some(width),
        })
    }

    /// A window without solutions, as `SolvableVisCal::createMemCalTable2`
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
    /// with their widths' summed magnitude; without solutions (another
    /// window solved), at the mean of all its channels with each column's
    /// signed sum.
    #[test]
    fn windows_collapse_as_casa_gaincal_wrote_them() {
        let widths = [LOWER_WIDTH; 4];
        let magnitudes = [-LOWER_WIDTH; 4];
        for (channels, frequency, width) in [
            (vec![0, 1, 2, 3], 217_105_500_000.0, 468_750_000.0),
            (vec![1, 2], 217_105_500_000.0, 234_375_000.0),
        ] {
            assert_eq!(
                CollapsedWindow::solved(&LOWER, &widths, &channels),
                Some(CollapsedWindow {
                    frequency,
                    width,
                    effective_bw: width,
                    resolution: width,
                    total_bandwidth: Some(width),
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
    #[test]
    fn the_mean_ignores_the_widths() {
        let window =
            CollapsedWindow::solved(&[1.0e9, 1.001e9], &[1.0e6, 3.0e6], &[0, 1]).expect("solved");
        assert_eq!((window.frequency, window.width), (1.0005e9, 4.0e6));
    }

    /// No channels, or a channel the window lacks, collapse to nothing.
    #[test]
    fn a_selection_outside_the_window_is_refused() {
        assert_eq!(CollapsedWindow::solved(&[1.0e9], &[1.0e6], &[]), None);
        assert_eq!(CollapsedWindow::solved(&[1.0e9], &[1.0e6], &[1]), None);
        assert_eq!(CollapsedWindow::unsolved(&[], &[], &[], &[]), None);
    }
}
