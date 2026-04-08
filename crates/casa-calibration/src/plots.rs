// SPDX-License-Identifier: LGPL-3.0-or-later
//! Plot payload builders for calibration workflows.
//!
//! The first shipped `calibrate` plot catalog deliberately focuses on the
//! highest-value diagnostics operators reach for immediately:
//!
//! - gain-table phase and amplitude against time
//! - bandpass amplitude and phase against frequency
//! - corrected-data amplitude/phase against time/frequency
//!
//! This follows common CASA practice around `plotcal`, `plotbandpass`, and
//! `plotms`, while keeping the application layer thin by lowering every plot
//! request into the existing generic `casacore-ms` scatter payloads.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};

use casacore_ms::{
    MeasurementSet, MsAxis, MsDataColumn, MsLegendPosition, MsPlotPayload, MsPlotPreset,
    MsPlotSpec, MsScatterPlotPayload, MsScatterPointRef, MsScatterSeries, MsSelectionSpec,
    VisibilityDataColumn, build_msexplore_plot_payload,
};
use casacore_tables::{Table, TableError, TableOptions};
use casacore_types::{ArrayValue, Complex32, ScalarValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{COL_CPARAM, COL_FLAG, COL_TIME};
use crate::{CalibrationTableError, CalibrationTableSummary, summarize_table};

/// Stable preset identifiers for the initial `calibrate` plot catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalibrationPlotPreset {
    /// Gain-table phase against time.
    GainPhaseVsTime,
    /// Gain-table amplitude against time.
    GainAmplitudeVsTime,
    /// Bandpass amplitude against channel-center frequency.
    BandpassAmplitudeVsFrequency,
    /// Bandpass phase against channel-center frequency.
    BandpassPhaseVsFrequency,
    /// Corrected-data amplitude against time.
    CorrectedAmplitudeVsTime,
    /// Corrected-data phase against time.
    CorrectedPhaseVsTime,
    /// Corrected-data amplitude against channel-center frequency.
    CorrectedAmplitudeVsFrequency,
    /// Corrected-data phase against channel-center frequency.
    CorrectedPhaseVsFrequency,
}

impl CalibrationPlotPreset {
    /// Stable catalog order used by the launcher.
    pub const ALL: [Self; 8] = [
        Self::CorrectedAmplitudeVsTime,
        Self::CorrectedPhaseVsTime,
        Self::CorrectedAmplitudeVsFrequency,
        Self::CorrectedPhaseVsFrequency,
        Self::GainPhaseVsTime,
        Self::GainAmplitudeVsTime,
        Self::BandpassAmplitudeVsFrequency,
        Self::BandpassPhaseVsFrequency,
    ];

    /// Human-readable catalog label.
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::GainPhaseVsTime => "Inspect: Gain Phase vs Time",
            Self::GainAmplitudeVsTime => "Inspect: Gain Amplitude vs Time",
            Self::BandpassAmplitudeVsFrequency => "Inspect: Bandpass Amplitude vs Frequency",
            Self::BandpassPhaseVsFrequency => "Inspect: Bandpass Phase vs Frequency",
            Self::CorrectedAmplitudeVsTime => "Diagnostic: Corrected Amplitude vs Time",
            Self::CorrectedPhaseVsTime => "Diagnostic: Corrected Phase vs Time",
            Self::CorrectedAmplitudeVsFrequency => "Diagnostic: Corrected Amplitude vs Frequency",
            Self::CorrectedPhaseVsFrequency => "Diagnostic: Corrected Phase vs Frequency",
        }
    }

    /// Short operator-facing explanation.
    pub const fn summary(self) -> &'static str {
        match self {
            Self::GainPhaseVsTime => {
                "Immediate gain-solve sanity check: phases should evolve smoothly in time per antenna."
            }
            Self::GainAmplitudeVsTime => {
                "Immediate gain-solve sanity check: amplitudes should stay smooth and close to unity unless expected otherwise."
            }
            Self::BandpassAmplitudeVsFrequency => {
                "Primary bandpass inspection: look for stable per-antenna spectral structure and obvious edge/pathology channels."
            }
            Self::BandpassPhaseVsFrequency => {
                "Bandpass phase inspection: phases should vary smoothly across frequency after prior gain preapply."
            }
            Self::CorrectedAmplitudeVsTime => {
                "Post-apply diagnostic on corrected visibilities: verify time stability and remaining outliers."
            }
            Self::CorrectedPhaseVsTime => {
                "Post-apply diagnostic on corrected visibilities: check residual phase coherence over time."
            }
            Self::CorrectedAmplitudeVsFrequency => {
                "Post-apply diagnostic on corrected visibilities: inspect residual spectral shape after calibration."
            }
            Self::CorrectedPhaseVsFrequency => {
                "Post-apply diagnostic on corrected visibilities: inspect residual phase structure across frequency."
            }
        }
    }

    /// Whether this preset inspects a calibration table rather than an MS.
    pub const fn uses_calibration_table(self) -> bool {
        matches!(
            self,
            Self::GainPhaseVsTime
                | Self::GainAmplitudeVsTime
                | Self::BandpassAmplitudeVsFrequency
                | Self::BandpassPhaseVsFrequency
        )
    }

    fn scatter_axes(self) -> (MsAxis, MsAxis) {
        match self {
            Self::GainPhaseVsTime | Self::CorrectedPhaseVsTime => (MsAxis::Time, MsAxis::Phase),
            Self::GainAmplitudeVsTime | Self::CorrectedAmplitudeVsTime => {
                (MsAxis::Time, MsAxis::Amplitude)
            }
            Self::BandpassAmplitudeVsFrequency | Self::CorrectedAmplitudeVsFrequency => {
                (MsAxis::Frequency, MsAxis::Amplitude)
            }
            Self::BandpassPhaseVsFrequency | Self::CorrectedPhaseVsFrequency => {
                (MsAxis::Frequency, MsAxis::Phase)
            }
        }
    }
}

/// Inputs needed to build one `calibrate` plot payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CalibrationPlotRequest {
    /// MeasurementSet path used by corrected-data diagnostic plots.
    pub measurement_set_path: Option<PathBuf>,
    /// Calibration-table path used by inspection plots.
    pub calibration_table_path: Option<PathBuf>,
    /// Shared MeasurementSet selection controls used by corrected-data plots.
    pub selection: MsSelectionSpec,
}

/// Errors returned while preparing calibration plot payloads.
#[derive(Debug, Error)]
pub enum CalibrationPlotError {
    /// A corrected-data plot was requested without an MS path.
    #[error("measurement set path is required for {preset}")]
    MissingMeasurementSet {
        /// Requested plot.
        preset: &'static str,
    },

    /// A calibration-table plot was requested without a table path.
    #[error("calibration table path is required for {preset}")]
    MissingCalibrationTable {
        /// Requested plot.
        preset: &'static str,
    },

    /// The MeasurementSet could not be opened.
    #[error("failed to open measurement set {path}: {message}")]
    OpenMeasurementSet {
        /// Requested MeasurementSet path.
        path: String,
        /// Underlying error rendered for display.
        message: String,
    },

    /// The requested corrected-data plot needs `CORRECTED_DATA`.
    #[error("measurement set {path} does not contain CORRECTED_DATA; run calibrate apply first")]
    MissingCorrectedData {
        /// Requested MeasurementSet path.
        path: String,
    },

    /// Building the generic scatter payload failed.
    #[error("failed to build corrected-data plot for {path}: {message}")]
    MeasurementSetPlot {
        /// Requested MeasurementSet path.
        path: String,
        /// Underlying error rendered for display.
        message: String,
    },

    /// The calibration table could not be summarized.
    #[error("failed to summarize calibration table {path}: {source}")]
    SummarizeCalibrationTable {
        /// Requested table path.
        path: String,
        /// Underlying summary error.
        #[source]
        source: Box<CalibrationTableError>,
    },

    /// The calibration table could not be opened.
    #[error("failed to open calibration table {path}: {source}")]
    OpenCalibrationTable {
        /// Requested table path.
        path: String,
        /// Underlying table error.
        #[source]
        source: TableError,
    },

    /// One linked subtable could not be opened.
    #[error("failed to open {subtable} subtable for {path}: {source}")]
    OpenCalibrationSubtable {
        /// Parent table path.
        path: String,
        /// Subtable name.
        subtable: &'static str,
        /// Underlying table error.
        #[source]
        source: TableError,
    },

    /// The selected plot does not match the table family.
    #[error("{path} does not look like a compatible table for {preset}: {reason}")]
    UnsupportedCalibrationTable {
        /// Requested table path.
        path: String,
        /// Requested plot.
        preset: &'static str,
        /// Additional detail.
        reason: String,
    },

    /// Required calibration table metadata was missing or malformed.
    #[error("missing or malformed calibration metadata in {path}: {reason}")]
    InvalidCalibrationTable {
        /// Requested table path.
        path: String,
        /// Additional detail.
        reason: String,
    },

    /// The selected plot had no unflagged samples to render.
    #[error("no plottable samples remain for {preset} from {path}")]
    EmptyPlot {
        /// Requested resource path.
        path: String,
        /// Requested plot.
        preset: &'static str,
    },
}

/// Build one launcher-ready scatter payload for the requested calibration plot.
pub fn build_calibration_plot_payload(
    request: &CalibrationPlotRequest,
    preset: CalibrationPlotPreset,
) -> Result<MsPlotPayload, CalibrationPlotError> {
    if preset.uses_calibration_table() {
        let path = request.calibration_table_path.as_ref().ok_or(
            CalibrationPlotError::MissingCalibrationTable {
                preset: preset.display_name(),
            },
        )?;
        build_calibration_table_plot(path, preset)
    } else {
        let path = request.measurement_set_path.as_ref().ok_or(
            CalibrationPlotError::MissingMeasurementSet {
                preset: preset.display_name(),
            },
        )?;
        build_corrected_data_plot(path, &request.selection, preset)
    }
}

fn build_corrected_data_plot(
    path: &Path,
    selection: &MsSelectionSpec,
    preset: CalibrationPlotPreset,
) -> Result<MsPlotPayload, CalibrationPlotError> {
    let ms =
        MeasurementSet::open(path).map_err(|error| CalibrationPlotError::OpenMeasurementSet {
            path: path.display().to_string(),
            message: error.to_string(),
        })?;
    if ms.data_column(VisibilityDataColumn::CorrectedData).is_err() {
        return Err(CalibrationPlotError::MissingCorrectedData {
            path: path.display().to_string(),
        });
    }

    let mapped_preset = match preset {
        CalibrationPlotPreset::CorrectedAmplitudeVsTime => MsPlotPreset::AmplitudeVsTime,
        CalibrationPlotPreset::CorrectedPhaseVsTime => MsPlotPreset::PhaseVsTime,
        CalibrationPlotPreset::CorrectedAmplitudeVsFrequency => MsPlotPreset::AmplitudeVsFrequency,
        CalibrationPlotPreset::CorrectedPhaseVsFrequency => MsPlotPreset::PhaseVsFrequency,
        _ => unreachable!("caller gated corrected-data presets"),
    };
    let mut plot = MsPlotSpec::from_preset(mapped_preset);
    plot.data_column = MsDataColumn::Corrected;
    build_msexplore_plot_payload(&ms, selection, &plot).map_err(|message| {
        CalibrationPlotError::MeasurementSetPlot {
            path: path.display().to_string(),
            message,
        }
    })
}

fn build_calibration_table_plot(
    path: &Path,
    preset: CalibrationPlotPreset,
) -> Result<MsPlotPayload, CalibrationPlotError> {
    let summary = summarize_table(path).map_err(|source| {
        CalibrationPlotError::SummarizeCalibrationTable {
            path: path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    let table = Table::open(TableOptions::new(path)).map_err(|source| {
        CalibrationPlotError::OpenCalibrationTable {
            path: path.display().to_string(),
            source,
        }
    })?;
    let series = match preset {
        CalibrationPlotPreset::GainPhaseVsTime | CalibrationPlotPreset::GainAmplitudeVsTime => {
            build_gain_time_series(&summary, &table, preset)?
        }
        CalibrationPlotPreset::BandpassAmplitudeVsFrequency
        | CalibrationPlotPreset::BandpassPhaseVsFrequency => {
            build_bandpass_frequency_series(&summary, &table, preset)?
        }
        _ => unreachable!("caller gated calibration-table presets"),
    };

    if series.is_empty() {
        return Err(CalibrationPlotError::EmptyPlot {
            path: path.display().to_string(),
            preset: preset.display_name(),
        });
    }

    let (x_axis, y_axis) = preset.scatter_axes();
    let (x_label, y_label) = match preset {
        CalibrationPlotPreset::GainPhaseVsTime => ("Time (MJD seconds)", "Phase (deg)"),
        CalibrationPlotPreset::GainAmplitudeVsTime => ("Time (MJD seconds)", "Amplitude"),
        CalibrationPlotPreset::BandpassAmplitudeVsFrequency => ("Frequency (Hz)", "Amplitude"),
        CalibrationPlotPreset::BandpassPhaseVsFrequency => ("Frequency (Hz)", "Phase (deg)"),
        _ => unreachable!("caller gated calibration-table presets"),
    };
    let point_count = series
        .iter()
        .map(|series| series.points.len())
        .sum::<usize>();
    let title = match preset {
        CalibrationPlotPreset::GainPhaseVsTime => "Gain Phase vs Time",
        CalibrationPlotPreset::GainAmplitudeVsTime => "Gain Amplitude vs Time",
        CalibrationPlotPreset::BandpassAmplitudeVsFrequency => "Bandpass Amplitude vs Frequency",
        CalibrationPlotPreset::BandpassPhaseVsFrequency => "Bandpass Phase vs Frequency",
        _ => unreachable!("caller gated calibration-table presets"),
    };
    let summary_line = format!(
        "{} from {} with {} series and {} points.",
        preset.summary(),
        path.display(),
        series.len(),
        point_count
    );
    Ok(MsPlotPayload::Scatter(MsScatterPlotPayload {
        title: title.to_string(),
        x_axis,
        y_axis,
        secondary_y_axis: None,
        x_label: x_label.to_string(),
        y_label: y_label.to_string(),
        secondary_y_label: None,
        fixed_x_bounds: None,
        fixed_y_bounds: matches!(y_axis, MsAxis::Phase).then_some((-180.0, 180.0)),
        secondary_fixed_y_bounds: None,
        showlegend: series.len() > 1,
        legend_position: MsLegendPosition::ExteriorRight,
        showmajorgrid: true,
        showminorgrid: false,
        series,
        header_lines: vec![
            format!("Table: {}", path.display()),
            format!("Family: {}", summary.table_subtype),
        ],
        summary: summary_line,
    }))
}

fn build_gain_time_series(
    summary: &CalibrationTableSummary,
    table: &Table,
    preset: CalibrationPlotPreset,
) -> Result<Vec<MsScatterSeries>, CalibrationPlotError> {
    let path = summary.path.display().to_string();
    let mut points_by_series = std::collections::BTreeMap::<String, Vec<SeriesPoint>>::new();
    for row in 0..table.row_count() {
        let time = get_f64_scalar(table, row, COL_TIME, &path)?;
        let gains = get_complex_array(table, row, COL_CPARAM, &path)?;
        let flags = get_flag_mask(table, row, gains.len());
        let antenna = get_i32_scalar(table, row, "ANTENNA1", &path).unwrap_or_default();
        let spw = get_i32_scalar(table, row, "SPECTRAL_WINDOW_ID", &path).unwrap_or_default();
        let (receptor_count, channel_count) = complex_shape(&gains, &path)?;
        for receptor in 0..receptor_count {
            for channel in 0..channel_count {
                let flat = receptor * channel_count + channel;
                if flags.get(flat).copied().unwrap_or(false) {
                    continue;
                }
                let value = complex_at(&gains, receptor, channel)?;
                let y = transform_complex(value, preset);
                let label = format!("ant{antenna} spw{spw} corr{}", receptor + 1);
                points_by_series
                    .entry(label.clone())
                    .or_default()
                    .push(SeriesPoint {
                        x: time,
                        y,
                        provenance: MsScatterPointRef {
                            row,
                            corr: receptor,
                            chan_start: channel,
                            chan_end: channel + 1,
                        },
                    });
            }
        }
    }

    Ok(finalize_series(points_by_series, preset.scatter_axes().1))
}

fn build_bandpass_frequency_series(
    summary: &CalibrationTableSummary,
    table: &Table,
    preset: CalibrationPlotPreset,
) -> Result<Vec<MsScatterSeries>, CalibrationPlotError> {
    let path = summary.path.display().to_string();
    if !summary.table_subtype.contains('B') {
        return Err(CalibrationPlotError::UnsupportedCalibrationTable {
            path,
            preset: preset.display_name(),
            reason: "the selected table is not a B Jones / bandpass family".to_string(),
        });
    }
    let spw_table = open_summary_subtable(summary, "SPECTRAL_WINDOW")?;
    let mut points_by_series = std::collections::BTreeMap::<String, Vec<SeriesPoint>>::new();

    for row in 0..table.row_count() {
        let gains = get_complex_array(table, row, COL_CPARAM, &path)?;
        let flags = get_flag_mask(table, row, gains.len());
        let antenna = get_i32_scalar(table, row, "ANTENNA1", &path).unwrap_or_default();
        let spw = get_i32_scalar(table, row, "SPECTRAL_WINDOW_ID", &path).unwrap_or_default();
        let frequencies = spectral_window_frequencies(&spw_table, spw, &path)?;
        let (receptor_count, channel_count) = complex_shape(&gains, &path)?;
        if channel_count != frequencies.len() {
            return Err(CalibrationPlotError::InvalidCalibrationTable {
                path,
                reason: format!(
                    "CPARAM channel count {channel_count} does not match SPECTRAL_WINDOW CHAN_FREQ length {} for spw {spw}",
                    frequencies.len()
                ),
            });
        }
        for receptor in 0..receptor_count {
            for (channel, frequency_hz) in frequencies.iter().copied().enumerate() {
                let flat = receptor * channel_count + channel;
                if flags.get(flat).copied().unwrap_or(false) {
                    continue;
                }
                let value = complex_at(&gains, receptor, channel)?;
                let y = transform_complex(value, preset);
                let label = format!("ant{antenna} spw{spw} corr{}", receptor + 1);
                points_by_series
                    .entry(label.clone())
                    .or_default()
                    .push(SeriesPoint {
                        x: frequency_hz,
                        y,
                        provenance: MsScatterPointRef {
                            row,
                            corr: receptor,
                            chan_start: channel,
                            chan_end: channel + 1,
                        },
                    });
            }
        }
    }

    Ok(finalize_series(points_by_series, preset.scatter_axes().1))
}

#[derive(Debug, Clone)]
struct SeriesPoint {
    x: f64,
    y: f64,
    provenance: MsScatterPointRef,
}

fn finalize_series(
    grouped: std::collections::BTreeMap<String, Vec<SeriesPoint>>,
    y_axis: MsAxis,
) -> Vec<MsScatterSeries> {
    grouped
        .into_iter()
        .map(|(label, mut points)| {
            points.sort_by(|left, right| {
                left.x
                    .partial_cmp(&right.x)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| left.provenance.row.cmp(&right.provenance.row))
                    .then_with(|| left.provenance.corr.cmp(&right.provenance.corr))
                    .then_with(|| left.provenance.chan_start.cmp(&right.provenance.chan_start))
            });
            let scatter_points = points.iter().map(|point| (point.x, point.y)).collect();
            let provenance = points.into_iter().map(|point| point.provenance).collect();
            MsScatterSeries {
                label: label.clone(),
                color_group: label,
                y_axis,
                points: scatter_points,
                provenance,
            }
        })
        .collect()
}

fn transform_complex(value: Complex32, preset: CalibrationPlotPreset) -> f64 {
    match preset {
        CalibrationPlotPreset::GainAmplitudeVsTime
        | CalibrationPlotPreset::BandpassAmplitudeVsFrequency => {
            let re = f64::from(value.re);
            let im = f64::from(value.im);
            (re * re + im * im).sqrt()
        }
        CalibrationPlotPreset::GainPhaseVsTime
        | CalibrationPlotPreset::BandpassPhaseVsFrequency => {
            f64::from(value.im).atan2(f64::from(value.re)).to_degrees()
        }
        _ => unreachable!("caller gated calibration-table presets"),
    }
}

fn get_i32_scalar(
    table: &Table,
    row: usize,
    column: &str,
    path: &str,
) -> Result<i32, CalibrationPlotError> {
    match table.get_scalar_cell(row, column).map_err(|source| {
        CalibrationPlotError::OpenCalibrationTable {
            path: path.to_string(),
            source,
        }
    })? {
        ScalarValue::Int32(value) => Ok(*value),
        other => Err(CalibrationPlotError::InvalidCalibrationTable {
            path: path.to_string(),
            reason: format!("expected Int32 in {column} row {row}, found {other:?}"),
        }),
    }
}

fn get_f64_scalar(
    table: &Table,
    row: usize,
    column: &str,
    path: &str,
) -> Result<f64, CalibrationPlotError> {
    match table.get_scalar_cell(row, column).map_err(|source| {
        CalibrationPlotError::OpenCalibrationTable {
            path: path.to_string(),
            source,
        }
    })? {
        ScalarValue::Float64(value) => Ok(*value),
        other => Err(CalibrationPlotError::InvalidCalibrationTable {
            path: path.to_string(),
            reason: format!("expected Float64 in {column} row {row}, found {other:?}"),
        }),
    }
}

fn get_complex_array(
    table: &Table,
    row: usize,
    column: &str,
    path: &str,
) -> Result<ndarray::ArrayD<Complex32>, CalibrationPlotError> {
    match table.get_array_cell(row, column).map_err(|source| {
        CalibrationPlotError::OpenCalibrationTable {
            path: path.to_string(),
            source,
        }
    })? {
        ArrayValue::Complex32(values) => Ok(values.clone()),
        other => Err(CalibrationPlotError::InvalidCalibrationTable {
            path: path.to_string(),
            reason: format!("expected complex array in {column} row {row}, found {other:?}"),
        }),
    }
}

fn get_flag_mask(table: &Table, row: usize, expected_len: usize) -> Vec<bool> {
    match table.get_array_cell(row, COL_FLAG) {
        Ok(ArrayValue::Bool(values)) => values.iter().copied().collect(),
        _ => vec![false; expected_len],
    }
}

fn complex_shape(
    gains: &ndarray::ArrayD<Complex32>,
    path: &str,
) -> Result<(usize, usize), CalibrationPlotError> {
    match gains.ndim() {
        1 => Ok((gains.len(), 1)),
        2 => Ok((gains.shape()[0], gains.shape()[1])),
        _ => Err(CalibrationPlotError::InvalidCalibrationTable {
            path: path.to_string(),
            reason: format!("unsupported CPARAM shape {:?}", gains.shape()),
        }),
    }
}

fn complex_at(
    gains: &ndarray::ArrayD<Complex32>,
    receptor: usize,
    channel: usize,
) -> Result<Complex32, CalibrationPlotError> {
    match gains.ndim() {
        1 => Ok(gains[[receptor]]),
        2 => Ok(gains[[receptor, channel]]),
        _ => unreachable!("shape validated before indexing"),
    }
}

fn spectral_window_frequencies(
    spw_table: &Table,
    spw_id: i32,
    path: &str,
) -> Result<Vec<f64>, CalibrationPlotError> {
    let row =
        usize::try_from(spw_id).map_err(|_| CalibrationPlotError::InvalidCalibrationTable {
            path: path.to_string(),
            reason: format!("invalid spectral-window id {spw_id}"),
        })?;
    if row >= spw_table.row_count() {
        return Err(CalibrationPlotError::InvalidCalibrationTable {
            path: path.to_string(),
            reason: format!("missing SPECTRAL_WINDOW row for id {spw_id}"),
        });
    }
    match spw_table
        .get_array_cell(row, "CHAN_FREQ")
        .map_err(|source| CalibrationPlotError::OpenCalibrationSubtable {
            path: path.to_string(),
            subtable: "SPECTRAL_WINDOW",
            source,
        })? {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        other => Err(CalibrationPlotError::InvalidCalibrationTable {
            path: path.to_string(),
            reason: format!(
                "expected Float64 CHAN_FREQ array for SPECTRAL_WINDOW row {row}, found {other:?}"
            ),
        }),
    }
}

fn open_summary_subtable(
    summary: &CalibrationTableSummary,
    name: &'static str,
) -> Result<Table, CalibrationPlotError> {
    let subtable = summary
        .subtables
        .iter()
        .find(|subtable| subtable.name == name)
        .ok_or_else(|| CalibrationPlotError::InvalidCalibrationTable {
            path: summary.path.display().to_string(),
            reason: format!("summary did not record the {name} subtable"),
        })?;
    let path = subtable.resolved_path.as_ref().ok_or_else(|| {
        CalibrationPlotError::InvalidCalibrationTable {
            path: summary.path.display().to_string(),
            reason: format!("{name} subtable link is missing"),
        }
    })?;
    Table::open(TableOptions::new(path)).map_err(|source| {
        CalibrationPlotError::OpenCalibrationSubtable {
            path: summary.path.display().to_string(),
            subtable: name,
            source,
        }
    })
}
