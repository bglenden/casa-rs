// SPDX-License-Identifier: LGPL-3.0-or-later
//! Statistical summaries over calibration tables.
//!
//! The first `calstat`-class surface stays library-first and focuses on
//! machine-readable statistics over calibration-table values. It deliberately
//! avoids selection or logging concerns and instead returns a stable report
//! shape that can back tests and future CLI/TUI surfaces.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casacore_tables::{Table, TableError, TableOptions};
use casacore_types::{ArrayValue, ScalarValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::constants::{
    COL_ANTENNA1, COL_FIELD_ID, COL_FLAG, COL_OBSERVATION_ID, COL_SPECTRAL_WINDOW_ID,
};

/// Axis transform used when computing calibration statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CalibrationStatsAxis {
    /// Magnitude of complex values.
    Amplitude,
    /// Phase of complex values in radians in `[-pi, pi]`.
    Phase,
    /// Real component of complex values.
    Real,
    /// Imaginary component of complex values.
    Imaginary,
    /// Use the named real-valued column directly.
    Column(String),
}

impl CalibrationStatsAxis {
    /// Parse a CASA-style axis token.
    pub fn parse(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "amp" | "amplitude" => Self::Amplitude,
            "phase" => Self::Phase,
            "real" => Self::Real,
            "imag" | "imaginary" => Self::Imaginary,
            other => Self::Column(other.to_ascii_uppercase()),
        }
    }

    pub fn display_name(&self) -> String {
        match self {
            Self::Amplitude => "amplitude".to_string(),
            Self::Phase => "phase".to_string(),
            Self::Real => "real".to_string(),
            Self::Imaginary => "imaginary".to_string(),
            Self::Column(column) => column.clone(),
        }
    }

    fn requires_complex_datacolumn(&self) -> bool {
        matches!(
            self,
            Self::Amplitude | Self::Phase | Self::Real | Self::Imaginary
        )
    }
}

/// Request for `calstat`-class statistics over a calibration table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationStatsRequest {
    /// Axis to compute.
    pub axis: CalibrationStatsAxis,
    /// Complex data column used when the axis transforms complex values.
    ///
    /// CASA uses `datacolumn='gain'` as the task default. This implementation
    /// normalizes the aliases `gain` -> `CPARAM` and `float` -> `FPARAM`.
    pub datacolumn: Option<String>,
    /// Whether flagged values should be excluded from the statistics.
    ///
    /// CASA documents `useflags` but historically does not implement it.
    /// This library surface does implement it. Keep `false` to match the broad
    /// CASA default behavior for parity checks.
    pub use_flags: bool,
}

impl Default for CalibrationStatsRequest {
    fn default() -> Self {
        Self {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: None,
            use_flags: false,
        }
    }
}

/// Descriptive statistics over one numeric value stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationValueStats {
    /// Number of numeric values included in the statistics.
    pub npts: u64,
    /// Number of flagged values encountered in the scanned cells.
    pub flagged_npts: u64,
    /// Total values scanned before flag filtering.
    pub total_npts: u64,
    /// Sum of the values.
    pub sum: f64,
    /// Sum of squared values.
    pub sumsq: f64,
    /// Minimum value.
    pub min: f64,
    /// Maximum value.
    pub max: f64,
    /// Arithmetic mean.
    pub mean: f64,
    /// Median.
    pub median: f64,
    /// Median absolute deviation from the median.
    pub medabsdevmed: f64,
    /// First quartile.
    pub q1: f64,
    /// Third quartile.
    pub q3: f64,
    /// Inter-quartile range.
    pub quartile: f64,
    /// Population variance.
    pub var: f64,
    /// Population standard deviation.
    pub stddev: f64,
    /// Root mean square.
    pub rms: f64,
}

/// Statistics for one integer-identified group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationIndexedStats {
    /// Group key, for example `FIELD_ID`.
    pub key: i32,
    /// Statistics for values in the group.
    pub stats: CalibrationValueStats,
}

/// Machine-readable `calstat`-class report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrationStatsReport {
    /// Table root path that was opened.
    pub path: PathBuf,
    /// Effective axis name.
    pub axis: CalibrationStatsAxis,
    /// Effective data column name, if any.
    pub datacolumn: Option<String>,
    /// Total MAIN row count.
    pub row_count: usize,
    /// Statistics over all included values.
    pub global: CalibrationValueStats,
    /// Statistics grouped by `FIELD_ID`.
    pub by_field_id: Vec<CalibrationIndexedStats>,
    /// Statistics grouped by `SPECTRAL_WINDOW_ID`.
    pub by_spectral_window_id: Vec<CalibrationIndexedStats>,
    /// Statistics grouped by `ANTENNA1`.
    pub by_antenna1_id: Vec<CalibrationIndexedStats>,
    /// Statistics grouped by `OBSERVATION_ID`.
    pub by_observation_id: Vec<CalibrationIndexedStats>,
}

/// Errors returned while computing calibration statistics.
#[derive(Debug, Error)]
pub enum CalibrationStatsError {
    /// The table could not be opened.
    #[error("failed to open calibration table {path}: {source}")]
    Open {
        /// Path that was being opened.
        path: String,
        /// Underlying table error.
        #[source]
        source: TableError,
    },

    /// The requested datacolumn is required but missing.
    #[error("missing required data column {column} in calibration table {path}")]
    MissingColumn {
        /// Opened table path.
        path: String,
        /// Missing column name.
        column: String,
    },

    /// The selected axis/column combination is unsupported.
    #[error("unsupported statistics axis {axis} for column {column} in {path}: {reason}")]
    UnsupportedAxis {
        /// Opened table path.
        path: String,
        /// Axis description.
        axis: String,
        /// Selected column.
        column: String,
        /// Additional detail.
        reason: String,
    },

    /// No numeric values were available after filtering.
    #[error("no values available for statistics on {path} using axis {axis}")]
    EmptyValues {
        /// Opened table path.
        path: String,
        /// Axis description.
        axis: String,
    },
}

/// Compute `calstat`-class statistics for one calibration table.
pub fn calibration_stats(
    path: impl AsRef<Path>,
    request: &CalibrationStatsRequest,
) -> Result<CalibrationStatsReport, CalibrationStatsError> {
    let path = path.as_ref().to_path_buf();
    let table =
        Table::open(TableOptions::new(&path)).map_err(|source| CalibrationStatsError::Open {
            path: path.display().to_string(),
            source,
        })?;
    build_stats_report(&path, &table, request)
}

fn build_stats_report(
    path: &Path,
    table: &Table,
    request: &CalibrationStatsRequest,
) -> Result<CalibrationStatsReport, CalibrationStatsError> {
    let datacolumn = resolve_datacolumn(table, request, path)?;
    let mut global = StatsAccumulator::default();
    let mut by_field = BTreeMap::<i32, StatsAccumulator>::new();
    let mut by_spw = BTreeMap::<i32, StatsAccumulator>::new();
    let mut by_ant = BTreeMap::<i32, StatsAccumulator>::new();
    let mut by_obs = BTreeMap::<i32, StatsAccumulator>::new();

    for row in 0..table.row_count() {
        let field_id = get_i32(table, row, COL_FIELD_ID).unwrap_or(0);
        let spw_id = get_i32(table, row, COL_SPECTRAL_WINDOW_ID).unwrap_or(0);
        let ant_id = get_i32(table, row, COL_ANTENNA1).unwrap_or(0);
        let obs_id = get_i32(table, row, COL_OBSERVATION_ID).unwrap_or(0);
        let flagged = load_flag_mask(table, row);
        let values = extract_row_values(
            table,
            row,
            request,
            datacolumn.as_deref(),
            flagged.as_ref(),
            path,
        )?;

        for sample in values {
            global.observe(sample.value, sample.flagged, request.use_flags);
            by_field.entry(field_id).or_default().observe(
                sample.value,
                sample.flagged,
                request.use_flags,
            );
            by_spw.entry(spw_id).or_default().observe(
                sample.value,
                sample.flagged,
                request.use_flags,
            );
            by_ant.entry(ant_id).or_default().observe(
                sample.value,
                sample.flagged,
                request.use_flags,
            );
            by_obs.entry(obs_id).or_default().observe(
                sample.value,
                sample.flagged,
                request.use_flags,
            );
        }
    }

    let axis_name = request.axis.display_name();
    Ok(CalibrationStatsReport {
        path: path.to_path_buf(),
        axis: request.axis.clone(),
        datacolumn,
        row_count: table.row_count(),
        global: global.finish(path, &axis_name)?,
        by_field_id: finish_groups(path, &axis_name, by_field)?,
        by_spectral_window_id: finish_groups(path, &axis_name, by_spw)?,
        by_antenna1_id: finish_groups(path, &axis_name, by_ant)?,
        by_observation_id: finish_groups(path, &axis_name, by_obs)?,
    })
}

fn resolve_datacolumn(
    table: &Table,
    request: &CalibrationStatsRequest,
    path: &Path,
) -> Result<Option<String>, CalibrationStatsError> {
    if !request.axis.requires_complex_datacolumn() {
        return Ok(None);
    }

    let raw = request
        .datacolumn
        .as_deref()
        .unwrap_or("gain")
        .to_ascii_uppercase();
    let normalized = match raw.as_str() {
        "GAIN" => "CPARAM".to_string(),
        "FLOAT" => "FPARAM".to_string(),
        other => other.to_string(),
    };
    let has_column = table
        .schema()
        .is_some_and(|schema| schema.contains_column(&normalized));
    if !has_column {
        return Err(CalibrationStatsError::MissingColumn {
            path: path.display().to_string(),
            column: normalized,
        });
    }
    Ok(Some(normalized))
}

fn load_flag_mask(table: &Table, row: usize) -> Option<Vec<bool>> {
    match table.get_array_cell(row, COL_FLAG).ok()? {
        ArrayValue::Bool(values) => Some(values.iter().copied().collect()),
        _ => None,
    }
}

struct SampleValue {
    value: f64,
    flagged: bool,
}

fn extract_row_values(
    table: &Table,
    row: usize,
    request: &CalibrationStatsRequest,
    datacolumn: Option<&str>,
    flags: Option<&Vec<bool>>,
    path: &Path,
) -> Result<Vec<SampleValue>, CalibrationStatsError> {
    match &request.axis {
        CalibrationStatsAxis::Amplitude
        | CalibrationStatsAxis::Phase
        | CalibrationStatsAxis::Real
        | CalibrationStatsAxis::Imaginary => {
            let column = datacolumn.expect("complex stats require datacolumn");
            let array = table.get_array_cell(row, column).map_err(|source| {
                CalibrationStatsError::Open {
                    path: path.display().to_string(),
                    source,
                }
            })?;
            let ArrayValue::Complex32(values) = array else {
                return Err(CalibrationStatsError::UnsupportedAxis {
                    path: path.display().to_string(),
                    axis: request.axis.display_name(),
                    column: column.to_string(),
                    reason: "selected datacolumn is not complex-valued".to_string(),
                });
            };
            Ok(values
                .iter()
                .enumerate()
                .map(|(index, value)| SampleValue {
                    value: match request.axis {
                        CalibrationStatsAxis::Amplitude => f64::from(value.norm()),
                        CalibrationStatsAxis::Phase => {
                            f64::from(value.im).atan2(f64::from(value.re))
                        }
                        CalibrationStatsAxis::Real => f64::from(value.re),
                        CalibrationStatsAxis::Imaginary => f64::from(value.im),
                        CalibrationStatsAxis::Column(_) => unreachable!(),
                    },
                    flagged: flags
                        .and_then(|flags| flags.get(index))
                        .copied()
                        .unwrap_or(false),
                })
                .collect())
        }
        CalibrationStatsAxis::Column(column) => {
            if let Ok(array) = table.get_array_cell(row, column) {
                flatten_real_array(array, flags, request, path, column)
            } else {
                let scalar = table.get_scalar_cell(row, column).map_err(|source| {
                    CalibrationStatsError::Open {
                        path: path.display().to_string(),
                        source,
                    }
                })?;
                scalar_to_sample(column, scalar, path).map(|sample| {
                    vec![SampleValue {
                        value: sample,
                        flagged: false,
                    }]
                })
            }
        }
    }
}

fn flatten_real_array(
    array: &ArrayValue,
    flags: Option<&Vec<bool>>,
    request: &CalibrationStatsRequest,
    path: &Path,
    column: &str,
) -> Result<Vec<SampleValue>, CalibrationStatsError> {
    let values = match array {
        ArrayValue::Bool(values) => values
            .iter()
            .map(|value| if *value { 1.0 } else { 0.0 })
            .collect::<Vec<_>>(),
        ArrayValue::Int32(values) => values
            .iter()
            .map(|value| f64::from(*value))
            .collect::<Vec<_>>(),
        ArrayValue::Int64(values) => values.iter().map(|value| *value as f64).collect::<Vec<_>>(),
        ArrayValue::Float32(values) => values
            .iter()
            .map(|value| f64::from(*value))
            .collect::<Vec<_>>(),
        ArrayValue::Float64(values) => values.iter().copied().collect::<Vec<_>>(),
        other => {
            return Err(CalibrationStatsError::UnsupportedAxis {
                path: path.display().to_string(),
                axis: request.axis.display_name(),
                column: column.to_string(),
                reason: format!("column is not real-valued: {:?}", other.primitive_type()),
            });
        }
    };
    Ok(values
        .into_iter()
        .enumerate()
        .map(|(index, value)| SampleValue {
            value,
            flagged: flags
                .and_then(|flags| flags.get(index))
                .copied()
                .unwrap_or(false),
        })
        .collect())
}

fn scalar_to_sample(
    column: &str,
    scalar: &ScalarValue,
    path: &Path,
) -> Result<f64, CalibrationStatsError> {
    match scalar {
        ScalarValue::Bool(value) => Ok(if *value { 1.0 } else { 0.0 }),
        ScalarValue::Int32(value) => Ok(f64::from(*value)),
        ScalarValue::Int64(value) => Ok(*value as f64),
        ScalarValue::Float32(value) => Ok(f64::from(*value)),
        ScalarValue::Float64(value) => Ok(*value),
        _ => Err(CalibrationStatsError::UnsupportedAxis {
            path: path.display().to_string(),
            axis: column.to_string(),
            column: column.to_string(),
            reason: "column is not real-valued".to_string(),
        }),
    }
}

fn get_i32(table: &Table, row: usize, column: &str) -> Result<i32, CalibrationStatsError> {
    let scalar =
        table
            .get_scalar_cell(row, column)
            .map_err(|source| CalibrationStatsError::Open {
                path: column.to_string(),
                source,
            })?;
    match scalar {
        ScalarValue::Int32(value) => Ok(*value),
        ScalarValue::Int64(value) => {
            i32::try_from(*value).map_err(|_| CalibrationStatsError::UnsupportedAxis {
                path: column.to_string(),
                axis: column.to_string(),
                column: column.to_string(),
                reason: "integer value does not fit in i32".to_string(),
            })
        }
        _ => Err(CalibrationStatsError::UnsupportedAxis {
            path: column.to_string(),
            axis: column.to_string(),
            column: column.to_string(),
            reason: "column is not integer-valued".to_string(),
        }),
    }
}

fn finish_groups(
    path: &Path,
    axis_name: &str,
    groups: BTreeMap<i32, StatsAccumulator>,
) -> Result<Vec<CalibrationIndexedStats>, CalibrationStatsError> {
    groups
        .into_iter()
        .map(|(key, accumulator)| {
            Ok(CalibrationIndexedStats {
                key,
                stats: accumulator.finish(path, axis_name)?,
            })
        })
        .collect()
}

#[derive(Default)]
struct StatsAccumulator {
    values: Vec<f64>,
    total_npts: u64,
    flagged_npts: u64,
}

impl StatsAccumulator {
    fn observe(&mut self, value: f64, flagged: bool, use_flags: bool) {
        self.total_npts += 1;
        if flagged {
            self.flagged_npts += 1;
            if use_flags {
                return;
            }
        }
        self.values.push(value);
    }

    fn finish(
        mut self,
        path: &Path,
        axis_name: &str,
    ) -> Result<CalibrationValueStats, CalibrationStatsError> {
        if self.values.is_empty() {
            return Err(CalibrationStatsError::EmptyValues {
                path: path.display().to_string(),
                axis: axis_name.to_string(),
            });
        }

        self.values.sort_by(|left, right| left.total_cmp(right));
        let n = self.values.len() as f64;
        let sum = self.values.iter().sum::<f64>();
        let sumsq = self.values.iter().map(|value| value * value).sum::<f64>();
        let min = *self.values.first().expect("non-empty values");
        let max = *self.values.last().expect("non-empty values");
        let mean = sum / n;
        let median = median_of_sorted(&self.values);
        let q1 = median_of_sorted(&self.values[..self.values.len() / 2]);
        let upper_start = self.values.len().div_ceil(2);
        let q3 = median_of_sorted(&self.values[upper_start..]);
        let quartile = q3 - q1;
        let medabsdevmed = {
            let mut deviations = self
                .values
                .iter()
                .map(|value| (value - median).abs())
                .collect::<Vec<_>>();
            deviations.sort_by(|left, right| left.total_cmp(right));
            median_of_sorted(&deviations)
        };
        let var = self
            .values
            .iter()
            .map(|value| {
                let centered = value - mean;
                centered * centered
            })
            .sum::<f64>()
            / n;
        let stddev = var.sqrt();
        let rms = (sumsq / n).sqrt();

        Ok(CalibrationValueStats {
            npts: self.values.len() as u64,
            flagged_npts: self.flagged_npts,
            total_npts: self.total_npts,
            sum,
            sumsq,
            min,
            max,
            mean,
            median,
            medabsdevmed,
            q1,
            q3,
            quartile,
            var,
            stddev,
            rms,
        })
    }
}

fn median_of_sorted(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}
