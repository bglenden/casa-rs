// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded MeasurementSet metadata and range probes.

use std::collections::{BTreeMap, BTreeSet};

use casa_types::{ArrayValue, ScalarValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{MeasurementSet, MsError, MsReadPlan, MsResult, MsSelectionIoBudget};

const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

/// One named MeasurementSet subtable row exposed by the context probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetNamedContext {
    /// Zero-based subtable row.
    pub row: usize,
    /// Persisted name.
    pub name: String,
}

/// One spectral-window row exposed by the context probe.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetSpectralWindowContext {
    /// Zero-based SPECTRAL_WINDOW row.
    pub row: usize,
    /// Number of channels in the row.
    pub channel_count: usize,
    /// Mean channel frequency, or REF_FREQUENCY for a row without channels.
    pub center_frequency_hz: f64,
}

/// One observation row exposed by the context probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetObservationContext {
    /// Zero-based OBSERVATION row.
    pub row: usize,
    /// Persisted project identifier.
    pub project: String,
    /// Persisted telescope name.
    pub telescope_name: String,
}

/// One known MeasurementSet subtable and its schema role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetSubtableContext {
    /// Canonical subtable keyword/name.
    pub name: String,
    /// Whether the MeasurementSet schema requires this subtable.
    pub required: bool,
}

/// Bounded metadata context shared by frontend projections and selector edits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetContext {
    /// MAIN-table row count.
    pub row_count: usize,
    /// FIELD rows.
    pub fields: Vec<MeasurementSetNamedContext>,
    /// SPECTRAL_WINDOW rows.
    pub spectral_windows: Vec<MeasurementSetSpectralWindowContext>,
    /// ANTENNA rows.
    pub antennas: Vec<MeasurementSetNamedContext>,
    /// OBSERVATION rows.
    pub observations: Vec<MeasurementSetObservationContext>,
    /// Unique STATE intent tokens in deterministic order.
    pub intents: Vec<String>,
    /// Unique non-negative FEED_ID values in deterministic order.
    pub feed_ids: Vec<i32>,
    /// Unique correlation/Stokes names in deterministic order.
    pub correlations: Vec<String>,
    /// MAIN-table column names in schema order.
    pub columns: Vec<String>,
    /// Present standard visibility-data columns in canonical order.
    pub data_columns: Vec<String>,
    /// Present standard subtables in canonical order.
    pub subtables: Vec<MeasurementSetSubtableContext>,
}

/// Finite UV-distance bounds observed in the MAIN table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetUvRange {
    /// Minimum projected UV distance in meters.
    pub min_meters: f64,
    /// Maximum projected UV distance in meters.
    pub max_meters: f64,
    /// Minimum projected UV distance in kilolambda.
    pub min_kilolambda: f64,
    /// Maximum projected UV distance in kilolambda.
    pub max_kilolambda: f64,
    /// Number of finite rows included in the probe.
    pub row_count: u64,
}

/// Finite MAIN-table time bounds in MJD seconds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetTimeRange {
    /// Minimum time in MJD seconds.
    pub min_seconds: f64,
    /// Maximum time in MJD seconds.
    pub max_seconds: f64,
    /// Number of finite rows included in the probe.
    pub row_count: u64,
}

impl MeasurementSet {
    /// Read the bounded metadata context used by dataset probes and selector editors.
    pub fn probe_context(&self) -> MsResult<MeasurementSetContext> {
        let fields = self.field()?;
        let fields = (0..fields.row_count())
            .map(|row| {
                Ok(MeasurementSetNamedContext {
                    row,
                    name: fields.name(row)?,
                })
            })
            .collect::<MsResult<Vec<_>>>()?;

        let spectral_windows = self.spectral_window()?;
        let spectral_windows = (0..spectral_windows.row_count())
            .map(|row| {
                let frequencies = spectral_windows.chan_freq(row)?;
                let center_frequency_hz = if frequencies.is_empty() {
                    spectral_windows.ref_frequency(row)?
                } else {
                    frequencies.iter().sum::<f64>() / frequencies.len() as f64
                };
                Ok(MeasurementSetSpectralWindowContext {
                    row,
                    channel_count: usize::try_from(spectral_windows.num_chan(row)?).map_err(
                        |_| {
                            MsError::InvalidInput(format!(
                                "SPECTRAL_WINDOW row {row} has a negative channel count"
                            ))
                        },
                    )?,
                    center_frequency_hz,
                })
            })
            .collect::<MsResult<Vec<_>>>()?;

        let antennas = self.antenna()?;
        let antennas = (0..antennas.row_count())
            .map(|row| {
                Ok(MeasurementSetNamedContext {
                    row,
                    name: antennas.name(row)?,
                })
            })
            .collect::<MsResult<Vec<_>>>()?;

        let observations = self.observation()?;
        let observations = (0..observations.row_count())
            .map(|row| {
                Ok(MeasurementSetObservationContext {
                    row,
                    project: observations.string(row, "PROJECT")?.trim().to_string(),
                    telescope_name: observations
                        .string(row, "TELESCOPE_NAME")?
                        .trim()
                        .to_string(),
                })
            })
            .collect::<MsResult<Vec<_>>>()?;

        let state = self.state()?;
        let mut intents = BTreeSet::new();
        for row in 0..state.row_count() {
            for intent in state
                .string(row, "OBS_MODE")?
                .split(',')
                .map(str::trim)
                .filter(|intent| !intent.is_empty())
            {
                intents.insert(intent.to_string());
            }
        }

        let feed = self.feed()?;
        let mut feed_ids = BTreeSet::new();
        for row in 0..feed.row_count() {
            let id = feed.i32(row, "FEED_ID")?;
            if id >= 0 {
                feed_ids.insert(id);
            }
        }

        let polarization = self.polarization()?;
        let mut correlations = BTreeSet::new();
        for row in 0..polarization.row_count() {
            correlations.extend(
                polarization
                    .corr_type(row)?
                    .into_iter()
                    .map(crate::listobs::stokes_name)
                    .map(str::to_string),
            );
        }

        let columns = self
            .main_table()
            .schema()
            .map(|schema| {
                schema
                    .columns()
                    .iter()
                    .map(|column| column.name().to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let data_columns = crate::schema::main_table::VisibilityDataColumn::ALL
            .iter()
            .map(|column| column.name())
            .filter(|name| columns.iter().any(|column| column == *name))
            .map(str::to_string)
            .collect();
        let mut subtable_ids = self.subtable_ids();
        subtable_ids.sort();
        let subtables = subtable_ids
            .into_iter()
            .map(|id| MeasurementSetSubtableContext {
                name: id.name().to_string(),
                required: id.is_required(),
            })
            .collect();

        Ok(MeasurementSetContext {
            row_count: self.row_count(),
            fields,
            spectral_windows,
            antennas,
            observations,
            intents: intents.into_iter().collect(),
            feed_ids: feed_ids.into_iter().collect(),
            correlations: correlations.into_iter().collect(),
            columns,
            data_columns,
            subtables,
        })
    }

    /// Probe projected UV-distance bounds with a workload-derived bounded scan.
    pub fn probe_uv_range(&self) -> MsResult<MeasurementSetUvRange> {
        let table = self.main_table();
        if table.row_count() == 0 {
            return Err(MsError::InvalidInput(
                "MeasurementSet has no MAIN rows".to_string(),
            ));
        }
        let data_description = self.data_description()?;
        let mut ddid_to_spw = BTreeMap::new();
        for row in 0..data_description.row_count() {
            ddid_to_spw.insert(row as i32, data_description.spectral_window_id(row)?);
        }
        let spectral_windows = self.spectral_window()?;
        let mut spw_center_frequency_hz = BTreeMap::new();
        for row in 0..spectral_windows.row_count() {
            let frequencies = spectral_windows.chan_freq(row)?;
            let center_hz = if frequencies.is_empty() {
                spectral_windows.ref_frequency(row)?
            } else {
                frequencies.iter().sum::<f64>() / frequencies.len() as f64
            };
            spw_center_frequency_hz.insert(row as i32, center_hz);
        }
        let plan = system_read_plan(table.row_count(), 3 * 8 + 4)?;
        let mut min_meters = f64::INFINITY;
        let mut max_meters = f64::NEG_INFINITY;
        let mut min_kilolambda = f64::INFINITY;
        let mut max_kilolambda = f64::NEG_INFINITY;
        let mut seen_rows = 0u64;
        for start in (0..table.row_count()).step_by(plan.rows_per_block) {
            let end = start
                .saturating_add(plan.rows_per_block)
                .min(table.row_count());
            let rows = (start..end).collect::<Vec<_>>();
            let uvw_values = selected_uvw_values(self, &rows)?;
            let ddids = selected_i32_values(self, "DATA_DESC_ID", &rows)?;
            for (uvw, ddid) in uvw_values.into_iter().zip(ddids) {
                let uv_meters = uvw[0].hypot(uvw[1]);
                if !uv_meters.is_finite() {
                    continue;
                }
                min_meters = min_meters.min(uv_meters);
                max_meters = max_meters.max(uv_meters);
                if let Some(frequency_hz) = ddid_to_spw
                    .get(&ddid)
                    .and_then(|spw| spw_center_frequency_hz.get(spw))
                    .copied()
                    .filter(|frequency| *frequency > 0.0)
                {
                    let kilolambda = uv_meters * frequency_hz / SPEED_OF_LIGHT_M_S / 1_000.0;
                    min_kilolambda = min_kilolambda.min(kilolambda);
                    max_kilolambda = max_kilolambda.max(kilolambda);
                }
                seen_rows += 1;
            }
        }
        if seen_rows == 0 || !min_meters.is_finite() || !max_meters.is_finite() {
            return Err(MsError::InvalidInput(
                "MeasurementSet UVW column did not contain finite UV distances".to_string(),
            ));
        }
        if !min_kilolambda.is_finite() || !max_kilolambda.is_finite() {
            min_kilolambda = 0.0;
            max_kilolambda = 0.0;
        }
        Ok(MeasurementSetUvRange {
            min_meters,
            max_meters,
            min_kilolambda,
            max_kilolambda,
            row_count: seen_rows,
        })
    }

    /// Probe finite MAIN-table time bounds with a workload-derived bounded scan.
    pub fn probe_time_range(&self) -> MsResult<MeasurementSetTimeRange> {
        let table = self.main_table();
        if table.row_count() == 0 {
            return Err(MsError::InvalidInput(
                "MeasurementSet has no MAIN rows".to_string(),
            ));
        }
        let plan = system_read_plan(table.row_count(), 8)?;
        let mut min_seconds = f64::INFINITY;
        let mut max_seconds = f64::NEG_INFINITY;
        let mut seen_rows = 0u64;
        for start in (0..table.row_count()).step_by(plan.rows_per_block) {
            let end = start
                .saturating_add(plan.rows_per_block)
                .min(table.row_count());
            let rows = (start..end).collect::<Vec<_>>();
            for seconds in selected_f64_values(self, "TIME", &rows)? {
                if seconds.is_finite() {
                    min_seconds = min_seconds.min(seconds);
                    max_seconds = max_seconds.max(seconds);
                    seen_rows += 1;
                }
            }
        }
        if seen_rows == 0 || !min_seconds.is_finite() || !max_seconds.is_finite() {
            return Err(MsError::InvalidInput(
                "MeasurementSet TIME column did not contain finite seconds".to_string(),
            ));
        }
        Ok(MeasurementSetTimeRange {
            min_seconds,
            max_seconds,
            row_count: seen_rows,
        })
    }
}

fn system_read_plan(row_count: usize, requested_bytes_per_row: usize) -> MsResult<MsReadPlan> {
    let budget = MsSelectionIoBudget::from_system_memory(1, requested_bytes_per_row, None)
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    MsReadPlan::new(row_count, budget).map_err(|error| MsError::InvalidInput(error.to_string()))
}

fn selected_i32_values(
    ms: &MeasurementSet,
    column: &'static str,
    rows: &[usize],
) -> MsResult<Vec<i32>> {
    ms.main_table()
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(ScalarValue::Int32(value)) => Ok(value),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Int32".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

fn selected_f64_values(
    ms: &MeasurementSet,
    column: &'static str,
    rows: &[usize],
) -> MsResult<Vec<f64>> {
    ms.main_table()
        .column_accessor(column)?
        .scalar_cells_owned_for_rows(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(ScalarValue::Float64(value)) => Ok(value),
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: column.to_string(),
                table: "MAIN".to_string(),
                expected: "Float64".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("{column}[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}

fn selected_uvw_values(ms: &MeasurementSet, rows: &[usize]) -> MsResult<Vec<[f64; 3]>> {
    ms.main_table()
        .column_accessor("UVW")?
        .array_cells_owned(rows)?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| match value {
            Some(ArrayValue::Float64(values)) => {
                let slice = values.as_slice().ok_or_else(|| {
                    MsError::InvalidInput("UVW cells must be contiguous f64 arrays".to_string())
                })?;
                match slice {
                    [u, v, w] => Ok([*u, *v, *w]),
                    _ => Err(MsError::InvalidInput(format!(
                        "UVW row {row} must have three values, found {}",
                        slice.len()
                    ))),
                }
            }
            Some(other) => Err(MsError::ColumnTypeMismatch {
                column: "UVW".to_string(),
                table: "MAIN".to_string(),
                expected: "Float64[3]".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
            None => Err(MsError::MissingColumn {
                column: format!("UVW[row={row}]"),
                table: "MAIN".to_string(),
            }),
        })
        .collect()
}
