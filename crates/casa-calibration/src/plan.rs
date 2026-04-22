// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apply-planning support for the implemented CASA calibration-table families.
//!
//! The planner resolves the static parts of an `applycal`-class operation
//! without mutating an MS:
//!
//! - selected MS rows and their DATA_DESC/SPW metadata
//! - whether `CORRECTED_DATA` must be created
//! - per-caltable `gainfield`, `spwmap`, and interpolation choices
//! - spectral-window grids needed by the future executor
//!
//! This separation keeps trial mode, parity diagnostics, and multithreaded
//! execution deterministic.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::MsSelection;
use casa_ms::{MsError, MsSpectralWindow};
use casa_tables::{Table, TableError, TableOptions};
use casa_types::{ArrayValue, ScalarValue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::model::CalibrationTableSummary;
use crate::summary::{CalibrationTableError, summarize_table};

/// Calibration-application mode planned for the executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ApplyMode {
    /// Apply calibrations and propagate flags.
    CalFlag,
    /// Apply calibrations without changing data flags.
    CalOnly,
    /// Build and report the plan without mutating the MeasurementSet.
    Trial,
}

/// Supported first-wave interpolation modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ApplyInterpolationMode {
    /// Nearest-neighbor lookup.
    Nearest,
    /// Linear interpolation on the primary solve axis.
    Linear,
    /// Nearest in time, linear in frequency.
    NearestLinear,
}

impl ApplyInterpolationMode {
    /// Returns `true` when the mode depends on frequency-grid metadata.
    pub fn uses_frequency_axis(self) -> bool {
        matches!(self, Self::NearestLinear)
    }
}

/// Explicit `gainfield` selector accepted by the planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainFieldSelector {
    /// Resolve a caltable field by exact integer FIELD_ID.
    FieldId(i32),
    /// Resolve a caltable field by exact FIELD.NAME match.
    FieldName(String),
    /// Resolve each selected MS field to the nearest caltable field on sky.
    Nearest,
}

/// Optional per-table applicability selection, used by callibrary-style apply
/// entries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyTableSelection {
    /// Restrict application to these MS FIELD_ID values.
    pub field_ids: Vec<i32>,
    /// Restrict application to these MS data spectral-window ids.
    pub spectral_window_ids: Vec<i32>,
    /// Restrict application to these MS OBSERVATION_ID values.
    pub observation_ids: Vec<i32>,
}

impl ApplyTableSelection {
    /// Returns `true` when the selection carries no constraints.
    pub fn is_empty(&self) -> bool {
        self.field_ids.is_empty()
            && self.spectral_window_ids.is_empty()
            && self.observation_ids.is_empty()
    }

    /// Returns `true` when the selection applies to the supplied planned row.
    pub fn matches(&self, row: &ApplyRowPlan) -> bool {
        if !self.field_ids.is_empty() && !self.field_ids.contains(&row.field_id) {
            return false;
        }
        if !self.spectral_window_ids.is_empty()
            && !self.spectral_window_ids.contains(&row.data_spw_id)
        {
            return false;
        }
        if !self.observation_ids.is_empty() && !self.observation_ids.contains(&row.observation_id) {
            return false;
        }
        true
    }
}

/// One input calibration table in an apply plan request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyCalibrationTableSpec {
    /// Caltable path.
    pub path: PathBuf,
    /// Optional per-table MS applicability selection.
    pub apply_to: ApplyTableSelection,
    /// Explicit gainfield override; absent means "use the MS field id".
    pub gainfield: Option<GainFieldSelector>,
    /// Optional CASA-style data-SPW to caltable-SPW mapping.
    pub spwmap: Vec<i32>,
    /// Interpolation mode for this table.
    pub interp: ApplyInterpolationMode,
    /// Whether this table participates in weight updates.
    pub calwt: bool,
}

impl ApplyCalibrationTableSpec {
    /// Create a default apply-table spec for `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            apply_to: ApplyTableSelection::default(),
            gainfield: None,
            spwmap: Vec::new(),
            interp: ApplyInterpolationMode::Nearest,
            calwt: false,
        }
    }
}

/// Request passed to the apply planner.
#[derive(Debug, Clone)]
pub struct ApplyPlanRequest {
    /// MS row selection applied before per-table planning.
    pub selection: MsSelection,
    /// Application mode planned for execution.
    pub apply_mode: ApplyMode,
    /// Whether to apply the parallactic-angle `P Jones` term.
    pub parang: bool,
    /// Ordered calibration tables to resolve.
    pub calibration_tables: Vec<ApplyCalibrationTableSpec>,
}

/// One selected MeasurementSet row carried forward by the plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyRowPlan {
    /// MAIN row index.
    pub row_index: usize,
    /// FIELD_ID from MAIN.
    pub field_id: i32,
    /// OBSERVATION_ID from MAIN.
    pub observation_id: i32,
    /// DATA_DESC_ID from MAIN.
    pub data_desc_id: i32,
    /// SPECTRAL_WINDOW_ID resolved through DATA_DESCRIPTION.
    pub data_spw_id: i32,
    /// POLARIZATION_ID resolved through DATA_DESCRIPTION.
    pub polarization_id: i32,
    /// ANTENNA1 from MAIN.
    pub antenna1: i32,
    /// ANTENNA2 from MAIN.
    pub antenna2: i32,
    /// FEED1 from MAIN.
    pub feed1: i32,
    /// FEED2 from MAIN.
    pub feed2: i32,
    /// TIME from MAIN.
    pub time_seconds: f64,
    /// INTERVAL from MAIN.
    pub interval_seconds: f64,
}

/// Frequency-grid summary attached to a selected or mapped spectral window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpectralWindowPlan {
    /// Spectral window identifier.
    pub spw_id: i32,
    /// Number of channels.
    pub num_chan: i32,
    /// Reference frequency in Hz.
    pub ref_frequency_hz: f64,
    /// Channel-center frequencies in Hz.
    pub channel_frequencies_hz: Vec<f64>,
}

/// Resolved explicit gainfield override.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedGainField {
    /// Original selector.
    pub selector: GainFieldSelector,
    /// Resolved FIELD_ID.
    pub field_id: i32,
    /// FIELD.NAME if available.
    pub field_name: Option<String>,
}

/// Resolved nearest-field mapping for one selected MS field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedNearestGainField {
    /// MS FIELD_ID to which this nearest mapping applies.
    pub measurement_set_field_id: i32,
    /// MS FIELD.NAME if available.
    pub measurement_set_field_name: Option<String>,
    /// Resolved caltable FIELD_ID.
    pub calibration_field_id: i32,
    /// Caltable FIELD.NAME if available.
    pub calibration_field_name: Option<String>,
    /// Angular separation in radians used for the nearest match.
    pub angular_separation_rad: f64,
}

/// Resolved mapping from an MS data SPW to a caltable SPW.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ApplySpwMapping {
    /// Selected MS data spectral window id.
    pub data_spw_id: i32,
    /// Caltable spectral window id to use for that data SPW.
    pub calibration_spw_id: i32,
}

/// Planner output for one input calibration table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyCalibrationTablePlan {
    /// Original request spec.
    pub spec: ApplyCalibrationTableSpec,
    /// Number of selected rows to which this table can apply.
    pub applicable_selected_row_count: usize,
    /// Normalized summary for the table.
    pub summary: CalibrationTableSummary,
    /// Resolved explicit gainfield, if any.
    pub resolved_gainfield: Option<ResolvedGainField>,
    /// Resolved per-MS-field nearest gainfield mappings, if requested.
    pub resolved_nearest_gainfields: Vec<ResolvedNearestGainField>,
    /// Mapped SPWs needed by the selected MS rows.
    pub spw_mapping: Vec<ApplySpwMapping>,
    /// Unique mapped caltable spectral windows and their grids.
    pub calibration_spectral_windows: Vec<SpectralWindowPlan>,
    /// Interpolation mode preserved in the resolved plan.
    pub interp: ApplyInterpolationMode,
    /// Whether the table will participate in weight updates.
    pub calwt: bool,
}

/// Full apply plan for one MeasurementSet selection and table chain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyPlan {
    /// Filesystem path of the MeasurementSet when planned from disk.
    pub measurement_set_path: Option<PathBuf>,
    /// Application mode passed to the planner.
    pub apply_mode: ApplyMode,
    /// Whether the executor must create `CORRECTED_DATA`.
    pub requires_corrected_data_column: bool,
    /// Selected rows carried forward to execution.
    pub selected_rows: Vec<ApplyRowPlan>,
    /// Number of selected rows.
    pub selected_row_count: usize,
    /// Whether the executor should apply the parallactic-angle term.
    pub parang: bool,
    /// Unique FIELD_IDs represented by `selected_rows`.
    pub selected_field_ids: Vec<i32>,
    /// Unique DATA_DESC_IDs represented by `selected_rows`.
    pub selected_data_desc_ids: Vec<i32>,
    /// Unique SPECTRAL_WINDOW_IDs represented by `selected_rows`.
    pub selected_data_spw_ids: Vec<i32>,
    /// Unique selected MS spectral windows and their grids.
    pub measurement_set_spectral_windows: Vec<SpectralWindowPlan>,
    /// Per-table resolved plans in request order.
    pub calibration_tables: Vec<ApplyCalibrationTablePlan>,
}

/// Timing breakdown for constructing an apply plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, JsonSchema)]
pub struct ApplyPlanTimings {
    /// Time spent applying the MS selection and collecting row indices.
    pub selection_ns: u64,
    /// Time spent expanding selected rows into executor-ready row metadata.
    pub selected_rows_ns: u64,
    /// Time spent loading MS spectral-window metadata for selected data SPWs.
    pub measurement_set_spectral_windows_ns: u64,
    /// Time spent resolving calibration-table summaries and per-table plans.
    pub calibration_table_plans_ns: u64,
}

/// Errors returned while constructing an apply plan.
#[derive(Debug, Error)]
pub enum ApplyPlanError {
    /// Opening the MeasurementSet failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path that was being opened.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// Opening or summarizing a calibration table failed.
    #[error("failed to summarize calibration table {path}: {source}")]
    SummarizeCalibrationTable {
        /// Path that was being summarized.
        path: String,
        /// Underlying summary error.
        #[source]
        source: Box<CalibrationTableError>,
    },

    /// The request omitted all calibration tables and did not request `parang`.
    #[error("apply planning requires at least one calibration table unless parang=true")]
    MissingCalibrationTables,

    /// The MeasurementSet is outside the supported complex-DATA surface.
    #[error("MeasurementSet is missing required MAIN.DATA column")]
    MissingDataColumn,

    /// Applying the selection failed.
    #[error("failed to apply MS selection: {source}")]
    Selection {
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// A selected row references a DATA_DESCRIPTION row that is not present.
    #[error("MAIN row {row_index} references missing DATA_DESCRIPTION row {data_desc_id}")]
    MissingDataDescription {
        /// MAIN row index.
        row_index: usize,
        /// Missing DATA_DESC_ID value.
        data_desc_id: i32,
    },

    /// A requested caltable is outside the supported v1 apply surface.
    #[error("calibration table {path} is outside the supported apply surface: {reason}")]
    UnsupportedCalibrationTable {
        /// Table path.
        path: String,
        /// Human-readable reason.
        reason: String,
    },

    /// A requested gainfield could not be resolved.
    #[error("failed to resolve gainfield {selector} for {path}: {reason}")]
    ResolveGainField {
        /// Table path.
        path: String,
        /// Human-readable selector.
        selector: String,
        /// Human-readable reason.
        reason: String,
    },

    /// `spwmap` omitted an entry needed by the selected rows.
    #[error("spwmap for {path} does not define a mapping for data spectral window {data_spw_id}")]
    MissingSpwMapEntry {
        /// Table path.
        path: String,
        /// Selected data spectral window id.
        data_spw_id: i32,
    },

    /// A mapped caltable SPW is absent from the table.
    #[error("calibration table {path} does not contain spectral window {calibration_spw_id}")]
    MissingCalibrationSpectralWindow {
        /// Table path.
        path: String,
        /// Missing caltable SPW id.
        calibration_spw_id: i32,
    },

    /// A scalar field had an unexpected type.
    #[error("{context}: expected scalar {expected}, found {found}")]
    ScalarTypeMismatch {
        /// Human-readable location.
        context: String,
        /// Expected scalar type.
        expected: &'static str,
        /// Found scalar type.
        found: &'static str,
    },

    /// Opening an auxiliary subtable failed.
    #[error("failed to open {subtable} for {path}: {source}")]
    OpenSubtable {
        /// Parent table path.
        path: String,
        /// Subtable name.
        subtable: &'static str,
        /// Underlying table error.
        #[source]
        source: TableError,
    },
}

/// Open a MeasurementSet from `path` and construct an apply plan.
pub fn plan_apply_from_path(
    path: impl AsRef<Path>,
    request: &ApplyPlanRequest,
) -> Result<ApplyPlan, ApplyPlanError> {
    let path = path.as_ref().to_path_buf();
    let ms = MeasurementSet::open(&path).map_err(|source| ApplyPlanError::OpenMeasurementSet {
        path: path.display().to_string(),
        source,
    })?;
    Ok(plan_apply_with_timings(&ms, request)?.0)
}

/// Construct an apply plan for an already-open MeasurementSet.
pub fn plan_apply(
    ms: &MeasurementSet,
    request: &ApplyPlanRequest,
) -> Result<ApplyPlan, ApplyPlanError> {
    Ok(plan_apply_with_timings(ms, request)?.0)
}

/// Construct an apply plan and return the planner timing breakdown.
pub fn plan_apply_with_timings(
    ms: &MeasurementSet,
    request: &ApplyPlanRequest,
) -> Result<(ApplyPlan, ApplyPlanTimings), ApplyPlanError> {
    if request.calibration_tables.is_empty() && !request.parang {
        return Err(ApplyPlanError::MissingCalibrationTables);
    }

    ms.data_column(VisibilityDataColumn::Data)
        .map_err(|error| match error {
            MsError::ColumnNotPresent(_) => ApplyPlanError::MissingDataColumn,
            other => ApplyPlanError::Selection { source: other },
        })?;

    let requires_corrected_data_column = match ms.data_column(VisibilityDataColumn::CorrectedData) {
        Ok(_) => false,
        Err(MsError::ColumnNotPresent(_)) => true,
        Err(other) => return Err(ApplyPlanError::Selection { source: other }),
    };

    let selection_started_at = Instant::now();
    let selected_row_indices = request
        .selection
        .apply(ms)
        .map_err(|source| ApplyPlanError::Selection { source })?;
    let selection_ns = selection_started_at.elapsed().as_nanos() as u64;

    let selected_rows_started_at = Instant::now();
    let selected_rows = build_selected_rows(ms, &selected_row_indices)?;
    let selected_rows_ns = selected_rows_started_at.elapsed().as_nanos() as u64;

    let selected_field_ids = unique_i32(selected_rows.iter().map(|row| row.field_id));
    let selected_data_desc_ids = unique_i32(selected_rows.iter().map(|row| row.data_desc_id));
    let selected_data_spw_ids = unique_i32(selected_rows.iter().map(|row| row.data_spw_id));

    let ms_spw_started_at = Instant::now();
    let measurement_set_spectral_windows = load_ms_spectral_windows(ms, &selected_data_spw_ids)?;
    let measurement_set_spectral_windows_ns = ms_spw_started_at.elapsed().as_nanos() as u64;

    let calibration_tables_started_at = Instant::now();
    let calibration_tables = request
        .calibration_tables
        .iter()
        .map(|spec| build_table_plan(ms, spec, &selected_rows))
        .collect::<Result<Vec<_>, _>>()?;
    let calibration_table_plans_ns = calibration_tables_started_at.elapsed().as_nanos() as u64;

    Ok((
        ApplyPlan {
            measurement_set_path: ms.path().map(Path::to_path_buf),
            apply_mode: request.apply_mode,
            requires_corrected_data_column,
            selected_row_count: selected_rows.len(),
            parang: request.parang,
            selected_rows,
            selected_field_ids,
            selected_data_desc_ids,
            selected_data_spw_ids,
            measurement_set_spectral_windows,
            calibration_tables,
        },
        ApplyPlanTimings {
            selection_ns,
            selected_rows_ns,
            measurement_set_spectral_windows_ns,
            calibration_table_plans_ns,
        },
    ))
}

fn build_selected_rows(
    ms: &MeasurementSet,
    row_indices: &[usize],
) -> Result<Vec<ApplyRowPlan>, ApplyPlanError> {
    let dd = ms
        .data_description()
        .map_err(|source| ApplyPlanError::Selection { source })?;
    let table = ms.main_table();
    let data_desc_ids = load_i32_column(table, "DATA_DESC_ID")?;
    let field_ids = load_i32_column(table, "FIELD_ID")?;
    let observation_ids = load_i32_column(table, "OBSERVATION_ID")?;
    let antenna1s = load_i32_column(table, "ANTENNA1")?;
    let antenna2s = load_i32_column(table, "ANTENNA2")?;
    let feed1s = load_i32_column(table, "FEED1")?;
    let feed2s = load_i32_column(table, "FEED2")?;
    let times = load_f64_column(table, "TIME")?;
    let intervals = load_f64_column(table, "INTERVAL")?;

    row_indices
        .iter()
        .map(|&row_index| {
            let data_desc_id = value_at_i32(&data_desc_ids, row_index, "DATA_DESC_ID")?;
            let ddid_index = usize::try_from(data_desc_id).map_err(|_| {
                ApplyPlanError::MissingDataDescription {
                    row_index,
                    data_desc_id,
                }
            })?;
            if ddid_index >= dd.row_count() {
                return Err(ApplyPlanError::MissingDataDescription {
                    row_index,
                    data_desc_id,
                });
            }

            Ok(ApplyRowPlan {
                row_index,
                field_id: value_at_i32(&field_ids, row_index, "FIELD_ID")?,
                observation_id: value_at_i32(&observation_ids, row_index, "OBSERVATION_ID")?,
                data_desc_id,
                data_spw_id: dd
                    .spectral_window_id(ddid_index)
                    .map_err(|source| ApplyPlanError::Selection { source })?,
                polarization_id: dd
                    .polarization_id(ddid_index)
                    .map_err(|source| ApplyPlanError::Selection { source })?,
                antenna1: value_at_i32(&antenna1s, row_index, "ANTENNA1")?,
                antenna2: value_at_i32(&antenna2s, row_index, "ANTENNA2")?,
                feed1: value_at_i32(&feed1s, row_index, "FEED1")?,
                feed2: value_at_i32(&feed2s, row_index, "FEED2")?,
                time_seconds: value_at_f64(&times, row_index, "TIME")?,
                interval_seconds: value_at_f64(&intervals, row_index, "INTERVAL")?,
            })
        })
        .collect()
}

fn load_i32_column(table: &Table, column: &str) -> Result<Vec<Option<i32>>, ApplyPlanError> {
    table
        .column_accessor(column)
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|source| ApplyPlanError::Selection {
            source: MsError::from(source),
        })?
        .into_iter()
        .enumerate()
        .map(|(row_index, value)| match value {
            Some(casa_types::ScalarValue::Int32(v)) => Ok(Some(v)),
            Some(other) => Err(ApplyPlanError::ScalarTypeMismatch {
                context: format!("{column} row {row_index}"),
                expected: "Int32",
                found: scalar_kind(&other),
            }),
            None => Ok(None),
        })
        .collect()
}

fn load_f64_column(table: &Table, column: &str) -> Result<Vec<Option<f64>>, ApplyPlanError> {
    table
        .column_accessor(column)
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|source| ApplyPlanError::Selection {
            source: MsError::from(source),
        })?
        .into_iter()
        .enumerate()
        .map(|(row_index, value)| match value {
            Some(casa_types::ScalarValue::Float64(v)) => Ok(Some(v)),
            Some(other) => Err(ApplyPlanError::ScalarTypeMismatch {
                context: format!("{column} row {row_index}"),
                expected: "Float64",
                found: scalar_kind(&other),
            }),
            None => Ok(None),
        })
        .collect()
}

fn value_at_i32(
    values: &[Option<i32>],
    row_index: usize,
    column: &str,
) -> Result<i32, ApplyPlanError> {
    values
        .get(row_index)
        .and_then(|value| *value)
        .ok_or_else(|| ApplyPlanError::Selection {
            source: MsError::MissingColumn {
                column: column.to_string(),
                table: format!("MAIN row {row_index}"),
            },
        })
}

fn value_at_f64(
    values: &[Option<f64>],
    row_index: usize,
    column: &str,
) -> Result<f64, ApplyPlanError> {
    values
        .get(row_index)
        .and_then(|value| *value)
        .ok_or_else(|| ApplyPlanError::Selection {
            source: MsError::MissingColumn {
                column: column.to_string(),
                table: format!("MAIN row {row_index}"),
            },
        })
}

fn load_ms_spectral_windows(
    ms: &MeasurementSet,
    spw_ids: &[i32],
) -> Result<Vec<SpectralWindowPlan>, ApplyPlanError> {
    let spw = ms
        .spectral_window()
        .map_err(|source| ApplyPlanError::Selection { source })?;
    spw_ids
        .iter()
        .map(|&spw_id| spectral_window_plan(&spw, spw_id))
        .collect()
}

fn build_table_plan(
    ms: &MeasurementSet,
    spec: &ApplyCalibrationTableSpec,
    selected_rows: &[ApplyRowPlan],
) -> Result<ApplyCalibrationTablePlan, ApplyPlanError> {
    let summary = summarize_table(&spec.path).map_err(|source| {
        ApplyPlanError::SummarizeCalibrationTable {
            path: spec.path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    if !summary.supported_for_v1_apply() {
        let reason = summary
            .issues
            .iter()
            .map(|issue| format!("{} ({:?})", issue.message, issue.severity))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(ApplyPlanError::UnsupportedCalibrationTable {
            path: spec.path.display().to_string(),
            reason,
        });
    }

    let applicable_rows = selected_rows
        .iter()
        .filter(|row| spec.apply_to.matches(row))
        .collect::<Vec<_>>();
    let applicable_field_ids = unique_i32(applicable_rows.iter().map(|row| row.field_id));
    let applicable_data_spw_ids = unique_i32(applicable_rows.iter().map(|row| row.data_spw_id));

    let is_bpoly = summary.table_subtype == "BPOLY";
    let (resolved_gainfield, resolved_nearest_gainfields) = if is_bpoly {
        if let Some(selector) = spec.gainfield.as_ref() {
            return Err(ApplyPlanError::ResolveGainField {
                path: spec.path.display().to_string(),
                selector: format!("{selector:?}"),
                reason: "BPOLY apply currently supports only the default field mapping".to_string(),
            });
        }
        (None, Vec::new())
    } else {
        resolve_gainfield(ms, spec, &summary, &applicable_field_ids)?
    };
    let available_calibration_spw_ids = if is_bpoly {
        load_bpoly_spectral_window_ids(&summary.path)?
    } else {
        summary.spectral_window_ids.clone()
    };
    let spw_mapping = resolve_spw_mapping(
        spec,
        &available_calibration_spw_ids,
        &applicable_data_spw_ids,
    )?;
    let calibration_spectral_windows = if is_bpoly {
        let mapped_cal_spw_ids =
            unique_i32(spw_mapping.iter().map(|mapping| mapping.calibration_spw_id));
        load_ms_spectral_windows(ms, &mapped_cal_spw_ids)?
    } else {
        load_caltable_spectral_windows(&summary, &spw_mapping)?
    };

    Ok(ApplyCalibrationTablePlan {
        spec: spec.clone(),
        applicable_selected_row_count: applicable_rows.len(),
        summary,
        resolved_gainfield,
        resolved_nearest_gainfields,
        spw_mapping,
        calibration_spectral_windows,
        interp: spec.interp,
        calwt: spec.calwt,
    })
}

fn resolve_gainfield(
    ms: &MeasurementSet,
    spec: &ApplyCalibrationTableSpec,
    summary: &CalibrationTableSummary,
    selected_field_ids: &[i32],
) -> Result<(Option<ResolvedGainField>, Vec<ResolvedNearestGainField>), ApplyPlanError> {
    let Some(selector) = spec.gainfield.as_ref() else {
        return Ok((None, Vec::new()));
    };

    match selector {
        GainFieldSelector::FieldId(field_id) => {
            if !summary.field_ids.contains(field_id) {
                return Err(ApplyPlanError::ResolveGainField {
                    path: spec.path.display().to_string(),
                    selector: field_id.to_string(),
                    reason: "FIELD_ID is not referenced by any MAIN row".to_string(),
                });
            }
            Ok((
                Some(ResolvedGainField {
                    selector: selector.clone(),
                    field_id: *field_id,
                    field_name: field_name_by_id(summary, *field_id)?,
                }),
                Vec::new(),
            ))
        }
        GainFieldSelector::FieldName(name) => resolve_gainfield_by_name(spec, summary, name)
            .map(|resolved| (Some(resolved), Vec::new())),
        GainFieldSelector::Nearest => {
            resolve_gainfield_nearest(ms, spec, summary, selected_field_ids)
                .map(|resolved| (None, resolved))
        }
    }
}

fn resolve_gainfield_by_name(
    spec: &ApplyCalibrationTableSpec,
    summary: &CalibrationTableSummary,
    field_name: &str,
) -> Result<ResolvedGainField, ApplyPlanError> {
    let table = open_summary_subtable(summary, "FIELD")?;
    let mut matches = Vec::new();

    for row_index in 0..table.row_count() {
        let name = get_string(&table, row_index, "NAME")?;
        if name == field_name && summary.field_ids.contains(&(row_index as i32)) {
            matches.push(row_index as i32);
        }
    }

    match matches.as_slice() {
        [field_id] => Ok(ResolvedGainField {
            selector: GainFieldSelector::FieldName(field_name.to_string()),
            field_id: *field_id,
            field_name: Some(field_name.to_string()),
        }),
        [] => Err(ApplyPlanError::ResolveGainField {
            path: spec.path.display().to_string(),
            selector: field_name.to_string(),
            reason: "no exact FIELD.NAME match referenced by the calibration rows".to_string(),
        }),
        _ => Err(ApplyPlanError::ResolveGainField {
            path: spec.path.display().to_string(),
            selector: field_name.to_string(),
            reason: "multiple exact FIELD.NAME matches were referenced by the calibration rows"
                .to_string(),
        }),
    }
}

#[derive(Debug, Clone)]
struct FieldDirectionInfo {
    field_id: i32,
    field_name: Option<String>,
    ra_rad: f64,
    dec_rad: f64,
}

fn resolve_gainfield_nearest(
    ms: &MeasurementSet,
    spec: &ApplyCalibrationTableSpec,
    summary: &CalibrationTableSummary,
    selected_field_ids: &[i32],
) -> Result<Vec<ResolvedNearestGainField>, ApplyPlanError> {
    let ms_fields = load_ms_field_directions(ms, selected_field_ids)?;
    let cal_fields = load_caltable_field_directions(summary)?;
    if cal_fields.is_empty() {
        return Err(ApplyPlanError::ResolveGainField {
            path: spec.path.display().to_string(),
            selector: "nearest".to_string(),
            reason: "no usable FIELD directions were found in the calibration table".to_string(),
        });
    }

    let mut resolved = Vec::new();
    for ms_field in ms_fields {
        let mut best = None::<(&FieldDirectionInfo, f64)>;
        for cal_field in &cal_fields {
            let separation = angular_separation_rad(
                ms_field.ra_rad,
                ms_field.dec_rad,
                cal_field.ra_rad,
                cal_field.dec_rad,
            );
            match best {
                Some((_, best_separation)) if separation >= best_separation => {}
                _ => best = Some((cal_field, separation)),
            }
        }
        let Some((cal_field, separation)) = best else {
            return Err(ApplyPlanError::ResolveGainField {
                path: spec.path.display().to_string(),
                selector: "nearest".to_string(),
                reason: format!(
                    "no caltable FIELD rows were available for MS field {}",
                    ms_field.field_id
                ),
            });
        };
        resolved.push(ResolvedNearestGainField {
            measurement_set_field_id: ms_field.field_id,
            measurement_set_field_name: ms_field.field_name.clone(),
            calibration_field_id: cal_field.field_id,
            calibration_field_name: cal_field.field_name.clone(),
            angular_separation_rad: separation,
        });
    }
    Ok(resolved)
}

fn load_ms_field_directions(
    ms: &MeasurementSet,
    selected_field_ids: &[i32],
) -> Result<Vec<FieldDirectionInfo>, ApplyPlanError> {
    let field = ms
        .field()
        .map_err(|source| ApplyPlanError::Selection { source })?;
    let mut out = Vec::new();
    for &field_id in selected_field_ids {
        let row = usize::try_from(field_id).map_err(|_| ApplyPlanError::ResolveGainField {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            selector: "nearest".to_string(),
            reason: format!("MS FIELD_ID {field_id} does not fit in usize"),
        })?;
        if row >= field.row_count() {
            return Err(ApplyPlanError::ResolveGainField {
                path: ms
                    .path()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<in-memory>".to_string()),
                selector: "nearest".to_string(),
                reason: format!("MS FIELD_ID {field_id} is outside the FIELD subtable"),
            });
        }
        let (ra_rad, dec_rad) = phase_direction_to_radec(
            field
                .phase_dir(row)
                .map_err(|source| ApplyPlanError::Selection { source })?,
        )
        .map_err(|reason| ApplyPlanError::ResolveGainField {
            path: ms
                .path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
            selector: "nearest".to_string(),
            reason: format!("MS FIELD_ID {field_id}: {reason}"),
        })?;
        out.push(FieldDirectionInfo {
            field_id,
            field_name: field.name(row).ok(),
            ra_rad,
            dec_rad,
        });
    }
    Ok(out)
}

fn load_caltable_field_directions(
    summary: &CalibrationTableSummary,
) -> Result<Vec<FieldDirectionInfo>, ApplyPlanError> {
    let table = open_summary_subtable(summary, "FIELD")?;
    let mut out = Vec::new();
    for row_index in 0..table.row_count() {
        let field_id = row_index as i32;
        if !summary.field_ids.contains(&field_id) {
            continue;
        }
        let phase_dir = table
            .cell_accessor(row_index, "PHASE_DIR")
            .and_then(|cell| cell.array())
            .map_err(|source| ApplyPlanError::OpenSubtable {
                path: summary.path.display().to_string(),
                subtable: "FIELD",
                source,
            })?;
        let (ra_rad, dec_rad) = phase_direction_to_radec(phase_dir).map_err(|reason| {
            ApplyPlanError::ResolveGainField {
                path: summary.path.display().to_string(),
                selector: "nearest".to_string(),
                reason: format!("caltable FIELD_ID {field_id}: {reason}"),
            }
        })?;
        out.push(FieldDirectionInfo {
            field_id,
            field_name: get_string(&table, row_index, "NAME").ok(),
            ra_rad,
            dec_rad,
        });
    }
    Ok(out)
}

fn phase_direction_to_radec(value: &ArrayValue) -> Result<(f64, f64), String> {
    let ArrayValue::Float64(values) = value else {
        return Err("PHASE_DIR was not a Float64 array".to_string());
    };
    if values.ndim() != 2 {
        return Err(format!("PHASE_DIR had shape {:?}", values.shape()));
    }
    let shape = values.shape();
    if shape[0] < 2 || shape[1] == 0 {
        return Err(format!("PHASE_DIR had unsupported shape {:?}", shape));
    }
    Ok((values[[0, 0]], values[[1, 0]]))
}

fn angular_separation_rad(ra1: f64, dec1: f64, ra2: f64, dec2: f64) -> f64 {
    let cos_sep = dec1.sin() * dec2.sin() + dec1.cos() * dec2.cos() * (ra1 - ra2).cos();
    cos_sep.clamp(-1.0, 1.0).acos()
}

fn resolve_spw_mapping(
    spec: &ApplyCalibrationTableSpec,
    available_calibration_spw_ids: &[i32],
    selected_data_spw_ids: &[i32],
) -> Result<Vec<ApplySpwMapping>, ApplyPlanError> {
    selected_data_spw_ids
        .iter()
        .map(|&data_spw_id| {
            let calibration_spw_id = if spec.spwmap.is_empty() {
                data_spw_id
            } else {
                spec.spwmap
                    .get(usize::try_from(data_spw_id).unwrap_or(usize::MAX))
                    .copied()
                    .ok_or_else(|| ApplyPlanError::MissingSpwMapEntry {
                        path: spec.path.display().to_string(),
                        data_spw_id,
                    })?
            };
            if !available_calibration_spw_ids.contains(&calibration_spw_id) {
                return Err(ApplyPlanError::MissingCalibrationSpectralWindow {
                    path: spec.path.display().to_string(),
                    calibration_spw_id,
                });
            }
            Ok(ApplySpwMapping {
                data_spw_id,
                calibration_spw_id,
            })
        })
        .collect()
}

fn load_bpoly_spectral_window_ids(path: &Path) -> Result<Vec<i32>, ApplyPlanError> {
    let table = Table::open(TableOptions::new(path.join("CAL_DESC"))).map_err(|source| {
        ApplyPlanError::OpenSubtable {
            path: path.display().to_string(),
            subtable: "CAL_DESC",
            source,
        }
    })?;
    let mut spw_ids = BTreeSet::new();
    for row in 0..table.row_count() {
        let values = table
            .cell_accessor(row, "SPECTRAL_WINDOW_ID")
            .and_then(|cell| cell.array())
            .map_err(|source| ApplyPlanError::OpenSubtable {
                path: path.display().to_string(),
                subtable: "CAL_DESC",
                source,
            })?;
        match values {
            ArrayValue::Int32(values) => {
                for spw_id in values.iter().copied() {
                    spw_ids.insert(spw_id);
                }
            }
            other => {
                return Err(ApplyPlanError::ScalarTypeMismatch {
                    context: format!(
                        "CAL_DESC:SPECTRAL_WINDOW_ID primitive {:?}",
                        other.primitive_type()
                    ),
                    expected: "Int32 array",
                    found: "non-Int32 array",
                });
            }
        }
    }
    Ok(spw_ids.into_iter().collect())
}

fn load_caltable_spectral_windows(
    summary: &CalibrationTableSummary,
    spw_mapping: &[ApplySpwMapping],
) -> Result<Vec<SpectralWindowPlan>, ApplyPlanError> {
    let table = open_summary_subtable(summary, "SPECTRAL_WINDOW")?;
    let spw = MsSpectralWindow::new(&table);
    unique_i32(spw_mapping.iter().map(|mapping| mapping.calibration_spw_id))
        .iter()
        .map(|&spw_id| spectral_window_plan(&spw, spw_id))
        .collect()
}

fn spectral_window_plan(
    spw: &MsSpectralWindow<'_>,
    spw_id: i32,
) -> Result<SpectralWindowPlan, ApplyPlanError> {
    let row =
        usize::try_from(spw_id).map_err(|_| ApplyPlanError::MissingCalibrationSpectralWindow {
            path: "<subtable>".to_string(),
            calibration_spw_id: spw_id,
        })?;
    Ok(SpectralWindowPlan {
        spw_id,
        num_chan: spw
            .num_chan(row)
            .map_err(|source| ApplyPlanError::Selection { source })?,
        ref_frequency_hz: spw
            .ref_frequency(row)
            .map_err(|source| ApplyPlanError::Selection { source })?,
        channel_frequencies_hz: spw
            .chan_freq(row)
            .map_err(|source| ApplyPlanError::Selection { source })?,
    })
}

fn field_name_by_id(
    summary: &CalibrationTableSummary,
    field_id: i32,
) -> Result<Option<String>, ApplyPlanError> {
    let table = open_summary_subtable(summary, "FIELD")?;
    let row = match usize::try_from(field_id) {
        Ok(row) if row < table.row_count() => row,
        _ => return Ok(None),
    };
    get_string(&table, row, "NAME").map(Some)
}

fn open_summary_subtable(
    summary: &CalibrationTableSummary,
    name: &'static str,
) -> Result<Table, ApplyPlanError> {
    let subtable = summary
        .subtables
        .iter()
        .find(|subtable| subtable.name == name)
        .ok_or_else(|| ApplyPlanError::ResolveGainField {
            path: summary.path.display().to_string(),
            selector: name.to_string(),
            reason: "summary did not record the requested subtable".to_string(),
        })?;
    let path = subtable
        .resolved_path
        .as_ref()
        .ok_or_else(|| ApplyPlanError::ResolveGainField {
            path: summary.path.display().to_string(),
            selector: name.to_string(),
            reason: "subtable link is missing".to_string(),
        })?;
    Table::open(TableOptions::new(path)).map_err(|source| ApplyPlanError::OpenSubtable {
        path: summary.path.display().to_string(),
        subtable: name,
        source,
    })
}

fn unique_i32(values: impl IntoIterator<Item = i32>) -> Vec<i32> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn get_string(table: &Table, row_index: usize, column: &str) -> Result<String, ApplyPlanError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyPlanError::Selection {
            source: MsError::from(source),
        })? {
        ScalarValue::String(value) => Ok(value.clone()),
        other => Err(ApplyPlanError::ScalarTypeMismatch {
            context: format!("{column} row {row_index}"),
            expected: "String",
            found: scalar_kind(other),
        }),
    }
}

#[cfg(test)]
fn get_i32(table: &Table, row_index: usize, column: &str) -> Result<i32, ApplyPlanError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyPlanError::Selection {
            source: MsError::from(source),
        })? {
        &ScalarValue::Int32(value) => Ok(value),
        other => Err(ApplyPlanError::ScalarTypeMismatch {
            context: format!("{column} row {row_index}"),
            expected: "Int32",
            found: scalar_kind(other),
        }),
    }
}

#[cfg(test)]
fn get_f64(table: &Table, row_index: usize, column: &str) -> Result<f64, ApplyPlanError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyPlanError::Selection {
            source: MsError::from(source),
        })? {
        &ScalarValue::Float64(value) => Ok(value),
        other => Err(ApplyPlanError::ScalarTypeMismatch {
            context: format!("{column} row {row_index}"),
            expected: "Float64",
            found: scalar_kind(other),
        }),
    }
}

fn scalar_kind(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Bool(_) => "Bool",
        ScalarValue::UInt8(_) => "UInt8",
        ScalarValue::UInt16(_) => "UInt16",
        ScalarValue::UInt32(_) => "UInt32",
        ScalarValue::Int16(_) => "Int16",
        ScalarValue::Int32(_) => "Int32",
        ScalarValue::Int64(_) => "Int64",
        ScalarValue::Float32(_) => "Float32",
        ScalarValue::Float64(_) => "Float64",
        ScalarValue::Complex32(_) => "Complex32",
        ScalarValue::Complex64(_) => "Complex64",
        ScalarValue::String(_) => "String",
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
    use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};
    use tempfile::tempdir;

    use super::*;
    use crate::model::{
        CalibrationColumnSummary, CalibrationIssueSeverity, CalibrationKeywordSummary,
        CalibrationParameterFamily, CalibrationSubtableSummary, CalibrationTableSummary,
        CalibrationValidationIssue,
    };

    fn row(fields: Vec<RecordField>) -> RecordValue {
        RecordValue::new(fields)
    }

    fn scalar_table(fields: Vec<RecordField>) -> Table {
        Table::from_rows_memory(vec![row(fields)])
    }

    fn empty_summary(path: PathBuf) -> CalibrationTableSummary {
        CalibrationTableSummary {
            path,
            table_type: "Calibration".to_string(),
            table_subtype: "G Jones".to_string(),
            row_count: 0,
            columns: Vec::new(),
            keywords: CalibrationKeywordSummary {
                par_type: None,
                vis_cal: None,
                ms_name: None,
                pol_basis: None,
                casa_version: None,
            },
            subtables: Vec::new(),
            parameter_family: CalibrationParameterFamily::Complex,
            parameter_column: CalibrationColumnSummary {
                parameter_column: None,
                parameter_primitive_type: None,
                first_cell_shape: None,
            },
            field_ids: Vec::new(),
            spectral_window_ids: Vec::new(),
            antenna1_ids: Vec::new(),
            antenna2_ids: Vec::new(),
            observation_ids: Vec::new(),
            time_coverage: None,
            issues: Vec::new(),
        }
    }

    #[test]
    fn helper_math_and_spw_mapping_cover_success_and_error_paths() {
        let phase_dir = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![1.25_f64, -0.5]).unwrap(),
        );
        assert_eq!(phase_direction_to_radec(&phase_dir).unwrap(), (1.25, -0.5));
        assert!(
            phase_direction_to_radec(&ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![1_i32, 2]).unwrap()
            ))
            .unwrap_err()
            .contains("Float64")
        );
        assert!(
            phase_direction_to_radec(&ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![1.0_f64, 2.0]).unwrap()
            ))
            .unwrap_err()
            .contains("unsupported shape")
        );

        assert!(angular_separation_rad(0.0, 0.0, 0.0, 0.0) < 1.0e-12);
        let pi = angular_separation_rad(0.0, 0.0, std::f64::consts::PI, 0.0);
        assert!((pi - std::f64::consts::PI).abs() < 1.0e-12);

        let mut spec = ApplyCalibrationTableSpec::new("/tmp/fake.gcal");
        spec.interp = ApplyInterpolationMode::Nearest;
        spec.spwmap = vec![7, 5];
        let mappings = resolve_spw_mapping(&spec, &[5, 7], &[0, 1]).unwrap();
        assert_eq!(
            mappings,
            vec![
                ApplySpwMapping {
                    data_spw_id: 0,
                    calibration_spw_id: 7,
                },
                ApplySpwMapping {
                    data_spw_id: 1,
                    calibration_spw_id: 5,
                },
            ]
        );
        match resolve_spw_mapping(&spec, &[5, 7], &[2]).unwrap_err() {
            ApplyPlanError::MissingSpwMapEntry {
                data_spw_id, path, ..
            } => {
                assert_eq!(data_spw_id, 2);
                assert!(path.contains("fake.gcal"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        match resolve_spw_mapping(
            &ApplyCalibrationTableSpec {
                spwmap: vec![],
                ..spec.clone()
            },
            &[5],
            &[3],
        )
        .unwrap_err()
        {
            ApplyPlanError::MissingCalibrationSpectralWindow {
                calibration_spw_id, ..
            } => assert_eq!(calibration_spw_id, 3),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn cal_desc_and_summary_helpers_round_trip_subtables() {
        let temp = tempdir().unwrap();
        let cal_desc_root = temp.path().join("bpoly.cal");
        std::fs::create_dir_all(&cal_desc_root).unwrap();
        let cal_desc_schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "SPECTRAL_WINDOW_ID",
            casa_types::PrimitiveType::Int32,
            Some(1),
        )])
        .unwrap();
        let mut cal_desc = Table::with_schema_memory(cal_desc_schema);
        cal_desc
            .add_row(row(vec![RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![3_i32]).unwrap(),
                )),
            )]))
            .unwrap();
        cal_desc
            .add_row(row(vec![RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![7_i32, 5]).unwrap(),
                )),
            )]))
            .unwrap();
        cal_desc
            .save(TableOptions::new(cal_desc_root.join("CAL_DESC")))
            .unwrap();
        assert_eq!(
            load_bpoly_spectral_window_ids(&cal_desc_root).unwrap(),
            vec![3, 5, 7]
        );

        let bad_root = temp.path().join("bad.cal");
        std::fs::create_dir_all(&bad_root).unwrap();
        let bad_schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "SPECTRAL_WINDOW_ID",
            casa_types::PrimitiveType::Float32,
            Some(1),
        )])
        .unwrap();
        let mut bad_cal_desc = Table::with_schema_memory(bad_schema);
        bad_cal_desc
            .add_row(row(vec![RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![1.0_f32]).unwrap(),
                )),
            )]))
            .unwrap();
        bad_cal_desc
            .save(TableOptions::new(bad_root.join("CAL_DESC")))
            .unwrap();
        match load_bpoly_spectral_window_ids(&bad_root).unwrap_err() {
            ApplyPlanError::ScalarTypeMismatch {
                expected, found, ..
            } => {
                assert_eq!(expected, "Int32 array");
                assert_eq!(found, "non-Int32 array");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let field_path = temp.path().join("FIELD");
        let mut field_table = Table::with_schema_memory(
            TableSchema::new(vec![ColumnSchema::scalar(
                "NAME",
                casa_types::PrimitiveType::String,
            )])
            .unwrap(),
        );
        field_table
            .add_row(row(vec![RecordField::new(
                "NAME",
                Value::Scalar(ScalarValue::String("target".to_string())),
            )]))
            .unwrap();
        field_table.save(TableOptions::new(&field_path)).unwrap();
        let spw_path = temp.path().join("SPECTRAL_WINDOW");
        let mut spw_table = Table::with_schema_memory(
            TableSchema::new(vec![
                ColumnSchema::scalar("NUM_CHAN", casa_types::PrimitiveType::Int32),
                ColumnSchema::scalar("REF_FREQUENCY", casa_types::PrimitiveType::Float64),
                ColumnSchema::array_variable(
                    "CHAN_FREQ",
                    casa_types::PrimitiveType::Float64,
                    Some(1),
                ),
            ])
            .unwrap(),
        );
        spw_table
            .add_row(row(vec![
                RecordField::new("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
                RecordField::new("REF_FREQUENCY", Value::Scalar(ScalarValue::Float64(1.4e9))),
                RecordField::new(
                    "CHAN_FREQ",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![1.4e9_f64, 1.401e9]).unwrap(),
                    )),
                ),
            ]))
            .unwrap();
        spw_table.save(TableOptions::new(&spw_path)).unwrap();

        let mut summary = empty_summary(temp.path().join("gaincal"));
        summary.subtables = vec![
            CalibrationSubtableSummary {
                name: "FIELD".to_string(),
                stored_reference: Some("Table: FIELD".to_string()),
                resolved_path: Some(field_path.clone()),
                exists: true,
                row_count: Some(1),
                open_error: None,
            },
            CalibrationSubtableSummary {
                name: "SPECTRAL_WINDOW".to_string(),
                stored_reference: Some("Table: SPECTRAL_WINDOW".to_string()),
                resolved_path: Some(spw_path.clone()),
                exists: true,
                row_count: Some(1),
                open_error: None,
            },
        ];

        assert_eq!(
            field_name_by_id(&summary, 0).unwrap(),
            Some("target".to_string())
        );
        assert_eq!(field_name_by_id(&summary, 4).unwrap(), None);
        let plan = spectral_window_plan(&MsSpectralWindow::new(&spw_table), 0).unwrap();
        assert_eq!(plan.num_chan, 2);
        assert_eq!(plan.channel_frequencies_hz, vec![1.4e9, 1.401e9]);
        let loaded = load_caltable_spectral_windows(
            &summary,
            &[
                ApplySpwMapping {
                    data_spw_id: 1,
                    calibration_spw_id: 0,
                },
                ApplySpwMapping {
                    data_spw_id: 2,
                    calibration_spw_id: 0,
                },
            ],
        )
        .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].spw_id, 0);
        assert_eq!(
            open_summary_subtable(&summary, "FIELD")
                .unwrap()
                .row_count(),
            1
        );

        let mut broken = summary.clone();
        broken.subtables[0].resolved_path = None;
        match open_summary_subtable(&broken, "FIELD").unwrap_err() {
            ApplyPlanError::ResolveGainField {
                selector, reason, ..
            } => {
                assert_eq!(selector, "FIELD");
                assert!(reason.contains("missing"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn scalar_access_helpers_report_expected_kinds() {
        let table = scalar_table(vec![
            RecordField::new(
                "NAME",
                Value::Scalar(ScalarValue::String("demo".to_string())),
            ),
            RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(3))),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(12.5))),
        ]);

        assert_eq!(get_string(&table, 0, "NAME").unwrap(), "demo");
        assert_eq!(get_i32(&table, 0, "FIELD_ID").unwrap(), 3);
        assert_eq!(get_f64(&table, 0, "TIME").unwrap(), 12.5);
        match get_i32(&table, 0, "NAME").unwrap_err() {
            ApplyPlanError::ScalarTypeMismatch {
                expected, found, ..
            } => {
                assert_eq!(expected, "Int32");
                assert_eq!(found, "String");
            }
            other => panic!("unexpected error: {other:?}"),
        }

        assert_eq!(unique_i32([5, 3, 5, 1, 3]), vec![1, 3, 5]);
        assert_eq!(scalar_kind(&ScalarValue::Bool(true)), "Bool");
        assert_eq!(
            scalar_kind(&ScalarValue::Complex64(casa_types::Complex64::new(
                1.0, 2.0
            ))),
            "Complex64"
        );
    }

    #[test]
    fn summary_support_predicate_accepts_expected_table_families() {
        let mut summary = empty_summary(PathBuf::from("/tmp/summary"));
        assert!(summary.supported_for_v1_apply());

        summary.parameter_family = CalibrationParameterFamily::Float;
        summary.table_subtype = "K Jones".to_string();
        assert!(summary.supported_for_v1_apply());

        summary.table_subtype = "BPOLY".to_string();
        assert!(summary.supported_for_v1_apply());

        summary.issues.push(CalibrationValidationIssue {
            code: "bad".to_string(),
            severity: CalibrationIssueSeverity::Error,
            message: "nope".to_string(),
        });
        assert!(!summary.supported_for_v1_apply());
    }
}
