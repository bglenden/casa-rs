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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_core::{ArrayValue, ScalarValue};
use casa_ms::ChannelSelection;
use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::{MsSelection, ResolvedMsSelection};
use casa_ms::{MsError, MsSpectralWindow};
use casa_tables::{Table, TableError, TableOptions};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::model::CalibrationTableSummary;
use crate::session::resolve_casa_calibration_selection;
use crate::summary::{CalibrationTableError, summarize_table};

#[path = "plan_fields.rs"]
mod fields;
pub(crate) use fields::resolve_gainfield;

#[path = "plan_callibrary.rs"]
mod library;

#[path = "plan_rows.rs"]
mod rows;
use rows::{build_selected_rows, load_i32_column_rows, value_at_i32};

#[path = "plan_spw.rs"]
mod windows;
pub(crate) use windows::resolve_spw_mapping;

/// Calibration-application mode planned for the executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ApplyMode {
    /// Apply calibrations and propagate flags.
    CalFlag,
    /// Apply calibrations and also flag selections without available calibration.
    CalFlagStrict,
    /// Update flags without changing corrected data or weights.
    FlagOnly,
    /// Update only flags, including selections without available calibration.
    FlagOnlyStrict,
    /// Apply calibrations without changing data flags.
    CalOnly,
    /// Build and report the plan without mutating the MeasurementSet.
    Trial,
}

impl ApplyMode {
    /// Whether this mode writes corrected visibilities and calibrated weights.
    pub const fn writes_corrected_data(self) -> bool {
        matches!(self, Self::CalFlag | Self::CalFlagStrict | Self::CalOnly)
    }

    /// Whether calibration flags are written back to the MeasurementSet.
    pub const fn writes_flags(self) -> bool {
        matches!(
            self,
            Self::CalFlag | Self::CalFlagStrict | Self::FlagOnly | Self::FlagOnlyStrict
        )
    }

    /// Whether a selection without calibration must be flagged instead of skipped.
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::CalFlagStrict | Self::FlagOnlyStrict)
    }
}

#[path = "plan_interpolation.rs"]
mod interpolation;
pub use interpolation::ApplyInterpolationMode;

/// Explicit `gainfield` selector accepted by the planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum GainFieldSelector {
    /// Resolve a caltable field by exact integer FIELD_ID.
    FieldId(i32),
    /// Select caltable rows with CASA FIELD grammar.
    Expression(String),
    /// Resolve each selected MS field to the nearest caltable field on sky.
    Nearest,
}

impl GainFieldSelector {
    pub fn parse_casa(value: &str) -> Option<Self> {
        match value {
            "" => None,
            "nearest" => Some(Self::Nearest),
            expression => Some(Self::Expression(expression.to_owned())),
        }
    }
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
    /// Callibrary slice controls; absent uses traditional setapply semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callibrary: Option<crate::CallibraryOptions>,
    /// Optional calibration-field selection; absent merges selected fields in time.
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
            callibrary: None,
            apply_to: ApplyTableSelection::default(),
            gainfield: None,
            spwmap: Vec::new(),
            interp: ApplyInterpolationMode::Linear,
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
    /// First selected OBSERVATION_ID in CASA's ARRAY/SCAN/FIELD/DDID/TIME buffer.
    pub calibration_observation_id: i32,
    /// SCAN_NUMBER from MAIN.
    pub scan_number: i32,
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
    /// Physical MS rows selected by a callibrary slice; None means no extra mask.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applicable_row_indices: Option<Vec<usize>>,
    /// Rows whose library mapping requests an absent calibration context.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unavailable_row_indices: Vec<usize>,
    /// Number of selected rows to which this table can apply.
    pub applicable_selected_row_count: usize,
    /// Normalized summary for the table.
    pub summary: CalibrationTableSummary,
    /// Selected caltable fields; empty selects every calibration field.
    pub resolved_gainfields: Vec<ResolvedGainField>,
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

impl ApplyCalibrationTablePlan {
    /// Whether this table contributes to the selected physical MS row.
    pub fn applies_to(&self, row: &ApplyRowPlan) -> bool {
        self.applies_to_scan(row, row.scan_number)
    }

    pub(crate) fn applies_to_scan(&self, row: &ApplyRowPlan, scan: i32) -> bool {
        self.spec.apply_to.matches(row)
            && self.spec.callibrary.as_ref().is_none_or(|library| {
                library
                    .observation_map
                    .covers(row.calibration_observation_id)
                    && library.scan_map.covers(scan)
                    && (library.field_map.is_empty()
                        || usize::try_from(row.field_id)
                            .is_ok_and(|id| id < library.field_map.len()))
            })
            && self
                .applicable_row_indices
                .as_ref()
                .is_none_or(|rows| rows.binary_search(&row.row_index).is_ok())
    }
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
    /// Nontrivial per-SPW channel selections; absent windows use all channels.
    pub channel_selections: BTreeMap<i32, ApplyChannelSelection>,
    /// Unique selected MS spectral windows and their grids.
    pub measurement_set_spectral_windows: Vec<SpectralWindowPlan>,
    /// Per-table resolved plans in request order.
    pub calibration_tables: Vec<ApplyCalibrationTablePlan>,
}

/// Selected data-channel grid for one MeasurementSet spectral window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ApplyChannelSelection {
    /// Original channel indices in increasing order, with overlaps removed.
    pub indices: Vec<usize>,
    /// The frequencies of those same channels, in Hz.
    pub frequencies_hz: Vec<f64>,
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

    /// One traditional argument failed after opening and selecting the MS.
    #[error(transparent)]
    Argument(ApplyArgumentError),

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

    /// A callibrary could not be parsed after the task selected its data.
    #[error("failed to parse callibrary: {message}")]
    Callibrary {
        /// Preserved parser or file-read diagnostic.
        message: String,
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
    plan_apply_with_preparation_error(ms, request, None)
}

/// A traditional applycal argument whose error follows MS setup and selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, thiserror::Error)]
pub enum ApplyArgumentError {
    /// A table's positional calwt element was missing.
    #[error("list index out of range")]
    MissingCalwt,
    /// A spwmap variant could not be read as a window map.
    #[error("{0}")]
    SpwMap(String),
}

/// Deferred preparation failure raised after MS opening and selection succeed.
#[derive(Clone, Copy)]
pub enum ApplyPreparationError<'a> {
    /// Callibrary parse failure.
    Callibrary(&'a str),
    /// Traditional argument parse failure.
    Argument(&'a ApplyArgumentError),
}

pub(crate) fn plan_apply_with_preparation_error(
    ms: &MeasurementSet,
    request: &ApplyPlanRequest,
    preparation_error: Option<ApplyPreparationError<'_>>,
) -> Result<(ApplyPlan, ApplyPlanTimings), ApplyPlanError> {
    if crate::model_columns::observed_column(ms) != "FLOAT_DATA" {
        ms.data_column(VisibilityDataColumn::Data)
            .map_err(|error| match error {
                MsError::ColumnNotPresent(_) => ApplyPlanError::MissingDataColumn,
                other => ApplyPlanError::Selection { source: other },
            })?;
    }
    plan_with_visibility_metadata(ms, request, preparation_error)
}

pub(crate) fn plan_with_visibility_metadata(
    ms: &MeasurementSet,
    request: &ApplyPlanRequest,
    preparation_error: Option<ApplyPreparationError<'_>>,
) -> Result<(ApplyPlan, ApplyPlanTimings), ApplyPlanError> {
    plan_with_selection(ms, request, preparation_error, None)
        .map(|(plan, timings, _)| (plan, timings))
}

pub(crate) use library::LegacyScanOverrides;
type PlannedApplication = (ApplyPlan, ApplyPlanTimings, LegacyScanOverrides);

#[derive(Clone, Copy)]
pub(crate) struct RegisteredApplySelection<'a> {
    pub(crate) selected: &'a ResolvedMsSelection,
    pub(crate) has_library_registration: bool,
    pub(crate) legacy_corruption: bool,
    pub(crate) legacy_libraries: &'a [crate::execute::LibraryApplyRegistration],
}

pub(crate) fn plan_with_selection(
    ms: &MeasurementSet,
    request: &ApplyPlanRequest,
    preparation_error: Option<ApplyPreparationError<'_>>,
    registered: Option<RegisteredApplySelection<'_>>,
) -> Result<PlannedApplication, ApplyPlanError> {
    let selection_started_at = Instant::now();
    let resolved;
    let resolved_selection = if let Some(registration) = registered {
        registration.selected
    } else {
        resolved = resolve_casa_calibration_selection(ms, &request.selection).map_err(|error| {
            ApplyPlanError::Selection {
                source: match error {
                    casa_ms::MsSelectionError::Domain(source) => source,
                    other => MsError::InvalidInput(other.to_string()),
                },
            }
        })?;
        &resolved
    };
    let selected_row_indices = resolved_selection.row_indices().collect::<Vec<_>>();
    let selection_ns = selection_started_at.elapsed().as_nanos() as u64;
    let (plan, mut timings, legacy_scans) = plan_selected_rows(
        ms,
        request,
        preparation_error,
        registered,
        &selected_row_indices,
        ApplyChannelInput::Ranges(&resolved_selection.channel_selections),
    )?;
    timings.selection_ns = selection_ns;
    Ok((plan, timings, legacy_scans))
}

pub(crate) fn plan_transform_rows(
    ms: &MeasurementSet,
    request: &ApplyPlanRequest,
    rows: &[usize],
    channels: &BTreeMap<i32, &[usize]>,
) -> Result<ApplyPlan, ApplyPlanError> {
    plan_selected_rows(
        ms,
        request,
        None,
        None,
        rows,
        ApplyChannelInput::Indices(channels),
    )
    .map(|(plan, _, _)| plan)
}

enum ApplyChannelInput<'a> {
    Ranges(&'a BTreeMap<i32, ChannelSelection>),
    Indices(&'a BTreeMap<i32, &'a [usize]>),
}

fn plan_selected_rows(
    ms: &MeasurementSet,
    request: &ApplyPlanRequest,
    preparation_error: Option<ApplyPreparationError<'_>>,
    registered: Option<RegisteredApplySelection<'_>>,
    selected_row_indices: &[usize],
    requested_channels: ApplyChannelInput<'_>,
) -> Result<PlannedApplication, ApplyPlanError> {
    let requires_corrected_data_column = request.apply_mode.writes_corrected_data()
        && match ms.data_column(VisibilityDataColumn::CorrectedData) {
            Ok(_) => false,
            Err(MsError::ColumnNotPresent(_)) => true,
            Err(other) => return Err(ApplyPlanError::Selection { source: other }),
        };
    if selected_row_indices.is_empty() {
        return Err(ApplyPlanError::Selection {
            source: MsError::InvalidInput("selection produced no rows".into()),
        });
    }
    let selected_rows_started_at = Instant::now();
    let selected_rows = build_selected_rows(ms, selected_row_indices)?;
    let selected_rows_ns = selected_rows_started_at.elapsed().as_nanos() as u64;

    let selected_field_ids = unique_i32(selected_rows.iter().map(|row| row.field_id));
    let selected_data_desc_ids = unique_i32(selected_rows.iter().map(|row| row.data_desc_id));
    let selected_data_spw_ids = unique_i32(selected_rows.iter().map(|row| row.data_spw_id));

    let ms_spw_started_at = Instant::now();
    let measurement_set_spectral_windows = load_ms_spectral_windows(ms, &selected_data_spw_ids)?;
    let mut channel_selections = BTreeMap::new();
    for spw in &measurement_set_spectral_windows {
        let indices = match requested_channels {
            ApplyChannelInput::Ranges(ranges) => ranges
                .get(&spw.spw_id)
                .map(|selection| selection.indices(spw.channel_frequencies_hz.len()))
                .transpose()
                .map_err(|source| ApplyPlanError::Selection { source })?,
            ApplyChannelInput::Indices(channels) => {
                channels.get(&spw.spw_id).map(|indices| indices.to_vec())
            }
        };
        if let Some(mut indices) = indices {
            indices.sort_unstable();
            indices.dedup();
            if indices.is_empty() {
                return Err(ApplyPlanError::Selection {
                    source: MsError::InvalidInput(format!(
                        "SPW {} has no selected channels",
                        spw.spw_id
                    )),
                });
            }
            if indices.len() != spw.channel_frequencies_hz.len() {
                let frequencies_hz = indices
                    .iter()
                    .map(|&channel| spw.channel_frequencies_hz[channel])
                    .collect();
                channel_selections.insert(
                    spw.spw_id,
                    ApplyChannelSelection {
                        indices,
                        frequencies_hz,
                    },
                );
            }
        }
    }
    let measurement_set_spectral_windows_ns = ms_spw_started_at.elapsed().as_nanos() as u64;

    if let Some(ApplyPreparationError::Callibrary(message)) = preparation_error {
        return Err(ApplyPlanError::Callibrary {
            message: message.to_string(),
        });
    }
    let calibration_tables_started_at = Instant::now();
    let mut calibration_tables = request
        .calibration_tables
        .iter()
        .map(|spec| {
            build_table_plan(
                ms,
                spec,
                &selected_rows,
                registered.is_some_and(|registration| registration.legacy_corruption),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(ApplyPreparationError::Argument(error)) = preparation_error {
        return Err(ApplyPlanError::Argument(error.clone()));
    }
    if calibration_tables.is_empty()
        && !request.parang
        && !registered.is_some_and(|registration| registration.has_library_registration)
    {
        return Err(ApplyPlanError::MissingCalibrationTables);
    }
    // The direct tool checks overlaps within each newly registered library.
    // Separate registrations of the same table are separate chain terms.
    if registered.is_none() {
        library::validate_overlaps(&calibration_tables)?;
    }
    let legacy_scans = registered.map_or_else(LegacyScanOverrides::new, |registration| {
        library::choose_legacy_slices(
            &mut calibration_tables,
            &selected_rows,
            registration.legacy_libraries,
        )
    });
    let calibration_tables = calibration_tables
        .into_iter()
        .map(|(table, _)| table)
        .collect();
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
            channel_selections,
            calibration_tables,
        },
        ApplyPlanTimings {
            selection_ns: 0,
            selected_rows_ns,
            measurement_set_spectral_windows_ns,
            calibration_table_plans_ns,
        },
        legacy_scans,
    ))
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
    legacy_corruption: bool,
) -> Result<library::PlannedTable, ApplyPlanError> {
    let summary = summarize_table(&spec.path).map_err(|source| {
        ApplyPlanError::SummarizeCalibrationTable {
            path: spec.path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    let is_bpoly = summary.table_subtype == "BPOLY";
    let is_gspline = summary.table_subtype == "GSPLINE";
    if !is_gspline && spec.interp == ApplyInterpolationMode::Invalid && spec.callibrary.is_none() {
        return Err(ApplyPlanError::UnsupportedCalibrationTable {
            path: spec.path.display().to_string(),
            reason: "unknown or deprecated CASA interpolation type".to_string(),
        });
    }
    let windows = ms
        .spectral_window()
        .map_err(|source| ApplyPlanError::Selection { source })?
        .row_count();
    if !is_gspline && spec.spwmap.len() > windows && spec.callibrary.is_none() {
        return Err(ApplyPlanError::UnsupportedCalibrationTable {
            path: spec.path.display().to_string(),
            reason: format!(
                "spwmap has {} entries but the MeasurementSet has {windows} spectral windows",
                spec.spwmap.len()
            ),
        });
    }
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

    let applicable_row_indices =
        library::selected_rows(ms, spec, selected_rows, legacy_corruption)?;
    let applicable_rows = selected_rows
        .iter()
        .filter(|row| {
            spec.apply_to.matches(row)
                && spec.callibrary.as_ref().is_none_or(|library| {
                    library
                        .observation_map
                        .covers(row.calibration_observation_id)
                        && library.scan_map.covers(row.scan_number)
                        && (library.field_map.is_empty()
                            || usize::try_from(row.field_id)
                                .is_ok_and(|id| id < library.field_map.len()))
                })
                && applicable_row_indices
                    .as_ref()
                    .is_none_or(|indices| indices.binary_search(&row.row_index).is_ok())
        })
        .collect::<Vec<_>>();
    if !is_gspline && !applicable_rows.is_empty() && spec.interp == ApplyInterpolationMode::Invalid
    {
        return Err(ApplyPlanError::Callibrary {
            message: "unknown library time interpolation".into(),
        });
    }
    let applicable_field_ids = unique_i32(applicable_rows.iter().map(|row| row.field_id));
    let applicable_data_spw_ids = unique_i32(applicable_rows.iter().map(|row| row.data_spw_id));

    let legacy_polynomial = is_bpoly || is_gspline;
    let (resolved_gainfields, resolved_nearest_gainfields) = if legacy_polynomial {
        // BPOLY is sampled with wildcard FIELD_ID=-1. GSPLINE matches the
        // data field directly. Neither uses the caller's field mapping.
        if spec.callibrary.is_some() {
            return Err(ApplyPlanError::Callibrary {
                message: format!(
                    "{} is not supported for application by cal library in CASA",
                    summary.table_subtype
                ),
            });
        }
        (Vec::new(), Vec::new())
    } else {
        let all_fields;
        let fields = if spec.callibrary.is_some()
            && matches!(spec.gainfield, Some(GainFieldSelector::Nearest))
        {
            all_fields = (0..ms
                .field()
                .map_err(|source| ApplyPlanError::Selection { source })?
                .row_count() as i32)
                .collect::<Vec<_>>();
            &all_fields
        } else {
            &applicable_field_ids
        };
        resolve_gainfield(ms, spec, &summary, fields)?
    };
    let available_calibration_spw_ids = if is_gspline {
        applicable_data_spw_ids.clone()
    } else if is_bpoly {
        load_bpoly_spectral_window_ids(&summary.path)?
    } else {
        fields::selected_spectral_windows(&summary, &resolved_gainfields)?
    };
    let calibration_window_count = if legacy_polynomial {
        windows
    } else {
        open_summary_subtable(&summary, "SPECTRAL_WINDOW")?.row_count()
    };
    let spw_mapping = if is_gspline {
        // The legacy solver evaluates at the current data window frequency.
        applicable_data_spw_ids
            .iter()
            .map(|&id| ApplySpwMapping {
                data_spw_id: id,
                calibration_spw_id: id,
            })
            .collect()
    } else {
        windows::resolve_calibration_spw_mapping(
            spec,
            &summary.table_subtype,
            &available_calibration_spw_ids,
            calibration_window_count,
            &applicable_data_spw_ids,
        )?
    };
    let available_mappings = spw_mapping
        .iter()
        .filter(|mapping| available_calibration_spw_ids.contains(&mapping.calibration_spw_id))
        .cloned()
        .collect::<Vec<_>>();
    let calibration_spectral_windows = if legacy_polynomial {
        let mapped_cal_spw_ids = unique_i32(
            available_mappings
                .iter()
                .map(|mapping| mapping.calibration_spw_id),
        );
        load_ms_spectral_windows(ms, &mapped_cal_spw_ids)?
    } else {
        load_caltable_spectral_windows(&summary, &available_mappings)?
    };

    let mut plan = ApplyCalibrationTablePlan {
        spec: spec.clone(),
        unavailable_row_indices: Vec::new(),
        applicable_row_indices,
        applicable_selected_row_count: applicable_rows.len(),
        summary,
        resolved_gainfields,
        resolved_nearest_gainfields,
        spw_mapping,
        calibration_spectral_windows,
        interp: if applicable_rows.is_empty() && spec.callibrary.is_some() {
            ApplyInterpolationMode::Linear
        } else if spec.callibrary.is_some() {
            spec.interp.for_callibrary()
        } else {
            spec.interp
        },
        calwt: spec.calwt,
    };
    let keys = library::arrange_contexts(ms, &mut plan, selected_rows)?;
    Ok((plan, keys))
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

    use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
    use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
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
    fn spw_mapping_covers_broadcast_prefix_auto_and_full_list_validation() {
        let mut spec = ApplyCalibrationTableSpec::new("/tmp/fake.gcal");
        for (map, expected) in [
            (vec![0], vec![0, 0, 0]),
            (vec![1, 0], vec![1, 0, 2]),
            (vec![-999], vec![1, 1, 1]),
            (vec![-1], vec![0, 1, 2]),
        ] {
            spec.spwmap = map;
            let result = resolve_spw_mapping(&spec, &[1, 2], 3, &[0, 1, 2]).unwrap();
            assert_eq!(
                result
                    .iter()
                    .map(|m| m.calibration_spw_id)
                    .collect::<Vec<_>>(),
                expected
            );
        }
        // Validation covers unselected supplied entries, but a metadata window
        // without rows remains a valid mapping for skip/strict handling.
        for map in [vec![0, 9], vec![0, -1]] {
            spec.spwmap = map;
            assert!(resolve_spw_mapping(&spec, &[1], 3, &[0]).is_err());
        }
        spec.spwmap = vec![2];
        assert_eq!(
            resolve_spw_mapping(&spec, &[1], 3, &[0]).unwrap()[0].calibration_spw_id,
            2
        );
    }

    #[test]
    fn cal_desc_and_summary_helpers_round_trip_subtables() {
        let temp = tempdir().unwrap();
        let cal_desc_root = temp.path().join("bpoly.cal");
        std::fs::create_dir_all(&cal_desc_root).unwrap();
        let cal_desc_schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "SPECTRAL_WINDOW_ID",
            casa_core::PrimitiveType::Int32,
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
            casa_core::PrimitiveType::Float32,
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
                casa_core::PrimitiveType::String,
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
                ColumnSchema::scalar("NUM_CHAN", casa_core::PrimitiveType::Int32),
                ColumnSchema::scalar("REF_FREQUENCY", casa_core::PrimitiveType::Float64),
                ColumnSchema::array_variable(
                    "CHAN_FREQ",
                    casa_core::PrimitiveType::Float64,
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
            scalar_kind(&ScalarValue::Complex64(casa_core::Complex64::new(1.0, 2.0))),
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
