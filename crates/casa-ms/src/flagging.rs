// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native CASA-style flagging primitives for MeasurementSets.
//!
//! CASA implements `flagdata` through the agentflagger framework. This module
//! follows the same user-facing task families that matter in the VLA flagging
//! tutorial: manual edits, clip-zero edits, quack, TFCrop, RFlag, extension,
//! summaries, and `flagmanager` flag-version snapshots. The automatic RFI
//! modes are native robust-statistics implementations of the CASA algorithm
//! families rather than a binding to CASA C++.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, Table, TableError, TableOptions, TableSchema,
    TableWriteLock,
};
#[cfg(unix)]
use casa_tables::{LockMode, LockOptions};
use casa_types::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, Ix2, IxDyn};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::ms::MeasurementSet;
use crate::schema::main_table::{OptionalMainColumn, VisibilityDataColumn};
use crate::selection::syntax::{ChannelSelection, parse_spw_selector};
use crate::selection::{
    MsSelection, ResolvedMsSelectionRow, TimeDefaults, TimeSelection, TimeSelectorDefaults,
    flagdata_c_string, resolve_flagdata_selection_with_system_budget,
    resolve_row_indices_with_system_budget, resolve_time_selector,
};
use crate::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetMutationBatch,
    MeasurementSetMutationColumnBatch, MeasurementSetMutationColumnValues,
    MeasurementSetWriteColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, MsError, MsReadPlan, MsSelectionIoBudget,
    maximum_visibility_cell_elements,
};
use casa_numerics::{solve_symmetric_ldlt_casacore_with_precision, solve_weighted_least_squares};

mod antint;
mod calibration;
mod clip;
mod clip_average;
mod clip_columns;
mod clip_models;
mod clip_time;
mod command;
mod correlation;
mod elevation;
mod extend;
mod geometry;
mod initial_version;
mod program;
mod shadow;
mod summary;

pub use antint::AntennaIntegrationOptions;
use clip::{ClipOptions, apply_clip_array};
use clip_average::{ChannelBins, bin_members, check_channel_bins};
use clip_columns::ClipColumns;
use clip_time::{TimeClip, time_bin_seconds};
pub use command::{append_flag_command, append_flag_commands};
use correlation::{ClipMetric, ClipProjection, CorrelationSelection, TfcropCorrelations};
pub use initial_version::save_initial_flag_version;
pub use program::{flagdata_ms_program, validate_calibration_flag_averaging};
pub use shadow::{ShadowAntenna, ShadowOptions};
mod version_backup;
mod version_storage;
pub use clip_models::{FlagDataModelFactory, FlagDataModelPredictor};
pub use summary::{FlagCounts, FlagSummaryBreakdown, FlagSummaryOptions};
pub use version_backup::{
    save_next_casa_flag_backup, save_next_flag_backup, save_next_partition_flag_backup,
};
pub use version_storage::{
    restore_flag_version, save_agent_disk_flag_version, save_disk_flag_version, save_flag_version,
};

const FLAG_COLUMN: &str = "FLAG";
const FLAG_ROW_COLUMN: &str = "FLAG_ROW";
const FLAG_VERSION_LIST: &str = "FLAG_VERSION_LIST";

fn is_calibration_table(ms: &MeasurementSet) -> bool {
    ms.main_table().info().table_type == "Calibration"
}

/// Input visibility column used by automatic flagging modes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlagDataColumn {
    /// MAIN.DATA.
    #[default]
    Data,
    /// MAIN.CORRECTED_DATA.
    CorrectedData,
    /// MAIN.FLOAT_DATA.
    FloatData,
    /// MAIN.MODEL_DATA.
    ModelData,
    /// Corrected visibilities minus the model.
    Residual,
    /// Observed visibilities minus the model.
    ResidualData,
    /// The stored weight spectrum or channel-repeated scalar weights.
    Weight,
    /// MAIN.WEIGHT_SPECTRUM.
    WeightSpectrum,
    /// Complex calibration parameters.
    Cparam,
    /// Real calibration parameters.
    Fparam,
    /// Calibration parameter uncertainties.
    ParamErr,
    /// Calibration solution signal-to-noise.
    Snr,
}

impl FlagDataColumn {
    /// The physical column name used by the flagging data source.
    pub fn name(self) -> &'static str {
        match self {
            Self::Data => VisibilityDataColumn::Data.name(),
            Self::CorrectedData => VisibilityDataColumn::CorrectedData.name(),
            Self::FloatData => OptionalMainColumn::FloatData.name(),
            Self::ModelData => VisibilityDataColumn::ModelData.name(),
            Self::Residual => "RESIDUAL",
            Self::ResidualData => "RESIDUAL_DATA",
            Self::Weight => "WEIGHT",
            Self::WeightSpectrum => OptionalMainColumn::WeightSpectrum.name(),
            Self::Cparam => "CPARAM",
            Self::Fparam => "FPARAM",
            Self::ParamErr => "PARAMERR",
            Self::Snr => "SNR",
        }
    }

    fn is_real(self) -> bool {
        matches!(
            self,
            Self::FloatData
                | Self::Weight
                | Self::WeightSpectrum
                | Self::Fparam
                | Self::ParamErr
                | Self::Snr
        )
    }
}

/// Native `flagdata` mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlagDataMode {
    /// Manual selected-row/sample flagging.
    Manual,
    /// Clip visibility samples or signed floating-point values.
    Clip,
    /// CASA quack-style scan-edge flagging.
    Quack,
    /// Flag selected baselines outside the antenna elevation limits.
    Elevation,
    /// Flag selected baselines involving a shadowed antenna.
    Shadow,
    /// Propagate flags from a reference antenna to selected integrations.
    Antint,
    /// CASA TFCrop-family robust time/frequency outlier flagging.
    Tfcrop,
    /// CASA RFlag-family two-pass robust time/frequency flagging.
    Rflag,
    /// Extend existing flags in polarization, time, or frequency.
    Extend,
    /// Summarize flags without mutating the MS.
    Summary,
}

/// Flag or unflag action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlagDataAction {
    /// Set selected samples to flagged.
    Flag,
    /// Set selected samples to unflagged.
    Unflag,
}

impl FlagDataAction {
    fn value(self) -> bool {
        matches!(self, Self::Flag)
    }
}

/// Quack scan-edge mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QuackMode {
    /// Flag the first interval in each scan.
    #[default]
    Beg,
    /// Flag the last interval in each scan.
    End,
    /// Flag before the end interval, CASA's `end` mode.
    BeforeEnd,
    /// Flag after the beginning interval.
    Tail,
    /// Accepted CASA options that leave the quack agent inactive.
    Disabled,
}

/// Request for native `flagdata`.
#[derive(Debug, Clone)]
pub struct FlagDataRequest {
    /// Structured row selection.
    pub selection: MsSelection,
    /// Interpret row selectors using the public CASA flagdata task grammar.
    pub casa_row_selection: bool,
    /// CASA-style SPW/channel selector.
    pub spw: Option<String>,
    /// CASA agent selection of stored correlation planes.
    pub correlation: Option<String>,
    /// Mode to execute.
    pub mode: FlagDataMode,
    /// Manual action.
    pub action: FlagDataAction,
    /// Set manual flags only on rows whose antenna IDs are equal.
    pub autocorr: bool,
    /// Data column for automatic modes.
    pub data_column: FlagDataColumn,
    /// Whether a requested display has a scalar data-column name.
    /// CASA checks this after selection and any apply backup.
    pub display_column_is_scalar: bool,
    /// Create a `flagdata_N` backup before mutating flags.
    pub flagbackup: bool,
    /// Optional breakdowns for summary mode.
    pub summary_options: FlagSummaryOptions,
    /// Clip exact zeros in [`FlagDataMode::Clip`].
    pub clipzeros: bool,
    /// Optional lower and upper values for the clip range.
    pub clipminmax: Option<[f64; 2]>,
    /// Flag outside or inside the range. An accepted vector disables the clip agent.
    pub clipoutside: Option<bool>,
    /// Average selected channels before clipping or public MS TFCrop. A singleton
    /// width is used for all spectral windows. Zero passes through and one takes
    /// the whole window.
    pub clip_channel_bins: Option<Vec<u32>>,
    /// Average integrations on each baseline before clipping or public MS TFCrop.
    pub clip_time_bin: Option<String>,
    /// Positive visibility-iterator chunk interval, in seconds.
    pub time_chunk: Option<f64>,
    /// Omit scan boundaries when grouping public TFCrop and extension rows.
    pub combinescans: bool,
    /// Quack interval in seconds.
    pub quackinterval: f64,
    /// Quack scan edge.
    pub quackmode: QuackMode,
    /// Start the interval from the first or last time with unflagged data.
    pub quackincrement: bool,
    /// Inclusive lower and upper antenna elevations, in degrees.
    pub elevation_limits_deg: [f64; 2],
    /// Dish shadowing parameters.
    pub shadow: ShadowOptions,
    /// Reference antenna integration flagging parameters.
    pub antint: AntennaIntegrationOptions,
    /// TFCrop time cutoff in robust sigma units.
    pub timecutoff: f64,
    /// TFCrop frequency cutoff in robust sigma units.
    pub freqcutoff: f64,
    /// Fit controls for the TFCrop agent. An invalid CASA configuration
    /// can leave this agent inactive while running post-extension.
    pub tfcrop: Option<TfcropOptions>,
    /// RFlag time threshold scale, applied to a given threshold and a
    /// computed one alike, as CASA applies it.
    pub timedevscale: f64,
    /// RFlag spectral threshold scale, applied to a given threshold and a
    /// computed one alike.
    pub freqdevscale: f64,
    /// RFlag maximum acceptable spectral standard deviation before flagging
    /// the full row spectrum.
    pub spectralmax: f64,
    /// RFlag minimum acceptable spectral standard deviation before flagging
    /// the full row spectrum.
    pub spectralmin: f64,
    /// Maximum number of time samples in an RFlag variance window.
    /// A rejected agent record disables RFlag and leaves extension available.
    pub rflag_window: Option<usize>,
    /// Direct calibration agents can calculate RFlag thresholds in a run
    /// that writes other agents' flags, or apply thresholds in a dry run.
    pub rflag_calculate: Option<bool>,
    /// Boolean-vector switch to reject after selecting rows and validating the column.
    pub automatic_average_vector: Option<&'static str>,
    /// Optional RFlag time threshold, before [`Self::timedevscale`]. `None`
    /// computes one from the data.
    pub timedev: Option<f64>,
    /// Optional RFlag spectral threshold, before [`Self::freqdevscale`].
    /// `None` computes one from the data.
    pub freqdev: Option<f64>,
    /// Explicit RFlag time thresholds keyed by field and spectral window.
    pub timedev_by_field_spw: BTreeMap<(i32, i32), f64>,
    /// Explicit RFlag frequency thresholds keyed by field and spectral window.
    pub freqdev_by_field_spw: BTreeMap<(i32, i32), f64>,
    /// Run CASA's automatic post-extension agent after TFCrop/RFlag.
    pub extendflags: bool,
    /// Extend flags across correlations/polarizations.
    pub extendpols: bool,
    /// Whether the public CASA values allow the extension agent to start.
    pub extension_enabled: bool,
    /// Flag samples with at least five flagged neighbors.
    pub growaround: bool,
    /// Extend existing flags one integration in each time direction.
    pub flagneartime: bool,
    /// Extend existing flags one selected channel in each frequency direction.
    pub flagnearfreq: bool,
    /// If > 0, flag full channel time columns whose flagged percentage meets
    /// this threshold.
    pub growtime: f64,
    /// If > 0, flag full row spectra whose flagged percentage meets this
    /// threshold.
    pub growfreq: f64,
}

impl Default for FlagDataRequest {
    fn default() -> Self {
        Self {
            selection: MsSelection::new(),
            casa_row_selection: false,
            spw: None,
            correlation: None,
            mode: FlagDataMode::Manual,
            action: FlagDataAction::Flag,
            autocorr: false,
            data_column: FlagDataColumn::Data,
            display_column_is_scalar: true,
            flagbackup: true,
            summary_options: FlagSummaryOptions::default(),
            clipzeros: false,
            clipminmax: None,
            clipoutside: Some(true),
            clip_channel_bins: None,
            clip_time_bin: None,
            time_chunk: None,
            combinescans: false,
            quackinterval: 0.0,
            quackmode: QuackMode::Beg,
            quackincrement: false,
            elevation_limits_deg: [0.0, 90.0],
            shadow: ShadowOptions::default(),
            antint: AntennaIntegrationOptions::default(),
            timecutoff: 4.0,
            freqcutoff: 3.0,
            tfcrop: Some(TfcropOptions::default()),
            timedevscale: 5.0,
            freqdevscale: 5.0,
            spectralmax: 1.0e6,
            spectralmin: 0.0,
            rflag_window: Some(3),
            rflag_calculate: None,
            automatic_average_vector: None,
            timedev: None,
            freqdev: None,
            timedev_by_field_spw: BTreeMap::new(),
            freqdev_by_field_spw: BTreeMap::new(),
            extendflags: true,
            extendpols: false,
            extension_enabled: true,
            growaround: false,
            flagneartime: false,
            flagnearfreq: false,
            growtime: 0.0,
            growfreq: 0.0,
        }
    }
}

/// RFlag thresholds represented by field ID, spectral-window ID and value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RFlagThresholdReport {
    /// Time deviation thresholds.
    pub timedev: Vec<[f64; 3]>,
    /// Frequency deviation thresholds.
    pub freqdev: Vec<[f64; 3]>,
}

/// Report returned by native `flagdata`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FlagDataReport {
    /// Mode executed.
    pub mode: FlagDataMode,
    /// Number of selected MAIN rows.
    pub selected_rows: usize,
    /// Number of changed MAIN rows.
    pub changed_rows: usize,
    /// Number of changed correlation/channel samples.
    pub changed_samples: usize,
    /// Number of flagged samples in the selected rows after execution.
    pub flagged_samples: usize,
    /// Number of total samples in the selected rows after execution.
    pub total_samples: usize,
    /// Auto-created backup version.
    pub backup_version: Option<String>,
    /// RFlag time threshold actually used.
    pub timedev: Option<f64>,
    /// RFlag spectral threshold actually used.
    pub freqdev: Option<f64>,
    /// Field/window thresholds for a calibration RFlag calculation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rflag_report: Option<RFlagThresholdReport>,
    /// CASA's summary of the selection's flags, in summary mode (#931).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<FlagSummaryBreakdown>,
}

/// Flag-version merge behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlagMerge {
    /// Replace destination flags.
    #[default]
    Replace,
    /// Logical OR source and destination flags.
    Or,
    /// Logical AND source and destination flags.
    And,
}

/// One `flagmanager` list entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FlagVersionEntry {
    /// Version name.
    pub name: String,
    /// Version comment.
    pub comment: String,
}

/// Native flagging errors.
#[derive(Debug, Error)]
pub enum FlaggingError {
    /// Opening an MS failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path being opened.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// Saving an MS failed.
    #[error("failed to save MeasurementSet {path}: {source}")]
    SaveMeasurementSet {
        /// Path being saved.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// A flag mutation failed.
    #[error("failed to mutate flags for {path}: {reason}")]
    MutateFlags {
        /// Path being mutated.
        path: String,
        /// Error context.
        reason: String,
    },

    /// The correlation selector resolved to no polarization setup for a summary.
    #[error("no flagdata summary report for the selected correlations")]
    MissingCorrelationSummary,

    /// A calibration-table row or channel selector was refused.
    #[error("{0}")]
    CalibrationSelection(String),
    /// A calibration program mixes incompatible agent configurations.
    #[error("{0}")]
    CalibrationAgentConfiguration(String),

    /// A mode cannot configure its agent for a calibration table.
    #[error("Failed to parse parameters for mode {0}")]
    UnsupportedCalibrationAgent(&'static str),
    /// The upstream antenna integration agent aborts on calibration tables.
    #[error("antint requires a MeasurementSet with visibility rows")]
    CalibrationAntint,

    /// A requested clipping column is missing from the MeasurementSet.
    #[error("Failed to parse parameters for mode clip")]
    MissingClipColumn,
    /// A requested TFCrop visibility column is missing from the MeasurementSet.
    #[error("Failed to parse parameters for mode tfcrop")]
    MissingTfcropColumn,
    /// A requested RFlag visibility column is missing from the MeasurementSet.
    #[error("Failed to parse parameters for mode rflag")]
    MissingRflagColumn,
    /// An active explicit RFlag cutoff has a zero or negative scale.
    #[error("{0} must be positive when its threshold is positive")]
    InvalidRFlagScale(&'static str),
    /// The automatic calibration agent expects a scalar Boolean.
    #[error("RecordInterface::asBool - invalid data type")]
    InvalidAutomaticAverageVector(&'static str),
    /// The automatic visibility operator did not name a polarization product.
    #[error(
        "Unknown polarization requested, ({0}) supported types are: XX,YY,XY,YX,RR,LL,RL,LR,I,Q,U,V"
    )]
    UnknownTfcropProduct(String),
    /// A display was requested with a vector of data-column names.
    #[error("'list' object has no attribute 'upper'")]
    DisplayColumnVector,
    /// The channel averaging widths do not match the spectral windows.
    #[error("Error parsing ChannelAverageTVI configuration")]
    InvalidClipChannelBins,

    /// Flag-version management failed.
    #[error("failed to manage flag version for {path}: {reason}")]
    FlagVersion {
        /// MeasurementSet path.
        path: String,
        /// Error context.
        reason: String,
    },
    /// The requested version table does not exist.
    #[error("Flag version {version} does not exist")]
    MissingFlagVersion {
        /// Requested flag version name.
        version: String,
    },
    /// A public flagmanager rename cannot proceed with these names.
    #[error("{message}")]
    FlagVersionRenameArgument {
        /// Which name the caller needs to change.
        parameter: &'static str,
        /// CASA's explanation of the invalid name.
        message: String,
    },
    /// A flag version cannot be deleted or renamed while its table is
    /// open, in this process or another, as casacore's `Table::deleteTable`
    /// and `Table::rename` refuse (`TableUtil::canDeleteTable`,
    /// `Table::isMultiUsed`).
    #[error("flag version {version:?} of {path} is open {holder}: {table}")]
    FlagVersionInUse {
        /// MeasurementSet path.
        path: String,
        /// The version's name.
        version: String,
        /// The version's table.
        table: String,
        /// Where the table is open.
        holder: casa_tables::TableHolder,
    },
}

/// Run native `flagdata` on an opened MeasurementSet.
pub fn flagdata(
    ms: &mut MeasurementSet,
    request: &FlagDataRequest,
) -> Result<FlagDataReport, FlaggingError> {
    flagdata_with_persistence(ms, request, true, None)
}

fn flagdata_with_persistence(
    ms: &mut MeasurementSet,
    request: &FlagDataRequest,
    persist: bool,
    model_factory: Option<FlagDataModelFactory>,
) -> Result<FlagDataReport, FlaggingError> {
    if is_calibration_table(ms) {
        return calibration::run(ms, request, persist);
    }
    program::run_ms_single(ms, request, persist, model_factory)
}

fn persist_changed_flags(
    ms: &mut MeasurementSet,
    changes: &ChangeSet,
    ms_path: &str,
) -> Result<(), FlaggingError> {
    let mut maximum_flag_bytes = 0usize;
    for &row in &changes.changed_row_indices {
        maximum_flag_bytes = maximum_flag_bytes.max(clone_flag_matrix(ms, row)?.len());
    }
    let mut columns = vec![MeasurementSetWriteColumnPlan {
        name: FLAG_COLUMN.to_string(),
        bytes_per_row: maximum_flag_bytes,
        mode: MeasurementSetColumnWriteMode::Replace,
        storage_manager: MeasurementSetColumnStorage::Persisted,
        tile_shape: None,
        create_source_column: None,
    }];
    if changes.changed_flag_row_rows > 0 {
        columns.push(MeasurementSetWriteColumnPlan {
            name: FLAG_ROW_COLUMN.to_string(),
            bytes_per_row: 1,
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        });
    }
    let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(|error| {
        FlaggingError::MutateFlags {
            path: ms_path.to_owned(),
            reason: format!("plan changed MAIN flag rows: {error}"),
        }
    })?;
    let include_flag_row = changes.changed_flag_row_rows > 0;
    let write_plan = MeasurementSetWritePlan::selected_row_mutation(
        changes.changed_row_indices.clone(),
        columns,
        resources,
    )
    .map_err(|error| FlaggingError::MutateFlags {
        path: ms_path.to_owned(),
        reason: format!("plan changed MAIN flag rows: {error}"),
    })?;
    let batch_rows = write_plan.batch_rows();
    let mut write_session = MeasurementSetWriteSession::start_selected_row_mutation(ms, write_plan)
        .map_err(|error| FlaggingError::MutateFlags {
            path: ms_path.to_owned(),
            reason: format!("start changed MAIN flag rows: {error}"),
        })?;
    for rows in changes.changed_row_indices.chunks(batch_rows) {
        let flag_values = rows
            .iter()
            .map(|&row| clone_flag_matrix(ms, row).map(ArrayValue::Bool))
            .collect::<Result<Vec<_>, _>>()?;
        let mut batch_columns = vec![MeasurementSetMutationColumnBatch {
            name: FLAG_COLUMN.to_string(),
            values: MeasurementSetMutationColumnValues::Arrays(flag_values),
        }];
        if include_flag_row {
            let values = load_bool_cells(ms.main_table(), rows, FLAG_ROW_COLUMN, ms_path)?
                .into_iter()
                .map(ScalarValue::Bool)
                .collect();
            batch_columns.push(MeasurementSetMutationColumnBatch {
                name: FLAG_ROW_COLUMN.to_string(),
                values: MeasurementSetMutationColumnValues::Scalars(values),
            });
        }
        write_session
            .write_mutation_batch(
                ms,
                MeasurementSetMutationBatch {
                    row_indices: rows.to_vec(),
                    columns: batch_columns,
                },
            )
            .map_err(|error| FlaggingError::MutateFlags {
                path: ms_path.to_owned(),
                reason: format!("write changed MAIN flag rows: {error}"),
            })?;
    }
    write_session
        .finish_mutation()
        .map_err(|error| FlaggingError::MutateFlags {
            path: ms_path.to_owned(),
            reason: format!("save changed MAIN flag rows: {error}"),
        })?;
    Ok(())
}

/// Open an MS, run native `flagdata`, and save the changed MAIN table.
pub fn flagdata_path(
    ms_path: impl AsRef<Path>,
    request: &FlagDataRequest,
) -> Result<FlagDataReport, FlaggingError> {
    flagdata_path_with_model(ms_path, request, true, None)
}

/// Calculate candidate flags from a privately opened MS and discard the
/// in-memory edits after building the report.
pub fn calculate_flagdata_path(
    ms_path: impl AsRef<Path>,
    request: &FlagDataRequest,
) -> Result<FlagDataReport, FlaggingError> {
    flagdata_path_with_model(ms_path, request, false, None)
}

/// Run the task using a model predictor supplied by the executable. The factory
/// is called only when clipping needs an FTMachine recipe, after any requested
/// backup and after row selection. `persist=false` leaves the input unchanged.
pub fn flagdata_path_with_model(
    path: impl AsRef<Path>,
    request: &FlagDataRequest,
    persist: bool,
    model_factory: Option<FlagDataModelFactory>,
) -> Result<FlagDataReport, FlaggingError> {
    let path = path.as_ref();
    tracing::info!(path = %path.display(), "opening MeasurementSet for flagdata");
    let mut ms =
        MeasurementSet::open(path).map_err(|source| FlaggingError::OpenMeasurementSet {
            path: path.display().to_string(),
            source,
        })?;
    flagdata_with_persistence(&mut ms, request, persist, model_factory)
}

/// Execute a sequence of calibration flagging agents on one table view.
/// Earlier commands are visible to subsequent agents and summaries. A
/// calculate-only run discards its staged flags, and an apply run saves the
/// combined changed rows after all commands have completed. An entry is
/// absent when CASA cannot configure that agent and continues the program.
/// With multiple commands, selection is local to each agent and a time
/// range gates the calibration buffers at their first integration.
pub fn flagdata_calibration_program(
    table: MeasurementSet,
    requests: &[FlagDataRequest],
    write_flags: bool,
    local_selection: bool,
) -> Result<Vec<Option<FlagDataReport>>, FlaggingError> {
    run_calibration_flag_program(
        table,
        requests,
        calibration::ProgramOptions {
            write_flags,
            sequential: true,
            local_selection,
            selected_rows: None,
        },
    )
}

/// Resolve global calibration-tool selection to stored rows. Channel
/// ranges in the global selection constrain spectral-window membership.
/// An agent selects its own channels when it executes on those rows.
pub fn flagdata_calibration_agent_rows(
    table: &MeasurementSet,
    selection: &MsSelection,
) -> Result<Vec<usize>, FlaggingError> {
    ensure_calibration_program(table)?;
    calibration::selected_rows(table, selection)
}

/// Run initialized calibration agents against the tool's global row view.
/// Combined runs stage unflag agents before other agents. Returned report
/// slots keep the configuration order for both execution modes.
pub fn flagdata_calibration_agents(
    table: MeasurementSet,
    requests: &[FlagDataRequest],
    selected_rows: &[usize],
    write_flags: bool,
    sequential: bool,
) -> Result<Vec<Option<FlagDataReport>>, FlaggingError> {
    run_calibration_flag_program(
        table,
        requests,
        calibration::ProgramOptions {
            write_flags,
            sequential,
            local_selection: true,
            selected_rows: Some(selected_rows),
        },
    )
}

fn run_calibration_flag_program(
    mut table: MeasurementSet,
    requests: &[FlagDataRequest],
    options: calibration::ProgramOptions<'_>,
) -> Result<Vec<Option<FlagDataReport>>, FlaggingError> {
    ensure_calibration_program(&table)?;
    calibration::run_program(&mut table, requests, options)
}

fn ensure_calibration_program(table: &MeasurementSet) -> Result<(), FlaggingError> {
    if !is_calibration_table(table) {
        return Err(FlaggingError::MutateFlags {
            path: display_ms_path(table),
            reason: "calibration flag commands require a calibration table".to_owned(),
        });
    }
    Ok(())
}

fn report_after(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
    rows: &[usize],
    changes: ChangeSet,
    backup_version: Option<String>,
    thresholds: Option<(f64, f64)>,
    time_selection: FlagTimeSelection,
) -> Result<FlagDataReport, FlaggingError> {
    let summary = match changes.summary {
        Some(summary) => summary,
        None if request.casa_row_selection
            && (time_selection != FlagTimeSelection::Global
                || (request.selection.timerange.is_none()
                    && request.selection.msselect.is_none())) =>
        {
            summarize_flags(ms, rows)?
        }
        None => {
            let summary_rows = selected_rows(ms, request, time_selection)?;
            summarize_flags(ms, &summary_rows.rows)?
        }
    };
    Ok(FlagDataReport {
        mode: request.mode,
        selected_rows: rows.len(),
        changed_rows: changes.changed_rows,
        changed_samples: changes.changed_samples,
        flagged_samples: summary.flagged_samples,
        total_samples: summary.total_samples,
        backup_version,
        timedev: thresholds.map(|pair| pair.0),
        freqdev: thresholds.map(|pair| pair.1),
        rflag_report: changes.rflag_report,
        summary: None,
    })
}

struct SelectedFlagRows {
    rows: Vec<usize>,
    channel_ranges: BTreeMap<i32, ChannelSelection>,
}

fn flagging_channels(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
    resolved_ranges: BTreeMap<i32, ChannelSelection>,
) -> Result<Option<BTreeMap<i32, ChannelSelection>>, FlaggingError> {
    let Some(text) = request.spw.as_deref() else {
        return Ok(None);
    };
    let retain_overlaps = request.casa_row_selection && request.mode == FlagDataMode::Extend;
    let ranges =
        if request.casa_row_selection && !retain_overlaps && request.spw == request.selection.spw {
            Ok(resolved_ranges)
        } else if request.casa_row_selection {
            ms.resolve_flagdata_spw_selector(text).and_then(|spw| {
                crate::selection::flagdata_channel_ranges(ms, &spw, retain_overlaps)
            })
        } else {
            parse_channel_selections(text)
        }
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: source.to_string(),
        })?;
    check_channel_selections(ms, &ranges)?;
    Ok(Some(ranges))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FlagTimeSelection {
    Global,
    GlobalAndAgent,
    Agent { initial_default_row: usize },
}

fn time_on_globally_selected_rows(
    text: &str,
    rows: &[ResolvedMsSelectionRow],
) -> crate::MsResult<TimeSelection> {
    let Some(first) = rows.first() else {
        return Ok(TimeSelection::all());
    };
    // Exposure affects the global predicate and the width in getTimeList(),
    // but only the endpoints are used by the list agent.
    let defaults = |row: &ResolvedMsSelectionRow| TimeDefaults {
        time_mjd_s: row.time_mjd_seconds,
        exposure_s: 0.0,
    };
    let first_unflagged = rows.iter().find(|row| !row.flag_row).unwrap_or(first);
    resolve_time_selector(
        text,
        TimeSelectorDefaults {
            first_term: defaults(first_unflagged),
            later_terms: defaults(first),
        },
    )
}

fn selected_rows(
    ms: &MeasurementSet,
    request: &FlagDataRequest,
    time_selection: FlagTimeSelection,
) -> Result<SelectedFlagRows, FlaggingError> {
    let selection_error = |source| FlaggingError::MutateFlags {
        path: display_ms_path(ms),
        reason: format!("select rows: {source}"),
    };
    let timerange = request
        .selection
        .timerange
        .as_deref()
        .filter(|_| request.selection.selectdata)
        .map(flagdata_c_string)
        .filter(|text| !text.is_empty());
    let agent_default_row = match time_selection {
        FlagTimeSelection::Agent {
            initial_default_row,
        } => Some(initial_default_row),
        _ => None,
    };
    let (rows, channel_ranges, missing_rows) = if request.casa_row_selection {
        let resolved = resolve_flagdata_selection_with_system_budget(
            ms,
            &request.selection,
            agent_default_row,
        )
        .map_err(selection_error)?;
        let missing_rows = resolved.selected_rows.is_empty();
        // CASA parses a one-command list's agent on the globally selected
        // table, so omitted dates can differ from the initial global parse.
        let agent_time = timerange
            .filter(|_| time_selection == FlagTimeSelection::GlobalAndAgent)
            .map(|text| time_on_globally_selected_rows(text, &resolved.selected_rows))
            .transpose()
            .map_err(selection_error)?;
        (
            resolved
                .selected_rows
                .into_iter()
                .filter(|row| {
                    agent_time
                        .as_ref()
                        .is_none_or(|time| time.holds_agent_time(row.time_mjd_seconds))
                })
                .map(|row| row.row_index)
                .collect(),
            resolved.channel_selections,
            missing_rows,
        )
    } else {
        let rows = resolve_row_indices_with_system_budget(ms, &request.selection)
            .map_err(selection_error)?;
        let missing_rows = rows.is_empty();
        (rows, BTreeMap::new(), missing_rows)
    };
    let field = request
        .selection
        .field
        .as_deref()
        .filter(|text| !text.trim().is_empty());
    let explicit_spw_filter = missing_rows
        && request.casa_row_selection
        && request
            .selection
            .spw
            .as_deref()
            .map(|text| {
                ms.resolve_flagdata_spw_selector(text)
                    .map(|spw| spw.rows_restricted())
            })
            .transpose()
            .map_err(selection_error)?
            .unwrap_or(false);
    let explicit_row_filter = field.is_some()
        || explicit_spw_filter
        || (request.casa_row_selection
            && [
                request.selection.scan.as_deref(),
                request.selection.antenna.as_deref(),
                request.selection.timerange.as_deref(),
                request.selection.observation.as_deref(),
                request.selection.array.as_deref(),
                request.selection.feed.as_deref(),
                request.selection.intent.as_deref(),
                request.selection.uvrange.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|text| !text.is_empty() && !text.starts_with('\0')));
    if missing_rows && request.selection.selectdata && explicit_row_filter {
        // An empty result can mean no matching FIELD metadata or simply no
        // selected MAIN rows. Keep separate diagnostics for the two cases.
        if let Some(field) = field {
            ms.resolve_field_selector(field).map_err(selection_error)?;
        }
        // An agent can have valid local time intervals that match no
        // records. Keep it in the list, including its empty summary.
        if !(request.casa_row_selection && timerange.is_some() && agent_default_row.is_some()) {
            return Err(FlaggingError::MutateFlags {
                path: display_ms_path(ms),
                reason: "MSSelectionNullSelection : The selected table has zero rows.".to_string(),
            });
        }
    }
    Ok(SelectedFlagRows {
        rows,
        channel_ranges,
    })
}

fn flag_read_plan(
    ms: &MeasurementSet,
    row_count: usize,
    primitive_bytes: usize,
    include_data_description: bool,
) -> Result<MsReadPlan, FlaggingError> {
    let path = display_ms_path(ms);
    let maximum_elements = maximum_flag_elements(ms)?;
    let requested_bytes_per_row = maximum_elements
        .checked_mul(primitive_bytes.saturating_add(2))
        .and_then(|bytes| bytes.checked_add(usize::from(include_data_description) * 4))
        .ok_or_else(|| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: "flag row byte accounting overflowed".to_string(),
        })?;
    let budget = MsSelectionIoBudget::from_system_memory(1, requested_bytes_per_row, None)
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: source.to_string(),
        })?;
    MsReadPlan::new(row_count, budget).map_err(|source| FlaggingError::MutateFlags {
        path,
        reason: source.to_string(),
    })
}

fn maximum_flag_elements(ms: &MeasurementSet) -> Result<usize, FlaggingError> {
    let path = display_ms_path(ms);
    let data_description = ms
        .data_description()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open DATA_DESCRIPTION: {source}"),
        })?;
    let spectral_window = ms
        .spectral_window()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open SPECTRAL_WINDOW: {source}"),
        })?;
    let polarization = ms
        .polarization()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open POLARIZATION: {source}"),
        })?;
    let mut maximum_elements = 0usize;
    for row in 0..data_description.row_count() {
        let spw_id = data_description.spectral_window_id(row).map_err(|source| {
            FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("read DATA_DESCRIPTION.SPECTRAL_WINDOW_ID row {row}: {source}"),
            }
        })?;
        let polarization_id =
            data_description
                .polarization_id(row)
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("read DATA_DESCRIPTION.POLARIZATION_ID row {row}: {source}"),
                })?;
        let channel_count = usize::try_from(spectral_window.num_chan(spw_id as usize).map_err(
            |source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("read SPECTRAL_WINDOW.NUM_CHAN row {spw_id}: {source}"),
            },
        )?)
        .map_err(|_| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("SPECTRAL_WINDOW.NUM_CHAN row {spw_id} is negative"),
        })?;
        let correlation_count = usize::try_from(
            polarization
                .num_corr(polarization_id as usize)
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("read POLARIZATION.NUM_CORR row {polarization_id}: {source}"),
                })?,
        )
        .map_err(|_| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("POLARIZATION.NUM_CORR row {polarization_id} is negative"),
        })?;
        maximum_elements =
            maximum_elements.max(channel_count.checked_mul(correlation_count).ok_or_else(
                || FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: "flag row shape byte accounting overflowed".to_string(),
                },
            )?);
    }
    Ok(maximum_elements)
}

fn apply_clip_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    request: &FlagDataRequest,
    model_factory: Option<FlagDataModelFactory>,
    averaged_time: Option<&TimeSelection>,
) -> Result<ChangeSet, FlaggingError> {
    let path = display_ms_path(ms);
    let widths = request.clip_channel_bins.as_deref();
    let time_average = request.clip_time_bin.as_deref().map(time_bin_seconds);
    let average = time_average.is_some_and(|interval| interval.is_some())
        || widths.is_some_and(|widths| widths.iter().any(|&width| width != 0));
    let models = if request.clipoutside.is_some() {
        clip_models::ClipModels::for_selection(
            ms,
            request.data_column,
            selected_rows,
            model_factory,
            &path,
        )?
    } else {
        None
    };
    let Some(mut columns) =
        ClipColumns::bind(ms.main_table(), request.data_column, average, models)?
    else {
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    };
    if time_average == Some(None) {
        tracing::warn!("unrecognized timebin disables the clip agent");
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    }
    let Some(outside) = request.clipoutside else {
        tracing::warn!("clipoutside must be a scalar Boolean to start the clip agent");
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    };
    if let Some(widths) = widths {
        check_channel_bins(ms, widths, &path)?;
    }
    let options = ClipOptions {
        zeros: request.clipzeros,
        signed_zeros: false,
        // CASA's clipping agent stores its bounds as single-precision values.
        bounds: request
            .clipminmax
            .map(|bounds| bounds.map(|limit| f64::from(limit as f32))),
        outside,
        projections: None,
        metric: correlations.and_then(CorrelationSelection::clip_metric),
    };
    // CASA flags non-finite data and, with clipzeros, near-zero samples.
    let ddid_to_spw = data_description_spw_map(ms)?;
    let mut changed = ChangeSet::default();
    let mut flagged_samples = 0usize;
    let mut total_samples = 0usize;
    let mut row_changes = Vec::<(usize, usize)>::new();
    let include_ddid = average
        || channel_selections.is_some()
        || correlations.is_some()
        || columns.uses_weight_spectrum();
    let bytes_per_sample = columns.bytes_per_sample(ms.main_table(), &path)?;
    let read_plan = flag_read_plan(ms, selected_rows.len(), bytes_per_sample, include_ddid)?;
    if let Some(Some(seconds)) = time_average {
        return TimeClip {
            averaging: clip_time::TimeAveraging {
                seconds,
                filter: averaged_time,
            },
            chunk_seconds: request.time_chunk,
            column: request.data_column,
            widths,
            channels: channel_selections,
            correlations,
            options,
            path: &path,
        }
        .apply(ms, selected_rows, &mut columns, &ddid_to_spw, read_plan);
    }
    for rows in selected_rows.chunks(read_plan.rows_per_block.max(1)) {
        let table = ms.main_table();
        let mut updates = BTreeMap::<usize, ArrayD<bool>>::new();
        let mut touched_rows = std::collections::BTreeSet::<usize>::new();
        let flag_cells = clip_time::read_flags(table, rows, &path)?;
        let ddids = if include_ddid {
            Some(load_i32_cells(table, rows, "DATA_DESC_ID", &path)?)
        } else {
            None
        };
        let data_cells =
            columns.read(ms, rows, &flag_cells, ddids.as_deref(), &ddid_to_spw, &path)?;
        for (row_slot, (&row, flags)) in rows.iter().zip(&flag_cells).enumerate() {
            total_samples += flags.len();
            flagged_samples += flags.iter().filter(|flag| **flag).count();
            let ddid = ddids.as_ref().map(|values| values[row_slot]);
            let projections = correlations
                .zip(ddid)
                .map(|(selection, ddid)| selection.clip_projections_for(ddid, &path))
                .transpose()?;
            if projections.is_some_and(|projections| projections.is_empty()) {
                continue;
            }
            let column = request.data_column.name();
            if flags.ndim() != 2 {
                return Err(FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("{column} row {row} is not rank-2"),
                });
            }
            let nchan = flags.shape()[1];
            let (channels, width) = if average || channel_selections.is_some() {
                clip_channel_selection(
                    ddid,
                    &ddid_to_spw,
                    channel_selections,
                    widths,
                    nchan,
                    &path,
                )?
            } else {
                (None, 0)
            };
            let bins = channels
                .as_deref()
                .and_then(|channels| ChannelBins::new(width, channels, flags));
            let data_value = data_cells.row(row_slot, flags, bins.as_ref(), row, &path)?;
            let (clip_flags, clip_channels) = bins
                .as_ref()
                .map_or((flags, channels.as_deref()), |bins| (bins.flags(), None));
            row_changes.clear();
            apply_clip_array(
                data_value.as_ref(),
                clip_flags,
                clip_channels,
                ClipOptions {
                    projections,
                    ..options
                },
                &path,
                row,
                &mut row_changes,
            )?;
            if !row_changes.is_empty() {
                let mut updated_flags = flags.clone();
                let mut flag_planes = updated_flags
                    .view_mut()
                    .into_dimensionality::<Ix2>()
                    .map_err(|source| FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("FLAG row {row} must be rank-2: {source}"),
                    })?;
                let may_overlap = projections.is_some_and(|items| {
                    items.len() > 1
                        && items
                            .iter()
                            .any(|item| matches!(item, ClipProjection::Stokes(..)))
                });
                let row_changed = if let Some(bins) = &bins {
                    row_changes
                        .iter()
                        .map(|&(corr, bin)| {
                            bins.original_channels(bin)
                                .iter()
                                .filter(|&&chan| {
                                    !std::mem::replace(&mut flag_planes[(corr, chan)], true)
                                })
                                .count()
                        })
                        .sum()
                } else if may_overlap {
                    row_changes
                        .iter()
                        .filter(|&&(corr, chan)| {
                            !std::mem::replace(&mut flag_planes[(corr, chan)], true)
                        })
                        .count()
                } else {
                    for &(corr, chan) in &row_changes {
                        flag_planes[(corr, chan)] = true;
                    }
                    row_changes.len()
                };
                changed.changed_samples += row_changed;
                flagged_samples += row_changed;
                updates.insert(row, updated_flags);
                touched_rows.insert(row);
            }
        }
        // CASA's clip agent writes the per-sample FLAG column.
        write_touched_flag_updates(ms, updates, touched_rows, |_| false, &mut changed)?;
    }
    changed.summary = Some(FlagSummary {
        flagged_samples,
        total_samples,
    });
    Ok(changed)
}

fn clip_channel_selection(
    ddid: Option<i32>,
    ddid_to_spw: &BTreeMap<i32, i32>,
    selections: Option<&BTreeMap<i32, ChannelSelection>>,
    widths: Option<&[u32]>,
    channel_count: usize,
    path: &str,
) -> Result<(Option<Vec<usize>>, u32), FlaggingError> {
    let ddid = ddid.expect("selected channels project DATA_DESC_ID");
    let spw = ddid_to_spw
        .get(&ddid)
        .copied()
        .ok_or_else(|| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
        })?;
    let width = match widths {
        None => 0,
        Some([width]) => *width,
        Some(widths) => {
            widths
                .get(spw as usize)
                .copied()
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("SPW {spw} has no channel bin"),
                })?
        }
    };
    let mut channels =
        match selections.and_then(|selectors| selectors.get(&spw)) {
            Some(selection) => Some(selection.indices(channel_count).map_err(|source| {
                FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!("resolve SPW {spw} channels: {source}"),
                }
            })?),
            None if width > 0 => Some((0..channel_count).collect()),
            None => None,
        };
    if width > 0
        && let Some(channels) = &mut channels
    {
        channels.sort_unstable();
    }
    Ok((channels, width))
}

fn apply_quack_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    request: &FlagDataRequest,
) -> Result<ChangeSet, FlaggingError> {
    let from_start = match request.quackmode {
        QuackMode::Beg | QuackMode::Tail => true,
        QuackMode::End | QuackMode::BeforeEnd => false,
        QuackMode::Disabled => {
            return Ok(ChangeSet {
                agent_skipped: true,
                ..ChangeSet::default()
            });
        }
    };
    if request.quackinterval.is_nan() || request.quackinterval <= 0.0 {
        tracing::warn!("quackinterval must be greater than zero to start the quack agent");
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    }
    let table = ms.main_table();
    let mut scan_times = Vec::with_capacity(selected_rows.len());
    let mut edges = BTreeMap::<i32, f64>::new();
    for &row in selected_rows {
        let scan = table_i32(table, row, "SCAN_NUMBER", ms.path())?;
        let time = table_f64(table, row, "TIME", ms.path())?;
        scan_times.push((row, scan, time));
        // CASA determines the unflagged time limits from whole flag cubes.
        if !request.quackincrement || clone_flag_matrix(ms, row)?.iter().any(|flag| !*flag) {
            edges
                .entry(scan)
                .and_modify(|edge| {
                    *edge = if from_start {
                        edge.min(time)
                    } else {
                        edge.max(time)
                    };
                })
                .or_insert(time);
        }
    }
    let mut quack_rows = Vec::new();
    for (row, scan, time) in scan_times {
        let Some(&edge_time) = edges.get(&scan) else {
            continue;
        };
        // CASA's edge intervals include equality. The complementary modes
        // select the samples strictly outside their respective intervals.
        let flag = match request.quackmode {
            QuackMode::Beg => time <= edge_time + request.quackinterval,
            QuackMode::End => time >= edge_time - request.quackinterval,
            QuackMode::BeforeEnd => time < edge_time - request.quackinterval,
            QuackMode::Tail => time > edge_time + request.quackinterval,
            QuackMode::Disabled => false,
        };
        if flag {
            quack_rows.push(row);
        }
    }
    // CASA updates FLAG_ROW at selected scan times only when the call
    // leaves SPW unspecified, even for an explicitly selected full window.
    let row_policy = if channel_selections.is_some() {
        SelectedFlagRowPolicy::Preserve
    } else {
        selected_flag_row_policy(correlations, SelectedFlagRowPolicy::AllSelectedRows)
    };
    set_selected_flags(
        ms,
        &quack_rows,
        channel_selections,
        correlations,
        request.action.value(),
        row_policy,
    )
}

fn apply_tfcrop_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
    model_factory: Option<FlagDataModelFactory>,
    averaged_time: Option<&TimeSelection>,
) -> Result<ChangeSet, FlaggingError> {
    let started = Instant::now();
    let column = request.data_column;
    let calibration = is_calibration_table(ms);
    let public_ms = request.casa_row_selection && !calibration;
    let time_average = request
        .clip_time_bin
        .as_deref()
        .filter(|_| public_ms)
        .map(time_bin_seconds);
    let correlations = public_ms
        .then(|| TfcropCorrelations::read(ms, request.correlation.as_deref(), column))
        .transpose()?;
    let corr_admitted = correlations
        .as_ref()
        .is_none_or(TfcropCorrelations::has_setup);
    let options = request.tfcrop.filter(|options| {
        (1..=9).contains(&options.max_pieces)
            && (1..=3).contains(&options.half_window)
            && time_average != Some(None)
            && corr_admitted
    });
    let widths = request.clip_channel_bins.as_deref().filter(|_| public_ms);
    let average = time_average.flatten().is_some()
        || widths.is_some_and(|widths| widths.iter().any(|&width| width != 0));
    let (mut columns, configures_averaging) = if public_ms {
        tfcrop_columns(
            ms,
            column,
            selected_rows,
            model_factory,
            options.is_some(),
            average,
        )?
    } else {
        (None, false)
    };
    let options =
        options.filter(|_| !request.casa_row_selection || calibration || columns.is_some());
    if (public_ms && !configures_averaging) || (options.is_none() && !request.extendflags) {
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    }
    let configures_averaging = configures_averaging && corr_admitted && time_average != Some(None);
    let widths = widths.filter(|_| configures_averaging);
    if let Some(widths) = widths {
        check_channel_bins(ms, widths, &display_ms_path(ms))?;
    }
    let loaded = if let Some(seconds) = time_average.flatten().filter(|_| configures_averaging) {
        load_time_averaged_tfcrop(
            ms,
            selected_rows,
            channel_selections,
            columns.as_mut(),
            request,
            clip_time::TimeAveraging {
                seconds,
                filter: averaged_time,
            },
            widths,
        )?
    } else {
        load_samples(
            ms,
            selected_rows,
            channel_selections,
            options.map(|_| {
                columns
                    .as_mut()
                    .map_or(SampleData::Stored(column), |operands| {
                        SampleData::Visibility(column, operands)
                    })
            }),
            None,
            false,
            widths,
        )?
    };
    let mut samples = loaded.samples;
    let calibration_projection = calibration
        .then(|| calibration::automatic_projection(request, &samples))
        .transpose()?;
    if let Some(projection) = &calibration_projection {
        for sample in &mut samples {
            sample.amp = projection.scalar(sample);
        }
    }
    let loaded_ms = started.elapsed().as_millis();
    let mut modified = vec![false; samples.len()];
    let mut freq_flags = vec![false; samples.len()];
    let mut time_flags = vec![false; samples.len()];
    let groups: Vec<_> = if request.casa_row_selection || calibration_projection.is_some() {
        ordered_autoflag_groups(ms, selected_rows, &samples, request)?
            .into_iter()
            .map(|(([_, _, _, _, description], _), group)| (Some(description), group))
            .collect()
    } else {
        group_samples(&samples)
            .into_values()
            .map(|group| (None, group))
            .collect()
    };
    if let Some(options) = &options {
        fit_tfcrop_groups(
            &groups,
            options,
            request,
            correlations.as_ref(),
            calibration_projection.as_ref(),
            &loaded.model_roundoff,
            &mut TfcropFitMasks {
                modified: &mut modified,
                freq_flags: &mut freq_flags,
                time_flags: &mut time_flags,
            },
        )?;
    }
    if request.extendflags {
        let fit_flags = loaded
            .axes
            .flags_by_bin
            .as_ref()
            .unwrap_or(&loaded.flags_by_row);
        extend_tfcrop_groups(
            &groups,
            &samples,
            fit_flags,
            request.casa_row_selection && channel_selections.is_some() && widths.is_none(),
            correlations.as_ref().filter(|_| corr_admitted),
            &mut modified,
        );
    }
    let union_candidates = count_mask(&modified);
    let planned_ms = started.elapsed().as_millis();
    trace_flagdata(|| {
        json!({
            "mode": "tfcrop",
            "implementation": "casa-rs-native-current",
            "selected_rows": selected_rows.len(),
            "samples": samples.len(),
            "groups": groups.len(),
            "freqcutoff": request.freqcutoff,
            "timecutoff": request.timecutoff,
            "freq_candidate_samples": count_mask(&freq_flags),
            "time_candidate_samples": count_mask(&time_flags),
            "union_candidate_samples": union_candidates,
            "union_by_spw_corr": flag_mask_counts_by_spw_corr(&modified, &samples),
            "timing_ms": {
                "load": loaded_ms,
                "plan": planned_ms.saturating_sub(loaded_ms),
            },
        })
    });
    let changes = apply_tfcrop_mask(
        &samples,
        &modified,
        loaded.flags_by_row,
        &loaded.axes,
        ms,
        !request.casa_row_selection,
        request.action.value(),
    )?;
    let applied_ms = started.elapsed().as_millis();
    trace_flagdata(|| {
        json!({
            "mode": "tfcrop",
            "implementation": "casa-rs-casa-shaped-tfcrop",
            "timing_ms": {
                "apply": applied_ms.saturating_sub(planned_ms),
                "total_before_save": applied_ms,
            },
        })
    });
    Ok(changes)
}

struct TfcropFitMasks<'a> {
    modified: &'a mut [bool],
    freq_flags: &'a mut [bool],
    time_flags: &'a mut [bool],
}

fn fit_tfcrop_groups(
    groups: &[(Option<i32>, Vec<Sample>)],
    options: &TfcropOptions,
    request: &FlagDataRequest,
    correlations: Option<&TfcropCorrelations>,
    calibration_projection: Option<&calibration::CalibrationProjection>,
    model_roundoff: &[f32],
    masks: &mut TfcropFitMasks<'_>,
) -> Result<(), FlaggingError> {
    let mut roundoff = if model_roundoff.is_empty() {
        Vec::new()
    } else {
        vec![0.0; masks.modified.len()]
    };
    for (description, group) in groups {
        let planes = samples_by_corr(group);
        let selection = correlations.zip(*description);
        let default_projections;
        let (projections, metric) = if let Some((selection, ddid)) = selection {
            (selection.for_description(ddid)?, Some(selection.metric))
        } else {
            default_projections = planes
                .iter()
                .filter(|plane| calibration_projection.is_none_or(|p| p.includes(plane[0].corr)))
                .map(|plane| ClipProjection::Stored(plane[0].corr))
                .collect::<Vec<_>>();
            (default_projections.as_slice(), None)
        };
        for &projection in projections {
            let Some(series) = tfcrop_series(
                &planes,
                projection,
                metric,
                masks.modified,
                model_roundoff,
                &mut roundoff,
            ) else {
                continue;
            };
            for &direction in options.passes.directions() {
                let (cutoff, candidates) = match direction {
                    TfcropDirection::Freq => (request.freqcutoff, &mut *masks.freq_flags),
                    TfcropDirection::Time => (request.timecutoff, &mut *masks.time_flags),
                };
                tfcrop_fit_base_and_flag(
                    &series.samples,
                    direction,
                    cutoff,
                    options,
                    TfcropArithmetic {
                        casa_arithmetic: request.casa_row_selection,
                        model_roundoff: &roundoff,
                    },
                    masks.modified,
                    candidates,
                );
            }
            for (first, second) in series.pairs {
                masks.modified[second] |= masks.modified[first];
                masks.freq_flags[second] |= masks.freq_flags[first];
                masks.time_flags[second] |= masks.time_flags[first];
            }
        }
    }
    Ok(())
}

fn extend_tfcrop_groups(
    groups: &[(Option<i32>, Vec<Sample>)],
    samples: &[Sample],
    flags: &BTreeMap<usize, ArrayD<bool>>,
    selected_channels: bool,
    correlations: Option<&TfcropCorrelations>,
    modified: &mut [bool],
) {
    let mut restricted_mask = correlations
        .filter(|selection| selection.restrict_extension)
        .map(|_| modified.to_vec());
    let mask = restricted_mask.as_deref_mut().unwrap_or(modified);
    if selected_channels {
        for (_, group) in groups {
            extend_tfcrop_selected_channels(group, flags, mask);
        }
    } else {
        extend_autoflag_grouped(
            groups.iter().map(|(_, group)| group.as_slice()),
            samples,
            mask,
        );
    }
    if let Some((selection, extended)) = correlations.zip(restricted_mask) {
        for (description, group) in groups {
            let planes = selection
                .extension_planes(description.expect("public TFCrop group has a data description"));
            for sample in group {
                if planes.binary_search(&sample.corr).is_ok() {
                    modified[sample.index] = extended[sample.index];
                }
            }
        }
    }
}

struct TfcropSeries {
    samples: Vec<Sample>,
    pairs: Vec<(usize, usize)>,
}

fn tfcrop_series(
    planes: &[Vec<Sample>],
    projection: ClipProjection,
    metric: Option<ClipMetric>,
    modified: &[bool],
    model_roundoff: &[f32],
    roundoff: &mut [f32],
) -> Option<TfcropSeries> {
    let plane = |plane| planes.iter().find(|samples| samples[0].corr == plane);
    let (first, second) = match projection {
        ClipProjection::Stored(first) => (plane(first)?, None),
        ClipProjection::Stokes(first, second, combine) => {
            (plane(first)?, Some((plane(second)?, combine)))
        }
    };
    let mut series = TfcropSeries {
        samples: Vec::with_capacity(first.len()),
        pairs: Vec::new(),
    };
    let second = second.map(|(samples, combine)| {
        let lookup = samples
            .iter()
            .map(|sample| ((sample.row, sample.chan), sample))
            .collect::<HashMap<_, _>>();
        (lookup, combine)
    });
    for original in first {
        let mut sample = *original;
        sample.flag = sample_is_modified(original, modified);
        let mut value = Complex64::new(sample.real, sample.imag);
        let mut uncertainty = model_roundoff.get(sample.index).copied().unwrap_or(0.0);
        if let Some((lookup, combine)) = &second {
            let Some(other) = lookup.get(&(sample.row, sample.chan)) else {
                continue;
            };
            value = combine.apply64(value, Complex64::new(other.real, other.imag));
            sample.real = value.re;
            sample.imag = value.im;
            sample.flag |= sample_is_modified(other, modified);
            uncertainty =
                (uncertainty + model_roundoff.get(other.index).copied().unwrap_or(0.0)) * 0.5;
            series.pairs.push((sample.index, other.index));
        }
        if let Some(metric) = metric {
            sample.amp = metric.apply64(value);
            if let Some(bound) = roundoff.get_mut(sample.index) {
                *bound = tfcrop_metric_roundoff(metric, value, uncertainty);
            }
        } else if let Some(bound) = roundoff.get_mut(sample.index) {
            *bound = uncertainty;
        }
        series.samples.push(sample);
    }
    Some(series)
}

fn tfcrop_metric_roundoff(metric: ClipMetric, value: Complex64, uncertainty: f32) -> f32 {
    if uncertainty == 0.0 {
        return 0.0;
    }
    let radius = f64::from(uncertainty);
    match metric {
        ClipMetric::SquaredMagnitude => (radius * (2.0 * value.norm() + radius)) as f32,
        ClipMetric::Phase
            if radius < value.norm() && (value.re >= 0.0 || radius < value.im.abs()) =>
        {
            (radius / value.norm()).asin() as f32
        }
        // The fit compares principal arguments on either side of ±π.
        ClipMetric::Phase => std::f32::consts::TAU,
        _ => uncertainty,
    }
}

fn tfcrop_columns(
    ms: &MeasurementSet,
    column: FlagDataColumn,
    selected_rows: &[usize],
    model_factory: Option<FlagDataModelFactory>,
    active: bool,
    average: bool,
) -> Result<(Option<ClipColumns>, bool), FlaggingError> {
    autoflag_columns(
        ms,
        column,
        selected_rows,
        model_factory,
        active,
        average,
        FlagDataMode::Tfcrop,
    )
}

fn autoflag_columns(
    ms: &MeasurementSet,
    column: FlagDataColumn,
    selected_rows: &[usize],
    model_factory: Option<FlagDataModelFactory>,
    active: bool,
    average: bool,
    mode: FlagDataMode,
) -> Result<(Option<ClipColumns>, bool), FlaggingError> {
    let missing = || {
        if mode == FlagDataMode::Rflag {
            FlaggingError::MissingRflagColumn
        } else {
            FlaggingError::MissingTfcropColumn
        }
    };
    let table = ms.main_table();
    let exists = |name| {
        table
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
    };
    let is_residual = matches!(
        column,
        FlagDataColumn::Residual | FlagDataColumn::ResidualData
    );
    let required_sources_exist = if is_residual {
        exists("MODEL_DATA") || !crate::scratch_columns::virtual_model_fields(ms).is_empty()
    } else {
        exists(column.name())
    };
    if !required_sources_exist {
        return Err(missing());
    }
    if column == FlagDataColumn::Residual && !exists("CORRECTED_DATA") {
        let agent = if mode == FlagDataMode::Rflag {
            "Rflag"
        } else {
            "Tfcrop"
        };
        tracing::warn!(
            "Cannot create agent {agent}: necessary CORRECTED_DATA column is not available"
        );
        return Ok((None, false));
    }
    // An admitted agent configures the iterator before validating its fit.
    // A surviving extension agent uses that iterator. Only a running fit
    // opens a virtual recipe's image.
    if !active {
        return Ok((None, true));
    }
    let path = display_ms_path(ms);
    let models =
        clip_models::ClipModels::for_selection(ms, column, selected_rows, model_factory, &path)?;
    match ClipColumns::bind(table, column, average, models) {
        Err(FlaggingError::MissingClipColumn) => Err(missing()),
        result => result.map(|columns| (columns, true)),
    }
}

fn extend_tfcrop_selected_channels(
    group: &[Sample],
    flags: &BTreeMap<usize, ArrayD<bool>>,
    modified: &mut [bool],
) {
    // The extension agent counts a whole window. Publish the results only
    // for samples in the explicit channel selection.
    let mut full = Vec::new();
    let mut indices = Vec::with_capacity(group.len());
    for row in sample_row_runs(group) {
        let sample = row[0];
        let flags = &flags[&sample.row];
        let channels = flags.shape()[1];
        let start = full.len();
        push_flag_samples(
            flags,
            &(0..channels).collect::<Vec<_>>(),
            None,
            RowSampleMetadata {
                row: sample.row,
                field: sample.field,
                spw: sample.spw,
                scan: sample.scan,
                ant1: sample.ant1,
                ant2: sample.ant2,
            },
            &mut full,
        );
        indices.extend(
            row.iter()
                .map(|sample| (sample.index, start + sample.corr * channels + sample.chan)),
        );
    }
    let mut extended = vec![false; full.len()];
    for &(selected, all) in &indices {
        extended[all] = modified[selected];
    }
    extend_autoflag_grouped([full.as_slice()], &full, &mut extended);
    for (selected, all) in indices {
        modified[selected] = extended[all];
    }
}

type AutoflagChunk = ([i32; 5], usize);

fn ordered_autoflag_groups(
    ms: &MeasurementSet,
    selected_rows: &[usize],
    samples: &[Sample],
    request: &FlagDataRequest,
) -> Result<Vec<(AutoflagChunk, Vec<Sample>)>, FlaggingError> {
    let timed = clip_time::ordered_baseline_chunks(
        ms,
        selected_rows,
        request.time_chunk,
        request.combinescans,
        &display_ms_path(ms),
    )?;
    let runs: HashMap<_, _> = sample_row_runs(samples)
        .into_iter()
        .map(|row| (row[0].row, row))
        .collect();
    Ok(timed
        .chunk_by(|left, right| left.baseline_key() == right.baseline_key())
        .map(|baseline| {
            let samples = baseline
                .iter()
                .filter_map(|row| runs.get(&row.row).copied())
                .flatten()
                .copied()
                .collect::<Vec<_>>();
            (baseline[0].chunk_key(), samples)
        })
        .filter(|(_, group)| !group.is_empty())
        .collect())
}

fn apply_rflag_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    request: &FlagDataRequest,
    calculate: bool,
    model_factory: Option<FlagDataModelFactory>,
    averaged_time: Option<&TimeSelection>,
) -> Result<ChangeSet, FlaggingError> {
    let started = Instant::now();
    let column = request.data_column;
    let calibration = is_calibration_table(ms);
    let public_ms = request.casa_row_selection && !calibration;
    let casa_rflag = request.casa_row_selection || calibration;
    let time_average = request
        .clip_time_bin
        .as_deref()
        .filter(|_| public_ms)
        .map(time_bin_seconds);
    let widths = request.clip_channel_bins.as_deref().filter(|_| public_ms);
    let average = time_average.flatten().is_some()
        || widths.is_some_and(|widths| widths.iter().any(|&width| width != 0));
    let correlations = public_ms
        .then(|| correlation::RflagCorrelations::read(ms, request.correlation.as_deref(), column))
        .transpose()?;
    let corr_admitted = correlations.as_ref().is_none_or(|c| c.admitted);
    let active = request.rflag_window.is_some() && time_average != Some(None) && corr_admitted;
    let (mut columns, configures_averaging) = if public_ms {
        autoflag_columns(
            ms,
            column,
            selected_rows,
            model_factory,
            active,
            average,
            FlagDataMode::Rflag,
        )?
    } else {
        (None, false)
    };
    let active = active && (!public_ms || columns.is_some());
    if (public_ms && !configures_averaging) || (!active && !request.extendflags) {
        return Ok(ChangeSet {
            agent_skipped: true,
            ..ChangeSet::default()
        });
    }
    let configures_averaging = configures_averaging && corr_admitted && time_average != Some(None);
    let widths = widths.filter(|_| configures_averaging);
    if let Some(widths) = widths {
        check_channel_bins(ms, widths, &display_ms_path(ms))?;
    }
    let loaded = if let Some(seconds) = time_average.flatten().filter(|_| configures_averaging) {
        load_time_averaged_tfcrop(
            ms,
            selected_rows,
            channel_selections,
            columns.as_mut(),
            request,
            clip_time::TimeAveraging {
                seconds,
                filter: averaged_time,
            },
            widths,
        )?
    } else {
        load_samples(
            ms,
            selected_rows,
            channel_selections,
            active.then(|| {
                columns
                    .as_mut()
                    .map_or(SampleData::Stored(column), |operands| {
                        SampleData::Visibility(column, operands)
                    })
            }),
            None,
            false,
            widths,
        )?
    };
    let samples = loaded.samples;
    let projection = calibration
        .then(|| calibration::automatic_projection(request, &samples))
        .transpose()?;
    let context = RflagContext {
        casa_arithmetic: casa_rflag,
        correlations: correlations.as_ref(),
        calibration: projection.as_ref(),
        model_roundoff: &loaded.model_roundoff,
    };
    let loaded_ms = started.elapsed().as_millis();
    let groups = if casa_rflag {
        ordered_autoflag_groups(ms, selected_rows, &samples, request)?
    } else {
        group_samples(&samples)
            .into_values()
            .map(|samples| (AutoflagChunk::default(), samples))
            .collect()
    };
    if !calculate && active && casa_rflag {
        validate_rflag_matrix_scales(&groups, request, context)?;
    }
    if calculate {
        let thresholds = if active {
            rflag_threshold_maps_for_groups(&groups, request, context)?
        } else {
            RFlagThresholds::default()
        };
        return Ok(ChangeSet {
            rflag_report: (active || request.extendflags)
                .then(|| thresholds.public_report(request, active)),
            ..ChangeSet::default()
        });
    }

    let mut modified = vec![false; samples.len()];
    let mut time_flags = vec![false; samples.len()];
    let mut freq_flags = vec![false; samples.len()];
    let RflagFitOutcome {
        thresholds,
        buffer_cutoffs,
        threshold_ms,
    } = if active {
        fit_rflag_chunks(
            &groups,
            request,
            context,
            &mut TfcropFitMasks {
                modified: &mut modified,
                freq_flags: &mut freq_flags,
                time_flags: &mut time_flags,
            },
        )?
    } else {
        RflagFitOutcome::default()
    };
    if request.extendflags {
        let fit_flags = loaded
            .axes
            .flags_by_bin
            .as_ref()
            .unwrap_or(&loaded.flags_by_row);
        if casa_rflag && channel_selections.is_some() && loaded.axes.channels_by_row.is_empty() {
            for (_, group) in &groups {
                extend_tfcrop_selected_channels(group, fit_flags, &mut modified);
            }
        } else {
            extend_autoflag_grouped(
                groups.iter().map(|(_, samples)| samples.as_slice()),
                &samples,
                &mut modified,
            );
        }
    }
    let union_candidates = count_mask(&modified);
    let planned_ms = started.elapsed().as_millis();
    trace_flagdata(|| {
        json!({
            "mode": "rflag",
            "implementation": "casa-rs-casa-shaped-rflag",
            "selected_rows": selected_rows.len(),
            "samples": samples.len(),
            "groups": groups.len(),
            "timedev_by_field_spw": format_threshold_map(&thresholds.timedev),
            "freqdev_by_field_spw": format_threshold_map(&thresholds.freqdev),
            "time_candidate_samples": count_mask(&time_flags),
            "freq_candidate_samples": count_mask(&freq_flags),
            "union_candidate_samples": union_candidates,
            "union_by_spw_corr": flag_mask_counts_by_spw_corr(&modified, &samples),
            "timing_ms": {
                "load": loaded_ms,
                "threshold": threshold_ms,
                "plan": planned_ms.saturating_sub(loaded_ms + threshold_ms),
            },
        })
    });
    let mut changes = apply_tfcrop_mask(
        &samples,
        &modified,
        loaded.flags_by_row,
        &loaded.axes,
        ms,
        !request.casa_row_selection,
        request.action.value(),
    )?;
    let applied_ms = started.elapsed().as_millis();
    changes.thresholds = if casa_rflag {
        buffer_cutoffs
            .first()
            .copied()
            .filter(|cutoff| buffer_cutoffs.iter().all(|candidate| candidate == cutoff))
    } else {
        Some(thresholds.representative_pair())
    };
    trace_flagdata(|| {
        json!({
            "mode": "rflag",
            "implementation": "casa-rs-casa-shaped-rflag",
            "timing_ms": {
                "apply": applied_ms.saturating_sub(planned_ms),
                "total_before_save": applied_ms,
            },
        })
    });
    Ok(changes)
}

#[derive(Clone, Copy, Default)]
struct RflagContext<'a> {
    casa_arithmetic: bool,
    correlations: Option<&'a correlation::RflagCorrelations>,
    calibration: Option<&'a calibration::CalibrationProjection>,
    model_roundoff: &'a [f32],
}

fn rflag_group_projections(
    group: &[Sample],
    description: Option<i32>,
    context: RflagContext<'_>,
) -> Result<Vec<ClipProjection>, FlaggingError> {
    if let Some((selection, ddid)) = context.correlations.zip(description) {
        return selection
            .for_description(ddid)
            .map(<[ClipProjection]>::to_vec);
    }
    Ok(samples_by_corr(group)
        .into_iter()
        .filter(|plane| {
            context
                .calibration
                .is_none_or(|projection| projection.includes(plane[0].corr))
        })
        .map(|plane| ClipProjection::Stored(plane[0].corr))
        .collect())
}

fn validate_rflag_matrix_scales(
    groups: &[(AutoflagChunk, Vec<Sample>)],
    request: &FlagDataRequest,
    context: RflagContext<'_>,
) -> Result<(), FlaggingError> {
    for (scale_name, scale, thresholds) in [
        (
            "timedevscale",
            request.timedevscale,
            &request.timedev_by_field_spw,
        ),
        (
            "freqdevscale",
            request.freqdevscale,
            &request.freqdev_by_field_spw,
        ),
    ] {
        if scale > 0.0 || thresholds.is_empty() {
            continue;
        }
        for (chunk, group) in groups {
            let Some(first) = group.first() else {
                continue;
            };
            if !thresholds
                .get(&(first.field, first.spw))
                .is_some_and(|&value| value > 0.0)
            {
                continue;
            }
            let ddid = context.correlations.map(|_| chunk.0[4]);
            if !rflag_group_projections(group, ddid, context)?.is_empty() {
                return Err(FlaggingError::InvalidRFlagScale(scale_name));
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct RflagFitOutcome {
    thresholds: RFlagThresholds,
    buffer_cutoffs: Vec<(f64, f64)>,
    threshold_ms: u128,
}

fn fit_rflag_chunks(
    groups: &[(AutoflagChunk, Vec<Sample>)],
    request: &FlagDataRequest,
    context: RflagContext<'_>,
    masks: &mut TfcropFitMasks<'_>,
) -> Result<RflagFitOutcome, FlaggingError> {
    let mut thresholds = RFlagThresholds::default();
    let mut buffer_cutoffs = Vec::new();
    let mut threshold_ms = 0;
    let mut roundoff_buf = if context.model_roundoff.is_empty() {
        Vec::new()
    } else {
        vec![0.0; masks.modified.len()]
    };
    // RFlag applies each buffer's own fitted thresholds. The calculate report
    // instead accumulates all buffers for each field/SPW.
    for chunk in groups.chunk_by(|(left, _), (right, _)| left == right) {
        let threshold_started = Instant::now();
        let chunk_thresholds =
            rflag_threshold_maps_with_roundoff(chunk, request, context, &mut roundoff_buf)?
                .scaled(request, context.casa_arithmetic);
        threshold_ms += threshold_started.elapsed().as_millis();
        for (chunk_key, group) in chunk {
            let ddid = context.correlations.map(|_| chunk_key.0[4]);
            rflag_group_flags(
                ddid,
                group,
                &chunk_thresholds,
                request,
                context,
                &mut roundoff_buf,
                masks,
            )?;
        }
        if context.casa_arithmetic {
            let ([observation, array, scan, field, window], _) = chunk[0].0;
            let cutoff = chunk_thresholds.representative_pair();
            buffer_cutoffs.push(cutoff);
            trace_flagdata(|| {
                json!({
                    "mode": "rflag",
                    "calibration_buffer": {
                        "observation": observation,
                        "array": array,
                        "scan": scan,
                        "field": field,
                        "spw": window,
                    },
                    "timedev": cutoff.0,
                    "freqdev": cutoff.1,
                })
            });
        } else {
            thresholds = chunk_thresholds;
        }
    }
    Ok(RflagFitOutcome {
        thresholds,
        buffer_cutoffs,
        threshold_ms,
    })
}

fn extend_autoflag_grouped<'a>(
    groups: impl IntoIterator<Item = &'a [Sample]>,
    samples: &[Sample],
    modified: &mut [bool],
) {
    let mut grown = modified.to_vec();
    for group in groups {
        for by_corr in samples_by_corr(group) {
            for by_row in sample_row_runs(&by_corr) {
                if percent_modified(by_row, modified) >= 80.0 {
                    for sample in by_row {
                        grown[sample.index] = true;
                    }
                }
            }
            for by_chan in samples_by_chan_dense(&by_corr) {
                if percent_modified(&by_chan, modified) >= 50.0 {
                    for sample in &by_chan {
                        grown[sample.index] = true;
                    }
                }
            }
        }
    }
    modified.copy_from_slice(&grown);
    for group in samples_by_row_chan(samples).values() {
        if group
            .iter()
            .any(|sample| sample.flag || modified[sample.index])
        {
            for sample in group {
                modified[sample.index] = true;
            }
        }
    }
}

/// Validate channel ranges before mutating flags. Public CASA-style calls
/// have already saved any requested backup by this stage.
fn check_channel_selections(
    ms: &MeasurementSet,
    selections: &BTreeMap<i32, ChannelSelection>,
) -> Result<(), FlaggingError> {
    let path = display_ms_path(ms);
    let spectral_window = ms
        .spectral_window()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("open SPECTRAL_WINDOW: {source}"),
        })?;
    for (&spw, selection) in selections {
        let resolved = usize::try_from(spw)
            .map_err(|_| format!("SPW {spw} is negative"))
            .and_then(|row| {
                let count = spectral_window.num_chan(row).map_err(|source| {
                    format!("read SPECTRAL_WINDOW.NUM_CHAN row {spw}: {source}")
                })?;
                usize::try_from(count).map_err(|_| format!("SPW {spw} has a negative NUM_CHAN"))
            })
            .and_then(|channels| {
                selection
                    .indices(channels)
                    .map(|_| ())
                    .map_err(|source| format!("resolve SPW {spw} channels: {source}"))
            });
        resolved.map_err(|reason| FlaggingError::MutateFlags {
            path: path.clone(),
            reason,
        })?;
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectedFlagRowPolicy {
    ChangedSamples,
    AllSelectedRows,
    FullySelectedRows,
    Preserve,
}

fn selected_flag_row_policy(
    correlations: Option<&CorrelationSelection>,
    unfiltered: SelectedFlagRowPolicy,
) -> SelectedFlagRowPolicy {
    match correlations {
        Some(selection) if selection.decorated_stored_products() => {
            SelectedFlagRowPolicy::FullySelectedRows
        }
        Some(_) => SelectedFlagRowPolicy::Preserve,
        None => unfiltered,
    }
}

fn set_selected_flags(
    ms: &mut MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    correlations: Option<&CorrelationSelection>,
    flag: bool,
    row_policy: SelectedFlagRowPolicy,
) -> Result<ChangeSet, FlaggingError> {
    let row_policy = if is_calibration_table(ms) {
        SelectedFlagRowPolicy::Preserve
    } else {
        row_policy
    };
    let loaded = load_samples(
        ms,
        selected_rows,
        channel_selections,
        None,
        correlations,
        false,
        None,
    )?;
    let mut updates = loaded.flags_by_row;
    let mut changed = ChangeSet::default();
    let mut touched_rows = std::collections::BTreeSet::new();
    let mut coverage = BTreeMap::<usize, usize>::new();
    for sample in loaded.samples {
        if row_policy == SelectedFlagRowPolicy::FullySelectedRows {
            *coverage.entry(sample.row).or_default() += 1;
        }
        if sample.flag != flag
            && let Some(flags) = updates.get_mut(&sample.row)
            && let Some(value) = flags.get_mut(IxDyn(&[sample.corr, sample.chan]))
            && *value != flag
        {
            *value = flag;
            changed.changed_samples += 1;
            touched_rows.insert(sample.row);
        }
    }
    if row_policy == SelectedFlagRowPolicy::AllSelectedRows {
        let path = display_ms_path(ms);
        let row_flags = load_bool_cells(ms.main_table(), selected_rows, FLAG_ROW_COLUMN, &path)?;
        for (&row, flag_row) in selected_rows.iter().zip(row_flags) {
            if !flag_row && updates.get(&row).is_some_and(|flags| !flags.is_empty()) {
                touched_rows.insert(row);
            }
        }
    }
    let fully_selected = coverage
        .into_iter()
        .filter_map(|(row, count)| (updates[&row].len() == count).then_some(row))
        .collect::<std::collections::BTreeSet<_>>();
    if !fully_selected.is_empty() {
        let rows = fully_selected.iter().copied().collect::<Vec<_>>();
        let path = display_ms_path(ms);
        let row_flags = load_bool_cells(ms.main_table(), &rows, FLAG_ROW_COLUMN, &path)?;
        touched_rows.extend(
            rows.into_iter()
                .zip(row_flags)
                .filter_map(|(row, row_flag)| (row_flag != flag).then_some(row)),
        );
    }
    write_touched_flag_updates(
        ms,
        updates,
        touched_rows,
        |row| match row_policy {
            SelectedFlagRowPolicy::FullySelectedRows => fully_selected.contains(&row),
            SelectedFlagRowPolicy::Preserve => false,
            _ => true,
        },
        &mut changed,
    )?;
    Ok(changed)
}

fn apply_flag_mask_with_preloaded(
    samples: &[Sample],
    mask: &[bool],
    mut updates: BTreeMap<usize, ArrayD<bool>>,
    ms: &mut MeasurementSet,
    update_flag_rows: bool,
    flag_value: bool,
) -> Result<ChangeSet, FlaggingError> {
    let (touched_rows, changed_samples) = stage_flag_mask(samples, mask, &mut updates, flag_value);
    let mut changed = ChangeSet {
        changed_samples,
        ..ChangeSet::default()
    };
    write_touched_flag_updates(
        ms,
        updates,
        touched_rows,
        |_| update_flag_rows,
        &mut changed,
    )?;
    Ok(changed)
}

fn apply_tfcrop_mask(
    samples: &[Sample],
    mask: &[bool],
    mut flags: BTreeMap<usize, ArrayD<bool>>,
    axes: &AveragedSampleAxes,
    ms: &mut MeasurementSet,
    update_flag_rows: bool,
    flag_value: bool,
) -> Result<ChangeSet, FlaggingError> {
    if axes.channels_by_row.is_empty() && axes.rows_by_bin.is_empty() {
        let mut changes =
            apply_flag_mask_with_preloaded(samples, mask, flags, ms, update_flag_rows, flag_value)?;
        changes.summary = Some(summary_after_flag_mask(
            samples,
            changes.changed_samples,
            flag_value,
        ));
        return Ok(changes);
    }

    // A flag on a bin is written back to each of its selected physical
    // channels. Preview and apply both count flags on that original axis.
    let coordinates = |sample| tfcrop_flag_coordinates(axes, sample);
    let mut summary = FlagSummary {
        flagged_samples: 0,
        total_samples: 0,
    };
    for (row, corr, chan) in samples.iter().flat_map(coordinates) {
        summary.flagged_samples += usize::from(flags[&row][[corr, chan]]);
        summary.total_samples += 1;
    }
    let coordinates = samples
        .iter()
        .zip(mask)
        .filter(|(_, flag)| **flag)
        .flat_map(|(sample, _)| coordinates(sample));
    let (touched, changed_samples) = stage_flag_coordinates(coordinates, &mut flags, flag_value);
    if flag_value {
        summary.flagged_samples += changed_samples;
    } else {
        summary.flagged_samples = summary.flagged_samples.saturating_sub(changed_samples);
    }
    let mut changed = ChangeSet {
        changed_samples,
        summary: Some(summary),
        ..ChangeSet::default()
    };
    let complete_rows = touched
        .iter()
        .copied()
        .filter(|row| !axes.rows_by_bin.is_empty() && flags[row].iter().all(|flag| *flag))
        .collect::<std::collections::BTreeSet<_>>();
    write_touched_flag_updates(
        ms,
        flags,
        touched,
        |row| update_flag_rows || complete_rows.contains(&row),
        &mut changed,
    )?;
    Ok(changed)
}

fn tfcrop_flag_coordinates<'a>(
    axes: &'a AveragedSampleAxes,
    sample: &'a Sample,
) -> impl Iterator<Item = (usize, usize, usize)> + 'a {
    let rows = axes
        .rows_by_bin
        .get(&sample.row)
        .map_or_else(|| std::slice::from_ref(&sample.row), Vec::as_slice);
    let channels = axes.channels_by_row.get(&sample.row).map_or_else(
        || std::slice::from_ref(&sample.chan),
        |average| bin_members(&average.channels, average.width, sample.chan),
    );
    rows.iter()
        .flat_map(move |&row| channels.iter().map(move |&chan| (row, sample.corr, chan)))
}

fn stage_flag_mask(
    samples: &[Sample],
    mask: &[bool],
    updates: &mut BTreeMap<usize, ArrayD<bool>>,
    flag_value: bool,
) -> (std::collections::BTreeSet<usize>, usize) {
    let coordinates = samples
        .iter()
        .zip(mask)
        .filter(|(_, flag)| **flag)
        .map(|(sample, _)| (sample.row, sample.corr, sample.chan));
    stage_flag_coordinates(coordinates, updates, flag_value)
}

fn stage_flag_coordinates(
    coordinates: impl Iterator<Item = (usize, usize, usize)>,
    updates: &mut BTreeMap<usize, ArrayD<bool>>,
    flag_value: bool,
) -> (std::collections::BTreeSet<usize>, usize) {
    let mut changed_samples = 0;
    let mut touched_rows = std::collections::BTreeSet::<usize>::new();
    for (row, corr, chan) in coordinates {
        let Some(flags) = updates.get_mut(&row) else {
            continue;
        };
        if let Some(value) = flags.get_mut(IxDyn(&[corr, chan]))
            && *value != flag_value
        {
            *value = flag_value;
            changed_samples += 1;
            touched_rows.insert(row);
        }
    }
    (touched_rows, changed_samples)
}

fn write_touched_flag_updates(
    ms: &mut MeasurementSet,
    mut updates: std::collections::BTreeMap<usize, ArrayD<bool>>,
    touched_rows: std::collections::BTreeSet<usize>,
    update_flag_row: impl Fn(usize) -> bool,
    changed: &mut ChangeSet,
) -> Result<(), FlaggingError> {
    let path = display_ms_path(ms);
    let row_flags = !is_calibration_table(ms);
    let mut prepared = Vec::with_capacity(touched_rows.len());
    for row in touched_rows {
        let Some(flags) = updates.remove(&row) else {
            continue;
        };
        prepared.push((row, flags));
    }
    let flag_row_updates = {
        let selected = prepared
            .iter()
            .filter(|(row, _)| row_flags && update_flag_row(*row));
        let rows = selected.clone().map(|(row, _)| *row).collect::<Vec<_>>();
        let current_flag_rows = if row_flags {
            load_bool_cells(ms.main_table(), &rows, FLAG_ROW_COLUMN, &path)?
        } else {
            Vec::new()
        };
        selected
            .zip(current_flag_rows)
            .filter_map(|((row, flags), current)| {
                let flag_row = flags.iter().all(|flag| *flag);
                (current != flag_row).then_some((*row, flag_row))
            })
            .collect::<Vec<_>>()
    };
    ms.main_table_mut()
        .reserve_array_cell_updates(FLAG_COLUMN, prepared.len());
    {
        let columns: &[&str] = if row_flags {
            &[FLAG_COLUMN, FLAG_ROW_COLUMN]
        } else {
            &[FLAG_COLUMN]
        };
        let mut writer = ms
            .main_table_mut()
            .row_accessor_mut()
            .prepare(columns)
            .map_err(|source| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("prepare flag updates: {source}"),
            })?;
        let flag_slot = writer
            .column_index(FLAG_COLUMN)
            .expect("prepared FLAG slot");
        let flag_row_slot = writer.column_index(FLAG_ROW_COLUMN);
        for (row, flags) in &prepared {
            writer
                .seek(*row)
                .and_then(|()| {
                    writer.set_value_at(flag_slot, Value::Array(ArrayValue::Bool(flags.clone())))
                })
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("write FLAG row {row}: {source}"),
                })?;
        }
        for (row, flag_row) in &flag_row_updates {
            let flag_row_slot = flag_row_slot.expect("prepared FLAG_ROW slot");
            writer
                .seek(*row)
                .and_then(|()| {
                    writer.set_value_at(flag_row_slot, Value::Scalar(ScalarValue::Bool(*flag_row)))
                })
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("write FLAG_ROW row {row}: {source}"),
                })?;
        }
    }
    changed.changed_flag_row_rows += flag_row_updates.len();
    for (row, _) in prepared {
        changed.changed_rows += 1;
        changed.changed_row_indices.push(row);
    }
    Ok(())
}

fn load_bool_cells(
    table: &Table,
    rows: &[usize],
    column: &str,
    path: &str,
) -> Result<Vec<bool>, FlaggingError> {
    table
        .column_accessor(column)
        .and_then(|column_accessor| column_accessor.scalar_cells_owned_for_rows(rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("read selected {column} rows: {source}"),
        })?
        .into_iter()
        .enumerate()
        .map(|(index, value)| match value {
            Some(ScalarValue::Bool(value)) => Ok(value),
            Some(other) => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "{column} row {} must be Bool scalar, found {other:?}",
                    rows[index]
                ),
            }),
            None => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!("{column} row {} is undefined", rows[index]),
            }),
        })
        .collect()
}

fn load_i32_cells(
    table: &Table,
    rows: &[usize],
    column: &str,
    path: &str,
) -> Result<Vec<i32>, FlaggingError> {
    table
        .column_accessor(column)
        .and_then(|column_accessor| column_accessor.scalar_cells_owned_for_rows(rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("read selected {column} rows: {source}"),
        })?
        .into_iter()
        .enumerate()
        .map(|(index, value)| match value {
            Some(ScalarValue::Int32(value)) => Ok(value),
            Some(other) => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "{column} row {} must be Int32 scalar, found {other:?}",
                    rows[index]
                ),
            }),
            None => Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!("{column} row {} is undefined", rows[index]),
            }),
        })
        .collect()
}

enum SampleData<'a> {
    Stored(FlagDataColumn),
    Visibility(FlagDataColumn, &'a mut ClipColumns),
}

fn load_time_averaged_tfcrop(
    ms: &MeasurementSet,
    selected: &[usize],
    selections: Option<&BTreeMap<i32, ChannelSelection>>,
    mut columns: Option<&mut ClipColumns>,
    request: &FlagDataRequest,
    averaging: clip_time::TimeAveraging<'_>,
    widths: Option<&[u32]>,
) -> Result<LoadedSamples, FlaggingError> {
    let path = display_ms_path(ms);
    let ddid_to_spw = data_description_spw_map(ms)?;
    let timed = clip_time::ordered_baseline_chunks(
        ms,
        selected,
        request.time_chunk,
        request.combinescans,
        &path,
    )?;
    let flags_by_row = selected
        .iter()
        .copied()
        .zip(clip_time::read_flags(ms.main_table(), selected, &path)?)
        .collect::<BTreeMap<_, _>>();
    let spw_for = |ddid: i32| {
        ddid_to_spw
            .get(&ddid)
            .copied()
            .ok_or_else(|| FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
            })
    };
    let rows_per_block = if let Some(columns) = columns.as_mut() {
        if columns.uses_weight_spectrum() {
            let mut first_rows = BTreeMap::<i32, usize>::new();
            for row in &timed {
                first_rows
                    .entry(spw_for(row.description())?)
                    .and_modify(|first| *first = (*first).min(row.row))
                    .or_insert(row.row);
            }
            columns.prepare_spectrum_fallbacks(ms.main_table(), &first_rows, &path)?;
        }
        let bytes_per_sample = columns.bytes_per_sample(ms.main_table(), &path)?;
        flag_read_plan(ms, selected.len(), bytes_per_sample, true)?
            .rows_per_block
            .max(1)
    } else {
        1
    };
    let mut axes = AveragedSampleAxes::default();
    let mut samples = Vec::new();
    let mut model_roundoff = Vec::new();
    let mut flags_by_bin = BTreeMap::new();
    let mut channel_layouts = HashMap::<(i32, usize), Arc<[usize]>>::new();
    for group in averaging.groups(&timed) {
        let first = &group[0];
        let ddid = first.description();
        let spw = spw_for(ddid)?;
        let row = first.row;
        let nchan = flags_by_row[&row].shape()[1];
        let (channels, width) =
            clip_channel_selection(Some(ddid), &ddid_to_spw, selections, widths, nchan, &path)?;
        let channels = channels.unwrap_or_else(|| (0..nchan).collect());
        let [ant1, ant2] = first.antennas();
        let metadata = RowSampleMetadata {
            row,
            field: first.field(),
            spw,
            scan: first.chunk_key().0[2],
            ant1,
            ant2,
        };
        let mut average = columns
            .as_mut()
            .map(|columns| {
                clip_time::collect_time_bin(ms, group, rows_per_block, columns, &ddid_to_spw, &path)
            })
            .transpose()?
            .map(|(average, _)| average);
        let combined = average
            .as_mut()
            .map(|average| {
                average.reduced_visibility(request.data_column, Some(&channels), width, row, &path)
            })
            .transpose()?;
        let averaged_flags;
        let flag_only_bins;
        let (flags, bins) = if let Some(combined) = &combined {
            (combined.flags, combined.bins.as_ref())
        } else {
            averaged_flags = time_bin_flags(group, &flags_by_row, &path)?;
            flag_only_bins = ChannelBins::new(width, &channels, &averaged_flags);
            (&averaged_flags, flag_only_bins.as_ref())
        };
        let binned_channels;
        let (fit_flags, fit_channels) = if let Some(bins) = bins {
            binned_channels = (0..bins.flags().shape()[1]).collect::<Vec<_>>();
            (bins.flags(), binned_channels.as_slice())
        } else {
            (flags, channels.as_slice())
        };
        let first_sample = samples.len();
        if let Some(combined) = &combined {
            push_row_samples(
                &combined.data,
                fit_flags,
                fit_channels,
                metadata,
                &path,
                request.data_column.name(),
                &mut samples,
            )?;
            if let Some(roundoff) = combined.fourier_roundoff {
                model_roundoff.resize(first_sample, 0.0);
                model_roundoff.extend(samples[first_sample..].iter().map(|sample| {
                    if let Some(bins) = bins {
                        bins.original_channels(sample.chan)
                            .iter()
                            .filter(|&&chan| flags[[sample.corr, chan]] == sample.flag)
                            .map(|&chan| roundoff[[sample.corr, chan]])
                            .fold(0.0, f32::max)
                    } else {
                        roundoff[[sample.corr, sample.chan]]
                    }
                }));
            }
        } else {
            push_flag_samples(fit_flags, fit_channels, None, metadata, &mut samples);
        }
        if selections.is_some() && bins.is_none() {
            flags_by_bin.insert(row, flags.clone());
        }
        if let Some(width) = bins.map(ChannelBins::width) {
            let channels = channel_layouts
                .entry((spw, nchan))
                .or_insert_with(|| channels.into())
                .clone();
            axes.channels_by_row
                .insert(row, AveragedChannels { channels, width });
        }
        axes.rows_by_bin
            .insert(row, group.iter().map(|entry| entry.row).collect());
    }
    axes.flags_by_bin = (!flags_by_bin.is_empty()).then_some(flags_by_bin);
    Ok(LoadedSamples {
        samples,
        flags_by_row,
        model_roundoff,
        axes,
    })
}

fn time_bin_flags(
    rows: &[clip_time::ClipTimedRow],
    flags_by_row: &BTreeMap<usize, ArrayD<bool>>,
    path: &str,
) -> Result<ArrayD<bool>, FlaggingError> {
    let mut flags = flags_by_row[&rows[0].row].clone();
    for row in &rows[1..] {
        let next = &flags_by_row[&row.row];
        if flags.shape() != next.shape() {
            return Err(FlaggingError::MutateFlags {
                path: path.to_string(),
                reason: format!(
                    "TFCrop time bin contains an inconsistent FLAG shape at row {}",
                    row.row
                ),
            });
        }
        ndarray::Zip::from(&mut flags)
            .and(next)
            .for_each(|flag, &next| *flag &= next);
    }
    Ok(flags)
}

/// The selected rows' samples and flags. Each sample carries its selected
/// input value, or zero when the caller reads the flags alone (`None`):
/// manual flagging and extension, as CASA's, need no data column.
fn load_samples(
    ms: &MeasurementSet,
    selected_rows: &[usize],
    channel_selections: Option<&BTreeMap<i32, ChannelSelection>>,
    data: Option<SampleData<'_>>,
    flag_correlations: Option<&CorrelationSelection>,
    retain_channel_overlaps: bool,
    widths: Option<&[u32]>,
) -> Result<LoadedSamples, FlaggingError> {
    let path = display_ms_path(ms);
    let calibration = is_calibration_table(ms);
    let ddid_to_spw = if calibration {
        BTreeMap::new()
    } else {
        data_description_spw_map(ms)?
    };
    let table = ms.main_table();
    let stored = if let Some(SampleData::Stored(column)) = data.as_ref() {
        Some(
            table
                .column_accessor(column.name())
                .and_then(|accessor| accessor.array_cells_owned(selected_rows))
                .map(|cells| (column.name(), cells))
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("read selected {} rows: {source}", column.name()),
                })?,
        )
    } else {
        None
    };
    let flag_cells = table
        .column_accessor(FLAG_COLUMN)
        .and_then(|column| column.array_cells_owned(selected_rows))
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.clone(),
            reason: format!("read selected FLAG rows: {source}"),
        })?
        .into_iter()
        .zip(selected_rows)
        .map(|(cell, row)| match cell {
            Some(ArrayValue::Bool(flags)) if flags.ndim() == 2 => Ok(flags),
            Some(ArrayValue::Bool(_)) => Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("FLAG row {row} is not rank-2"),
            }),
            Some(other) => Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("FLAG row {row} must be bool array, found {other:?}"),
            }),
            None => Err(FlaggingError::MutateFlags {
                path: path.clone(),
                reason: format!("FLAG row {row} is undefined"),
            }),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let row_window = if calibration {
        "SPECTRAL_WINDOW_ID"
    } else {
        "DATA_DESC_ID"
    };
    let ddids = load_i32_cells(table, selected_rows, row_window, &path)?;
    let visibility = if let Some(SampleData::Visibility(column, columns)) = data {
        Some((
            column.name(),
            columns.read(
                ms,
                selected_rows,
                &flag_cells,
                Some(&ddids),
                &ddid_to_spw,
                &path,
            )?,
        ))
    } else {
        None
    };
    let fields = load_i32_cells(table, selected_rows, "FIELD_ID", &path)?;
    let scans = load_i32_cells(table, selected_rows, "SCAN_NUMBER", &path)?;
    let ant1s = load_i32_cells(table, selected_rows, "ANTENNA1", &path)?;
    let ant2s = load_i32_cells(table, selected_rows, "ANTENNA2", &path)?;
    let mut samples = Vec::new();
    let mut flags_by_row = BTreeMap::new();
    let mut model_roundoff = Vec::new();
    let mut averaged_channels = BTreeMap::new();
    let mut channel_layouts = HashMap::<(i32, usize), Arc<[usize]>>::new();
    for (row_slot, (&row, flags)) in selected_rows.iter().zip(flag_cells).enumerate() {
        let ddid = ddids[row_slot];
        let spw = if calibration {
            ddid
        } else {
            ddid_to_spw
                .get(&ddid)
                .copied()
                .ok_or_else(|| FlaggingError::MutateFlags {
                    path: path.clone(),
                    reason: format!("DATA_DESC_ID {ddid} has no SPW mapping"),
                })?
        };
        let nchan = flags.shape()[1];
        let (channels, width) = if widths.is_some() {
            clip_channel_selection(
                Some(ddid),
                &ddid_to_spw,
                channel_selections,
                widths,
                nchan,
                &path,
            )?
        } else {
            let channels = channel_selections
                .and_then(|selectors| selectors.get(&spw))
                .map(|selection| {
                    if retain_channel_overlaps {
                        selection.indices_with_overlaps(nchan)
                    } else {
                        selection.indices(nchan)
                    }
                    .map_err(|source| FlaggingError::MutateFlags {
                        path: path.clone(),
                        reason: format!("resolve SPW {spw} channels: {source}"),
                    })
                })
                .transpose()?;
            (channels, 0)
        };
        let channels = channels.unwrap_or_else(|| (0..nchan).collect());
        let bins = ChannelBins::new(width, &channels, &flags);
        let data = if let Some((column, cells)) = &stored {
            let value = checked_row_data(cells.get(row_slot), &flags, row, column, &path)?;
            Some((*column, Cow::Borrowed(value)))
        } else if let Some((column, cells)) = &visibility {
            Some((
                *column,
                cells.row(row_slot, &flags, bins.as_ref(), row, &path)?,
            ))
        } else {
            None
        };
        let binned_channels;
        let (fit_flags, fit_channels) = if let Some(bins) = &bins {
            binned_channels = (0..bins.flags().shape()[1]).collect::<Vec<_>>();
            (bins.flags(), binned_channels.as_slice())
        } else {
            (&flags, channels.as_slice())
        };
        let metadata = RowSampleMetadata {
            row,
            field: fields[row_slot],
            spw,
            scan: scans[row_slot],
            ant1: ant1s[row_slot],
            ant2: ant2s[row_slot],
        };
        let first_sample = samples.len();
        match data {
            Some((column, value)) => push_row_samples(
                value.as_ref(),
                fit_flags,
                fit_channels,
                metadata,
                &path,
                column,
                &mut samples,
            )?,
            None => push_flag_samples(
                fit_flags,
                fit_channels,
                flag_correlations.map(|selection| selection.planes(ddid)),
                metadata,
                &mut samples,
            ),
        }
        if let Some((_, cells)) = &visibility
            && let Some(roundoff) = cells.fourier_residual_roundoff(row_slot, &flags, row, &path)?
        {
            model_roundoff.resize(first_sample, 0.0);
            model_roundoff.extend(samples[first_sample..].iter().map(|sample| {
                if let Some(bins) = &bins {
                    bins.original_channels(sample.chan)
                        .iter()
                        .filter(|&&chan| flags[[sample.corr, chan]] == sample.flag)
                        .map(|&chan| roundoff[[sample.corr, chan]])
                        .fold(0.0, f32::max)
                } else {
                    roundoff[[sample.corr, sample.chan]]
                }
            }));
        }
        if let Some(width) = bins.map(|bins| bins.width()) {
            // The selection and effective width are fixed for this window
            // and input channel count. Reuse the physical axis across rows.
            let channels = channel_layouts
                .entry((spw, nchan))
                .or_insert_with(|| channels.into())
                .clone();
            averaged_channels.insert(row, AveragedChannels { channels, width });
        }
        flags_by_row.insert(row, flags);
    }
    Ok(LoadedSamples {
        samples,
        flags_by_row,
        model_roundoff,
        axes: AveragedSampleAxes {
            channels_by_row: averaged_channels,
            ..Default::default()
        },
    })
}

/// `row`'s cell of `column`: a numeric array of its flags' shape.
fn checked_row_data<'a>(
    cell: Option<&'a Option<ArrayValue>>,
    flags: &ArrayD<bool>,
    row: usize,
    column: &str,
    path: &str,
) -> Result<&'a ArrayValue, FlaggingError> {
    let invalid = |reason: String| FlaggingError::MutateFlags {
        path: path.to_string(),
        reason,
    };
    let value = cell
        .and_then(Option::as_ref)
        .ok_or_else(|| invalid(format!("{column} row {row} is undefined")))?;
    let shape = numeric_array_value_shape(value).ok_or_else(|| {
        invalid(format!(
            "{column} row {row} must be numeric array, found {value:?}"
        ))
    })?;
    if shape != flags.shape() {
        return Err(invalid(format!(
            "{column} and FLAG shapes differ on row {row}: {shape:?} vs {:?}",
            flags.shape()
        )));
    }
    Ok(value)
}

/// The samples of a row read for its flags alone, each value zero.
fn push_flag_samples(
    flags: &ArrayD<bool>,
    channels: &[usize],
    planes: Option<&[usize]>,
    metadata: RowSampleMetadata,
    samples: &mut Vec<Sample>,
) {
    for corr in 0..flags.shape()[0] {
        if planes.is_some_and(|indices| indices.binary_search(&corr).is_err()) {
            continue;
        }
        for &chan in channels {
            push_sample(samples, metadata, corr, chan, 0.0, 0.0, flags[[corr, chan]]);
        }
    }
}

/// Whether clip mode with no clip range flags the amplitude `value`: non-finite,
/// and with `zeros` a value at or below `f32::EPSILON` too, as CASA's
/// `FlagAgentBase::isNaN` and `isNaNOrZero` test it.
fn is_casa_clip(value: f64, zeros: bool) -> bool {
    !value.is_finite() || (zeros && value <= f64::from(f32::EPSILON))
}

fn is_casa_clip_complex32(value: Complex32, zeros: bool) -> bool {
    !value.norm().is_finite()
        || (zeros
            && f64::from(value.re * value.re + value.im * value.im)
                <= f64::from(f32::EPSILON) * f64::from(f32::EPSILON))
}

fn is_casa_clip_complex64(value: Complex64, zeros: bool) -> bool {
    !value.norm().is_finite()
        || (zeros
            && value.re * value.re + value.im * value.im
                <= f64::from(f32::EPSILON) * f64::from(f32::EPSILON))
}

fn numeric_array_value_shape(value: &ArrayValue) -> Option<&[usize]> {
    match value {
        ArrayValue::Complex32(values) => Some(values.shape()),
        ArrayValue::Complex64(values) => Some(values.shape()),
        ArrayValue::Float32(values) => Some(values.shape()),
        ArrayValue::Float64(values) => Some(values.shape()),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct RowSampleMetadata {
    row: usize,
    field: i32,
    spw: i32,
    scan: i32,
    ant1: i32,
    ant2: i32,
}

fn push_row_samples(
    data: &ArrayValue,
    flags: &ArrayD<bool>,
    channels: &[usize],
    metadata: RowSampleMetadata,
    path: &str,
    data_column: &str,
    samples: &mut Vec<Sample>,
) -> Result<(), FlaggingError> {
    let flag_view = flags
        .view()
        .into_dimensionality::<Ix2>()
        .map_err(|source| FlaggingError::MutateFlags {
            path: path.to_string(),
            reason: format!("FLAG row {} must be rank-2: {source}", metadata.row),
        })?;
    match data {
        ArrayValue::Complex32(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    let value = values[(corr, chan)];
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        f64::from(value.re),
                        f64::from(value.im),
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Complex64(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    let value = values[(corr, chan)];
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        value.re,
                        value.im,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Float32(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        f64::from(values[(corr, chan)]),
                        0.0,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        ArrayValue::Float64(values) => {
            let values = values
                .view()
                .into_dimensionality::<Ix2>()
                .map_err(|source| FlaggingError::MutateFlags {
                    path: path.to_string(),
                    reason: format!(
                        "{data_column} row {} must be rank-2: {source}",
                        metadata.row
                    ),
                })?;
            for corr in 0..values.shape()[0] {
                for &chan in channels {
                    push_sample(
                        samples,
                        metadata,
                        corr,
                        chan,
                        values[(corr, chan)],
                        0.0,
                        flag_view[(corr, chan)],
                    );
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn push_sample(
    samples: &mut Vec<Sample>,
    metadata: RowSampleMetadata,
    corr: usize,
    chan: usize,
    real: f64,
    imag: f64,
    flag: bool,
) {
    samples.push(Sample {
        index: samples.len(),
        row: metadata.row,
        corr,
        chan,
        amp: real.hypot(imag),
        real,
        imag,
        flag,
        field: metadata.field,
        spw: metadata.spw,
        scan: metadata.scan,
        ant1: metadata.ant1,
        ant2: metadata.ant2,
    });
}

fn summarize_flags(
    ms: &MeasurementSet,
    selected_rows: &[usize],
) -> Result<FlagSummary, FlaggingError> {
    let mut flagged_samples = 0usize;
    let mut total_samples = 0usize;
    for &row in selected_rows {
        let flags = clone_flag_matrix(ms, row)?;
        flagged_samples += flags.iter().filter(|flag| **flag).count();
        total_samples += flags.len();
    }
    Ok(FlagSummary {
        flagged_samples,
        total_samples,
    })
}

fn rflag_threshold_maps_for_groups(
    groups: &[(AutoflagChunk, Vec<Sample>)],
    request: &FlagDataRequest,
    context: RflagContext<'_>,
) -> Result<RFlagThresholds, FlaggingError> {
    if request.rflag_window.is_none() {
        return Ok(RFlagThresholds::default());
    }
    let mut roundoff_buf = if context.model_roundoff.is_empty() {
        Vec::new()
    } else {
        vec![0.0; context.model_roundoff.len()]
    };
    rflag_threshold_maps_with_roundoff(groups, request, context, &mut roundoff_buf)
}

fn rflag_threshold_maps_with_roundoff(
    groups: &[(AutoflagChunk, Vec<Sample>)],
    request: &FlagDataRequest,
    context: RflagContext<'_>,
    roundoff_buf: &mut [f32],
) -> Result<RFlagThresholds, FlaggingError> {
    let Some(window_size) = request.rflag_window else {
        return Ok(RFlagThresholds::default());
    };
    let mut histograms = RFlagHistograms::default();
    for (chunk_key, group) in groups {
        let ddid = context.correlations.map(|_| chunk_key.0[4]);
        rflag_accumulate_group_histograms(
            ddid,
            group,
            &mut histograms,
            window_size,
            context,
            roundoff_buf,
        )?;
    }
    Ok(histograms.into_thresholds(request, context.casa_arithmetic))
}

#[derive(Debug, Clone, Copy)]
enum TfcropDirection {
    Freq,
    Time,
}

/// Model fitted to the selected time or frequency axis.
#[derive(Debug, Clone, Copy)]
pub enum TfcropFit {
    /// A single line.
    Line,
    /// A piecewise polynomial.
    Poly,
}

/// Ordered TFCrop axis passes.
#[derive(Debug, Clone, Copy)]
pub enum TfcropPasses {
    /// Frequency alone.
    Frequency,
    /// Time alone.
    Time,
    /// Frequency followed by time.
    FrequencyThenTime,
    /// Time followed by frequency.
    TimeThenFrequency,
}

impl TfcropPasses {
    fn directions(self) -> &'static [TfcropDirection] {
        use TfcropDirection::{Freq, Time};
        match self {
            Self::Frequency => &[Freq],
            Self::Time => &[Time],
            Self::FrequencyThenTime => &[Freq, Time],
            Self::TimeThenFrequency => &[Time, Freq],
        }
    }
}

/// Additional statistics evaluated around each candidate sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TfcropWindowStats {
    /// Skip the neighborhood pass.
    None,
    /// Mean absolute deviation in the window.
    Sum,
    /// Root mean square deviation in the window.
    Std,
    /// Both statistics.
    Both,
}

/// Model fitting, axis order and optional neighborhood tests for TFCrop.
#[derive(Debug, Clone, Copy)]
pub struct TfcropOptions {
    /// Model along time.
    pub time_fit: TfcropFit,
    /// Model along frequency.
    pub frequency_fit: TfcropFit,
    /// Axis passes in the order they run.
    pub passes: TfcropPasses,
    /// Sliding-window statistics.
    pub window_stats: TfcropWindowStats,
    /// Maximum piece count for a polynomial fit.
    pub max_pieces: usize,
    /// Number of neighbors per side in a sliding window.
    pub half_window: usize,
}

impl Default for TfcropOptions {
    fn default() -> Self {
        Self {
            time_fit: TfcropFit::Line,
            frequency_fit: TfcropFit::Poly,
            passes: TfcropPasses::FrequencyThenTime,
            window_stats: TfcropWindowStats::None,
            max_pieces: 7,
            half_window: 1,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct TfcropArithmetic<'a> {
    casa_arithmetic: bool,
    model_roundoff: &'a [f32],
}

fn tfcrop_fit_base_and_flag(
    samples: &[Sample],
    direction: TfcropDirection,
    cutoff: f64,
    options: &TfcropOptions,
    arithmetic: TfcropArithmetic<'_>,
    modified: &mut [bool],
    direction_flags: &mut [bool],
) {
    let TfcropArithmetic {
        casa_arithmetic,
        model_roundoff,
    } = arithmetic;
    if samples.is_empty() {
        return;
    }
    let rows = unique_sample_rows(samples);
    let chans = unique_sample_chans(samples);
    let (n0, n1) = match direction {
        TfcropDirection::Freq => (chans.len(), rows.len()),
        TfcropDirection::Time => (rows.len(), chans.len()),
    };

    let dense = rows.len().checked_mul(chans.len()) == Some(samples.len())
        && rows.windows(2).all(|pair| pair[0] < pair[1])
        && samples
            .chunks_exact(chans.len())
            .zip(&rows)
            .all(|(cells, row)| {
                cells
                    .iter()
                    .zip(&chans)
                    .all(|(cell, chan)| cell.row == *row && cell.chan == *chan)
            });
    let sparse = (!dense).then(|| {
        samples
            .iter()
            .map(|sample| ((sample.row, sample.chan), sample))
            .collect::<HashMap<_, _>>()
    });
    let sample_at = |i0, i1| {
        let (row, chan) = tfcrop_row_chan(direction, i0, i1);
        if let Some(sparse) = &sparse {
            sparse.get(&(rows[row], chans[chan])).copied()
        } else {
            samples.get(row * chans.len() + chan)
        }
    };

    let mut avg_dat = vec![0.0f32; n0];
    let mut avg_flag = vec![false; n0];
    let mut avg_roundoff = (!model_roundoff.is_empty()).then(|| vec![0.0; n0]);
    let mut all_zeros = true;
    for i0 in 0..n0 {
        let mut sum = 0.0f32;
        let mut count = 0usize;
        for i1 in 0..n1 {
            if let Some(sample) = sample_at(i0, i1)
                && !sample_is_modified(sample, modified)
            {
                sum += tfcrop_value(sample);
                if let Some(roundoff) = &mut avg_roundoff {
                    roundoff[i0] += model_roundoff.get(sample.index).copied().unwrap_or(0.0);
                }
                count += 1;
            }
        }
        if count > 0 {
            avg_dat[i0] = sum / count as f32;
            if let Some(roundoff) = &mut avg_roundoff {
                roundoff[i0] /= count as f32;
            }
            all_zeros = false;
        } else {
            avg_flag[i0] = true;
        }
    }
    if all_zeros {
        return;
    }

    let fit = match direction {
        TfcropDirection::Freq => options.frequency_fit,
        TfcropDirection::Time => options.time_fit,
    };
    let avg_fit = match fit {
        TfcropFit::Line => {
            tfcrop_fit_piecewise_poly(&avg_dat, &mut avg_flag, 1, 1, casa_arithmetic)
        }
        TfcropFit::Poly => tfcrop_fit_piecewise_poly(
            &avg_dat,
            &mut avg_flag,
            options.max_pieces,
            4,
            casa_arithmetic,
        ),
    };

    let mut normalized_roundoff = avg_roundoff.as_ref().map(|_| vec![0.0; n0]);
    // The fitted baseline shares information across the axis, including
    // channels at which the model has no spectral coverage.
    let fit_roundoff = avg_roundoff.as_ref().map_or(0.0, |bounds| {
        bounds
            .iter()
            .copied()
            .filter(|bound| bound.is_finite())
            .fold(0.0, f32::max)
    });
    for i1 in 0..n1 {
        let mut normalized = vec![0.0f32; n0];
        let mut flags = vec![false; n0];
        if let Some(roundoff) = &mut normalized_roundoff {
            roundoff.fill(0.0);
        }
        for i0 in 0..n0 {
            if let Some(sample) = sample_at(i0, i1) {
                flags[i0] = sample_is_modified(sample, modified);
                if !flags[i0] && (casa_arithmetic || avg_fit[i0].abs() > f32::EPSILON) {
                    let fit = avg_fit[i0];
                    normalized[i0] = tfcrop_value(sample) / fit;
                    if let Some(roundoff) = &mut normalized_roundoff {
                        // The fitted baseline also uses residuals made with the
                        // reconstructed model. Compare above both uncertainties.
                        let measured = model_roundoff.get(sample.index).copied().unwrap_or(0.0);
                        let uncertainty = (measured + fit_roundoff) / fit.abs();
                        if uncertainty.is_finite() {
                            roundoff[i0] = uncertainty;
                        }
                    }
                }
            } else {
                flags[i0] = true;
            }
        }

        let mut previous_sd = 0.0f32;
        let mut sd = 0.0;
        for _ in 0..5 {
            sd = tfcrop_std_about_mean(&normalized, &flags, 1.0);
            if !casa_arithmetic && (!sd.is_finite() || sd <= f32::EPSILON) {
                break;
            }
            for i0 in 0..n0 {
                let deviation = (normalized[i0] - 1.0).abs();
                if !flags[i0]
                    && deviation > cutoff as f32 * sd
                    && normalized_roundoff
                        .as_ref()
                        .is_none_or(|bounds| bounds[i0] == 0.0 || deviation > bounds[i0])
                {
                    flags[i0] = true;
                }
            }
            if (previous_sd - sd).abs() < 0.1 {
                break;
            }
            previous_sd = sd;
        }

        tfcrop_window_flags(
            &normalized,
            normalized_roundoff.as_deref(),
            &mut flags,
            options,
            cutoff as f32 * sd,
        );

        for (i0, &flagged) in flags.iter().enumerate() {
            if !flagged {
                continue;
            }
            if let Some(sample) = sample_at(i0, i1)
                && !modified[sample.index]
            {
                modified[sample.index] = true;
                direction_flags[sample.index] = true;
            }
        }
    }
}

fn tfcrop_window_flags(
    normalized: &[f32],
    roundoff: Option<&[f32]>,
    flags: &mut [bool],
    options: &TfcropOptions,
    threshold: f32,
) {
    let half_width = options.half_window;
    let stats = options.window_stats;
    if stats == TfcropWindowStats::None || half_width == 0 {
        return;
    }
    let width = half_width.saturating_mul(2).saturating_add(1);
    for (start, samples) in normalized.windows(width).enumerate() {
        let deviations = || {
            samples.iter().enumerate().map(|(offset, value)| {
                let deviation = (*value - 1.0).abs();
                roundoff.map_or(deviation, |bounds| {
                    let bound = bounds[start + offset];
                    if deviation <= bound {
                        0.0
                    } else {
                        deviation - bound
                    }
                })
            })
        };
        let exceeds_mean = matches!(stats, TfcropWindowStats::Sum | TfcropWindowStats::Both)
            && deviations().sum::<f32>() / width as f32 > threshold;
        let exceeds_rms = matches!(stats, TfcropWindowStats::Std | TfcropWindowStats::Both)
            && (deviations().map(|value| value * value).sum::<f32>() / width as f32).sqrt()
                > threshold;
        flags[start + half_width] |= exceeds_mean || exceeds_rms;
    }
}

fn tfcrop_row_chan(direction: TfcropDirection, axis0: usize, axis1: usize) -> (usize, usize) {
    match direction {
        TfcropDirection::Freq => (axis1, axis0),
        TfcropDirection::Time => (axis0, axis1),
    }
}

fn tfcrop_value(sample: &Sample) -> f32 {
    sample.amp as f32
}

fn sample_is_modified(sample: &Sample, modified: &[bool]) -> bool {
    sample.flag || modified.get(sample.index).copied().unwrap_or(false)
}

fn tfcrop_fit_piecewise_poly(
    data: &[f32],
    flag: &mut [bool],
    max_pieces: usize,
    max_degree: usize,
    casa_arithmetic: bool,
) -> Vec<f32> {
    let mut tdata = data.to_vec();
    for i in 0..tdata.len() {
        if tdata[i] != 0.0 {
            continue;
        }
        if i == 0 {
            if let Some((index, _)) = tdata
                .iter()
                .enumerate()
                .skip(1)
                .find(|(_, value)| **value != 0.0)
            {
                tdata[i] = tdata[index];
            }
        } else {
            let left = (0..=i).rev().find(|index| tdata[*index] != 0.0);
            let right = (i + 1..tdata.len()).find(|index| tdata[*index] != 0.0);
            match (left, right) {
                (Some(left), Some(right)) => tdata[i] = (tdata[left] + tdata[right]) / 2.0,
                (Some(left), None) => tdata[i] = tdata[left],
                (None, Some(right)) => tdata[i] = tdata[right],
                (None, None) => {}
            }
        }
    }
    for (index, value) in tdata.iter().enumerate() {
        if *value == 0.0 {
            flag[index] = true;
        }
    }

    let mut fitted = tdata.clone();
    for iteration in 0..5usize {
        let npieces = (2 * iteration + 1).min(max_pieces);
        let mut degree = 1usize;
        if iteration > 1 {
            degree = 2;
        }
        if iteration > 2 {
            degree = 3;
        }
        degree = degree.min(max_degree);
        let piece_size = tdata.len() / npieces;
        let leftover = tdata.len() % npieces;
        let leftover_front = leftover / 2;
        for piece in 0..npieces {
            let (left, right) = if npieces > 1 {
                let mut left = leftover_front + piece * piece_size;
                let mut right = left + piece_size;
                if piece == 0 {
                    left = 0;
                    right = leftover_front + piece_size;
                }
                if piece == npieces - 1 {
                    right = tdata.len() - 1;
                }
                (left, right)
            } else {
                (0, tdata.len() - 1)
            };
            if degree == 1 {
                tfcrop_line_fit(&tdata, flag, &mut fitted, left, right, casa_arithmetic);
            } else {
                tfcrop_poly_fit(
                    &tdata,
                    flag,
                    &mut fitted,
                    left,
                    right,
                    degree,
                    casa_arithmetic,
                );
            }
        }

        for i in 2..tdata.len().saturating_sub(2) {
            let win_start = i - 2;
            let win_end = (i + 2).min(tdata.len() - 1);
            if win_end <= win_start {
                break;
            }
            let sum = (win_start..=win_end)
                .map(|index| fitted[index])
                .sum::<f32>();
            fitted[i] = sum / (win_end - win_start + 1) as f32;
        }

        let sd = tfcrop_std_about_fit(&tdata, flag, &fitted);
        if !sd.is_finite() {
            continue;
        }
        let tolerance = if iteration >= 2 { 2.0 } else { 3.0 };
        for i in 0..tdata.len() {
            if tdata[i] - fitted[i] > tolerance * sd {
                flag[i] = true;
            }
        }
    }
    fitted
}

fn tfcrop_line_fit(
    data: &[f32],
    flag: &[bool],
    fit: &mut [f32],
    left: usize,
    right: usize,
    casa_arithmetic: bool,
) {
    let mean = tfcrop_mean(data, flag);
    let sd = tfcrop_std_about_mean(data, flag, mean);
    if !casa_arithmetic && (!sd.is_finite() || sd <= f32::EPSILON) {
        for fitted in fit.iter_mut().take(right + 1).skip(left) {
            *fitted = mean;
        }
        return;
    }
    let mut sum_w = 0.0f32;
    let mut sum_x = 0.0f32;
    let mut sum_y = 0.0f32;
    let mut sum_xx = 0.0f32;
    let mut sum_xy = 0.0f32;
    let variance = sd * sd;
    let weight = 1.0f32 / variance;
    // CASA lets a degenerate fit produce IEEE non-finite values. The
    // normalized pass then leaves the flags alone in that fit direction.
    let weighted = |term| {
        if casa_arithmetic {
            term / variance
        } else {
            term * weight
        }
    };
    for i in left..=right {
        if !flag[i] {
            let x = i as f32;
            sum_w += weight;
            sum_x += weighted(x);
            sum_y += weighted(data[i]);
            sum_xx += weighted(x * x);
            sum_xy += weighted(x * data[i]);
        }
    }
    let denom = sum_w * sum_xx - sum_x * sum_x;
    if !casa_arithmetic && denom.abs() <= f32::EPSILON {
        for fitted in fit.iter_mut().take(right + 1).skip(left) {
            *fitted = mean;
        }
        return;
    }
    let intercept = (sum_xx * sum_y - sum_x * sum_xy) / denom;
    let slope = (sum_w * sum_xy - sum_x * sum_y) / denom;
    for (i, fitted) in fit.iter_mut().enumerate().take(right + 1).skip(left) {
        *fitted = intercept + slope * i as f32;
    }
}

fn tfcrop_poly_fit(
    data: &[f32],
    flag: &[bool],
    fit: &mut [f32],
    left: usize,
    right: usize,
    degree: usize,
    casa_arithmetic: bool,
) {
    // CASA's LinearFit forms a small normal system before solving it. The
    // degrees used by the CASA-shaped fit need only three or four terms.
    if casa_arithmetic
        && match degree {
            2 => tfcrop_casa_poly_fit::<3>(data, flag, fit, left, right),
            3 => tfcrop_casa_poly_fit::<4>(data, flag, fit, left, right),
            _ => false,
        }
    {
        return;
    }
    let rows = (left..=right)
        .filter(|index| !flag[*index])
        .map(|index| {
            let x = index as f64 + 1.0;
            let basis = (0..=degree)
                .map(|power| x.powi(power as i32))
                .collect::<Vec<_>>();
            (basis, f64::from(data[index]), 1.0)
        })
        .collect::<Vec<_>>();
    if rows.len() <= degree {
        tfcrop_line_fit(data, flag, fit, left, right, casa_arithmetic);
        return;
    }
    let Some(solution) = solve_weighted_least_squares(&rows, degree + 1) else {
        tfcrop_line_fit(data, flag, fit, left, right, casa_arithmetic);
        return;
    };
    for (i, fitted) in fit.iter_mut().enumerate().take(right + 1).skip(left) {
        let x = i as f64 + 1.0;
        *fitted = solution
            .iter()
            .enumerate()
            .map(|(power, coefficient)| coefficient * x.powi(power as i32))
            .sum::<f64>() as f32;
    }
}

fn tfcrop_casa_poly_fit<const N: usize>(
    data: &[f32],
    flag: &[bool],
    fit: &mut [f32],
    left: usize,
    right: usize,
) -> bool {
    let mut normal = [[0.0; N]; N];
    let mut known = [0.0; N];
    let mut count = 0;
    for index in (left..=right).filter(|&index| !flag[index]) {
        let sample = f64::from(data[index]);
        let x = index as f64 + 1.0;
        let powers = std::array::from_fn::<_, N, _>(|power| x.powi(power as i32));
        for (column, &value) in powers.iter().enumerate() {
            known[column] += value * sample;
            for (other, &factor) in powers.iter().enumerate().skip(column) {
                normal[column][other] += value * factor;
            }
        }
        count += 1;
    }
    if count < N {
        return false;
    }
    // Non-SVD LinearFit uses zero collinearity tolerance and returns its
    // initial zero coefficients if inversion fails.
    let solution =
        solve_symmetric_ldlt_casacore_with_precision(normal, known, 0.0).unwrap_or([0.0; N]);
    for (index, value) in fit.iter_mut().enumerate().take(right + 1).skip(left) {
        let x = index as f64 + 1.0;
        // FlagAgentTimeFreqCrop::polyFit rounds each sum to Float.
        *value = solution
            .iter()
            .enumerate()
            .fold(0.0f32, |sum, (power, coefficient)| {
                (f64::from(sum) + coefficient * x.powi(power as i32)) as f32
            });
    }
    true
}

fn tfcrop_mean(values: &[f32], flags: &[bool]) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for (value, flag) in values.iter().zip(flags.iter()) {
        if !*flag {
            sum += value;
            count += 1;
        }
    }
    if count == 0 { 0.0 } else { sum / count as f32 }
}

fn tfcrop_std_about_fit(values: &[f32], flags: &[bool], fit: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for ((value, flag), fitted) in values.iter().zip(flags.iter()).zip(fit.iter()) {
        if !*flag {
            count += 1;
            sum += (value - fitted) * (value - fitted);
        }
    }
    if count == 0 {
        0.0
    } else {
        (sum / count as f32).sqrt()
    }
}

fn tfcrop_std_about_mean(values: &[f32], flags: &[bool], mean: f32) -> f32 {
    let mut sum = 0.0f32;
    let mut count = 0usize;
    for (value, flag) in values.iter().zip(flags.iter()) {
        if !*flag {
            count += 1;
            sum += (value - mean) * (value - mean);
        }
    }
    if count == 0 {
        0.0
    } else {
        (sum / count as f32).sqrt()
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    median_in_place(&mut values)
}

fn median_in_place(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

#[derive(Debug, Default)]
struct RFlagHistograms {
    time: BTreeMap<FieldSpwKey, ChannelHistogram>,
    freq: BTreeMap<FieldSpwKey, ChannelHistogram>,
}

impl RFlagHistograms {
    /// Unscaled thresholds for the public calculate report and flagging.
    /// CASA applies the scale factors only when testing for flags.
    fn into_thresholds(self, request: &FlagDataRequest, casa_arithmetic: bool) -> RFlagThresholds {
        let cutoffs = |histograms: BTreeMap<FieldSpwKey, ChannelHistogram>,
                       scalar: Option<f64>,
                       supplied: &BTreeMap<FieldSpwKey, f64>| {
            histograms
                .into_iter()
                .map(|(key, histogram)| {
                    let threshold = supplied
                        .get(&key)
                        .copied()
                        .filter(|&value| value > 0.0)
                        .or(scalar.filter(|&value| !casa_arithmetic || value > 0.0))
                        .unwrap_or_else(|| histogram.threshold());
                    (key, threshold)
                })
                .collect()
        };
        RFlagThresholds {
            timedev: cutoffs(self.time, request.timedev, &request.timedev_by_field_spw),
            freqdev: cutoffs(self.freq, request.freqdev, &request.freqdev_by_field_spw),
        }
    }
}

#[derive(Debug, Default)]
struct RFlagThresholds {
    timedev: BTreeMap<FieldSpwKey, f64>,
    freqdev: BTreeMap<FieldSpwKey, f64>,
}

impl RFlagThresholds {
    fn scaled(mut self, request: &FlagDataRequest, casa_arithmetic: bool) -> Self {
        // CASA uses -1 for an unavailable applied cutoff. A negative scalar
        // skips the fitted cutoff, and a negative scale skips the prepass
        // histogram. CASA scales its sentinel together with defined cutoffs.
        let applied = |threshold: f64, scalar: Option<f64>, supplied: Option<f64>, scale: f64| {
            if let Some(value) = supplied {
                return if value > 0.0 { value * scale } else { -scale };
            }
            if casa_arithmetic
                && !scalar.is_some_and(|value| value > 0.0)
                && (threshold == 0.0 || scale < 0.0 || scalar.is_some_and(|value| value < 0.0))
            {
                -scale
            } else {
                threshold * scale
            }
        };
        for (key, value) in &mut self.timedev {
            *value = applied(
                *value,
                request.timedev,
                request.timedev_by_field_spw.get(key).copied(),
                request.timedevscale,
            );
        }
        for (key, value) in &mut self.freqdev {
            *value = applied(
                *value,
                request.freqdev,
                request.freqdev_by_field_spw.get(key).copied(),
                request.freqdevscale,
            );
        }
        self
    }

    fn public_report(&self, request: &FlagDataRequest, active: bool) -> RFlagThresholdReport {
        let rows = |values: &BTreeMap<FieldSpwKey, f64>,
                    scalar: Option<f64>,
                    supplied: &BTreeMap<FieldSpwKey, f64>| {
            if !active || request.rflag_window.is_none() {
                return Vec::new();
            }
            let mut report = supplied.clone();
            if !scalar.is_some_and(|value| value > 0.0) {
                report.extend(values);
            }
            report
                .into_iter()
                .map(|((field, spw), value)| [f64::from(field), f64::from(spw), value])
                .collect()
        };
        RFlagThresholdReport {
            timedev: rows(
                &self.timedev,
                request.timedev,
                &request.timedev_by_field_spw,
            ),
            freqdev: rows(
                &self.freqdev,
                request.freqdev,
                &request.freqdev_by_field_spw,
            ),
        }
    }

    fn representative_pair(&self) -> (f64, f64) {
        (
            representative_threshold(&self.timedev),
            representative_threshold(&self.freqdev),
        )
    }
}

#[derive(Debug, Clone)]
struct ChannelHistogram {
    sum: Vec<f64>,
    counts: Vec<f64>,
}

impl ChannelHistogram {
    fn new(nchan: usize) -> Self {
        Self {
            sum: vec![0.0; nchan],
            counts: vec![0.0; nchan],
        }
    }

    fn add(&mut self, chan: usize, value: f64) {
        if let (Some(sum), Some(count)) = (self.sum.get_mut(chan), self.counts.get_mut(chan)) {
            *sum += value;
            *count += 1.0;
        }
    }

    fn threshold(&self) -> f64 {
        let samples = self
            .sum
            .iter()
            .zip(self.counts.iter())
            .map(|(sum, count)| if *count > 0.0 { *sum / *count } else { 0.0 })
            .collect::<Vec<_>>();
        let med = median(samples.clone());
        let mad = median(samples.iter().map(|value| (*value - med).abs()).collect());
        med + 1.4826 * mad
    }
}

fn representative_threshold(thresholds: &BTreeMap<FieldSpwKey, f64>) -> f64 {
    if thresholds.is_empty() {
        return f64::INFINITY;
    }
    median(thresholds.values().copied().collect())
}

fn rflag_accumulate_group_histograms(
    description: Option<i32>,
    group: &[Sample],
    histograms: &mut RFlagHistograms,
    window_size: usize,
    context: RflagContext<'_>,
    roundoff_buf: &mut [f32],
) -> Result<(), FlaggingError> {
    let Some(first) = group.first() else {
        return Ok(());
    };
    let planes = samples_by_corr(group);
    let projections = rflag_group_projections(group, description, context)?;
    if projections.is_empty() {
        return Ok(());
    }
    let key = (first.field, first.spw);
    // CASA histograms have one bin per selected channel.
    let channels = context.casa_arithmetic.then(|| {
        let mut channels = unique_sample_chans(group);
        channels.sort_unstable();
        channels
    });
    let nchan = channels.as_ref().map_or_else(
        || group.iter().map(|sample| sample.chan).max().unwrap_or(0) + 1,
        Vec::len,
    );
    let index = |channel| {
        channels.as_ref().map_or(channel, |channels| {
            channels.binary_search(&channel).expect("loaded channel")
        })
    };
    histograms
        .time
        .entry(key)
        .or_insert_with(|| ChannelHistogram::new(nchan));
    histograms
        .freq
        .entry(key)
        .or_insert_with(|| ChannelHistogram::new(nchan));
    for projection in projections {
        let Some(series) = tfcrop_series(
            &planes,
            projection,
            None,
            &[],
            context.model_roundoff,
            roundoff_buf,
        ) else {
            continue;
        };
        let time_hist = histograms
            .time
            .get_mut(&key)
            .expect("initialized time histogram");
        for by_chan in samples_by_chan_dense(&series.samples) {
            for (start, stop) in rflag_time_windows(by_chan.len(), window_size) {
                let window = &by_chan[start..=stop];
                if let Some(value) = rflag_time_std_total(
                    window,
                    context.casa_arithmetic,
                    RflagRoundoff::Slice(roundoff_buf),
                ) {
                    time_hist.add(index(by_chan[0].chan), value);
                }
            }
        }
        let freq_hist = histograms
            .freq
            .get_mut(&key)
            .expect("initialized frequency histogram");
        for by_row in sample_row_runs(&series.samples) {
            let stats = rflag_spectral_stats(
                by_row,
                context.casa_arithmetic,
                RflagRoundoff::Slice(roundoff_buf),
            );
            let row_roundoff = rflag_row_roundoff(by_row, RflagRoundoff::Slice(roundoff_buf));
            for sample in by_row {
                if sample.flag {
                    continue;
                }
                let bound = f64::from(
                    roundoff_buf.get(sample.index).copied().unwrap_or(0.0) + row_roundoff,
                );
                if stats.sum_weight_real > 0.0 {
                    freq_hist.add(
                        index(sample.chan),
                        rflag_clamped_deviation((sample.real - stats.average_real).abs(), bound),
                    );
                }
                if stats.sum_weight_imag > 0.0 {
                    freq_hist.add(
                        index(sample.chan),
                        rflag_clamped_deviation((sample.imag - stats.average_imag).abs(), bound),
                    );
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum RflagRoundoff<'a> {
    Slice(&'a [f32]),
    Sparse(&'a HashMap<usize, f32>),
}

impl RflagRoundoff<'_> {
    fn is_empty(self) -> bool {
        match self {
            Self::Slice(slice) => slice.is_empty(),
            Self::Sparse(map) => map.is_empty(),
        }
    }

    fn get(self, index: usize) -> f32 {
        match self {
            Self::Slice(slice) => slice.get(index).copied().unwrap_or(0.0),
            Self::Sparse(map) => map.get(&index).copied().unwrap_or(0.0),
        }
    }
}

struct RflagProjectionSeries {
    rows: Vec<Vec<Sample>>,
    chans: Vec<Vec<Sample>>,
    partners: HashMap<usize, usize>,
    roundoff: HashMap<usize, f32>,
}

fn rflag_projection_series(
    planes: &[Vec<Sample>],
    projection: ClipProjection,
    model_roundoff: &[f32],
    roundoff_buf: &mut [f32],
) -> Option<RflagProjectionSeries> {
    let series = tfcrop_series(planes, projection, None, &[], model_roundoff, roundoff_buf)?;
    let rows = sample_row_runs(&series.samples)
        .into_iter()
        .map(<[Sample]>::to_vec)
        .collect::<Vec<_>>();
    let chans = samples_by_chan_dense(&series.samples);
    let roundoff = if roundoff_buf.is_empty() {
        HashMap::new()
    } else {
        series
            .samples
            .iter()
            .map(|sample| {
                (
                    sample.index,
                    roundoff_buf.get(sample.index).copied().unwrap_or(0.0),
                )
            })
            .collect()
    };
    Some(RflagProjectionSeries {
        rows,
        chans,
        partners: series.pairs.into_iter().collect(),
        roundoff,
    })
}

fn is_rflag_flagged(sample: &Sample, partners: &HashMap<usize, usize>, modified: &[bool]) -> bool {
    sample.flag
        || modified[sample.index]
        || partners
            .get(&sample.index)
            .is_some_and(|&partner| modified[partner])
}

fn mark_rflag_flagged(
    partners: &HashMap<usize, usize>,
    index: usize,
    axis_flags: &mut [bool],
    modified: &mut [bool],
) {
    axis_flags[index] = true;
    modified[index] = true;
    if let Some(&partner) = partners.get(&index) {
        axis_flags[partner] = true;
        modified[partner] = true;
    }
}

fn rflag_group_flags(
    description: Option<i32>,
    group: &[Sample],
    thresholds: &RFlagThresholds,
    request: &FlagDataRequest,
    context: RflagContext<'_>,
    roundoff_buf: &mut [f32],
    masks: &mut TfcropFitMasks<'_>,
) -> Result<(), FlaggingError> {
    let Some(window_size) = request.rflag_window else {
        return Ok(());
    };
    let Some(first) = group.first() else {
        return Ok(());
    };
    let key = (first.field, first.spw);
    let timedev = thresholds
        .timedev
        .get(&key)
        .copied()
        .unwrap_or(f64::INFINITY);
    let freqdev = thresholds
        .freqdev
        .get(&key)
        .copied()
        .unwrap_or(f64::INFINITY);
    let planes = samples_by_corr(group);
    let mut projections = Vec::new();
    for projection in rflag_group_projections(group, description, context)? {
        if let Some(series) =
            rflag_projection_series(&planes, projection, context.model_roundoff, roundoff_buf)
        {
            projections.push(series);
        }
    }
    let row_count = projections.first().map_or(0, |series| series.rows.len());
    let do_time = timedev >= 0.0 || !context.casa_arithmetic;
    let do_freq = freqdev >= 0.0 || !context.casa_arithmetic;
    for timestep in 0..row_count {
        if do_time
            && let Some((start, stop)) = rflag_time_window_at(row_count, window_size, timestep)
        {
            for series in &projections {
                let roundoff = RflagRoundoff::Sparse(&series.roundoff);
                for by_chan in &series.chans {
                    let Some(window) = by_chan.get(start..=stop) else {
                        continue;
                    };
                    if rflag_time_std_total(window, context.casa_arithmetic, roundoff)
                        .is_some_and(|deviation| deviation > timedev)
                    {
                        for sample in window {
                            if !is_rflag_flagged(sample, &series.partners, masks.modified) {
                                mark_rflag_flagged(
                                    &series.partners,
                                    sample.index,
                                    masks.time_flags,
                                    masks.modified,
                                );
                            }
                        }
                    }
                }
            }
        }
        if do_freq {
            for series in &projections {
                let Some(by_row) = series.rows.get(timestep) else {
                    continue;
                };
                let roundoff = RflagRoundoff::Sparse(&series.roundoff);
                let stats = rflag_spectral_stats(by_row, context.casa_arithmetic, roundoff);
                let row_roundoff = rflag_row_roundoff(by_row, roundoff);
                let flag_all = stats.std_real > request.spectralmax
                    || stats.std_imag > request.spectralmax
                    || stats.std_real < request.spectralmin
                    || stats.std_imag < request.spectralmin;
                for sample in by_row {
                    if is_rflag_flagged(sample, &series.partners, masks.modified) {
                        continue;
                    }
                    let bound = f64::from(roundoff.get(sample.index) + row_roundoff);
                    let dev_real =
                        rflag_clamped_deviation((sample.real - stats.average_real).abs(), bound);
                    let dev_imag =
                        rflag_clamped_deviation((sample.imag - stats.average_imag).abs(), bound);
                    if flag_all || dev_real > freqdev || dev_imag > freqdev {
                        mark_rflag_flagged(
                            &series.partners,
                            sample.index,
                            masks.freq_flags,
                            masks.modified,
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn rflag_time_window_at(len: usize, window_size: usize, center: usize) -> Option<(usize, usize)> {
    let effective = len.min(window_size);
    let delta = effective.saturating_sub(1) / 2;
    if delta == 0 {
        (center < len).then_some((center, center))
    } else if center >= delta && center < len - delta {
        Some((center - delta, center + delta))
    } else {
        None
    }
}

fn rflag_time_windows(len: usize, window_size: usize) -> impl Iterator<Item = (usize, usize)> {
    (0..len).filter_map(move |center| rflag_time_window_at(len, window_size, center))
}

fn rflag_time_std_total(
    samples: &[Sample],
    casa_arithmetic: bool,
    roundoff: RflagRoundoff<'_>,
) -> Option<f64> {
    // CASA's visibility mapper supplies Float values and sums v and v * v in Double.
    let term = |value: f64| {
        if casa_arithmetic {
            let value = value as f32;
            (f64::from(value), f64::from(value * value))
        } else {
            (value, value * value)
        }
    };
    let mut count = 0usize;
    let mut sum_real = 0.0;
    let mut sum_imag = 0.0;
    let mut sumsq_real = 0.0;
    let mut sumsq_imag = 0.0;
    for sample in samples {
        if sample.flag {
            continue;
        }
        count += 1;
        let (real, real_sq) = term(sample.real);
        let (imag, imag_sq) = term(sample.imag);
        sum_real += real;
        sum_imag += imag;
        sumsq_real += real_sq;
        sumsq_imag += imag_sq;
    }
    if count == 0 {
        return (!casa_arithmetic).then_some(0.0);
    }
    let sum_weight = count as f64;
    let avg_real = sum_real / sum_weight;
    let avg_imag = sum_imag / sum_weight;
    if !roundoff.is_empty()
        && samples.iter().filter(|sample| !sample.flag).all(|sample| {
            let bound = f64::from(roundoff.get(sample.index)) * 2.0;
            (sample.real - avg_real).abs() <= bound && (sample.imag - avg_imag).abs() <= bound
        })
    {
        return Some(0.0);
    }
    let var_real = sumsq_real / sum_weight - avg_real * avg_real;
    let var_imag = sumsq_imag / sum_weight - avg_imag * avg_imag;
    Some((var_real.max(0.0) + var_imag.max(0.0)).sqrt())
}

fn rflag_row_roundoff(samples: &[Sample], roundoff: RflagRoundoff<'_>) -> f32 {
    if roundoff.is_empty() {
        return 0.0;
    }
    samples
        .iter()
        .filter(|sample| !sample.flag)
        .map(|sample| roundoff.get(sample.index))
        .filter(|bound| bound.is_finite())
        .fold(0.0, f32::max)
}

fn rflag_clamped_deviation(deviation: f64, roundoff_bound: f64) -> f64 {
    if roundoff_bound > 0.0 && deviation <= roundoff_bound {
        0.0
    } else {
        deviation
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RFlagSpectralStats {
    average_real: f64,
    average_imag: f64,
    std_real: f64,
    std_imag: f64,
    sum_weight_real: f64,
    sum_weight_imag: f64,
}

fn rflag_spectral_stats(
    samples: &[Sample],
    casa_arithmetic: bool,
    roundoff: RflagRoundoff<'_>,
) -> RFlagSpectralStats {
    let mut real_values = Vec::with_capacity(samples.len());
    let mut imag_values = Vec::with_capacity(samples.len());
    for sample in samples {
        if sample.flag {
            continue;
        }
        real_values.push(sample.real);
        imag_values.push(sample.imag);
    }
    if real_values.is_empty() {
        return RFlagSpectralStats::default();
    }
    let average_real = median_in_place(&mut real_values);
    let average_imag = median_in_place(&mut imag_values);
    let row_bound = f64::from(rflag_row_roundoff(samples, roundoff)) * 2.0;
    let residual = |value: f64, reference: f64| {
        let delta = value - reference;
        if row_bound > 0.0 && delta.abs() <= row_bound {
            0.0
        } else if casa_arithmetic {
            delta
        } else {
            delta.abs()
        }
    };
    for value in &mut real_values {
        *value = residual(*value, average_real);
    }
    for value in &mut imag_values {
        *value = residual(*value, average_imag);
    }
    let std_real = 1.4826 * median_in_place(&mut real_values);
    let std_imag = 1.4826 * median_in_place(&mut imag_values);
    let weight = real_values.len() as f64;
    RFlagSpectralStats {
        average_real,
        average_imag,
        std_real,
        std_imag,
        sum_weight_real: weight,
        sum_weight_imag: weight,
    }
}

fn format_threshold_map(thresholds: &BTreeMap<FieldSpwKey, f64>) -> BTreeMap<String, f64> {
    thresholds
        .iter()
        .map(|((field, spw), value)| (format!("field{field}_spw{spw}"), *value))
        .collect()
}

fn summary_after_flag_mask(
    samples: &[Sample],
    changed_samples: usize,
    flag_value: bool,
) -> FlagSummary {
    let flagged = samples.iter().filter(|sample| sample.flag).count();
    FlagSummary {
        flagged_samples: if flag_value {
            flagged + changed_samples
        } else {
            flagged.saturating_sub(changed_samples)
        },
        total_samples: samples.len(),
    }
}

fn percent_modified(samples: &[Sample], modified: &[bool]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    100.0
        * samples
            .iter()
            .filter(|sample| sample.flag || modified[sample.index])
            .count() as f64
        / samples.len() as f64
}

fn count_mask(mask: &[bool]) -> usize {
    mask.iter().filter(|value| **value).count()
}

/// The `tracing` target of the flagging trace (`debug`): one JSON record per
/// tfcrop or rflag stage.
const FLAGDATA_TRACE: &str = "casa_ms::flagdata_trace";

/// Log the record `value` forms when the logging configuration takes the
/// flagging trace; otherwise it is not formed.
fn trace_flagdata(value: impl FnOnce() -> serde_json::Value) {
    if tracing::enabled!(target: FLAGDATA_TRACE, tracing::Level::DEBUG) {
        tracing::debug!(target: FLAGDATA_TRACE, "{}", value());
    }
}

fn flag_mask_counts_by_spw_corr(mask: &[bool], samples: &[Sample]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::<String, usize>::new();
    for (sample, flag) in samples.iter().zip(mask.iter()) {
        if *flag {
            *counts
                .entry(format!("spw{}_corr{}", sample.spw, sample.corr))
                .or_default() += 1;
        }
    }
    counts
}

type GroupKey = (i32, i32, i32, i32, i32);
type FieldSpwKey = (i32, i32);

fn group_samples(samples: &[Sample]) -> BTreeMap<GroupKey, Vec<Sample>> {
    let mut groups = BTreeMap::<GroupKey, Vec<Sample>>::new();
    for sample in samples {
        let ant_low = sample.ant1.min(sample.ant2);
        let ant_high = sample.ant1.max(sample.ant2);
        groups
            .entry((sample.field, sample.spw, sample.scan, ant_low, ant_high))
            .or_default()
            .push(*sample);
    }
    groups
}

fn samples_by_corr(samples: &[Sample]) -> Vec<Vec<Sample>> {
    let Some(max_corr) = samples.iter().map(|sample| sample.corr).max() else {
        return Vec::new();
    };
    let mut groups = vec![Vec::<Sample>::new(); max_corr + 1];
    for sample in samples {
        groups[sample.corr].push(*sample);
    }
    groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .collect()
}

fn samples_by_chan_dense(samples: &[Sample]) -> Vec<Vec<Sample>> {
    let Some(max_chan) = samples.iter().map(|sample| sample.chan).max() else {
        return Vec::new();
    };
    let mut groups = vec![Vec::<Sample>::new(); max_chan + 1];
    for sample in samples {
        groups[sample.chan].push(*sample);
    }
    groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .collect()
}

fn sample_row_runs(samples: &[Sample]) -> Vec<&[Sample]> {
    let mut runs = Vec::new();
    let mut start = 0usize;
    while start < samples.len() {
        let row = samples[start].row;
        let mut end = start + 1;
        while end < samples.len() && samples[end].row == row {
            end += 1;
        }
        runs.push(&samples[start..end]);
        start = end;
    }
    runs
}

fn unique_sample_rows(samples: &[Sample]) -> Vec<usize> {
    let mut rows = Vec::new();
    let mut previous = None;
    for sample in samples {
        if previous != Some(sample.row) {
            rows.push(sample.row);
            previous = Some(sample.row);
        }
    }
    rows
}

fn unique_sample_chans(samples: &[Sample]) -> Vec<usize> {
    let mut seen = HashSet::new();
    let mut chans = Vec::new();
    for sample in samples {
        if seen.insert(sample.chan) {
            chans.push(sample.chan);
        }
    }
    chans
}

fn samples_by_row_chan(samples: &[Sample]) -> BTreeMap<(usize, usize), Vec<Sample>> {
    let mut groups = BTreeMap::<(usize, usize), Vec<Sample>>::new();
    for sample in samples {
        groups
            .entry((sample.row, sample.chan))
            .or_default()
            .push(*sample);
    }
    groups
}

fn parse_channel_selections(value: &str) -> Result<BTreeMap<i32, ChannelSelection>, MsError> {
    let mut selections = BTreeMap::new();
    for selector in parse_spw_selector(value)? {
        if let Some(channels) = selector.channels {
            selections.insert(selector.spw_id, channels);
        }
    }
    Ok(selections)
}

fn data_description_spw_map(ms: &MeasurementSet) -> Result<BTreeMap<i32, i32>, FlaggingError> {
    let table = ms
        .data_description()
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_ms_path(ms),
            reason: format!("open DATA_DESCRIPTION: {source}"),
        })?;
    let mut result = BTreeMap::new();
    for row in 0..table.row_count() {
        result.insert(
            row as i32,
            table
                .spectral_window_id(row)
                .map_err(|source| FlaggingError::MutateFlags {
                    path: display_ms_path(ms),
                    reason: format!("read DATA_DESCRIPTION row {row}: {source}"),
                })?,
        );
    }
    Ok(result)
}

/// Resolve a flag store through a MeasurementSet alias.
///
/// An existing companion at the supplied path remains accessible for versions
/// saved by older casa-rs releases. New companions follow the physical MS.
pub fn flag_version_ms_path(vis: &Path) -> PathBuf {
    let path: PathBuf = vis.components().collect();
    if flagversions_dir(&path).exists() {
        path
    } else {
        path.canonicalize().unwrap_or(path)
    }
}

/// Initialize the companion catalog that flagmanager opens with the MeasurementSet.
pub fn initialize_flag_versions(ms: &MeasurementSet) -> Result<(), FlaggingError> {
    ensure_flagversions_dir(ms_path(ms)?).map(|_| ())
}

/// Return flag-version list entries.
pub fn list_flag_versions(ms: &MeasurementSet) -> Result<Vec<FlagVersionEntry>, FlaggingError> {
    let path = ms_path(ms)?;
    list_flag_versions_for_path(path)
}

/// Refuse to delete or rename the table of flag version `versionname` of
/// the MeasurementSet at `path` while a handle in this process or another
/// process has it open, as casacore's `Table::deleteTable` and
/// `Table::rename` refuse a table that is still open in this process or
/// `isMultiUsed` (#695). [`casa_tables::table_holder`] says what this can
/// and cannot see.
fn ensure_version_not_open(
    path: &Path,
    versionname: &str,
    version_path: &Path,
) -> Result<(), FlaggingError> {
    #[cfg(unix)]
    {
        let holder = casa_tables::table_holder(version_path).map_err(|source| {
            FlaggingError::FlagVersion {
                path: path.display().to_string(),
                reason: format!("test whether flag version {versionname:?} is open: {source}"),
            }
        })?;
        if let Some(holder) = holder {
            return Err(FlaggingError::FlagVersionInUse {
                path: path.display().to_string(),
                version: versionname.to_string(),
                table: version_path.display().to_string(),
                holder,
            });
        }
    }
    #[cfg(not(unix))]
    let _ = (path, versionname, version_path);
    Ok(())
}

/// Delete a named flag version.
///
/// # Errors
///
/// [`FlaggingError::FlagVersionInUse`] while a handle in this process or
/// another process has the version's table open.
pub fn delete_flag_version(ms: &MeasurementSet, versionname: &str) -> Result<(), FlaggingError> {
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    let version_path = flag_version_table_path(path, versionname);
    if version_path.exists() {
        ensure_version_not_open(path, versionname, &version_path)?;
        fs::remove_dir_all(&version_path).map_err(|source| FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("delete flag version {versionname:?}: {source}"),
        })?;
    }
    remove_flag_version_entry(path, versionname)
}

/// Rename a flag version, giving it `comment`. Renaming a version to its
/// own name changes only its comment.
///
/// # Errors
///
/// [`FlaggingError::FlagVersionInUse`] while a handle in this process or
/// another process has the version's table open, and
/// [`FlaggingError::FlagVersion`] when a version named `versionname`
/// already exists.
pub fn rename_flag_version(
    ms: &MeasurementSet,
    oldname: &str,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, oldname)?;
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    let old_path = flag_version_table_path(path, oldname);
    let new_path = flag_version_table_path(path, versionname);
    if versionname != oldname
        && (new_path.exists()
            || list_flag_versions_for_path(path)?
                .iter()
                .any(|entry| entry.name == versionname))
    {
        return Err(FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!(
                "rename flag version {oldname:?}: flag version {versionname:?} already exists"
            ),
        });
    }
    ensure_version_not_open(path, oldname, &old_path)?;
    fs::rename(old_path, new_path).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("rename flag version {oldname:?}: {source}"),
    })?;
    rename_flag_version_entry(path, oldname, versionname, comment)
}

/// Rename a saved version through the CASA flagmanager task.
///
/// The task requires a saved source and a free destination, including when
/// the source and destination names are equal.
pub fn rename_flag_version_for_task(
    ms: &MeasurementSet,
    oldname: &str,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    validate_version_name(ms, oldname)?;
    validate_version_name(ms, versionname)?;
    let path = ms_path(ms)?;
    if !flag_version_table_path(path, oldname).is_dir() {
        return Err(FlaggingError::FlagVersionRenameArgument {
            parameter: "oldname",
            message: format!("No such flagversions: {oldname}"),
        });
    }
    if flag_version_table_path(path, versionname).exists() {
        return Err(FlaggingError::FlagVersionRenameArgument {
            parameter: "versionname",
            message: format!("Flagversions {versionname} already exists!"),
        });
    }
    rename_flag_version(ms, oldname, versionname, comment)
}

fn list_flag_versions_for_path(path: &Path) -> Result<Vec<FlagVersionEntry>, FlaggingError> {
    let mut entries = vec![FlagVersionEntry {
        name: "main".to_string(),
        comment: "working copy in main table".to_string(),
    }];
    let list_path = flag_version_list_path(path);
    if !list_path.exists() {
        return Ok(entries);
    }
    let text = fs::read_to_string(&list_path).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("read FLAG_VERSION_LIST: {source}"),
    })?;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let (name, comment) = line
            .split_once(" : ")
            .map(|(name, comment)| (name.to_string(), comment.to_string()))
            .unwrap_or_else(|| (line.to_string(), String::new()));
        entries.push(FlagVersionEntry { name, comment });
    }
    Ok(entries)
}

fn append_flag_version_entry(
    path: &Path,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |mut entries| {
        entries.push(FlagVersionEntry {
            name: versionname.to_string(),
            comment: comment.to_string(),
        });
        entries
    })
}

fn update_flag_version_comment(
    path: &Path,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    if comment.is_empty() {
        return Ok(());
    }
    rewrite_flag_version_text(path, |mut entries| {
        for entry in &mut entries {
            if entry.name == versionname {
                entry.comment = comment.to_string();
            }
        }
        entries
    })
}

fn remove_flag_version_entry(path: &Path, versionname: &str) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |entries| {
        entries
            .into_iter()
            .filter(|entry| entry.name != versionname)
            .collect()
    })
}

fn rename_flag_version_entry(
    path: &Path,
    oldname: &str,
    versionname: &str,
    comment: &str,
) -> Result<(), FlaggingError> {
    rewrite_flag_version_text(path, |mut entries| {
        for entry in &mut entries {
            if entry.name == oldname {
                entry.name = versionname.to_string();
                entry.comment = comment.to_string();
            }
        }
        entries
    })
}

fn rewrite_flag_version_text<F>(path: &Path, edit: F) -> Result<(), FlaggingError>
where
    F: FnOnce(Vec<FlagVersionEntry>) -> Vec<FlagVersionEntry>,
{
    ensure_flagversions_dir(path)?;
    let entries = edit(
        list_flag_versions_for_path(path)?
            .into_iter()
            .skip(1)
            .collect(),
    );
    let text = entries
        .into_iter()
        .map(|entry| format!("{} : {}\n", entry.name, entry.comment))
        .collect::<String>();
    fs::write(flag_version_list_path(path), text).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("write FLAG_VERSION_LIST: {source}"),
    })
}

/// Open a flag-version table to read it, holding casacore's read lock until
/// the table is dropped. The open waits, as a casacore `AutoLocking` open
/// does, while another process (casacore's flagmanager or casa-rs) holds the
/// version's write lock, so a version being written is never read half
/// written; the table is read only once the lock is held.
fn open_flag_version_for_reading(version_path: &Path) -> Result<Table, TableError> {
    #[cfg(unix)]
    {
        Table::open_with_lock(
            TableOptions::new(version_path),
            LockOptions::new(LockMode::AutoLocking),
        )
    }
    #[cfg(not(unix))]
    {
        Table::open(TableOptions::new(version_path))
    }
}

fn merge_bool(source: bool, dest: bool, merge: FlagMerge) -> bool {
    match merge {
        FlagMerge::Replace => source,
        FlagMerge::Or => source || dest,
        FlagMerge::And => source && dest,
    }
}

fn merge_bool_arrays(
    source: &ArrayD<bool>,
    dest: &ArrayD<bool>,
    merge: FlagMerge,
) -> Result<ArrayD<bool>, FlaggingError> {
    if source.shape() != dest.shape() {
        return Err(FlaggingError::MutateFlags {
            path: "<memory>".to_string(),
            reason: format!(
                "cannot merge flag arrays with shapes {:?} and {:?}",
                source.shape(),
                dest.shape()
            ),
        });
    }
    let values = source
        .iter()
        .zip(dest.iter())
        .map(|(source, dest)| match merge {
            FlagMerge::Replace => *source,
            FlagMerge::Or => *source || *dest,
            FlagMerge::And => *source && *dest,
        })
        .collect::<Vec<_>>();
    ArrayD::from_shape_vec(IxDyn(source.shape()), values).map_err(|source| {
        FlaggingError::MutateFlags {
            path: "<memory>".to_string(),
            reason: format!("build merged flag array: {source}"),
        }
    })
}

fn ensure_flagversions_dir(path: &Path) -> Result<PathBuf, FlaggingError> {
    let dir = flagversions_dir(path);
    fs::create_dir_all(&dir).map_err(|source| FlaggingError::FlagVersion {
        path: path.display().to_string(),
        reason: format!("create flagversions directory: {source}"),
    })?;
    if let Err(source) = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(FLAG_VERSION_LIST))
        && source.kind() != std::io::ErrorKind::AlreadyExists
    {
        return Err(FlaggingError::FlagVersion {
            path: path.display().to_string(),
            reason: format!("create FLAG_VERSION_LIST: {source}"),
        });
    }
    Ok(dir)
}

pub(crate) fn flagversions_dir(path: &Path) -> PathBuf {
    let mut os = OsString::from(path.as_os_str());
    os.push(".flagversions");
    PathBuf::from(os)
}

fn flag_version_list_path(path: &Path) -> PathBuf {
    flagversions_dir(path).join(FLAG_VERSION_LIST)
}

fn flag_version_table_path(path: &Path, versionname: &str) -> PathBuf {
    flagversions_dir(path).join(format!("flags.{versionname}"))
}

fn validate_version_name(ms: &MeasurementSet, versionname: &str) -> Result<(), FlaggingError> {
    if versionname.is_empty() || versionname == "main" || versionname.contains('/') {
        return Err(FlaggingError::FlagVersion {
            path: display_ms_path(ms),
            reason: format!("illegal flag version name {versionname:?}"),
        });
    }
    Ok(())
}

fn clone_flag_matrix(ms: &MeasurementSet, row: usize) -> Result<ArrayD<bool>, FlaggingError> {
    table_flag_matrix(ms.main_table(), row, FLAG_COLUMN, ms.path())
}

fn flag_row_value(ms: &MeasurementSet, row: usize) -> Result<bool, FlaggingError> {
    table_bool(ms.main_table(), row, FLAG_ROW_COLUMN, ms.path())
}

fn table_flag_matrix(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<ArrayD<bool>, FlaggingError> {
    let value = table
        .column_accessor(column)
        .and_then(|column| column.array_cell(row))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value {
        ArrayValue::Bool(flags) => Ok(flags.clone()),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Bool array, found {other:?}"),
        }),
    }
}

fn table_bool(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<bool, FlaggingError> {
    let value = table
        .column_accessor(column)
        .and_then(|column| column.scalar_cell(row))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value {
        ScalarValue::Bool(value) => Ok(*value),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Bool scalar, found {other:?}"),
        }),
    }
}

fn table_i32(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<i32, FlaggingError> {
    let value = table
        .column_accessor(column)
        .and_then(|column| column.scalar_cell(row))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value {
        ScalarValue::Int32(value) => Ok(*value),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Int32 scalar, found {other:?}"),
        }),
    }
}

fn table_f64(
    table: &Table,
    row: usize,
    column: &str,
    path: Option<&Path>,
) -> Result<f64, FlaggingError> {
    let value = table
        .column_accessor(column)
        .and_then(|column| column.scalar_cell(row))
        .map_err(|source| FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("read {column} row {row}: {source}"),
        })?;
    match value {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Float32(value) => Ok(f64::from(*value)),
        other => Err(FlaggingError::MutateFlags {
            path: display_path(path),
            reason: format!("{column} row {row} must be Float scalar, found {other:?}"),
        }),
    }
}

fn ms_path(ms: &MeasurementSet) -> Result<&Path, FlaggingError> {
    ms.path().ok_or_else(|| FlaggingError::FlagVersion {
        path: "<memory>".to_string(),
        reason: "flag versions require a disk-backed MeasurementSet".to_string(),
    })
}

fn display_ms_path(ms: &MeasurementSet) -> String {
    ms.path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<memory>".to_string())
}

fn display_path(path: Option<&Path>) -> String {
    path.map(|path| path.display().to_string())
        .unwrap_or_else(|| "<memory>".to_string())
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    index: usize,
    row: usize,
    corr: usize,
    chan: usize,
    amp: f64,
    real: f64,
    imag: f64,
    flag: bool,
    field: i32,
    spw: i32,
    scan: i32,
    ant1: i32,
    ant2: i32,
}

struct LoadedSamples {
    samples: Vec<Sample>,
    flags_by_row: BTreeMap<usize, ArrayD<bool>>,
    model_roundoff: Vec<f32>,
    axes: AveragedSampleAxes,
}

#[derive(Default)]
struct AveragedSampleAxes {
    channels_by_row: BTreeMap<usize, AveragedChannels>,
    rows_by_bin: BTreeMap<usize, Vec<usize>>,
    flags_by_bin: Option<BTreeMap<usize, ArrayD<bool>>>,
}

struct AveragedChannels {
    channels: Arc<[usize]>,
    width: usize,
}

#[derive(Default)]
struct ChangeSet {
    changed_rows: usize,
    changed_samples: usize,
    changed_row_indices: Vec<usize>,
    summary: Option<FlagSummary>,
    thresholds: Option<(f64, f64)>,
    rflag_report: Option<RFlagThresholdReport>,
    changed_flag_row_rows: usize,
    agent_skipped: bool,
}

#[derive(Clone)]
struct FlagSummary {
    flagged_samples: usize,
    total_samples: usize,
}

#[allow(dead_code)]
fn _complex_norm32(value: Complex32) -> f64 {
    value.norm() as f64
}

#[allow(dead_code)]
fn _complex_norm64(value: Complex64) -> f64 {
    value.norm()
}

#[cfg(test)]
mod tests {
    const CLIP_ZEROS: ClipOptions<'static> = ClipOptions {
        zeros: true,
        signed_zeros: false,
        bounds: None,
        outside: true,
        projections: None,
        metric: None,
    };

    use super::*;
    use ndarray::array;

    #[test]
    fn tfcrop_angle_roundoff_covers_the_principal_phase_branch_cut() {
        let above = Complex64::new(-1.0, 1.0e-8);
        let below = Complex64::new(-1.0, -1.0e-8);
        let bound = tfcrop_metric_roundoff(ClipMetric::Phase, above, 1.0e-6);
        assert!(f64::from(bound) >= (above.arg() - below.arg()).abs());
        let away_from_cut =
            tfcrop_metric_roundoff(ClipMetric::Phase, Complex64::new(1.0, 0.0), 1.0e-6);
        assert!((1.0e-6..=1.1e-6).contains(&away_from_cut));
    }

    fn sample(index: usize, row: usize, corr: usize, chan: usize, real: f64, flag: bool) -> Sample {
        Sample {
            index,
            row,
            corr,
            chan,
            amp: real.abs(),
            real,
            imag: 0.0,
            flag,
            field: 0,
            spw: 0,
            scan: 1,
            ant1: 0,
            ant2: 1,
        }
    }

    #[test]
    fn clip_zero_and_sample_loading_helpers_cover_numeric_families() {
        assert!(is_casa_clip(f64::NAN, true));
        assert!(is_casa_clip(f64::from(f32::EPSILON), true));
        assert!(!is_casa_clip(f64::from(f32::EPSILON) * 2.0, true));
        assert!(is_casa_clip_complex32(Complex32::new(f32::NAN, 0.0), true));
        assert!(is_casa_clip_complex64(Complex64::new(0.0, f64::NAN), true));
        assert!(!is_casa_clip_complex64(Complex64::new(1.0, 0.0), true));
        // Without clipzeros CASA's clip mode flags NaNs alone (#997 F4).
        assert!(is_casa_clip(f64::NAN, false));
        assert!(!is_casa_clip(0.0, false));
        assert!(is_casa_clip_complex32(Complex32::new(0.0, f32::NAN), false));
        assert!(!is_casa_clip_complex64(Complex64::new(0.0, 0.0), false));

        let flags = ArrayD::from_elem(IxDyn(&[2, 3]), false);
        let mut changed = Vec::new();
        let c64 = ArrayValue::Complex64(
            array![
                [
                    Complex64::new(0.0, 0.0),
                    Complex64::new(2.0, 0.0),
                    Complex64::new(f64::NAN, 0.0)
                ],
                [
                    Complex64::new(3.0, 0.0),
                    Complex64::new(0.0, 0.0),
                    Complex64::new(4.0, 0.0)
                ]
            ]
            .into_dyn(),
        );
        apply_clip_array(
            &c64,
            &flags,
            Some(&[0, 2]),
            CLIP_ZEROS,
            "<test>",
            4,
            &mut changed,
        )
        .expect("clip complex64");
        assert_eq!(changed, vec![(0, 0), (0, 2)]);
        changed.clear();
        apply_clip_array(
            &c64,
            &flags,
            Some(&[0, 2]),
            ClipOptions {
                zeros: false,
                ..CLIP_ZEROS
            },
            "<test>",
            4,
            &mut changed,
        )
        .expect("clip complex64 NaNs");
        assert_eq!(changed, vec![(0, 2)]);

        changed.clear();
        let flags_2x2 = ArrayD::from_elem(IxDyn(&[2, 2]), false);
        let f32s = ArrayValue::Float32(array![[0.0_f32, 1.0], [f32::EPSILON, 2.0]].into_dyn());
        apply_clip_array(
            &f32s,
            &flags_2x2,
            None,
            CLIP_ZEROS,
            "<test>",
            5,
            &mut changed,
        )
        .expect("clip float32");
        assert_eq!(changed, vec![(0, 0), (1, 0)]);

        changed.clear();
        let f64s = ArrayValue::Float64(array![[0.0_f64, 5.0], [f64::NAN, 6.0]].into_dyn());
        apply_clip_array(
            &f64s,
            &flags_2x2,
            None,
            CLIP_ZEROS,
            "<test>",
            6,
            &mut changed,
        )
        .expect("clip float64");
        assert_eq!(changed, vec![(0, 0), (1, 0)]);

        let mut samples = Vec::new();
        let metadata = RowSampleMetadata {
            row: 9,
            field: 2,
            spw: 3,
            scan: 4,
            ant1: 5,
            ant2: 6,
        };
        push_row_samples(
            &c64,
            &flags,
            &[1, 2],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .expect("push complex64");
        push_row_samples(
            &f32s,
            &flags_2x2,
            &[0],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .expect("push float32");
        push_row_samples(
            &f64s,
            &flags_2x2,
            &[1],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .expect("push float64");
        assert_eq!(samples.len(), 8);
        assert!(
            samples
                .iter()
                .any(|s| s.field == 2 && s.spw == 3 && s.amp == 5.0)
        );
    }

    #[test]
    fn tfcrop_cubic_fits_narrow_support_at_high_channels() {
        let mut data = [0.0; 103];
        let mut flag = [true; 103];
        let mut fitted = [0.0; 103];
        data[99..].copy_from_slice(&[0.25, 1.75, 0.125, 0.5]);
        flag[99..].fill(false);
        tfcrop_poly_fit(&data, &flag, &mut fitted, 99, 102, 3, true);

        // C++ casacore LinearFit followed by CASA's Float accumulation.
        let expected = [0.201156, 1.80258, 0.216449, 0.567763];
        for (actual, expected) in fitted[99..].iter().zip(expected) {
            assert!((actual - expected).abs() < 2.0e-5, "{actual} != {expected}");
        }
    }

    #[test]
    fn tfcrop_fitting_helpers_cover_zero_interpolation_and_fallbacks() {
        let data = [1.0, 0.0, 3.0, 4.0, 40.0, 6.0, 7.0, 8.0, 9.0];
        let mut flags = vec![false; data.len()];
        let fit = tfcrop_fit_piecewise_poly(&data, &mut flags, 7, 4, false);
        assert_eq!(fit.len(), data.len());
        assert!(fit.iter().all(|value| value.is_finite()));
        assert!(flags.iter().any(|flag| *flag));

        let mut all_flagged_fit = vec![0.0; 3];
        tfcrop_line_fit(
            &[2.0, 2.0, 2.0],
            &[true, true, true],
            &mut all_flagged_fit,
            0,
            2,
            false,
        );
        assert_eq!(all_flagged_fit, vec![0.0, 0.0, 0.0]);

        let mut fallback_fit = vec![0.0; 3];
        tfcrop_poly_fit(
            &[1.0, 2.0, 3.0],
            &[false, true, true],
            &mut fallback_fit,
            0,
            2,
            3,
            false,
        );
        assert_eq!(fallback_fit, vec![1.0, 1.0, 1.0]);
        assert_eq!(tfcrop_mean(&[1.0, 3.0], &[false, false]), 2.0);
        assert_eq!(
            tfcrop_std_about_mean(&[1.0, 3.0], &[false, false], 2.0),
            1.0
        );
        assert_eq!(
            tfcrop_std_about_fit(&[1.0, 3.0], &[false, false], &[1.0, 1.0]),
            2.0_f32.sqrt()
        );
        assert_eq!(median(Vec::new()), 0.0);
        assert_eq!(median(vec![1.0, 4.0, 2.0, 3.0]), 2.5);
    }

    #[test]
    fn rflag_thresholds_and_group_flags_cover_time_and_spectral_paths() {
        let samples = vec![
            sample(0, 0, 0, 0, 1.0, false),
            sample(1, 0, 0, 1, 1.0, false),
            sample(2, 1, 0, 0, 1.0, false),
            sample(3, 1, 0, 1, 1.0, false),
            sample(4, 2, 0, 0, 10.0, false),
            sample(5, 2, 0, 1, 1.0, false),
            sample(6, 0, 1, 0, 2.0, true),
            sample(7, 0, 1, 1, 2.0, true),
        ];
        let groups = group_samples(&samples)
            .into_values()
            .map(|group| (AutoflagChunk::default(), group))
            .collect::<Vec<_>>();
        let request = FlagDataRequest {
            timedev: Some(1.0),
            freqdev: Some(2.0),
            timedevscale: 1.0,
            freqdevscale: 1.0,
            spectralmax: 0.1,
            spectralmin: 0.0,
            ..FlagDataRequest::default()
        };
        let thresholds =
            rflag_threshold_maps_for_groups(&groups, &request, RflagContext::default())
                .expect("rflag thresholds")
                .scaled(&request, false);
        assert_eq!(thresholds.representative_pair(), (1.0, 2.0));
        // CASA scales a given threshold as it scales a computed one.
        let scaled_request = FlagDataRequest {
            timedevscale: 5.0,
            freqdevscale: 3.0,
            ..request.clone()
        };
        let scaled =
            rflag_threshold_maps_for_groups(&groups, &scaled_request, RflagContext::default())
                .expect("scaled rflag thresholds")
                .scaled(&scaled_request, false);
        assert_eq!(scaled.representative_pair(), (5.0, 6.0));
        assert_eq!(
            format_threshold_map(&thresholds.timedev).get("field0_spw0"),
            Some(&1.0)
        );

        let mut modified = vec![false; samples.len()];
        let mut time_flags = vec![false; samples.len()];
        let mut freq_flags = vec![false; samples.len()];
        for (_, group) in &groups {
            let mut masks = TfcropFitMasks {
                modified: &mut modified,
                freq_flags: &mut freq_flags,
                time_flags: &mut time_flags,
            };
            rflag_group_flags(
                None,
                group,
                &thresholds,
                &request,
                RflagContext::default(),
                &mut Vec::new(),
                &mut masks,
            )
            .expect("rflag group flags");
        }
        assert!(time_flags[0] || time_flags[2] || time_flags[4]);
        assert!(freq_flags[4] || freq_flags[5]);
        assert!(!freq_flags[6]);

        assert_eq!(
            rflag_time_windows(0, 3).collect::<Vec<_>>(),
            Vec::<(usize, usize)>::new()
        );
        assert_eq!(rflag_time_windows(1, 3).collect::<Vec<_>>(), vec![(0, 0)]);
        assert_eq!(
            rflag_time_windows(4, 3).collect::<Vec<_>>(),
            vec![(0, 2), (1, 3)]
        );
        assert_eq!(
            rflag_time_std_total(&samples[6..8], false, RflagRoundoff::Slice(&[])),
            Some(0.0)
        );
        assert_eq!(
            rflag_spectral_stats(&samples[6..8], false, RflagRoundoff::Slice(&[])).sum_weight_real,
            0.0
        );

        let mut histogram = ChannelHistogram::new(2);
        histogram.add(0, 1.0);
        histogram.add(0, 3.0);
        histogram.add(1, 9.0);
        assert!(histogram.threshold().is_finite());
        assert_eq!(representative_threshold(&BTreeMap::new()), f64::INFINITY);
    }

    #[test]
    fn grouping_extension_and_merge_helpers_cover_boundary_cases() {
        let mut samples = Vec::new();
        for row in 0..2 {
            for corr in 0..2 {
                for chan in 0..3 {
                    let index = samples.len();
                    samples.push(sample(
                        index,
                        row,
                        corr,
                        chan,
                        (row + chan + 1) as f64,
                        false,
                    ));
                }
            }
        }
        samples[0].flag = true;
        let groups = group_samples(&samples);
        let mut modified = vec![false; samples.len()];
        modified[1] = true;
        modified[2] = true;
        modified[3] = true;
        modified[4] = true;
        extend_autoflag_grouped(groups.values().map(Vec::as_slice), &samples, &mut modified);
        assert!(modified[0]);
        assert!(modified[5]);

        assert_eq!(samples_by_corr(&samples).len(), 2);
        assert_eq!(samples_by_chan_dense(&samples).len(), 3);
        assert_eq!(sample_row_runs(&samples).len(), 2);
        assert_eq!(unique_sample_rows(&samples), vec![0, 1]);
        assert_eq!(unique_sample_chans(&samples), vec![0, 1, 2]);
        assert_eq!(samples_by_row_chan(&samples).len(), 6);
        assert_eq!(percent_modified(&samples, &modified), 100.0);
        assert_eq!(count_mask(&modified), 12);
        assert_eq!(
            summary_after_flag_mask(&samples, 2, true).flagged_samples,
            3
        );
        assert_eq!(
            flag_mask_counts_by_spw_corr(&modified, &samples).get("spw0_corr0"),
            Some(&6)
        );

        assert!(merge_bool(true, false, FlagMerge::Or));
        assert!(!merge_bool(true, false, FlagMerge::And));
        let source = ArrayD::from_elem(IxDyn(&[1, 2]), true);
        let dest = ArrayD::from_elem(IxDyn(&[1, 2]), false);
        let merged = merge_bool_arrays(&source, &dest, FlagMerge::Replace).expect("merge arrays");
        assert!(merged.iter().all(|flag| *flag));
        let mismatched = ArrayD::from_elem(IxDyn(&[2, 1]), false);
        assert!(merge_bool_arrays(&source, &mismatched, FlagMerge::Or).is_err());
    }

    #[test]
    fn clip_sample_and_threshold_helpers_cover_errors_and_defaults() {
        let flags_rank1 = ArrayD::from_elem(IxDyn(&[2]), false);
        let data_rank2 = ArrayValue::Float32(array![[0.0_f32, 1.0]].into_dyn());
        let mut changed = Vec::new();
        assert!(
            apply_clip_array(
                &data_rank2,
                &flags_rank1,
                None,
                CLIP_ZEROS,
                "<test>",
                12,
                &mut changed
            )
            .is_err()
        );

        let flags_rank2 = ArrayD::from_elem(IxDyn(&[1, 2]), false);
        let data_rank1 = ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 1.0_f64));
        assert!(
            apply_clip_array(
                &data_rank1,
                &flags_rank2,
                None,
                CLIP_ZEROS,
                "<test>",
                13,
                &mut changed
            )
            .is_err()
        );

        let metadata = RowSampleMetadata {
            row: 14,
            field: 0,
            spw: 0,
            scan: 0,
            ant1: 2,
            ant2: 1,
        };
        let mut samples = Vec::new();
        assert!(
            push_row_samples(
                &data_rank2,
                &flags_rank1,
                &[0],
                metadata,
                "<test>",
                "DATA",
                &mut samples,
            )
            .is_err()
        );
        assert!(
            push_row_samples(
                &data_rank1,
                &flags_rank2,
                &[0],
                metadata,
                "<test>",
                "DATA",
                &mut samples,
            )
            .is_err()
        );

        let strings = ArrayValue::String(ArrayD::from_elem(IxDyn(&[1, 1]), "x".to_string()));
        push_row_samples(
            &strings,
            &flags_rank2,
            &[0],
            metadata,
            "<test>",
            "DATA",
            &mut samples,
        )
        .expect("ignore non-numeric arrays");
        assert!(samples.is_empty());

        let groups = vec![(
            AutoflagChunk::default(),
            vec![
                sample(0, 0, 0, 0, 1.0, false),
                sample(1, 1, 0, 0, 3.0, false),
                sample(2, 2, 0, 0, 5.0, false),
            ],
        )];
        let thresholds = rflag_threshold_maps_for_groups(
            &groups,
            &FlagDataRequest {
                timedev: None,
                freqdev: None,
                timedevscale: 2.0,
                freqdevscale: 3.0,
                ..FlagDataRequest::default()
            },
            RflagContext::default(),
        )
        .expect("rflag thresholds");
        assert!(thresholds.representative_pair().0.is_finite());
        assert!(thresholds.representative_pair().1.is_finite());
        assert_eq!(
            format_threshold_map(&thresholds.timedev)
                .keys()
                .collect::<Vec<_>>(),
            vec![&"field0_spw0".to_string()]
        );
    }

    #[test]
    fn tfcrop_flagger_handles_empty_all_flagged_and_missing_axis_cases() {
        let mut modified = Vec::new();
        let mut direction_flags = Vec::new();
        tfcrop_fit_base_and_flag(
            &[],
            TfcropDirection::Freq,
            1.0,
            &TfcropOptions::default(),
            TfcropArithmetic::default(),
            &mut modified,
            &mut direction_flags,
        );

        let samples = vec![
            sample(0, 0, 0, 0, 1.0, true),
            sample(1, 0, 0, 2, 1.0, true),
            sample(2, 2, 0, 0, 1.0, true),
        ];
        let mut modified = vec![false; samples.len()];
        let mut direction_flags = vec![false; samples.len()];
        tfcrop_fit_base_and_flag(
            &samples,
            TfcropDirection::Time,
            1.0,
            &TfcropOptions::default(),
            TfcropArithmetic::default(),
            &mut modified,
            &mut direction_flags,
        );
        assert_eq!(modified, vec![false; samples.len()]);
        assert_eq!(direction_flags, vec![false; samples.len()]);

        let mut flagged = vec![false, true, false, true];
        let fit = tfcrop_fit_piecewise_poly(&[0.0, 0.0, 2.0, 0.0], &mut flagged, 3, 2, false);
        assert_eq!(fit.len(), 4);
        assert!(fit.iter().all(|value| value.is_finite()));

        let mut all_zero_flags = vec![false, false];
        let all_zero_fit = tfcrop_fit_piecewise_poly(&[0.0, 0.0], &mut all_zero_flags, 1, 1, false);
        assert_eq!(all_zero_fit, vec![0.0, 0.0]);
        assert_eq!(all_zero_flags, vec![true, true]);
    }

    #[test]
    fn rflag_roundoff_storage_is_scoped_per_series_and_clamps_jitter() {
        let mut samples = Vec::new();
        for baseline in 0..2 {
            for t in 0..3 {
                let row = baseline * 3 + t;
                for chan in 0..3 {
                    for corr in 0..2 {
                        let mut real = 1.0;
                        if baseline == 0 && t == 1 && corr == 0 && chan == 1 {
                            real += 5.0e-4;
                        } else if baseline == 1
                            && ((t == 0 && corr == 0 && chan == 1)
                                || (t == 1 && corr == 1 && chan == 1))
                        {
                            real = 10.0;
                        }
                        let mut entry = sample(samples.len(), row, corr, chan, real, false);
                        entry.ant1 = 0;
                        entry.ant2 = (baseline + 1) as i32;
                        samples.push(entry);
                    }
                }
            }
        }
        let model_roundoff = vec![2.0e-3_f32; samples.len()];
        let groups = group_samples(&samples)
            .into_values()
            .map(|group| (AutoflagChunk::default(), group))
            .collect::<Vec<_>>();
        assert_eq!(groups.len(), 2);

        let planes = samples_by_corr(&groups[0].1);
        let mut roundoff_buf = vec![0.0; samples.len()];
        let stored_series = rflag_projection_series(
            &planes,
            ClipProjection::Stored(0),
            &model_roundoff,
            &mut roundoff_buf,
        )
        .expect("stored projection");
        assert!(stored_series.roundoff.len() <= groups[0].1.len());
        assert_eq!(stored_series.roundoff.len(), 9);
        assert!(stored_series.roundoff.len() < samples.len());

        let stokes_series = rflag_projection_series(
            &planes,
            ClipProjection::Stokes(0, 1, correlation::StokesCombination::Sum),
            &model_roundoff,
            &mut roundoff_buf,
        )
        .expect("stokes I projection");
        assert!(stokes_series.roundoff.len() <= groups[0].1.len());
        assert_eq!(stokes_series.roundoff.len(), 9);
        let max_stored_roundoff = stokes_series
            .roundoff
            .values()
            .copied()
            .fold(0.0_f32, f32::max);
        assert!((max_stored_roundoff - 2.0e-3).abs() <= 1.0e-5);

        let request = FlagDataRequest {
            timedev: Some(1.0e-5),
            freqdev: Some(1.0e-5),
            timedevscale: 1.0,
            freqdevscale: 1.0,
            rflag_window: Some(3),
            ..FlagDataRequest::default()
        };
        let mut modified = vec![false; samples.len()];
        let mut time_flags = vec![false; samples.len()];
        let mut freq_flags = vec![false; samples.len()];
        fit_rflag_chunks(
            &groups,
            &request,
            RflagContext {
                casa_arithmetic: false,
                correlations: None,
                calibration: None,
                model_roundoff: &model_roundoff,
            },
            &mut TfcropFitMasks {
                modified: &mut modified,
                freq_flags: &mut freq_flags,
                time_flags: &mut time_flags,
            },
        )
        .expect("fit rflag chunks with roundoff");

        assert!(
            !modified[..18].iter().any(|&flag| flag),
            "roundoff-bounded jitter should not be flagged"
        );
        let freq_spike_index = samples
            .iter()
            .position(|entry| {
                entry.ant2 == 2 && entry.row == 3 && entry.corr == 0 && entry.chan == 1
            })
            .expect("freq spike sample");
        let time_spike_index = samples
            .iter()
            .position(|entry| {
                entry.ant2 == 2 && entry.row == 4 && entry.corr == 1 && entry.chan == 1
            })
            .expect("time spike sample");
        assert!(modified[freq_spike_index] && freq_flags[freq_spike_index]);
        assert!(modified[time_spike_index] && time_flags[time_spike_index]);
    }
}
