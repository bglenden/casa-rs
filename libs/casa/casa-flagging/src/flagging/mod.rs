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
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use casa_core::cancellation::Cancellation;
use casa_core::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use casa_tables::{
    ColumnBinding, ColumnSchema, DataManagerKind, Table, TableError, TableOptions, TableSchema,
    TableWriteLock,
};
#[cfg(unix)]
use casa_tables::{LockMode, LockOptions};
use ndarray::{ArrayD, Ix2, IxDyn};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use casa_ms::MeasurementSet;
use casa_ms::schema::main_table::{OptionalMainColumn, VisibilityDataColumn};
use casa_ms::selection::syntax::{ChannelSelection, parse_spw_selector};
use casa_ms::selection::{
    MsSelection, ResolvedMsSelectionRow, TimeDefaults, TimeSelection, TimeSelectorDefaults,
    UvDistanceSelection, flagdata_c_string, resolve_flagdata_selection_with_system_budget,
    resolve_row_indices_with_system_budget, resolve_time_selector,
};
use casa_ms::{
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
mod shared_average;
mod summary;
#[cfg(feature = "flagdata-test-hooks")]
pub mod test_hooks;

pub use antint::AntennaIntegrationOptions;
use clip::{ClipOptions, apply_clip_array};
use clip_average::{ChannelBins, bin_members, check_channel_bins};
use clip_columns::ClipColumns;
use clip_time::{TimeClip, time_bin_seconds};
pub use command::{append_flag_command, append_flag_commands};
use correlation::{ClipMetric, ClipProjection, CorrelationSelection, TfcropCorrelations};
pub use initial_version::save_initial_flag_version;
pub use program::{
    flagdata_ms_program, validate_calibration_flag_averaging, validate_ms_flag_averaging,
};
pub use shadow::{ShadowAntenna, ShadowOptions};
use shared_average::{SharedAgentSelection, SharedFlagView};
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

mod clip_quack;
mod rflag;
mod samples;
mod selection;
mod tfcrop;
mod versions;
mod write_flags;

use clip_quack::*;
use rflag::*;
use samples::*;
use selection::*;
pub use tfcrop::TfcropFit;
pub use tfcrop::TfcropOptions;
pub use tfcrop::TfcropPasses;
pub use tfcrop::TfcropWindowStats;
use tfcrop::*;
pub use versions::delete_flag_version;
pub use versions::flag_version_ms_path;
pub use versions::initialize_flag_versions;
pub use versions::list_flag_versions;
pub use versions::rename_flag_version;
pub use versions::rename_flag_version_for_task;
use versions::*;
use write_flags::*;

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
    /// Cooperative stop request used while calculating and staging flags.
    /// A completed backup is retained. Once MAIN publication begins, it
    /// finishes even if cancellation is requested.
    pub cancellation: Option<Cancellation>,
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
            cancellation: None,
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
    /// Cancellation was observed before the staged flags were published.
    #[error("flagdata interrupted")]
    Interrupted,

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

impl FlagDataRequest {
    fn check_cancelled(&self) -> Result<(), FlaggingError> {
        check_cancelled(self.cancellation.as_ref())
    }
}

fn check_cancelled(cancellation: Option<&Cancellation>) -> Result<(), FlaggingError> {
    if cancellation.is_some_and(Cancellation::is_cancelled) {
        return Err(FlaggingError::Interrupted);
    }
    Ok(())
}

/// Run native `flagdata` on an opened MeasurementSet.
///
/// On interruption, discard and reopen the supplied table to clear any
/// staged in-memory flags. The path-based entry point does this for callers.
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
    cancellation: Option<&Cancellation>,
) -> Result<(), FlaggingError> {
    let mut maximum_flag_bytes = 0usize;
    for &row in &changes.changed_row_indices {
        check_cancelled(cancellation)?;
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
    #[cfg(feature = "flagdata-test-hooks")]
    test_hooks::observe(test_hooks::Checkpoint::BeforeCommit);
    check_cancelled(cancellation)?;
    // This session publishes FLAG and FLAG_ROW in batches under the MAIN
    // write lock. Once accepted, finish the whole session and flush both columns.
    #[cfg(feature = "flagdata-test-hooks")]
    test_hooks::observe(test_hooks::Checkpoint::CommitAccepted);
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
/// Use the same cancellation token for the program's requests.
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
        selected_rows: changes.selected_rows.unwrap_or(rows.len()),
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

#[derive(Default)]
struct ChangeSet {
    selected_rows: Option<usize>,
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
mod tests;
