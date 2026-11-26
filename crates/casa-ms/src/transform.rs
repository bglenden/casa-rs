// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped `mstransform`-style MeasurementSet materialization.
//!
//! CASA routes `mstransform` through the `mstransformer` tool and a chain of
//! TVI layers. This module implements the IRC+10216 tutorial subset needed by
//! downstream line-imaging workflows: row selection plus per-SPW channel
//! selection and channel averaging into a new on-disk MeasurementSet while
//! preserving the standard subtables and updating spectral-window channel
//! metadata.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use casa_tables::{
    ColumnOverrides, ColumnType, DataManagerKind, StreamedScalarType, StreamedTiledShapeValueType,
    Table, TableError, TableInfo, TableOptions,
};
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis, IxDyn, ShapeBuilder, Slice};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::MsError;
use crate::ms::MeasurementSet;
use crate::ms::{measurement_set_main_table_bindings, measurement_set_table_options};
use crate::schema::SubtableId;
use crate::schema::main_table::OptionalMainColumn;
use crate::selection::{
    ChannelRange, MsReadPlan, MsSelection, MsSelectionError, MsSelectionIoBudget,
    ResolvedMsSelectionRow, RowGrammar, SpwSelection, TimeSelection,
};
use crate::subtables::SubTable;
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetCreateTarget, MeasurementSetScalarColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, MeasurementSetWriteTelemetry,
};

mod baseline;
mod channel_average;
mod combine;
mod continuum;
mod convolution;
mod correlation_metadata;
mod flag_commands;
mod hanning;
mod hanning_tool;
pub use hanning_tool::{HanningColumns, HanningDataColumn, smooth_ms_hanning};
mod input_view;
mod legacy_split;
mod legacy_time;
mod main_arrays;
mod model_references;
mod multi_ms;
mod output_layout;
mod regrid;
pub mod selection_metadata;
mod single_dish_polarization;
mod spectral_metadata;
mod subtable_copy;
pub(crate) mod task_history;
pub mod time_average;
mod time_projection;
mod visibility_columns;

pub use baseline::{SpectrumBaseline, SpectrumRow, SpectrumWeights};
use channel_average::{ChannelAverages, SpectrumCells, unaveraged_spectra};
pub use channel_average::{
    WeightKind, mean_weight_preferring_unflagged, sigma_from_weight, visibility_weight_kind,
    weight_from_sigma,
};
use continuum::ContinuumFit;
pub use continuum::ContinuumFitOptions;
use convolution::{SpectralOperation, SpectrumOperator};
pub use convolution::{SpectrumConvolution, convolve_spectra, transform_baseline_spectra};
use correlation_metadata::{CorrelationSelection, select_visibility_axes};
pub use input_view::{PreparedTransformInputView, TransformInputColumns, TransformInputView};
use main_arrays::MainArrayBlock;
pub use model_references::{PlotMsModelReferences, PlotMsModelSpwSelection};
pub use regrid::{
    LegacySpectralGrid, ToolGridFrameTables, ToolGridPhaseCentre, ToolSpectralWindow,
    reframe_staged_ms, stage_frame_only_ms, tool_channel_frequencies, tool_grid_frame_at_epoch,
    tool_grid_reference_epoch,
};
pub use selection_metadata::selected_plotms_integration_order;
use single_dish_polarization::PolarizationProjection;
pub use single_dish_polarization::{SingleDishPolarizationAverage, ensure_stokes_i_polarization};
use subtable_copy::copy_subtables;
pub use task_history::{TransformTaskHistory, append_task_history};
pub use time_average::{
    PlotTimeGroups, PlotTimePhaseReferences, TimeUvwAverage, centered_time_and_interval,
    selected_plot_time_groups,
};
use time_projection::{TIME_OUTPUT_SCALARS, TimeGroupRead, TimeProjection};
use visibility_columns::VisibilityColumns;

/// Visibility column layout requested by the transform.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransformDataColumn {
    /// MAIN.DATA.
    Data,
    /// MAIN.CORRECTED_DATA.
    #[default]
    CorrectedData,
    /// Store MAIN.MODEL_DATA in output DATA.
    ModelData,
    /// Copy the three named complex visibility columns.
    DataModelCorrected,
    /// Preserve single-dish FLOAT_DATA.
    FloatData,
    /// Copy both FLOAT_DATA and DATA.
    FloatDataAndData,
    /// Preserve LAG_DATA and its lag axis.
    LagData,
    /// Copy both LAG_DATA and DATA.
    LagDataAndData,
    /// Copy every available physical visibility column.
    All,
}

/// Request for a native tutorial-scoped MeasurementSet transform.
#[derive(Debug, Clone)]
pub struct MsTransformRequest {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// CASA-style SPW/channel selector such as `0:7~58`.
    pub spw: String,
    /// Number of adjacent selected channels to average into each output channel.
    pub width: usize,
    /// Visibility layout to materialize.
    pub data_column: TransformDataColumn,
    /// Structured row selection.
    pub selection: MsSelection,
    /// Preserve fully flagged rows.
    ///
    /// CASA `split(keepflags=False)` drops rows whose `FLAG_ROW` is true or
    /// whose selected `FLAG` cube is fully true.
    pub keep_flags: bool,
}

/// Additional options used by the CASA task surfaces.
#[derive(Debug, Clone, Default)]
pub struct MsTransformTaskOptions {
    /// Explicit visibility columns for callers that accept arbitrary combinations.
    /// Ordinary task surfaces select a predefined combination on the request.
    pub visibility_columns: Option<Vec<OptionalMainColumn>>,
    /// Match the legacy Python MeasurementSet tool's output conventions.
    pub legacy_tool: Option<LegacyMsTool>,
    /// Copy only these spectral windows to MAIN, while retaining the request's
    /// full spectral-window selection for the output subtable IDs.
    pub legacy_output_spw: Option<String>,
    /// Create and fill weight and sigma spectra even when the input has none.
    pub use_weight_spectrum: bool,
    /// CASA task preference for the output's array storage.
    pub tiling: Option<MsTransformTiling>,
    /// Keep original field, antenna, observation, state and spectral IDs.
    pub preserve_ids: bool,
    /// First output data-description ID when spectral IDs are reassigned.
    pub ddi_start: usize,
    /// Apply a three-channel Hanning window after selection and averaging.
    pub hanning: bool,
    /// Combine selected spectral windows for each simultaneous baseline sample.
    pub combine_spws: bool,
    /// Fit and subtract a continuum, or produce just the fitted continuum.
    pub continuum: Option<ContinuumFitOptions>,
    /// Transform the selected spectra onto a new frequency grid.
    pub regrid: Option<MsRegridOptions>,
    /// Active per-selected-SPW widths, or a single width broadcast to each SPW.
    /// When absent, the task uses the scalar width in the request.
    /// In legacy split mode, zero represents an invalid negative Python step.
    pub channel_widths: Option<Vec<usize>>,
    /// Average visibilities along each baseline within this time bin.
    pub time_averaging: Option<TimeAverageOptions>,
    /// Combine selected single-dish parallel hands when time averaging is absent.
    pub single_dish_polarization: Option<SingleDishPolarizationAverage>,
    /// Provenance rows to append to the output's HISTORY table.
    pub history: Option<TransformTaskHistory>,
    /// Supplied bin widths used when rewriting saved FLAG_CMD selectors.
    /// CASA interprets them independently of visibility averaging.
    pub flag_command_widths: Option<Vec<i64>>,
    /// The split task's decision to retain or flatten a Multi-MS input.
    pub keep_mms: Option<bool>,
    /// The raw numeric timebin for retained split's exact combine="scan" check.
    pub retained_scan_span: Option<f64>,
    /// Multi-MS dispatch for the public mstransform task.
    pub multi_ms: Option<MsTransformMultiMsOptions>,
    /// Computed arrays to read in place of stored input values.
    pub input_view: Option<Arc<dyn TransformInputView>>,
}

/// Materialization behavior for the public legacy MeasurementSet tools.
#[derive(Debug, Clone, Copy)]
pub enum LegacyMsTool {
    /// Preserve the input subtable IDs.
    Partition(LegacyTimeOptions),
    /// Compact subtable IDs while keeping partial channel bins.
    Split(LegacyTimeOptions),
    /// Preserve the spectral calculation and layout of the legacy regrid tools.
    Regrid(LegacySpectralGrid),
}

/// Additional chunk boundaries used by the legacy MeasurementSet tools.
#[derive(Debug, Clone, Copy)]
pub struct LegacyTimeOptions {
    /// Permit one time bin to contain integrations from multiple observations.
    pub combine_observations: bool,
}

/// Partition allocation for a new Multi-MS product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsPartitionAxis {
    /// Keep scans together.
    Scan,
    /// Keep spectral windows together.
    SpectralWindow,
    /// Keep baselines together.
    Baseline,
    /// Distribute scan and data-description pairs by visibility count.
    Automatic,
}

/// Multi-MS creation and dispatch arguments for mstransform.
#[derive(Debug, Clone, Copy)]
pub struct MsTransformMultiMsOptions {
    /// Create a Multi-MS when transforming a monolithic input.
    pub create: bool,
    /// How to allocate input rows among newly created members.
    pub axis: MsPartitionAxis,
    /// Requested member count. None selects CASA's standalone default.
    pub count: Option<std::num::NonZeroUsize>,
    /// Treat an input Multi-MS as a single table.
    pub monolithic: bool,
    /// The original task requests cross-scan averaging with only the scan boundary removed.
    pub scan_span: bool,
    /// The original Multi-MS request asks for more than one output SPW.
    pub multiple_spws: bool,
}

/// Tile layout options used by the CASA MeasurementSet transform surfaces.
#[derive(Debug, Clone, Copy)]
pub enum MsTransformTiling {
    /// Select the default layout for the first selected visibility shape.
    Standard,
    /// Size the default tile for one pointing of the selected array.
    FastMosaic,
    /// Explicit default tile dimensions in correlation, channel, row order.
    Explicit([usize; 3]),
    /// Use the explicit dimensions for stored visibility tiles as well as their defaults.
    /// Zero extents use the corresponding cell axis or an automatic row extent.
    Fixed([usize; 3]),
}

/// Spectral regridding arguments for the public mstransform task.
#[derive(Debug, Clone)]
pub struct MsRegridOptions {
    /// Axis parameterization: channel, channel_b, frequency or velocity.
    pub mode: String,
    /// Requested channel count per output window. Negative means automatic.
    pub nchan: i64,
    /// First channel or a frequency/velocity quantity.
    pub start: String,
    /// Channel step or a frequency/velocity quantity.
    pub width: String,
    /// Number of output spectral windows.
    pub nspw: usize,
    /// Interpolation method used to sample the new grid.
    pub interpolation: String,
    /// Reference FIELD ID or a direction measure.
    pub phasecenter: String,
    /// Rest frequency for velocity-based grids.
    pub restfreq: String,
    /// Requested output frequency reference frame.
    pub outframe: String,
    /// Radio or optical velocity convention.
    pub veltype: String,
    /// Bin wide channels ahead of interpolation.
    pub preaverage: bool,
}

/// Time averaging parameters shared by the split and mstransform tasks.
#[derive(Debug, Clone, Copy)]
pub struct TimeAverageOptions {
    /// Maximum duration of an averaging group, in seconds.
    pub interval_seconds: f64,
    /// Let the averaging group contain rows from different scans.
    pub span_scans: bool,
    /// Let the averaging group contain rows from different states.
    pub span_states: bool,
    /// Let the averaging group contain rows from different fields.
    pub span_fields: bool,
    /// Baseline motion limit in metres. Nonpositive values disable the limit.
    pub max_uvw_distance: f64,
}

/// Report returned after materializing a transformed MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsTransformReport {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// Number of MAIN rows in the output.
    pub row_count: usize,
    /// Comma-separated source column names.
    pub source_column: String,
    /// Output visibility data column populated.
    pub output_column: String,
    /// CASA-style SPW/channel selector used for the transform.
    pub spw: String,
    /// Spectral windows represented in the output.
    pub spectral_window_ids: Vec<i32>,
    /// Output channel counts by spectral window.
    pub output_channels_by_spw: BTreeMap<i32, usize>,
    /// Number of adjacent selected channels averaged into each output channel.
    pub width: usize,
    /// End-to-end runtime in nanoseconds.
    pub elapsed_ns: u64,
    /// Public return value for retained Multi-MS execution, if dispatched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_mms: Option<bool>,
    /// Canonical bounded-writer telemetry for the output MAIN table.
    pub write_telemetry: MeasurementSetWriteTelemetry,
}

/// Errors returned by [`mstransform`].
#[derive(Debug, Error)]
pub enum MsTransformError {
    /// Validation or computation of the selected spectrum failed.
    #[error("{0}")]
    Spectrum(String),
    /// A spectral regridding argument was rejected.
    #[error("{reason}")]
    RegridParameter {
        /// Name of the argument.
        parameter: &'static str,
        /// Reason for rejection.
        reason: String,
    },
    /// The legacy spectral writer declines a non-equidistant channel grid
    /// for FFT interpolation and leaves the input unchanged.
    #[error("FFT interpolation requires equidistant output channels")]
    LegacyRegridUnchanged,
    /// A Multi-MS argument was rejected.
    #[error("{reason}")]
    MultiMsParameter {
        /// Name of the argument.
        parameter: &'static str,
        /// Message for the task caller.
        reason: String,
    },
    /// The retained Multi-MS cannot average across the selected scans.
    #[error("Unable to continue with MMS processing")]
    MultiMsScanSpan,
    /// Opening an MS failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path being opened.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// Input and output paths are identical.
    #[error("input and output MeasurementSet paths must differ: {path}")]
    SameInputOutput {
        /// Duplicated path.
        path: String,
    },

    /// The output MeasurementSet exists. CASA's `mstransform` and `split` have
    /// no overwrite and refuse it with this message (#931).
    #[error("Output MS {path} already exists - will not overwrite it.")]
    OutputExists {
        /// Output MS path.
        path: String,
    },

    /// A pre-existing flag-version directory belongs to a different output.
    #[error("The flagversions {path} for the output MS already exists. Please delete it.")]
    OutputFlagVersionsExist {
        /// Existing flag-version directory.
        path: String,
    },

    /// The requested source column is absent.
    #[error("MeasurementSet {path} does not contain {column}")]
    MissingDataColumn {
        /// Input MS path.
        path: String,
        /// Missing column.
        column: String,
    },

    /// The input has no visibility column in the requested layout.
    #[error("Did not find and select any data columns.")]
    NoDataColumns,

    /// A spectrum used by the selected rows is undefined in a later cell.
    #[error(
        "Table DataManager error: Invalid operation: SSMIndColumn::getShape: no array in row {row} in column {column} of table {path}"
    )]
    MissingSpectrumCell {
        /// Input MAIN table path.
        path: String,
        /// Physical input row.
        row: usize,
        /// Projected spectral statistics column.
        column: &'static str,
    },

    /// No complete output bin fits a selected spectral window.
    #[error(
        "Channel selection does not allow to produce any output channel with the requested width"
    )]
    NoCompleteChannelBins,

    /// Width vectors with more than one entry must cover the selected SPWs.
    #[error("Number of channel widths is different from the number of spw")]
    ChannelWidthCount,

    /// A legacy split step is negative.
    #[error("Cannot channel average with width < 1")]
    InvalidSplitStep,

    /// The row selection failed.
    #[error("failed to select rows from MeasurementSet {path}: {source}")]
    SelectRows {
        /// Input MS path.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// The row selection was empty.
    #[error("MSSelectionNullSelection : The selected table has zero rows.")]
    EmptySelection {
        /// Input MS path.
        path: String,
    },

    /// Parsing or resolving `spw` failed.
    #[error("invalid spectral-window selection {selector:?}: {reason}")]
    InvalidSpw {
        /// CASA-style selector.
        selector: String,
        /// Human-readable reason.
        reason: String,
    },

    /// Required spectral metadata is missing or inconsistent.
    #[error("missing or invalid spectral metadata in {path}: {reason}")]
    SpectralMetadata {
        /// Input MS path.
        path: String,
        /// Human-readable reason.
        reason: String,
    },

    /// MAIN or subtable mutation failed.
    #[error("failed to transform MeasurementSet data for {path}: {source}")]
    MutateMeasurementSet {
        /// MS path.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Output filesystem preparation failed.
    #[error("failed to prepare output MeasurementSet {path}: {reason}")]
    PrepareOutput {
        /// Output path.
        path: String,
        /// Error context.
        reason: String,
    },

    /// Saving the output MS failed.
    #[error("failed to save MeasurementSet {path}: {source}")]
    SaveMeasurementSet {
        /// Output path.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// Bounded MAIN-column planning or execution failed.
    #[error("failed to write transformed MeasurementSet {path}: {reason}")]
    WriteMeasurementSet {
        /// Output MeasurementSet path.
        path: String,
        /// Physical planning or write failure.
        reason: String,
    },
}

/// Materialize a selected/channel-subset MeasurementSet.
pub fn mstransform(request: &MsTransformRequest) -> Result<MsTransformReport, MsTransformError> {
    materialize_transform(
        request,
        RowGrammar::Casa,
        &MsTransformTaskOptions::default(),
    )
}

/// Materialize the selection with the CASA split and mstransform task grammar.
///
/// Retained Multi-MS splits that select no partition report zero rows and
/// do not publish an output.
pub fn mstransform_task(
    request: &MsTransformRequest,
    options: &MsTransformTaskOptions,
) -> Result<MsTransformReport, MsTransformError> {
    materialize_transform(request, RowGrammar::Transform, options)
}

fn materialize_transform(
    request: &MsTransformRequest,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
) -> Result<MsTransformReport, MsTransformError> {
    if request.input_ms == request.output_ms {
        return Err(MsTransformError::SameInputOutput {
            path: request.input_ms.display().to_string(),
        });
    }
    if request.output_ms.exists() {
        return Err(MsTransformError::OutputExists {
            path: request.output_ms.display().to_string(),
        });
    }
    let flagversions = crate::flagging::flagversions_dir(&request.output_ms);
    if flagversions.exists() {
        return Err(MsTransformError::OutputFlagVersionsExist {
            path: flagversions.display().to_string(),
        });
    }
    let target =
        MeasurementSetCreateTarget::prepare(&request.output_ms, false).map_err(|error| {
            MsTransformError::PrepareOutput {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            }
        })?;
    let mut physical_request = request.clone();
    physical_request.output_ms = target.staging_path().to_path_buf();
    if let Some(configuration) = options.multi_ms
        && let Some(mut report) = multi_ms::transform(
            &physical_request,
            &request.output_ms,
            grammar,
            options,
            configuration,
        )?
    {
        if report.row_count > 0 {
            target
                .commit()
                .map_err(|error| MsTransformError::PrepareOutput {
                    path: request.output_ms.display().to_string(),
                    reason: error.to_string(),
                })?;
        }
        report.output_ms.clone_from(&request.output_ms);
        return Ok(report);
    }
    let members = if options.keep_mms.is_some() {
        multi_ms::members(&request.input_ms)?
    } else {
        None
    };
    let mut report = match (options.keep_mms, members) {
        (Some(true), Some(members)) => {
            let Some(report) = multi_ms::retain(
                &physical_request,
                &request.output_ms,
                grammar,
                options,
                &members,
            )?
            else {
                return Ok(empty_transform_report(request));
            };
            report
        }
        (Some(false), Some(members)) => transform_product(
            &physical_request,
            grammar,
            options,
            members.first().map(PathBuf::as_path),
            None,
        )?,
        _ => transform_product(&physical_request, grammar, options, None, None)?,
    };
    target
        .commit()
        .map_err(|error| MsTransformError::PrepareOutput {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        })?;
    report.output_ms = request.output_ms.clone();
    Ok(report)
}

fn empty_transform_report(request: &MsTransformRequest) -> MsTransformReport {
    MsTransformReport {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        row_count: 0,
        source_column: String::new(),
        output_column: String::new(),
        spw: request.spw.clone(),
        spectral_window_ids: Vec::new(),
        output_channels_by_spw: BTreeMap::new(),
        width: normalized_width(request.width),
        elapsed_ns: 0,
        retained_mms: None,
        write_telemetry: MeasurementSetWriteTelemetry::default(),
    }
}

fn transform_product(
    request: &MsTransformRequest,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    metadata_template: Option<&Path>,
    operation: Option<SpectralOperation<'_>>,
) -> Result<MsTransformReport, MsTransformError> {
    transform_selected_product(
        request,
        grammar,
        options,
        metadata_template,
        operation,
        None,
    )
}

fn transform_selected_product(
    request: &MsTransformRequest,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    metadata_template: Option<&Path>,
    operation: Option<SpectralOperation<'_>>,
    partition: Option<TransformSelection>,
) -> Result<MsTransformReport, MsTransformError> {
    let report = mstransform_staged(
        request,
        grammar,
        options,
        metadata_template,
        operation,
        partition,
    )?;
    if let Some(history) = &options.history {
        task_history::append_task_history(&request.output_ms, history)?;
    }
    Ok(report)
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn mstransform_staged(
    request: &MsTransformRequest,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    metadata_template: Option<&Path>,
    mut operation: Option<SpectralOperation<'_>>,
    partition: Option<TransformSelection>,
) -> Result<MsTransformReport, MsTransformError> {
    let order = match operation.as_ref().map(|op| &op.kernel) {
        Some(SpectrumOperator::Convolution(_)) => selection_metadata::MainRowOrder::SingleDish,
        Some(SpectrumOperator::Baseline(kernel)) if kernel.atmospheric_order() => {
            selection_metadata::MainRowOrder::Atmosphere
        }
        Some(SpectrumOperator::Baseline(_)) => selection_metadata::MainRowOrder::Baseline,
        None => selection_metadata::MainRowOrder::Transform,
    };
    let legacy_time = match options.legacy_tool {
        Some(LegacyMsTool::Partition(time) | LegacyMsTool::Split(time)) => Some(time),
        _ => None,
    };
    let legacy_grid = match options.legacy_tool {
        Some(LegacyMsTool::Regrid(grid)) => Some(grid),
        _ => None,
    };
    let legacy = legacy_time.is_some();
    let splitting = matches!(options.legacy_tool, Some(LegacyMsTool::Split(_)));
    let convolving = matches!(order, selection_metadata::MainRowOrder::SingleDish);
    let hanning = options.hanning && operation.is_none();
    let started_at = Instant::now();
    tracing::info!(
        input_ms = %request.input_ms.display(),
        output_ms = %request.output_ms.display(),
        spw = %request.spw,
        width = request.width,
        "mstransform started"
    );
    let stage_started_at = Instant::now();
    let input = MeasurementSet::open(&request.input_ms).map_err(|source| {
        MsTransformError::OpenMeasurementSet {
            path: request.input_ms.display().to_string(),
            source,
        }
    })?;
    maybe_log_transform_progress(
        "open_measurement_set",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let selection_error = |error| MsTransformError::SelectRows {
        path: request.input_ms.display().to_string(),
        source: match error {
            MsSelectionError::Domain(source) => source,
            other => MsError::InvalidInput(other.to_string()),
        },
    };
    let budget = MsSelectionIoBudget::from_system_memory(
        2,
        std::mem::size_of::<ResolvedMsSelectionRow>(),
        None,
    )
    .map_err(selection_error)?;
    let spw = match request.spw.trim() {
        "" => "*",
        spw => spw,
    };
    let spw_selection =
        input
            .resolve_spw_selector(spw)
            .map_err(|source| MsTransformError::InvalidSpw {
                selector: request.spw.clone(),
                reason: source.to_string(),
            })?;
    if splitting {
        let mut windows = BTreeSet::new();
        if spw_selection
            .channel_list()
            .iter()
            .any(|range| !windows.insert(range.spw_id))
        {
            return Err(MsTransformError::EmptySelection {
                path: request.input_ms.display().to_string(),
            });
        }
    }
    if splitting
        && options.time_averaging.is_some()
        && spw_selection
            .channel_list()
            .first()
            .is_some_and(|range| range.step > 1)
    {
        return Err(MsTransformError::Spectrum(
            "Simultaneous time and channel averaging is not handled.".into(),
        ));
    }
    let view_factory = options.input_view.as_deref();
    let mut visibility = VisibilityColumns::resolve(
        &input,
        request.data_column,
        options.visibility_columns.as_deref(),
        &request.input_ms,
        view_factory,
    )?;
    if legacy {
        visibility.promote_singleton_visibility();
    }
    let TransformSelection {
        selection,
        correlations,
        time: time_selection,
        mut rows,
        antenna_ids,
        tile_window,
    } = match partition {
        Some(partition) => partition,
        None => resolve_transform_selection(&input, request, grammar, budget, operation.is_some())?,
    };
    maybe_log_transform_progress(
        "select_rows",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let channel_selection = resolve_transform_channels(&input, &spw_selection, &request.spw)?;
    let continuum = options
        .continuum
        .as_ref()
        .map(|options| ContinuumFit::new(&input, &channel_selection, options))
        .transpose()?;
    let task_average = grammar == RowGrammar::Transform
        && !legacy
        && (normalized_width(request.width) > 1 || options.channel_widths.is_some());
    let widths = channel_widths_by_spw(
        &channel_selection,
        &spw_selection,
        request.width,
        options.channel_widths.as_deref(),
    )?;
    if splitting
        && options
            .channel_widths
            .as_ref()
            .is_some_and(|widths| widths.contains(&0))
    {
        return Err(MsTransformError::InvalidSplitStep);
    }
    let average_metadata = task_average.then(|| {
        channel_selection
            .iter()
            .map(|(&spw, selection)| (spw, selection.metadata_for_width(widths[&spw])))
            .collect::<BTreeMap<_, _>>()
    });
    let regrid_options = options.regrid.as_ref();
    let retained_combination = if options.preserve_ids && options.combine_spws {
        let all_spws =
            input
                .resolve_spw_selector("*")
                .map_err(|source| MsTransformError::InvalidSpw {
                    selector: "*".into(),
                    reason: source.to_string(),
                })?;
        let mut channels = resolve_transform_channels(&input, &all_spws, "*")?;
        channels.extend(
            channel_selection
                .iter()
                .map(|(&id, selection)| (id, selection.clone())),
        );
        Some((all_spws, channels))
    } else {
        None
    };
    let mut channel_bins = channel_bins_by_spw(
        &channel_selection,
        &widths,
        if regrid_options.is_some() {
            None
        } else {
            average_metadata.as_ref()
        },
    )?;
    let mut combined_grid =
        if options.combine_spws || regrid_options.is_some_and(|options| options.nspw > 1) {
            if options.channel_widths.is_some() {
                return Err(MsTransformError::Spectrum(
                    "If SPW combination is active, chabin cannot be an array".into(),
                ));
            }
            combine::Grid::new(
                &input,
                &channel_selection,
                retained_combination
                    .as_ref()
                    .map_or(&channel_selection, |(_, channels)| channels),
                normalized_width(request.width),
                budget.available_bytes / budget.maximum_live_blocks,
                matches!(legacy_grid, Some(LegacySpectralGrid::Cvel)),
            )?
        } else {
            None
        };
    let ddid_to_spw = data_description_spw_map(&input)?;
    if splitting {
        let mut seen = BTreeSet::new();
        if ddid_to_spw
            .values()
            .filter(|spw| channel_selection.contains_key(spw))
            .any(|spw| !seen.insert(spw))
        {
            return Err(MsTransformError::EmptySelection {
                path: request.input_ms.display().to_string(),
            });
        }
    }
    let output_spws = options
        .legacy_output_spw
        .as_deref()
        .map(|selector| {
            input
                .resolve_spw_selector(selector)
                .map_err(|error| MsTransformError::InvalidSpw {
                    selector: selector.to_owned(),
                    reason: error.to_string(),
                })
                .map(|selection| {
                    selection
                        .spw_ids()
                        .iter()
                        .map(|&id| id as i32)
                        .collect::<BTreeSet<_>>()
                })
        })
        .transpose()?;
    rows.retain(|row| {
        spw_selection.selects_row(row.data_desc_id)
            && channel_bins.contains_key(&row.spectral_window_id)
            && output_spws
                .as_ref()
                .is_none_or(|spws| spws.contains(&row.spectral_window_id))
    });
    let (mut selected_rows, mut selected_ddids): (Vec<_>, Vec<_>) = rows
        .iter()
        .map(|row| (row.row_index, row.data_desc_id))
        .unzip();
    maybe_log_transform_progress(
        "filter_rows_by_spw",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    if selected_rows.is_empty() {
        return Err(MsTransformError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    if !request.keep_flags {
        let stage_started_at = Instant::now();
        let dimensions = transform_array_dimensions(
            &input,
            &selected_ddids,
            &ddid_to_spw,
            &channel_bins,
            correlations.as_ref(),
            None,
        )?;
        let flag_rows = flag_filter_read_plan(selected_rows.len(), budget, dimensions.values())
            .map_err(selection_error)?;
        (selected_rows, selected_ddids) =
            keep_unflagged_rows(&input, &selected_rows, &selected_ddids, flag_rows, None)?;
        if options.time_averaging.is_some() || view_factory.is_some() {
            let kept = selected_rows.iter().copied().collect::<BTreeSet<_>>();
            rows.retain(|row| kept.contains(&row.row_index));
        }
        maybe_log_transform_progress(
            "drop_fully_flagged_rows",
            stage_started_at.elapsed(),
            started_at.elapsed(),
        );
        if selected_rows.is_empty() {
            tracing::warn!(
                input_ms = %request.input_ms.display(),
                "mstransform selected no rows after dropping fully flagged data"
            );
            return Err(MsTransformError::EmptySelection {
                path: request.input_ms.display().to_string(),
            });
        }
    }
    let legacy_direct_band = legacy_grid.is_some()
        && combined_grid.is_none()
        && options.time_averaging.is_none()
        && options.channel_widths.is_none()
        && normalized_width(request.width) == 1
        && spw == "*";
    let first_selected_row = selected_rows
        .iter()
        .copied()
        .min()
        .expect("nonempty row selection");
    let prepared_view = if let Some(view) = view_factory {
        let channels = channel_selection
            .iter()
            .map(|(&id, selection)| (id, selection.channels.as_slice()))
            .collect();
        Some(view.prepare(&input, &rows, &channels).map_err(|source| {
            MsTransformError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            }
        })?)
    } else {
        None
    };
    let input_view = prepared_view.as_deref();
    let mut regrid = regrid_options
        .map(|options| {
            regrid::Plan::new(
                &input,
                (options, legacy_grid),
                combined_grid.as_mut(),
                &channel_selection,
                &mut channel_bins,
                (
                    first_selected_row,
                    ((legacy_direct_band
                        && matches!(
                            legacy_grid,
                            Some(
                                LegacySpectralGrid::CenteredChannels { .. }
                                    | LegacySpectralGrid::CenteredNumeric { .. }
                            )
                        )
                        && options.nspw == 1)
                        || (matches!(legacy_grid, Some(LegacySpectralGrid::Cvel))
                            && !options.outframe.trim().is_empty()))
                    .then_some(selected_rows.as_slice()),
                ),
                budget.available_bytes / budget.maximum_live_blocks,
            )
        })
        .transpose()?;
    let stage_started_at = Instant::now();
    let time_groups = options
        .time_averaging
        .map(|time| {
            if let Some(legacy) = legacy_time {
                let dimensions = transform_array_dimensions(
                    &input,
                    &selected_ddids,
                    &ddid_to_spw,
                    &channel_bins,
                    correlations.as_ref(),
                    None,
                )?;
                let per_block = flag_filter_read_plan(rows.len(), budget, dimensions.values())
                    .map_err(selection_error)?;
                let flag_axes = (splitting && (spw.contains(':') || correlations.is_some()))
                    .then_some(SelectedFlagAxes {
                        channels: &channel_selection,
                        ddid_to_spw: &ddid_to_spw,
                        correlations: correlations.as_ref(),
                    });
                let (valid, _) = keep_unflagged_rows(
                    &input,
                    &selected_rows,
                    &selected_ddids,
                    per_block,
                    flag_axes.as_ref(),
                )?;
                let groups = legacy_time::plan(&input, &rows, valid, time, legacy)?;
                if groups.is_empty() {
                    return Err(MsTransformError::EmptySelection {
                        path: request.input_ms.display().to_string(),
                    });
                }
                Ok(groups)
            } else {
                time_average::plan(&input, &rows, time, order, None)
            }
        })
        .transpose()?;
    drop(rows);
    let selection_metadata::OrderedMainRows {
        rows: selected_rows,
        descriptions: selected_ddids,
        starts_visibility_buffer,
        ..
    } = if let Some(groups) = &time_groups {
        let (rows, descriptions) = groups
            .iter()
            .map(|group| (group.first.row, group.first.description))
            .unzip();
        selection_metadata::OrderedMainRows {
            rows,
            descriptions,
            starts_visibility_buffer: Vec::new(),
            phase_reference_rows: Vec::new(),
        }
    } else if legacy || legacy_direct_band {
        // In-place regridding leaves existing MAIN rows in their original order.
        selection_metadata::OrderedMainRows {
            rows: selected_rows,
            descriptions: selected_ddids,
            starts_visibility_buffer: Vec::new(),
            phase_reference_rows: Vec::new(),
        }
    } else {
        selection_metadata::sort_selected_rows(
            input.main_table(),
            &request.input_ms,
            selected_rows,
            selected_ddids,
            order,
            false,
        )?
    };
    if let Some(SpectralOperation {
        kernel: SpectrumOperator::Convolution(kernel),
        ..
    }) = &mut operation
    {
        let windows = input
            .spectral_window()
            .map_err(|source| selection_error(MsSelectionError::Domain(source)))?;
        let selected = selected_ddids.iter().copied().collect::<BTreeSet<_>>();
        let counts = selected
            .iter()
            .map(|ddid| {
                windows
                    .num_chan(ddid_to_spw[ddid] as usize)
                    .map(|channels| channels as usize)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| selection_error(MsSelectionError::Domain(source)))?;
        kernel
            .prepare(&counts)
            .map_err(MsTransformError::Spectrum)?;
    }
    let hands = match &operation {
        Some(SpectralOperation {
            kernel: SpectrumOperator::Baseline(kernel),
            ..
        }) if kernel.reads_parallel_hands() => {
            correlation_metadata::parallel_hands(&input, correlations.as_ref(), &selected_ddids)
                .map_err(|source| selection_error(MsSelectionError::Domain(source)))?
        }
        _ => BTreeMap::new(),
    };
    let spectral_rows = matches!(
        order,
        selection_metadata::MainRowOrder::Baseline | selection_metadata::MainRowOrder::Atmosphere
    )
    .then(|| {
        baseline::spectrum_rows(
            input.main_table(),
            request,
            &selected_rows,
            selected_ddids
                .iter()
                .map(|id| (ddid_to_spw[id], hands.get(id).copied())),
        )
    })
    .transpose()?;
    let averaging_time = time_groups.is_some();
    let legacy_channel_averaging = splitting
        && !averaging_time
        && channel_bins
            .values()
            .any(|bins| !bins_are_single_channel(bins));
    let combination = combined_grid
        .as_ref()
        .map(|grid| {
            let times = time_groups
                .as_deref()
                .map(|groups| {
                    combine::projected_times(
                        &input,
                        groups,
                        &channel_selection,
                        &ddid_to_spw,
                        correlations.as_ref(),
                        input_view,
                    )
                })
                .transpose()?;
            combine::group_rows(
                &input,
                &selected_rows,
                &selected_ddids,
                &ddid_to_spw,
                &grid.windows,
                times.as_deref().zip(time_groups.as_deref()),
            )
        })
        .transpose()?;
    let representative_rows;
    let representative_ddids;
    let (output_input_rows, output_input_ddids) = if let Some(groups) = &combination {
        representative_rows = groups
            .iter()
            .map(|group| selected_rows[group.representative])
            .collect::<Vec<_>>();
        representative_ddids = groups
            .iter()
            .map(|group| selected_ddids[group.representative])
            .collect::<Vec<_>>();
        (&representative_rows, &representative_ddids)
    } else {
        (&selected_rows, &selected_ddids)
    };
    let polarization = options
        .single_dish_polarization
        .filter(|_| !averaging_time)
        .map(|mode| {
            PolarizationProjection::resolve(&input, correlations.as_ref(), &selected_ddids, mode)
        })
        .transpose()
        .map_err(|source| selection_error(MsSelectionError::Domain(source)))?
        .flatten();
    let selected_field_ids_for_metadata =
        selected_field_ids_for_rows(input.main_table(), output_input_rows, &request.output_ms)?;
    maybe_log_transform_progress(
        "sort_selected_rows",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let float_convolution = convolving && request.data_column == TransformDataColumn::FloatData;
    let read_spectral_weights = polarization.is_some()
        || matches!(
            &operation,
            Some(SpectralOperation { kernel: SpectrumOperator::Baseline(kernel), .. })
                if kernel.reads_spectral_weights()
        );
    let mut spectra = if legacy {
        SpectrumProjection::partition(input.main_table(), first_selected_row, averaging_time)
    } else if legacy_grid.is_some() {
        Ok(SpectrumProjection::existing(input.main_table()))
    } else {
        SpectrumProjection::resolve(
            input.main_table(),
            first_selected_row,
            grammar,
            options.use_weight_spectrum,
            (
                task_average,
                averaging_time,
                convolving || combination.is_some() || regrid.is_some(),
                hanning,
            ),
            &visibility,
        )
    }
    .map_err(|source| MsTransformError::MutateMeasurementSet {
        path: request.input_ms.display().to_string(),
        source: Box::new(source),
    })?;
    if float_convolution && !averaging_time && polarization.is_none() {
        spectra.source.clear();
    }
    if continuum.is_some() {
        for name in STANDARD_SPECTRA {
            if !spectra.source.contains(&name)
                && input
                    .main_table()
                    .schema()
                    .is_some_and(|schema| schema.contains_column(name))
            {
                spectra.source.push(name);
            }
        }
    }
    if read_spectral_weights
        && !spectra.source.contains(&"WEIGHT_SPECTRUM")
        && input
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"))
    {
        spectra.source.push("WEIGHT_SPECTRUM");
    }

    let mut writes_rows = true;
    if let Some(SpectralOperation {
        kernel: SpectrumOperator::Baseline(kernel),
        ..
    }) = operation.as_ref()
    {
        writes_rows = kernel.writes_rows();
        if kernel.updates_weight() || !writes_rows {
            if !averaging_time && !read_spectral_weights {
                spectra.source.clear();
            }
            spectra.fill_standard = false;
            spectra.propagate_weights = false;
            if kernel.omit_spectral_weights() {
                spectra.output.clear();
            }
        }
    }
    let partitions = regrid.as_ref().and_then(regrid::Plan::split).cloned();
    let copies_per_row = partitions.as_ref().map_or(1, regrid::Partitions::count);
    let selected_output_rows = if writes_rows {
        output_input_rows.len()
    } else {
        0
    };
    let output_rows = selected_output_rows
        .checked_mul(copies_per_row)
        .ok_or_else(|| MsTransformError::Spectrum("Too many partitioned output rows.".into()))?;
    prepare_output_root(&request.output_ms)?;
    let metadata_path = metadata_template.unwrap_or(&request.input_ms);
    let mut output_main = materialize_empty_main_table(
        metadata_path,
        output_rows,
        &request.output_ms,
        &visibility,
        grammar,
        &spectra,
        legacy,
    )?;
    maybe_log_transform_progress(
        "materialize_selected_main",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    copy_subtables(
        &input,
        metadata_path,
        &request.output_ms,
        &mut output_main,
        legacy,
    )?;
    if grammar == RowGrammar::Transform
        && (task_average || !matches!(request.spw.as_str(), "" | "*"))
        && !legacy
    {
        flag_commands::rewrite(
            &input,
            &request.output_ms,
            &request.spw,
            &channel_selection,
            normalized_width(request.width),
            options.flag_command_widths.as_deref(),
            options.channel_widths.as_deref(),
        )?;
    }
    maybe_log_transform_progress(
        "copy_subtables",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let reindex =
        !options.preserve_ids && operation.as_ref().is_none_or(|operation| operation.reindex);
    let reindex_spectra = reindex || combined_grid.is_some() || copies_per_row > 1;
    let ddi_offset = if reindex_spectra {
        i32::try_from(options.ddi_start).map_err(|_| {
            MsTransformError::Spectrum("ddistart exceeds the data-description index limit.".into())
        })?
    } else {
        0
    };
    let mut compact_metadata = if reindex_spectra {
        let (metadata_spws, metadata_bins) = if let Some((all_spws, channels)) =
            &retained_combination
            && combined_grid.is_some()
        {
            (
                all_spws,
                Some(
                    channels
                        .iter()
                        .map(|(&id, channels)| (id, vec![channels.channels.clone()]))
                        .collect::<BTreeMap<_, _>>(),
                ),
            )
        } else {
            (&spw_selection, None)
        };
        compact_spectral_subtables(
            &input,
            &request.output_ms,
            metadata_bins.as_ref().unwrap_or(&channel_bins),
            &ddid_to_spw,
            metadata_spws,
            correlations.as_ref(),
            grammar,
        )?
    } else {
        preserve_spectral_ids(&input, request, &channel_bins, &ddid_to_spw)?
    };
    if let Some(correlations) = correlations.as_ref() {
        correlations.write_polarizations(&input, request)?;
    }
    if let Some(grid) = &combined_grid {
        combine::write_metadata(request, grid, &mut compact_metadata)?;
    }
    if let Some(projection) = &polarization {
        projection.write_metadata(request, &compact_metadata.ddid_map)?;
    }
    let (field_id_map, row_id_maps) = if reindex {
        let field_id_map = selection_metadata::reindex_fields_and_sources(
            &input,
            request,
            selection.field.as_deref(),
            &compact_metadata.spw_map,
        )?;
        let antenna_id_map = selection_metadata::reindex_antenna_subtables(
            &input,
            request,
            antenna_ids.as_deref(),
            &compact_metadata.spw_map,
            time_selection.as_ref(),
        )?;
        let row_id_maps = selection_metadata::reindex_selected_row_ids(
            &input,
            request,
            antenna_id_map,
            grammar == RowGrammar::Transform,
            splitting.then_some(selected_rows.as_slice()),
        )?;
        (field_id_map, row_id_maps)
    } else {
        (None, selection_metadata::RowIdMaps::default())
    };
    if combined_grid.is_some() {
        combine::remap_subtable_references(request)?;
    }
    if grammar == RowGrammar::Transform && (legacy_grid.is_none() || combined_grid.is_some()) {
        selection_metadata::remove_virtual_model_links(
            &input,
            &mut output_main,
            &request.output_ms,
        )?;
    }
    maybe_log_transform_progress(
        "compact_spectral_subtables",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    let row_count = output_input_rows.len();
    let write_resources = MeasurementSetWriteResources::from_system_memory(2).map_err(|error| {
        MsTransformError::WriteMeasurementSet {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        }
    })?;
    let mut tile = options
        .tiling
        .map(|mode| {
            output_layout::resolve_tiling(
                &input,
                mode,
                (&spw_selection, tile_window),
                (
                    &channel_selection,
                    (combined_grid.is_none() && regrid_options.is_none()).then_some(&widths),
                ),
                (&selected_ddids, &ddid_to_spw, correlations.as_ref()),
            )
        })
        .transpose()?;
    if tile.is_none() && legacy {
        tile = Some(output_layout::partition_tiling(
            &input,
            &visibility,
            &channel_selection,
            &selected_ddids,
        )?);
    }
    let (write_plan, input_batch_rows) = transform_array_write_plan(
        &input,
        TransformArrayRows {
            selected_rows: output_input_rows,
            selected_ddids: output_input_ddids,
            groups: if combination.is_some() {
                None
            } else {
                time_groups.as_deref()
            },
            output_rows,
            output_schema: &output_main,
            regrid: regrid.as_ref(),
            tile,
            combined: combined_grid
                .as_ref()
                .zip(combination.as_deref())
                .map(|(grid, groups)| CombinedArrayRows {
                    grid,
                    groups,
                    source_rows: &selected_rows,
                    source_ddids: &selected_ddids,
                    source_time_groups: time_groups.as_deref(),
                }),
        },
        &ddid_to_spw,
        &channel_bins,
        (correlations.as_ref(), polarization.as_ref()),
        (
            &visibility,
            &spectra,
            view_factory
                .map_or(0, TransformInputView::additional_bytes_per_sample)
                .saturating_add(
                    continuum
                        .as_ref()
                        .map_or(0, ContinuumFit::working_bytes_per_sample),
                ),
        ),
        write_resources,
    )?;
    let batch_rows = if writes_rows {
        let per_output = combination
            .as_ref()
            .and_then(|groups| groups.iter().map(|group| group.members.len()).max())
            .unwrap_or(1);
        write_plan
            .batch_rows()
            .min((input_batch_rows / per_output).max(1))
    } else {
        input_batch_rows
    };
    let mut write_session = MeasurementSetWriteSession::start(&request.output_ms, write_plan)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        })?;
    let stage_started_at = Instant::now();
    let partitioned_rows;
    let scalar_input_rows = if copies_per_row == 1 {
        &output_input_rows[..selected_output_rows]
    } else {
        partitioned_rows = output_input_rows[..selected_output_rows]
            .iter()
            .flat_map(|&row| std::iter::repeat_n(row, copies_per_row))
            .collect::<Vec<_>>();
        &partitioned_rows
    };
    let mut output_column_overrides = gather_selected_main_column_overrides(
        (input.main_table(), &output_main),
        scalar_input_rows,
        &request.output_ms,
        batch_rows,
        &mut write_session,
        &row_id_maps,
        (
            averaging_time,
            polarization.is_some() || continuum.is_some(),
            combination.is_some(),
        ),
    )?;
    maybe_log_transform_progress(
        "load_main_column_overrides",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let mut touched_spws = BTreeSet::new();
    let row_spectrum_names = if averaging_time || combination.is_some() {
        &STANDARD_SPECTRA[..]
    } else {
        &spectra.source
    };
    let local_channel_bins = averaging_time.then(|| {
        channel_bins
            .iter()
            .map(|(&spw, bins)| {
                let mut start = 0;
                let positions = bins
                    .iter()
                    .map(|bin| {
                        let end = start + bin.len();
                        let local = (start..end).collect();
                        start = end;
                        local
                    })
                    .collect();
                (spw, positions)
            })
            .collect::<BTreeMap<i32, Vec<Vec<usize>>>>()
    });
    let binned_time_channels = (splitting
        && averaging_time
        && channel_bins
            .values()
            .any(|bins| !bins_are_single_channel(bins)))
    .then(|| {
        channel_bins
            .iter()
            .map(|(&spw, bins)| (spw, (0..bins.len()).map(|channel| vec![channel]).collect()))
            .collect::<BTreeMap<i32, Vec<Vec<usize>>>>()
    });
    let mut spectrum_available = vec![false; row_spectrum_names.len()];
    let mut time_projection = time_groups.as_ref().map(|_| {
        TimeProjection::new(
            input.main_table(),
            &request.input_ms,
            &spectra.source,
            &visibility,
            input_batch_rows,
            input_view,
        )
    });
    for batch_start in (0..row_count).step_by(batch_rows.max(1)) {
        let batch_end = (batch_start + batch_rows.max(1)).min(row_count);
        let input_rows = &output_input_rows[batch_start..batch_end];
        let mut combined_scalars = Vec::new();
        let (mut input_block, mut group_times) = if let (Some(grid), Some(groups)) =
            (&combined_grid, &combination)
        {
            let result = combine::read_batch(
                grid,
                &groups[batch_start..batch_end],
                &input,
                (&selected_rows, &selected_ddids),
                (&channel_selection, &ddid_to_spw, correlations.as_ref()),
                (&visibility, &spectra.source, input_view, continuum.as_ref()),
                time_projection.as_mut().zip(time_groups.as_deref()),
            )?;
            combined_scalars = result.exposure_flags;
            (result.arrays, result.coordinates)
        } else if let (Some(projection), Some(groups)) = (&mut time_projection, &time_groups) {
            let plans = groups[batch_start..batch_end]
                .iter()
                .map(|group| {
                    let description = group.first.description;
                    let spw = ddid_to_spw[&description];
                    TimeGroupRead {
                        group,
                        channels: &channel_selection[&spw].channels,
                        axes: correlations
                            .as_ref()
                            .and_then(|plan| plan.axes(description)),
                        legacy_bins: binned_time_channels.as_ref().and_then(|_| {
                            let bins = &channel_bins[&spw];
                            (!bins_are_single_channel(bins)).then(|| {
                                let first = &bins[0];
                                let width = first[0].abs_diff(first[first.len() - 1]) + 1;
                                (
                                    &local_channel_bins.as_ref().expect("time averaging")[&spw][..],
                                    width,
                                )
                            })
                        }),
                    }
                })
                .collect::<Vec<_>>();
            projection.average_groups(&plans)?
        } else {
            let block = MainArrayBlock::read(
                input.main_table(),
                input_rows,
                &visibility,
                &spectra.source,
                input_view,
            )
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            })?;
            (block, Vec::new())
        };
        let descriptions = &output_input_ddids[batch_start..batch_end];
        if grammar == RowGrammar::Transform
            && !averaging_time
            && !read_spectral_weights
            && combination.is_none()
            && !legacy
            && legacy_grid.is_none()
        {
            input_block.resolve_buffer_spectra(
                row_spectrum_names,
                &mut spectrum_available,
                &starts_visibility_buffer[batch_start..batch_end],
                input_rows,
                &request.input_ms,
            )?;
        }
        if let Some(fit) = &continuum
            && combination.is_none()
        {
            fit.apply(
                &mut input_block,
                &visibility,
                descriptions.iter().map(|id| ddid_to_spw[id]),
                row_spectrum_names,
                averaging_time,
            )?;
            for (coordinates, flags) in group_times.iter_mut().zip(&input_block.flags) {
                coordinates.flag_row = continuum::row_flag(flags.as_ref(), None);
            }
        }
        let legacy_row_flags = if legacy_channel_averaging {
            Some(
                input
                    .main_table()
                    .column_accessor("FLAG_ROW")
                    .and_then(|column| column.scalar_cells_owned_for_rows(input_rows))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: request.input_ms.display().to_string(),
                        source: Box::new(source),
                    })?,
            )
        } else {
            None
        };
        let mut input_row_flags = if let Some(projection) = &polarization {
            let row_flags = input
                .main_table()
                .column_accessor("FLAG_ROW")
                .and_then(|column| column.scalar_cells_owned_for_rows(input_rows))
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: request.input_ms.display().to_string(),
                    source: Box::new(source),
                })?;
            projection
                .apply(
                    &mut input_block,
                    (descriptions, &row_flags),
                    (&ddid_to_spw, &channel_selection),
                    &spectra.source,
                )
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: request.input_ms.display().to_string(),
                    source: Box::new(source),
                })?;
            writes_rows.then(|| row_flags.into_iter())
        } else {
            None
        };
        let mut data_values = visibility
            .copies()
            .iter()
            .zip(input_block.visibility.into_iter().map(Vec::into_iter))
            .collect::<Vec<_>>();
        let mut spectrum_values = row_spectrum_names
            .iter()
            .copied()
            .zip(input_block.spectra.into_iter().map(Vec::into_iter))
            .collect::<Vec<_>>();
        let flag_values = input_block.flags;
        let mut flag_category_values = input_block.categories.into_iter();
        let mut uvw_values = input_block.uvw.into_iter();
        let mut weight_values = input_block.weights.into_iter();
        let mut sigma_values = input_block.sigmas.into_iter();
        let mut row_spectra = vec![None; spectrum_values.len()];
        for (batch_offset, (flags, &ddid)) in flag_values.into_iter().zip(descriptions).enumerate()
        {
            let row_index = batch_start + batch_offset;
            let write_error =
                |source: crate::MeasurementSetWriteError| MsTransformError::WriteMeasurementSet {
                    path: request.output_ms.display().to_string(),
                    reason: source.to_string(),
                };
            let write_array = |session: &mut MeasurementSetWriteSession,
                               name: &str,
                               value: &ArrayValue| {
                if let Some(parts) = &partitions {
                    parts.write_channel_array(session, name, value, &request.output_ms, row_index)
                } else {
                    regrid::write_spectrum(session, name, value, &request.output_ms, row_index)
                }
            };
            let undefined = |session: &mut MeasurementSetWriteSession, name: &str| {
                for _ in 0..copies_per_row {
                    session.push_undefined_row(name).map_err(write_error)?;
                }
                Ok::<_, MsTransformError>(())
            };
            let averaged_polarization = polarization
                .as_ref()
                .is_some_and(|projection| projection.contains(ddid));
            let axes = if averaging_time || averaged_polarization || combination.is_some() {
                None
            } else {
                correlations
                    .as_ref()
                    .and_then(|selection| selection.axes(ddid))
            };
            if writes_rows && let Some(coordinates) = group_times.get(batch_offset) {
                for (name, value) in TIME_OUTPUT_SCALARS
                    .into_iter()
                    .zip(coordinates.scalar_cells())
                {
                    for _ in 0..copies_per_row {
                        write_session
                            .push_scalar_row(name, Some(value.clone()))
                            .map_err(write_error)?;
                    }
                }
            } else if let Some(&(exposure, flag_row)) = combined_scalars.get(batch_offset) {
                for (name, value) in [
                    ("EXPOSURE", ScalarValue::Float64(exposure)),
                    ("FLAG_ROW", ScalarValue::Bool(flag_row)),
                ] {
                    for _ in 0..copies_per_row {
                        write_session
                            .push_scalar_row(name, Some(value.clone()))
                            .map_err(write_error)?;
                    }
                }
            }
            let select_correlations = |value, name| {
                select_visibility_axes(value, axes, name).map_err(|source| {
                    MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    }
                })
            };
            let flags =
                flags.ok_or_else(|| missing_column_error(&request.output_ms, row_index, "FLAG"))?;
            let flags = select_correlations(flags, "FLAG")?;
            let spw_id = ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: request.output_ms.display().to_string(),
                    reason: format!("MAIN row {row_index} references DATA_DESC_ID {ddid}, which has no DATA_DESCRIPTION row"),
                }
            })?;
            let bins = binned_time_channels.as_ref().or(local_channel_bins.as_ref()).unwrap_or(&channel_bins).get(&spw_id).ok_or_else(|| MsTransformError::InvalidSpw {
                selector: request.spw.clone(),
                reason: format!("selected row {row_index} maps to spectral window {spw_id}, but --spw does not include that SPW"),
            })?;
            let bins = combined_grid.as_ref().map_or(bins, |grid| &grid.bins);
            if writes_rows && continuum.is_some() && !averaging_time && combination.is_none() {
                let flagged =
                    continuum::row_flag(Some(&flags), Some(&channel_selection[&spw_id].channels));
                for _ in 0..copies_per_row {
                    write_session
                        .push_scalar_row("FLAG_ROW", Some(ScalarValue::Bool(flagged)))
                        .map_err(write_error)?;
                }
            }
            let regrid = regrid
                .as_mut()
                .map(|plan| plan.row(spw_id, input.main_table(), input_rows[batch_offset]))
                .transpose()?;
            let mutation_error = |source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            };
            let noise =
                if float_convolution {
                    None
                } else {
                    let weight = weight_values.next().flatten().ok_or_else(|| {
                        missing_column_error(&request.output_ms, row_index, "WEIGHT")
                    })?;
                    let sigma = sigma_values.next().flatten().ok_or_else(|| {
                        missing_column_error(&request.output_ms, row_index, "SIGMA")
                    })?;
                    Some((
                        select_correlations(weight, "WEIGHT")?,
                        select_correlations(sigma, "SIGMA")?,
                    ))
                };
            for ((name, values), output) in spectrum_values.iter_mut().zip(&mut row_spectra) {
                *output = values
                    .next()
                    .flatten()
                    .filter(|value| !value.is_empty())
                    .map(|value| select_correlations(value, name))
                    .transpose()?;
            }
            let source_spectrum =
                |name| row_spectrum_names.iter().position(|&source| source == name);
            let spectra_cells = SpectrumCells {
                weight: source_spectrum("WEIGHT_SPECTRUM")
                    .and_then(|index| row_spectra[index].as_ref()),
                sigma: source_spectrum("SIGMA_SPECTRUM")
                    .and_then(|index| row_spectra[index].as_ref()),
            };
            let legacy_averages = if let Some(row_flags) = &legacy_row_flags
                && !bins_are_single_channel(bins)
            {
                let flag_row =
                    scalar_bool(row_flags[batch_offset].as_ref(), "FLAG_ROW", row_index)?;
                Some(
                    legacy_split::ChannelAverages::new(
                        &flags,
                        spectra_cells.weight,
                        bins,
                        flag_row,
                    )
                    .map_err(mutation_error)?,
                )
            } else {
                None
            };
            let projected_weights = if read_spectral_weights && !averaging_time {
                let channels = &channel_selection[&spw_id].channels;
                spectra_cells
                    .weight
                    .filter(|weight| {
                        weight
                            .shape()
                            .get(1)
                            .is_none_or(|&count| !all_channels_selected(channels, count))
                    })
                    .map(|weight| select_channels(weight.clone(), channels))
                    .transpose()
                    .map_err(mutation_error)?
            } else {
                None
            };
            let average_regrid_input =
                !legacy_direct_band && regrid.is_some() && !bins_are_single_channel(bins);
            let averages = if let (true, Some((weight, sigma))) =
                (task_average || average_regrid_input, &noise)
            {
                Some(
                    ChannelAverages::new(
                        &flags,
                        bins,
                        weight,
                        sigma,
                        spectra_cells,
                        &visibility,
                        (
                            spectra.propagate_weights
                                || averaging_time
                                || combination.is_some()
                                || regrid.is_some(),
                            averaging_time,
                        ),
                    )
                    .map_err(mutation_error)?,
                )
            } else {
                None
            };
            let mut transformed_flags = if let Some(averages) = &legacy_averages {
                averages.flags()
            } else if let Some(averages) = &averages {
                averages.flags()
            } else if legacy_direct_band {
                flags.clone()
            } else {
                transform_flag_channels(flags.clone(), bins).map_err(mutation_error)?
            };
            let hanning_noise = if hanning && legacy_grid.is_some() {
                if bins.len() > 1 {
                    hanning::flag(&mut transformed_flags);
                }
                None
            } else if let (true, Some(noise)) = (hanning, noise.as_ref()) {
                let statistics = hanning::statistics(
                    noise,
                    spectra_cells,
                    &flags,
                    bins,
                    averages.as_ref(),
                    &visibility,
                    averaging_time,
                )
                .map_err(mutation_error)?;
                hanning::flag(&mut transformed_flags);
                Some(statistics)
            } else {
                None
            };
            let fft_flags = regrid
                .as_ref()
                .map(|regrid| regrid.fft_input_flags(&transformed_flags))
                .transpose()
                .map_err(mutation_error)?
                .flatten();
            let combined_noise = if let Some(regrid) = &regrid
                && legacy_grid.is_some()
            {
                transformed_flags = regrid
                    .apply(transformed_flags, None)
                    .map_err(mutation_error)?;
                None
            } else if let Some(regrid) = &regrid {
                let source_noise = if let Some(statistics) =
                    hanning_noise.or_else(|| averages.as_ref().map(ChannelAverages::statistics))
                {
                    Some((statistics.weight_spectrum, statistics.sigma_spectrum))
                } else if combination.is_some() {
                    spectra_cells
                        .weight
                        .cloned()
                        .zip(spectra_cells.sigma.cloned())
                } else if let Some((weight, sigma)) = &noise {
                    Some(
                        regrid::input_noise(
                            (weight, sigma),
                            spectra_cells,
                            &flags,
                            bins,
                            &visibility,
                            averaging_time,
                        )
                        .map_err(mutation_error)?,
                    )
                } else {
                    None
                };
                transformed_flags = regrid
                    .apply(transformed_flags, None)
                    .map_err(mutation_error)?;
                source_noise
                    .map(|(weight, sigma)| {
                        regrid.statistics(weight, sigma, &transformed_flags, fft_flags.as_ref())
                    })
                    .transpose()
                    .map_err(mutation_error)?
            } else if let Some(grid) = &combined_grid {
                let result = combine::finish_noise(
                    grid,
                    hanning_noise,
                    averages.as_ref(),
                    spectra_cells,
                    &transformed_flags,
                )
                .map_err(mutation_error)?;
                transformed_flags = grid
                    .interpolate(transformed_flags)
                    .map_err(mutation_error)?;
                Some(result)
            } else {
                hanning_noise
            };
            if let Some(original) = input_row_flags.as_mut().and_then(Iterator::next) {
                let flag_row = match &transformed_flags {
                    ArrayValue::Bool(flags) if averaged_polarization => {
                        Some(ScalarValue::Bool(flags.iter().all(|&flag| flag)))
                    }
                    _ => original,
                };
                write_session
                    .push_scalar_row("FLAG_ROW", flag_row)
                    .map_err(|source| MsTransformError::WriteMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        reason: source.to_string(),
                    })?;
            }
            let mut baseline_weights = None;
            for (column, values) in &mut data_values {
                let source = column.source.name();
                let target = column.output.name();
                let independent_lag =
                    column.source == OptionalMainColumn::LagData && legacy_grid.is_none();
                let Some(value) = values.next().flatten() else {
                    if legacy_direct_band || column.source == OptionalMainColumn::LagData {
                        undefined(&mut write_session, target)?;
                        continue;
                    }
                    return Err(missing_column_error(&request.output_ms, row_index, source));
                };
                let value = select_correlations(value, source)?;
                let mut transformed = if independent_lag || legacy_direct_band {
                    value
                } else if let Some(averages) = &legacy_averages {
                    averages.visibility(value).map_err(mutation_error)?
                } else if let Some(averages) = &averages {
                    averages
                        .visibility(column.source, value)
                        .map_err(mutation_error)?
                } else {
                    transform_data_channels(value, &flags, bins).map_err(|source| {
                        MsTransformError::MutateMeasurementSet {
                            path: request.output_ms.display().to_string(),
                            source: Box::new(source),
                        }
                    })?
                };
                let smooth_visibility = if legacy_grid.is_some() {
                    column.source != OptionalMainColumn::ModelData
                } else {
                    !independent_lag
                };
                if hanning && smooth_visibility {
                    if legacy_grid.is_some() {
                        hanning::smooth_regrid(&mut transformed, &flags).map_err(mutation_error)?;
                    } else {
                        hanning::smooth(
                            &mut transformed,
                            &[transformed_flags.shape()[0], bins.len()],
                        )
                        .map_err(mutation_error)?;
                    }
                }
                if !independent_lag {
                    if let Some(regrid) = &regrid {
                        transformed = regrid
                            .apply(transformed, fft_flags.as_ref())
                            .map_err(mutation_error)?;
                    } else if let Some(grid) = &combined_grid {
                        transformed = grid.interpolate(transformed).map_err(mutation_error)?;
                    }
                }
                if let Some(operation) = &mut operation {
                    let ArrayValue::Bool(output_flags) = &mut transformed_flags else {
                        return Err(MsTransformError::Spectrum(
                            "Spectral flags must be Bool.".into(),
                        ));
                    };
                    match &mut operation.kernel {
                        SpectrumOperator::Convolution(kernel) => {
                            kernel
                                .apply(&mut transformed, output_flags)
                                .map_err(MsTransformError::Spectrum)?;
                        }
                        SpectrumOperator::Baseline(handler) => {
                            let coordinates = spectral_rows
                                .as_ref()
                                .and_then(|rows| rows.get(row_index))
                                .ok_or_else(|| {
                                    MsTransformError::Spectrum(
                                        "Missing original spectrum coordinates.".into(),
                                    )
                                })?;
                            let mut coordinates = *coordinates;
                            if let Some(averaged) = group_times.get(batch_offset) {
                                coordinates.time = averaged.time;
                                coordinates.interval = averaged.interval;
                            }
                            if let Some(group) = time_groups
                                .as_ref()
                                .and_then(|groups| groups.get(row_index))
                            {
                                coordinates.selection_row = group.row_in_chunk;
                            }
                            baseline_weights = handler
                                .process(
                                    &coordinates,
                                    &mut transformed,
                                    output_flags,
                                    SpectrumWeights {
                                        scalar: noise.as_ref().map(|(weight, _)| weight),
                                        spectrum: projected_weights
                                            .as_ref()
                                            .or(spectra_cells.weight),
                                    },
                                )
                                .map_err(MsTransformError::Spectrum)?;
                        }
                    }
                }
                if !writes_rows {
                    continue;
                }
                if independent_lag {
                    for _ in 0..copies_per_row {
                        regrid::write_spectrum(
                            &mut write_session,
                            target,
                            &transformed,
                            &request.output_ms,
                            row_index,
                        )?;
                    }
                } else {
                    write_array(&mut write_session, target, &transformed)?;
                }
            }
            if !writes_rows {
                touched_spws.insert(spw_id);
                continue;
            }
            write_array(&mut write_session, "FLAG", &transformed_flags)?;
            if let Some(flag_category) = flag_category_values
                .next()
                .flatten()
                .filter(|value| !value.is_empty())
            {
                let flag_category = select_correlations(flag_category, "FLAG_CATEGORY")?;
                let mut transformed = transform_flag_category_channels(flag_category, bins)
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    })?;
                if let Some(regrid) = &regrid {
                    if transformed.shape()[1] != 1 || combination.is_some() {
                        transformed = regrid
                            .apply_categories(transformed)
                            .map_err(mutation_error)?;
                    }
                } else if let Some(grid) = &combined_grid {
                    transformed = grid
                        .interpolate_categories(transformed)
                        .map_err(mutation_error)?;
                }
                write_array(&mut write_session, "FLAG_CATEGORY", &transformed)?;
            } else {
                undefined(&mut write_session, "FLAG_CATEGORY")?;
            }
            let uvw = uvw_values
                .next()
                .flatten()
                .ok_or_else(|| missing_column_error(&request.output_ms, row_index, "UVW"))?;
            for _ in 0..copies_per_row {
                push_f64_array_row(
                    &mut write_session,
                    "UVW",
                    &uvw,
                    &request.output_ms,
                    row_index,
                )?;
            }
            let averaged_noise =
                combined_noise.or_else(|| averages.map(|averages| averages.statistics()));
            if let Some(weights) = baseline_weights
                && let Some((original_weight, sigma)) = &noise
            {
                let (mut original_weight, mut sigma) = (original_weight.clone(), sigma.clone());
                visibility.align_scalar_weights(&mut original_weight, &mut sigma);
                push_f32_array_row(
                    &mut write_session,
                    "WEIGHT",
                    &weights,
                    &request.output_ms,
                    row_index,
                )?;
                push_f32_array_row(
                    &mut write_session,
                    "SIGMA",
                    &sigma,
                    &request.output_ms,
                    row_index,
                )?;
                for &column in &spectra.output {
                    undefined(&mut write_session, column)?;
                }
                touched_spws.insert(spw_id);
                continue;
            }
            let Some((weight, sigma)) = noise else {
                // CASA publishes these columns with undefined cells when the
                // transformed visibility column is FLOAT_DATA.
                for column in ["WEIGHT", "SIGMA"]
                    .into_iter()
                    .chain(spectra.output.iter().copied())
                {
                    undefined(&mut write_session, column)?;
                }
                touched_spws.insert(spw_id);
                continue;
            };
            if let Some(parts) = &partitions {
                let statistics = averaged_noise.ok_or_else(|| {
                    MsTransformError::Spectrum("Missing split-regrid noise statistics.".into())
                })?;
                parts.write_noise(
                    &mut write_session,
                    &statistics,
                    &transformed_flags,
                    spectra.fill_standard,
                    &request.output_ms,
                    row_index,
                )?;
                if !spectra.fill_standard {
                    for &column in &spectra.output {
                        undefined(&mut write_session, column)?;
                    }
                }
                touched_spws.insert(spw_id);
                continue;
            }
            // TimeProjection already put the legacy weights on this grid.
            let projected_legacy_weight = legacy
                && averaging_time
                && spectra.fill_standard
                && spectra_cells.weight.is_some_and(|weight| {
                    weight.shape() == flags.shape()
                        && weight.shape().get(1) == Some(&bins.len())
                        && bins
                            .iter()
                            .enumerate()
                            .all(|(i, bin)| bin.as_slice() == [i])
                });
            let (mut weight, mut sigma, task_spectra) = if let Some(SpectralOperation {
                kernel: SpectrumOperator::Convolution(kernel),
                ..
            }) = &operation
            {
                let corrected = visibility.corrected_becomes_data();
                let (weight, sigma) =
                    unaveraged_spectra(&weight, &sigma, spectra_cells, &flags, bins, corrected)
                        .map_err(mutation_error)?;
                let ArrayValue::Bool(output_flags) = &transformed_flags else {
                    return Err(MsTransformError::Spectrum(
                        "Spectral flags must be Bool.".into(),
                    ));
                };
                let statistics = convolution::statistics(
                    weight,
                    sigma,
                    output_flags,
                    kernel.weight_kernel(),
                    corrected,
                )
                .map_err(mutation_error)?;
                (
                    statistics.weight,
                    statistics.sigma,
                    spectra
                        .fill_standard
                        .then_some((statistics.weight_spectrum, statistics.sigma_spectrum)),
                )
            } else if let Some(averages) = averaged_noise {
                (
                    averages.weight,
                    averages.sigma,
                    spectra
                        .fill_standard
                        .then_some((averages.weight_spectrum, averages.sigma_spectrum)),
                )
            } else if spectra.fill_standard && !projected_legacy_weight {
                let values = unaveraged_spectra(
                    &weight,
                    &sigma,
                    spectra_cells,
                    &flags,
                    bins,
                    visibility.corrected_becomes_data() && !averaging_time,
                )
                .map_err(mutation_error)?;
                (weight, sigma, Some(values))
            } else {
                (weight, sigma, None)
            };
            if let Some(regrid) = &regrid
                && legacy_grid.is_some()
            {
                regrid
                    .scale_noise(&mut weight, false, hanning)
                    .map_err(mutation_error)?;
                regrid
                    .scale_noise(&mut sigma, true, hanning)
                    .map_err(mutation_error)?;
            } else if continuum.is_some()
                && !task_average
                && !convolving
                && !hanning
                && combination.is_none()
                && regrid.is_none()
            {
                let channels =
                    (!averaging_time).then_some(channel_selection[&spw_id].channels.as_slice());
                continuum::mean_noise(&mut weight, spectra_cells.weight, &flags, channels);
                continuum::mean_noise(&mut sigma, spectra_cells.sigma, &flags, channels);
                visibility.align_scalar_weights(&mut weight, &mut sigma);
            } else if splitting && !averaging_time {
                if let Some(bin) = bins.first().filter(|bin| bin.len() > 1) {
                    let width = bin[0].abs_diff(bin[bin.len() - 1]) + 1;
                    legacy_split::scale_channel_noise(&visibility, width, &mut weight, &mut sigma)
                        .map_err(mutation_error)?;
                }
                legacy_split::align_row_noise(&visibility, &mut weight, &mut sigma)
                    .map_err(mutation_error)?;
            } else if grammar == RowGrammar::Transform
                && !task_average
                && !averaging_time
                && !convolving
                && !hanning
                && combination.is_none()
                && regrid.is_none()
                && !legacy
            {
                visibility.align_scalar_weights(&mut weight, &mut sigma);
            }
            push_f32_array_row(
                &mut write_session,
                "WEIGHT",
                &weight,
                &request.output_ms,
                row_index,
            )?;
            push_f32_array_row(
                &mut write_session,
                "SIGMA",
                &sigma,
                &request.output_ms,
                row_index,
            )?;
            let (mut task_weight, mut task_sigma) =
                task_spectra.map_or((None, None), |(weight, sigma)| (Some(weight), Some(sigma)));
            let mut legacy_weight_spectrum = legacy_averages
                .as_ref()
                .map(legacy_split::ChannelAverages::weight_spectrum);
            for &name in &spectra.output {
                let transformed = match name {
                    _ if legacy_grid.is_some() => {
                        let projector = regrid.as_ref().expect("legacy spectral rewrite grid");
                        source_spectrum(name)
                            .and_then(|index| row_spectra[index].take())
                            .map(|value| {
                                let mut output = projector.apply(value, fft_flags.as_ref())?;
                                projector.scale_noise(
                                    &mut output,
                                    name == "SIGMA_SPECTRUM",
                                    hanning,
                                )?;
                                Ok(output)
                            })
                            .transpose()
                            .map_err(mutation_error)?
                    }
                    "WEIGHT_SPECTRUM" if legacy_weight_spectrum.is_some() => {
                        legacy_weight_spectrum.take().flatten()
                    }
                    "WEIGHT_SPECTRUM" if projected_legacy_weight => {
                        source_spectrum(name).and_then(|index| row_spectra[index].take())
                    }
                    "WEIGHT_SPECTRUM"
                        if grammar == RowGrammar::Transform && (!legacy || averaging_time) =>
                    {
                        task_weight.take()
                    }
                    "SIGMA_SPECTRUM" if grammar == RowGrammar::Transform => task_sigma.take(),
                    _ => source_spectrum(name)
                        .and_then(|index| row_spectra[index].take())
                        .map(|value| {
                            if name == "SIGMA_SPECTRUM" {
                                transform_sigma_channels(value, &flags, bins)
                            } else {
                                transform_weight_channels(value, &flags, bins)
                            }
                        })
                        .transpose()
                        .map_err(mutation_error)?,
                };
                if let Some(transformed) = transformed {
                    push_f32_array_row(
                        &mut write_session,
                        name,
                        &transformed,
                        &request.output_ms,
                        row_index,
                    )?;
                } else {
                    undefined(&mut write_session, name)?;
                }
            }
            touched_spws.insert(spw_id);
        }
    }
    maybe_log_transform_progress(
        "select_channels",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    let stage_started_at = Instant::now();
    for column in visibility.copies() {
        output_column_overrides.insert_deferred(column.output.name());
    }
    output_column_overrides.insert_deferred("FLAG");
    output_column_overrides.insert_deferred("FLAG_CATEGORY");
    output_column_overrides.insert_deferred("UVW");
    output_column_overrides.insert_deferred("WEIGHT");
    output_column_overrides.insert_deferred("SIGMA");
    for name in spectra.output {
        output_column_overrides.insert_deferred(name);
    }
    for (&input_row, ddid) in output_input_rows[..selected_output_rows]
        .iter()
        .zip(&output_input_ddids[..selected_output_rows])
    {
        let compact = regrid
            .as_ref()
            .and_then(|plan| plan.output_description(input_row))
            .or_else(|| compact_metadata.ddid_map.get(ddid).copied())
            .ok_or_else(|| MsTransformError::SpectralMetadata {
                path: request.output_ms.display().to_string(),
                reason: format!(
                    "selected DATA_DESC_ID {ddid} was not present in compacted metadata"
                ),
            })?;
        for part in 0..copies_per_row {
            let id = compact
                .checked_mul(copies_per_row as i32)
                .and_then(|id| id.checked_add(part as i32))
                .and_then(|id| id.checked_add(ddi_offset))
                .ok_or_else(|| {
                    MsTransformError::Spectrum(
                        "Output data-description ID exceeds the index limit.".into(),
                    )
                })?;
            write_session
                .push_scalar_row("DATA_DESC_ID", Some(ScalarValue::Int32(id)))
                .map_err(|error| MsTransformError::WriteMeasurementSet {
                    path: request.output_ms.display().to_string(),
                    reason: error.to_string(),
                })?;
        }
    }
    for field_id in &selected_field_ids_for_metadata[..selected_output_rows] {
        let compact = match &field_id_map {
            Some(ids) => ids.get(field_id).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: request.output_ms.display().to_string(),
                    reason: format!(
                        "selected FIELD_ID {field_id} was not present in compacted FIELD metadata"
                    ),
                }
            })?,
            None => *field_id,
        };
        for _ in 0..copies_per_row {
            write_session
                .push_scalar_row("FIELD_ID", Some(ScalarValue::Int32(compact)))
                .map_err(|error| MsTransformError::WriteMeasurementSet {
                    path: request.output_ms.display().to_string(),
                    reason: error.to_string(),
                })?;
        }
    }
    let output_options = measurement_set_table_options(&request.output_ms);
    let write_telemetry = if writes_rows {
        let output_bindings = measurement_set_main_table_bindings(&output_main);
        write_session
            .save_table_and_finish(
                &mut output_main,
                output_options,
                &output_bindings,
                &output_column_overrides,
            )
            .map_err(|error| MsTransformError::WriteMeasurementSet {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            })?
    } else {
        // Empty MAIN has no visibilities from which to define tiled hypercubes.
        output_main
            .save(output_options.with_data_manager(DataManagerKind::StandardStMan))
            .map_err(|error| MsTransformError::WriteMeasurementSet {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            })?;
        MeasurementSetWriteTelemetry::default()
    };
    maybe_log_transform_progress(
        "save_output_main",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    if let Some(regrid) = &regrid {
        regrid.write_metadata(request, &compact_metadata)?;
    } else if combined_grid.is_some() {
        // The combined grid was written with the compact spectral metadata.
    } else if splitting || average_metadata.is_some() || normalized_width(request.width) == 1 {
        spectral_metadata::write_channel_selection(
            &request.output_ms,
            &compact_metadata.spw_map,
            &channel_selection,
            average_metadata.as_ref(),
            splitting.then_some(&widths),
        )?;
    } else {
        update_spectral_window_metadata(&compact_metadata.channel_bins, &request.output_ms)?;
    }
    if ddi_offset > 0 {
        output_layout::pad_ddids(
            &request.output_ms,
            ddi_offset as usize,
            budget.available_bytes / budget.maximum_live_blocks,
        )?;
    }
    maybe_log_transform_progress(
        "update_spectral_window",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    tracing::info!(
        input_ms = %request.input_ms.display(),
        output_ms = %request.output_ms.display(),
        rows = output_rows,
        write_bytes = write_telemetry.bytes_written,
        write_peak_planned_resident_bytes = write_telemetry.maximum_resident_bytes,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "mstransform completed"
    );
    Ok(MsTransformReport {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        row_count: output_rows,
        source_column: visibility.source_names(),
        output_column: visibility.output_names(),
        spw: request.spw.clone(),
        spectral_window_ids: if combined_grid.is_some() {
            vec![0]
        } else {
            touched_spws.into_iter().collect()
        },
        output_channels_by_spw: if let Some(grid) = &combined_grid {
            BTreeMap::from([(0, grid.frequencies.len())])
        } else {
            channel_bins
                .iter()
                .map(|(spw, bins)| (*spw, bins.len()))
                .collect()
        },
        width: normalized_width(request.width),
        elapsed_ns: started_at.elapsed().as_nanos() as u64,
        retained_mms: None,
        write_telemetry,
    })
}

struct TransformSelection {
    selection: MsSelection,
    correlations: Option<CorrelationSelection>,
    time: Option<TimeSelection>,
    rows: Vec<ResolvedMsSelectionRow>,
    antenna_ids: Option<Vec<i32>>,
    tile_window: Option<i32>,
}

fn resolve_transform_selection(
    input: &MeasurementSet,
    request: &MsTransformRequest,
    grammar: RowGrammar,
    budget: MsSelectionIoBudget,
    auto_correlations: bool,
) -> Result<TransformSelection, MsTransformError> {
    let error = |reason: MsSelectionError| MsTransformError::SelectRows {
        path: request.input_ms.display().to_string(),
        source: match reason {
            MsSelectionError::Domain(source) => source,
            other => MsError::InvalidInput(other.to_string()),
        },
    };
    let mut selection = request.selection.clone();
    if auto_correlations {
        selection.antenna = Some(convolution::auto_correlations(
            input,
            selection.antenna.as_deref(),
        )?);
    }
    if selection.spw.is_none() {
        let spw = request.spw.trim();
        selection.spw = Some(if spw.is_empty() { "*" } else { spw }.into());
    }
    let correlations = if grammar == RowGrammar::Transform {
        selection
            .correlation
            .take()
            .filter(|text| !text.is_empty())
            .map(|text| CorrelationSelection::resolve(input, &text))
            .transpose()
            .map_err(|source| error(MsSelectionError::Domain(source)))?
    } else {
        None
    };
    if let Some(correlations) = correlations.as_ref()
        && !correlations
            .constrain(&mut selection)
            .map_err(|source| error(MsSelectionError::Domain(source)))?
    {
        return Err(MsTransformError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    let (resolved, time) = input
        .resolve_selection_with_time(&selection, budget, grammar)
        .map_err(error)?;
    let rows = resolved.selected_rows;
    let antenna_ids =
        selection_metadata::selected_antenna_ids(input, selection.antenna.as_deref(), &rows)?;
    Ok(TransformSelection {
        selection,
        correlations,
        time,
        rows,
        antenna_ids,
        tile_window: None,
    })
}

/// Log one completed transform stage (`debug`, target
/// `casa_ms::mstransform_progress`).
fn maybe_log_transform_progress(stage: &str, stage_elapsed: Duration, total_elapsed: Duration) {
    tracing::debug!(
        target: "casa_ms::mstransform_progress",
        "mstransform stage={} stage_elapsed_s={:.3} total_elapsed_s={:.3}",
        stage,
        stage_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64(),
    );
}

struct TransformArrayDimensions {
    input_correlations: usize,
    input_channels: usize,
    output_correlations: usize,
    output_channels: usize,
    output_parts: Vec<usize>,
}

fn transform_array_dimensions(
    input: &MeasurementSet,
    selected_ddids: &[i32],
    ddid_to_spw: &BTreeMap<i32, i32>,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    correlations: Option<&CorrelationSelection>,
    polarization_projection: Option<&PolarizationProjection>,
) -> Result<BTreeMap<i32, TransformArrayDimensions>, MsTransformError> {
    let input_path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let invalid = |reason| MsTransformError::SpectralMetadata {
        path: input_path.display().to_string(),
        reason,
    };
    let data_description = input
        .data_description()
        .map_err(|error| invalid(error.to_string()))?;
    let polarization = input
        .polarization()
        .map_err(|error| invalid(error.to_string()))?;
    let spectral_window = input
        .spectral_window()
        .map_err(|error| invalid(error.to_string()))?;
    let mut dimensions = BTreeMap::new();
    for &ddid in selected_ddids {
        let std::collections::btree_map::Entry::Vacant(entry) = dimensions.entry(ddid) else {
            continue;
        };
        let row =
            usize::try_from(ddid).map_err(|_| invalid(format!("negative DATA_DESC_ID {ddid}")))?;
        let pol_id = data_description
            .polarization_id(row)
            .map_err(|error| invalid(format!("read DATA_DESCRIPTION row {ddid}: {error}")))?;
        let pol_row = usize::try_from(pol_id).map_err(|_| {
            invalid(format!(
                "DATA_DESCRIPTION row {ddid} has negative POLARIZATION_ID {pol_id}"
            ))
        })?;
        let input_correlations = usize::try_from(
            polarization
                .num_corr(pol_row)
                .map_err(|error| invalid(format!("read POLARIZATION row {pol_id}: {error}")))?,
        )
        .map_err(|_| invalid(format!("POLARIZATION row {pol_id} has negative NUM_CORR")))?;
        let spw = ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
            invalid(format!(
                "DATA_DESC_ID {ddid} has no spectral-window mapping"
            ))
        })?;
        let output_channels = channel_bins
            .get(&spw)
            .map(Vec::len)
            .ok_or_else(|| invalid(format!("spectral window {spw} has no output channel bins")))?;
        let input_channels = usize::try_from(
            spectral_window
                .num_chan(spw as usize)
                .map_err(|error| invalid(format!("read SPECTRAL_WINDOW row {spw}: {error}")))?,
        )
        .map_err(|_| invalid(format!("SPECTRAL_WINDOW row {spw} has negative NUM_CHAN")))?;
        entry.insert(TransformArrayDimensions {
            input_correlations,
            input_channels,
            output_correlations: if polarization_projection.is_some_and(|plan| plan.contains(ddid))
            {
                1
            } else {
                correlations
                    .and_then(|plan| plan.axes(ddid))
                    .map_or(input_correlations, <[usize]>::len)
            },
            output_channels,
            output_parts: vec![output_channels],
        });
    }
    Ok(dimensions)
}

struct TransformArrayRows<'a> {
    selected_rows: &'a [usize],
    selected_ddids: &'a [i32],
    groups: Option<&'a [time_average::TimeGroup]>,
    output_rows: usize,
    output_schema: &'a Table,
    combined: Option<CombinedArrayRows<'a>>,
    regrid: Option<&'a regrid::Plan>,
    tile: Option<output_layout::OutputTiling>,
}

#[derive(Clone, Copy)]
struct CombinedArrayRows<'a> {
    grid: &'a combine::Grid,
    groups: &'a [combine::Group],
    source_rows: &'a [usize],
    source_ddids: &'a [i32],
    source_time_groups: Option<&'a [time_average::TimeGroup]>,
}

fn transform_array_write_plan(
    input: &MeasurementSet,
    TransformArrayRows {
        selected_rows,
        selected_ddids,
        groups,
        output_rows,
        output_schema,
        combined,
        regrid,
        tile,
    }: TransformArrayRows<'_>,
    ddid_to_spw: &BTreeMap<i32, i32>,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    (correlations, polarization): (
        Option<&CorrelationSelection>,
        Option<&PolarizationProjection>,
    ),
    (visibility, spectra, view_bytes): (&VisibilityColumns, &SpectrumProjection, usize),
    resources: MeasurementSetWriteResources,
) -> Result<(MeasurementSetWritePlan, usize), MsTransformError> {
    let input_path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let mut dimensions = transform_array_dimensions(
        input,
        combined.map_or(selected_ddids, |rows| rows.source_ddids),
        ddid_to_spw,
        channel_bins,
        correlations,
        polarization,
    )?;
    if let Some(CombinedArrayRows { grid, .. }) = combined {
        for dimension in dimensions.values_mut() {
            dimension.output_channels = grid.frequencies.len();
            dimension.output_parts = vec![dimension.output_channels];
        }
    }
    if let Some(regrid) = regrid {
        for (&ddid, dimension) in &mut dimensions {
            let spw = ddid_to_spw[&ddid];
            dimension.output_channels = regrid.channel_count(spw);
            dimension.output_parts = regrid
                .parts(spw)
                .ranges()
                .iter()
                .map(|part| part.len())
                .collect();
        }
    }
    let mut shape_counts = BTreeMap::<Vec<usize>, usize>::new();
    let mut weight_shape_counts = BTreeMap::<Vec<usize>, usize>::new();
    for (&row, &ddid) in selected_rows.iter().zip(selected_ddids) {
        let shape = &dimensions[&ddid];
        let field_channels = regrid.and_then(|plan| plan.field_channel_count(row));
        let parts = field_channels
            .as_ref()
            .map_or(&shape.output_parts[..], std::slice::from_ref);
        for &channels in parts {
            *shape_counts
                .entry(vec![shape.output_correlations, channels])
                .or_default() += 1;
            *weight_shape_counts
                .entry(vec![shape.output_correlations])
                .or_default() += 1;
        }
    }
    let shapes = shape_counts
        .into_iter()
        .map(|(cell_shape, row_count)| MeasurementSetArrayShapePlan {
            tile_shape: crate::ms::casa_visibility_tile_shape(cell_shape[0], cell_shape[1], ""),
            cell_shape,
            row_count,
        })
        .collect::<Vec<_>>();
    let weight_shapes = weight_shape_counts
        .into_iter()
        .map(|(cell_shape, row_count)| {
            let visibility_tile = crate::ms::casa_visibility_tile_shape(cell_shape[0], 1, "");
            MeasurementSetArrayShapePlan {
                tile_shape: crate::ms::casa_weight_tile_shape(&visibility_tile),
                cell_shape,
                row_count,
            }
        })
        .collect::<Vec<_>>();
    let metadata_budget = MsSelectionIoBudget {
        available_bytes: resources.available_bytes,
        maximum_live_blocks: resources.maximum_live_batches,
        requested_bytes_per_row: std::mem::size_of::<Option<Vec<usize>>>()
            + 5 * std::mem::size_of::<usize>(),
        storage_alignment_rows: None,
    };
    let (flag_category_shapes, largest_input_category) = plan_flag_category_shapes(
        input,
        selected_rows,
        selected_ddids,
        &dimensions,
        correlations.is_some() || polarization.is_some(),
        (groups, metadata_budget),
        (combined, regrid),
    )?;
    let io_error = |error: MsSelectionError| MsTransformError::WriteMeasurementSet {
        path: input_path.display().to_string(),
        reason: error.to_string(),
    };
    let (lag_shapes, maximum_lag_bytes) = if visibility.contains_output("LAG_DATA") {
        plan_lag_shapes(
            input,
            selected_rows,
            selected_ddids,
            &dimensions,
            (groups, metadata_budget),
        )?
    } else {
        (Vec::new(), 0)
    };
    let (source_count, source_time_groups) = combined
        .map_or((selected_rows.len(), groups), |rows| {
            (rows.source_ddids.len(), rows.source_time_groups)
        });
    let raw_row_count = source_time_groups.map_or(source_count, |groups| {
        groups.iter().map(|group| group.rows.len()).sum()
    });
    let input_batch_rows = transform_input_batch_rows(
        raw_row_count,
        resources,
        dimensions.values(),
        spectra.source.len(),
        visibility
            .regular_bytes_per_sample()
            .saturating_add(view_bytes),
        largest_input_category.saturating_add(maximum_lag_bytes),
    )
    .map_err(io_error)?;
    let visibility_tile = shapes
        .first()
        .map(|shape| shape.tile_shape.clone())
        .unwrap_or_else(|| crate::ms::casa_visibility_tile_shape(1, 1, ""));
    let mut columns = vec![
        MeasurementSetArrayColumnPlan {
            name: "FLAG".to_string(),
            value_type: StreamedTiledShapeValueType::Bool,
            shapes: shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "FLAG_CATEGORY".to_string(),
            value_type: StreamedTiledShapeValueType::Bool,
            shapes: flag_category_shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "UVW".to_string(),
            value_type: StreamedTiledShapeValueType::Float64,
            shapes: vec![MeasurementSetArrayShapePlan {
                cell_shape: vec![3],
                row_count: shapes.iter().map(|shape| shape.row_count).sum(),
                tile_shape: crate::ms::casa_uvw_tile_shape(&visibility_tile),
            }],
            storage_manager: MeasurementSetColumnStorage::TiledColumn,
        },
        MeasurementSetArrayColumnPlan {
            name: "WEIGHT".to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: weight_shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "SIGMA".to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: weight_shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
    ];
    for &name in &spectra.output {
        columns.push(MeasurementSetArrayColumnPlan {
            name: name.to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        });
    }
    let independent_lag = !regrid.is_some_and(regrid::Plan::is_legacy);
    for column in visibility.copies() {
        let shapes = if independent_lag && column.source == OptionalMainColumn::LagData {
            lag_shapes.clone()
        } else {
            shapes.clone()
        };
        columns.push(MeasurementSetArrayColumnPlan {
            name: column.output.name().to_string(),
            value_type: column.value_type(),
            shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        });
    }
    if output_rows == 0 {
        columns.clear();
    }
    if let Some(tile) = tile {
        tile.apply(&mut columns);
    }
    let writer = MeasurementSetWritePlan::variable_array_creation(
        output_rows,
        columns,
        transform_scalar_column_plans(output_schema),
        resources,
    )
    .map_err(|error| MsTransformError::WriteMeasurementSet {
        path: input_path.display().to_string(),
        reason: error.to_string(),
    })?;
    let writer = if let Some(tile) = tile {
        writer.with_array_default_tiles(|name| tile.default_for(name))
    } else {
        writer
    };
    Ok((writer, input_batch_rows))
}

const STANDARD_SPECTRA: [&str; 2] = ["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"];
const MAIN_SPECTRA: [&str; 3] = [
    "WEIGHT_SPECTRUM",
    "SIGMA_SPECTRUM",
    "CORRECTED_WEIGHT_SPECTRUM",
];

struct SpectrumProjection {
    source: Vec<&'static str>,
    output: Vec<&'static str>,
    propagate_weights: bool,
    fill_standard: bool,
}

impl SpectrumProjection {
    fn existing(table: &Table) -> Self {
        let source = MAIN_SPECTRA
            .into_iter()
            .filter(|name| {
                table
                    .schema()
                    .is_some_and(|schema| schema.contains_column(name))
            })
            .collect::<Vec<_>>();
        Self {
            output: source.clone(),
            source,
            propagate_weights: false,
            fill_standard: false,
        }
    }

    fn partition(
        table: &Table,
        selected_row: usize,
        averaging_time: bool,
    ) -> Result<Self, TableError> {
        let name = "WEIGHT_SPECTRUM";
        let present = table
            .schema()
            .is_some_and(|schema| schema.contains_column(name));
        let defined = present && table.array_shape_uncached(selected_row, name)?.is_some();
        Ok(Self {
            source: defined.then_some(name).into_iter().collect(),
            output: vec![name],
            propagate_weights: defined,
            fill_standard: defined && averaging_time,
        })
    }

    fn resolve(
        table: &Table,
        selected_row: usize,
        grammar: RowGrammar,
        force_spectra: bool,
        (channel_average, time_average, spectral_convolution, hanning): (bool, bool, bool, bool),
        visibility: &VisibilityColumns,
    ) -> Result<Self, TableError> {
        let exists = |name| {
            table
                .schema()
                .is_some_and(|schema| schema.contains_column(name))
        };
        if grammar != RowGrammar::Transform {
            return Ok(Self::existing(table));
        }
        let defined = |row, name| -> Result<bool, TableError> {
            Ok(exists(name) && table.array_shape_uncached(row, name)?.is_some())
        };
        let propagate_weights = defined(selected_row, "WEIGHT_SPECTRUM")?;
        let fill_standard = force_spectra || propagate_weights;
        // The selected MS can supply spectra even if row zero of the physical
        // input has no spectrum. Allocate columns for the selected values.
        let output = if fill_standard || defined(0, "WEIGHT_SPECTRUM")? {
            STANDARD_SPECTRA.to_vec()
        } else {
            Vec::new()
        };
        let mut source = Vec::new();
        let needs_weight = if time_average {
            let copies = visibility.copies();
            copies
                .iter()
                .any(|column| column.source == OptionalMainColumn::CorrectedData)
                || (copies.len() == 1 && copies[0].source == OptionalMainColumn::LagData)
        } else {
            channel_average || fill_standard || spectral_convolution || hanning
        };
        if needs_weight && exists("WEIGHT_SPECTRUM") {
            source.push("WEIGHT_SPECTRUM");
        }
        let needs_sigma = if channel_average || time_average || hanning {
            visibility.copies().iter().any(|column| {
                column.source == OptionalMainColumn::Data
                    || (time_average && column.source == OptionalMainColumn::FloatData)
            })
        } else {
            (fill_standard || spectral_convolution) && !visibility.corrected_becomes_data()
        };
        if needs_sigma && exists("SIGMA_SPECTRUM") {
            source.push("SIGMA_SPECTRUM");
        }
        Ok(Self {
            source,
            output,
            propagate_weights,
            fill_standard,
        })
    }
}

fn flag_filter_read_plan<'a>(
    rows: usize,
    mut budget: MsSelectionIoBudget,
    dimensions: impl Iterator<Item = &'a TransformArrayDimensions>,
) -> Result<usize, MsSelectionError> {
    // FLAG is read at its original shape, even when only a few selected
    // channels or correlations will appear in the product.
    let flag_bytes = dimensions
        .map(|shape| {
            shape
                .input_correlations
                .saturating_mul(shape.input_channels)
        })
        .max()
        .unwrap_or(0);
    budget.requested_bytes_per_row = flag_bytes.saturating_add(
        std::mem::size_of::<Option<ArrayValue>>() + std::mem::size_of::<Option<ScalarValue>>() + 32,
    );
    Ok(MsReadPlan::new(rows, budget)?.rows_per_block)
}

struct SelectedFlagAxes<'a> {
    channels: &'a BTreeMap<i32, TransformChannelSelection>,
    ddid_to_spw: &'a BTreeMap<i32, i32>,
    correlations: Option<&'a CorrelationSelection>,
}

impl SelectedFlagAxes<'_> {
    fn all_flagged(&self, flags: &ArrayValue, ddid: i32) -> Result<bool, TableError> {
        let ArrayValue::Bool(flags) = flags else {
            return Err(TableError::Schema("FLAG must hold Bool".into()));
        };
        let flags = flags
            .view()
            .into_dimensionality::<ndarray::Ix2>()
            .map_err(|_| {
                TableError::Schema("FLAG must have correlation and channel axes".into())
            })?;
        let channels = &self.channels[&self.ddid_to_spw[&ddid]].channels;
        let axes = self.correlations.and_then(|selection| selection.axes(ddid));
        if channels.iter().any(|&chan| chan >= flags.ncols())
            || axes.is_some_and(|axes| axes.iter().any(|&corr| corr >= flags.nrows()))
        {
            return Err(TableError::Schema(
                "selected axes exceed the FLAG shape".into(),
            ));
        }
        Ok((0..flags.nrows())
            .filter(|corr| axes.is_none_or(|axes| axes.contains(corr)))
            .all(|corr| channels.iter().all(|&chan| flags[[corr, chan]])))
    }
}

fn keep_unflagged_rows(
    input: &MeasurementSet,
    rows: &[usize],
    descriptions: &[i32],
    rows_per_block: usize,
    selection: Option<&SelectedFlagAxes<'_>>,
) -> Result<(Vec<usize>, Vec<i32>), MsTransformError> {
    let error = |source| MsTransformError::MutateMeasurementSet {
        path: input
            .path()
            .unwrap_or_else(|| Path::new("<memory>"))
            .display()
            .to_string(),
        source: Box::new(source),
    };
    let table = input.main_table();
    let flag_row = table.column_accessor("FLAG_ROW").map_err(error)?;
    let flag = table.column_accessor("FLAG").map_err(error)?;
    let mut kept_rows = Vec::with_capacity(rows.len());
    let mut kept_descriptions = Vec::with_capacity(rows.len());
    for (rows, descriptions) in rows
        .chunks(rows_per_block)
        .zip(descriptions.chunks(rows_per_block))
    {
        let flag_rows = flag_row.scalar_cells_owned_for_rows(rows).map_err(error)?;
        let flags = flag.array_cells_owned_uncached(rows).map_err(error)?;
        for (((&row, &ddid), flag_row), flags) in
            rows.iter().zip(descriptions).zip(flag_rows).zip(flags)
        {
            let flagged_row = scalar_bool(flag_row.as_ref(), "FLAG_ROW", row)?;
            let flagged_samples = match (selection, flags.as_ref()) {
                (Some(selection), Some(flags)) => {
                    selection.all_flagged(flags, ddid).map_err(error)?
                }
                _ => bool_array_all_true(flags.as_ref(), "FLAG", row)?,
            };
            if !flagged_row && !flagged_samples {
                kept_rows.push(row);
                kept_descriptions.push(ddid);
            }
        }
    }
    Ok((kept_rows, kept_descriptions))
}

fn transform_input_batch_rows<'a>(
    rows: usize,
    resources: MeasurementSetWriteResources,
    dimensions: impl Iterator<Item = &'a TransformArrayDimensions>,
    spectrum_columns: usize,
    visibility_bytes_per_sample: usize,
    extra_array_bytes: usize,
) -> Result<usize, MsSelectionError> {
    // The input arrays are loaded before slicing, so a small output cell
    // does not imply a small source block. Allow space for array headers too.
    let elements = dimensions
        .map(|shape| {
            shape.input_correlations.saturating_mul(
                shape
                    .input_channels
                    .saturating_mul(1 + visibility_bytes_per_sample + 4 * spectrum_columns)
                    .saturating_add(8),
            )
        })
        .max()
        .unwrap_or(0);
    let overhead = (10 + spectrum_columns) * (std::mem::size_of::<Option<ArrayValue>>() + 32);
    let bytes_per_row = elements
        .saturating_add(24 + overhead)
        .saturating_add(extra_array_bytes);
    let plan = MsReadPlan::new(
        rows,
        MsSelectionIoBudget {
            available_bytes: resources.available_bytes,
            maximum_live_blocks: resources.maximum_live_batches,
            requested_bytes_per_row: bytes_per_row,
            storage_alignment_rows: None,
        },
    )?;
    Ok(plan.rows_per_block)
}

fn plan_flag_category_shapes(
    input: &MeasurementSet,
    selected_rows: &[usize],
    selected_ddids: &[i32],
    dimensions: &BTreeMap<i32, TransformArrayDimensions>,
    correlation_subset: bool,
    (groups, budget): (Option<&[time_average::TimeGroup]>, MsSelectionIoBudget),
    (combined, regrid): (Option<CombinedArrayRows<'_>>, Option<&regrid::Plan>),
) -> Result<(Vec<MeasurementSetArrayShapePlan>, usize), MsTransformError> {
    let main = input.main_table();
    let default_categories = match main
        .column_keywords("FLAG_CATEGORY")
        .and_then(|keywords| keywords.get("CATEGORY"))
    {
        Some(Value::Array(categories)) => categories.len().max(1),
        _ => 1,
    };
    let mut counts = BTreeMap::new();
    let mut largest_input = 0;
    let shapes = if let Some(combined) = combined {
        let source_shapes = optional_array_shapes(
            input,
            combined.source_rows,
            combined.source_time_groups,
            "FLAG_CATEGORY",
            budget,
        )?;
        largest_input = source_shapes
            .iter()
            .flatten()
            .map(|shape| shape.iter().copied().fold(1usize, usize::saturating_mul))
            .max()
            .unwrap_or(0);
        combined
            .groups
            .iter()
            .map(|group| {
                group
                    .members
                    .iter()
                    .find_map(|&(_, pos)| source_shapes[pos].clone())
            })
            .collect()
    } else {
        optional_array_shapes(input, selected_rows, groups, "FLAG_CATEGORY", budget)?
    };
    for ((original, ddid), &row) in shapes.into_iter().zip(selected_ddids).zip(selected_rows) {
        let shape = &dimensions[ddid];
        let cell = match original.as_deref() {
            Some(&[corr, chan, categories]) if corr > 0 && chan > 0 && categories > 0 => {
                largest_input =
                    largest_input.max(corr.saturating_mul(chan).saturating_mul(categories));
                [
                    if correlation_subset {
                        shape.output_correlations
                    } else {
                        corr
                    },
                    if chan == 1 && combined.is_none() {
                        1
                    } else {
                        shape.output_channels
                    },
                    categories,
                ]
            }
            _ => [shape.output_correlations, 1, default_categories],
        };
        let field_channels = regrid.and_then(|plan| plan.field_channel_count(row));
        let parts = field_channels
            .as_ref()
            .map_or(&shape.output_parts[..], std::slice::from_ref);
        for &channels in parts {
            let cell = [cell[0], if cell[1] == 1 { 1 } else { channels }, cell[2]];
            *counts.entry(cell).or_default() += 1;
        }
    }
    let plans = counts
        .into_iter()
        .map(|([corr, chan, categories], row_count)| {
            let visibility_tile = crate::ms::casa_visibility_tile_shape(corr, chan, "");
            MeasurementSetArrayShapePlan {
                cell_shape: vec![corr, chan, categories],
                row_count,
                tile_shape: vec![
                    visibility_tile[0],
                    visibility_tile[1],
                    1,
                    visibility_tile[2],
                ],
            }
        })
        .collect();
    Ok((plans, largest_input))
}

fn plan_lag_shapes(
    input: &MeasurementSet,
    rows: &[usize],
    descriptions: &[i32],
    dimensions: &BTreeMap<i32, TransformArrayDimensions>,
    (groups, budget): (Option<&[time_average::TimeGroup]>, MsSelectionIoBudget),
) -> Result<(Vec<MeasurementSetArrayShapePlan>, usize), MsTransformError> {
    let path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let mut counts = BTreeMap::new();
    let mut max_bytes = 0;
    let shapes = optional_array_shapes(input, rows, groups, "LAG_DATA", budget)?;
    for ((&row, &description), shape) in rows.iter().zip(descriptions).zip(shapes) {
        let output_correlations = dimensions[&description].output_correlations;
        let lags = match shape.as_deref() {
            Some(&[stored_correlations, lags]) if lags > 0 => {
                max_bytes =
                    max_bytes.max(stored_correlations.saturating_mul(lags).saturating_mul(8));
                lags
            }
            None => 1, // An undefined cell needs no physical lag extent.
            _ => {
                return Err(MsTransformError::SpectralMetadata {
                    path: path.display().to_string(),
                    reason: format!("LAG_DATA row {row} has an invalid shape {shape:?}"),
                });
            }
        };
        *counts.entry([output_correlations, lags]).or_default() +=
            dimensions[&description].output_parts.len();
    }
    let plans = counts
        .into_iter()
        .map(
            |([correlations, lags], row_count)| MeasurementSetArrayShapePlan {
                cell_shape: vec![correlations, lags],
                row_count,
                tile_shape: crate::ms::casa_visibility_tile_shape(correlations, lags, ""),
            },
        )
        .collect();
    Ok((plans, max_bytes))
}

fn optional_array_shapes(
    input: &MeasurementSet,
    rows: &[usize],
    groups: Option<&[time_average::TimeGroup]>,
    column: &str,
    budget: MsSelectionIoBudget,
) -> Result<Vec<Option<Vec<usize>>>, MsTransformError> {
    let path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let table_error = |source| MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut members: Box<dyn Iterator<Item = (usize, usize)>> = match groups {
        Some(groups) => Box::new(groups.iter().enumerate().flat_map(|(index, group)| {
            group
                .rows
                .iter()
                .map(move |sample| (index, sample.physical))
        })),
        None => Box::new(rows.iter().copied().enumerate()),
    };
    let raw_rows = groups.map_or(rows.len(), |groups| {
        groups.iter().map(|group| group.rows.len()).sum()
    });
    let plan = MsReadPlan::new(raw_rows, budget).map_err(|error| {
        MsTransformError::WriteMeasurementSet {
            path: path.display().to_string(),
            reason: error.to_string(),
        }
    })?;
    let accessor = input
        .main_table()
        .column_accessor(column)
        .map_err(table_error)?;
    let mut shapes: Vec<Option<Vec<usize>>> = vec![None; rows.len()];
    loop {
        let block = members
            .by_ref()
            .take(plan.rows_per_block)
            .collect::<Vec<_>>();
        if block.is_empty() {
            return Ok(shapes);
        }
        let indices = block.iter().map(|&(_, row)| row).collect::<Vec<_>>();
        let projected = accessor
            .array_shapes_uncached(&indices)
            .or_else(|_| {
                indices
                    .iter()
                    .map(|&row| input.main_table().array_shape_uncached(row, column))
                    .collect()
            })
            .map_err(table_error)?;
        for ((index, row), found) in block.into_iter().zip(projected) {
            let Some(found) = found.filter(|shape| groups.is_none() || !shape.contains(&0)) else {
                continue;
            };
            if let Some(previous) = &shapes[index]
                && previous != &found
            {
                return Err(MsTransformError::SpectralMetadata {
                    path: path.display().to_string(),
                    reason: format!(
                        "{column} row {row} shape {found:?} differs from its time bin's shape {previous:?}"
                    ),
                });
            }
            shapes[index] = Some(found);
        }
    }
}

fn push_complex32_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Complex32(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Complex32"),
        });
    };
    if array.ndim() != 2 {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not rank-2"),
        });
    }
    let values = array_in_storage_order(array);
    session
        .push_complex32_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

fn push_bool_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Bool(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Bool"),
        });
    };
    let values = array_in_storage_order(array);
    session
        .push_bool_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

fn push_f64_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Float64(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Float64"),
        });
    };
    let values = array_in_storage_order(array);
    session
        .push_f64_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

fn push_f32_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Float32(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Float32"),
        });
    };
    let rank = array.ndim();
    if !matches!(rank, 1 | 2) {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} has unsupported rank {rank}"),
        });
    }
    let values = array_in_storage_order(array);
    session
        .push_f32_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

fn array_in_storage_order<T: Copy>(array: &ArrayD<T>) -> Cow<'_, [T]> {
    let storage_order = array.t();
    match storage_order.to_slice() {
        Some(values) => Cow::Borrowed(values),
        None => Cow::Owned(storage_order.iter().copied().collect()),
    }
}

fn materialize_empty_main_table(
    input_path: &Path,
    row_count: usize,
    output_path: &Path,
    visibility: &VisibilityColumns,
    grammar: RowGrammar,
    spectra: &SpectrumProjection,
    legacy_partition: bool,
) -> Result<Table, MsTransformError> {
    let mut table = Table::open_metadata_for_construction(TableOptions::new(input_path), row_count)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    if grammar == RowGrammar::Transform {
        table.keywords_mut().remove("SORTED_TABLE");
        table.keywords_mut().remove("SORT_COLUMNS");
        table.set_info(TableInfo {
            table_type: "Measurement Set".into(),
            sub_type: "UVFITS".into(),
            readme: vec![
                "This is a MeasurementSet Table holding measurements from a Telescope".into(),
                "This is a measurement set Table holding astronomical observations".into(),
            ],
        });
    }
    let renamed_keywords = visibility
        .copies()
        .iter()
        .find(|copy| {
            copy.source != copy.output
                && (grammar != RowGrammar::Transform
                    || copy.source == OptionalMainColumn::CorrectedData)
        })
        .and_then(|copy| table.column_keywords(copy.source.name()).cloned());
    let projected_columns = table
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| {
            let name = column.name();
            (legacy_partition
                && !crate::schema::main_table::REQUIRED_COLUMNS
                    .iter()
                    .any(|column| column.name == name)
                && !visibility.contains_output(name)
                && !spectra.output.contains(&name))
                || (is_visibility_column(name) && !visibility.contains_output(name))
                || (MAIN_SPECTRA.contains(&name) && !spectra.output.contains(&name))
        })
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    for column in projected_columns {
        table
            .remove_column_metadata_for_construction(&column)
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: output_path.display().to_string(),
                source: Box::new(source),
            })?;
    }
    for column in [
        OptionalMainColumn::WeightSpectrum,
        OptionalMainColumn::SigmaSpectrum,
    ] {
        if spectra.output.contains(&column.name())
            && !table
                .schema()
                .is_some_and(|schema| schema.contains_column(column.name()))
        {
            table
                .add_column(crate::column_def::column_schema(column.column_def()), None)
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: output_path.display().to_string(),
                    source: Box::new(source),
                })?;
        }
    }
    let mut arrays = vec![
        "FLAG",
        "FLAG_CATEGORY",
        "WEIGHT",
        "SIGMA",
        "WEIGHT_SPECTRUM",
        "SIGMA_SPECTRUM",
        "CORRECTED_WEIGHT_SPECTRUM",
    ];
    for copy in visibility.copies() {
        let name = copy.output.name();
        arrays.push(name);
        if !table
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
        {
            table
                .add_column(
                    crate::column_def::column_schema(copy.output.column_def()),
                    None,
                )
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: output_path.display().to_string(),
                    source: Box::new(source),
                })?;
        }
        if copy.source != copy.output
            && let Some(source) = &renamed_keywords
        {
            let mut keywords = table.column_keywords(name).cloned().unwrap_or_default();
            for field in source.fields() {
                if grammar != RowGrammar::Transform
                    || matches!(field.value, Value::Scalar(ScalarValue::String(_)))
                {
                    keywords.upsert(field.name.clone(), field.value.clone());
                }
            }
            table.set_column_keywords(name, keywords);
        } else if grammar == RowGrammar::Transform
            && copy.output == OptionalMainColumn::ModelData
            && visibility.copies().len() > 1
        {
            // CASA constructs the independent MODEL_DATA column without input keywords.
            table.set_column_keywords(name, RecordValue::default());
        }
    }
    table
        .widen_array_metadata_for_construction(&arrays)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(table)
}

fn gather_selected_main_column_overrides(
    (table, output): (&Table, &Table),
    selected_rows: &[usize],
    output_path: &Path,
    batch_rows: usize,
    write_session: &mut MeasurementSetWriteSession,
    row_id_maps: &selection_metadata::RowIdMaps,
    (averaging_time, averaging_polarization, combining): (bool, bool, bool),
) -> Result<ColumnOverrides, MsTransformError> {
    let schema = output
        .schema()
        .ok_or_else(|| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(TableError::Schema(
                "MAIN table is missing schema metadata".to_string(),
            )),
        })?;
    let mut columns = ColumnOverrides::for_row_count(selected_rows.len());
    for column in schema.columns() {
        let column_started_at = Instant::now();
        let name = column.name();
        if is_deferred_visibility_column(name) {
            continue;
        }
        if (averaging_time && TIME_OUTPUT_SCALARS.contains(&name))
            || (averaging_polarization && name == "FLAG_ROW")
            || (combining && matches!(name, "EXPOSURE" | "FLAG_ROW"))
        {
            continue;
        }
        match column.column_type() {
            ColumnType::Scalar => {
                if is_remapped_scalar_column(name) {
                    continue;
                }
                if column.data_type().and_then(streamed_scalar_type).is_some() {
                    let id_map = row_id_maps.for_column(name);
                    let array_selection = row_id_maps.array.as_ref().filter(|_| name == "ARRAY_ID");
                    for rows in selected_rows.chunks(batch_rows.max(1)) {
                        let values = table
                            .column_accessor(name)
                            .and_then(|column| column.scalar_cells_owned_for_rows(rows))
                            .map_err(|source| MsTransformError::MutateMeasurementSet {
                                path: output_path.display().to_string(),
                                source: Box::new(source),
                            })?;
                        for (row, value) in rows.iter().zip(values) {
                            let value = match (id_map, array_selection) {
                                (Some(ids), _) => {
                                    let old_id = scalar_i32(value.as_ref(), name, *row)?;
                                    let new_id = ids.get(&old_id).ok_or_else(|| {
                                        MsTransformError::SpectralMetadata {
                                            path: output_path.display().to_string(),
                                            reason: format!("{name} {old_id} is absent from the selected metadata"),
                                        }
                                    })?;
                                    Some(ScalarValue::Int32(*new_id))
                                }
                                (_, Some(selection)) => {
                                    let old_id = scalar_i32(value.as_ref(), name, *row)?;
                                    let new_id = selection.reindex(old_id).map_err(|source| {
                                        MsTransformError::SpectralMetadata {
                                            path: output_path.display().to_string(),
                                            reason: source.to_string(),
                                        }
                                    })?;
                                    Some(ScalarValue::Int32(new_id))
                                }
                                _ => value,
                            };
                            write_session
                                .push_scalar_row(name, value)
                                .map_err(|source| MsTransformError::WriteMeasurementSet {
                                    path: output_path.display().to_string(),
                                    reason: source.to_string(),
                                })?;
                        }
                    }
                } else {
                    let values = table
                        .column_accessor(name)
                        .and_then(|column| column.scalar_cells_owned_for_rows(selected_rows))
                        .map_err(|source| MsTransformError::MutateMeasurementSet {
                            path: output_path.display().to_string(),
                            source: Box::new(source),
                        })?;
                    columns.insert_values(
                        name.to_string(),
                        values
                            .into_iter()
                            .map(|value| value.map(Value::Scalar))
                            .collect(),
                    );
                }
            }
            ColumnType::Array(_) => {
                let values = table
                    .column_accessor(name)
                    .and_then(|column| column.array_cells_owned(selected_rows))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: output_path.display().to_string(),
                        source: Box::new(source),
                    })?;
                columns.insert_values(
                    name.to_string(),
                    values
                        .into_iter()
                        .map(|value| value.map(Value::Array))
                        .collect(),
                );
            }
            ColumnType::Record => {
                let values = selected_rows
                    .iter()
                    .copied()
                    .map(|input_row| {
                        table
                            .column_accessor(name)
                            .and_then(|column| column.record_cell(input_row))
                            .map(|value| Some(Value::Record(value)))
                            .map_err(|source| MsTransformError::MutateMeasurementSet {
                                path: output_path.display().to_string(),
                                source: Box::new(source),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                columns.insert_values(name.to_string(), values);
            }
        }
        maybe_log_transform_progress(
            &format!("load_main_column_overrides/column/{name}"),
            column_started_at.elapsed(),
            column_started_at.elapsed(),
        );
    }
    Ok(columns)
}

fn streamed_scalar_type(primitive_type: PrimitiveType) -> Option<StreamedScalarType> {
    match primitive_type {
        PrimitiveType::Bool => Some(StreamedScalarType::Bool),
        PrimitiveType::Int32 => Some(StreamedScalarType::Int32),
        PrimitiveType::Float32 => Some(StreamedScalarType::Float32),
        PrimitiveType::Float64 => Some(StreamedScalarType::Float64),
        _ => None,
    }
}

fn transform_scalar_column_plans(table: &Table) -> Vec<MeasurementSetScalarColumnPlan> {
    table
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| {
            *column.column_type() == ColumnType::Scalar
                && !is_deferred_visibility_column(column.name())
        })
        .filter_map(|column| {
            Some(MeasurementSetScalarColumnPlan {
                name: column.name().to_string(),
                value_type: streamed_scalar_type(column.data_type()?)?,
            })
        })
        .collect()
}

fn is_remapped_scalar_column(column: &str) -> bool {
    matches!(column, "DATA_DESC_ID" | "FIELD_ID")
}

fn is_deferred_visibility_column(column: &str) -> bool {
    is_visibility_column(column)
        || [
            "FLAG",
            "FLAG_CATEGORY",
            "UVW",
            "WEIGHT",
            "SIGMA",
            "WEIGHT_SPECTRUM",
            "SIGMA_SPECTRUM",
            "CORRECTED_WEIGHT_SPECTRUM",
        ]
        .contains(&column)
}

fn is_visibility_column(column: &str) -> bool {
    matches!(
        column,
        "DATA" | "CORRECTED_DATA" | "MODEL_DATA" | "FLOAT_DATA" | "LAG_DATA"
    )
}

pub(crate) fn open_staged_subtable_for_write(path: &Path) -> Result<Table, TableError> {
    let options = TableOptions::new(path);
    #[cfg(unix)]
    return Table::open_with_lock(
        options,
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait),
    );
    #[cfg(not(unix))]
    Table::open(options)
}

struct CompactSpectralMetadata {
    ddid_map: BTreeMap<i32, i32>,
    spw_map: BTreeMap<i32, i32>,
    channel_bins: BTreeMap<i32, Vec<Vec<usize>>>,
}

fn preserve_spectral_ids(
    input: &MeasurementSet,
    request: &MsTransformRequest,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    ddid_to_spw: &BTreeMap<i32, i32>,
) -> Result<CompactSpectralMetadata, MsTransformError> {
    let name = SubtableId::SpectralWindow.name();
    let windows = input
        .spectral_window()
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: request.input_ms.join(name).display().to_string(),
            reason: source.to_string(),
        })?;
    let source_path = windows
        .table()
        .path()
        .ok_or_else(|| MsTransformError::SpectralMetadata {
            path: request.input_ms.join(name).display().to_string(),
            reason: "resolved spectral window table has no persisted path".into(),
        })?;
    let rows = (0..windows.row_count()).collect::<Vec<_>>();
    rewrite_table_rows_with_variable_arrays(
        source_path,
        &request.output_ms.join(name),
        &rows,
        &["CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"],
        |_, row| Ok(row),
    )?;
    Ok(CompactSpectralMetadata {
        ddid_map: ddid_to_spw.keys().map(|&id| (id, id)).collect(),
        spw_map: channel_bins.keys().map(|&id| (id, id)).collect(),
        channel_bins: channel_bins.clone(),
    })
}

fn compact_spectral_subtables(
    input: &MeasurementSet,
    output_ms: &Path,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    ddid_to_spw: &BTreeMap<i32, i32>,
    selection: &SpwSelection,
    correlations: Option<&CorrelationSelection>,
    grammar: RowGrammar,
) -> Result<CompactSpectralMetadata, MsTransformError> {
    let selected_spws = if grammar == RowGrammar::Transform {
        selected_spw_order(channel_bins, selection)
    } else {
        channel_bins.keys().copied().collect::<Vec<_>>()
    };
    let spw_map = selected_spws
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| (*old_id, new_id as i32))
        .collect::<BTreeMap<_, _>>();
    let compact_channel_bins = selected_spws
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| {
            (
                new_id as i32,
                channel_bins.get(old_id).cloned().unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    rewrite_table_rows_with_variable_arrays(
        subtable_copy::input_subtable_path(input, SubtableId::SpectralWindow)?,
        &output_ms.join(SubtableId::SpectralWindow.name()),
        &selected_spws
            .iter()
            .map(|spw| *spw as usize)
            .collect::<Vec<_>>(),
        &["CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"],
        |_, row| Ok(spectral_metadata::remap_associations(row, &spw_map)),
    )?;

    // CASA uses getSPWDDIDList in expression order, including repeated
    // entries. MAIN refers to the last such entry. An omitted spw is '*'.
    let ordered_ddids = selection
        .data_description_ids()
        .iter()
        .map(|&id| id as i32)
        .filter(|&id| correlations.is_none_or(|selection| selection.axes(id).is_some()))
        .collect::<Vec<_>>();
    let ddid_map = ordered_ddids
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| (*old_id, new_id as i32))
        .collect::<BTreeMap<_, _>>();
    let descriptions = subtable_copy::input_subtable_path(input, SubtableId::DataDescription)?;
    rewrite_selected_table_rows(
        descriptions,
        &output_ms.join(SubtableId::DataDescription.name()),
        &ordered_ddids
            .iter()
            .map(|ddid| *ddid as usize)
            .collect::<Vec<_>>(),
        |old_row, mut row| {
            let old_ddid = old_row as i32;
            let old_spw = ddid_to_spw.get(&old_ddid).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: descriptions.display().to_string(),
                    reason: format!("DATA_DESCRIPTION row {old_ddid} has no SPW mapping"),
                }
            })?;
            let new_spw = spw_map.get(&old_spw).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: output_ms
                        .join(SubtableId::DataDescription.name())
                        .display()
                        .to_string(),
                    reason: format!(
                        "DATA_DESCRIPTION row {old_ddid} references unselected SPW {old_spw}"
                    ),
                }
            })?;
            row.upsert(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::Int32(new_spw)),
            );
            Ok(row)
        },
    )?;

    Ok(CompactSpectralMetadata {
        ddid_map,
        spw_map,
        channel_bins: compact_channel_bins,
    })
}

fn selected_field_ids_for_rows(
    table: &Table,
    selected_rows: &[usize],
    output_path: &Path,
) -> Result<Vec<i32>, MsTransformError> {
    table
        .column_accessor("FIELD_ID")
        .and_then(|column| column.scalar_cells_owned_for_rows(selected_rows))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?
        .into_iter()
        .zip(selected_rows.iter().copied())
        .map(|(value, row_index)| scalar_i32(value.as_ref(), "FIELD_ID", row_index))
        .collect()
}

fn rewrite_selected_table_rows<F>(
    source_path: &Path,
    destination_path: &Path,
    selected_rows: &[usize],
    rewrite: F,
) -> Result<(), MsTransformError>
where
    F: FnMut(usize, RecordValue) -> Result<RecordValue, MsTransformError>,
{
    rewrite_table_rows_with_variable_arrays(
        source_path,
        destination_path,
        selected_rows,
        &[],
        rewrite,
    )
}

fn rewrite_table_rows_with_variable_arrays<F>(
    source_path: &Path,
    destination_path: &Path,
    selected_rows: &[usize],
    variable_arrays: &[&str],
    mut rewrite: F,
) -> Result<(), MsTransformError>
where
    F: FnMut(usize, RecordValue) -> Result<RecordValue, MsTransformError>,
{
    let source = Table::open(TableOptions::new(source_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: source_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    if destination_path.exists() {
        fs::remove_dir_all(destination_path).map_err(|source| MsTransformError::PrepareOutput {
            path: destination_path.display().to_string(),
            reason: source.to_string(),
        })?;
    }
    source
        .shallow_copy(TableOptions::new(destination_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    let mut destination = Table::open(TableOptions::new(destination_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    destination
        .widen_array_metadata_for_construction(variable_arrays)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    let mut appender = destination.prepare_row_appender().map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for indices in selected_rows.chunks(256) {
        let records =
            selection_metadata::read_subtable_rows(&source, indices).map_err(|source| {
                MsTransformError::MutateMeasurementSet {
                    path: source_path.display().to_string(),
                    source: Box::new(source),
                }
            })?;
        for (&row_index, row) in indices.iter().zip(records) {
            appender
                .append(rewrite(row_index, row)?)
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: destination_path.display().to_string(),
                    source: Box::new(source),
                })?;
        }
    }
    drop(appender);
    destination
        .prepare_write()
        .save(TableOptions::new(destination_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}

#[derive(Clone)]
struct TransformChannelSelection {
    channels: Vec<usize>,
    metadata_ends: Vec<usize>,
    ranges: Vec<ChannelRange>,
}

impl TransformChannelSelection {
    fn metadata_for_width(&self, width: usize) -> Vec<spectral_metadata::ChannelWindow> {
        let width = width.min(self.channels.len());
        let mut windows = Vec::new();
        for range in &self.ranges {
            let start = range.start as usize;
            let stop = range.stop as usize;
            let span = (range.step.max(1) as usize).saturating_mul(width);
            for first in (start..=stop).step_by(span) {
                let nominal_last = first.saturating_add(span - 1);
                windows.push(spectral_metadata::ChannelWindow {
                    first,
                    last: nominal_last.min(stop),
                    dropped: nominal_last > stop,
                });
            }
        }
        windows
    }
}

fn resolve_transform_channels(
    ms: &MeasurementSet,
    selection: &SpwSelection,
    spw: &str,
) -> Result<BTreeMap<i32, TransformChannelSelection>, MsTransformError> {
    let spectral_window =
        ms.spectral_window()
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: source.to_string(),
            })?;
    let mut by_spw = BTreeMap::new();
    for row in 0..spectral_window.row_count() {
        if selection.rows_restricted() && !selection.spw_ids().contains(&row) {
            continue;
        }
        let num_chan =
            spectral_window
                .num_chan(row)
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                    reason: source.to_string(),
                })?;
        if num_chan < 0 {
            return Err(MsTransformError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: format!("spectral-window {row} has negative NUM_CHAN {num_chan}",),
            });
        }
        let num_chan = num_chan as usize;
        let mut ranges = selection
            .channel_list()
            .iter()
            .filter(|range| range.spw_id == row)
            .copied()
            .collect::<Vec<_>>();
        if ranges.is_empty() {
            ranges.push(ChannelRange {
                spw_id: row,
                start: 0,
                stop: num_chan as i32 - 1,
                step: 1,
            });
        }
        let mut channels = Vec::new();
        let mut metadata_ends = Vec::new();
        for range in &ranges {
            let indices = range
                .channels()
                .ok_or_else(|| MsTransformError::InvalidSpw {
                    selector: spw.to_string(),
                    reason: format!("spectral-window {row} range starts before its first channel"),
                })?;
            for channel in indices {
                channels.push(channel);
                metadata_ends
                    .push((channel + range.step.max(1) as usize - 1).min(range.stop as usize));
            }
        }
        if channels.is_empty() {
            return Err(MsTransformError::InvalidSpw {
                selector: spw.to_string(),
                reason: format!("spectral-window {row} selection produced no channels",),
            });
        }
        by_spw.insert(
            row as i32,
            TransformChannelSelection {
                channels,
                metadata_ends,
                ranges,
            },
        );
    }
    Ok(by_spw)
}

fn normalized_width(width: usize) -> usize {
    width.max(1)
}

fn selected_spw_order<T>(windows: &BTreeMap<i32, T>, selection: &SpwSelection) -> Vec<i32> {
    let mut seen = BTreeSet::new();
    let mut order = selection
        .channel_list()
        .iter()
        .map(|range| range.spw_id as i32)
        .filter(|spw| windows.contains_key(spw) && seen.insert(*spw))
        .collect::<Vec<_>>();
    order.extend(windows.keys().copied().filter(|spw| seen.insert(*spw)));
    order
}

fn channel_widths_by_spw(
    channels: &BTreeMap<i32, TransformChannelSelection>,
    spw_selection: &SpwSelection,
    scalar: usize,
    vector: Option<&[usize]>,
) -> Result<BTreeMap<i32, usize>, MsTransformError> {
    let simple = |width| {
        channels
            .keys()
            .map(|&spw| (spw, normalized_width(width)))
            .collect()
    };
    match vector {
        None => Ok(simple(scalar)),
        Some(&[width]) => Ok(simple(width)),
        Some(vector) => {
            let order = selected_spw_order(channels, spw_selection);
            if order.len() != vector.len() {
                return Err(MsTransformError::ChannelWidthCount);
            }
            Ok(order
                .into_iter()
                .zip(vector.iter().copied().map(normalized_width))
                .collect())
        }
    }
}

fn channel_bins_by_spw(
    channel_selection: &BTreeMap<i32, TransformChannelSelection>,
    widths: &BTreeMap<i32, usize>,
    average_metadata: Option<&BTreeMap<i32, Vec<spectral_metadata::ChannelWindow>>>,
) -> Result<BTreeMap<i32, Vec<Vec<usize>>>, MsTransformError> {
    let mut by_spw = BTreeMap::new();
    for (&spw_id, selection) in channel_selection {
        let width = widths[&spw_id].min(selection.channels.len()).max(1);
        let bins = if let Some(windows) = average_metadata {
            let count = windows[&spw_id]
                .iter()
                .filter(|window| !window.dropped)
                .count();
            if count == 0 {
                return Err(MsTransformError::NoCompleteChannelBins);
            }
            // CASA averages the selected visibility stripe in sequence. It
            // drops the trailing samples according to the output SPW shape.
            selection
                .channels
                .chunks_exact(width)
                .take(count)
                .map(<[usize]>::to_vec)
                .collect::<Vec<_>>()
        } else {
            selection
                .channels
                .chunks(width)
                .map(<[usize]>::to_vec)
                .collect::<Vec<_>>()
        };
        if bins.is_empty() {
            return Err(MsTransformError::InvalidSpw {
                selector: format!("{spw_id}"),
                reason: format!("spectral-window {spw_id} selection produced no channel bins"),
            });
        }
        by_spw.insert(spw_id, bins);
    }
    Ok(by_spw)
}

fn data_description_spw_map(ms: &MeasurementSet) -> Result<BTreeMap<i32, i32>, MsTransformError> {
    let data_description =
        ms.data_description()
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: "<measurement-set/DATA_DESCRIPTION>".to_string(),
                reason: source.to_string(),
            })?;
    let mut map = BTreeMap::new();
    for row in 0..data_description.row_count() {
        let spw = data_description.spectral_window_id(row).map_err(|source| {
            MsTransformError::SpectralMetadata {
                path: "<measurement-set/DATA_DESCRIPTION>".to_string(),
                reason: source.to_string(),
            }
        })?;
        map.insert(row as i32, spw);
    }
    Ok(map)
}

fn select_channels(value: ArrayValue, channels: &[usize]) -> Result<ArrayValue, TableError> {
    match value {
        ArrayValue::Complex32(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "visibility DATA arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Complex32(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Complex32(values));
                }
                return Ok(ArrayValue::Complex32(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Complex32(values.select(Axis(1), channels)))
        }
        ArrayValue::Bool(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "FLAG arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Bool(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Bool(values));
                }
                return Ok(ArrayValue::Bool(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Bool(values.select(Axis(1), channels)))
        }
        ArrayValue::Float32(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "WEIGHT_SPECTRUM arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Float32(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Float32(values));
                }
                return Ok(ArrayValue::Float32(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Float32(values.select(Axis(1), channels)))
        }
        other => Err(TableError::Schema(format!(
            "unsupported rank-2 channel selection for {:?}",
            other.primitive_type()
        ))),
    }
}

fn transform_data_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    match value {
        ArrayValue::Complex32(array) => {
            average_visibility(array, flags, bins).map(ArrayValue::Complex32)
        }
        ArrayValue::Float32(array) => {
            average_visibility(array, flags, bins).map(ArrayValue::Float32)
        }
        _ => Err(TableError::Schema(
            "visibility arrays must be Complex32 or Float32".to_string(),
        )),
    }
}

fn average_visibility<T>(
    values: ArrayD<T>,
    flags: &ArrayD<bool>,
    bins: &[Vec<usize>],
) -> Result<ArrayD<T>, TableError>
where
    T: Copy + Default + std::ops::AddAssign + std::ops::Div<f32, Output = T>,
{
    if values.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "DATA and FLAG arrays must have matching rank-2 [corr, chan] shapes".to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::from_elem((corr_count, bins.len()), T::default());
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            let mut sum = T::default();
            let mut count = 0usize;
            for &chan in bin {
                if !flags[[corr, chan]] {
                    sum += values[[corr, chan]];
                    count += 1;
                }
            }
            if count == 0 {
                for &chan in bin {
                    sum += values[[corr, chan]];
                }
                count = bin.len();
            }
            output[[corr, out_chan]] = sum / count as f32;
        }
    }
    Ok(output.into_dyn())
}

fn transform_flag_channels(
    value: ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Bool(values) = value else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 {
        return Err(TableError::Schema(
            "FLAG arrays must be rank-2 [corr, chan]".to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::<bool>::from_elem((corr_count, bins.len()), false);
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            output[[corr, out_chan]] = bin.iter().copied().all(|chan| values[[corr, chan]]);
        }
    }
    Ok(ArrayValue::Bool(output.into_dyn()))
}

fn transform_flag_category_channels(
    value: ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    let ArrayValue::Bool(values) = value else {
        return Err(TableError::Schema(
            "FLAG_CATEGORY arrays must be Bool rank-3 [corr, chan, category]".to_string(),
        ));
    };
    if values.ndim() != 3 {
        return Err(TableError::Schema(
            "FLAG_CATEGORY arrays must be rank-3 [corr, chan, category]".to_string(),
        ));
    }
    let correlation_count = values.shape()[0];
    let channel_count = values.shape()[1];
    let category_count = values.shape()[2];
    if channel_count == 1 {
        return Ok(ArrayValue::Bool(values));
    }
    validate_channel_bins(bins, channel_count)?;
    let mut output = ArrayD::from_elem(
        IxDyn(&[correlation_count, bins.len(), category_count]).f(),
        false,
    );
    for correlation in 0..correlation_count {
        for (output_channel, bin) in bins.iter().enumerate() {
            for category in 0..category_count {
                output[[correlation, output_channel, category]] = bin
                    .iter()
                    .copied()
                    .all(|channel| values[[correlation, channel, category]]);
            }
        }
    }
    Ok(ArrayValue::Bool(output))
}

fn transform_weight_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Float32(values) = value else {
        return Err(TableError::Schema(
            "WEIGHT_SPECTRUM arrays must be Float32 rank-2 [corr, chan]".to_string(),
        ));
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 || flags.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "WEIGHT_SPECTRUM and FLAG arrays must have matching rank-2 [corr, chan] shapes"
                .to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::<f32>::zeros((corr_count, bins.len()));
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            let mut sum = 0.0f32;
            for &chan in bin {
                if !flags[[corr, chan]] {
                    sum += values[[corr, chan]];
                }
            }
            if sum == 0.0 {
                for &chan in bin {
                    sum += values[[corr, chan]];
                }
            }
            output[[corr, out_chan]] = sum;
        }
    }
    Ok(ArrayValue::Float32(output.into_dyn()))
}

fn transform_sigma_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Float32(values) = value else {
        return Err(TableError::Schema(
            "SIGMA_SPECTRUM arrays must be Float32 rank-2 [corr, chan]".to_string(),
        ));
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 || flags.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "SIGMA_SPECTRUM and FLAG arrays must have matching rank-2 shapes".to_string(),
        ));
    }
    validate_channel_bins(bins, values.shape()[1])?;
    let mut output = ndarray::Array2::<f32>::zeros((values.shape()[0], bins.len()));
    for correlation in 0..values.shape()[0] {
        for (output_channel, bin) in bins.iter().enumerate() {
            let mut inverse_variance = 0.0_f64;
            for &channel in bin {
                let sigma = values[[correlation, channel]];
                if !flags[[correlation, channel]] && sigma.is_finite() && sigma > 0.0 {
                    inverse_variance += 1.0 / f64::from(sigma).powi(2);
                }
            }
            output[[correlation, output_channel]] = if inverse_variance > 0.0 {
                (1.0 / inverse_variance.sqrt()) as f32
            } else {
                0.0
            };
        }
    }
    Ok(ArrayValue::Float32(output.into_dyn()))
}

fn bins_are_single_channel(bins: &[Vec<usize>]) -> bool {
    bins.iter().all(|bin| bin.len() == 1)
}

fn validate_channel_bins(bins: &[Vec<usize>], channel_count: usize) -> Result<(), TableError> {
    if bins.is_empty() {
        return Err(TableError::Schema(
            "channel averaging produced no output channels".to_string(),
        ));
    }
    for bin in bins {
        if bin.is_empty() {
            return Err(TableError::Schema(
                "channel averaging bin was empty".to_string(),
            ));
        }
        for &channel in bin {
            if channel >= channel_count {
                return Err(TableError::Schema(format!(
                    "channel {channel} is outside array with {channel_count} channels"
                )));
            }
        }
    }
    Ok(())
}

fn select_fortran_contiguous_axis1<T: Clone>(
    values: ndarray::ArrayViewD<'_, T>,
    start: usize,
    end: usize,
) -> Result<Option<ArrayD<T>>, TableError> {
    let shape = values.shape();
    if shape.len() != 2 || start > end || end > shape[1] {
        return Err(TableError::Schema(format!(
            "channel range {start}..{end} is outside array shape {shape:?}"
        )));
    }
    let corr_count = shape[0];
    if (corr_count > 1 && values.strides()[0] != 1)
        || (shape[1] > 1 && values.strides()[1] != corr_count as isize)
    {
        return Ok(None);
    }
    let Some(input) = values.as_slice_memory_order() else {
        return Ok(None);
    };
    let output_shape = [corr_count, end - start];
    let start_offset = start * corr_count;
    let end_offset = end * corr_count;
    ArrayD::from_shape_vec(
        IxDyn(&output_shape).f(),
        input[start_offset..end_offset].to_vec(),
    )
    .map(Some)
    .map_err(|error| TableError::Schema(format!("channel slice shape mismatch: {error}")))
}

fn contiguous_channel_range(channels: &[usize]) -> Option<(usize, usize)> {
    let (&start, rest) = channels.split_first()?;
    for (offset, &channel) in rest.iter().enumerate() {
        if channel != start + offset + 1 {
            return None;
        }
    }
    Some((start, start + channels.len()))
}

fn all_channels_selected(channels: &[usize], channel_count: usize) -> bool {
    channels.len() == channel_count && channels.iter().copied().enumerate().all(|(i, ch)| ch == i)
}

fn update_spectral_window_metadata(
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    output_ms: &Path,
) -> Result<(), MsTransformError> {
    let spw_path = output_ms.join(SubtableId::SpectralWindow.name());
    let mut spectral_window = Table::open(TableOptions::new(&spw_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for (&spw_id, bins) in channel_bins {
        let row = spw_id as usize;
        update_f64_frequency_column(&mut spectral_window, row, "CHAN_FREQ", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "CHAN_WIDTH", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "EFFECTIVE_BW", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "RESOLUTION", bins, &spw_path)?;
        spectral_window
            .column_accessor_mut("NUM_CHAN")
            .and_then(|mut column| {
                column.set(row, Value::Scalar(ScalarValue::Int32(bins.len() as i32)))
            })
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: spw_path.display().to_string(),
                reason: source.to_string(),
            })?;
        let chan_freq = f64_vector_cell(&spectral_window, row, "CHAN_FREQ", &spw_path)?;
        if let (Some(first), Some(last)) = (chan_freq.first(), chan_freq.last()) {
            spectral_window
                .column_accessor_mut("REF_FREQUENCY")
                .and_then(|mut column| column.set(row, Value::Scalar(ScalarValue::Float64(*first))))
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
            let total_bw = f64_vector_cell(&spectral_window, row, "CHAN_WIDTH", &spw_path)
                .map(|widths| widths.iter().map(|width| width.abs()).sum())
                .unwrap_or_else(|_| {
                    if chan_freq.len() > 1 {
                        (last - first).abs()
                    } else {
                        0.0
                    }
                });
            spectral_window
                .column_accessor_mut("TOTAL_BANDWIDTH")
                .and_then(|mut column| {
                    column.set(row, Value::Scalar(ScalarValue::Float64(total_bw)))
                })
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
        }
    }
    spectral_window
        .prepare_write()
        .save(TableOptions::new(&spw_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}

fn update_f64_frequency_column(
    spectral_window: &mut Table,
    row: usize,
    column: &str,
    bins: &[Vec<usize>],
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    let values = spectral_window
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = values else {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    validate_bins_for_vector(bins, values.len(), column, spw_path)?;
    let averaged = bins
        .iter()
        .map(|bin| {
            bin.iter()
                .copied()
                .map(|channel| values[channel])
                .sum::<f64>()
                / bin.len() as f64
        })
        .collect::<Vec<_>>();
    spectral_window
        .column_accessor_mut(column)
        .and_then(|mut column| {
            column.set(
                row,
                Value::Array(ArrayValue::Float64(
                    ndarray::Array1::from(averaged).into_dyn(),
                )),
            )
        })
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: source.to_string(),
        })
}

fn update_f64_width_column(
    spectral_window: &mut Table,
    row: usize,
    column: &str,
    bins: &[Vec<usize>],
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    let values = spectral_window
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = values else {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    validate_bins_for_vector(bins, values.len(), column, spw_path)?;
    let averaged = bins
        .iter()
        .map(|bin| {
            bin.iter()
                .copied()
                .map(|channel| values[channel])
                .sum::<f64>()
        })
        .collect::<Vec<_>>();
    spectral_window
        .column_accessor_mut(column)
        .and_then(|mut column| {
            column.set(
                row,
                Value::Array(ArrayValue::Float64(
                    ndarray::Array1::from(averaged).into_dyn(),
                )),
            )
        })
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: source.to_string(),
        })
}

fn validate_bins_for_vector(
    bins: &[Vec<usize>],
    len: usize,
    column: &str,
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    for bin in bins {
        for &channel in bin {
            if channel >= len {
                return Err(MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: format!("{column} channel {channel} is outside vector length {len}"),
                });
            }
        }
    }
    Ok(())
}

fn f64_vector_cell(
    table: &Table,
    row: usize,
    column: &str,
    path: &Path,
) -> Result<Vec<f64>, MsTransformError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = value else {
        return Err(MsTransformError::SpectralMetadata {
            path: path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    Ok(values.iter().copied().collect())
}

fn scalar_i32(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<i32, MsTransformError> {
    match value {
        Some(ScalarValue::Int32(value)) => Ok(*value),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn scalar_bool(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<bool, MsTransformError> {
    match value {
        Some(ScalarValue::Bool(value)) => Ok(*value),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn bool_array_all_true(
    value: Option<&ArrayValue>,
    column: &str,
    row_index: usize,
) -> Result<bool, MsTransformError> {
    match value {
        Some(ArrayValue::Bool(values)) => Ok(values.iter().all(|value| *value)),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn scalar_f64(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<f64, MsTransformError> {
    match value {
        Some(ScalarValue::Float64(value)) => Ok(*value),
        Some(ScalarValue::Float32(value)) => Ok(f64::from(*value)),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn missing_column_error(path: &Path, row_index: usize, column: &str) -> MsTransformError {
    MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(TableError::ColumnNotFound {
            row_index,
            column: column.to_string(),
        }),
    }
}

fn prepare_output_root(path: &Path) -> Result<(), MsTransformError> {
    if path.exists() {
        fs::remove_dir_all(path)
            .or_else(|_| fs::remove_file(path))
            .map_err(|error| MsTransformError::PrepareOutput {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| MsTransformError::PrepareOutput {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::Complex32;

    #[test]
    fn select_channels_covers_contiguous_noncontiguous_and_full_width_paths() {
        let complex = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 4]).f(),
                (0..8)
                    .map(|value| Complex32::new(value as f32, value as f32 + 0.5))
                    .collect(),
            )
            .unwrap(),
        );
        let selected = select_channels(complex, &[1, 2]).expect("contiguous selection");
        let ArrayValue::Complex32(selected) = selected else {
            panic!("expected Complex32");
        };
        assert_eq!(selected.shape(), &[2, 2]);
        assert_eq!(selected[[0, 0]], Complex32::new(2.0, 2.5));
        assert_eq!(selected[[1, 1]], Complex32::new(5.0, 5.5));

        let flags = ArrayValue::Bool(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 4]).f(),
                vec![false, true, false, true, true, false, true, false],
            )
            .unwrap(),
        );
        let selected = select_channels(flags, &[0, 3]).expect("non-contiguous selection");
        let ArrayValue::Bool(selected) = selected else {
            panic!("expected Bool");
        };
        assert_eq!(selected.shape(), &[2, 2]);
        assert!(!selected[[0, 0]]);
        assert!(!selected[[1, 1]]);

        let weights = ArrayValue::Float32(
            ArrayD::from_shape_vec(IxDyn(&[1, 3]).f(), vec![1.0, 2.0, 3.0]).unwrap(),
        );
        let selected = select_channels(weights, &[0, 1, 2]).expect("full-width selection");
        let ArrayValue::Float32(selected) = selected else {
            panic!("expected Float32");
        };
        assert_eq!(selected.shape(), &[1, 3]);
        assert_eq!(selected[[0, 2]], 3.0);
    }

    #[test]
    fn select_channels_rejects_wrong_rank_and_unsupported_arrays() {
        let wrong_rank =
            ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[2, 2, 1]), vec![false; 4]).unwrap());
        let err = select_channels(wrong_rank, &[0]).unwrap_err();
        assert!(err.to_string().contains("rank-2"));

        let unsupported =
            ArrayValue::Float64(ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.0; 4]).unwrap());
        let err = select_channels(unsupported, &[0]).unwrap_err();
        assert!(err.to_string().contains("unsupported rank-2"));
    }

    #[test]
    fn scalar_helpers_accept_expected_numeric_types_and_reject_missing_cells() {
        assert_eq!(
            scalar_i32(Some(&ScalarValue::Int32(7)), "DATA_DESC_ID", 3).unwrap(),
            7
        );
        assert_eq!(
            scalar_f64(Some(&ScalarValue::Float64(1.25)), "TIME", 3).unwrap(),
            1.25
        );
        assert_eq!(
            scalar_f64(Some(&ScalarValue::Float32(2.5)), "TIME", 3).unwrap(),
            2.5
        );
        assert!(scalar_i32(None, "DATA_DESC_ID", 3).is_err());
        assert!(scalar_f64(Some(&ScalarValue::Bool(false)), "TIME", 3).is_err());
    }

    #[test]
    fn channel_range_helpers_identify_full_and_contiguous_selections() {
        assert_eq!(contiguous_channel_range(&[2, 3, 4]), Some((2, 5)));
        assert_eq!(contiguous_channel_range(&[2, 4]), None);
        assert!(all_channels_selected(&[0, 1, 2], 3));
        assert!(!all_channels_selected(&[0, 2], 3));
    }
}

#[cfg(test)]
mod array_budget_tests {
    use super::*;

    #[test]
    fn keepflags_batches_full_flags_even_for_a_small_selection() {
        let dimensions = [TransformArrayDimensions {
            input_correlations: 4,
            input_channels: 4096,
            output_correlations: 1,
            output_channels: 1,
            output_parts: vec![1],
        }];
        let budget = MsSelectionIoBudget {
            available_bytes: 128 * 1024,
            maximum_live_blocks: 2,
            requested_bytes_per_row: 1,
            storage_alignment_rows: None,
        };
        let rows = flag_filter_read_plan(100_000, budget, dimensions.iter()).unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn transform_batch_budget_counts_unsliced_input_arrays() {
        let dimensions = [TransformArrayDimensions {
            input_correlations: 4,
            input_channels: 4096,
            output_correlations: 1,
            output_channels: 4096,
            output_parts: vec![4096],
        }];
        let resources = MeasurementSetWriteResources {
            available_bytes: 1_200_000,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        };
        let rows = transform_input_batch_rows(100, resources, dimensions.iter(), 2, 8, 32_768)
            .expect("a full input row fits in the read budget");
        assert_eq!(rows, 1);
    }
}
