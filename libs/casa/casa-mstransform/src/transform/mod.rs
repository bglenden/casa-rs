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

use casa_core::cancellation::Cancellation;
use casa_core::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use casa_tables::streamed_tiled::StreamedTiledShapeValueType;
use casa_tables::{
    ColumnOverrides, ColumnType, DataManagerKind, StreamedScalarType, Table, TableError, TableInfo,
    TableOptions,
};
use ndarray::{ArrayD, Axis, IxDyn, ShapeBuilder, Slice};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use casa_ms::MeasurementSet;
use casa_ms::MsError;
use casa_ms::observation_io::creation::Stage;
use casa_ms::schema::SubtableId;
use casa_ms::schema::main_table::OptionalMainColumn;
use casa_ms::selection::{
    ChannelRange, MsReadPlan, MsSelection, MsSelectionError, MsSelectionIoBudget,
    ResolvedMsSelectionRow, RowGrammar, SpwSelection, TimeSelection,
};
use casa_ms::subtables::SubTable;
use casa_ms::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetScalarColumnPlan, MeasurementSetWritePlan, MeasurementSetWriteResources,
    MeasurementSetWriteSession, MeasurementSetWriteTelemetry,
};
use casa_ms::{measurement_set_main_table_bindings, measurement_set_table_options};

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

mod array_io;
mod channel_transforms;
mod main_output;
use casa_ms::open_staged_subtable_for_write;
mod staged;
#[cfg(feature = "transform-test-hooks")]
pub mod test_hooks;

use array_io::*;
use channel_transforms::*;
use main_output::*;
use staged::*;

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
    /// Stop at a safe boundary when cancellation is requested. An interrupted
    /// transform discards its staged output before returning.
    pub cancel: Option<Cancellation>,
    /// Explicit visibility columns for callers that accept arbitrary combinations.
    /// Ordinary task surfaces select a predefined combination on the request.
    pub visibility_columns: Option<Vec<OptionalMainColumn>>,
    /// Match the legacy Python MeasurementSet tool's output conventions.
    pub legacy_tool: Option<LegacyMsTool>,
    /// The `contsub` fit's output spectral windows. The request's union still
    /// determines subtable IDs, and its correlation selector filters rows.
    pub legacy_continuum_output: Option<String>,
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

impl MsTransformTaskOptions {
    fn check_cancelled(&self) -> Result<(), MsTransformError> {
        if self.cancel.as_ref().is_some_and(Cancellation::is_cancelled) {
            return Err(MsTransformError::Interrupted);
        }
        Ok(())
    }
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
    /// The caller cancelled the transform before the final output was published.
    #[error("MeasurementSet transform interrupted")]
    Interrupted,
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
    if request.output_ms.symlink_metadata().is_ok() {
        return Err(MsTransformError::OutputExists {
            path: request.output_ms.display().to_string(),
        });
    }
    let flagversions = casa_ms::flagversions_dir(&request.output_ms);
    if flagversions.exists() {
        return Err(MsTransformError::OutputFlagVersionsExist {
            path: flagversions.display().to_string(),
        });
    }
    options.check_cancelled()?;
    let output = &request.output_ms;
    if let Some(parent) = output.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(|error| output_error(output, error))?;
    }
    let stage = Stage::reserve(output, "casa-rs-incomplete")
        .map_err(|error| output_error(output, error))?;
    let result = stage_then_publish_transform(request, grammar, options, &stage);
    stage.remove();
    result
}

fn output_error(output: &Path, error: impl ToString) -> MsTransformError {
    MsTransformError::PrepareOutput {
        path: output.display().to_string(),
        reason: error.to_string(),
    }
}

fn stage_then_publish_transform(
    request: &MsTransformRequest,
    grammar: RowGrammar,
    options: &MsTransformTaskOptions,
    stage: &Stage,
) -> Result<MsTransformReport, MsTransformError> {
    let mut physical_request = request.clone();
    physical_request.output_ms = stage.path().to_path_buf();
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
            publish_transform_stage(request, options, stage)?;
        } else {
            options.check_cancelled()?;
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
                options.check_cancelled()?;
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
    publish_transform_stage(request, options, stage)?;
    report.output_ms = request.output_ms.clone();
    Ok(report)
}

fn publish_transform_stage(
    request: &MsTransformRequest,
    options: &MsTransformTaskOptions,
    stage: &Stage,
) -> Result<(), MsTransformError> {
    #[cfg(feature = "transform-test-hooks")]
    test_hooks::notify(test_hooks::Checkpoint::BeforePublication);
    options.check_cancelled()?;
    stage
        .publish_new(&request.output_ms)
        .map_err(|error| output_error(&request.output_ms, error))
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
    options.check_cancelled()?;
    let report = mstransform_staged(
        request,
        grammar,
        options,
        metadata_template,
        operation,
        partition,
    )?;
    options.check_cancelled()?;
    if let Some(history) = &options.history {
        task_history::append_task_history(&request.output_ms, history)?;
    }
    Ok(report)
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

#[cfg(test)]
mod tests {
    use super::*;
    use casa_core::Complex32;

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
