// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Typed MeasurementSet (MS) API for radio interferometry data.
//!
//! This crate provides idiomatic Rust APIs for reading and writing
//! MeasurementSet files — the primary data format for radio interferometry.
//! It builds on [`casa_tables`] for table I/O and [`casa_core`] for
//! value types, measures, and quanta.
//!
//! The on-disk layout is intended to remain interoperable with C++ casacore:
//! subtable links are emitted as real `TpTable` keywords using casacore's
//! relative-path conventions, and required unit / measure metadata is attached
//! to the standard MS columns.
//!
//! # Architecture
//!
//! The C++ MeasurementSet uses template inheritance with enum metadata maps.
//! This Rust design replaces that with:
//!
//! - **`const` arrays of [`ColumnDef`](column_def::ColumnDef)** for schema metadata
//! - **[`SubtableId`]** enum for the 17 subtables
//! - **Typed wrapper structs** for the standard subtables, holding `&Table`
//!   or `&mut Table` references
//!
//! # Schema modules
//!
//! The [`schema`] module and its submodules define all column and keyword
//! metadata as compile-time constants. Each subtable module exports
//! `REQUIRED_COLUMNS` and `OPTIONAL_COLUMNS` arrays that exactly match
//! the C++ `MS*Enums.h` headers.
//!
//! # Example
//!
//! ```rust
//! use casa_ms::schema::{SubtableId, required_columns};
//! use casa_ms::column_def::build_table_schema;
//!
//! // Build a TableSchema for the ANTENNA subtable
//! let schema = build_table_schema(required_columns(SubtableId::Antenna))
//!     .expect("valid schema");
//! assert_eq!(schema.columns().len(), 8);
//! ```

pub mod asdm_table;
pub mod builder;
pub mod column_def;
pub mod columns;
#[cfg(unix)]
pub mod concat;
pub mod derived;
mod ephemeris;
pub mod error;
pub mod flagging;
pub mod grouping;
pub mod listobs;
mod metadata;
pub mod ms;
pub mod observation_io;
mod probes;
pub mod schema;
pub mod scratch_columns;
pub mod selected_pointing;
pub mod selection;
pub mod shadowing;
pub mod single_dish_import;
pub mod spectral_selection;
mod subtable_overrides;
mod subtable_rows;
pub mod subtables;
pub mod tool_concatenate;
pub mod tool_ephemeris;
pub mod tool_history;
pub mod tool_iteration;
pub mod tool_lister;
pub mod tool_multims;
pub mod tool_range;
pub mod tool_scan_summary;
pub mod tool_sort;
pub mod tool_statistics;
pub mod tool_visibility;
pub mod transform;
pub mod validate;
pub mod virtual_model;
pub mod visibility_buffer;
pub mod voltage_pattern;
pub mod write_session;

#[cfg(test)]
pub(crate) mod test_helpers;

pub use builder::{MeasurementSetBuilder, MsSchemas};
pub use casa_measures::PlanetaryEphemeris;
pub use ephemeris::{EphemerisBudget, PhaseCentreEphemeris, PhaseCentreEphemerisError};
pub use error::{MsError, MsResult};
pub use flagging::calculate_flagdata_path;
pub use flagging::{
    FlagCounts, FlagDataAction, FlagDataColumn, FlagDataMode, FlagDataReport, FlagDataRequest,
    FlagMerge, FlagSummaryBreakdown, FlagSummaryOptions, FlagVersionEntry, FlaggingError,
    QuackMode, append_flag_command, append_flag_commands, delete_flag_version,
    flag_version_ms_path, flagdata, flagdata_ms_program, flagdata_path, initialize_flag_versions,
    list_flag_versions, rename_flag_version, rename_flag_version_for_task, restore_flag_version,
    save_flag_version, save_next_casa_flag_backup, save_next_flag_backup,
    save_next_partition_flag_backup,
};
pub use listobs::{
    ListObsOptions as MeasurementSetSummaryOptions,
    ListObsOutputFormat as MeasurementSetSummaryOutputFormat,
    ListObsSummary as MeasurementSetSummary, ListObsUvCoverage as MeasurementSetUvCoverage,
    ListObsUvPoint as MeasurementSetUvPoint, ListObsUvTrack as MeasurementSetUvTrack,
    ScanSummary as MeasurementSetScanSummary,
};
pub use listobs::{ListObsOptions, ListObsSummary, ListObsUvCoverage};
pub use ms::MeasurementSet;
pub use probes::{
    MeasurementSetContext, MeasurementSetNamedContext, MeasurementSetObservationContext,
    MeasurementSetSpectralWindowContext, MeasurementSetSubtableContext, MeasurementSetTimeRange,
    MeasurementSetUvRange,
};
pub use schema::SubtableId;
pub use schema::main_table::{OptionalMainColumn, VisibilityDataColumn, initial_model_visibility};
pub use selected_pointing::{
    PointingDirectionColumn, PointingReadPlan, SelectedPointingQueryDomain,
};
pub use selection::syntax::{
    ChannelSelection, ChannelSelectionSegment, IntentSelector, SpwSelector, parse_intent_selector,
    parse_numeric_id_selector, parse_numeric_row_id_ranges, parse_spw_selector,
};
pub use selection::{
    AntennaRow, BaselineSelection, ChannelFrequencyRange, ChannelRange, DataDescriptionRow,
    FieldSelectorRow, LogicalRowFilter, MsReadPlan, MsSelection, MsSelectionError,
    MsSelectionIoBudget, MsSelectionRowBlock, MsSelectorEditContext, MsSelectorKind,
    ResolvedMsSelection, ResolvedMsSelectionRow, SpectralWindowRow, SpwSelection, TimeCondition,
    TimeDefaults, TimeListEntry, TimeSelection, TimeSelector, TimeSelectorDefaults, TimeTerm,
    UvDistanceInterval, UvDistanceLimit, msselection_first_logical_row,
    parse_uv_distance_intervals, resolve_antenna_selector, resolve_field_selector_rows,
    resolve_spw_selector, resolve_time_selector, validate_ms_selector_edit,
};
pub use spectral_selection::{
    CubeAxisConfig, CubeAxisValue, CubeChannelContribution, CubeInterpolation, CubeSpecMode,
    CubeSpectralSetup, ResolvedChannelSelection, casa_selected_band_hz, combine_spectral_windows,
    convert_frequency_to_frame, convert_frequency_to_frame_with_frame, parse_rest_frequency_hz,
    resolve_channel_selector_selection, resolve_contiguous_channel_selection,
    velocity_ms_from_frequency_hz,
};
pub use subtable_overrides::SubtableColumnOverrides;
pub use subtables::{
    MsAntenna, MsAntennaMut, MsDataDescription, MsDataDescriptionMut, MsDoppler, MsDopplerMut,
    MsFeed, MsFeedMut, MsField, MsFieldMut, MsFlagCmd, MsFlagCmdMut, MsFreqOffset, MsFreqOffsetMut,
    MsHistory, MsHistoryMut, MsObservation, MsObservationMut, MsPointing, MsPointingMut,
    MsPolarization, MsPolarizationMut, MsProcessor, MsProcessorMut, MsSource, MsSourceMut,
    MsSpectralWindow, MsSpectralWindowMut, MsState, MsStateMut, MsSysCal, MsSysCalMut, MsWeather,
    MsWeatherMut, SubTable,
};
pub use transform::{
    ContinuumFitOptions, MsPartitionAxis, MsRegridOptions, MsTransformError,
    MsTransformMultiMsOptions, MsTransformReport, MsTransformRequest, MsTransformTaskOptions,
    MsTransformTiling, PlotMsModelReferences, PlotMsModelSpwSelection, PreparedTransformInputView,
    TimeAverageOptions, TransformDataColumn, TransformInputColumns, TransformInputView,
    TransformTaskHistory, append_task_history, mstransform, mstransform_task,
};
pub use virtual_model::{
    VirtualModelCombinations, VirtualModelEntry, VirtualModelKind, VirtualModelRecord,
    VirtualModelRow, VirtualModelSelection, clear_virtual_model, put_virtual_model, virtual_model,
    virtual_model_clear_ids, virtual_model_image_path, virtual_model_index_combination,
};
pub use visibility_buffer::{
    SourcePartition, SourcePartitionId, VisibilityBuffer, VisibilityBufferAllocationReport,
    VisibilityBufferColumnReport, VisibilityBufferFillReport, VisibilityBufferRequest,
    VisibilityBufferTimings, VisibilityChannelReadGranularity, VisibilityChannelReadRange,
    VisibilityComplexSamples, VisibilityFloatSamples, VisibilityReadBlockPlan, VisibilityShape,
};
pub use write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetColumnWriteMode, MeasurementSetColumnWriteTelemetry, MeasurementSetCreateTarget,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetScalarColumnPlan,
    MeasurementSetWriteColumnPlan, MeasurementSetWriteError, MeasurementSetWriteOperation,
    MeasurementSetWritePlan, MeasurementSetWriteResources, MeasurementSetWriteSession,
    MeasurementSetWriteTelemetry, ScalarColumnId, TARGET_BATCH_BYTES, VisibilityColumns,
    maximum_visibility_cell_elements, standard_main_scalar_column_plans,
};

/// The most heap the provider [`open_measures_runtime`] returns holds while
/// it loads its catalogs and after
/// ([`casa_measures::MeasuresRuntime::LOAD_BYTES`]), for an application
/// that admits it before its first lookup.
pub const MEASURES_RUNTIME_BYTES: usize = casa_measures::MeasuresRuntime::LOAD_BYTES;

/// The budget of the process-wide table-read cache every table read in the
/// process shares ([`casa_tables::table_cache_budget_bytes`]); it fills as
/// MeasurementSets are read and keeps what it holds.
#[must_use]
pub fn table_read_cache_bytes() -> usize {
    casa_tables::table_cache_budget_bytes()
}

/// Open the production Measures provider used by MeasurementSet-backed applications.
///
/// Keeping discovery here ensures frontends use the same reference-data authority
/// as selected-observation resolution and derived-column evaluation.
pub fn open_measures_runtime() -> MsResult<std::sync::Arc<dyn casa_core::measures::MeasuresProvider>>
{
    #[cfg(test)]
    {
        Ok(casa_test_support::deterministic_measures_provider())
    }
    #[cfg(not(test))]
    {
        casa_measures::MeasuresRuntime::open_discovered(Default::default())
            .map(|runtime| {
                let runtime: std::sync::Arc<dyn casa_core::measures::MeasuresProvider> =
                    std::sync::Arc::new(runtime);
                runtime
            })
            .map_err(|error| MsError::MeasuresRuntime(error.to_string()))
    }
}
