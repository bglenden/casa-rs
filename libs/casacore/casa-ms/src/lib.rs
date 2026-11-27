// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]
//!
//! <details>
//! <summary>API details</summary>
//!
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
//! - **`const` arrays of [`ColumnDef`]** for schema metadata
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
//! use casa_ms::build_table_schema;
//!
//! // Build a TableSchema for the ANTENNA subtable
//! let schema = build_table_schema(required_columns(SubtableId::Antenna))
//!     .expect("valid schema");
//! assert_eq!(schema.columns().len(), 8);
//! ```
//!
//! </details>

mod asdm_table;
mod builder;
mod column_def;
pub mod columns;
pub mod derived;
mod ephemeris;
mod error;
pub mod fits_visibility;
mod grouping;
mod listobs;
mod metadata;
mod ms;
pub mod observation_io;
mod probes;
pub mod schema;
mod scratch_columns;
mod selected_pointing;
pub mod selection;
mod spectral_selection;
mod subtable_overrides;
#[doc(hidden)]
pub mod subtable_rows;
pub mod subtables;
mod tool_ephemeris;
mod tool_history;
mod tool_scan_summary;
mod validate;
mod virtual_model;
mod visibility_buffer;
mod voltage_pattern;
mod write_session;

#[cfg(test)]
pub(crate) mod test_helpers;

pub use asdm_table::read_asdm_table;
pub use builder::{MeasurementSetBuilder, MsSchemas};
pub use casa_measures::PlanetaryEphemeris;
pub use column_def::{ColumnDef, ColumnKind, build_table_schema, column_schema};
pub use ephemeris::{
    EphemerisBudget, PhaseCentreEphemeris, PhaseCentreEphemerisError, find_ephemeris_in_field,
};
pub use error::{MsError, MsResult};
pub use grouping::{DEFAULT_SORT_COLUMNS, iter_ms, iter_ms_by};
pub use listobs::{
    AntennaSummary, DataDescriptionSummary, FieldSummary, ListObsOptions,
    ListObsOptions as MeasurementSetSummaryOptions,
    ListObsOutputFormat as MeasurementSetSummaryOutputFormat, ListObsSummary,
    ListObsSummary as MeasurementSetSummary, ListObsUvCoverage,
    ListObsUvCoverage as MeasurementSetUvCoverage, ListObsUvPoint as MeasurementSetUvPoint,
    ListObsUvTrack as MeasurementSetUvTrack, MeasurementSetInfo, ObservationSummary,
    PolarizationSummary, ScanSummary as MeasurementSetScanSummary, SourceSummary,
    SpectralWindowSummary, stokes_name,
};
pub use ms::linked_subtables::strict_subtable_path;
pub use ms::{
    MeasurementSet, casa_interferometer_count, casa_uvw_tile_shape, casa_visibility_tile_shape,
    casa_weight_tile_shape, held_subtable_reader, measurement_set_main_table_bindings,
    measurement_set_table_options, subtable_path_from_main,
};
pub use probes::{
    MeasurementSetContext, MeasurementSetNamedContext, MeasurementSetObservationContext,
    MeasurementSetSpectralWindowContext, MeasurementSetSubtableContext, MeasurementSetTimeRange,
    MeasurementSetUvRange,
};
pub use schema::SubtableId;
pub use schema::main_table::{OptionalMainColumn, VisibilityDataColumn, initial_model_visibility};
pub use scratch_columns::{
    prepare_write, prepare_write_holding, remove_scratch_model_holding, virtual_model_fields,
};
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
    CasaChannelFrequencies, CombinedChannel, CubeAxisConfig, CubeAxisValue,
    CubeChannelContribution, CubeInterpolation, CubeSpecMode, CubeSpectralSetup,
    LegacyRegridQuantity, RegridQuantity, RegridRequest, ResolvedChannelSelection,
    casa_data_channel_frequencies, casa_default_start_channels, casa_selected_band_hz,
    centered_numeric_frequencies, combine_spectral_windows, combine_windows,
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
pub use tool_ephemeris::{ToolEphemeris, add_tool_ephemeris};
pub use tool_history::{
    ToolHistoryEntry, append_tool_history, history_rows, plain_history_path, read_tool_history,
};
pub use tool_scan_summary::{ToolScanSummary, tool_scan_summary};
pub use validate::ValidationIssue;
pub use virtual_model::{
    VirtualModelCombinations, VirtualModelEntry, VirtualModelKind, VirtualModelPublication,
    VirtualModelRecord, VirtualModelRow, VirtualModelSelection, clear_virtual_model,
    clear_virtual_model_holding, put_virtual_model, put_virtual_model_holding,
    selected_model_combinations, verify_virtual_model_target_holding, virtual_model,
    virtual_model_clear_ids, virtual_model_image_path, virtual_model_index_combination,
    virtual_model_target_for_field,
};
pub use visibility_buffer::{
    SourcePartition, SourcePartitionId, VisibilityBuffer, VisibilityBufferAllocationReport,
    VisibilityBufferColumnReport, VisibilityBufferFillReport, VisibilityBufferRequest,
    VisibilityBufferTimings, VisibilityChannelReadGranularity, VisibilityChannelReadRange,
    VisibilityComplexSamples, VisibilityFloatSamples, VisibilityReadBlockPlan, VisibilityShape,
};
pub use voltage_pattern::{
    VoltagePatternError, VoltagePatternTable, radial_curve_storage, radial_lookup, radial_pattern,
    radial_squint,
};
pub use write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetColumnWriteMode, MeasurementSetColumnWriteTelemetry, MeasurementSetCreateTarget,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetScalarColumnPlan,
    MeasurementSetWriteColumnPlan, MeasurementSetWriteError, MeasurementSetWriteOperation,
    MeasurementSetWritePlan, MeasurementSetWriteResources, MeasurementSetWriteSession,
    MeasurementSetWriteTelemetry, ScalarColumnId, TARGET_BATCH_BYTES, VisibilityColumns,
    VisibilityWriteDestination, maximum_visibility_cell_elements,
    standard_main_scalar_column_plans,
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
    casa_measures::MeasuresRuntime::open_discovered(Default::default())
        .map(|runtime| {
            let runtime: std::sync::Arc<dyn casa_core::measures::MeasuresProvider> =
                std::sync::Arc::new(runtime);
            runtime
        })
        .map_err(|error| MsError::MeasuresRuntime(error.to_string()))
}

/// Open a staged subtable for persisted in-place edits.
pub fn open_staged_subtable_for_write(
    path: &std::path::Path,
) -> Result<casa_tables::Table, casa_tables::TableError> {
    let options = casa_tables::TableOptions::new(path);
    #[cfg(unix)]
    return casa_tables::Table::open_with_lock(
        options,
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait),
    );
    #[cfg(not(unix))]
    casa_tables::Table::open(options)
}
/// Return the sibling directory holding the flag snapshots for an MS.
pub fn flagversions_dir(path: &std::path::Path) -> std::path::PathBuf {
    let mut os = std::ffi::OsString::from(path.as_os_str());
    os.push(".flagversions");
    std::path::PathBuf::from(os)
}
