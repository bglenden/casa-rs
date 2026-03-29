// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]
//! Typed MeasurementSet (MS) API for radio interferometry data.
//!
//! This crate provides idiomatic Rust APIs for reading and writing
//! MeasurementSet files — the primary data format for radio interferometry.
//! It builds on [`casacore_tables`] for table I/O and [`casacore_types`] for
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
//! use casacore_ms::schema::{SubtableId, required_columns};
//! use casacore_ms::column_def::build_table_schema;
//!
//! // Build a TableSchema for the ANTENNA subtable
//! let schema = build_table_schema(required_columns(SubtableId::Antenna))
//!     .expect("valid schema");
//! assert_eq!(schema.columns().len(), 8);
//! ```

pub mod builder;
pub mod column_def;
pub mod columns;
pub mod derived;
pub mod error;
pub mod grouping;
pub mod listobs;
mod metadata;
pub mod ms;
pub mod msexplore;
pub mod plot;
pub mod schema;
pub mod selection;
pub mod selection_helpers;
pub mod subtables;
pub mod validate;

#[cfg(test)]
pub(crate) mod test_helpers;

pub use builder::{MeasurementSetBuilder, MsSchemas};
pub use error::{MsError, MsResult};
pub use listobs::{
    ListObsOptions, ListObsOutputFormat, ListObsSummary, ListObsUvCoverage, ListObsUvPoint,
    ListObsUvTrack,
};
pub use ms::MeasurementSet;
pub use msexplore::{
    MsAverageSpec, MsAxis, MsColorAxis, MsDataColumn, MsExploreSpec, MsExportFormat, MsFlagAction,
    MsFlagEditSpec, MsIterationAxis, MsIterationSpec, MsLayoutSpec, MsPlotPayload, MsPlotPreset,
    MsPlotSpec, MsPlotStyleSpec, MsScatterGridPayload, MsScatterPanelPayload, MsScatterPlotPayload,
    MsScatterSeries, MsSelectionSpec, MsTransformSpec, build_msexplore_plot_payload,
    build_msexplore_plot_payload_from_path, export_msexplore_plot, render_msexplore_plot_image,
};
pub use plot::{
    ListObsPlotExportFormat, ListObsPlotKind, ListObsPlotPayload, ListObsPlotRenderStyle,
    ListObsPlotSpec, ListObsPlotTheme, build_listobs_plot_payload_from_summary,
    build_listobs_uv_plot_payload, build_listobs_visibility_plot_payload, export_listobs_plot,
    render_listobs_plot_image, render_listobs_plot_image_with_style,
};
pub use schema::SubtableId;
pub use schema::main_table::{OptionalMainColumn, VisibilityDataColumn};
pub use subtables::{
    MsAntenna, MsAntennaMut, MsDataDescription, MsDataDescriptionMut, MsDoppler, MsDopplerMut,
    MsFeed, MsFeedMut, MsField, MsFieldMut, MsFlagCmd, MsFlagCmdMut, MsFreqOffset, MsFreqOffsetMut,
    MsHistory, MsHistoryMut, MsObservation, MsObservationMut, MsPointing, MsPointingMut,
    MsPolarization, MsPolarizationMut, MsProcessor, MsProcessorMut, MsSource, MsSourceMut,
    MsSpectralWindow, MsSpectralWindowMut, MsState, MsStateMut, MsSysCal, MsSysCalMut, MsWeather,
    MsWeatherMut, SubTable,
};
