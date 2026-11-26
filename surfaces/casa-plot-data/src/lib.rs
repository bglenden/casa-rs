// SPDX-License-Identifier: LGPL-3.0-or-later
//! Feature-gated renderer-neutral plot documents and MeasurementSet plot builders.

mod document;

#[cfg(feature = "calibration")]
pub mod calibration;
#[cfg(feature = "ms")]
pub mod msexplore;
#[cfg(feature = "ms")]
pub mod plot;
#[cfg(feature = "ms")]
pub mod plot_data;
#[cfg(feature = "ms")]
pub mod plot_document;
#[cfg(feature = "ms")]
pub(crate) mod plot_visibility;

pub use document::*;

#[cfg(feature = "calibration")]
pub use calibration::plots::{
    CalibrationPlotError, CalibrationPlotPreset, CalibrationPlotRequest,
    build_calibration_plot_payload,
};
#[cfg(feature = "ms")]
pub use msexplore::{
    MsAverageSpec, MsAxis, MsColorAxis, MsDataColumn, MsExploreSpec, MsExportFormat, MsFlagAction,
    MsFlagEditPreview, MsFlagEditSpec, MsFlagRegion, MsFlagRowEdit, MsFlagSampleEdit,
    MsIterationAxis, MsLegendPosition, MsPageExportRange, MsPageHeaderItem, MsPlotPayload,
    MsPlotPreset, MsPlotSpec, MsPlotStyleSpec, MsScatterGridPayload, MsScatterPageItemPayload,
    MsScatterPagePayload, MsScatterPanelPayload, MsScatterPlotMsAverageIds,
    MsScatterPlotMsAverageSpw, MsScatterPlotMsMetadata, MsScatterPlotPayload, MsScatterPointRef,
    MsScatterSeries, apply_msexplore_flag_edit, apply_msexplore_flag_edit_for_request,
    build_msexplore_payload, build_msexplore_payload_from_spec, build_msexplore_plot_payload,
    build_msexplore_plot_payload_from_path, export_msexplore_plot, preview_msexplore_flag_edit,
    preview_msexplore_flag_edit_for_request,
};
#[cfg(feature = "ms")]
pub use plot::{
    ListObsPlotKind as MeasurementSetPlotKind, ListObsPlotPayload as MeasurementSetPlotPayload,
    ListObsPlotSpec as MeasurementSetPlotSpec,
    build_listobs_plot_payload_from_summary as build_measurement_set_plot_payload_from_summary,
    build_listobs_uv_plot_payload,
    build_listobs_visibility_plot_payload as build_measurement_set_visibility_plot_payload,
};
#[cfg(feature = "ms")]
pub use plot_data::{MS_PLOT_DATA_SCHEMA_VERSION, MsPlotData, MsPlotDataPanel, MsPlotDataSeries};
#[cfg(feature = "ms")]
pub use plot_document::{
    MeasurementSetPlot, MeasurementSetPlotError, MeasurementSetPlotRequest,
    PLOT_DOCUMENT_DEFAULT_MAX_POINTS, build_measurement_set_plot,
};

#[cfg(not(feature = "ms"))]
pub mod plot_document {
    //! Renderer-neutral plot documents and binary array descriptors.
    pub use crate::document::*;
}
