// SPDX-License-Identifier: LGPL-3.0-or-later

//! Typed MeasurementSet plot documents for renderers outside Rust.
//!
//! [`build_measurement_set_plot`] plans one msexplore preset
//! ([`MsPlotPreset`]) over a MeasurementSet and returns a
//! [`MeasurementSetPlot`]: a renderer-neutral [`PlotDocument`] of panels,
//! axes and layers, with the metadata a renderer shows beside it.
//!
//! A document is either one panel, held in its top-level `axes`, `layers` and
//! `annotations`, or a list of [`PlotDocumentPanel`]s. Each
//! [`PlotDocumentLayer`] is a point cloud (scatter samples, antenna
//! positions) or a set of intervals (scan and spectral-window lanes); the
//! layer's [`PlotPayloadStrategy`] says which of its arrays are filled.
//!
//! The Python package renders these documents with Matplotlib (ADR-0017
//! item 3). The types serialize with serde, so a task can return a document
//! in its reply, and they depend on no binding crate.

mod convert;

use std::path::PathBuf;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use self::convert::{plot_document, plot_payload_metadata};
pub use crate::document::*;
use crate::msexplore::{
    MsColorAxis, MsDataColumn, MsExploreSpec, MsIterationAxis, MsPageExportRange, MsPlotPayload,
    MsPlotPreset, MsPlotSpec, build_msexplore_payload_from_spec,
};
use casa_ms::{MeasurementSetSummaryOutputFormat, MsSelection};

pub use convert::{label_unit, series_y_bounds};

/// Convert an already prepared visibility payload for the PlotMS renderer.
pub fn plotms_visibility_document(payload: &MsPlotPayload) -> PlotDocument {
    let preset = MsPlotPreset::AmplitudeVsTime;
    let metadata = plot_payload_metadata(payload, preset, u64::MAX);
    let mut document = plot_document(payload, &metadata, preset);
    document.id = "plotms-visibility".into();
    document.subtitle.clear();
    document
}

/// Point budget used when a [`MeasurementSetPlotRequest`] asks for zero points.
pub const PLOT_DOCUMENT_DEFAULT_MAX_POINTS: usize = 250_000;

/// Largest layer that keeps per-point provenance; bigger layers drop it.
const POINT_PROVENANCE_LIMIT: usize = 8_000;

/// One MeasurementSet plot to build: a preset, a selection and averaging.
///
/// Text selectors use msexplore's syntax. Each is trimmed, and an empty
/// selector or `all` selects everything.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeasurementSetPlotRequest {
    /// MeasurementSet root directory.
    pub dataset_path: PathBuf,
    /// The plot to build.
    pub preset: MsPlotPreset,
    /// FIELD selector.
    pub field: Option<String>,
    /// Spectral-window and channel selector.
    pub spw: Option<String>,
    /// TIME range selector.
    pub timerange: Option<String>,
    /// UV-distance range selector.
    pub uvrange: Option<String>,
    /// Antenna and baseline selector.
    pub antenna: Option<String>,
    /// SCAN_NUMBER selector.
    pub scan: Option<String>,
    /// Correlation selector.
    pub correlation: Option<String>,
    /// ARRAY_ID selector.
    pub array: Option<String>,
    /// OBSERVATION_ID selector.
    pub observation: Option<String>,
    /// Scan-intent selector.
    pub intent: Option<String>,
    /// FEED selector.
    pub feed: Option<String>,
    /// TaQL expression over the MAIN table.
    pub msselect: Option<String>,
    /// Visibility column, case-insensitive; empty selects `data`.
    pub data_column: String,
    /// Colour axis overriding the preset's default.
    pub color_by: Option<String>,
    /// Channels averaged per plotted sample.
    pub avgchannel: Option<usize>,
    /// Averaging interval in seconds.
    pub avgtime: Option<f64>,
    /// Average across scans within `avgtime`.
    pub avgscan: bool,
    /// Average across fields within `avgtime`.
    pub avgfield: bool,
    /// Average across baselines.
    pub avgbaseline: bool,
    /// Average per antenna.
    pub avgantenna: bool,
    /// Average across spectral windows.
    pub avgspw: bool,
    /// Scalar rather than vector averaging.
    pub scalar: bool,
    /// Axis that splits the plot into panels.
    pub iteraxis: Option<String>,
    /// Point budget; zero selects [`PLOT_DOCUMENT_DEFAULT_MAX_POINTS`].
    pub max_plot_points: usize,
}

impl MeasurementSetPlotRequest {
    /// A preset over the whole MeasurementSet: the `data` column, the
    /// preset's colour axis, no averaging and the default point budget.
    pub fn new(dataset_path: impl Into<PathBuf>, preset: MsPlotPreset) -> Self {
        Self {
            dataset_path: dataset_path.into(),
            preset,
            field: None,
            spw: None,
            timerange: None,
            uvrange: None,
            antenna: None,
            scan: None,
            correlation: None,
            array: None,
            observation: None,
            intent: None,
            feed: None,
            msselect: None,
            data_column: "data".to_string(),
            color_by: None,
            avgchannel: None,
            avgtime: None,
            avgscan: false,
            avgfield: false,
            avgbaseline: false,
            avgantenna: false,
            avgspw: false,
            scalar: false,
            iteraxis: None,
            max_plot_points: 0,
        }
    }
}

/// Why a plot could not be built.
///
/// The message is the bare reason, without a prefix.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MeasurementSetPlotError {
    /// The dataset path is not a MeasurementSet directory.
    #[error("{0}")]
    InvalidPath(String),
    /// A request option was invalid, or planning or reading the plot failed.
    #[error("{0}")]
    Plot(String),
}

/// A built MeasurementSet plot: its document and the metadata around it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeasurementSetPlot {
    /// The preset that was built.
    pub preset: MsPlotPreset,
    /// The preset's human-readable name.
    pub preset_label: String,
    /// Plot title.
    pub title: String,
    /// Data-preparation summary.
    pub summary: String,
    /// MeasurementSet path as given.
    pub dataset_path: String,
    /// Normalized visibility column name.
    pub data_column: String,
    /// The data column and the field, spw and correlation selections.
    pub selection_summary: String,
    /// Primary X axis.
    pub x_axis: PlotAxisMetadata,
    /// Primary Y axis.
    pub y_axis: PlotAxisMetadata,
    /// Every drawn series.
    pub series: Vec<PlotSeriesMetadata>,
    /// Point counts and notes.
    pub sampling: PlotSamplingDiagnostics,
    /// The renderer-neutral document.
    pub document: PlotDocument,
}

/// Build one preset plot of a MeasurementSet.
///
/// The request's selectors, averaging and colour axis are applied to the
/// preset's msexplore plan, the plan runs over the MeasurementSet, and its
/// payload is converted into a [`PlotDocument`] with metadata.
pub fn build_measurement_set_plot(
    request: &MeasurementSetPlotRequest,
) -> Result<MeasurementSetPlot, MeasurementSetPlotError> {
    let dataset_path = &request.dataset_path;
    if !dataset_path.is_dir() {
        return Err(MeasurementSetPlotError::InvalidPath(format!(
            "{} is not a MeasurementSet directory",
            dataset_path.display()
        )));
    }
    let plot_error = |error: String| {
        MeasurementSetPlotError::Plot(format!("{}: {error}", dataset_path.display()))
    };

    let max_plot_points = if request.max_plot_points == 0 {
        PLOT_DOCUMENT_DEFAULT_MAX_POINTS
    } else {
        request.max_plot_points
    };
    let data_column = normalize_data_column(&request.data_column);
    let mut plot = MsPlotSpec::from_preset(request.preset);
    plot.data_column = MsDataColumn::parse(&data_column).map_err(plot_error)?;
    if let Some(color_by) = normalized_optional(request.color_by.as_deref()) {
        plot.color_by = MsColorAxis::parse(&color_by).map_err(plot_error)?;
    }
    plot.averaging.avgchannel = request.avgchannel;
    plot.averaging.avgtime = request.avgtime;
    plot.averaging.avgscan = request.avgscan;
    plot.averaging.avgfield = request.avgfield;
    plot.averaging.avgbaseline = request.avgbaseline;
    plot.averaging.avgantenna = request.avgantenna;
    plot.averaging.avgspw = request.avgspw;
    plot.averaging.scalar = request.scalar;
    plot.iteration.iteraxis = normalized_optional(request.iteraxis.as_deref())
        .map(|iteraxis| MsIterationAxis::parse(&iteraxis))
        .transpose()
        .map_err(plot_error)?;

    let spec = MsExploreSpec {
        ms_path: dataset_path.clone(),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection: request_selection(request),
        header_items: vec![],
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points,
        plots: vec![plot],
    };

    let payload_started = Instant::now();
    let payload = build_msexplore_payload_from_spec(&spec).map_err(plot_error)?;
    let payload_elapsed = payload_started.elapsed();
    let metadata_started = Instant::now();
    let mut metadata = plot_payload_metadata(&payload, request.preset, max_plot_points as u64);
    let metadata_elapsed = metadata_started.elapsed();
    let document_started = Instant::now();
    let document = plot_document(&payload, &metadata, request.preset);
    let document_elapsed = document_started.elapsed();
    metadata.sampling.diagnostics.push(format!(
        "timing: payload={} ms, metadata={} ms, document={} ms",
        payload_elapsed.as_millis(),
        metadata_elapsed.as_millis(),
        document_elapsed.as_millis()
    ));

    Ok(MeasurementSetPlot {
        preset: request.preset,
        preset_label: request.preset.display_name().to_string(),
        title: metadata.title,
        summary: metadata.summary,
        dataset_path: dataset_path.display().to_string(),
        data_column,
        selection_summary: selection_summary(request),
        x_axis: metadata.x_axis,
        y_axis: metadata.y_axis,
        series: metadata.series,
        sampling: metadata.sampling,
        document,
    })
}

fn request_selection(request: &MeasurementSetPlotRequest) -> MsSelection {
    MsSelection {
        field: normalized_optional(request.field.as_deref()),
        spw: normalized_optional(request.spw.as_deref()),
        timerange: normalized_optional(request.timerange.as_deref()),
        uvrange: normalized_optional(request.uvrange.as_deref()),
        antenna: normalized_optional(request.antenna.as_deref()),
        scan: normalized_optional(request.scan.as_deref()),
        correlation: normalized_optional(request.correlation.as_deref()),
        array: normalized_optional(request.array.as_deref()),
        observation: normalized_optional(request.observation.as_deref()),
        intent: normalized_optional(request.intent.as_deref()),
        feed: normalized_optional(request.feed.as_deref()),
        data_description: None,
        state: None,
        msselect: normalized_optional(request.msselect.as_deref()),
        ..MsSelection::default()
    }
}

fn normalize_data_column(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "data".to_string();
    }
    trimmed.to_ascii_lowercase()
}

fn normalized_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "all")
        .map(str::to_string)
}

fn selection_summary(request: &MeasurementSetPlotRequest) -> String {
    let mut parts = vec![format!(
        "data column {}",
        normalize_data_column(&request.data_column)
    )];
    if let Some(field) = normalized_optional(request.field.as_deref()) {
        parts.push(format!("field {field}"));
    }
    if let Some(spw) = normalized_optional(request.spw.as_deref()) {
        parts.push(format!("spw {spw}"));
    }
    if let Some(correlation) = normalized_optional(request.correlation.as_deref()) {
        parts.push(format!("correlation {correlation}"));
    }
    parts.join(", ")
}

#[cfg(test)]
mod tests;
