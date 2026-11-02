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
use crate::{
    MeasurementSetSummaryOutputFormat, MsColorAxis, MsDataColumn, MsExploreSpec, MsIterationAxis,
    MsPageExportRange, MsPlotPreset, MsPlotSpec, MsSelection, build_msexplore_payload_from_spec,
};

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

/// One axis of a [`MeasurementSetPlot`], as its metadata names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlotAxisMetadata {
    /// Stable axis identifier.
    pub id: String,
    /// Human-readable axis label.
    pub label: String,
    /// Unit parsed from the label's trailing parentheses, or empty.
    pub unit: String,
}

/// Scale of a document axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlotAxisScale {
    /// Linear scale.
    Linear,
    /// Logarithmic scale.
    Log,
}

/// How a renderer draws a layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlotLayerKind {
    /// Unconnected markers.
    Scatter,
    /// Connected line.
    Line,
    /// Horizontal bars over lanes.
    Interval,
}

/// Which of a layer's arrays hold its data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlotPayloadStrategy {
    /// `x_values` and `y_values`, with optional labels, sizes and provenance.
    PointCloud,
    /// The `interval_*` arrays.
    Intervals,
}

/// One axis of a document or panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlotDocumentAxis {
    /// Identifier that layers reference.
    pub id: String,
    /// Human-readable axis label.
    pub label: String,
    /// Display unit, or empty.
    pub unit: String,
    /// Lower display bound.
    pub lower: f64,
    /// Upper display bound.
    pub upper: f64,
    /// Axis scale.
    pub scale: PlotAxisScale,
    /// Names of the integer lanes of a categorical axis, or empty.
    pub lane_labels: Vec<String>,
    /// Whether the axis is drawn on the trailing (right or top) edge.
    pub draws_on_trailing_edge: bool,
}

/// MeasurementSet source of one plotted point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlotPointProvenance {
    /// MAIN-table row.
    pub row: u64,
    /// Correlation index within the row.
    pub corr: u64,
    /// First channel, inclusive.
    pub chan_start: u64,
    /// Last channel, exclusive.
    pub chan_end: u64,
}

/// One drawable layer of a document or panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlotDocumentLayer {
    /// Identifier unique within its panel.
    pub id: String,
    /// Legend title.
    pub title: String,
    /// How a renderer draws the layer.
    pub kind: PlotLayerKind,
    /// Axis that `x_values` and the interval ends refer to.
    pub x_axis_id: String,
    /// Axis that `y_values` and the interval lanes refer to.
    pub y_axis_id: String,
    /// Point X coordinates.
    pub x_values: Vec<f64>,
    /// Point Y coordinates, paired with `x_values`.
    pub y_values: Vec<f64>,
    /// Per-point labels, empty or paired with `x_values`.
    pub point_labels: Vec<String>,
    /// Per-point marker sizes, empty or paired with `x_values`.
    pub point_symbol_sizes: Vec<f64>,
    /// Interval starts.
    pub interval_x_start: Vec<f64>,
    /// Interval ends, paired with `interval_x_start`.
    pub interval_x_end: Vec<f64>,
    /// Interval lane centres.
    pub interval_y: Vec<f64>,
    /// Interval bar heights in lane units.
    pub interval_height: Vec<f64>,
    /// Interval labels.
    pub interval_labels: Vec<String>,
    /// Per-point provenance, empty or paired with `x_values`; omitted for
    /// layers of more than 8,000 points.
    pub provenance: Vec<PlotPointProvenance>,
    /// Palette grouping key.
    pub color_group: String,
    /// Default marker size.
    pub symbol_size: f64,
    /// Line width.
    pub line_width: f64,
    /// Opacity from 0 to 1.
    pub opacity: f64,
    /// Number of points or intervals in the layer.
    pub source_sample_count: u64,
    /// Which arrays hold the layer's data.
    pub payload_strategy: PlotPayloadStrategy,
    /// Where the layer's data came from.
    pub provenance_summary: String,
}

/// A text annotation at a data position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlotDocumentAnnotation {
    /// Identifier unique within its panel.
    pub id: String,
    /// X position in data coordinates.
    pub x: f64,
    /// Y position in data coordinates.
    pub y: f64,
    /// Annotation text.
    pub text: String,
}

/// One panel of a multi-panel document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlotDocumentPanel {
    /// Stable panel identifier.
    pub id: String,
    /// Panel title.
    pub title: String,
    /// Axes the panel's layers reference.
    pub axes: Vec<PlotDocumentAxis>,
    /// Drawable layers.
    pub layers: Vec<PlotDocumentLayer>,
    /// Text annotations.
    pub annotations: Vec<PlotDocumentAnnotation>,
}

/// A renderer-neutral plot: one panel, or a list of panels.
///
/// A single-panel document fills `axes`, `layers` and `annotations` and
/// leaves `panels` empty; a multi-panel document does the reverse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlotDocument {
    /// Stable document identifier.
    pub id: String,
    /// Document title.
    pub title: String,
    /// Subtitle, usually the data-preparation summary.
    pub subtitle: String,
    /// Page header lines.
    pub header_lines: Vec<String>,
    /// Axes of a single-panel document.
    pub axes: Vec<PlotDocumentAxis>,
    /// Layers of a single-panel document.
    pub layers: Vec<PlotDocumentLayer>,
    /// Annotations of a single-panel document.
    pub annotations: Vec<PlotDocumentAnnotation>,
    /// Panels of a multi-panel document.
    pub panels: Vec<PlotDocumentPanel>,
    /// Whether renderers show a legend.
    pub show_legend: bool,
}

/// One series of a [`MeasurementSetPlot`] and the rows it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlotSeriesMetadata {
    /// Series label.
    pub label: String,
    /// Palette grouping key.
    pub color_group: String,
    /// Number of points.
    pub point_count: u64,
    /// MAIN-table row of the first point, when known.
    pub first_row: Option<u64>,
    /// MAIN-table row of the last point, when known.
    pub last_row: Option<u64>,
}

/// How many points a plot drew against its budget, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlotSamplingDiagnostics {
    /// Point budget the plot was built with.
    pub requested_max_points: u64,
    /// Points drawn.
    pub rendered_point_count: u64,
    /// Series drawn.
    pub series_count: u64,
    /// Notes on empty plots, decimation and timing.
    pub diagnostics: Vec<String>,
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
#[path = "plot_document_tests.rs"]
mod tests;
