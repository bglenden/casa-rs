// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core renderer-neutral plot document structures shared without MeasurementSet dependencies.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::rich::{PlotAspect, PlotLayerData, PlotLayerStyle, PlotLayout, PlotTextStyle};

/// One axis of a plot document or metadata description.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlotAxisScale {
    /// Linear scale.
    Linear,
    /// Logarithmic scale.
    Log,
    /// Linear Modified Julian Date seconds displayed as clock ticks.
    MjdSecondsClock {
        /// Midnight UTC of the earliest selected observation day in MJD seconds.
        reference_mjd_seconds: i64,
    },
}

/// How a renderer draws a layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlotLayerKind {
    /// Unconnected markers.
    Scatter,
    /// Connected line.
    Line,
    /// Horizontal bars over lanes.
    Interval,
    /// A two-dimensional image.
    Image,
    /// Circles in the data coordinate system.
    Circles,
}

/// Which of a layer's arrays hold its data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PlotPayloadStrategy {
    /// `x_values` and `y_values`, with optional labels, sizes and provenance.
    PointCloud,
    /// The `interval_*` arrays.
    Intervals,
    /// Arrays in the caller-owned raw payload file.
    BinaryArrays,
}

/// One axis of a document or panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
    /// File-backed data, absent for legacy inline layers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<PlotLayerData>,
    /// Optional renderer styling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style: Option<PlotLayerStyle>,
}

/// A text annotation at a data position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PlotDocumentAnnotation {
    /// Identifier unique within its panel.
    pub id: String,
    /// X position in data coordinates.
    pub x: f64,
    /// Y position in data coordinates.
    pub y: f64,
    /// Annotation text.
    pub text: String,
    /// Optional axes-relative placement and style.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style: Option<PlotTextStyle>,
}

/// One panel of a multi-panel document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PlotDocumentPanel {
    /// Stable panel identifier.
    pub id: String,
    /// Panel title.
    pub title: String,
    /// Override the page's font size for this panel's title only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title_font_size: Option<String>,
    /// Axes the panel's layers reference.
    pub axes: Vec<PlotDocumentAxis>,
    /// Drawable layers.
    pub layers: Vec<PlotDocumentLayer>,
    /// Text annotations.
    pub annotations: Vec<PlotDocumentAnnotation>,
    /// Aspect of this panel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aspect: Option<PlotAspect>,
    /// Draw the panel's border; absent keeps the renderer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_visible: Option<bool>,
}

/// A renderer-neutral plot: one panel, or a list of panels.
///
/// A single-panel document fills `axes`, `layers` and `annotations` and
/// leaves `panels` empty; a multi-panel document does the reverse.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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
    /// Optional explicit panel grid and margins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<PlotLayout>,
}

/// One series of a built plot and the rows it came from.
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
