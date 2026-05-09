// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared frontend services exposed to Swift and Python through UniFFI.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_images::{AnyPagedImage, ImageBrowserSession, ImageInfo, ImagePixelType};
use casa_ms::columns::main_ids;
use casa_ms::plot::{
    AntennaLayoutPlotPayload, AntennaLayoutPoint, ScanTimelineBar, ScanTimelinePlotPayload,
    SpectralWindowCoverageBar, SpectralWindowCoveragePlotPayload, UvCoverageSeries,
};
use casa_ms::{
    MeasurementSet, MeasurementSetPlotPayload, MeasurementSetSummaryOutputFormat, MsExploreSpec,
    MsPageExportRange, MsPlotPayload, MsPlotPreset, MsPlotSpec, MsScatterGridPayload,
    MsScatterPagePayload, MsScatterPlotPayload, MsScatterSeries, MsSelectionSpec,
    VisibilityDataColumn, build_msexplore_payload_from_spec,
};
use casa_tables::{ArrayShapeContract, ColumnType, Table, TableBrowser, TableOptions};
use casa_types::measures::direction::{
    angular_increment_arcseconds, declination_increment_arcseconds, format_declination_labeled,
    format_right_ascension_labeled,
};
use casa_types::{ArrayValue, PrimitiveType, ScalarValue, Value};
use casars_imagebrowser_protocol::ImageBrowserViewport;
use casars_imagebrowser_protocol::{
    ImageBrowserCommand, ImageBrowserFocus, ImageBrowserParameters, ImageBrowserPreviewRequest,
    ImagePlaneContentMode,
};
use casars_tablebrowser_protocol::{BrowserCommand, BrowserFocus, BrowserView, BrowserViewport};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const TASK_CATALOG_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/task-catalog.json"
));
const MAX_PROJECT_SCAN_ENTRIES: usize = 512;
const MAX_PROJECT_SCAN_DEPTH: usize = 4;
const DEFAULT_GUI_MAX_PLOT_POINTS: u64 = 250_000;
const MAIN_SCALAR_CHUNK_ROWS: usize = 65_536;
const FRONTEND_POINT_PROVENANCE_LIMIT: usize = 8_000;
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;
#[cfg(test)]
const DEFAULT_PLOT_WIDTH: u32 = 960;
#[cfg(test)]
const DEFAULT_PLOT_HEIGHT: u32 = 600;

#[derive(Debug, Clone, Deserialize)]
struct ImageExplorerSnapshotRequest {
    dataset_path: String,
    #[serde(default = "default_image_browser_width")]
    width: u16,
    #[serde(default = "default_image_browser_height")]
    height: u16,
    #[serde(default = "default_image_browser_inspector_height")]
    inspector_height: u16,
    #[serde(default = "default_image_browser_plane_pixel_width")]
    plane_pixel_width: u16,
    #[serde(default = "default_image_browser_plane_pixel_height")]
    plane_pixel_height: u16,
    #[serde(default)]
    active_view: Option<String>,
    #[serde(default)]
    focus: Option<String>,
    #[serde(default)]
    plane_content_mode: Option<String>,
    #[serde(default)]
    parameters: ImageBrowserParameters,
    #[serde(default)]
    cursor_x: Option<u64>,
    #[serde(default)]
    cursor_y: Option<u64>,
    #[serde(default)]
    selected_profile_axis: Option<u64>,
    #[serde(default)]
    non_display_indices: Vec<u64>,
    #[serde(default)]
    commands: Vec<ImageBrowserCommand>,
    #[serde(default)]
    transient_commands: Vec<ImageBrowserCommand>,
    #[serde(default = "default_include_image_profile")]
    include_profile: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct TableBrowserSnapshotRequest {
    dataset_path: String,
    #[serde(default = "default_table_browser_width")]
    width: u16,
    #[serde(default = "default_table_browser_height")]
    height: u16,
    #[serde(default = "default_table_browser_inspector_height")]
    inspector_height: u16,
    #[serde(default)]
    selected_view: Option<String>,
    #[serde(default)]
    focus: Option<String>,
    #[serde(default)]
    commands: Vec<BrowserCommand>,
    #[serde(default)]
    transient_commands: Vec<BrowserCommand>,
}

#[derive(Debug, Clone, Deserialize)]
struct TableBrowserCellWindowRequest {
    dataset_path: String,
    #[serde(default)]
    row_start: u64,
    #[serde(default = "default_table_browser_cell_row_limit")]
    row_limit: u64,
    #[serde(default)]
    column_start: u64,
    #[serde(default = "default_table_browser_cell_column_limit")]
    column_limit: u64,
    #[serde(default)]
    column_options: Vec<TableBrowserCellColumnDisplayOption>,
}

#[derive(Debug, Clone, Deserialize)]
struct TableBrowserCellColumnDisplayOption {
    column_index: u64,
    #[serde(default)]
    array_inline_limit: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct TableBrowserCellValueRequest {
    dataset_path: String,
    row_index: u64,
    column_index: u64,
}

#[derive(Debug, Clone, Serialize)]
struct TableBrowserCellWindowSnapshot {
    table_path: String,
    row_count: u64,
    column_count: u64,
    row_start: u64,
    column_start: u64,
    columns: Vec<TableBrowserCellWindowColumn>,
    rows: Vec<TableBrowserCellWindowRow>,
}

#[derive(Debug, Clone, Serialize)]
struct TableBrowserCellWindowColumn {
    index: u64,
    name: String,
    header: String,
    summary: String,
    width: u64,
    keywords: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct TableBrowserCellWindowRow {
    index: u64,
    cells: Vec<TableBrowserCellWindowCell>,
}

#[derive(Debug, Clone, Serialize)]
struct TableBrowserCellWindowCell {
    column_index: u64,
    display: String,
    defined: bool,
}

const fn default_image_browser_width() -> u16 {
    120
}

const fn default_image_browser_height() -> u16 {
    36
}

const fn default_image_browser_inspector_height() -> u16 {
    10
}

const fn default_image_browser_plane_pixel_width() -> u16 {
    512
}

const fn default_image_browser_plane_pixel_height() -> u16 {
    384
}

const fn default_include_image_profile() -> bool {
    true
}

const fn default_table_browser_width() -> u16 {
    120
}

const fn default_table_browser_height() -> u16 {
    32
}

const fn default_table_browser_inspector_height() -> u16 {
    10
}

const fn default_table_browser_cell_row_limit() -> u64 {
    1024
}

const fn default_table_browser_cell_column_limit() -> u64 {
    24
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum DatasetKind {
    MeasurementSet,
    Image,
    Table,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct DatasetProbe {
    pub id: String,
    pub name: String,
    pub path: String,
    pub kind: DatasetKind,
    pub size_bytes: u64,
    pub modified_unix_seconds: Option<u64>,
    pub probed_unix_seconds: u64,
    pub logical_size: String,
    pub units: String,
    pub fields: Vec<String>,
    pub spectral_windows: Vec<String>,
    pub scans: Vec<String>,
    pub arrays: Vec<String>,
    pub observations: Vec<String>,
    pub antennas: Vec<String>,
    pub intents: Vec<String>,
    pub feeds: Vec<String>,
    pub correlations: Vec<String>,
    pub columns: Vec<String>,
    pub data_columns: Vec<String>,
    pub subtables: Vec<String>,
    pub shape: Vec<u64>,
    pub notes: String,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct ProjectProbe {
    pub name: String,
    pub root_path: String,
    pub datasets: Vec<DatasetProbe>,
    pub diagnostics: Vec<String>,
    pub scanned_entry_count: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FrontendTaskCatalog {
    schema_version: u64,
    tasks: Vec<FrontendTaskCatalogEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FrontendTaskCatalogEntry {
    id: String,
    category: String,
    display_name: String,
    binary_name: String,
    cargo_package: String,
    override_env: String,
    shell_kind: String,
    interaction: String,
    browser_kind: Option<String>,
    dataset_kinds: Vec<String>,
    schema_source: String,
    show_in_tui: bool,
    show_in_swift: bool,
    include_in_suite: bool,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetUvRangeProbe {
    pub min_meters: f64,
    pub max_meters: f64,
    pub min_kilolambda: f64,
    pub max_kilolambda: f64,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetTimeRangeProbe {
    pub min_seconds: f64,
    pub max_seconds: f64,
    pub row_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum MeasurementSetPlotPreset {
    UvCoverage,
    AntennaLayout,
    ScanTimeline,
    SpectralWindowCoverage,
    PhaseVsTime,
    AmplitudePhaseVsTimeStacked,
    WeightVsTime,
    SigmaVsTime,
    FlagVsTime,
    WeightSpectrumVsTime,
    SigmaSpectrumVsTime,
    FlagRowVsTime,
    ElevationVsTime,
    AzimuthVsTime,
    HourAngleVsTime,
    ParallacticAngleVsTime,
    AzimuthVsElevation,
    AmplitudeVsFrequency,
    AmplitudeVsChannel,
    PhaseVsChannel,
    PhaseVsFrequency,
    AmplitudeVsVelocity,
    PhaseVsVelocity,
    AmplitudeVsUvDistance,
    AmplitudeVsTime,
    RealVsImaginary,
}

#[cfg(test)]
impl MeasurementSetPlotPreset {
    fn all() -> &'static [Self] {
        &[
            Self::UvCoverage,
            Self::AntennaLayout,
            Self::ScanTimeline,
            Self::SpectralWindowCoverage,
            Self::AmplitudeVsTime,
            Self::PhaseVsTime,
            Self::AmplitudePhaseVsTimeStacked,
            Self::AmplitudeVsUvDistance,
            Self::WeightVsTime,
            Self::SigmaVsTime,
            Self::FlagVsTime,
            Self::WeightSpectrumVsTime,
            Self::SigmaSpectrumVsTime,
            Self::FlagRowVsTime,
            Self::ElevationVsTime,
            Self::AzimuthVsTime,
            Self::HourAngleVsTime,
            Self::ParallacticAngleVsTime,
            Self::AzimuthVsElevation,
            Self::AmplitudeVsChannel,
            Self::PhaseVsChannel,
            Self::AmplitudeVsFrequency,
            Self::PhaseVsFrequency,
            Self::AmplitudeVsVelocity,
            Self::PhaseVsVelocity,
            Self::RealVsImaginary,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetPlotRequest {
    pub dataset_path: String,
    pub preset: MeasurementSetPlotPreset,
    pub field: Option<String>,
    pub spectral_window: Option<String>,
    pub timerange: Option<String>,
    pub uvrange: Option<String>,
    pub antenna: Option<String>,
    pub scan: Option<String>,
    pub correlation: Option<String>,
    pub array: Option<String>,
    pub observation: Option<String>,
    pub intent: Option<String>,
    pub feed: Option<String>,
    pub msselect: Option<String>,
    pub data_column: String,
    pub avgchannel: Option<u64>,
    pub avgtime: Option<f64>,
    pub avgscan: bool,
    pub avgfield: bool,
    pub avgbaseline: bool,
    pub avgantenna: bool,
    pub avgspw: bool,
    pub scalar: bool,
    pub width: u32,
    pub height: u32,
    pub max_plot_points: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotAxisMetadata {
    pub id: String,
    pub label: String,
    pub unit: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum PlotAxisScale {
    Linear,
    Log,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum PlotLayerKind {
    Scatter,
    Line,
    Interval,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentAxis {
    pub id: String,
    pub label: String,
    pub unit: String,
    pub lower: f64,
    pub upper: f64,
    pub scale: PlotAxisScale,
    pub lane_labels: Vec<String>,
    pub draws_on_trailing_edge: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PlotPointProvenance {
    pub row: u64,
    pub corr: u64,
    pub chan_start: u64,
    pub chan_end: u64,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentLayer {
    pub id: String,
    pub title: String,
    pub kind: PlotLayerKind,
    pub x_axis_id: String,
    pub y_axis_id: String,
    pub x_values: Vec<f64>,
    pub y_values: Vec<f64>,
    pub point_labels: Vec<String>,
    pub point_symbol_sizes: Vec<f64>,
    pub interval_x_start: Vec<f64>,
    pub interval_x_end: Vec<f64>,
    pub interval_y: Vec<f64>,
    pub interval_height: Vec<f64>,
    pub interval_labels: Vec<String>,
    pub provenance: Vec<PlotPointProvenance>,
    pub color_group: String,
    pub symbol_size: f64,
    pub line_width: f64,
    pub opacity: f64,
    pub source_sample_count: u64,
    pub payload_strategy: String,
    pub provenance_summary: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentAnnotation {
    pub id: String,
    pub x: f64,
    pub y: f64,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentPanel {
    pub id: String,
    pub title: String,
    pub axes: Vec<PlotDocumentAxis>,
    pub layers: Vec<PlotDocumentLayer>,
    pub annotations: Vec<PlotDocumentAnnotation>,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PlotDocumentPayload {
    pub id: String,
    pub title: String,
    pub subtitle: String,
    pub header_lines: Vec<String>,
    pub axes: Vec<PlotDocumentAxis>,
    pub layers: Vec<PlotDocumentLayer>,
    pub annotations: Vec<PlotDocumentAnnotation>,
    pub panels: Vec<PlotDocumentPanel>,
    pub show_legend: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PlotSeriesMetadata {
    pub label: String,
    pub color_group: String,
    pub point_count: u64,
    pub first_row: Option<u64>,
    pub last_row: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PlotSamplingDiagnostics {
    pub requested_max_points: u64,
    pub rendered_point_count: u64,
    pub series_count: u64,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct PlotRenderProvenance {
    pub renderer: String,
    pub image_format: String,
    pub width: u32,
    pub height: u32,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct MeasurementSetPlotResult {
    pub preset: MeasurementSetPlotPreset,
    pub preset_label: String,
    pub title: String,
    pub summary: String,
    pub dataset_path: String,
    pub data_column: String,
    pub selection_summary: String,
    pub x_axis: PlotAxisMetadata,
    pub y_axis: PlotAxisMetadata,
    pub series: Vec<PlotSeriesMetadata>,
    pub sampling: PlotSamplingDiagnostics,
    pub document: PlotDocumentPayload,
    pub render: PlotRenderProvenance,
    pub image_bytes: Vec<u8>,
}

#[derive(Debug, Error, uniffi::Error)]
pub enum FrontendServiceError {
    #[error("invalid path: {reason}")]
    InvalidPath { reason: String },
    #[error("I/O error: {reason}")]
    Io { reason: String },
    #[error("probe failed: {reason}")]
    Probe { reason: String },
    #[error("plot failed: {reason}")]
    Plot { reason: String },
    #[error("image explorer failed: {reason}")]
    ImageExplorer { reason: String },
    #[error("table explorer failed: {reason}")]
    TableExplorer { reason: String },
}

type FrontendResult<T> = Result<T, FrontendServiceError>;

#[uniffi::export]
pub fn task_catalog_json() -> FrontendResult<String> {
    let catalog: FrontendTaskCatalog =
        serde_json::from_str(TASK_CATALOG_JSON).map_err(|error| FrontendServiceError::Probe {
            reason: format!("parse task catalog: {error}"),
        })?;
    serde_json::to_string(&catalog).map_err(|error| FrontendServiceError::Probe {
        reason: format!("serialize task catalog: {error}"),
    })
}

#[uniffi::export]
pub fn probe_path(path: String) -> FrontendResult<Option<DatasetProbe>> {
    let path = PathBuf::from(path);
    probe_dataset_path(&path).map_err(|error| FrontendServiceError::Probe {
        reason: format!("{}: {error}", path.display()),
    })
}

#[uniffi::export]
pub fn probe_project(path: String) -> FrontendResult<ProjectProbe> {
    let root = PathBuf::from(path);
    let metadata = fs::metadata(&root).map_err(|error| FrontendServiceError::Io {
        reason: format!("metadata {}: {error}", root.display()),
    })?;
    if !metadata.is_dir() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} is not a directory", root.display()),
        });
    }

    let mut scan = ProjectScan::default();
    scan_path(&root, 0, &mut scan);
    let name = root
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("Project")
        .to_string();

    Ok(ProjectProbe {
        name,
        root_path: root.display().to_string(),
        datasets: scan.datasets,
        diagnostics: scan.diagnostics,
        scanned_entry_count: scan.scanned_entry_count as u64,
        truncated: scan.truncated,
    })
}

#[uniffi::export]
pub fn build_measurement_set_plot(
    request: MeasurementSetPlotRequest,
) -> FrontendResult<MeasurementSetPlotResult> {
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.is_dir() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!(
                "{} is not a MeasurementSet directory",
                dataset_path.display()
            ),
        });
    }

    let width = request.width.clamp(320, 4096);
    let height = request.height.clamp(240, 4096);
    let max_plot_points = if request.max_plot_points == 0 {
        DEFAULT_GUI_MAX_PLOT_POINTS
    } else {
        request.max_plot_points
    };
    let data_column = normalize_data_column(&request.data_column);
    let mut plot = MsPlotSpec::from_preset(ms_plot_preset(request.preset));
    plot.data_column =
        casa_ms::MsDataColumn::parse(&data_column).map_err(|error| FrontendServiceError::Plot {
            reason: format!("{}: {error}", dataset_path.display()),
        })?;
    plot.averaging.avgchannel = request
        .avgchannel
        .map(usize::try_from)
        .transpose()
        .map_err(|_| FrontendServiceError::Plot {
            reason: "avgchannel exceeds platform usize".to_string(),
        })?;
    plot.averaging.avgtime = request.avgtime;
    plot.averaging.avgscan = request.avgscan;
    plot.averaging.avgfield = request.avgfield;
    plot.averaging.avgbaseline = request.avgbaseline;
    plot.averaging.avgantenna = request.avgantenna;
    plot.averaging.avgspw = request.avgspw;
    plot.averaging.scalar = request.scalar;

    let selection = MsSelectionSpec {
        field: normalized_optional(request.field.clone()),
        spw: normalized_optional(request.spectral_window.clone()),
        timerange: normalized_optional(request.timerange.clone()),
        uvrange: normalized_optional(request.uvrange.clone()),
        antenna: normalized_optional(request.antenna.clone()),
        scan: normalized_optional(request.scan.clone()),
        correlation: normalized_optional(request.correlation.clone()),
        array: normalized_optional(request.array.clone()),
        observation: normalized_optional(request.observation.clone()),
        intent: normalized_optional(request.intent.clone()),
        feed: normalized_optional(request.feed.clone()),
        msselect: normalized_optional(request.msselect.clone()),
        ..MsSelectionSpec::default()
    };
    let spec = MsExploreSpec {
        ms_path: dataset_path.clone(),
        summary_format: MeasurementSetSummaryOutputFormat::Text,
        selection,
        header_items: vec![],
        page_title: None,
        exprange: MsPageExportRange::Current,
        max_plot_points: usize::try_from(max_plot_points).unwrap_or(usize::MAX),
        plots: vec![plot],
    };

    let payload_started = Instant::now();
    let payload =
        build_frontend_msexplore_payload(&spec).map_err(|error| FrontendServiceError::Plot {
            reason: format!("{}: {error}", dataset_path.display()),
        })?;
    let payload_elapsed = payload_started.elapsed();
    let metadata_started = Instant::now();
    let mut metadata = plot_payload_metadata(&payload, request.preset, max_plot_points);
    let metadata_elapsed = metadata_started.elapsed();
    let document_started = Instant::now();
    let document = plot_document_payload(&payload, &metadata, request.preset);
    let document_elapsed = document_started.elapsed();
    metadata.sampling.diagnostics.push(format!(
        "timing: payload={} ms, metadata={} ms, document={} ms",
        payload_elapsed.as_millis(),
        metadata_elapsed.as_millis(),
        document_elapsed.as_millis()
    ));

    Ok(MeasurementSetPlotResult {
        preset: request.preset,
        preset_label: ms_plot_preset(request.preset).display_name().to_string(),
        title: metadata.title,
        summary: metadata.summary,
        dataset_path: dataset_path.display().to_string(),
        data_column,
        selection_summary: selection_summary(&request),
        x_axis: metadata.x_axis,
        y_axis: metadata.y_axis,
        series: metadata.series,
        sampling: metadata.sampling,
        document,
        render: PlotRenderProvenance {
            renderer: "casa-rs plot document".to_string(),
            image_format: "none".to_string(),
            width,
            height,
            source: "Rust casa-ms MeasurementSet payload converted to a structured plot document through UniFFI".to_string(),
        },
        image_bytes: Vec::new(),
    })
}

fn build_frontend_msexplore_payload(spec: &MsExploreSpec) -> Result<MsPlotPayload, String> {
    let [plot] = spec.plots.as_slice() else {
        return build_msexplore_payload_from_spec(spec);
    };
    match plot.preset {
        Some(MsPlotPreset::AntennaLayout)
            if spec.selection.spw.is_none() && spec.selection.correlation.is_none() =>
        {
            let ms = MeasurementSet::open(&spec.ms_path).map_err(|error| error.to_string())?;
            build_antenna_layout_payload_fast(&ms, spec.selection.field.as_deref()).map(|payload| {
                MsPlotPayload::ListObs(MeasurementSetPlotPayload::AntennaLayout(payload))
            })
        }
        Some(MsPlotPreset::ScanTimeline) if spec.selection.correlation.is_none() => {
            let ms = MeasurementSet::open(&spec.ms_path).map_err(|error| error.to_string())?;
            build_scan_timeline_payload_fast(
                &ms,
                spec.selection.field.as_deref(),
                spec.selection.spw.as_deref(),
            )
            .map(|payload| MsPlotPayload::ListObs(MeasurementSetPlotPayload::ScanTimeline(payload)))
        }
        Some(MsPlotPreset::SpectralWindowCoverage)
            if spec.selection.field.is_none() && spec.selection.correlation.is_none() =>
        {
            let ms = MeasurementSet::open(&spec.ms_path).map_err(|error| error.to_string())?;
            build_spectral_window_coverage_payload_fast(&ms, spec.selection.spw.as_deref()).map(
                |payload| {
                    MsPlotPayload::ListObs(MeasurementSetPlotPayload::SpectralWindowCoverage(
                        payload,
                    ))
                },
            )
        }
        _ => build_msexplore_payload_from_spec(spec),
    }
}

fn build_antenna_layout_payload_fast(
    ms: &MeasurementSet,
    field_selection: Option<&str>,
) -> Result<AntennaLayoutPlotPayload, String> {
    let selected_field = field_selection.and_then(parse_single_index_selector);
    let used_antennas = selected_field
        .map(|field| selected_antennas_for_field(ms, field))
        .transpose()?;
    let antenna = ms.antenna().map_err(|error| error.to_string())?;
    let mut points = Vec::new();
    let mut omitted = 0usize;
    for row in 0..antenna.row_count() {
        if used_antennas
            .as_ref()
            .is_some_and(|used| !used.contains(&row))
        {
            continue;
        }
        let position = antenna.position(row).map_err(|error| error.to_string())?;
        let x = position[0];
        let y = position[1];
        if !x.is_finite() || !y.is_finite() {
            omitted += 1;
            continue;
        }
        points.push(AntennaLayoutPoint {
            label: antenna.name(row).map_err(|error| error.to_string())?,
            x,
            y,
            marker_radius: ((antenna.dish_diameter(row).unwrap_or(15.0) / 3.0).round() as i32)
                .clamp(3, 12),
        });
    }
    if points.is_empty() {
        return Err("Antenna layout requires finite ANTENNA positions".to_string());
    }
    let suffix = if omitted == 0 {
        String::new()
    } else {
        format!(" ({} omitted without finite coordinates)", omitted)
    };
    Ok(AntennaLayoutPlotPayload {
        x_label: "ITRF X (m)".to_string(),
        y_label: "ITRF Y (m)".to_string(),
        labels_enabled: true,
        summary: format!("Antenna layout. Antennas={}{}", points.len(), suffix),
        antennas: points,
    })
}

fn selected_antennas_for_field(
    ms: &MeasurementSet,
    field_id: usize,
) -> Result<BTreeSet<usize>, String> {
    let main = ms.main_table();
    let field_column = main_ids::field_id(main);
    let antenna1 = main_ids::antenna1(main);
    let antenna2 = main_ids::antenna2(main);
    let mut antennas = BTreeSet::new();
    for row in 0..main.row_count() {
        if field_column.get(row).map_err(|error| error.to_string())? == field_id as i32 {
            let ant1 = antenna1.get(row).map_err(|error| error.to_string())?;
            let ant2 = antenna2.get(row).map_err(|error| error.to_string())?;
            if ant1 >= 0 {
                antennas.insert(ant1 as usize);
            }
            if ant2 >= 0 {
                antennas.insert(ant2 as usize);
            }
        }
    }
    Ok(antennas)
}

fn build_scan_timeline_payload_fast(
    ms: &MeasurementSet,
    field_selection: Option<&str>,
    spw_selection: Option<&str>,
) -> Result<ScanTimelinePlotPayload, String> {
    #[derive(Default)]
    struct ScanAccum {
        lane: usize,
        start: f64,
        end: f64,
        field_ids: BTreeSet<i32>,
        row_count: usize,
    }

    let selected_field = field_selection.and_then(parse_single_index_selector);
    let selected_spw = spw_selection.and_then(parse_single_spw_selector);
    let dd_to_spw = data_description_to_spw(ms)?;
    let field_names = field_names(ms)?;
    let main = ms.main_table();
    let row_numbers = (0..main.row_count()).collect::<Vec<_>>();
    let mut lane_lookup = BTreeMap::<i32, usize>::new();
    let mut scans = BTreeMap::<i32, ScanAccum>::new();

    for row_chunk in row_numbers.chunks(MAIN_SCALAR_CHUNK_ROWS) {
        let scan_values = selected_i32_values(main, "SCAN_NUMBER", row_chunk)?;
        let field_values = selected_i32_values(main, "FIELD_ID", row_chunk)?;
        let data_desc_values = selected_i32_values(main, "DATA_DESC_ID", row_chunk)?;
        let time_values = selected_f64_values(main, "TIME", row_chunk)?;
        for row_slot in 0..row_chunk.len() {
            let row_field = field_values[row_slot];
            if selected_field.is_some_and(|field| row_field != field as i32) {
                continue;
            }
            let row_dd = data_desc_values[row_slot];
            let row_spw = dd_to_spw.get(&row_dd).copied().unwrap_or(-1);
            if selected_spw.is_some_and(|spw| row_spw != spw as i32) {
                continue;
            }
            let scan = scan_values[row_slot];
            let row_time = time_values[row_slot];
            let next_lane = lane_lookup.len();
            let lane = *lane_lookup.entry(scan).or_insert(next_lane);
            let entry = scans.entry(scan).or_insert_with(|| ScanAccum {
                lane,
                start: row_time,
                end: row_time,
                ..ScanAccum::default()
            });
            entry.start = entry.start.min(row_time);
            entry.end = entry.end.max(row_time);
            entry.field_ids.insert(row_field);
            entry.row_count += 1;
        }
    }

    let mut lane_labels = vec![String::new(); lane_lookup.len()];
    for (scan, lane) in &lane_lookup {
        if let Some(label) = lane_labels.get_mut(*lane) {
            *label = format!("Scan {scan}");
        }
    }
    let mut bars = Vec::new();
    let mut start = f64::INFINITY;
    let mut end = f64::NEG_INFINITY;
    for (scan, entry) in scans {
        start = start.min(entry.start);
        end = end.max(entry.end);
        let field_label = entry
            .field_ids
            .iter()
            .next()
            .and_then(|field| field_names.get(field))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        bars.push(ScanTimelineBar {
            lane: entry.lane,
            start_mjd_seconds: entry.start,
            end_mjd_seconds: entry.end.max(entry.start + 1.0e-6),
            label: format!("Scan {scan}"),
            color_group: field_label,
        });
    }
    if !start.is_finite() || !end.is_finite() {
        start = 0.0;
        end = 1.0;
    }
    Ok(ScanTimelinePlotPayload {
        start_mjd_seconds: start,
        end_mjd_seconds: if end > start { end } else { start + 1.0 },
        bars,
        lane_labels,
        summary: format!("Scan timeline. Scans={}", lane_lookup.len()),
    })
}

fn selected_i32_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<i32>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Int32(value) => Ok(value),
        other => Err(format!(
            "plot service requires INT {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

fn selected_f64_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<f64>, String> {
    selected_scalar_values(table, column, rows, |value| match value {
        ScalarValue::Float64(value) => Ok(value),
        other => Err(format!(
            "plot service requires DOUBLE {column} cells, found {:?}",
            other.primitive_type()
        )),
    })
}

fn selected_scalar_values<T>(
    table: &Table,
    column: &'static str,
    rows: &[usize],
    convert: impl Fn(ScalarValue) -> Result<T, String>,
) -> Result<Vec<T>, String> {
    table
        .column_accessor(column)
        .map_err(|error| error.to_string())?
        .scalar_cells_owned_for_rows(rows)
        .map_err(|error| error.to_string())?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| {
            value
                .ok_or_else(|| format!("plot service requires {column} data for row {row}"))
                .and_then(&convert)
        })
        .collect()
}

fn build_spectral_window_coverage_payload_fast(
    ms: &MeasurementSet,
    spw_selection: Option<&str>,
) -> Result<SpectralWindowCoveragePlotPayload, String> {
    let spectral_windows = ms.spectral_window().map_err(|error| error.to_string())?;
    let selected_spw = spw_selection.and_then(parse_single_spw_selector);
    let mut bars = Vec::new();
    for row in 0..spectral_windows.row_count() {
        if selected_spw.is_some_and(|selected| selected != row) {
            continue;
        }
        let chan_freq = spectral_windows
            .chan_freq(row)
            .map_err(|error| error.to_string())?;
        let chan_width = spectral_windows.chan_width(row).unwrap_or_default();
        let (start, end) = spectral_window_frequency_bounds_ghz(
            &chan_freq,
            &chan_width,
            spectral_windows
                .ref_frequency(row)
                .map_err(|error| error.to_string())?,
            spectral_windows.total_bandwidth(row).unwrap_or(0.0),
        );
        let name = spectral_windows.name(row).unwrap_or_default();
        let label = if name.trim().is_empty() {
            format!("SPW {row}")
        } else {
            format!("SPW {row} {name}")
        };
        bars.push(SpectralWindowCoverageBar {
            spectral_window_id: row,
            lane: bars.len(),
            start,
            end,
            label,
            color_group: format!("spw-{row}"),
        });
    }
    Ok(SpectralWindowCoveragePlotPayload {
        x_label: "Frequency (GHz)".to_string(),
        summary: format!("Spectral window coverage. Windows={}", bars.len()),
        bars,
    })
}

fn parse_single_spw_selector(value: &str) -> Option<usize> {
    parse_single_index_selector(value)
}

fn parse_single_index_selector(value: &str) -> Option<usize> {
    let trimmed = value.trim();
    let without_prefix = trimmed.strip_prefix("spw ").unwrap_or(trimmed);
    without_prefix
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>()
        .parse()
        .ok()
}

fn data_description_to_spw(ms: &MeasurementSet) -> Result<BTreeMap<i32, i32>, String> {
    let data_description = ms.data_description().map_err(|error| error.to_string())?;
    let mut lookup = BTreeMap::new();
    for row in 0..data_description.row_count() {
        lookup.insert(
            row as i32,
            data_description
                .spectral_window_id(row)
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(lookup)
}

fn field_names(ms: &MeasurementSet) -> Result<BTreeMap<i32, String>, String> {
    let fields = ms.field().map_err(|error| error.to_string())?;
    let mut names = BTreeMap::new();
    for row in 0..fields.row_count() {
        names.insert(
            row as i32,
            fields.name(row).map_err(|error| error.to_string())?,
        );
    }
    Ok(names)
}

fn spectral_window_frequency_bounds_ghz(
    chan_freq_hz: &[f64],
    chan_width_hz: &[f64],
    ref_frequency_hz: f64,
    total_bandwidth_hz: f64,
) -> (f64, f64) {
    let mut bounds = None;
    for (index, frequency) in chan_freq_hz.iter().copied().enumerate() {
        let half_width = chan_width_hz
            .get(index)
            .copied()
            .or_else(|| chan_width_hz.first().copied())
            .unwrap_or(0.0)
            .abs()
            / 2.0;
        bounds = bounds_accumulator(bounds, frequency - half_width);
        bounds = bounds_accumulator(bounds, frequency + half_width);
    }
    let (lower, upper) = bounds.unwrap_or_else(|| {
        let half_width = total_bandwidth_hz.abs() / 2.0;
        (ref_frequency_hz - half_width, ref_frequency_hz + half_width)
    });
    (lower / 1.0e9, upper / 1.0e9)
}

#[uniffi::export]
pub fn build_image_explorer_snapshot_json(
    dataset_path: String,
    width: u16,
    height: u16,
    inspector_height: u16,
    plane_pixel_width: u16,
    plane_pixel_height: u16,
    active_view: Option<String>,
) -> FrontendResult<String> {
    let dataset_path = PathBuf::from(dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let viewport = ImageBrowserViewport::with_plane_pixels(
        width.max(20),
        height.max(8),
        inspector_height,
        plane_pixel_width,
        plane_pixel_height,
    );
    let mut session = ImageBrowserSession::open(&dataset_path, viewport).map_err(|error| {
        FrontendServiceError::ImageExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        }
    })?;
    let snapshot = image_snapshot_for_requested_view(&mut session, active_view.as_deref())
        .map_err(|error| FrontendServiceError::ImageExplorer {
            reason: format!("snapshot {}: {error}", dataset_path.display()),
        })?;
    serde_json::to_string(&snapshot).map_err(|error| FrontendServiceError::ImageExplorer {
        reason: format!("encode snapshot {}: {error}", dataset_path.display()),
    })
}

#[uniffi::export]
pub fn build_image_explorer_snapshot_from_request_json(
    request_json: String,
) -> FrontendResult<String> {
    let request: ImageExplorerSnapshotRequest =
        serde_json::from_str(&request_json).map_err(|error| {
            FrontendServiceError::ImageExplorer {
                reason: format!("decode image explorer snapshot request: {error}"),
            }
        })?;
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let viewport = ImageBrowserViewport::with_plane_pixels(
        request.width.max(20),
        request.height.max(8),
        request.inspector_height,
        request.plane_pixel_width,
        request.plane_pixel_height,
    );
    let mut session = ImageBrowserSession::open_with_parameters(
        &dataset_path,
        viewport,
        Some(&request.parameters),
    )
    .map_err(|error| FrontendServiceError::ImageExplorer {
        reason: format!("open {}: {error}", dataset_path.display()),
    })?;
    apply_image_explorer_snapshot_request(&mut session, &request).map_err(|error| {
        FrontendServiceError::ImageExplorer {
            reason: format!("snapshot {}: {error}", dataset_path.display()),
        }
    })?;
    let mode = parse_image_plane_content_mode(request.plane_content_mode.as_deref())
        .map_err(|reason| FrontendServiceError::ImageExplorer { reason })?;
    let snapshot = if !request.non_display_indices.is_empty() {
        session
            .preview_occurrence(&ImageBrowserPreviewRequest {
                viewport,
                parameters: request.parameters.clone(),
                plane_content_mode: mode,
                non_display_indices: request
                    .non_display_indices
                    .iter()
                    .copied()
                    .map(|index| index as usize)
                    .collect(),
                include_profile: request.include_profile,
            })
            .map(|payload| *payload.snapshot)
    } else if request.include_profile {
        session.snapshot()
    } else {
        session.handle_command(ImageBrowserCommand::GetSnapshot)
    }
    .map_err(|error| FrontendServiceError::ImageExplorer {
        reason: format!("snapshot {}: {error}", dataset_path.display()),
    })?;
    serde_json::to_string(&snapshot).map_err(|error| FrontendServiceError::ImageExplorer {
        reason: format!("encode snapshot {}: {error}", dataset_path.display()),
    })
}

#[uniffi::export]
pub fn build_table_browser_snapshot_json(
    dataset_path: String,
    width: u16,
    height: u16,
    inspector_height: u16,
    view: Option<String>,
) -> FrontendResult<String> {
    let dataset_path = PathBuf::from(dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let mut browser =
        TableBrowser::open(&dataset_path).map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        })?;
    let viewport =
        BrowserViewport::with_inspector_height(width.max(20), height.max(8), inspector_height);
    if let Some(view) = view.as_deref() {
        browser.set_view(
            parse_table_browser_view(view)
                .map_err(|reason| FrontendServiceError::TableExplorer { reason })?,
        );
    }
    browser
        .apply(casars_tablebrowser_protocol::BrowserCommand::GetSnapshot {
            viewport: Some(viewport),
        })
        .map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("snapshot {}: {error}", dataset_path.display()),
        })
        .and_then(|snapshot| {
            serde_json::to_string(&snapshot).map_err(|error| FrontendServiceError::TableExplorer {
                reason: format!("encode snapshot {}: {error}", dataset_path.display()),
            })
        })
}

#[uniffi::export]
pub fn build_table_browser_snapshot_from_request_json(
    request_json: String,
) -> FrontendResult<String> {
    let request: TableBrowserSnapshotRequest =
        serde_json::from_str(&request_json).map_err(|error| {
            FrontendServiceError::TableExplorer {
                reason: format!("decode table browser snapshot request: {error}"),
            }
        })?;
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let mut browser =
        TableBrowser::open(&dataset_path).map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        })?;
    let viewport = BrowserViewport::with_inspector_height(
        request.width.max(20),
        request.height.max(8),
        request.inspector_height,
    );
    if let Some(view) = request.selected_view.as_deref() {
        browser.set_view(
            parse_table_browser_view(view)
                .map_err(|reason| FrontendServiceError::TableExplorer { reason })?,
        );
    }
    if let Some(focus) = parse_table_browser_focus(request.focus.as_deref())
        .map_err(|reason| FrontendServiceError::TableExplorer { reason })?
    {
        browser
            .apply(BrowserCommand::SetFocus {
                focus,
                viewport: Some(viewport),
            })
            .map_err(|error| FrontendServiceError::TableExplorer {
                reason: format!("set focus {}: {error}", dataset_path.display()),
            })?;
    }
    for command in request
        .commands
        .iter()
        .chain(request.transient_commands.iter())
        .cloned()
    {
        browser
            .apply(command)
            .map_err(|error| FrontendServiceError::TableExplorer {
                reason: format!("apply command {}: {error}", dataset_path.display()),
            })?;
    }
    browser
        .apply(BrowserCommand::GetSnapshot {
            viewport: Some(viewport),
        })
        .map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("snapshot {}: {error}", dataset_path.display()),
        })
        .and_then(|snapshot| {
            serde_json::to_string(&snapshot).map_err(|error| FrontendServiceError::TableExplorer {
                reason: format!("encode snapshot {}: {error}", dataset_path.display()),
            })
        })
}

#[uniffi::export]
pub fn build_table_browser_cell_window_json(request_json: String) -> FrontendResult<String> {
    let request: TableBrowserCellWindowRequest =
        serde_json::from_str(&request_json).map_err(|error| {
            FrontendServiceError::TableExplorer {
                reason: format!("decode table browser cell window request: {error}"),
            }
        })?;
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let table = Table::open(TableOptions::new(&dataset_path)).map_err(|error| {
        FrontendServiceError::TableExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        }
    })?;
    let snapshot = build_table_browser_cell_window(&table, &dataset_path, &request)?;
    serde_json::to_string(&snapshot).map_err(|error| FrontendServiceError::TableExplorer {
        reason: format!("encode cell window {}: {error}", dataset_path.display()),
    })
}

#[uniffi::export]
pub fn build_table_browser_cell_value_json(request_json: String) -> FrontendResult<String> {
    let request: TableBrowserCellValueRequest =
        serde_json::from_str(&request_json).map_err(|error| {
            FrontendServiceError::TableExplorer {
                reason: format!("decode table browser cell value request: {error}"),
            }
        })?;
    let dataset_path = PathBuf::from(&request.dataset_path);
    if !dataset_path.exists() {
        return Err(FrontendServiceError::InvalidPath {
            reason: format!("{} does not exist", dataset_path.display()),
        });
    }
    let table = Table::open(TableOptions::new(&dataset_path)).map_err(|error| {
        FrontendServiceError::TableExplorer {
            reason: format!("open {}: {error}", dataset_path.display()),
        }
    })?;
    let column_names = table_browser_cell_column_names(&table)?;
    let column_name = column_names
        .get(request.column_index as usize)
        .ok_or_else(|| FrontendServiceError::TableExplorer {
            reason: format!(
                "column index {} is outside {} columns",
                request.column_index,
                column_names.len()
            ),
        })?;
    let row_index = request.row_index as usize;
    if row_index >= table.row_count() {
        return Err(FrontendServiceError::TableExplorer {
            reason: format!(
                "row index {} is outside {} rows",
                row_index,
                table.row_count()
            ),
        });
    }
    let value = table
        .cell_accessor(row_index, column_name)
        .and_then(|accessor| accessor.value())
        .map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!(
                "read {} row {} column {}: {error}",
                dataset_path.display(),
                row_index,
                column_name
            ),
        })?;
    serde_json::to_string(&format_table_browser_copy_value(value)).map_err(|error| {
        FrontendServiceError::TableExplorer {
            reason: format!("encode cell value {}: {error}", dataset_path.display()),
        }
    })
}

fn build_table_browser_cell_window(
    table: &Table,
    table_path: &Path,
    request: &TableBrowserCellWindowRequest,
) -> FrontendResult<TableBrowserCellWindowSnapshot> {
    let column_names = table_browser_cell_column_names(table)?;
    let columns = column_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let header = table_browser_cell_column_header(table, name);
            let width = table_browser_cell_column_width(&header);
            TableBrowserCellWindowColumn {
                index: index as u64,
                name: name.clone(),
                header,
                summary: table_browser_cell_column_summary(table, name),
                width: width as u64,
                keywords: table_browser_column_keyword_lines(table, name),
            }
        })
        .collect::<Vec<_>>();
    let column_display_options = request
        .column_options
        .iter()
        .map(|option| (option.column_index, option.array_inline_limit as usize))
        .collect::<BTreeMap<_, _>>();

    let row_count = table.row_count();
    let row_start = (request.row_start as usize).min(row_count);
    let row_limit = request.row_limit.clamp(1, 4096) as usize;
    let row_end = row_start.saturating_add(row_limit).min(row_count);
    let column_count = columns.len();
    let column_start = (request.column_start as usize).min(column_count);
    let column_limit = request.column_limit.clamp(1, 128) as usize;
    let column_end = column_start.saturating_add(column_limit).min(column_count);
    let visible_columns = &columns[column_start..column_end];
    let mut rows = Vec::with_capacity(row_end.saturating_sub(row_start));
    for row_index in row_start..row_end {
        let mut cells = Vec::with_capacity(visible_columns.len());
        for column in visible_columns {
            let array_inline_limit = column_display_options
                .get(&column.index)
                .copied()
                .unwrap_or(0);
            let (display, defined) =
                table_browser_cell_display(table, row_index, &column.name, array_inline_limit)
                    .map_err(|error| FrontendServiceError::TableExplorer {
                        reason: format!(
                            "read {} row {} column {}: {error}",
                            table_path.display(),
                            row_index,
                            column.name
                        ),
                    })?;
            cells.push(TableBrowserCellWindowCell {
                column_index: column.index,
                display,
                defined,
            });
        }
        rows.push(TableBrowserCellWindowRow {
            index: row_index as u64,
            cells,
        });
    }

    Ok(TableBrowserCellWindowSnapshot {
        table_path: table_path.display().to_string(),
        row_count: row_count as u64,
        column_count: column_count as u64,
        row_start: row_start as u64,
        column_start: column_start as u64,
        columns,
        rows,
    })
}

fn table_browser_cell_column_names(table: &Table) -> FrontendResult<Vec<String>> {
    if let Some(schema) = table.schema() {
        return Ok(schema
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect());
    }

    let mut names = BTreeSet::new();
    for row in table
        .rows()
        .map_err(|error| FrontendServiceError::TableExplorer {
            reason: format!("read schema-free table rows: {error}"),
        })?
    {
        for field in row.fields() {
            names.insert(field.name.clone());
        }
    }
    Ok(names.into_iter().collect())
}

fn table_browser_cell_column_header(table: &Table, name: &str) -> String {
    let Some(schema) = table.schema().and_then(|schema| schema.column(name)) else {
        return name.to_string();
    };
    format!("{name} {}", table_browser_cell_column_type_label(schema))
}

fn table_browser_cell_column_summary(table: &Table, name: &str) -> String {
    table
        .schema()
        .and_then(|schema| schema.column(name))
        .map(table_browser_cell_column_summary_label)
        .unwrap_or_else(|| "Dynamic".to_string())
}

fn table_browser_cell_column_width(header: &str) -> usize {
    header.chars().count().clamp(8, 32)
}

fn table_browser_cell_display(
    table: &Table,
    row_index: usize,
    name: &str,
    array_inline_limit: usize,
) -> Result<(String, bool), casa_tables::TableError> {
    if let Some(column) = table.schema().and_then(|schema| schema.column(name)) {
        match column.column_type() {
            ColumnType::Array(_) => {
                if array_inline_limit > 0
                    && let Some(display) = table_browser_expanded_array_display(
                        table,
                        row_index,
                        name,
                        column,
                        array_inline_limit,
                    )?
                {
                    return Ok((display, true));
                }
                return Ok((format_table_browser_schema_array(column), true));
            }
            ColumnType::Record => {
                return Ok(("record".to_string(), true));
            }
            ColumnType::Scalar => {}
        }
    }

    let value = table.cell_accessor(row_index, name)?.value()?;
    Ok((format_table_browser_cell_value(value), value.is_some()))
}

fn table_browser_expanded_array_display(
    table: &Table,
    row_index: usize,
    name: &str,
    column: &casa_tables::ColumnSchema,
    array_inline_limit: usize,
) -> Result<Option<String>, casa_tables::TableError> {
    let fixed_size = match column.column_type() {
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
            Some(shape.iter().copied().product::<usize>())
        }
        ColumnType::Array(ArrayShapeContract::Variable { .. }) => None,
        ColumnType::Scalar | ColumnType::Record => return Ok(None),
    };
    if let Some(size) = fixed_size
        && size > array_inline_limit
    {
        return Ok(None);
    }
    let Some(value) = table.cell_accessor(row_index, name)?.value()? else {
        return Ok(Some("<undef>".to_string()));
    };
    let Value::Array(array) = value else {
        return Ok(None);
    };
    if array.len() > array_inline_limit {
        return Ok(Some(table_browser_array_type_label(array)));
    }
    Ok(Some(format_table_browser_array_expanded(array)))
}

fn format_table_browser_schema_array(column: &casa_tables::ColumnSchema) -> String {
    let primitive = column
        .data_type()
        .map(table_browser_short_primitive_name)
        .unwrap_or("dyn");
    match column.column_type() {
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
            format!("{primitive}[{}]", table_browser_shape_label(shape))
        }
        ColumnType::Array(ArrayShapeContract::Variable { ndim: Some(ndim) }) => {
            format!("{primitive}[{ndim}d]")
        }
        ColumnType::Array(ArrayShapeContract::Variable { ndim: None }) => {
            format!("{primitive}[]")
        }
        ColumnType::Scalar | ColumnType::Record => primitive.to_string(),
    }
}

fn table_browser_shape_label(shape: &[usize]) -> String {
    if shape.is_empty() {
        return "scalar".to_string();
    }
    shape
        .iter()
        .map(|extent| extent.to_string())
        .collect::<Vec<_>>()
        .join("x")
}

fn table_browser_column_keyword_lines(table: &Table, name: &str) -> Vec<String> {
    let Some(record) = table.column_keywords(name) else {
        return Vec::new();
    };
    let mut lines = Vec::new();
    push_table_browser_keyword_lines(record, &[], &mut lines);
    lines
}

fn push_table_browser_keyword_lines(
    record: &casa_types::RecordValue,
    prefix: &[String],
    lines: &mut Vec<String>,
) {
    for field in record.fields() {
        if matches!(field.value, Value::TableRef(_)) {
            continue;
        }
        let mut path = prefix.to_vec();
        path.push(field.name.clone());
        match &field.value {
            Value::Record(record) => push_table_browser_keyword_lines(record, &path, lines),
            value => lines.push(format!(
                "{} = {}",
                path.join("."),
                format_table_browser_cell_value(Some(value))
            )),
        }
    }
}

fn table_browser_cell_column_type_label(column: &casa_tables::ColumnSchema) -> String {
    match column.column_type() {
        ColumnType::Scalar => column
            .data_type()
            .map(table_browser_short_primitive_name)
            .unwrap_or("dyn")
            .to_string(),
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => format!(
            "{}[{}]",
            column
                .data_type()
                .map(table_browser_short_primitive_name)
                .unwrap_or("dyn"),
            shape
                .iter()
                .map(|extent| extent.to_string())
                .collect::<Vec<_>>()
                .join("x")
        ),
        ColumnType::Array(ArrayShapeContract::Variable { ndim: Some(ndim) }) => format!(
            "{}[{}d]",
            column
                .data_type()
                .map(table_browser_short_primitive_name)
                .unwrap_or("dyn"),
            ndim
        ),
        ColumnType::Array(ArrayShapeContract::Variable { ndim: None }) => format!(
            "{}[]",
            column
                .data_type()
                .map(table_browser_short_primitive_name)
                .unwrap_or("dyn")
        ),
        ColumnType::Record => "record".to_string(),
    }
}

fn table_browser_cell_column_summary_label(column: &casa_tables::ColumnSchema) -> String {
    match column.column_type() {
        ColumnType::Scalar => format!(
            "Scalar {:?}",
            column
                .data_type()
                .expect("scalar columns always carry a data type")
        ),
        ColumnType::Array(contract) => format!(
            "Array<{:?}> {:?}",
            column
                .data_type()
                .expect("array columns always carry a data type"),
            contract
        ),
        ColumnType::Record => "Record".to_string(),
    }
}

fn table_browser_short_primitive_name(primitive: PrimitiveType) -> &'static str {
    match primitive {
        PrimitiveType::Bool => "bool",
        PrimitiveType::UInt8 => "u8",
        PrimitiveType::UInt16 => "u16",
        PrimitiveType::UInt32 => "u32",
        PrimitiveType::Int16 => "i16",
        PrimitiveType::Int32 => "i32",
        PrimitiveType::Int64 => "i64",
        PrimitiveType::Float32 => "f32",
        PrimitiveType::Float64 => "f64",
        PrimitiveType::Complex32 => "c32",
        PrimitiveType::Complex64 => "c64",
        PrimitiveType::String => "str",
    }
}

fn format_table_browser_cell_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::Scalar(scalar)) => format_table_browser_scalar(scalar),
        Some(Value::Array(array)) => format_table_browser_array(array),
        Some(Value::Record(record)) => format!("record{{{}}}", record.fields().len()),
        Some(Value::TableRef(path)) => format!("table({path})"),
        None => "<undef>".to_string(),
    }
}

fn format_table_browser_copy_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::Array(array)) => table_browser_array_all_values(array),
        other => format_table_browser_cell_value(other),
    }
}

fn format_table_browser_scalar(scalar: &ScalarValue) -> String {
    match scalar {
        ScalarValue::Bool(value) => value.to_string(),
        ScalarValue::UInt8(value) => value.to_string(),
        ScalarValue::UInt16(value) => value.to_string(),
        ScalarValue::UInt32(value) => value.to_string(),
        ScalarValue::Int16(value) => value.to_string(),
        ScalarValue::Int32(value) => value.to_string(),
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::Float32(value) => format!("{value:.6}"),
        ScalarValue::Float64(value) => format!("{value:.6}"),
        ScalarValue::Complex32(value) => format!("{:.4}+{:.4}i", value.re, value.im),
        ScalarValue::Complex64(value) => format!("{:.4}+{:.4}i", value.re, value.im),
        ScalarValue::String(value) => format!("{value:?}"),
    }
}

fn format_table_browser_array(array: &ArrayValue) -> String {
    if array.ndim() <= 1 && array.len() <= 3 {
        return table_browser_array_preview(array);
    }
    format!(
        "{} {}",
        table_browser_array_type_label(array),
        table_browser_array_preview(array)
    )
}

fn format_table_browser_array_expanded(array: &ArrayValue) -> String {
    table_browser_array_all_values(array)
}

fn table_browser_array_type_label(array: &ArrayValue) -> String {
    format!(
        "{}[{}]",
        table_browser_short_primitive_name(array.primitive_type()),
        table_browser_shape_label(array.shape())
    )
}

fn table_browser_array_all_values(array: &ArrayValue) -> String {
    match array {
        ArrayValue::Bool(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt8(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt16(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt32(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int16(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int32(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int64(values) => {
            table_browser_all_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Float32(values) => {
            table_browser_all_values(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Float64(values) => {
            table_browser_all_values(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Complex32(values) => table_browser_all_values(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::Complex64(values) => table_browser_all_values(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::String(values) => {
            table_browser_all_values(values.iter().map(|value| format!("{value:?}")))
        }
    }
}

fn table_browser_array_preview(array: &ArrayValue) -> String {
    match array {
        ArrayValue::Bool(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt8(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt16(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::UInt32(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int16(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int32(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Int64(values) => {
            table_browser_preview_values(values.iter().map(|value| value.to_string()))
        }
        ArrayValue::Float32(values) => {
            table_browser_preview_values(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Float64(values) => {
            table_browser_preview_values(values.iter().map(|value| format!("{value:.4}")))
        }
        ArrayValue::Complex32(values) => table_browser_preview_values(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::Complex64(values) => table_browser_preview_values(
            values
                .iter()
                .map(|value| format!("{:.3}+{:.3}i", value.re, value.im)),
        ),
        ArrayValue::String(values) => {
            table_browser_preview_values(values.iter().map(|value| format!("{value:?}")))
        }
    }
}

fn table_browser_all_values(values: impl Iterator<Item = String>) -> String {
    format!("[{}]", values.collect::<Vec<_>>().join(", "))
}

fn table_browser_preview_values(values: impl Iterator<Item = String>) -> String {
    let mut preview = Vec::new();
    for value in values.take(4) {
        preview.push(value);
    }
    if preview.is_empty() {
        "[]".to_string()
    } else {
        format!(
            "[{}{}]",
            preview.join(", "),
            if preview.len() == 4 { ", ..." } else { "" }
        )
    }
}

fn image_snapshot_for_requested_view(
    session: &mut ImageBrowserSession,
    active_view: Option<&str>,
) -> Result<casars_imagebrowser_protocol::ImageBrowserSnapshot, casa_images::ImageError> {
    let Some(active_view) = active_view else {
        return session.snapshot();
    };
    let target = active_view.trim();
    if target.is_empty() {
        return session.snapshot();
    }
    for _ in 0..4 {
        let snapshot = session.snapshot()?;
        if snapshot.active_view.label().eq_ignore_ascii_case(target)
            || format!("{:?}", snapshot.active_view).eq_ignore_ascii_case(target)
            || serde_plain_view_name(snapshot.active_view.label()).eq_ignore_ascii_case(target)
        {
            return Ok(snapshot);
        }
        session.handle_command(
            casars_imagebrowser_protocol::ImageBrowserCommand::CycleView { forward: true },
        )?;
    }
    session.snapshot()
}

fn apply_image_explorer_snapshot_request(
    session: &mut ImageBrowserSession,
    request: &ImageExplorerSnapshotRequest,
) -> Result<(), casa_images::ImageError> {
    let mode = parse_image_plane_content_mode(request.plane_content_mode.as_deref())
        .map_err(casa_images::ImageError::InvalidMetadata)?;
    session.handle_command(ImageBrowserCommand::SetPlaneContentMode { mode })?;
    if let Some(focus) = parse_image_browser_focus(request.focus.as_deref())
        .map_err(casa_images::ImageError::InvalidMetadata)?
    {
        session.handle_command(ImageBrowserCommand::SetFocus { focus })?;
    }
    if let (Some(x), Some(y)) = (request.cursor_x, request.cursor_y) {
        session.handle_command(ImageBrowserCommand::SetCursor {
            x: x as usize,
            y: y as usize,
        })?;
    }
    if let Some(axis) = request.selected_profile_axis {
        session.handle_command(ImageBrowserCommand::SetSelectedNonDisplayAxis {
            axis: axis as usize,
        })?;
    }
    for command in request
        .commands
        .iter()
        .chain(request.transient_commands.iter())
        .cloned()
    {
        session.handle_command(command)?;
    }
    let _ = image_snapshot_for_requested_view(session, request.active_view.as_deref())?;
    Ok(())
}

fn parse_image_browser_focus(value: Option<&str>) -> Result<Option<ImageBrowserFocus>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" => Ok(None),
        "content" => Ok(Some(ImageBrowserFocus::Content)),
        "inspector" => Ok(Some(ImageBrowserFocus::Inspector)),
        other => Err(format!("unknown image browser focus {other:?}")),
    }
}

fn parse_image_plane_content_mode(value: Option<&str>) -> Result<ImagePlaneContentMode, String> {
    let Some(value) = value else {
        return Ok(ImagePlaneContentMode::Raster);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "raster" => Ok(ImagePlaneContentMode::Raster),
        "spreadsheet" => Ok(ImagePlaneContentMode::Spreadsheet),
        other => Err(format!("unknown image plane content mode {other:?}")),
    }
}

fn serde_plain_view_name(label: &str) -> String {
    label.replace(' ', "_").to_ascii_lowercase()
}

fn parse_table_browser_view(value: &str) -> Result<BrowserView, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "overview" => Ok(BrowserView::Overview),
        "columns" => Ok(BrowserView::Columns),
        "keywords" => Ok(BrowserView::Keywords),
        "cells" => Ok(BrowserView::Cells),
        "subtables" => Ok(BrowserView::Subtables),
        other => Err(format!("unknown table browser view {other:?}")),
    }
}

fn parse_table_browser_focus(value: Option<&str>) -> Result<Option<BrowserFocus>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" => Ok(None),
        "main" => Ok(Some(BrowserFocus::Main)),
        "inspector" => Ok(Some(BrowserFocus::Inspector)),
        other => Err(format!("unknown table browser focus {other:?}")),
    }
}

struct PayloadMetadata {
    title: String,
    summary: String,
    x_axis: PlotAxisMetadata,
    y_axis: PlotAxisMetadata,
    series: Vec<PlotSeriesMetadata>,
    sampling: PlotSamplingDiagnostics,
}

fn ms_plot_preset(preset: MeasurementSetPlotPreset) -> MsPlotPreset {
    match preset {
        MeasurementSetPlotPreset::UvCoverage => MsPlotPreset::UvCoverage,
        MeasurementSetPlotPreset::AntennaLayout => MsPlotPreset::AntennaLayout,
        MeasurementSetPlotPreset::ScanTimeline => MsPlotPreset::ScanTimeline,
        MeasurementSetPlotPreset::SpectralWindowCoverage => MsPlotPreset::SpectralWindowCoverage,
        MeasurementSetPlotPreset::PhaseVsTime => MsPlotPreset::PhaseVsTime,
        MeasurementSetPlotPreset::AmplitudePhaseVsTimeStacked => {
            MsPlotPreset::AmplitudePhaseVsTimeStacked
        }
        MeasurementSetPlotPreset::WeightVsTime => MsPlotPreset::WeightVsTime,
        MeasurementSetPlotPreset::SigmaVsTime => MsPlotPreset::SigmaVsTime,
        MeasurementSetPlotPreset::FlagVsTime => MsPlotPreset::FlagVsTime,
        MeasurementSetPlotPreset::WeightSpectrumVsTime => MsPlotPreset::WeightSpectrumVsTime,
        MeasurementSetPlotPreset::SigmaSpectrumVsTime => MsPlotPreset::SigmaSpectrumVsTime,
        MeasurementSetPlotPreset::FlagRowVsTime => MsPlotPreset::FlagRowVsTime,
        MeasurementSetPlotPreset::ElevationVsTime => MsPlotPreset::ElevationVsTime,
        MeasurementSetPlotPreset::AzimuthVsTime => MsPlotPreset::AzimuthVsTime,
        MeasurementSetPlotPreset::HourAngleVsTime => MsPlotPreset::HourAngleVsTime,
        MeasurementSetPlotPreset::ParallacticAngleVsTime => MsPlotPreset::ParallacticAngleVsTime,
        MeasurementSetPlotPreset::AzimuthVsElevation => MsPlotPreset::AzimuthVsElevation,
        MeasurementSetPlotPreset::AmplitudeVsFrequency => MsPlotPreset::AmplitudeVsFrequency,
        MeasurementSetPlotPreset::AmplitudeVsChannel => MsPlotPreset::AmplitudeVsChannel,
        MeasurementSetPlotPreset::PhaseVsChannel => MsPlotPreset::PhaseVsChannel,
        MeasurementSetPlotPreset::PhaseVsFrequency => MsPlotPreset::PhaseVsFrequency,
        MeasurementSetPlotPreset::AmplitudeVsVelocity => MsPlotPreset::AmplitudeVsVelocity,
        MeasurementSetPlotPreset::PhaseVsVelocity => MsPlotPreset::PhaseVsVelocity,
        MeasurementSetPlotPreset::AmplitudeVsUvDistance => MsPlotPreset::AmplitudeVsUvDistance,
        MeasurementSetPlotPreset::AmplitudeVsTime => MsPlotPreset::AmplitudeVsTime,
        MeasurementSetPlotPreset::RealVsImaginary => MsPlotPreset::RealVsImaginary,
    }
}

fn plot_payload_metadata(
    payload: &MsPlotPayload,
    preset: MeasurementSetPlotPreset,
    requested_max_points: u64,
) -> PayloadMetadata {
    match payload {
        MsPlotPayload::Scatter(payload) => scatter_metadata(payload, requested_max_points),
        MsPlotPayload::ScatterGrid(payload) => scatter_grid_metadata(payload, requested_max_points),
        MsPlotPayload::ScatterPage(payload) => scatter_page_metadata(payload, requested_max_points),
        MsPlotPayload::ListObs(payload) => listobs_metadata(payload, preset, requested_max_points),
    }
}

fn plot_document_payload(
    payload: &MsPlotPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    match payload {
        MsPlotPayload::Scatter(payload) => scatter_plot_document(payload, metadata, preset),
        MsPlotPayload::ScatterGrid(payload) => {
            scatter_grid_plot_document(payload, metadata, preset)
        }
        MsPlotPayload::ScatterPage(payload) => {
            scatter_page_plot_document(payload, metadata, preset)
        }
        MsPlotPayload::ListObs(payload) => listobs_plot_document(payload, metadata, preset),
    }
}

fn listobs_plot_document(
    payload: &MeasurementSetPlotPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    match payload {
        MeasurementSetPlotPayload::UvCoverage(payload) => {
            let axis_extent = payload.axis_extent_lambda / 1_000.0;
            let axes = vec![
                document_axis(
                    "u",
                    "u",
                    "kλ",
                    -axis_extent,
                    axis_extent,
                    PlotAxisScale::Linear,
                ),
                document_axis(
                    "v",
                    "v",
                    "kλ",
                    -axis_extent,
                    axis_extent,
                    PlotAxisScale::Linear,
                ),
            ];
            let mut grouped_layers = BTreeMap::<String, (Vec<f64>, Vec<f64>, usize)>::new();
            for track in &payload.tracks {
                let group = uv_coverage_group_label(&track.label);
                let entry = grouped_layers.entry(group).or_default();
                entry.2 += track.points.len();
                for (u_lambda, v_lambda) in &track.points {
                    entry.0.push(*u_lambda / 1_000.0);
                    entry.1.push(*v_lambda / 1_000.0);
                    if payload.mirror {
                        entry.0.push(-*u_lambda / 1_000.0);
                        entry.1.push(-*v_lambda / 1_000.0);
                    }
                }
            }
            let layers = grouped_layers
                .into_iter()
                .enumerate()
                .map(|(index, (group, (x_values, y_values, source_count)))| {
                    point_layer(PointLayerSpec {
                        id: format!("uv-coverage-{index}"),
                        title: group.clone(),
                        x_axis_id: "u",
                        y_axis_id: "v",
                        x_values,
                        y_values,
                        point_labels: Vec::new(),
                        point_symbol_sizes: Vec::new(),
                        provenance: Vec::new(),
                        color_group: group,
                        symbol_size: 2.5,
                        line_width: 1.0,
                        provenance_summary: format!(
                            "UV coverage samples from Rust msexplore payload, coalesced from {source_count} samples"
                        ),
                    })
                })
                .collect();
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                layers,
                Vec::new(),
                Vec::new(),
            )
        }
        MeasurementSetPlotPayload::VisibilityScatter(payload) => {
            let axes = axes_for_ranges(
                "x",
                &payload.x_label,
                "y",
                &payload.y_label,
                None,
                payload.fixed_y_bounds,
                payload
                    .series
                    .iter()
                    .flat_map(|series| series.points.iter().copied()),
            );
            let layers = payload
                .series
                .iter()
                .enumerate()
                .map(|(index, series)| {
                    let (x_values, y_values): (Vec<_>, Vec<_>) =
                        series.points.iter().copied().unzip();
                    point_layer(PointLayerSpec {
                        id: format!("visibility-series-{index}"),
                        title: series.label.clone(),
                        x_axis_id: "x",
                        y_axis_id: "y",
                        x_values,
                        y_values,
                        point_labels: Vec::new(),
                        point_symbol_sizes: Vec::new(),
                        provenance: Vec::new(),
                        color_group: series.color_group.clone(),
                        symbol_size: 2.5,
                        line_width: 1.0,
                        provenance_summary: "Visibility scatter points from Rust msexplore payload"
                            .to_string(),
                    })
                })
                .collect();
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                layers,
                Vec::new(),
                Vec::new(),
            )
        }
        MeasurementSetPlotPayload::AntennaLayout(payload) => {
            let x_values = payload
                .antennas
                .iter()
                .map(|antenna| antenna.x)
                .collect::<Vec<_>>();
            let y_values = payload
                .antennas
                .iter()
                .map(|antenna| antenna.y)
                .collect::<Vec<_>>();
            let point_labels = if payload.labels_enabled {
                payload
                    .antennas
                    .iter()
                    .map(|antenna| antenna.label.clone())
                    .collect()
            } else {
                Vec::new()
            };
            let point_symbol_sizes = payload
                .antennas
                .iter()
                .map(|antenna| f64::from(antenna.marker_radius.max(1)) * 2.0)
                .collect::<Vec<_>>();
            let axes = axes_for_ranges(
                "x",
                &payload.x_label,
                "y",
                &payload.y_label,
                None,
                None,
                x_values.iter().copied().zip(y_values.iter().copied()),
            );
            let layer = point_layer(PointLayerSpec {
                id: "antennas".to_string(),
                title: "Antennas".to_string(),
                x_axis_id: "x",
                y_axis_id: "y",
                x_values,
                y_values,
                point_labels,
                point_symbol_sizes,
                provenance: Vec::new(),
                color_group: "antenna".to_string(),
                symbol_size: 4.0,
                line_width: 1.0,
                provenance_summary: "ANTENNA table positions from Rust msexplore payload"
                    .to_string(),
            });
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                vec![layer],
                Vec::new(),
                Vec::new(),
            )
        }
        MeasurementSetPlotPayload::ScanTimeline(payload) => {
            let axes = vec![
                document_axis(
                    "time",
                    "Time",
                    "MJD seconds",
                    payload.start_mjd_seconds,
                    payload.end_mjd_seconds,
                    PlotAxisScale::Linear,
                ),
                document_lane_axis("scan", "Scan", payload.lane_labels.clone()),
            ];
            let layers = payload
                .bars
                .iter()
                .enumerate()
                .map(|(index, bar)| {
                    interval_layer(IntervalLayerSpec {
                        id: format!("scan-bar-{index}"),
                        title: nonempty_or(&bar.label, "Scan").to_string(),
                        x_axis_id: "time",
                        y_axis_id: "scan",
                        interval_x_start: vec![bar.start_mjd_seconds],
                        interval_x_end: vec![bar.end_mjd_seconds],
                        interval_y: vec![bar.lane as f64],
                        interval_height: vec![0.72],
                        interval_labels: vec![bar.label.clone()],
                        color_group: bar.color_group.clone(),
                        provenance_summary: "MAIN-table scan interval from Rust msexplore payload"
                            .to_string(),
                    })
                })
                .collect();
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                layers,
                Vec::new(),
                Vec::new(),
            )
        }
        MeasurementSetPlotPayload::SpectralWindowCoverage(payload) => {
            let lane_labels = payload
                .bars
                .iter()
                .map(|bar| format!("spw {}", bar.spectral_window_id))
                .collect::<Vec<_>>();
            let x_bounds = payload
                .bars
                .iter()
                .flat_map(|bar| [bar.start, bar.end])
                .fold(None, bounds_accumulator);
            let (lower, upper) = expanded_bounds(x_bounds.unwrap_or((0.0, 1.0)));
            let axes = vec![
                document_axis(
                    "frequency",
                    &payload.x_label,
                    label_unit(&payload.x_label).as_str(),
                    lower,
                    upper,
                    PlotAxisScale::Linear,
                ),
                document_lane_axis("spw", "Spectral window", lane_labels),
            ];
            let layers = payload
                .bars
                .iter()
                .map(|bar| {
                    interval_layer(IntervalLayerSpec {
                        id: format!("spw-bar-{}", bar.spectral_window_id),
                        title: nonempty_or(&bar.label, "Spectral window").to_string(),
                        x_axis_id: "frequency",
                        y_axis_id: "spw",
                        interval_x_start: vec![bar.start],
                        interval_x_end: vec![bar.end],
                        interval_y: vec![bar.lane as f64],
                        interval_height: vec![0.72],
                        interval_labels: vec![bar.label.clone()],
                        color_group: bar.color_group.clone(),
                        provenance_summary:
                            "SPECTRAL_WINDOW coverage interval from Rust msexplore payload"
                                .to_string(),
                    })
                })
                .collect();
            base_document(
                preset,
                metadata,
                Vec::new(),
                axes,
                layers,
                Vec::new(),
                Vec::new(),
            )
        }
    }
}

fn scatter_plot_document(
    payload: &MsScatterPlotPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    let axes = scatter_axes(
        "x",
        &payload.x_label,
        "y",
        &payload.y_label,
        payload.fixed_x_bounds,
        payload.fixed_y_bounds,
        payload
            .series
            .iter()
            .flat_map(|series| series.points.iter().copied()),
    );
    let mut layers = scatter_layers(&payload.series, "x", "y", payload.symbol_size_px);
    if let Some(secondary_label) = &payload.secondary_y_label {
        let secondary_axis = document_axis(
            "y-secondary",
            secondary_label,
            &label_unit(secondary_label),
            payload
                .secondary_fixed_y_bounds
                .unwrap_or_else(|| {
                    series_y_bounds(
                        payload
                            .series
                            .iter()
                            .filter(|series| Some(series.y_axis) == payload.secondary_y_axis)
                            .flat_map(|series| series.points.iter().map(|(_, y)| *y)),
                    )
                })
                .0,
            payload
                .secondary_fixed_y_bounds
                .unwrap_or_else(|| {
                    series_y_bounds(
                        payload
                            .series
                            .iter()
                            .filter(|series| Some(series.y_axis) == payload.secondary_y_axis)
                            .flat_map(|series| series.points.iter().map(|(_, y)| *y)),
                    )
                })
                .1,
            PlotAxisScale::Linear,
        );
        let mut axes = axes;
        axes.push(PlotDocumentAxis {
            draws_on_trailing_edge: true,
            ..secondary_axis
        });
        for layer in &mut layers {
            if payload
                .series
                .iter()
                .find(|series| series.label == layer.title)
                .is_some_and(|series| Some(series.y_axis) == payload.secondary_y_axis)
            {
                layer.y_axis_id = "y-secondary".to_string();
            }
        }
        return base_document(
            preset,
            metadata,
            payload.header_lines.clone(),
            axes,
            layers,
            Vec::new(),
            Vec::new(),
        );
    }
    base_document(
        preset,
        metadata,
        payload.header_lines.clone(),
        axes,
        layers,
        Vec::new(),
        Vec::new(),
    )
}

fn scatter_grid_plot_document(
    payload: &MsScatterGridPayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    let panels = payload
        .panels
        .iter()
        .enumerate()
        .map(|(index, panel)| {
            let axes = scatter_axes(
                "x",
                &payload.x_label,
                "y",
                &payload.y_label,
                payload.fixed_x_bounds,
                payload.fixed_y_bounds,
                panel
                    .series
                    .iter()
                    .flat_map(|series| series.points.iter().copied()),
            );
            PlotDocumentPanel {
                id: format!("panel-{index}"),
                title: panel.label.clone(),
                axes,
                layers: scatter_layers(&panel.series, "x", "y", payload.symbol_size_px),
                annotations: Vec::new(),
            }
        })
        .collect();
    base_document(
        preset,
        metadata,
        payload.header_lines.clone(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        panels,
    )
}

fn scatter_page_plot_document(
    payload: &MsScatterPagePayload,
    metadata: &PayloadMetadata,
    preset: MeasurementSetPlotPreset,
) -> PlotDocumentPayload {
    let panels = payload
        .items
        .iter()
        .map(|item| {
            let axes = scatter_axes(
                "x",
                &item.plot.x_label,
                "y",
                &item.plot.y_label,
                item.plot.fixed_x_bounds,
                item.plot.fixed_y_bounds,
                item.plot
                    .series
                    .iter()
                    .flat_map(|series| series.points.iter().copied()),
            );
            PlotDocumentPanel {
                id: format!("plot-{}", item.plotindex),
                title: item.plot.title.clone(),
                axes,
                layers: scatter_layers(&item.plot.series, "x", "y", item.plot.symbol_size_px),
                annotations: Vec::new(),
            }
        })
        .collect();
    base_document(
        preset,
        metadata,
        payload.header_lines.clone(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        panels,
    )
}

fn base_document(
    preset: MeasurementSetPlotPreset,
    metadata: &PayloadMetadata,
    header_lines: Vec<String>,
    axes: Vec<PlotDocumentAxis>,
    layers: Vec<PlotDocumentLayer>,
    annotations: Vec<PlotDocumentAnnotation>,
    panels: Vec<PlotDocumentPanel>,
) -> PlotDocumentPayload {
    PlotDocumentPayload {
        id: format!("msexplore-{}", ms_plot_preset(preset).as_str()),
        title: metadata.title.clone(),
        subtitle: metadata.summary.clone(),
        header_lines,
        axes,
        layers,
        annotations,
        panels,
        show_legend: true,
    }
}

fn scatter_axes(
    x_id: &str,
    x_label: &str,
    y_id: &str,
    y_label: &str,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    points: impl Iterator<Item = (f64, f64)>,
) -> Vec<PlotDocumentAxis> {
    axes_for_ranges(
        x_id,
        x_label,
        y_id,
        y_label,
        fixed_x_bounds,
        fixed_y_bounds,
        points,
    )
}

fn axes_for_ranges(
    x_id: &str,
    x_label: &str,
    y_id: &str,
    y_label: &str,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    points: impl Iterator<Item = (f64, f64)>,
) -> Vec<PlotDocumentAxis> {
    let (x_bounds, y_bounds) = points.fold((None, None), |(x_bounds, y_bounds), (x, y)| {
        (
            bounds_accumulator(x_bounds, x),
            bounds_accumulator(y_bounds, y),
        )
    });
    let x_bounds = expanded_bounds(fixed_x_bounds.or(x_bounds).unwrap_or((0.0, 1.0)));
    let y_bounds = expanded_bounds(fixed_y_bounds.or(y_bounds).unwrap_or((0.0, 1.0)));
    vec![
        document_axis(
            x_id,
            x_label,
            &label_unit(x_label),
            x_bounds.0,
            x_bounds.1,
            PlotAxisScale::Linear,
        ),
        document_axis(
            y_id,
            y_label,
            &label_unit(y_label),
            y_bounds.0,
            y_bounds.1,
            PlotAxisScale::Linear,
        ),
    ]
}

fn scatter_layers(
    series: &[MsScatterSeries],
    x_axis_id: &str,
    y_axis_id: &str,
    symbol_size_px: Option<u32>,
) -> Vec<PlotDocumentLayer> {
    series
        .iter()
        .enumerate()
        .map(|(index, series)| {
            let (x_values, y_values): (Vec<_>, Vec<_>) = series.points.iter().copied().unzip();
            let provenance = if series.points.len() <= FRONTEND_POINT_PROVENANCE_LIMIT {
                series
                    .provenance
                    .iter()
                    .map(|point| PlotPointProvenance {
                        row: point.row as u64,
                        corr: point.corr as u64,
                        chan_start: point.chan_start as u64,
                        chan_end: point.chan_end as u64,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            point_layer(PointLayerSpec {
                id: format!("series-{index}"),
                title: series.label.clone(),
                x_axis_id,
                y_axis_id,
                x_values,
                y_values,
                point_labels: Vec::new(),
                point_symbol_sizes: Vec::new(),
                provenance,
                color_group: series.color_group.clone(),
                symbol_size: symbol_size_px.unwrap_or(3) as f64,
                line_width: 1.0,
                provenance_summary: "Visibility samples from Rust msexplore payload".to_string(),
            })
        })
        .collect()
}

struct PointLayerSpec<'a> {
    id: String,
    title: String,
    x_axis_id: &'a str,
    y_axis_id: &'a str,
    x_values: Vec<f64>,
    y_values: Vec<f64>,
    point_labels: Vec<String>,
    point_symbol_sizes: Vec<f64>,
    provenance: Vec<PlotPointProvenance>,
    color_group: String,
    symbol_size: f64,
    line_width: f64,
    provenance_summary: String,
}

fn point_layer(spec: PointLayerSpec<'_>) -> PlotDocumentLayer {
    let source_sample_count = spec.x_values.len() as u64;
    PlotDocumentLayer {
        id: spec.id,
        title: spec.title,
        kind: PlotLayerKind::Scatter,
        x_axis_id: spec.x_axis_id.to_string(),
        y_axis_id: spec.y_axis_id.to_string(),
        x_values: spec.x_values,
        y_values: spec.y_values,
        point_labels: spec.point_labels,
        point_symbol_sizes: spec.point_symbol_sizes,
        interval_x_start: Vec::new(),
        interval_x_end: Vec::new(),
        interval_y: Vec::new(),
        interval_height: Vec::new(),
        interval_labels: Vec::new(),
        provenance: spec.provenance,
        color_group: spec.color_group,
        symbol_size: spec.symbol_size,
        line_width: spec.line_width,
        opacity: 0.82,
        source_sample_count,
        payload_strategy: "point_cloud".to_string(),
        provenance_summary: spec.provenance_summary,
    }
}

struct IntervalLayerSpec<'a> {
    id: String,
    title: String,
    x_axis_id: &'a str,
    y_axis_id: &'a str,
    interval_x_start: Vec<f64>,
    interval_x_end: Vec<f64>,
    interval_y: Vec<f64>,
    interval_height: Vec<f64>,
    interval_labels: Vec<String>,
    color_group: String,
    provenance_summary: String,
}

fn interval_layer(spec: IntervalLayerSpec<'_>) -> PlotDocumentLayer {
    let source_sample_count = spec.interval_x_start.len() as u64;
    PlotDocumentLayer {
        id: spec.id,
        title: spec.title,
        kind: PlotLayerKind::Interval,
        x_axis_id: spec.x_axis_id.to_string(),
        y_axis_id: spec.y_axis_id.to_string(),
        x_values: Vec::new(),
        y_values: Vec::new(),
        point_labels: Vec::new(),
        point_symbol_sizes: Vec::new(),
        interval_x_start: spec.interval_x_start,
        interval_x_end: spec.interval_x_end,
        interval_y: spec.interval_y,
        interval_height: spec.interval_height,
        interval_labels: spec.interval_labels,
        provenance: Vec::new(),
        color_group: spec.color_group,
        symbol_size: 1.0,
        line_width: 1.0,
        opacity: 0.78,
        source_sample_count,
        payload_strategy: "intervals".to_string(),
        provenance_summary: spec.provenance_summary,
    }
}

fn document_axis(
    id: &str,
    label: &str,
    unit: &str,
    lower: f64,
    upper: f64,
    scale: PlotAxisScale,
) -> PlotDocumentAxis {
    PlotDocumentAxis {
        id: id.to_string(),
        label: label.to_string(),
        unit: unit.to_string(),
        lower,
        upper,
        scale,
        lane_labels: Vec::new(),
        draws_on_trailing_edge: false,
    }
}

fn document_lane_axis(id: &str, label: &str, lane_labels: Vec<String>) -> PlotDocumentAxis {
    PlotDocumentAxis {
        id: id.to_string(),
        label: label.to_string(),
        unit: String::new(),
        lower: -0.5,
        upper: lane_labels.len().saturating_sub(1) as f64 + 0.5,
        scale: PlotAxisScale::Linear,
        lane_labels,
        draws_on_trailing_edge: false,
    }
}

fn series_y_bounds(values: impl Iterator<Item = f64>) -> (f64, f64) {
    expanded_bounds(values.fold(None, bounds_accumulator).unwrap_or((0.0, 1.0)))
}

fn bounds_accumulator(bounds: Option<(f64, f64)>, value: f64) -> Option<(f64, f64)> {
    if !value.is_finite() {
        return bounds;
    }
    Some(match bounds {
        Some((lower, upper)) => (lower.min(value), upper.max(value)),
        None => (value, value),
    })
}

fn expanded_bounds((lower, upper): (f64, f64)) -> (f64, f64) {
    if !lower.is_finite() || !upper.is_finite() {
        return (0.0, 1.0);
    }
    if (upper - lower).abs() < f64::EPSILON {
        let pad = lower.abs().max(1.0) * 0.05;
        return (lower - pad, upper + pad);
    }
    let pad = (upper - lower).abs() * 0.04;
    (lower - pad, upper + pad)
}

fn listobs_metadata(
    payload: &MeasurementSetPlotPayload,
    preset: MeasurementSetPlotPreset,
    requested_max_points: u64,
) -> PayloadMetadata {
    match payload {
        MeasurementSetPlotPayload::UvCoverage(payload) => {
            let rendered_point_count = payload
                .tracks
                .iter()
                .map(|track| track.points.len() as u64)
                .sum();
            let series = uv_coverage_series_metadata(&payload.tracks);
            let series_count = series.len() as u64;
            PayloadMetadata {
                title: "UV Coverage".to_string(),
                summary: payload.summary.clone(),
                x_axis: axis_metadata("u", "u (kλ)"),
                y_axis: axis_metadata("v", "v (kλ)"),
                series,
                sampling: PlotSamplingDiagnostics {
                    requested_max_points,
                    rendered_point_count,
                    series_count,
                    diagnostics: sampling_diagnostics(&payload.summary, rendered_point_count),
                },
            }
        }
        _ => PayloadMetadata {
            title: ms_plot_preset(preset).display_name().to_string(),
            summary: "Metadata-oriented MeasurementSet plot.".to_string(),
            x_axis: axis_metadata(
                ms_plot_preset(preset).as_str(),
                ms_plot_preset(preset).display_name(),
            ),
            y_axis: axis_metadata("metadata", "Metadata"),
            series: vec![],
            sampling: PlotSamplingDiagnostics {
                requested_max_points,
                rendered_point_count: 0,
                series_count: 0,
                diagnostics: vec!["metadata-oriented plot has no visibility series".to_string()],
            },
        },
    }
}

fn scatter_grid_metadata(
    payload: &MsScatterGridPayload,
    requested_max_points: u64,
) -> PayloadMetadata {
    let series = payload
        .panels
        .iter()
        .flat_map(|panel| panel.series.iter())
        .map(series_metadata)
        .collect::<Vec<_>>();
    let rendered_point_count = series.iter().map(|series| series.point_count).sum();
    PayloadMetadata {
        title: payload.title.clone(),
        summary: payload.summary.clone(),
        x_axis: axis_metadata(payload.x_axis.as_str(), &payload.x_label),
        y_axis: axis_metadata(payload.y_axis.as_str(), &payload.y_label),
        series,
        sampling: PlotSamplingDiagnostics {
            requested_max_points,
            rendered_point_count,
            series_count: payload
                .panels
                .iter()
                .map(|panel| panel.series.len() as u64)
                .sum(),
            diagnostics: sampling_diagnostics(&payload.summary, rendered_point_count),
        },
    }
}

fn scatter_page_metadata(
    payload: &MsScatterPagePayload,
    requested_max_points: u64,
) -> PayloadMetadata {
    let first_plot = payload.items.first().map(|item| &item.plot);
    let series = payload
        .items
        .iter()
        .flat_map(|item| item.plot.series.iter())
        .map(series_metadata)
        .collect::<Vec<_>>();
    let rendered_point_count = series.iter().map(|series| series.point_count).sum();
    PayloadMetadata {
        title: payload.title.clone(),
        summary: payload.summary.clone(),
        x_axis: first_plot
            .map(|plot| axis_metadata(plot.x_axis.as_str(), &plot.x_label))
            .unwrap_or_else(|| axis_metadata("unknown", "Unknown")),
        y_axis: first_plot
            .map(|plot| axis_metadata(plot.y_axis.as_str(), &plot.y_label))
            .unwrap_or_else(|| axis_metadata("unknown", "Unknown")),
        series,
        sampling: PlotSamplingDiagnostics {
            requested_max_points,
            rendered_point_count,
            series_count: payload
                .items
                .iter()
                .map(|item| item.plot.series.len() as u64)
                .sum(),
            diagnostics: sampling_diagnostics(&payload.summary, rendered_point_count),
        },
    }
}

fn scatter_metadata(payload: &MsScatterPlotPayload, requested_max_points: u64) -> PayloadMetadata {
    let series = payload
        .series
        .iter()
        .map(series_metadata)
        .collect::<Vec<_>>();
    let series_count = series.len() as u64;
    let rendered_point_count = series.iter().map(|series| series.point_count).sum();
    PayloadMetadata {
        title: payload.title.clone(),
        summary: payload.summary.clone(),
        x_axis: axis_metadata(payload.x_axis.as_str(), &payload.x_label),
        y_axis: axis_metadata(payload.y_axis.as_str(), &payload.y_label),
        series,
        sampling: PlotSamplingDiagnostics {
            requested_max_points,
            rendered_point_count,
            series_count,
            diagnostics: sampling_diagnostics(&payload.summary, rendered_point_count),
        },
    }
}

fn uv_coverage_series_metadata(tracks: &[UvCoverageSeries]) -> Vec<PlotSeriesMetadata> {
    let mut grouped = BTreeMap::<String, u64>::new();
    for track in tracks {
        *grouped
            .entry(uv_coverage_group_label(&track.label))
            .or_default() += track.points.len() as u64;
    }
    grouped
        .into_iter()
        .map(|(group, point_count)| PlotSeriesMetadata {
            label: group.clone(),
            color_group: group,
            point_count,
            first_row: None,
            last_row: None,
        })
        .collect()
}

fn uv_coverage_group_label(track_label: &str) -> String {
    let parts = track_label.split_whitespace().collect::<Vec<_>>();
    let field = parts
        .windows(2)
        .find_map(|window| (window[0] == "field").then_some(window[1]));
    let spw = parts
        .windows(2)
        .find_map(|window| (window[0] == "spw").then_some(window[1]));
    match (field, spw) {
        (Some(field), Some(spw)) => format!("field {field} spw {spw}"),
        (Some(field), None) => format!("field {field}"),
        (None, Some(spw)) => format!("spw {spw}"),
        (None, None) => "UV samples".to_string(),
    }
}

fn series_metadata(series: &MsScatterSeries) -> PlotSeriesMetadata {
    let first_row = series.provenance.first().map(|point| point.row as u64);
    let last_row = series.provenance.last().map(|point| point.row as u64);
    PlotSeriesMetadata {
        label: series.label.clone(),
        color_group: series.color_group.clone(),
        point_count: series.points.len() as u64,
        first_row,
        last_row,
    }
}

fn axis_metadata(id: &str, label: &str) -> PlotAxisMetadata {
    PlotAxisMetadata {
        id: id.to_string(),
        label: label.to_string(),
        unit: label_unit(label),
    }
}

fn label_unit(label: &str) -> String {
    label
        .rsplit_once('(')
        .and_then(|(_, suffix)| suffix.strip_suffix(')'))
        .unwrap_or("")
        .trim()
        .to_string()
}

fn nonempty_or<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

fn sampling_diagnostics(summary: &str, rendered_point_count: u64) -> Vec<String> {
    let mut diagnostics = Vec::new();
    if rendered_point_count == 0 {
        diagnostics.push("plot produced no drawable visibility points".to_string());
    }
    if summary.contains("Decimated points") || summary.contains("Downsampled plot") {
        diagnostics.push("point budget decimation was applied".to_string());
    }
    diagnostics
}

fn normalize_data_column(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "data".to_string();
    }
    trimmed.to_ascii_lowercase()
}

fn normalized_optional(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "all")
}

fn selection_summary(request: &MeasurementSetPlotRequest) -> String {
    let mut parts = vec![format!(
        "data column {}",
        normalize_data_column(&request.data_column)
    )];
    if let Some(field) = normalized_optional(request.field.clone()) {
        parts.push(format!("field {field}"));
    }
    if let Some(spw) = normalized_optional(request.spectral_window.clone()) {
        parts.push(format!("spw {spw}"));
    }
    if let Some(correlation) = normalized_optional(request.correlation.clone()) {
        parts.push(format!("correlation {correlation}"));
    }
    parts.join(", ")
}

#[derive(Default)]
struct ProjectScan {
    datasets: Vec<DatasetProbe>,
    diagnostics: Vec<String>,
    scanned_entry_count: usize,
    truncated: bool,
}

fn scan_path(path: &Path, depth: usize, scan: &mut ProjectScan) {
    if scan.scanned_entry_count >= MAX_PROJECT_SCAN_ENTRIES {
        scan.truncated = true;
        return;
    }
    scan.scanned_entry_count += 1;

    match probe_dataset_path(path) {
        Ok(Some(dataset)) => {
            scan.datasets.push(dataset);
            return;
        }
        Ok(None) => {}
        Err(error) => scan
            .diagnostics
            .push(format!("{}: {error}", path.display())),
    }

    if depth >= MAX_PROJECT_SCAN_DEPTH {
        return;
    }
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if !metadata.is_dir() {
        return;
    }

    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) => {
            scan.diagnostics
                .push(format!("read {}: {error}", path.display()));
            return;
        }
    };
    for entry in entries {
        if scan.scanned_entry_count >= MAX_PROJECT_SCAN_ENTRIES {
            scan.truncated = true;
            break;
        }
        match entry {
            Ok(entry) => scan_path(&entry.path(), depth + 1, scan),
            Err(error) => scan.diagnostics.push(format!("directory entry: {error}")),
        }
    }
}

fn probe_dataset_path(path: &Path) -> Result<Option<DatasetProbe>, String> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => return Err(format!("metadata failed: {error}")),
    };
    if !(metadata.is_dir() || metadata.is_file()) {
        return Ok(None);
    }

    if metadata.is_dir() {
        if let Some(probe) = probe_measurement_set(path, &metadata)? {
            return Ok(Some(probe));
        }
        if let Some(probe) = probe_image(path, &metadata)? {
            return Ok(Some(probe));
        }
        if let Some(probe) = probe_table(path, &metadata)? {
            return Ok(Some(probe));
        }
    }

    Ok(None)
}

fn probe_measurement_set(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<Option<DatasetProbe>, String> {
    let (size_bytes, diagnostics) = dataset_size_bytes(path, metadata);
    let ms = match MeasurementSet::open(path) {
        Ok(ms) => ms,
        Err(_) => return Ok(None),
    };
    let fields = match ms_field_labels(&ms) {
        Ok(fields) => fields,
        Err(_) => return Ok(None),
    };
    let spectral_windows = match ms_spectral_window_labels(&ms) {
        Ok(spectral_windows) => spectral_windows,
        Err(_) => return Ok(None),
    };
    let antennas = match ms_antenna_labels(&ms) {
        Ok(antennas) => antennas,
        Err(_) => return Ok(None),
    };
    let scans = ms_main_id_labels(&ms, "SCAN_NUMBER", "scan").unwrap_or_default();
    let arrays = ms_main_id_labels(&ms, "ARRAY_ID", "array").unwrap_or_default();
    let observations = ms_main_id_labels(&ms, "OBSERVATION_ID", "observation").unwrap_or_default();
    let intents = ms_intent_labels(&ms).unwrap_or_default();
    let feeds = ms_feed_labels(&ms).unwrap_or_default();
    let correlations = match ms_correlation_labels(&ms) {
        Ok(correlations) => correlations,
        Err(_) => return Ok(None),
    };
    let columns = table_columns(ms.main_table());
    let data_columns = visibility_data_columns(&columns);
    let subtables = ms_subtables(&ms);
    let row_count = ms.row_count();

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::MeasurementSet,
        size_bytes,
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format!(
            "{} rows, {} fields, {} spw, {} antennas",
            row_count,
            fields.len(),
            spectral_windows.len(),
            antennas.len()
        ),
        units: "Jy, Hz, seconds".to_string(),
        fields,
        spectral_windows,
        scans,
        arrays,
        observations,
        antennas,
        intents,
        feeds,
        correlations,
        columns,
        data_columns,
        subtables,
        shape: vec![row_count as u64],
        notes: "Recognized by opening the path as a MeasurementSet and reading lightweight MS metadata."
            .to_string(),
        diagnostics,
    }))
}

fn ms_field_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let fields = ms.field().map_err(|error| error.to_string())?;
    (0..fields.row_count())
        .map(|row| {
            fields
                .name(row)
                .map(|name| format!("{row}: {name}"))
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn ms_spectral_window_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let spectral_windows = ms.spectral_window().map_err(|error| error.to_string())?;
    (0..spectral_windows.row_count())
        .map(|row| {
            let num_chan = spectral_windows
                .num_chan(row)
                .map_err(|error| error.to_string())?;
            let chan_freq = spectral_windows.chan_freq(row).unwrap_or_default();
            let center_hz = if chan_freq.is_empty() {
                spectral_windows
                    .ref_frequency(row)
                    .map_err(|error| error.to_string())?
            } else {
                mean_or_zero(&chan_freq)
            };
            Ok(format!(
                "spw {}: {} chan, {:.6} GHz center",
                row,
                num_chan,
                center_hz / 1.0e9
            ))
        })
        .collect()
}

fn ms_antenna_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let antennas = ms.antenna().map_err(|error| error.to_string())?;
    (0..antennas.row_count())
        .map(|row| antennas.name(row).map_err(|error| error.to_string()))
        .collect()
}

fn ms_main_id_labels(
    ms: &MeasurementSet,
    column: &'static str,
    prefix: &str,
) -> Result<Vec<String>, String> {
    let table = ms.main_table();
    let row_numbers = (0..table.row_count()).collect::<Vec<_>>();
    let mut ids = BTreeSet::new();
    for row_chunk in row_numbers.chunks(MAIN_SCALAR_CHUNK_ROWS) {
        for id in selected_i32_values(table, column, row_chunk)? {
            if id >= 0 {
                ids.insert(id);
            }
        }
    }
    Ok(ids.into_iter().map(|id| format!("{prefix} {id}")).collect())
}

fn ms_intent_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let state = ms.state().map_err(|error| error.to_string())?;
    let mut intents = BTreeSet::new();
    for row in 0..state.row_count() {
        let intent = state
            .string(row, "OBS_MODE")
            .map_err(|error| error.to_string())?;
        for item in intent
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            intents.insert(item.to_string());
        }
    }
    Ok(intents.into_iter().collect())
}

fn ms_feed_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let table = ms.main_table();
    let row_numbers = (0..table.row_count()).collect::<Vec<_>>();
    let mut ids = BTreeSet::new();
    for row_chunk in row_numbers.chunks(MAIN_SCALAR_CHUNK_ROWS) {
        for id in selected_i32_values(table, "FEED1", row_chunk)? {
            if id >= 0 {
                ids.insert(id);
            }
        }
        if table.column_accessor("FEED2").is_ok() {
            for id in selected_i32_values(table, "FEED2", row_chunk)? {
                if id >= 0 {
                    ids.insert(id);
                }
            }
        }
    }
    Ok(ids.into_iter().map(|id| format!("feed {id}")).collect())
}

#[uniffi::export]
pub fn probe_measurement_set_uv_range(
    dataset_path: String,
) -> FrontendResult<MeasurementSetUvRangeProbe> {
    probe_measurement_set_uv_range_inner(&dataset_path).map_err(|error| {
        FrontendServiceError::Probe {
            reason: format!("{dataset_path}: {error}"),
        }
    })
}

#[uniffi::export]
pub fn probe_measurement_set_time_range(
    dataset_path: String,
) -> FrontendResult<MeasurementSetTimeRangeProbe> {
    probe_measurement_set_time_range_inner(&dataset_path).map_err(|error| {
        FrontendServiceError::Probe {
            reason: format!("{dataset_path}: {error}"),
        }
    })
}

fn probe_measurement_set_uv_range_inner(
    dataset_path: &str,
) -> Result<MeasurementSetUvRangeProbe, String> {
    let ms = MeasurementSet::open(Path::new(dataset_path)).map_err(|error| error.to_string())?;
    let table = ms.main_table();
    if table.row_count() == 0 {
        return Err("MeasurementSet has no MAIN rows".to_string());
    }
    let ddid_to_spw = data_description_to_spw(&ms)?;
    let spw_center_frequency_hz = spw_center_frequency_lookup(&ms)?;
    let row_numbers = (0..table.row_count()).collect::<Vec<_>>();
    let mut min_meters = f64::INFINITY;
    let mut max_meters = f64::NEG_INFINITY;
    let mut min_kilolambda = f64::INFINITY;
    let mut max_kilolambda = f64::NEG_INFINITY;
    let mut seen_rows = 0u64;

    for row_chunk in row_numbers.chunks(MAIN_SCALAR_CHUNK_ROWS) {
        let uvw_values = selected_uvw_values(table, "UVW", row_chunk)?;
        let ddids = selected_i32_values(table, "DATA_DESC_ID", row_chunk)?;
        for (uvw, ddid) in uvw_values.into_iter().zip(ddids) {
            let uv_meters = uvw[0].hypot(uvw[1]);
            if !uv_meters.is_finite() {
                continue;
            }
            min_meters = min_meters.min(uv_meters);
            max_meters = max_meters.max(uv_meters);
            if let Some(frequency_hz) = ddid_to_spw
                .get(&ddid)
                .and_then(|spw| spw_center_frequency_hz.get(spw))
                .copied()
                .filter(|frequency| *frequency > 0.0)
            {
                let kilo_lambda = uv_meters * frequency_hz / SPEED_OF_LIGHT_M_S / 1_000.0;
                min_kilolambda = min_kilolambda.min(kilo_lambda);
                max_kilolambda = max_kilolambda.max(kilo_lambda);
            }
            seen_rows += 1;
        }
    }

    if seen_rows == 0 || !min_meters.is_finite() || !max_meters.is_finite() {
        return Err("MeasurementSet UVW column did not contain finite UV distances".to_string());
    }
    if !min_kilolambda.is_finite() || !max_kilolambda.is_finite() {
        min_kilolambda = 0.0;
        max_kilolambda = 0.0;
    }

    Ok(MeasurementSetUvRangeProbe {
        min_meters,
        max_meters,
        min_kilolambda,
        max_kilolambda,
        row_count: seen_rows,
    })
}

fn probe_measurement_set_time_range_inner(
    dataset_path: &str,
) -> Result<MeasurementSetTimeRangeProbe, String> {
    let ms = MeasurementSet::open(Path::new(dataset_path)).map_err(|error| error.to_string())?;
    let table = ms.main_table();
    if table.row_count() == 0 {
        return Err("MeasurementSet has no MAIN rows".to_string());
    }
    let row_numbers = (0..table.row_count()).collect::<Vec<_>>();
    let mut min_seconds = f64::INFINITY;
    let mut max_seconds = f64::NEG_INFINITY;
    let mut seen_rows = 0u64;
    for row_chunk in row_numbers.chunks(MAIN_SCALAR_CHUNK_ROWS) {
        for seconds in selected_f64_values(table, "TIME", row_chunk)? {
            if !seconds.is_finite() {
                continue;
            }
            min_seconds = min_seconds.min(seconds);
            max_seconds = max_seconds.max(seconds);
            seen_rows += 1;
        }
    }
    if seen_rows == 0 || !min_seconds.is_finite() || !max_seconds.is_finite() {
        return Err("MeasurementSet TIME column did not contain finite seconds".to_string());
    }
    Ok(MeasurementSetTimeRangeProbe {
        min_seconds,
        max_seconds,
        row_count: seen_rows,
    })
}

fn spw_center_frequency_lookup(ms: &MeasurementSet) -> Result<BTreeMap<i32, f64>, String> {
    let spectral_windows = ms.spectral_window().map_err(|error| error.to_string())?;
    let mut lookup = BTreeMap::new();
    for row in 0..spectral_windows.row_count() {
        let chan_freq = spectral_windows.chan_freq(row).unwrap_or_default();
        let center_hz = if chan_freq.is_empty() {
            spectral_windows
                .ref_frequency(row)
                .map_err(|error| error.to_string())?
        } else {
            mean_or_zero(&chan_freq)
        };
        lookup.insert(row as i32, center_hz);
    }
    Ok(lookup)
}

fn selected_uvw_values(
    table: &Table,
    column: &'static str,
    rows: &[usize],
) -> Result<Vec<[f64; 3]>, String> {
    table
        .column_accessor(column)
        .map_err(|error| error.to_string())?
        .array_cells_owned(rows)
        .map_err(|error| error.to_string())?
        .into_iter()
        .zip(rows.iter().copied())
        .map(|(value, row)| {
            let value = value
                .ok_or_else(|| format!("plot service requires {column} data for row {row}"))?;
            match value {
                casa_types::ArrayValue::Float64(values) => {
                    let slice = values.as_slice().ok_or_else(|| {
                        format!("plot service requires contiguous {column} f64[3] cells")
                    })?;
                    if slice.len() != 3 {
                        return Err(format!(
                            "plot service requires {column} cells with shape [3], found [{}]",
                            slice.len()
                        ));
                    }
                    Ok([slice[0], slice[1], slice[2]])
                }
                other => Err(format!(
                    "plot service requires DOUBLE array {column} cells, found {:?}",
                    other.primitive_type()
                )),
            }
        })
        .collect()
}

fn ms_correlation_labels(ms: &MeasurementSet) -> Result<Vec<String>, String> {
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let mut labels = Vec::new();
    for row in 0..polarization.row_count() {
        labels.extend(
            polarization
                .corr_type(row)
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(stokes_name)
                .map(str::to_string),
        );
    }
    Ok(unique_sorted(labels))
}

fn stokes_name(code: i32) -> &'static str {
    match code {
        1 => "I",
        2 => "Q",
        3 => "U",
        4 => "V",
        5 => "RR",
        6 => "RL",
        7 => "LR",
        8 => "LL",
        9 => "XX",
        10 => "XY",
        11 => "YX",
        12 => "YY",
        _ => "??",
    }
}

fn mean_or_zero(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn probe_image(path: &Path, metadata: &fs::Metadata) -> Result<Option<DatasetProbe>, String> {
    let (size_bytes, mut diagnostics) = dataset_size_bytes(path, metadata);
    let image = match AnyPagedImage::open(path) {
        Ok(image) => image,
        Err(_) => return Ok(None),
    };
    let pixel_type = match image.pixel_type() {
        ImagePixelType::Float32 => "float32",
        ImagePixelType::Float64 => "float64",
        ImagePixelType::Complex32 => "complex64",
        ImagePixelType::Complex64 => "complex128",
    };
    let shape: Vec<u64> = image.shape().iter().map(|value| *value as u64).collect();
    let mask_names = image.mask_names();
    let region_names = image.region_names();
    let brightness_unit = image_units(&image).to_string();
    diagnostics.push(format!("Pixel type: {pixel_type}"));
    diagnostics.extend(image_coordinate_diagnostics(&image));
    if let Some(default_mask) = image.default_mask_name() {
        diagnostics.push(format!("Default mask: {default_mask}"));
    }

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::Image,
        size_bytes,
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format_shape(&shape),
        units: brightness_unit,
        fields: vec![],
        spectral_windows: vec![],
        scans: vec![],
        arrays: vec![],
        observations: vec![],
        antennas: vec![],
        intents: vec![],
        feeds: vec![],
        correlations: vec![],
        columns: vec!["map".to_string()],
        data_columns: vec![],
        subtables: vec![],
        shape,
        notes: format!(
            "Recognized by opening the path as a casa-rs image; {} masks, {} regions.",
            mask_names.len(),
            region_names.len()
        ),
        diagnostics,
    }))
}

fn image_units(image: &AnyPagedImage) -> &str {
    match image {
        AnyPagedImage::Float32(image) => image.units(),
        AnyPagedImage::Float64(image) => image.units(),
        AnyPagedImage::Complex32(image) => image.units(),
        AnyPagedImage::Complex64(image) => image.units(),
    }
}

fn image_coordinates(image: &AnyPagedImage) -> &CoordinateSystem {
    match image {
        AnyPagedImage::Float32(image) => image.coordinates(),
        AnyPagedImage::Float64(image) => image.coordinates(),
        AnyPagedImage::Complex32(image) => image.coordinates(),
        AnyPagedImage::Complex64(image) => image.coordinates(),
    }
}

fn image_info(image: &AnyPagedImage) -> Result<ImageInfo, String> {
    match image {
        AnyPagedImage::Float32(image) => image.image_info(),
        AnyPagedImage::Float64(image) => image.image_info(),
        AnyPagedImage::Complex32(image) => image.image_info(),
        AnyPagedImage::Complex64(image) => image.image_info(),
    }
    .map_err(|error| error.to_string())
}

fn image_coordinate_diagnostics(image: &AnyPagedImage) -> Vec<String> {
    let coords = image_coordinates(image);
    let shape = image.shape();
    let mut diagnostics = Vec::new();
    diagnostics.extend(image_direction_diagnostics(coords, shape));
    diagnostics.extend(image_spectral_diagnostics(coords, shape));
    match image_info(image) {
        Ok(info) => diagnostics.extend(image_beam_diagnostics(&info)),
        Err(error) => diagnostics.push(format!("Image info unavailable: {error}")),
    }
    diagnostics
}

fn image_direction_diagnostics(coords: &CoordinateSystem, shape: &[usize]) -> Vec<String> {
    let mut diagnostics = Vec::new();
    let mut pixel_offset = 0usize;
    for index in 0..coords.n_coordinates() {
        let coord = coords.coordinate(index);
        let n_pixel_axes = coord.n_pixel_axes();
        if coord.coordinate_type() == CoordinateType::Direction && n_pixel_axes >= 2 {
            let increments = coord.increment();
            if increments.len() >= 2 {
                let ra_cell = angular_increment_arcseconds(increments[0]).value().abs();
                let dec_cell = declination_increment_arcseconds(increments[1])
                    .value()
                    .abs();
                diagnostics.push(format!(
                    "Cell size: {} x {} arcsec",
                    format_compact_float(ra_cell),
                    format_compact_float(dec_cell)
                ));
            }

            let Some(x_len) = shape.get(pixel_offset).copied() else {
                break;
            };
            let Some(y_len) = shape.get(pixel_offset + 1).copied() else {
                break;
            };
            let center_pixel = [
                center_pixel_coordinate(x_len),
                center_pixel_coordinate(y_len),
            ];
            if let Ok(world) = coord.to_world(&center_pixel) {
                if world.len() >= 2 {
                    diagnostics.push(format!(
                        "Center: RA {} Dec {}",
                        format_right_ascension_labeled(world[0], 3),
                        format_declination_labeled(world[1], 2)
                    ));
                }
            }
            break;
        }
        pixel_offset += n_pixel_axes;
    }
    diagnostics
}

fn image_spectral_diagnostics(coords: &CoordinateSystem, shape: &[usize]) -> Vec<String> {
    let mut diagnostics = Vec::new();
    let mut pixel_offset = 0usize;
    for index in 0..coords.n_coordinates() {
        let coord = coords.coordinate(index);
        let n_pixel_axes = coord.n_pixel_axes();
        if coord.coordinate_type() == CoordinateType::Spectral && n_pixel_axes >= 1 {
            let Some(channel_count) = shape.get(pixel_offset).copied() else {
                break;
            };
            if channel_count == 0 {
                break;
            }
            let center_pixel = [center_pixel_coordinate(channel_count)];
            let unit = coord
                .axis_units()
                .first()
                .cloned()
                .unwrap_or_else(|| "Hz".to_string());
            if let Ok(world) = coord.to_world(&center_pixel) {
                if let Some(center) = world.first().copied() {
                    diagnostics.push(format!(
                        "Cube center frequency: {}",
                        format_frequency_like_value(center, &unit)
                    ));
                }
            }
            let channel_separation = coord
                .increment()
                .first()
                .copied()
                .map(f64::abs)
                .unwrap_or_default();
            if channel_separation.is_finite() && channel_separation > 0.0 {
                diagnostics.push(format!(
                    "Total bandwidth: {}",
                    format_frequency_like_value(channel_separation * channel_count as f64, &unit)
                ));
                diagnostics.push(format!(
                    "Channel separation: {}",
                    format_frequency_like_value(channel_separation, &unit)
                ));
            }
            break;
        }
        pixel_offset += n_pixel_axes;
    }
    diagnostics
}

fn image_beam_diagnostics(info: &ImageInfo) -> Vec<String> {
    let beam_set = &info.beam_set;
    if let Some(beam) = beam_set.single_beam() {
        if beam.is_null() {
            return Vec::new();
        }
        return match (
            beam.major_in("arcsec"),
            beam.minor_in("arcsec"),
            beam.position_angle_in("deg"),
        ) {
            (Ok(major), Ok(minor), Ok(pa)) => vec![format!(
                "Beam: {} x {} arcsec, PA {} deg",
                format_compact_float(major),
                format_compact_float(minor),
                format_compact_float(pa)
            )],
            _ => Vec::new(),
        };
    }
    if beam_set.is_empty() {
        return Vec::new();
    }
    let (channels, stokes) = beam_set.shape();
    let mut diagnostics = vec![format!(
        "Beam: {} per-plane beams ({} chan x {} stokes)",
        beam_set.size(),
        channels,
        stokes
    )];
    if let Some(beam) = beam_set.median_area_beam() {
        if !beam.is_null() {
            if let (Ok(major), Ok(minor), Ok(pa)) = (
                beam.major_in("arcsec"),
                beam.minor_in("arcsec"),
                beam.position_angle_in("deg"),
            ) {
                diagnostics.push(format!(
                    "Median beam: {} x {} arcsec, PA {} deg",
                    format_compact_float(major),
                    format_compact_float(minor),
                    format_compact_float(pa)
                ));
            }
        }
    }
    diagnostics
}

fn center_pixel_coordinate(axis_len: usize) -> f64 {
    axis_len.saturating_sub(1) as f64 / 2.0
}

fn format_frequency_like_value(value: f64, unit: &str) -> String {
    if unit.eq_ignore_ascii_case("Hz") {
        let abs_value = value.abs();
        if abs_value >= 1.0e9 {
            format!("{} GHz", format_compact_float(value / 1.0e9))
        } else if abs_value >= 1.0e6 {
            format!("{} MHz", format_compact_float(value / 1.0e6))
        } else if abs_value >= 1.0e3 {
            format!("{} kHz", format_compact_float(value / 1.0e3))
        } else {
            format!("{} Hz", format_compact_float(value))
        }
    } else if unit.is_empty() {
        format_compact_float(value)
    } else {
        format!("{} {unit}", format_compact_float(value))
    }
}

fn format_compact_float(value: f64) -> String {
    let mut text = format!("{value:.6}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" { "0".to_string() } else { text }
}

fn probe_table(path: &Path, metadata: &fs::Metadata) -> Result<Option<DatasetProbe>, String> {
    let (size_bytes, diagnostics) = dataset_size_bytes(path, metadata);
    let table = match Table::open(TableOptions::new(path)) {
        Ok(table) => table,
        Err(_) => return Ok(None),
    };
    let columns = table_columns(&table);
    if columns.is_empty() && table.row_count() == 0 && table.info().table_type.is_empty() {
        return Ok(None);
    }
    let table_type = if table.info().table_type.is_empty() {
        "casacore table".to_string()
    } else {
        table.info().table_type.clone()
    };

    Ok(Some(DatasetProbe {
        id: stable_id(path),
        name: path_name(path),
        path: path.display().to_string(),
        kind: DatasetKind::Table,
        size_bytes,
        modified_unix_seconds: modified_unix_seconds(metadata),
        probed_unix_seconds: now_unix_seconds(),
        logical_size: format!("{} rows, {} columns", table.row_count(), columns.len()),
        units: table_type.clone(),
        fields: vec![],
        spectral_windows: vec![],
        scans: vec![],
        arrays: vec![],
        observations: vec![],
        antennas: vec![],
        intents: vec![],
        feeds: vec![],
        correlations: vec![],
        columns,
        data_columns: vec![],
        subtables: vec![],
        shape: vec![table.row_count() as u64],
        notes: format!("Recognized by opening the path as a {table_type}."),
        diagnostics,
    }))
}

fn table_columns(table: &Table) -> Vec<String> {
    table
        .schema()
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn visibility_data_columns(columns: &[String]) -> Vec<String> {
    VisibilityDataColumn::ALL
        .iter()
        .map(|column| column.name())
        .filter(|name| columns.iter().any(|column| column == *name))
        .map(str::to_string)
        .collect()
}

fn ms_subtables(ms: &MeasurementSet) -> Vec<String> {
    let mut ids = ms.subtable_ids();
    ids.sort();
    ids.into_iter()
        .map(|id| {
            let role = if id.is_required() {
                "required"
            } else {
                "optional"
            };
            format!("{} ({role})", id.name())
        })
        .collect()
}

fn dataset_size_bytes(path: &Path, metadata: &fs::Metadata) -> (u64, Vec<String>) {
    if metadata.is_file() {
        return (metadata.len(), vec![]);
    }
    if !metadata.is_dir() {
        return (metadata.len(), vec![]);
    }

    let mut diagnostics = Vec::new();
    match directory_size_bytes(path, 2_048) {
        Ok((size, truncated)) => {
            if truncated {
                diagnostics
                    .push("size estimate truncated after 2048 filesystem entries".to_string());
            }
            (size, diagnostics)
        }
        Err(error) => {
            diagnostics.push(format!("size estimate failed: {error}"));
            (metadata.len(), diagnostics)
        }
    }
}

fn directory_size_bytes(path: &Path, max_entries: usize) -> Result<(u64, bool), String> {
    let mut stack = vec![path.to_path_buf()];
    let mut seen_entries = 0usize;
    let mut total = 0u64;
    let mut truncated = false;

    while let Some(current) = stack.pop() {
        if seen_entries >= max_entries {
            truncated = true;
            break;
        }
        seen_entries += 1;

        let metadata = fs::metadata(&current)
            .map_err(|error| format!("metadata {}: {error}", current.display()))?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        } else if metadata.is_dir() {
            let entries = fs::read_dir(&current)
                .map_err(|error| format!("read {}: {error}", current.display()))?;
            for entry in entries {
                let entry = entry
                    .map_err(|error| format!("directory entry {}: {error}", current.display()))?;
                stack.push(entry.path());
            }
        }
    }

    Ok((total, truncated))
}

fn stable_id(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn path_name(path: &Path) -> String {
    if let Some(name) = path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
    {
        return name.to_string();
    }
    path.to_string_lossy().into_owned()
}

fn modified_unix_seconds(metadata: &fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn unique_sorted(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn format_shape(shape: &[u64]) -> String {
    if shape.is_empty() {
        return "scalar".to_string();
    }
    shape
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(" x ")
}

uniffi::setup_scaffolding!();

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    use casa_coordinates::{
        CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    };
    use casa_images::beam::{GaussianBeam, ImageBeamSet};
    use casa_images::{ImageInfo, ImageType, PagedImage};
    use casa_tables::{ColumnSchema, TableInfo, TableSchema};
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;
    use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use flate2::read::GzDecoder;
    use ndarray::ArrayD;
    use tempfile::TempDir;

    fn unpack_small_ms() -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let fixture = File::open("../casa-ms/tests/fixtures/mssel_test_small.ms.tgz")
            .expect("small MS fixture");
        let decoder = GzDecoder::new(fixture);
        let mut archive = tar::Archive::new(decoder);
        archive.unpack(dir.path()).expect("unpack MS fixture");
        let ms_path = dir.path().join("mssel_test_small.ms");
        (dir, ms_path)
    }

    fn make_table(path: &Path) {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        table.set_info(TableInfo {
            table_type: "Calibration Table".to_string(),
            sub_type: "G Jones".to_string(),
        });
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String("gain".to_string())),
                ),
            ]))
            .expect("row");
        table.save(TableOptions::new(path)).expect("save table");
    }

    #[test]
    fn probe_measurement_set_reads_real_metadata() {
        let (_dir, ms_path) = unpack_small_ms();

        let probe = probe_path(ms_path.display().to_string())
            .expect("probe")
            .expect("recognized");

        assert_eq!(probe.kind, DatasetKind::MeasurementSet);
        assert!(probe.logical_size.contains("rows"));
        assert!(!probe.fields.is_empty());
        assert!(!probe.spectral_windows.is_empty());
        assert!(!probe.antennas.is_empty());
        assert!(probe.columns.iter().any(|column| column == "DATA"));
        assert_eq!(probe.data_columns, vec!["DATA"]);
        assert!(
            probe
                .subtables
                .iter()
                .any(|subtable| subtable == "ANTENNA (required)")
        );
        assert!(probe.size_bytes > 0);
    }

    #[test]
    fn probe_measurement_set_uv_range_reads_main_uvw() {
        let (_dir, ms_path) = unpack_small_ms();

        let probe =
            probe_measurement_set_uv_range(ms_path.display().to_string()).expect("uv probe");

        assert!(probe.row_count > 0);
        assert!(probe.min_meters.is_finite());
        assert!(probe.max_meters >= probe.min_meters);
    }

    #[test]
    fn probe_measurement_set_time_range_reads_main_time() {
        let (_dir, ms_path) = unpack_small_ms();

        let probe =
            probe_measurement_set_time_range(ms_path.display().to_string()).expect("time probe");

        assert!(probe.row_count > 0);
        assert!(probe.min_seconds.is_finite());
        assert!(probe.max_seconds >= probe.min_seconds);
    }

    #[test]
    fn task_catalog_json_exposes_shared_frontend_task_surface() {
        let catalog_json = task_catalog_json().expect("task catalog");
        let catalog: serde_json::Value =
            serde_json::from_str(&catalog_json).expect("task catalog json");
        let tasks = catalog["tasks"].as_array().expect("tasks array");
        assert!(tasks.iter().any(|task| {
            task["id"] == "msexplore"
                && task["show_in_tui"] == true
                && task["show_in_swift"] == true
                && task["include_in_suite"] == true
        }));
        assert!(tasks.iter().any(|task| {
            task["id"] == "tablebrowser"
                && task["show_in_tui"] == true
                && task["show_in_swift"] == true
                && task["include_in_suite"] == false
        }));
        assert!(tasks.iter().any(|task| {
            task["id"] == "casars"
                && task["show_in_tui"] == false
                && task["include_in_suite"] == true
        }));
    }

    #[test]
    fn measurement_set_plot_builds_typed_document_without_gui_png_work() {
        let (_dir, ms_path) = unpack_small_ms();

        assert_eq!(
            MeasurementSetPlotPreset::all().len(),
            MsPlotPreset::ALL.len()
        );
        for preset in MeasurementSetPlotPreset::all().iter().copied() {
            let plot = match build_measurement_set_plot(MeasurementSetPlotRequest {
                dataset_path: ms_path.display().to_string(),
                preset,
                field: None,
                spectral_window: None,
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
                data_column: "DATA".to_string(),
                avgchannel: None,
                avgtime: None,
                avgscan: false,
                avgfield: false,
                avgbaseline: false,
                avgantenna: false,
                avgspw: false,
                scalar: false,
                width: DEFAULT_PLOT_WIDTH,
                height: DEFAULT_PLOT_HEIGHT,
                max_plot_points: 10_000,
            }) {
                Ok(plot) => plot,
                Err(FrontendServiceError::Plot { reason })
                    if matches!(
                        preset,
                        MeasurementSetPlotPreset::WeightSpectrumVsTime
                            | MeasurementSetPlotPreset::SigmaSpectrumVsTime
                    ) && (reason.contains("requires") || reason.contains("shape [0, 0]")) =>
                {
                    continue;
                }
                Err(error) => panic!("plot {preset:?}: {error}"),
            };

            assert_eq!(plot.preset, preset);
            assert_eq!(plot.data_column, "data");
            assert_eq!(plot.render.image_format, "none");
            assert_eq!(plot.render.width, DEFAULT_PLOT_WIDTH);
            assert_eq!(plot.render.height, DEFAULT_PLOT_HEIGHT);
            assert!(plot.image_bytes.is_empty());
            assert!(!plot.title.is_empty());
            assert!(!plot.x_axis.label.is_empty());
            assert!(!plot.y_axis.label.is_empty());
            assert_eq!(plot.document.title, plot.title);
            assert!(
                !plot.document.layers.is_empty() || !plot.document.panels.is_empty(),
                "plot document should expose manipulable layers or panels for {preset:?}"
            );
            match preset {
                MeasurementSetPlotPreset::UvCoverage => {
                    assert_eq!(
                        plot.document.layers.len() as u64,
                        plot.sampling.series_count
                    );
                    assert!(
                        plot.document.layers.len() <= 32,
                        "UV coverage should coalesce baseline tracks into coarse display groups"
                    );
                }
                MeasurementSetPlotPreset::AntennaLayout => {
                    let layer = plot.document.layers.first().expect("antenna layer");
                    assert_eq!(layer.x_values.len(), layer.point_labels.len());
                    assert_eq!(layer.x_values.len(), layer.point_symbol_sizes.len());
                }
                MeasurementSetPlotPreset::ScanTimeline
                | MeasurementSetPlotPreset::SpectralWindowCoverage => {
                    assert!(
                        plot.document
                            .layers
                            .iter()
                            .any(|layer| !layer.interval_labels.is_empty()),
                        "interval labels should survive document conversion for {preset:?}"
                    );
                    assert!(
                        plot.document
                            .layers
                            .iter()
                            .any(|layer| !layer.color_group.is_empty()),
                        "interval color groups should survive document conversion for {preset:?}"
                    );
                }
                _ => {}
            }
            if preset == MeasurementSetPlotPreset::AntennaLayout
                || preset == MeasurementSetPlotPreset::ScanTimeline
                || preset == MeasurementSetPlotPreset::SpectralWindowCoverage
            {
                assert!(plot.sampling.diagnostics.iter().any(|line| {
                    line.contains("metadata-oriented plot") || line.contains("no drawable")
                }));
            } else {
                assert!(!plot.series.is_empty());
                assert!(plot.sampling.rendered_point_count > 0);
            }
            assert_eq!(plot.sampling.requested_max_points, 10_000);
            assert!(plot.selection_summary.contains("data column data"));
        }
    }

    #[test]
    #[ignore = "diagnostic timing helper for local tutorial MeasurementSets"]
    fn twhya_measurement_set_plot_timing_diagnostic() {
        let ms_path = std::env::var("CASA_RS_TWHYA_MS")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from("/private/tmp/casa-rs-wave6-prof/twhya_calibrated.ms")
            });
        if !ms_path.is_dir() {
            eprintln!("skipping: {} is not staged", ms_path.display());
            return;
        }
        for preset in MeasurementSetPlotPreset::all().iter().copied() {
            let started = Instant::now();
            let plot = build_measurement_set_plot(MeasurementSetPlotRequest {
                dataset_path: ms_path.display().to_string(),
                preset,
                field: None,
                spectral_window: None,
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
                data_column: "DATA".to_string(),
                avgchannel: None,
                avgtime: None,
                avgscan: false,
                avgfield: false,
                avgbaseline: false,
                avgantenna: false,
                avgspw: false,
                scalar: false,
                width: DEFAULT_PLOT_WIDTH,
                height: DEFAULT_PLOT_HEIGHT,
                max_plot_points: 250_000,
            })
            .unwrap_or_else(|error| panic!("plot {preset:?}: {error}"));
            eprintln!(
                "{preset:?}: total={} ms, points={}, layers={}, panels={}, diagnostics={:?}",
                started.elapsed().as_millis(),
                plot.sampling.rendered_point_count,
                plot.document.layers.len(),
                plot.document.panels.len(),
                plot.sampling.diagnostics
            );
        }

        let ms = MeasurementSet::open(&ms_path).expect("open timing MS");
        let row_count = ms.main_table().row_count();
        let target_rows = 250_000usize / (384 * 2);
        let rows = sampled_indices_for_test(row_count, target_rows);
        let scalars_started = Instant::now();
        let _times = selected_f64_values(ms.main_table(), "TIME", &rows).expect("time rows");
        let _fields = selected_i32_values(ms.main_table(), "FIELD_ID", &rows).expect("field rows");
        let _ddids =
            selected_i32_values(ms.main_table(), "DATA_DESC_ID", &rows).expect("ddid rows");
        let scalars_elapsed = scalars_started.elapsed();
        let flags_started = Instant::now();
        let flags = ms
            .main_table()
            .column_accessor("FLAG")
            .expect("FLAG")
            .array_cells_owned(&rows)
            .expect("flag rows");
        let flags_elapsed = flags_started.elapsed();
        let data_started = Instant::now();
        let data = ms
            .main_table()
            .column_accessor("DATA")
            .expect("DATA")
            .array_cells_owned(&rows)
            .expect("data rows");
        let data_elapsed = data_started.elapsed();
        let scan_started = Instant::now();
        let mut sample_count = 0usize;
        for cell in data {
            sample_count += match cell {
                Some(ArrayValue::Complex32(values)) => values.len(),
                Some(ArrayValue::Complex64(values)) => values.len(),
                _ => 0,
            };
        }
        let scan_elapsed = scan_started.elapsed();
        eprintln!(
            "selected row probe: rows={}, samples={}, scalar={} ms, FLAG={} ms ({} cells), DATA={} ms, sample-scan={} ms",
            rows.len(),
            sample_count,
            scalars_elapsed.as_millis(),
            flags_elapsed.as_millis(),
            flags.len(),
            data_elapsed.as_millis(),
            scan_elapsed.as_millis()
        );
    }

    fn sampled_indices_for_test(total: usize, target: usize) -> Vec<usize> {
        if target >= total {
            return (0..total).collect();
        }
        if target <= 1 {
            return vec![0];
        }
        let step = (total - 1) as f64 / (target - 1) as f64;
        (0..target)
            .map(|index| ((index as f64 * step).round() as usize).min(total - 1))
            .collect()
    }

    #[test]
    fn probe_project_discovers_ms_and_table_without_suffix_guessing() {
        let (dir, ms_path) = unpack_small_ms();
        let table_path = dir.path().join("derived_gain");
        make_table(&table_path);
        fs::write(dir.path().join("notes.txt"), "not a dataset").expect("notes");

        let project = probe_project(dir.path().display().to_string()).expect("project");

        assert!(
            project
                .datasets
                .iter()
                .any(|dataset| dataset.path == ms_path.display().to_string()
                    && dataset.kind == DatasetKind::MeasurementSet)
        );
        assert!(
            project
                .datasets
                .iter()
                .any(|dataset| dataset.path == table_path.display().to_string()
                    && dataset.kind == DatasetKind::Table)
        );
        assert!(
            !project
                .datasets
                .iter()
                .any(|dataset| dataset.name == "notes.txt")
        );
    }

    #[test]
    fn probe_image_reports_science_units_and_observing_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("restored.image");
        let mut coords = CoordinateSystem::new();
        let cell_rad = (0.1_f64 / 3600.0).to_radians();
        coords.add_coordinate(Box::new(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, -0.5],
            [-cell_rad, cell_rad],
            [1.5, 1.5],
        )));
        coords.add_coordinate(Box::new(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            100.0e9,
            2.0e6,
            2.0,
            100.0e9,
        )));
        let mut image =
            PagedImage::<f32>::create(vec![4, 4, 5], coords, &path).expect("create image");
        image.set_units("Jy/beam").expect("set units");
        image
            .set_image_info(&ImageInfo {
                image_type: ImageType::Intensity,
                object_name: "TW Hya".to_string(),
                beam_set: ImageBeamSet::new(GaussianBeam::new(
                    (0.42_f64 / 3600.0).to_radians(),
                    (0.31_f64 / 3600.0).to_radians(),
                    12.0_f64.to_radians(),
                )),
            })
            .expect("set image info");
        image.save().expect("save image");

        let probe = probe_path(path.display().to_string())
            .expect("probe")
            .expect("recognized");

        assert_eq!(probe.kind, DatasetKind::Image);
        assert_eq!(probe.units, "Jy/beam");
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Pixel type: float32")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Cell size: 0.1 x 0.1 arcsec")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line.starts_with("Center: RA ") && line.contains(" Dec "))
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Cube center frequency: 100 GHz")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Total bandwidth: 10 MHz")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Channel separation: 2 MHz")
        );
        assert!(
            probe
                .diagnostics
                .iter()
                .any(|line| line == "Beam: 0.42 x 0.31 arcsec, PA 12 deg")
        );

        let plane_snapshot_json = build_image_explorer_snapshot_json(
            path.display().to_string(),
            100,
            32,
            8,
            128,
            96,
            Some("plane".to_string()),
        )
        .expect("image explorer plane snapshot");
        let plane_snapshot: serde_json::Value =
            serde_json::from_str(&plane_snapshot_json).expect("plane snapshot json");
        assert_eq!(plane_snapshot["active_view"], "plane");
        assert_eq!(plane_snapshot["capabilities"]["renderable_plane"], true);
        assert!(
            plane_snapshot["plane"]["width"]
                .as_u64()
                .is_some_and(|width| width > 0)
        );
        assert!(
            plane_snapshot["plane"]["height"]
                .as_u64()
                .is_some_and(|height| height > 0)
        );

        let snapshot_json = build_image_explorer_snapshot_json(
            path.display().to_string(),
            100,
            32,
            8,
            128,
            96,
            Some("spectrum".to_string()),
        )
        .expect("image explorer snapshot");
        let snapshot: serde_json::Value =
            serde_json::from_str(&snapshot_json).expect("snapshot json");
        assert_eq!(snapshot["active_view"], "spectrum");
        assert!(snapshot["profile"]["samples"].as_array().is_some());

        let request_json = serde_json::json!({
            "dataset_path": path.display().to_string(),
            "width": 100,
            "height": 32,
            "inspector_height": 8,
            "plane_pixel_width": 128,
            "plane_pixel_height": 96,
            "active_view": "plane",
            "focus": "inspector",
            "plane_content_mode": "spreadsheet",
            "parameters": {
                "blc": "0,0,0",
                "trc": "3,3,0",
                "inc": "1,1,1",
                "stretch": "percentile99",
                "autoscale": "per_plane",
                "clip_low": "",
                "clip_high": ""
            },
            "cursor_x": 1,
            "cursor_y": 1,
            "selected_profile_axis": 0,
            "non_display_indices": [],
            "include_profile": true
        })
        .to_string();
        let requested_snapshot_json = build_image_explorer_snapshot_from_request_json(request_json)
            .expect("image explorer requested snapshot");
        let requested_snapshot: serde_json::Value =
            serde_json::from_str(&requested_snapshot_json).expect("requested snapshot json");
        assert_eq!(requested_snapshot["active_view"], "plane");
        assert_eq!(requested_snapshot["focus"], "inspector");
        assert_eq!(requested_snapshot["plane_cursor"]["pixel_x"], 1);
        assert_eq!(requested_snapshot["plane_cursor"]["pixel_y"], 1);
    }

    #[test]
    fn table_browser_snapshot_json_uses_tablebrowser_protocol() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let snapshot_json = build_table_browser_snapshot_json(
            table_path.display().to_string(),
            100,
            24,
            8,
            Some("columns".to_string()),
        )
        .expect("table browser snapshot");
        let snapshot: serde_json::Value =
            serde_json::from_str(&snapshot_json).expect("snapshot json");
        assert_eq!(snapshot["view"], "columns");
        assert_eq!(snapshot["capabilities"]["editable"], false);
        assert_eq!(snapshot["table_path"], table_path.display().to_string());
        assert!(
            snapshot["content_lines"]
                .as_array()
                .is_some_and(|lines| !lines.is_empty())
        );
    }

    #[test]
    fn table_browser_request_json_replays_navigation_commands() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let request_json = serde_json::json!({
            "dataset_path": table_path,
            "selected_view": "columns",
            "focus": "main",
            "commands": [
                { "command": "move_down", "steps": 1 }
            ]
        })
        .to_string();
        let snapshot_json = build_table_browser_snapshot_from_request_json(request_json)
            .expect("table browser requested snapshot");
        let snapshot: serde_json::Value =
            serde_json::from_str(&snapshot_json).expect("snapshot json");
        assert_eq!(snapshot["view"], "columns");
        assert_eq!(snapshot["focus"], "main");
        assert_eq!(
            snapshot["vertical_metrics"]["selected_index"]
                .as_u64()
                .expect("selected index"),
            1
        );
        assert!(snapshot["selected_address"].is_object());
        assert!(snapshot["inspector"].is_object());
    }

    #[test]
    fn table_browser_cell_window_json_returns_typed_scroll_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("gain_table");
        make_table(&table_path);

        let request_json = serde_json::json!({
            "dataset_path": table_path,
            "row_start": 0,
            "row_limit": 4,
            "column_start": 1,
            "column_limit": 1
        })
        .to_string();
        let window_json =
            build_table_browser_cell_window_json(request_json).expect("table browser cell window");
        let window: serde_json::Value =
            serde_json::from_str(&window_json).expect("cell window json");

        assert_eq!(window["row_count"].as_u64(), Some(1));
        assert_eq!(window["column_count"].as_u64(), Some(2));
        assert_eq!(window["row_start"].as_u64(), Some(0));
        assert_eq!(window["column_start"].as_u64(), Some(1));
        assert_eq!(window["columns"][0]["name"], "id");
        assert_eq!(window["columns"][1]["header"], "name str");
        assert_eq!(window["rows"][0]["index"].as_u64(), Some(0));
        assert_eq!(
            window["rows"][0]["cells"][0]["column_index"].as_u64(),
            Some(1)
        );
        assert_eq!(window["rows"][0]["cells"][0]["display"], "\"gain\"");
        assert_eq!(window["rows"][0]["cells"][0]["defined"], true);
    }

    #[test]
    fn table_browser_cell_window_summarizes_schema_array_without_preview_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let table_path = dir.path().join("array_table");
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "DATA",
            PrimitiveType::Float32,
            vec![2, 2],
        )])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        let mut keywords = RecordValue::default();
        keywords.upsert(
            "MEASINFO",
            Value::Record(RecordValue::new(vec![RecordField::new(
                "type",
                Value::Scalar(ScalarValue::String("visibility".to_string())),
            )])),
        );
        table.set_column_keywords("DATA", keywords);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "DATA",
                Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![2, 2], vec![1.0, 2.0, 3.0, 4.0]).expect("array"),
                )),
            )]))
            .expect("row");
        table
            .save(TableOptions::new(&table_path))
            .expect("save table");

        let request_json = serde_json::json!({
            "dataset_path": table_path,
            "row_start": 0,
            "row_limit": 1,
            "column_start": 0,
            "column_limit": 1,
            "column_options": [{
                "column_index": 0,
                "array_inline_limit": 4
            }]
        })
        .to_string();
        let window_json =
            build_table_browser_cell_window_json(request_json).expect("table browser cell window");
        let window: serde_json::Value =
            serde_json::from_str(&window_json).expect("cell window json");

        assert_eq!(
            window["columns"][0]["keywords"][0],
            "MEASINFO.type = \"visibility\""
        );
        assert_eq!(
            window["rows"][0]["cells"][0]["display"],
            "[1.0000, 2.0000, 3.0000, 4.0000]"
        );

        let copy_request = serde_json::json!({
            "dataset_path": table_path,
            "row_index": 0,
            "column_index": 0
        })
        .to_string();
        let copy_json =
            build_table_browser_cell_value_json(copy_request).expect("table browser cell value");
        let copy_value: String = serde_json::from_str(&copy_json).expect("cell value json");
        assert_eq!(copy_value, "[1.0000, 2.0000, 3.0000, 4.0000]");
    }

    #[test]
    fn unrecognized_path_returns_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("plain.dat");
        fs::write(&file, "plain").expect("write");

        assert!(
            probe_path(file.display().to_string())
                .expect("probe")
                .is_none()
        );
    }
}
