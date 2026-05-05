// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared frontend services exposed to Swift and Python through UniFFI.

use std::collections::BTreeSet;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use casa_coordinates::{CoordinateSystem, CoordinateType};
use casa_images::{AnyPagedImage, ImageInfo, ImagePixelType};
use casa_ms::{
    MeasurementSet, MeasurementSetPlotPayload, MeasurementSetPlotTheme,
    MeasurementSetSummaryOutputFormat, MsExploreSpec, MsPageExportRange, MsPlotPayload,
    MsPlotPreset, MsPlotSpec, MsScatterGridPayload, MsScatterPagePayload, MsScatterPlotPayload,
    MsScatterSeries, MsSelectionSpec, VisibilityDataColumn, build_msexplore_payload_from_spec,
    render_msexplore_plot_image,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::direction::{
    angular_increment_arcseconds, declination_increment_arcseconds, format_declination_labeled,
    format_right_ascension_labeled,
};
use image::ImageFormat;
use thiserror::Error;

const MAX_PROJECT_SCAN_ENTRIES: usize = 512;
const MAX_PROJECT_SCAN_DEPTH: usize = 4;
const DEFAULT_GUI_MAX_PLOT_POINTS: u64 = 250_000;
#[cfg(test)]
const DEFAULT_PLOT_WIDTH: u32 = 960;
#[cfg(test)]
const DEFAULT_PLOT_HEIGHT: u32 = 600;

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
    pub antennas: Vec<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum MeasurementSetPlotPreset {
    UvCoverage,
    AmplitudeVsFrequency,
    AmplitudeVsChannel,
    AmplitudeVsUvDistance,
    AmplitudeVsTime,
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct MeasurementSetPlotRequest {
    pub dataset_path: String,
    pub preset: MeasurementSetPlotPreset,
    pub field: Option<String>,
    pub spectral_window: Option<String>,
    pub correlation: Option<String>,
    pub data_column: String,
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
}

type FrontendResult<T> = Result<T, FrontendServiceError>;

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
    if matches!(
        request.preset,
        MeasurementSetPlotPreset::AmplitudeVsChannel
            | MeasurementSetPlotPreset::AmplitudeVsFrequency
    ) {
        plot.averaging.avgchannel = Some(4);
    }

    let selection = MsSelectionSpec {
        field: normalized_optional(request.field.clone()),
        spw: normalized_optional(request.spectral_window.clone()),
        correlation: normalized_optional(request.correlation.clone()),
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

    let payload =
        build_msexplore_payload_from_spec(&spec).map_err(|error| FrontendServiceError::Plot {
            reason: format!("{}: {error}", dataset_path.display()),
        })?;
    let metadata = plot_payload_metadata(&payload, request.preset, max_plot_points);
    let image =
        render_msexplore_plot_image(&payload, MeasurementSetPlotTheme::light(), width, height)
            .map_err(|error| FrontendServiceError::Plot {
                reason: format!("render {}: {error}", dataset_path.display()),
            })?;
    let mut encoded = Cursor::new(Vec::new());
    image
        .write_to(&mut encoded, ImageFormat::Png)
        .map_err(|error| FrontendServiceError::Plot {
            reason: format!("encode PNG {}: {error}", dataset_path.display()),
        })?;

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
        render: PlotRenderProvenance {
            renderer: "casa-ms msexplore plotters PNG".to_string(),
            image_format: "png".to_string(),
            width,
            height,
            source: "Rust casa-ms MeasurementSet payload rendered through UniFFI".to_string(),
        },
        image_bytes: encoded.into_inner(),
    })
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
        MeasurementSetPlotPreset::AmplitudeVsFrequency => MsPlotPreset::AmplitudeVsFrequency,
        MeasurementSetPlotPreset::AmplitudeVsChannel => MsPlotPreset::AmplitudeVsChannel,
        MeasurementSetPlotPreset::AmplitudeVsUvDistance => MsPlotPreset::AmplitudeVsUvDistance,
        MeasurementSetPlotPreset::AmplitudeVsTime => MsPlotPreset::AmplitudeVsTime,
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
            PayloadMetadata {
                title: "UV Coverage".to_string(),
                summary: payload.summary.clone(),
                x_axis: axis_metadata("u", "u (kλ)"),
                y_axis: axis_metadata("v", "v (kλ)"),
                series: payload
                    .tracks
                    .iter()
                    .map(|track| PlotSeriesMetadata {
                        label: track.label.clone(),
                        color_group: "uv-track".to_string(),
                        point_count: track.points.len() as u64,
                        first_row: None,
                        last_row: None,
                    })
                    .collect(),
                sampling: PlotSamplingDiagnostics {
                    requested_max_points,
                    rendered_point_count,
                    series_count: payload.tracks.len() as u64,
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
        scans: vec![],
        antennas,
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
        antennas: vec![],
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
        antennas: vec![],
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
    use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use flate2::read::GzDecoder;
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
    fn measurement_set_plot_builds_real_png_and_typed_metadata() {
        let (_dir, ms_path) = unpack_small_ms();

        for preset in [
            MeasurementSetPlotPreset::UvCoverage,
            MeasurementSetPlotPreset::AmplitudeVsFrequency,
            MeasurementSetPlotPreset::AmplitudeVsUvDistance,
        ] {
            let plot = build_measurement_set_plot(MeasurementSetPlotRequest {
                dataset_path: ms_path.display().to_string(),
                preset,
                field: None,
                spectral_window: None,
                correlation: None,
                data_column: "DATA".to_string(),
                width: DEFAULT_PLOT_WIDTH,
                height: DEFAULT_PLOT_HEIGHT,
                max_plot_points: 10_000,
            })
            .expect("plot");

            assert_eq!(plot.preset, preset);
            assert_eq!(plot.data_column, "data");
            assert_eq!(plot.render.image_format, "png");
            assert_eq!(plot.render.width, DEFAULT_PLOT_WIDTH);
            assert_eq!(plot.render.height, DEFAULT_PLOT_HEIGHT);
            assert!(plot.image_bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
            assert!(!plot.title.is_empty());
            assert!(!plot.x_axis.label.is_empty());
            assert!(!plot.y_axis.label.is_empty());
            assert!(!plot.series.is_empty());
            assert!(plot.sampling.rendered_point_count > 0);
            assert_eq!(plot.sampling.requested_max_points, 10_000);
            assert!(plot.selection_summary.contains("data column data"));
        }
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
