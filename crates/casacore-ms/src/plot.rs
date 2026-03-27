// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared plot specification, payload building, rendering, and export support
//! for `listobs`.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::path::Path;

use image::{DynamicImage, ImageFormat, RgbImage};
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;
use printpdf::{Mm, Op, PdfDocument, PdfPage, PdfSaveOptions, Pt, RawImage, XObjectTransform};
use serde::{Deserialize, Serialize};

use crate::{ListObsSummary, ListObsUvCoverage};

const EXPORT_DPI: f32 = 72.0;

/// Stable plot identifiers supported by `listobs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListObsPlotKind {
    /// UV-coverage plot built from grouped UVW samples.
    UvCoverage,
    /// ANTENNA subtable layout plot.
    AntennaLayout,
    /// MAIN-table scan timeline plot.
    ScanTimeline,
    /// SPECTRAL_WINDOW frequency-coverage plot.
    SpectralWindowCoverage,
}

impl ListObsPlotKind {
    /// All plot kinds shipped in the first `listobs` plot workspace wave.
    pub const ALL: [Self; 4] = [
        Self::UvCoverage,
        Self::AntennaLayout,
        Self::ScanTimeline,
        Self::SpectralWindowCoverage,
    ];

    /// Stable machine-readable identifier used by CLI and serialized specs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UvCoverage => "uv_coverage",
            Self::AntennaLayout => "antenna_layout",
            Self::ScanTimeline => "scan_timeline",
            Self::SpectralWindowCoverage => "spectral_window_coverage",
        }
    }

    /// Human-readable label suitable for TUI catalogs and export filenames.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::UvCoverage => "UV Coverage",
            Self::AntennaLayout => "Antenna Layout",
            Self::ScanTimeline => "Scan Timeline",
            Self::SpectralWindowCoverage => "Spectral Window Coverage",
        }
    }

    /// Parse a stable CLI / serialized identifier.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "uv_coverage" | "uv" => Ok(Self::UvCoverage),
            "antenna_layout" | "antennas" => Ok(Self::AntennaLayout),
            "scan_timeline" | "scans" => Ok(Self::ScanTimeline),
            "spectral_window_coverage" | "spw_coverage" | "spws" => {
                Ok(Self::SpectralWindowCoverage)
            }
            other => Err(format!(
                "unsupported plot kind {other:?}; expected one of: uv_coverage, antenna_layout, scan_timeline, spectral_window_coverage"
            )),
        }
    }

    /// Build the default plot specification for this kind.
    pub fn default_spec(self) -> ListObsPlotSpec {
        let mut options = BTreeMap::new();
        match self {
            Self::UvCoverage => {
                options.insert("draw_mode".to_string(), "tracks".to_string());
                options.insert("mirror".to_string(), "on".to_string());
                options.insert("axis_extent".to_string(), "auto".to_string());
            }
            Self::AntennaLayout => {
                options.insert("labels".to_string(), "name".to_string());
                options.insert("coordinates".to_string(), "offset".to_string());
                options.insert("size_by_diameter".to_string(), "on".to_string());
            }
            Self::ScanTimeline => {
                options.insert("lanes".to_string(), "scan".to_string());
                options.insert("color_by".to_string(), "field".to_string());
                options.insert("labels".to_string(), "scan".to_string());
            }
            Self::SpectralWindowCoverage => {
                options.insert("unit".to_string(), "ghz".to_string());
                options.insert("labels".to_string(), "on".to_string());
                options.insert("color_by".to_string(), "spw".to_string());
            }
        }
        ListObsPlotSpec {
            kind: self,
            options,
        }
    }
}

impl fmt::Display for ListObsPlotKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Supported file formats for saved plots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListObsPlotExportFormat {
    /// Save the rendered bitmap as `PNG`.
    Png,
    /// Save the rendered bitmap inside a single-page raster-backed `PDF`.
    Pdf,
}

impl ListObsPlotExportFormat {
    /// Parse a CLI / serialized export format.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "png" => Ok(Self::Png),
            "pdf" => Ok(Self::Pdf),
            other => Err(format!(
                "unsupported plot format {other:?}; expected one of: png, pdf"
            )),
        }
    }

    /// Conventional lowercase filename extension without a leading dot.
    pub fn extension(self) -> &'static str {
        match self {
            Self::Png => "png",
            Self::Pdf => "pdf",
        }
    }
}

/// Serializable plot specification shared by the CLI and TUI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListObsPlotSpec {
    /// Selected plot kind.
    pub kind: ListObsPlotKind,
    /// Plot-specific options encoded as stable string pairs.
    pub options: BTreeMap<String, String>,
}

impl ListObsPlotSpec {
    /// Create a default specification for the chosen plot kind.
    pub fn new(kind: ListObsPlotKind) -> Self {
        kind.default_spec()
    }

    /// Return one option value, if present.
    pub fn option(&self, key: &str) -> Option<&str> {
        self.options.get(key).map(String::as_str)
    }

    /// Update one option after validating it against this plot kind.
    pub fn set_option(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), String> {
        let key = key.into();
        let value = value.into();
        validate_option(self.kind, &key, &value)?;
        self.options.insert(key, value);
        Ok(())
    }

    /// Apply repeated CLI `key=value` assignments on top of the plot defaults.
    pub fn from_cli_assignments(
        kind: ListObsPlotKind,
        assignments: &[String],
    ) -> Result<Self, String> {
        let mut spec = Self::new(kind);
        for assignment in assignments {
            let Some((key, value)) = assignment.split_once('=') else {
                return Err(format!(
                    "invalid --plot-option {assignment:?}; expected key=value"
                ));
            };
            spec.set_option(key.trim(), value.trim())?;
        }
        Ok(spec)
    }

    /// Render the current options as sorted `key=value` pairs for CLI output.
    pub fn cli_assignments(&self) -> Vec<String> {
        self.options
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect()
    }
}

/// Simple RGB theme used by both TUI image rendering and file export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListObsPlotTheme {
    /// Background color.
    pub background: [u8; 3],
    /// Axis / outline color.
    pub axis: [u8; 3],
    /// Secondary label color.
    pub label: [u8; 3],
    /// Grid color.
    pub grid: [u8; 3],
    /// Accent palette used for series / bars / points.
    pub accents: [[u8; 3]; 6],
}

impl ListObsPlotTheme {
    /// Light export-oriented theme.
    pub fn light() -> Self {
        Self {
            background: [248, 246, 239],
            axis: [38, 45, 55],
            label: [87, 94, 103],
            grid: [207, 213, 220],
            accents: [
                [32, 111, 145],
                [191, 96, 58],
                [83, 143, 61],
                [122, 83, 161],
                [191, 146, 29],
                [71, 93, 168],
            ],
        }
    }

    /// Dark terminal-oriented theme.
    pub fn dark() -> Self {
        Self {
            background: [16, 18, 20],
            axis: [210, 214, 220],
            label: [170, 176, 186],
            grid: [64, 72, 82],
            accents: [
                [88, 196, 221],
                [244, 167, 89],
                [136, 207, 98],
                [182, 152, 229],
                [250, 219, 94],
                [118, 143, 226],
            ],
        }
    }
}

/// Typed data prepared for one plot render / export.
#[derive(Debug, Clone, PartialEq)]
pub enum ListObsPlotPayload {
    /// UV-coverage series grouped by baseline / field / SPW.
    UvCoverage(UvCoveragePlotPayload),
    /// Antenna layout scatter payload.
    AntennaLayout(AntennaLayoutPlotPayload),
    /// Scan timeline bar payload.
    ScanTimeline(ScanTimelinePlotPayload),
    /// Spectral window coverage bar payload.
    SpectralWindowCoverage(SpectralWindowCoveragePlotPayload),
}

impl ListObsPlotPayload {
    /// Return the plot kind represented by this payload.
    pub fn kind(&self) -> ListObsPlotKind {
        match self {
            Self::UvCoverage(_) => ListObsPlotKind::UvCoverage,
            Self::AntennaLayout(_) => ListObsPlotKind::AntennaLayout,
            Self::ScanTimeline(_) => ListObsPlotKind::ScanTimeline,
            Self::SpectralWindowCoverage(_) => ListObsPlotKind::SpectralWindowCoverage,
        }
    }
}

/// UV-coverage render payload.
#[derive(Debug, Clone, PartialEq)]
pub struct UvCoveragePlotPayload {
    /// Draw isolated points instead of joined tracks.
    pub draw_points: bool,
    /// Mirror `(-u, -v)` during rendering.
    pub mirror: bool,
    /// Symmetric axis extent in lambda units.
    pub axis_extent_lambda: f64,
    /// Grouped tracks.
    pub tracks: Vec<UvCoverageSeries>,
    /// Render summary.
    pub summary: String,
}

/// One UV-coverage track / point series.
#[derive(Debug, Clone, PartialEq)]
pub struct UvCoverageSeries {
    /// Stable label for hover/debug/export summaries.
    pub label: String,
    /// Plot points in lambda units.
    pub points: Vec<(f64, f64)>,
}

/// Antenna-layout render payload.
#[derive(Debug, Clone, PartialEq)]
pub struct AntennaLayoutPlotPayload {
    /// X-axis label.
    pub x_label: String,
    /// Y-axis label.
    pub y_label: String,
    /// Render labels next to points when present.
    pub labels_enabled: bool,
    /// Scatter points.
    pub antennas: Vec<AntennaLayoutPoint>,
    /// Render summary.
    pub summary: String,
}

/// One antenna point in the layout plot.
#[derive(Debug, Clone, PartialEq)]
pub struct AntennaLayoutPoint {
    /// Point label shown when labels are enabled.
    pub label: String,
    /// X coordinate.
    pub x: f64,
    /// Y coordinate.
    pub y: f64,
    /// Relative marker size.
    pub marker_radius: i32,
}

/// Scan-timeline render payload.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanTimelinePlotPayload {
    /// X-axis minimum in MJD seconds.
    pub start_mjd_seconds: f64,
    /// X-axis maximum in MJD seconds.
    pub end_mjd_seconds: f64,
    /// Bars to render.
    pub bars: Vec<ScanTimelineBar>,
    /// Lane labels in Y-axis order.
    pub lane_labels: Vec<String>,
    /// Render summary.
    pub summary: String,
}

/// One scan bar within the timeline.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanTimelineBar {
    /// Inclusive lane index.
    pub lane: usize,
    /// Bar start in MJD seconds.
    pub start_mjd_seconds: f64,
    /// Bar end in MJD seconds.
    pub end_mjd_seconds: f64,
    /// Series label.
    pub label: String,
    /// Color group key.
    pub color_group: String,
}

/// Spectral-window coverage render payload.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralWindowCoveragePlotPayload {
    /// X-axis label.
    pub x_label: String,
    /// Coverage bars.
    pub bars: Vec<SpectralWindowCoverageBar>,
    /// Render summary.
    pub summary: String,
}

/// One spectral-window bar.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralWindowCoverageBar {
    /// Y-axis row index.
    pub lane: usize,
    /// Low-frequency edge.
    pub start: f64,
    /// High-frequency edge.
    pub end: f64,
    /// Series label.
    pub label: String,
    /// Color group key.
    pub color_group: String,
}

/// Build one plot payload from structured summary data.
pub fn build_listobs_plot_payload_from_summary(
    summary: &ListObsSummary,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    match spec.kind {
        ListObsPlotKind::UvCoverage => Err(
            "UV coverage requires ListObsUvCoverage data; build it with build_listobs_uv_plot_payload"
                .to_string(),
        ),
        ListObsPlotKind::AntennaLayout => build_antenna_layout_payload(summary, spec),
        ListObsPlotKind::ScanTimeline => build_scan_timeline_payload(summary, spec),
        ListObsPlotKind::SpectralWindowCoverage => {
            build_spectral_window_coverage_payload(summary, spec)
        }
    }
}

/// Build a UV-coverage plot payload from grouped UV data.
pub fn build_listobs_uv_plot_payload(
    coverage: &ListObsUvCoverage,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    if spec.kind != ListObsPlotKind::UvCoverage {
        return Err(format!(
            "plot spec kind {} does not match UV coverage payload builder",
            spec.kind
        ));
    }
    let draw_points = match spec.option("draw_mode").unwrap_or("tracks") {
        "tracks" => false,
        "points" => true,
        value => return Err(format!("invalid draw_mode {value:?} for uv_coverage")),
    };
    let mirror = parse_on_off(spec.option("mirror").unwrap_or("on"), "mirror")?;
    let axis_extent_lambda = match spec.option("axis_extent").unwrap_or("auto") {
        "auto" => coverage.max_abs_uv_lambda.max(1.0),
        value => value
            .parse::<f64>()
            .map_err(|_| {
                format!("invalid axis_extent {value:?}; expected auto or numeric lambda extent")
            })?
            .max(1.0),
    };
    Ok(ListObsPlotPayload::UvCoverage(UvCoveragePlotPayload {
        draw_points,
        mirror,
        axis_extent_lambda,
        tracks: coverage
            .tracks
            .iter()
            .map(|track| UvCoverageSeries {
                label: format!(
                    "a{}-a{} field {} spw {}",
                    track.antenna1, track.antenna2, track.field_id, track.spectral_window_id
                ),
                points: track
                    .samples
                    .iter()
                    .map(|sample| (sample.u_lambda, sample.v_lambda))
                    .collect(),
            })
            .collect(),
        summary: format!(
            "UV coverage. Tracks={} Samples={} Max |u,v|={:.1} λ",
            coverage.tracks.len(),
            coverage.sample_count,
            coverage.max_abs_uv_lambda
        ),
    }))
}

/// Render one plot payload into a bitmap image.
pub fn render_listobs_plot_image(
    payload: &ListObsPlotPayload,
    theme: ListObsPlotTheme,
    width: u32,
    height: u32,
) -> Result<DynamicImage, String> {
    if width == 0 || height == 0 {
        return Err("plot size must be non-zero".to_string());
    }
    let mut buffer = vec![0u8; (width as usize) * (height as usize) * 3];
    let backend = BitMapBackend::with_buffer(&mut buffer, (width, height));
    let root = backend.into_drawing_area();
    root.fill(&rgb(theme.background))
        .map_err(|error| error.to_string())?;

    match payload {
        ListObsPlotPayload::UvCoverage(payload) => render_uv_coverage_plot(&root, payload, theme)?,
        ListObsPlotPayload::AntennaLayout(payload) => {
            render_antenna_layout_plot(&root, payload, theme)?
        }
        ListObsPlotPayload::ScanTimeline(payload) => {
            render_scan_timeline_plot(&root, payload, theme)?
        }
        ListObsPlotPayload::SpectralWindowCoverage(payload) => {
            render_spectral_window_coverage_plot(&root, payload, theme)?
        }
    }

    root.present().map_err(|error| error.to_string())?;
    drop(root);
    let image = RgbImage::from_raw(width, height, buffer)
        .ok_or_else(|| "failed to assemble rendered plot image".to_string())?;
    Ok(DynamicImage::ImageRgb8(image))
}

/// Export a plot payload as `PNG` or raster-backed single-page `PDF`.
pub fn export_listobs_plot(
    payload: &ListObsPlotPayload,
    theme: ListObsPlotTheme,
    output_path: &Path,
    format: ListObsPlotExportFormat,
    width: u32,
    height: u32,
) -> Result<(), String> {
    let image = render_listobs_plot_image(payload, theme, width, height)?;
    match format {
        ListObsPlotExportFormat::Png => image
            .save_with_format(output_path, ImageFormat::Png)
            .map_err(|error| error.to_string()),
        ListObsPlotExportFormat::Pdf => export_plot_pdf(&image, output_path, payload.kind()),
    }
}

fn export_plot_pdf(
    image: &DynamicImage,
    output_path: &Path,
    kind: ListObsPlotKind,
) -> Result<(), String> {
    let mut png_bytes = Vec::new();
    image
        .write_to(&mut Cursor::new(&mut png_bytes), ImageFormat::Png)
        .map_err(|error| error.to_string())?;
    let raw = RawImage::decode_from_bytes(&png_bytes, &mut Vec::new())
        .map_err(|error| error.to_string())?;

    let page_width_mm = Mm((image.width() as f32) * 25.4 / EXPORT_DPI);
    let page_height_mm = Mm((image.height() as f32) * 25.4 / EXPORT_DPI);

    let mut document = PdfDocument::new(kind.display_name());
    let image_id = document.add_image(&raw);
    let page = PdfPage::new(
        page_width_mm,
        page_height_mm,
        vec![Op::UseXobject {
            id: image_id,
            transform: XObjectTransform {
                translate_x: Some(Pt(0.0)),
                translate_y: Some(Pt(0.0)),
                rotate: None,
                scale_x: None,
                scale_y: None,
                dpi: Some(EXPORT_DPI),
            },
        }],
    );
    let bytes = document
        .with_pages(vec![page])
        .save(&PdfSaveOptions::default(), &mut Vec::new());
    std::fs::write(output_path, bytes).map_err(|error| error.to_string())
}

fn build_antenna_layout_payload(
    summary: &ListObsSummary,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    let labels = spec.option("labels").unwrap_or("name");
    let labels_enabled = labels != "off";
    let coordinates = spec.option("coordinates").unwrap_or("offset");
    let size_by_diameter = parse_on_off(
        spec.option("size_by_diameter").unwrap_or("on"),
        "size_by_diameter",
    )?;
    let mut omitted = 0usize;
    let mut antennas = Vec::<(AntennaLayoutPoint, String, String)>::new();
    for antenna in &summary.antennas {
        let (x, y, x_label, y_label) = if coordinates == "absolute" {
            (
                antenna.position_m[0],
                antenna.position_m[1],
                "ITRF X (m)",
                "ITRF Y (m)",
            )
        } else {
            (
                antenna.offset_from_observatory_m[0],
                antenna.offset_from_observatory_m[1],
                "East Offset (m)",
                "North Offset (m)",
            )
        };
        if !x.is_finite() || !y.is_finite() {
            omitted += 1;
            continue;
        }
        let label = match labels {
            "off" => String::new(),
            "id" => antenna.antenna_id.to_string(),
            "name" => antenna.name.clone(),
            value => return Err(format!("invalid labels {value:?} for antenna_layout")),
        };
        antennas.push((
            AntennaLayoutPoint {
                label,
                x,
                y,
                marker_radius: if size_by_diameter {
                    ((antenna.dish_diameter_m / 3.0).round() as i32).clamp(3, 12)
                } else {
                    5
                },
            },
            x_label.to_string(),
            y_label.to_string(),
        ));
    }
    if antennas.is_empty() {
        return Err(format!(
            "Antenna layout requires finite {} coordinates in the ANTENNA table.",
            if coordinates == "absolute" {
                "ITRF"
            } else {
                "offset"
            }
        ));
    }
    let x_label = antennas
        .first()
        .map(|entry| entry.1.clone())
        .unwrap_or_else(|| "X".to_string());
    let y_label = antennas
        .first()
        .map(|entry| entry.2.clone())
        .unwrap_or_else(|| "Y".to_string());
    Ok(ListObsPlotPayload::AntennaLayout(
        AntennaLayoutPlotPayload {
            x_label,
            y_label,
            labels_enabled,
            antennas: antennas.into_iter().map(|entry| entry.0).collect(),
            summary: if omitted == 0 {
                format!("Antenna layout. Antennas={}", summary.antennas.len())
            } else {
                format!(
                    "Antenna layout. Antennas={} ({} omitted without finite coordinates)",
                    summary.antennas.len() - omitted,
                    omitted
                )
            },
        },
    ))
}

fn build_scan_timeline_payload(
    summary: &ListObsSummary,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    let lanes = spec.option("lanes").unwrap_or("scan");
    let color_by = spec.option("color_by").unwrap_or("field");
    let labels = spec.option("labels").unwrap_or("scan");
    let mut lane_lookup = BTreeMap::<String, usize>::new();
    let mut bars = Vec::with_capacity(summary.scans.len());
    let mut start = f64::INFINITY;
    let mut end = f64::NEG_INFINITY;

    for scan in &summary.scans {
        let lane_label = match lanes {
            "scan" => format!("Scan {}", scan.scan_number),
            "field" => scan.field_name.clone(),
            value => return Err(format!("invalid lanes {value:?} for scan_timeline")),
        };
        let next_lane = lane_lookup.len();
        let lane = *lane_lookup.entry(lane_label.clone()).or_insert(next_lane);
        let color_group = match color_by {
            "field" => scan.field_name.clone(),
            "intent" => scan
                .scan_intents
                .first()
                .cloned()
                .unwrap_or_else(|| "none".to_string()),
            value => return Err(format!("invalid color_by {value:?} for scan_timeline")),
        };
        let label = match labels {
            "none" => String::new(),
            "scan" => format!("Scan {}", scan.scan_number),
            "field" => scan.field_name.clone(),
            value => return Err(format!("invalid labels {value:?} for scan_timeline")),
        };
        start = start.min(scan.start_mjd_seconds);
        end = end.max(scan.end_mjd_seconds);
        bars.push(ScanTimelineBar {
            lane,
            start_mjd_seconds: scan.start_mjd_seconds,
            end_mjd_seconds: scan.end_mjd_seconds.max(scan.start_mjd_seconds + 1e-6),
            label,
            color_group,
        });
    }

    if !start.is_finite() || !end.is_finite() {
        start = 0.0;
        end = 1.0;
    }

    let mut lane_labels = vec![String::new(); lane_lookup.len()];
    for (label, lane) in lane_lookup {
        lane_labels[lane] = label;
    }

    Ok(ListObsPlotPayload::ScanTimeline(ScanTimelinePlotPayload {
        start_mjd_seconds: start,
        end_mjd_seconds: if end > start { end } else { start + 1.0 },
        bars,
        lane_labels,
        summary: format!("Scan timeline. Scans={}", summary.scans.len()),
    }))
}

fn build_spectral_window_coverage_payload(
    summary: &ListObsSummary,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    let unit = spec.option("unit").unwrap_or("ghz");
    let scale = match unit {
        "ghz" => (1.0e9, "Frequency (GHz)"),
        "mhz" => (1.0e6, "Frequency (MHz)"),
        value => {
            return Err(format!(
                "invalid unit {value:?} for spectral_window_coverage"
            ));
        }
    };
    let labels = parse_on_off(spec.option("labels").unwrap_or("on"), "labels")?;
    let color_by = spec.option("color_by").unwrap_or("spw");

    let bars = summary
        .spectral_windows
        .iter()
        .enumerate()
        .map(|(lane, spw)| {
            let label = if labels {
                if spw.name.trim().is_empty() {
                    format!("SPW {}", spw.spectral_window_id)
                } else {
                    format!("SPW {} {}", spw.spectral_window_id, spw.name)
                }
            } else {
                String::new()
            };
            let color_group = match color_by {
                "spw" => format!("spw-{}", spw.spectral_window_id),
                "polarization" => spw
                    .correlation_types
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "none".to_string()),
                value => {
                    return Err(format!(
                        "invalid color_by {value:?} for spectral_window_coverage"
                    ));
                }
            };
            Ok(SpectralWindowCoverageBar {
                lane,
                start: spw.first_channel_frequency_hz / scale.0,
                end: (spw.first_channel_frequency_hz + spw.total_bandwidth_hz) / scale.0,
                label,
                color_group,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(ListObsPlotPayload::SpectralWindowCoverage(
        SpectralWindowCoveragePlotPayload {
            x_label: scale.1.to_string(),
            bars,
            summary: format!(
                "Spectral window coverage. SPWs={}",
                summary.spectral_windows.len()
            ),
        },
    ))
}

fn render_uv_coverage_plot(
    root: &DrawingArea<BitMapBackend<'_>, plotters::coord::Shift>,
    payload: &UvCoveragePlotPayload,
    theme: ListObsPlotTheme,
) -> Result<(), String> {
    let axis_scale = uv_axis_scale(payload.axis_extent_lambda);
    let extent = (payload.axis_extent_lambda / axis_scale.lambda_scale).max(1.0);
    let mut chart = ChartBuilder::on(root)
        .margin(16)
        .x_label_area_size(44)
        .y_label_area_size(56)
        .build_cartesian_2d(-extent..extent, -extent..extent)
        .map_err(|error| error.to_string())?;
    configure_mesh(
        &mut chart,
        theme,
        &format!("u ({})", axis_scale.unit_label),
        &format!("v ({})", axis_scale.unit_label),
    )?;

    for (index, series) in payload.tracks.iter().enumerate() {
        let color = rgb(theme.accents[index % theme.accents.len()]);
        let points = series
            .points
            .iter()
            .map(|(u, v)| (*u / axis_scale.lambda_scale, *v / axis_scale.lambda_scale))
            .collect::<Vec<_>>();
        if payload.draw_points {
            chart
                .draw_series(PointSeries::of_element(
                    points.iter().copied(),
                    3,
                    color.filled(),
                    &|coord, size, style| {
                        EmptyElement::at(coord) + Circle::new((0, 0), size, style)
                    },
                ))
                .map_err(|error| error.to_string())?;
            if payload.mirror {
                chart
                    .draw_series(PointSeries::of_element(
                        points.iter().map(|(u, v)| (-u, -v)),
                        2,
                        color.mix(0.5).filled(),
                        &|coord, size, style| {
                            EmptyElement::at(coord) + Circle::new((0, 0), size, style)
                        },
                    ))
                    .map_err(|error| error.to_string())?;
            }
        } else if points.len() >= 2 {
            chart
                .draw_series(LineSeries::new(
                    points.iter().copied(),
                    color.stroke_width(2),
                ))
                .map_err(|error| error.to_string())?;
            if payload.mirror {
                chart
                    .draw_series(LineSeries::new(
                        points.iter().map(|(u, v)| (-u, -v)),
                        color.mix(0.5).stroke_width(1),
                    ))
                    .map_err(|error| error.to_string())?;
            }
        } else if let Some(point) = points.first().copied() {
            chart
                .draw_series(PointSeries::of_element(
                    [point],
                    3,
                    color.filled(),
                    &|coord, size, style| {
                        EmptyElement::at(coord) + Circle::new((0, 0), size, style)
                    },
                ))
                .map_err(|error| error.to_string())?;
        }
    }

    Ok(())
}

fn render_antenna_layout_plot(
    root: &DrawingArea<BitMapBackend<'_>, plotters::coord::Shift>,
    payload: &AntennaLayoutPlotPayload,
    theme: ListObsPlotTheme,
) -> Result<(), String> {
    let (min_x, max_x, min_y, max_y) =
        bounds(payload.antennas.iter().map(|point| (point.x, point.y)))
            .unwrap_or((-1.0, 1.0, -1.0, 1.0));
    let padding_x = ((max_x - min_x).abs() * 0.1).max(1.0);
    let padding_y = ((max_y - min_y).abs() * 0.1).max(1.0);
    let mut chart = ChartBuilder::on(root)
        .margin(16)
        .x_label_area_size(44)
        .y_label_area_size(56)
        .build_cartesian_2d(
            (min_x - padding_x)..(max_x + padding_x),
            (min_y - padding_y)..(max_y + padding_y),
        )
        .map_err(|error| error.to_string())?;
    configure_mesh(&mut chart, theme, &payload.x_label, &payload.y_label)?;

    for (index, point) in payload.antennas.iter().enumerate() {
        let color = rgb(theme.accents[index % theme.accents.len()]);
        chart
            .draw_series(std::iter::once(
                EmptyElement::at((point.x, point.y))
                    + Circle::new((0, 0), point.marker_radius, color.filled())
                    + if payload.labels_enabled && !point.label.is_empty() {
                        Text::new(
                            point.label.clone(),
                            (10, -10),
                            ("sans-serif", 16).into_font().color(&rgb(theme.axis)),
                        )
                    } else {
                        Text::new(String::new(), (0, 0), ("sans-serif", 1).into_font())
                    },
            ))
            .map_err(|error| error.to_string())?;
    }

    Ok(())
}

fn render_scan_timeline_plot(
    root: &DrawingArea<BitMapBackend<'_>, plotters::coord::Shift>,
    payload: &ScanTimelinePlotPayload,
    theme: ListObsPlotTheme,
) -> Result<(), String> {
    let lane_count = payload.lane_labels.len().max(1);
    let mut chart = ChartBuilder::on(root)
        .margin(16)
        .x_label_area_size(44)
        .y_label_area_size(92)
        .build_cartesian_2d(
            payload.start_mjd_seconds..payload.end_mjd_seconds,
            0f64..lane_count as f64,
        )
        .map_err(|error| error.to_string())?;
    chart
        .configure_mesh()
        .x_desc("Time (MJD seconds)")
        .y_desc("Lane")
        .axis_desc_style(("sans-serif", 18).into_font().color(&rgb(theme.axis)))
        .axis_style(rgb(theme.axis))
        .label_style(("sans-serif", 14).into_font().color(&rgb(theme.label)))
        .light_line_style(rgb(theme.grid).mix(0.55))
        .bold_line_style(rgb(theme.grid))
        .y_labels(lane_count)
        .y_label_formatter(&|value| {
            let lane = (*value).floor() as usize;
            payload
                .lane_labels
                .get(lane)
                .cloned()
                .unwrap_or_else(String::new)
        })
        .draw()
        .map_err(|error| error.to_string())?;

    for bar in &payload.bars {
        let color = palette_color(&bar.color_group, theme);
        let lane = bar.lane as f64;
        chart
            .draw_series(std::iter::once(Rectangle::new(
                [
                    (bar.start_mjd_seconds, lane + 0.15),
                    (bar.end_mjd_seconds, lane + 0.85),
                ],
                color.filled(),
            )))
            .map_err(|error| error.to_string())?;
        if !bar.label.is_empty() {
            chart
                .draw_series(std::iter::once(Text::new(
                    bar.label.clone(),
                    (bar.start_mjd_seconds, lane + 0.5),
                    ("sans-serif", 14).into_font().color(&rgb(theme.axis)),
                )))
                .map_err(|error| error.to_string())?;
        }
    }

    Ok(())
}

fn render_spectral_window_coverage_plot(
    root: &DrawingArea<BitMapBackend<'_>, plotters::coord::Shift>,
    payload: &SpectralWindowCoveragePlotPayload,
    theme: ListObsPlotTheme,
) -> Result<(), String> {
    let (min_x, max_x) = payload
        .bars
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(min_x, max_x), bar| {
            (min_x.min(bar.start), max_x.max(bar.end))
        });
    let min_x = if min_x.is_finite() { min_x } else { 0.0 };
    let max_x = if max_x.is_finite() && max_x > min_x {
        max_x
    } else {
        min_x + 1.0
    };
    let lane_count = payload.bars.len().max(1);
    let mut chart = ChartBuilder::on(root)
        .margin(16)
        .x_label_area_size(44)
        .y_label_area_size(92)
        .build_cartesian_2d(min_x..max_x, 0f64..lane_count as f64)
        .map_err(|error| error.to_string())?;
    chart
        .configure_mesh()
        .x_desc(&payload.x_label)
        .y_desc("SPW")
        .axis_desc_style(("sans-serif", 18).into_font().color(&rgb(theme.axis)))
        .axis_style(rgb(theme.axis))
        .label_style(("sans-serif", 14).into_font().color(&rgb(theme.label)))
        .light_line_style(rgb(theme.grid).mix(0.55))
        .bold_line_style(rgb(theme.grid))
        .y_labels(lane_count)
        .y_label_formatter(&|value| {
            let lane = (*value).floor() as usize;
            payload
                .bars
                .get(lane)
                .map(|_| format!("SPW {lane}"))
                .unwrap_or_default()
        })
        .draw()
        .map_err(|error| error.to_string())?;

    for bar in &payload.bars {
        let color = palette_color(&bar.color_group, theme);
        let lane = bar.lane as f64;
        chart
            .draw_series(std::iter::once(Rectangle::new(
                [(bar.start, lane + 0.2), (bar.end, lane + 0.8)],
                color.filled(),
            )))
            .map_err(|error| error.to_string())?;
        if !bar.label.is_empty() {
            chart
                .draw_series(std::iter::once(Text::new(
                    bar.label.clone(),
                    (bar.start, lane + 0.5),
                    ("sans-serif", 14).into_font().color(&rgb(theme.axis)),
                )))
                .map_err(|error| error.to_string())?;
        }
    }

    Ok(())
}

fn validate_option(kind: ListObsPlotKind, key: &str, value: &str) -> Result<(), String> {
    match kind {
        ListObsPlotKind::UvCoverage => match key {
            "draw_mode" if matches!(value, "tracks" | "points") => Ok(()),
            "mirror" => {
                parse_on_off(value, key)?;
                Ok(())
            }
            "axis_extent" if value == "auto" || value.parse::<f64>().is_ok() => Ok(()),
            _ => Err(format!(
                "unsupported option {key:?}={value:?} for uv_coverage"
            )),
        },
        ListObsPlotKind::AntennaLayout => match key {
            "labels" if matches!(value, "off" | "id" | "name") => Ok(()),
            "coordinates" if matches!(value, "offset" | "absolute") => Ok(()),
            "size_by_diameter" => {
                parse_on_off(value, key)?;
                Ok(())
            }
            _ => Err(format!(
                "unsupported option {key:?}={value:?} for antenna_layout"
            )),
        },
        ListObsPlotKind::ScanTimeline => match key {
            "lanes" if matches!(value, "scan" | "field") => Ok(()),
            "color_by" if matches!(value, "field" | "intent") => Ok(()),
            "labels" if matches!(value, "none" | "scan" | "field") => Ok(()),
            _ => Err(format!(
                "unsupported option {key:?}={value:?} for scan_timeline"
            )),
        },
        ListObsPlotKind::SpectralWindowCoverage => match key {
            "unit" if matches!(value, "ghz" | "mhz") => Ok(()),
            "labels" => {
                parse_on_off(value, key)?;
                Ok(())
            }
            "color_by" if matches!(value, "spw" | "polarization") => Ok(()),
            _ => Err(format!(
                "unsupported option {key:?}={value:?} for spectral_window_coverage"
            )),
        },
    }
}

fn configure_mesh<DB: DrawingBackend>(
    chart: &mut ChartContext<'_, DB, Cartesian2d<RangedCoordf64, RangedCoordf64>>,
    theme: ListObsPlotTheme,
    x_desc: &str,
    y_desc: &str,
) -> Result<(), String> {
    chart
        .configure_mesh()
        .x_desc(x_desc)
        .y_desc(y_desc)
        .axis_desc_style(("sans-serif", 18).into_font().color(&rgb(theme.axis)))
        .axis_style(rgb(theme.axis))
        .label_style(("sans-serif", 14).into_font().color(&rgb(theme.label)))
        .light_line_style(rgb(theme.grid).mix(0.55))
        .bold_line_style(rgb(theme.grid))
        .draw()
        .map_err(|error| error.to_string())
}

fn parse_on_off(value: &str, key: &str) -> Result<bool, String> {
    match value {
        "on" | "true" => Ok(true),
        "off" | "false" => Ok(false),
        other => Err(format!(
            "invalid value {other:?} for {key}; expected on/off"
        )),
    }
}

fn rgb(color: [u8; 3]) -> RGBColor {
    RGBColor(color[0], color[1], color[2])
}

fn palette_color(group: &str, theme: ListObsPlotTheme) -> RGBColor {
    let mut hash = 0usize;
    for byte in group.bytes() {
        hash = hash.wrapping_mul(131).wrapping_add(byte as usize);
    }
    rgb(theme.accents[hash % theme.accents.len()])
}

fn bounds(points: impl Iterator<Item = (f64, f64)>) -> Option<(f64, f64, f64, f64)> {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    let mut seen = false;
    for (x, y) in points {
        if !x.is_finite() || !y.is_finite() {
            continue;
        }
        seen = true;
        min_x = min_x.min(x);
        max_x = max_x.max(x);
        min_y = min_y.min(y);
        max_y = max_y.max(y);
    }
    seen.then_some((min_x, max_x, min_y, max_y))
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct UvAxisScale {
    unit_label: &'static str,
    lambda_scale: f64,
}

fn uv_axis_scale(max_abs_uv_lambda: f64) -> UvAxisScale {
    if max_abs_uv_lambda >= 1_000_000.0 {
        UvAxisScale {
            unit_label: "Mλ",
            lambda_scale: 1_000_000.0,
        }
    } else if max_abs_uv_lambda >= 1_000.0 {
        UvAxisScale {
            unit_label: "kλ",
            lambda_scale: 1_000.0,
        }
    } else {
        UvAxisScale {
            unit_label: "λ",
            lambda_scale: 1.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ListObsPlotExportFormat, ListObsPlotKind, ListObsPlotSpec, ListObsPlotTheme,
        build_listobs_plot_payload_from_summary, build_listobs_uv_plot_payload,
        export_listobs_plot, render_listobs_plot_image,
    };
    use crate::listobs::{
        AntennaSummary, DataDescriptionSummary, FieldSummary, MeasurementSetInfo,
        ObservationSummary, PolarizationSummary, ScanSummary, SourceSummary, SpectralWindowSummary,
    };
    use crate::{
        ListObsOptions, ListObsSummary, ListObsUvCoverage, ListObsUvPoint, ListObsUvTrack,
    };
    use tempfile::tempdir;

    #[test]
    fn default_specs_can_round_trip_cli_assignments() {
        for kind in ListObsPlotKind::ALL {
            let spec = kind.default_spec();
            let rebuilt =
                ListObsPlotSpec::from_cli_assignments(kind, &spec.cli_assignments()).unwrap();
            assert_eq!(rebuilt, spec);
        }
    }

    #[test]
    fn metadata_plot_payloads_render_png_images() {
        let summary = synthetic_summary();
        for kind in [
            ListObsPlotKind::AntennaLayout,
            ListObsPlotKind::ScanTimeline,
            ListObsPlotKind::SpectralWindowCoverage,
        ] {
            let spec = kind.default_spec();
            let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
            let image =
                render_listobs_plot_image(&payload, ListObsPlotTheme::light(), 800, 480).unwrap();
            assert_eq!(image.width(), 800);
            assert_eq!(image.height(), 480);
        }
    }

    #[test]
    fn uv_plot_payload_renders_png_image() {
        let coverage = synthetic_uv_coverage();
        let spec = ListObsPlotKind::UvCoverage.default_spec();
        let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
        let image =
            render_listobs_plot_image(&payload, ListObsPlotTheme::dark(), 640, 640).unwrap();
        assert_eq!(image.width(), 640);
        assert_eq!(image.height(), 640);
    }

    #[test]
    fn plot_export_writes_png_and_pdf_files() {
        let temp = tempdir().unwrap();
        let summary = synthetic_summary();
        let spec = ListObsPlotKind::AntennaLayout.default_spec();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        let png_path = temp.path().join("antenna-layout.png");
        let pdf_path = temp.path().join("antenna-layout.pdf");

        export_listobs_plot(
            &payload,
            ListObsPlotTheme::light(),
            &png_path,
            ListObsPlotExportFormat::Png,
            640,
            360,
        )
        .unwrap();
        export_listobs_plot(
            &payload,
            ListObsPlotTheme::light(),
            &pdf_path,
            ListObsPlotExportFormat::Pdf,
            640,
            360,
        )
        .unwrap();

        assert!(png_path.exists());
        assert!(pdf_path.exists());
        assert!(std::fs::metadata(&png_path).unwrap().len() > 0);
        assert!(std::fs::metadata(&pdf_path).unwrap().len() > 0);
    }

    #[test]
    fn antenna_layout_rejects_missing_coordinate_metadata() {
        let mut summary = synthetic_summary();
        for antenna in &mut summary.antennas {
            antenna.offset_from_observatory_m = [f64::NAN, f64::NAN, f64::NAN];
            antenna.position_m = [f64::NAN, f64::NAN, f64::NAN];
        }
        let spec = ListObsPlotKind::AntennaLayout.default_spec();
        let error = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap_err();
        assert!(error.contains("finite offset coordinates"));
    }

    fn synthetic_summary() -> ListObsSummary {
        ListObsSummary {
            schema_version: 1,
            options: ListObsOptions::default(),
            measurement_set: MeasurementSetInfo {
                path: Some("fixture.ms".to_string()),
                ms_version: Some(2.0),
                row_count: 4,
                observation_count: 1,
                field_count: 2,
                spectral_window_count: 2,
                polarization_count: 1,
                data_description_count: 2,
                source_count: 1,
                antenna_count: 3,
                antenna_table_count: 3,
                time_reference: Some("UTC".to_string()),
                start_mjd_seconds: Some(100.0),
                end_mjd_seconds: Some(180.0),
                total_elapsed_seconds: Some(80.0),
            },
            observations: vec![ObservationSummary {
                observation_id: 0,
                telescope_name: "VLA".to_string(),
                observer: "Observer".to_string(),
                project: "Project".to_string(),
                release_date_mjd_seconds: 0.0,
                start_mjd_seconds: Some(100.0),
                end_mjd_seconds: Some(180.0),
            }],
            scans: vec![
                ScanSummary {
                    observation_id: 0,
                    array_id: 0,
                    scan_number: 1,
                    row_count: 2,
                    field_id: 0,
                    field_name: "FIELD_A".to_string(),
                    field_ids: vec![0],
                    field_names: vec!["FIELD_A".to_string()],
                    data_description_ids: vec![0],
                    spectral_window_ids: vec![0],
                    state_ids: vec![0],
                    scan_intents: vec!["CALIBRATE_PHASE.ON_SOURCE".to_string()],
                    start_mjd_seconds: 100.0,
                    end_mjd_seconds: 130.0,
                    mean_interval_seconds: 15.0,
                    mean_interval_seconds_by_spw: vec![15.0],
                    unflagged_row_count: None,
                },
                ScanSummary {
                    observation_id: 0,
                    array_id: 0,
                    scan_number: 2,
                    row_count: 2,
                    field_id: 1,
                    field_name: "FIELD_B".to_string(),
                    field_ids: vec![1],
                    field_names: vec!["FIELD_B".to_string()],
                    data_description_ids: vec![1],
                    spectral_window_ids: vec![1],
                    state_ids: vec![1],
                    scan_intents: vec!["OBSERVE_TARGET.ON_SOURCE".to_string()],
                    start_mjd_seconds: 140.0,
                    end_mjd_seconds: 180.0,
                    mean_interval_seconds: 20.0,
                    mean_interval_seconds_by_spw: vec![20.0],
                    unflagged_row_count: None,
                },
            ],
            fields: vec![
                FieldSummary {
                    field_id: 0,
                    name: "FIELD_A".to_string(),
                    code: "".to_string(),
                    source_id: 0,
                    row_count: 2,
                    unflagged_row_count: None,
                    time_mjd_seconds: 100.0,
                    direction_reference: Some("J2000".to_string()),
                    phase_direction_radians: [0.0, 0.0],
                },
                FieldSummary {
                    field_id: 1,
                    name: "FIELD_B".to_string(),
                    code: "".to_string(),
                    source_id: 0,
                    row_count: 2,
                    unflagged_row_count: None,
                    time_mjd_seconds: 140.0,
                    direction_reference: Some("J2000".to_string()),
                    phase_direction_radians: [0.1, 0.1],
                },
            ],
            polarization_setups: vec![PolarizationSummary {
                polarization_id: 0,
                num_correlations: 2,
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            }],
            data_descriptions: vec![
                DataDescriptionSummary {
                    data_description_id: 0,
                    spectral_window_id: 0,
                    polarization_id: 0,
                    flagged: false,
                },
                DataDescriptionSummary {
                    data_description_id: 1,
                    spectral_window_id: 1,
                    polarization_id: 0,
                    flagged: false,
                },
            ],
            spectral_windows: vec![
                SpectralWindowSummary {
                    spectral_window_id: 0,
                    name: "LOW".to_string(),
                    num_channels: 16,
                    frame: Some("TOPO".to_string()),
                    first_channel_frequency_hz: 1.0e9,
                    channel_width_hz: 1.0e6,
                    reference_frequency_hz: 1.0e9,
                    center_frequency_hz: 1.008e9,
                    total_bandwidth_hz: 16.0e6,
                    data_description_ids: vec![0],
                    polarization_ids: vec![0],
                    correlation_types: vec!["XX".to_string(), "YY".to_string()],
                },
                SpectralWindowSummary {
                    spectral_window_id: 1,
                    name: "HIGH".to_string(),
                    num_channels: 32,
                    frame: Some("TOPO".to_string()),
                    first_channel_frequency_hz: 1.2e9,
                    channel_width_hz: 5.0e5,
                    reference_frequency_hz: 1.2e9,
                    center_frequency_hz: 1.208e9,
                    total_bandwidth_hz: 16.0e6,
                    data_description_ids: vec![1],
                    polarization_ids: vec![0],
                    correlation_types: vec!["XX".to_string(), "YY".to_string()],
                },
            ],
            sources: vec![SourceSummary {
                source_id: 0,
                name: "SOURCE".to_string(),
                code: "".to_string(),
                spectral_window_id: -1,
                calibration_group: 0,
                num_lines: 0,
                rest_frequency_hz: None,
                system_velocity_m_s: None,
                time_mjd_seconds: 100.0,
                direction_radians: [0.0, 0.0],
            }],
            antennas: vec![
                AntennaSummary {
                    antenna_id: 0,
                    name: "ea01".to_string(),
                    station: "A01".to_string(),
                    antenna_type: "GROUND-BASED".to_string(),
                    mount: "ALT-AZ".to_string(),
                    dish_diameter_m: 25.0,
                    longitude_radians: 0.0,
                    latitude_radians: 0.0,
                    offset_from_observatory_m: [0.0, 0.0, 0.0],
                    position_m: [1000.0, 2000.0, 0.0],
                },
                AntennaSummary {
                    antenna_id: 1,
                    name: "ea02".to_string(),
                    station: "A02".to_string(),
                    antenna_type: "GROUND-BASED".to_string(),
                    mount: "ALT-AZ".to_string(),
                    dish_diameter_m: 25.0,
                    longitude_radians: 0.0,
                    latitude_radians: 0.0,
                    offset_from_observatory_m: [120.0, 40.0, 0.0],
                    position_m: [1120.0, 2040.0, 0.0],
                },
                AntennaSummary {
                    antenna_id: 2,
                    name: "ea03".to_string(),
                    station: "A03".to_string(),
                    antenna_type: "GROUND-BASED".to_string(),
                    mount: "ALT-AZ".to_string(),
                    dish_diameter_m: 18.0,
                    longitude_radians: 0.0,
                    latitude_radians: 0.0,
                    offset_from_observatory_m: [-90.0, 150.0, 0.0],
                    position_m: [910.0, 2150.0, 0.0],
                },
            ],
        }
    }

    fn synthetic_uv_coverage() -> ListObsUvCoverage {
        ListObsUvCoverage {
            schema_version: 1,
            options: ListObsOptions::default(),
            measurement_set_path: Some("fixture.ms".to_string()),
            axis_unit: "lambda".to_string(),
            mirrored_display: true,
            sample_count: 4,
            max_abs_uv_lambda: 2400.0,
            tracks: vec![
                ListObsUvTrack {
                    antenna1: 0,
                    antenna2: 1,
                    field_id: 0,
                    spectral_window_id: 0,
                    center_frequency_hz: 1.0e9,
                    samples: vec![
                        ListObsUvPoint {
                            row: 0,
                            time_mjd_seconds: 100.0,
                            u_lambda: 200.0,
                            v_lambda: 400.0,
                            w_lambda: 0.0,
                        },
                        ListObsUvPoint {
                            row: 1,
                            time_mjd_seconds: 101.0,
                            u_lambda: 1200.0,
                            v_lambda: 1800.0,
                            w_lambda: 0.0,
                        },
                    ],
                },
                ListObsUvTrack {
                    antenna1: 0,
                    antenna2: 2,
                    field_id: 1,
                    spectral_window_id: 1,
                    center_frequency_hz: 1.2e9,
                    samples: vec![
                        ListObsUvPoint {
                            row: 2,
                            time_mjd_seconds: 102.0,
                            u_lambda: -2400.0,
                            v_lambda: 1000.0,
                            w_lambda: 0.0,
                        },
                        ListObsUvPoint {
                            row: 3,
                            time_mjd_seconds: 103.0,
                            u_lambda: -400.0,
                            v_lambda: -800.0,
                            w_lambda: 0.0,
                        },
                    ],
                },
            ],
        }
    }
}
