// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared plot specification and typed payload building for `listobs`
//! metadata plots plus curated raw MeasurementSet visibility plots analogous
//! to the common CASA `plotms` views.
//!
//! The payloads are typed plot data, not pictures: Python renders them with
//! Matplotlib, and `msexplore`/`plotms` can write them as a text point
//! manifest. PNG and PDF rasterisation was removed (#833).

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use casa_ms::SpectralWindowSummary;
use casa_ms::{ListObsOptions, ListObsSummary, ListObsUvCoverage, MeasurementSet};

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
    /// Vector-averaged visibility amplitude against MAIN.TIME.
    AmplitudeVsTime,
    /// Vector-averaged visibility phase against MAIN.TIME.
    PhaseVsTime,
    /// Vector-averaged visibility amplitude against `sqrt(u² + v²)` in meters.
    AmplitudeVsUvDistance,
}

impl ListObsPlotKind {
    /// All plot kinds shipped in the curated `listobs` plot workspace.
    pub const ALL: [Self; 7] = [
        Self::UvCoverage,
        Self::AntennaLayout,
        Self::ScanTimeline,
        Self::SpectralWindowCoverage,
        Self::AmplitudeVsTime,
        Self::PhaseVsTime,
        Self::AmplitudeVsUvDistance,
    ];

    /// Stable machine-readable identifier used by CLI and serialized specs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UvCoverage => "uv_coverage",
            Self::AntennaLayout => "antenna_layout",
            Self::ScanTimeline => "scan_timeline",
            Self::SpectralWindowCoverage => "spectral_window_coverage",
            Self::AmplitudeVsTime => "amplitude_vs_time",
            Self::PhaseVsTime => "phase_vs_time",
            Self::AmplitudeVsUvDistance => "amplitude_vs_uv_distance",
        }
    }

    /// Human-readable label suitable for plot catalogs and titles.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::UvCoverage => "UV Coverage",
            Self::AntennaLayout => "Antenna Layout",
            Self::ScanTimeline => "Scan Timeline",
            Self::SpectralWindowCoverage => "Spectral Window Coverage",
            Self::AmplitudeVsTime => "Amplitude vs Time",
            Self::PhaseVsTime => "Phase vs Time",
            Self::AmplitudeVsUvDistance => "Amplitude vs UV Distance",
        }
    }

    /// Returns `true` when this plot needs MAIN-table visibility data.
    pub fn is_raw_visibility(self) -> bool {
        matches!(
            self,
            Self::AmplitudeVsTime | Self::PhaseVsTime | Self::AmplitudeVsUvDistance
        )
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
            "amplitude_vs_time" | "amp_time" => Ok(Self::AmplitudeVsTime),
            "phase_vs_time" | "phase_time" => Ok(Self::PhaseVsTime),
            "amplitude_vs_uv_distance" | "amplitude_vs_uvdist" | "amp_uvdist" => {
                Ok(Self::AmplitudeVsUvDistance)
            }
            other => Err(format!(
                "unsupported plot kind {other:?}; expected one of: uv_coverage, antenna_layout, scan_timeline, spectral_window_coverage, amplitude_vs_time, phase_vs_time, amplitude_vs_uv_distance"
            )),
        }
    }

    /// Build the default plot specification for this kind.
    pub fn default_spec(self) -> ListObsPlotSpec {
        let mut options = BTreeMap::new();
        match self {
            Self::UvCoverage => {
                options.insert("draw_mode".to_string(), "points".to_string());
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
            Self::AmplitudeVsTime | Self::PhaseVsTime | Self::AmplitudeVsUvDistance => {
                options.insert("data_column".to_string(), "data".to_string());
                options.insert(
                    "color_by".to_string(),
                    match self {
                        Self::AmplitudeVsUvDistance => "spw",
                        _ => "field",
                    }
                    .to_string(),
                );
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

/// Serializable plot specification shared by the CLI and Python.
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

/// Typed data prepared for one plot (rendered by Python, or exported as a
/// text point manifest).
#[derive(Debug, Clone, PartialEq)]
pub enum ListObsPlotPayload {
    /// UV-coverage series grouped by baseline / field / SPW.
    UvCoverage(UvCoveragePlotPayload),
    /// Curated raw visibility scatter payload.
    VisibilityScatter(VisibilityScatterPlotPayload),
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
            Self::VisibilityScatter(payload) => payload.kind,
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

/// Generic scatter payload for curated raw visibility plots.
#[derive(Debug, Clone, PartialEq)]
pub struct VisibilityScatterPlotPayload {
    /// Specific plot kind represented by this scatter payload.
    pub kind: ListObsPlotKind,
    /// X-axis label.
    pub x_label: String,
    /// Y-axis label.
    pub y_label: String,
    /// Optional fixed y-axis bounds.
    pub fixed_y_bounds: Option<(f64, f64)>,
    /// Series keyed by one selected metadata grouping.
    pub series: Vec<VisibilityScatterSeries>,
    /// Render summary.
    pub summary: String,
}

/// One grouped visibility scatter series.
#[derive(Debug, Clone, PartialEq)]
pub struct VisibilityScatterSeries {
    /// Stable label for hover/debug/export summaries.
    pub label: String,
    /// Stable color-group key.
    pub color_group: String,
    /// Plot points.
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
    /// Spectral-window identifier for labeling.
    pub spectral_window_id: usize,
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
        ListObsPlotKind::AmplitudeVsTime
        | ListObsPlotKind::PhaseVsTime
        | ListObsPlotKind::AmplitudeVsUvDistance => Err(format!(
            "{} requires MAIN-table visibility data; build it with build_listobs_visibility_plot_payload",
            spec.kind.display_name()
        )),
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
    let draw_points = match spec.option("draw_mode").unwrap_or("points") {
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

/// Build one curated raw-visibility plot payload directly from a MeasurementSet.
///
/// This supports the common CASA `plotms` views implemented in this crate:
/// amplitude vs time, phase vs time, and amplitude vs UV distance.
pub fn build_listobs_visibility_plot_payload(
    ms: &MeasurementSet,
    options: &ListObsOptions,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    crate::plot_visibility::build_listobs_visibility_plot_payload(ms, options, spec)
}

fn build_antenna_layout_payload(
    summary: &ListObsSummary,
    spec: &ListObsPlotSpec,
) -> Result<ListObsPlotPayload, String> {
    let labels = spec.option("labels").unwrap_or("name");
    let labels_enabled = labels != "off";
    let coordinates = spec.option("coordinates").unwrap_or("offset");
    let use_absolute_coordinates =
        coordinates == "absolute" || should_fallback_to_absolute_antenna_coordinates(summary);
    let size_by_diameter = parse_on_off(
        spec.option("size_by_diameter").unwrap_or("on"),
        "size_by_diameter",
    )?;
    let mut omitted = 0usize;
    let mut antennas = Vec::<(AntennaLayoutPoint, String, String)>::new();
    for antenna in &summary.antennas {
        let (x, y, x_label, y_label) = if use_absolute_coordinates {
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
            if use_absolute_coordinates {
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
                if coordinates == "offset" && use_absolute_coordinates {
                    format!(
                        "Antenna layout. Antennas={} (offsets unavailable; using ITRF X/Y)",
                        summary.antennas.len()
                    )
                } else {
                    format!("Antenna layout. Antennas={}", summary.antennas.len())
                }
            } else {
                let suffix = if coordinates == "offset" && use_absolute_coordinates {
                    "; offsets unavailable; using ITRF X/Y"
                } else {
                    ""
                };
                format!(
                    "Antenna layout. Antennas={} ({} omitted without finite coordinates{})",
                    summary.antennas.len() - omitted,
                    omitted,
                    suffix
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
                "polarization" => spectral_window_polarization_group(spw),
                value => {
                    return Err(format!(
                        "invalid color_by {value:?} for spectral_window_coverage"
                    ));
                }
            };
            Ok(SpectralWindowCoverageBar {
                spectral_window_id: spw.spectral_window_id,
                lane,
                start: spw.min_frequency_hz / scale.0,
                end: spw.max_frequency_hz / scale.0,
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

fn spectral_window_polarization_group(spw: &SpectralWindowSummary) -> String {
    if spw.polarization_ids.is_empty() {
        "pol-none".to_string()
    } else {
        format!(
            "pol-{}",
            spw.polarization_ids
                .iter()
                .map(|id: &usize| id.to_string())
                .collect::<Vec<_>>()
                .join("-")
        )
    }
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
        ListObsPlotKind::AmplitudeVsTime
        | ListObsPlotKind::PhaseVsTime
        | ListObsPlotKind::AmplitudeVsUvDistance => match key {
            "data_column" if matches!(value, "data" | "corrected" | "model") => Ok(()),
            "color_by"
                if matches!(
                    value,
                    "field" | "scan" | "spw" | "baseline" | "correlation" | "none"
                ) =>
            {
                Ok(())
            }
            _ => Err(format!(
                "unsupported option {key:?}={value:?} for {}",
                kind.as_str()
            )),
        },
    }
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

fn should_fallback_to_absolute_antenna_coordinates(summary: &ListObsSummary) -> bool {
    let mut saw_nonzero_offset = false;
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    let mut saw_absolute_position = false;

    for antenna in &summary.antennas {
        let [east, north, _elevation] = antenna.offset_from_observatory_m;
        if east.is_finite() && north.is_finite() && (east.abs() > 1e-6 || north.abs() > 1e-6) {
            saw_nonzero_offset = true;
        }

        let [x, y, _z] = antenna.position_m;
        if x.is_finite() && y.is_finite() {
            saw_absolute_position = true;
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    }

    !saw_nonzero_offset
        && saw_absolute_position
        && ((max_x - min_x).abs() > 1e-3 || (max_y - min_y).abs() > 1e-3)
}

#[cfg(test)]
mod tests;
