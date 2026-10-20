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

use crate::listobs::SpectralWindowSummary;
use crate::{ListObsOptions, ListObsSummary, ListObsUvCoverage, MeasurementSet};

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
mod tests {
    use super::{
        ListObsPlotKind, ListObsPlotPayload, ListObsPlotSpec,
        build_listobs_plot_payload_from_summary, build_listobs_uv_plot_payload, parse_on_off,
    };
    use crate::listobs::{
        AntennaSummary, DataDescriptionSummary, FieldSummary, MeasurementSetInfo,
        ObservationSummary, PolarizationSummary, ScanSummary, SourceSummary, SpectralWindowSummary,
    };
    use crate::listobs::{ListObsUvPoint, ListObsUvTrack};
    use crate::{ListObsOptions, ListObsSummary, ListObsUvCoverage};

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
    fn plot_kinds_parse_aliases_and_errors() {
        let aliases = [
            ("uv_coverage", ListObsPlotKind::UvCoverage),
            ("uv", ListObsPlotKind::UvCoverage),
            ("antenna_layout", ListObsPlotKind::AntennaLayout),
            ("antennas", ListObsPlotKind::AntennaLayout),
            ("scan_timeline", ListObsPlotKind::ScanTimeline),
            ("scans", ListObsPlotKind::ScanTimeline),
            (
                "spectral_window_coverage",
                ListObsPlotKind::SpectralWindowCoverage,
            ),
            ("spw_coverage", ListObsPlotKind::SpectralWindowCoverage),
            ("spws", ListObsPlotKind::SpectralWindowCoverage),
        ];
        for (value, expected) in aliases {
            assert_eq!(ListObsPlotKind::parse(value).unwrap(), expected);
            assert_eq!(expected.to_string(), expected.as_str());
            assert!(!expected.display_name().is_empty());
        }

        let error = ListObsPlotKind::parse("bogus").unwrap_err();
        assert!(error.contains("unsupported plot kind"));
    }

    #[test]
    fn parse_on_off_accepts_boolean_tokens() {
        assert!(parse_on_off("on", "labels").unwrap());
        assert!(parse_on_off("true", "labels").unwrap());
        assert!(!parse_on_off("off", "labels").unwrap());
        assert!(!parse_on_off("false", "labels").unwrap());
        let error = parse_on_off("maybe", "labels").unwrap_err();
        assert!(error.contains("expected on/off"));
    }

    #[test]
    fn plot_specs_validate_options_and_preserve_assignments() {
        let mut spec = ListObsPlotSpec::new(ListObsPlotKind::AntennaLayout);
        spec.set_option("labels", "id").unwrap();
        spec.set_option("coordinates", "absolute").unwrap();
        spec.set_option("size_by_diameter", "false").unwrap();
        assert_eq!(spec.option("labels"), Some("id"));
        assert_eq!(spec.option("coordinates"), Some("absolute"));
        assert_eq!(spec.option("size_by_diameter"), Some("false"));
        assert_eq!(
            spec.cli_assignments(),
            vec![
                "coordinates=absolute".to_string(),
                "labels=id".to_string(),
                "size_by_diameter=false".to_string(),
            ]
        );

        let error = spec.set_option("labels", "bogus").unwrap_err();
        assert!(error.contains("unsupported option"));

        let error = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::ScanTimeline,
            &["missing-separator".to_string()],
        )
        .unwrap_err();
        assert!(error.contains("expected key=value"));

        let error = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::SpectralWindowCoverage,
            &["labels=maybe".to_string()],
        )
        .unwrap_err();
        assert!(error.contains("expected on/off"));
    }

    #[test]
    fn metadata_plot_payloads_build_for_each_metadata_kind() {
        let summary = synthetic_summary();
        for kind in [
            ListObsPlotKind::AntennaLayout,
            ListObsPlotKind::ScanTimeline,
            ListObsPlotKind::SpectralWindowCoverage,
        ] {
            let spec = kind.default_spec();
            let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
            assert_eq!(payload.kind(), kind);
        }
    }

    #[test]
    fn uv_plot_payload_defaults_to_mirrored_points() {
        let coverage = synthetic_uv_coverage();
        let spec = ListObsPlotKind::UvCoverage.default_spec();
        let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
        let ListObsPlotPayload::UvCoverage(uv_payload) = &payload else {
            panic!("expected uv payload");
        };
        assert!(uv_payload.draw_points);
        assert!(uv_payload.mirror);
        assert_eq!(uv_payload.tracks.len(), coverage.tracks.len());
    }

    #[test]
    fn uv_points_mode_and_single_sample_tracks_build() {
        let mut coverage = synthetic_uv_coverage();
        coverage.tracks[0].samples.truncate(1);
        let spec = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::UvCoverage,
            &["draw_mode=points".to_string(), "mirror=off".to_string()],
        )
        .unwrap();
        let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
        let ListObsPlotPayload::UvCoverage(payload) = payload else {
            panic!("expected uv payload");
        };
        assert!(payload.draw_points);
        assert!(!payload.mirror);
        assert_eq!(payload.tracks[0].points.len(), 1);
    }

    #[test]
    fn uv_tracks_mode_remains_explicitly_available() {
        let coverage = synthetic_uv_coverage();
        let spec = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::UvCoverage,
            &["draw_mode=tracks".to_string()],
        )
        .unwrap();
        let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
        let ListObsPlotPayload::UvCoverage(payload) = payload else {
            panic!("expected uv payload");
        };
        assert!(!payload.draw_points);
    }

    #[test]
    fn uv_payload_builder_covers_runtime_validation_paths() {
        let coverage = synthetic_uv_coverage();

        let error = build_listobs_plot_payload_from_summary(
            &synthetic_summary(),
            &ListObsPlotKind::UvCoverage.default_spec(),
        )
        .unwrap_err();
        assert!(error.contains("build_listobs_uv_plot_payload"));

        let error = build_listobs_uv_plot_payload(
            &coverage,
            &ListObsPlotKind::AntennaLayout.default_spec(),
        )
        .unwrap_err();
        assert!(error.contains("does not match UV coverage payload builder"));

        let mut spec = ListObsPlotKind::UvCoverage.default_spec();
        spec.options
            .insert("axis_extent".to_string(), "42.5".to_string());
        let payload = build_listobs_uv_plot_payload(&coverage, &spec).unwrap();
        assert_eq!(payload.kind(), ListObsPlotKind::UvCoverage);
        let ListObsPlotPayload::UvCoverage(payload) = payload else {
            panic!("expected uv payload");
        };
        assert_eq!(payload.axis_extent_lambda, 42.5);

        let mut invalid_axis = ListObsPlotKind::UvCoverage.default_spec();
        invalid_axis
            .options
            .insert("axis_extent".to_string(), "oops".to_string());
        let error = build_listobs_uv_plot_payload(&coverage, &invalid_axis).unwrap_err();
        assert!(error.contains("invalid axis_extent"));

        let mut invalid_draw_mode = ListObsPlotKind::UvCoverage.default_spec();
        invalid_draw_mode
            .options
            .insert("draw_mode".to_string(), "bogus".to_string());
        let error = build_listobs_uv_plot_payload(&coverage, &invalid_draw_mode).unwrap_err();
        assert!(error.contains("invalid draw_mode"));
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

    #[test]
    fn antenna_layout_falls_back_to_absolute_coordinates_when_offsets_are_all_zero() {
        let mut summary = synthetic_summary();
        for antenna in &mut summary.antennas {
            antenna.offset_from_observatory_m = [0.0, 0.0, 0.0];
        }
        let spec = ListObsPlotKind::AntennaLayout.default_spec();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        let ListObsPlotPayload::AntennaLayout(payload) = payload else {
            panic!("expected antenna layout payload");
        };
        assert_eq!(payload.x_label, "ITRF X (m)");
        assert_eq!(payload.y_label, "ITRF Y (m)");
        assert!(payload.summary.contains("using ITRF X/Y"));
        assert!(payload.antennas.iter().any(|point| point.x.abs() > 1.0));
    }

    #[test]
    fn antenna_layout_supports_id_labels_absolute_coordinates_and_fixed_marker_sizes() {
        let summary = synthetic_summary();
        let spec = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::AntennaLayout,
            &[
                "labels=id".to_string(),
                "coordinates=absolute".to_string(),
                "size_by_diameter=off".to_string(),
            ],
        )
        .unwrap();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        let ListObsPlotPayload::AntennaLayout(payload) = payload else {
            panic!("expected antenna layout payload");
        };
        assert_eq!(payload.x_label, "ITRF X (m)");
        assert_eq!(payload.y_label, "ITRF Y (m)");
        assert!(payload.labels_enabled);
        assert!(
            payload
                .antennas
                .iter()
                .all(|point| point.marker_radius == 5 && point.label.parse::<usize>().is_ok())
        );
    }

    #[test]
    fn spectral_window_plot_uses_true_band_edges_for_descending_windows() {
        let mut summary = synthetic_summary();
        summary.spectral_windows = vec![SpectralWindowSummary {
            spectral_window_id: 7,
            name: "DESC".to_string(),
            num_channels: 4,
            frame: Some("TOPO".to_string()),
            first_channel_frequency_hz: 10.0e9,
            channel_width_hz: -1.0e9,
            reference_frequency_hz: 9.0e9,
            center_frequency_hz: 8.5e9,
            min_frequency_hz: 7.0e9,
            max_frequency_hz: 10.5e9,
            total_bandwidth_hz: 4.0e9,
            data_description_ids: vec![0],
            polarization_ids: vec![0],
            correlation_types: vec!["XX".to_string()],
        }];

        let spec = ListObsPlotKind::SpectralWindowCoverage.default_spec();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
            panic!("expected spectral window payload");
        };
        assert_eq!(payload.bars.len(), 1);
        let bar = &payload.bars[0];
        assert_eq!(bar.spectral_window_id, 7);
        assert_eq!(bar.start, 7.0);
        assert_eq!(bar.end, 10.5);
        assert!(bar.start < summary.spectral_windows[0].center_frequency_hz / 1.0e9);
        assert!(bar.end > summary.spectral_windows[0].center_frequency_hz / 1.0e9);
    }

    #[test]
    fn spectral_window_lane_labels_use_real_spw_ids() {
        let mut summary = synthetic_summary();
        summary.spectral_windows = vec![
            SpectralWindowSummary {
                spectral_window_id: 1,
                name: "LOW".to_string(),
                num_channels: 16,
                frame: Some("TOPO".to_string()),
                first_channel_frequency_hz: 1.0e9,
                channel_width_hz: 1.0e6,
                reference_frequency_hz: 1.0e9,
                center_frequency_hz: 1.008e9,
                min_frequency_hz: 0.9995e9,
                max_frequency_hz: 1.0165e9,
                total_bandwidth_hz: 16.0e6,
                data_description_ids: vec![0],
                polarization_ids: vec![0],
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            },
            SpectralWindowSummary {
                spectral_window_id: 2,
                name: "HIGH".to_string(),
                num_channels: 32,
                frame: Some("TOPO".to_string()),
                first_channel_frequency_hz: 1.2e9,
                channel_width_hz: 5.0e5,
                reference_frequency_hz: 1.2e9,
                center_frequency_hz: 1.208e9,
                min_frequency_hz: 1.19975e9,
                max_frequency_hz: 1.21625e9,
                total_bandwidth_hz: 16.0e6,
                data_description_ids: vec![1],
                polarization_ids: vec![0],
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            },
        ];

        let spec = ListObsPlotKind::SpectralWindowCoverage.default_spec();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
            panic!("expected spectral window payload");
        };

        let ids = payload
            .bars
            .iter()
            .map(|bar| bar.spectral_window_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn spectral_window_polarization_color_group_uses_setup_ids() {
        let mut summary = synthetic_summary();
        summary.polarization_setups = vec![
            PolarizationSummary {
                polarization_id: 0,
                num_correlations: 2,
                correlation_types: vec!["XX".to_string(), "YY".to_string()],
            },
            PolarizationSummary {
                polarization_id: 1,
                num_correlations: 4,
                correlation_types: vec![
                    "XX".to_string(),
                    "YY".to_string(),
                    "XY".to_string(),
                    "YX".to_string(),
                ],
            },
        ];
        summary.data_descriptions = vec![
            DataDescriptionSummary {
                data_description_id: 0,
                spectral_window_id: 0,
                polarization_id: 0,
                flagged: false,
            },
            DataDescriptionSummary {
                data_description_id: 1,
                spectral_window_id: 1,
                polarization_id: 1,
                flagged: false,
            },
        ];
        summary.spectral_windows = vec![
            SpectralWindowSummary {
                spectral_window_id: 0,
                name: "LOW".to_string(),
                num_channels: 16,
                frame: Some("TOPO".to_string()),
                first_channel_frequency_hz: 1.0e9,
                channel_width_hz: 1.0e6,
                reference_frequency_hz: 1.0e9,
                center_frequency_hz: 1.008e9,
                min_frequency_hz: 0.9995e9,
                max_frequency_hz: 1.0165e9,
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
                min_frequency_hz: 1.19975e9,
                max_frequency_hz: 1.21625e9,
                total_bandwidth_hz: 16.0e6,
                data_description_ids: vec![1],
                polarization_ids: vec![1],
                correlation_types: vec![
                    "XX".to_string(),
                    "YY".to_string(),
                    "XY".to_string(),
                    "YX".to_string(),
                ],
            },
        ];

        let spec = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::SpectralWindowCoverage,
            &["color_by=polarization".to_string()],
        )
        .unwrap();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
            panic!("expected spectral window payload");
        };

        let color_groups = payload
            .bars
            .iter()
            .map(|bar| bar.color_group.clone())
            .collect::<Vec<_>>();
        assert_eq!(color_groups, vec!["pol-0".to_string(), "pol-1".to_string()]);
    }

    #[test]
    fn spectral_window_plot_supports_mhz_units_and_hidden_labels() {
        let summary = synthetic_summary();
        let spec = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::SpectralWindowCoverage,
            &["unit=mhz".to_string(), "labels=off".to_string()],
        )
        .unwrap();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
            panic!("expected spectral window payload");
        };
        assert_eq!(payload.x_label, "Frequency (MHz)");
        assert!(payload.bars.iter().all(|bar| bar.label.is_empty()));
        assert!(payload.bars.iter().all(|bar| bar.start > 900.0));
    }

    #[test]
    fn scan_timeline_plot_supports_field_lanes_and_hidden_labels() {
        let summary = synthetic_summary();
        let spec = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::ScanTimeline,
            &[
                "lanes=field".to_string(),
                "color_by=intent".to_string(),
                "labels=none".to_string(),
            ],
        )
        .unwrap();
        let payload = build_listobs_plot_payload_from_summary(&summary, &spec).unwrap();
        let ListObsPlotPayload::ScanTimeline(payload) = payload else {
            panic!("expected scan timeline payload");
        };
        assert_eq!(
            payload.lane_labels,
            vec!["FIELD_A".to_string(), "FIELD_B".to_string()]
        );
        assert!(
            payload
                .bars
                .iter()
                .all(|bar| bar.label.is_empty() && bar.color_group.contains("ON_SOURCE"))
        );
    }

    #[test]
    fn scan_timeline_and_spectral_payloads_handle_empty_and_fallback_data() {
        let mut empty_summary = synthetic_summary();
        empty_summary.scans.clear();
        empty_summary.spectral_windows = vec![SpectralWindowSummary {
            spectral_window_id: 9,
            name: String::new(),
            num_channels: 8,
            frame: Some("TOPO".to_string()),
            first_channel_frequency_hz: 1.4e9,
            channel_width_hz: 1.0e6,
            reference_frequency_hz: 1.4e9,
            center_frequency_hz: 1.404e9,
            min_frequency_hz: 1.3995e9,
            max_frequency_hz: 1.4085e9,
            total_bandwidth_hz: 8.0e6,
            data_description_ids: vec![0],
            polarization_ids: vec![],
            correlation_types: vec!["XX".to_string()],
        }];

        let timeline = build_listobs_plot_payload_from_summary(
            &empty_summary,
            &ListObsPlotKind::ScanTimeline.default_spec(),
        )
        .unwrap();
        let ListObsPlotPayload::ScanTimeline(timeline) = timeline else {
            panic!("expected scan timeline payload");
        };
        assert!(timeline.bars.is_empty());
        assert!(timeline.lane_labels.is_empty());
        assert_eq!(timeline.start_mjd_seconds, 0.0);
        assert_eq!(timeline.end_mjd_seconds, 1.0);

        let spec = ListObsPlotSpec::from_cli_assignments(
            ListObsPlotKind::SpectralWindowCoverage,
            &["color_by=polarization".to_string()],
        )
        .unwrap();
        let payload = build_listobs_plot_payload_from_summary(&empty_summary, &spec).unwrap();
        let ListObsPlotPayload::SpectralWindowCoverage(payload) = payload else {
            panic!("expected spectral window payload");
        };
        assert_eq!(payload.bars.len(), 1);
        assert_eq!(payload.bars[0].label, "SPW 9");
        assert_eq!(payload.bars[0].color_group, "pol-none");
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
                    min_frequency_hz: 0.9995e9,
                    max_frequency_hz: 1.0165e9,
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
                    min_frequency_hz: 1.19975e9,
                    max_frequency_hz: 1.21625e9,
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
