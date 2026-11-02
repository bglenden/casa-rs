// SPDX-License-Identifier: LGPL-3.0-or-later

//! Conversion of msexplore plot payloads into [`PlotDocument`]s and their
//! metadata.
//!
//! Scatter payloads go through the shared [`MsPlotData`] projection, except a
//! one-panel scatter with a secondary Y axis; listobs payloads (UV coverage,
//! antenna layout, scan and spectral-window lanes) are converted per kind.

use std::collections::BTreeMap;

use super::{
    POINT_PROVENANCE_LIMIT, PlotAxisMetadata, PlotAxisScale, PlotDocument, PlotDocumentAnnotation,
    PlotDocumentAxis, PlotDocumentLayer, PlotDocumentPanel, PlotLayerKind, PlotPayloadStrategy,
    PlotPointProvenance, PlotSamplingDiagnostics, PlotSeriesMetadata,
};
use crate::plot::{
    AntennaLayoutPlotPayload, ScanTimelinePlotPayload, SpectralWindowCoveragePlotPayload,
    UvCoveragePlotPayload, UvCoverageSeries, VisibilityScatterPlotPayload,
};
use crate::{
    MeasurementSetPlotPayload, MsPlotData, MsPlotDataPanel, MsPlotPayload, MsPlotPreset,
    MsScatterGridPayload, MsScatterPagePayload, MsScatterPlotPayload, MsScatterSeries,
};

/// A plot's title, summary, primary axes, series and sampling.
pub(super) struct PayloadMetadata {
    pub(super) title: String,
    pub(super) summary: String,
    pub(super) x_axis: PlotAxisMetadata,
    pub(super) y_axis: PlotAxisMetadata,
    pub(super) series: Vec<PlotSeriesMetadata>,
    pub(super) sampling: PlotSamplingDiagnostics,
}

/// The metadata of one payload built with `requested_max_points`.
pub(super) fn plot_payload_metadata(
    payload: &MsPlotPayload,
    preset: MsPlotPreset,
    requested_max_points: u64,
) -> PayloadMetadata {
    match payload {
        MsPlotPayload::Scatter(payload) => scatter_metadata(payload, requested_max_points),
        MsPlotPayload::ScatterGrid(payload) => scatter_grid_metadata(payload, requested_max_points),
        MsPlotPayload::ScatterPage(payload) => scatter_page_metadata(payload, requested_max_points),
        MsPlotPayload::ListObs(payload) => listobs_metadata(payload, preset, requested_max_points),
    }
}

/// The document of one payload, titled from its metadata.
pub(super) fn plot_document(
    payload: &MsPlotPayload,
    metadata: &PayloadMetadata,
    preset: MsPlotPreset,
) -> PlotDocument {
    match payload {
        MsPlotPayload::Scatter(scatter) if scatter.secondary_y_axis.is_some() => {
            secondary_axis_scatter_document(scatter, metadata, preset)
        }
        MsPlotPayload::Scatter(_)
        | MsPlotPayload::ScatterGrid(_)
        | MsPlotPayload::ScatterPage(_) => shared_scatter_plot_document(payload, metadata, preset),
        MsPlotPayload::ListObs(payload) => listobs_plot_document(payload, metadata, preset),
    }
}

fn listobs_plot_document(
    payload: &MeasurementSetPlotPayload,
    metadata: &PayloadMetadata,
    preset: MsPlotPreset,
) -> PlotDocument {
    let (axes, layers) = match payload {
        MeasurementSetPlotPayload::UvCoverage(payload) => uv_coverage_layers(payload),
        MeasurementSetPlotPayload::VisibilityScatter(payload) => visibility_scatter_layers(payload),
        MeasurementSetPlotPayload::AntennaLayout(payload) => antenna_layout_layers(payload),
        MeasurementSetPlotPayload::ScanTimeline(payload) => scan_timeline_layers(payload),
        MeasurementSetPlotPayload::SpectralWindowCoverage(payload) => {
            spectral_window_coverage_layers(payload)
        }
    };
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

type AxesAndLayers = (Vec<PlotDocumentAxis>, Vec<PlotDocumentLayer>);

fn uv_coverage_layers(payload: &UvCoveragePlotPayload) -> AxesAndLayers {
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
    (axes, layers)
}

fn visibility_scatter_layers(payload: &VisibilityScatterPlotPayload) -> AxesAndLayers {
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
            let (x_values, y_values): (Vec<_>, Vec<_>) = series.points.iter().copied().unzip();
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
    (axes, layers)
}

fn antenna_layout_layers(payload: &AntennaLayoutPlotPayload) -> AxesAndLayers {
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
        provenance_summary: "ANTENNA table positions from Rust msexplore payload".to_string(),
    });
    (axes, vec![layer])
}

fn scan_timeline_layers(payload: &ScanTimelinePlotPayload) -> AxesAndLayers {
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
    (axes, layers)
}

fn spectral_window_coverage_layers(payload: &SpectralWindowCoveragePlotPayload) -> AxesAndLayers {
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
                provenance_summary: "SPECTRAL_WINDOW coverage interval from Rust msexplore payload"
                    .to_string(),
            })
        })
        .collect();
    (axes, layers)
}

/// A one-panel scatter whose series split between a primary and a
/// secondary Y axis, drawn on the trailing edge.
fn secondary_axis_scatter_document(
    payload: &MsScatterPlotPayload,
    metadata: &PayloadMetadata,
    preset: MsPlotPreset,
) -> PlotDocument {
    let mut axes = axes_for_ranges(
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
        let (lower, upper) = payload.secondary_fixed_y_bounds.unwrap_or_else(|| {
            series_y_bounds(
                payload
                    .series
                    .iter()
                    .filter(|series| Some(series.y_axis) == payload.secondary_y_axis)
                    .flat_map(|series| series.points.iter().map(|(_, y)| *y)),
            )
        });
        axes.push(PlotDocumentAxis {
            draws_on_trailing_edge: true,
            ..document_axis(
                "y-secondary",
                secondary_label,
                &label_unit(secondary_label),
                lower,
                upper,
                PlotAxisScale::Linear,
            )
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

fn shared_scatter_plot_document(
    payload: &MsPlotPayload,
    metadata: &PayloadMetadata,
    preset: MsPlotPreset,
) -> PlotDocument {
    let shared = MsPlotData::from_payload(payload)
        .expect("scatter payloads always project into the shared plot-data contract");
    let mut panels = shared
        .panels
        .iter()
        .map(shared_plot_panel)
        .collect::<Vec<_>>();
    if matches!(payload, MsPlotPayload::Scatter(_)) {
        let panel = panels.remove(0);
        return base_document(
            preset,
            metadata,
            shared.header_lines,
            panel.axes,
            panel.layers,
            panel.annotations,
            Vec::new(),
        );
    }
    base_document(
        preset,
        metadata,
        shared.header_lines,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        panels,
    )
}

fn shared_plot_panel(panel: &MsPlotDataPanel) -> PlotDocumentPanel {
    PlotDocumentPanel {
        id: panel.id.clone(),
        title: panel.title.clone(),
        axes: panel
            .axes
            .iter()
            .map(|axis| {
                document_axis(
                    &axis.id,
                    &axis.label,
                    &axis.unit,
                    axis.lower,
                    axis.upper,
                    PlotAxisScale::Linear,
                )
            })
            .collect(),
        layers: panel
            .series
            .iter()
            .enumerate()
            .map(|(index, series)| {
                point_layer(PointLayerSpec {
                    id: format!("series-{index}"),
                    title: series.label.clone(),
                    x_axis_id: "x",
                    y_axis_id: &series.y_axis_id,
                    x_values: series.x.clone(),
                    y_values: series.y.clone(),
                    point_labels: Vec::new(),
                    point_symbol_sizes: Vec::new(),
                    provenance: if series.x.len() <= POINT_PROVENANCE_LIMIT {
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
                    },
                    color_group: series.color_group.clone(),
                    symbol_size: panel.symbol_size,
                    line_width: 1.0,
                    provenance_summary: "Visibility samples from shared casa-ms plot data"
                        .to_owned(),
                })
            })
            .collect(),
        annotations: Vec::new(),
    }
}

fn base_document(
    preset: MsPlotPreset,
    metadata: &PayloadMetadata,
    header_lines: Vec<String>,
    axes: Vec<PlotDocumentAxis>,
    layers: Vec<PlotDocumentLayer>,
    annotations: Vec<PlotDocumentAnnotation>,
    panels: Vec<PlotDocumentPanel>,
) -> PlotDocument {
    PlotDocument {
        id: format!("msexplore-{}", preset.as_str()),
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
            let provenance = if series.points.len() <= POINT_PROVENANCE_LIMIT {
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
                symbol_size: f64::from(symbol_size_px.unwrap_or(3)),
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
        payload_strategy: PlotPayloadStrategy::PointCloud,
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
        payload_strategy: PlotPayloadStrategy::Intervals,
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
    preset: MsPlotPreset,
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
            title: preset.display_name().to_string(),
            summary: "Metadata-oriented MeasurementSet plot.".to_string(),
            x_axis: axis_metadata(preset.as_str(), preset.display_name()),
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
