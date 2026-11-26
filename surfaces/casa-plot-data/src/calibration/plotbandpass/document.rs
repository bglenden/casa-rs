// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pack calibration panels and file names into the shared plot document.

use std::path::PathBuf;

use crate::plot_document::{
    PlotAxisScale, PlotDocument, PlotDocumentAnnotation, PlotDocumentAxis, PlotDocumentLayer,
    PlotDocumentPanel, PlotFigure, PlotLayerKind, PlotLayerStyle, PlotLayout, PlotPayloadStrategy,
    PlotTextStyle,
};
use casa_types::quanta::format::{YMD, format_time};
use serde::Serialize;

use super::atmosphere::AtmosphericCurve;
use super::{BandpassPanel, BandpassPlots, BandpassSeries, FdmWindow};

const IDENTITY_COLORS: &[&str] = &[
    "k", "b", "g", "r", "#00bfbf", "#404040", "#bf4040", "#f2f200", "#4040bf", "#00ff00",
    "#c2912b", "#8aa138", "#5791eb", "#ff1a99", "#197d78", "#a857a6", "#fc693b",
];

fn identity_color(index: usize) -> &'static str {
    IDENTITY_COLORS[index % IDENTITY_COLORS.len()]
}

pub struct DisplayOptions<'a> {
    pub layout: [u32; 2],
    pub figfile: &'a str,
    pub sequential_names: bool,
    pub density: u32,
    pub markersize: f64,
    pub show_points: Option<bool>,
    pub show_lines: Option<bool>,
    pub interactive: bool,
    pub limits: [f64; 4],
    pub phase_limits: Option<[f64; 2]>,
    pub channel_range: Option<ChannelRange>,
    pub limit_channels: bool,
    pub resample: u32,
}

#[derive(Clone, Copy)]
pub enum ChannelRange {
    Indices([usize; 2]),
    CentralPercent(f64),
}

impl ChannelRange {
    fn frequencies(self, coordinates: &[f64]) -> Option<[f64; 2]> {
        let last = coordinates.len().checked_sub(1)?;
        let [start, end] = match self {
            Self::Indices(bounds) => bounds,
            Self::CentralPercent(percent) => {
                let outer = (100.0 - percent) * 0.005;
                [
                    (last as f64 * outer).round_ties_even() as usize,
                    (last as f64 * (1.0 - outer)).round_ties_even() as usize,
                ]
            }
        };
        Some([*coordinates.get(start)?, *coordinates.get(end)?])
    }
}

#[derive(Serialize)]
pub struct BandpassPage {
    figure: PlotFigure,
    density: u32,
    y_limits: Vec<Option<[f64; 2]>>,
    atmosphere: Vec<Vec<AtmosphericCurve>>,
    fdm: Vec<Option<Vec<FdmWindow>>>,
    prompt: bool,
    quit_immediately: bool,
    footer: String,
    diagnostics: Vec<String>,
    legends: Vec<OverlayLegend>,
}

#[derive(Default, Serialize)]
struct OverlayLegend {
    identities: Vec<LegendIdentity>,
    markers: Vec<LegendMarker>,
}

#[derive(Serialize)]
struct LegendIdentity {
    text: String,
    color: &'static str,
    x: f64,
    y: f64,
}

#[derive(Serialize)]
struct LegendMarker {
    marker: &'static str,
    x: f64,
    y: f64,
}

pub fn pages(
    plot: BandpassPlots,
    display: &DisplayOptions<'_>,
) -> Result<Vec<BandpassPage>, String> {
    let [rows, columns] = display.layout;
    let capacity = (rows as usize)
        .checked_mul(columns as usize)
        .filter(|&capacity| capacity > 0)
        .ok_or("bandpass subplot needs a positive grid size")?;
    let subtitle = plot
        .measurement_set
        .as_deref()
        .and_then(|path| path.file_name())
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    let with_phase_page = capacity == 1
        && plot
            .panels
            .iter()
            .any(|panel| panel.ordinate == super::Ordinate::Phase);
    plot.panels
        .chunks(capacity)
        .enumerate()
        .map(|(index, source)| {
            let (panels, legends) = source
                .iter()
                .enumerate()
                .map(|(index, panel)| document_panel(panel, index, display))
                .unzip();
            let y_limits = source
                .iter()
                .map(|panel| y_limits(panel, display))
                .collect();
            let document = PlotDocument {
                id: format!("plotbandpass-{index:03}"),
                title: String::new(),
                subtitle: subtitle.clone(),
                header_lines: vec![plot.header.clone()],
                axes: Vec::new(),
                layers: Vec::new(),
                annotations: Vec::new(),
                panels,
                show_legend: false,
                layout: Some(PlotLayout {
                    rows,
                    columns,
                    first_panel: None,
                    aspect: None,
                    margins: None,
                    font_size: None,
                }),
            };
            document
                .validate_payload(0)
                .map_err(|error| error.to_string())?;
            let first = &source[0];
            let last = &source[source.len() - 1];
            // CASA records the page cursor before trying to draw a panel, so
            // a multi-panel filename can refer to a skipped flagged group.
            let suffix = if capacity == 1 {
                first.suffix.as_str()
            } else {
                first
                    .first_skipped_suffix
                    .as_deref()
                    .unwrap_or(&first.suffix)
            };
            let output_file = output_file(display, suffix, index);
            let date = format_time(last.epoch / 86_400.0, YMD, 0);
            let date = date.get(..10).unwrap_or(&date).replace('/', "-");
            let mut footer = format!("{subtitle}  ObsDate={date}");
            let regression = std::path::Path::new(display.figfile)
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("regression"));
            if !regression {
                footer.push_str("   plotbandpass v2.20 = 2024/09/04 16:00:03");
            }
            Ok(BandpassPage {
                figure: PlotFigure {
                    document,
                    output_file,
                    show: display.interactive,
                },
                density: display.density,
                y_limits,
                atmosphere: source
                    .iter()
                    .map(|panel| panel.atmosphere.clone())
                    .collect(),
                fdm: source.iter().map(|panel| panel.fdm.clone()).collect(),
                prompt: display.interactive && source.len() == capacity,
                quit_immediately: plot.polynomial
                    || plot.immediate_combined_overlay
                    || (with_phase_page && last.ordinate != super::Ordinate::Phase),
                footer,
                diagnostics: source
                    .iter()
                    .flat_map(|panel| panel.diagnostics.iter().cloned())
                    .collect(),
                legends,
            })
        })
        .collect()
}

fn output_file(display: &DisplayOptions<'_>, suffix: &str, page: usize) -> Option<PathBuf> {
    if display.figfile.is_empty() {
        return None;
    }
    let name = display
        .figfile
        .split_once(".png")
        .map_or(display.figfile, |(base, _)| base);
    let name = if display.sequential_names {
        format!("{name}.{page:03}")
    } else {
        format!("{name}.{suffix}")
    };
    if display.resample > 1 {
        Some(format!("{name}.resample{}.png", display.resample).into())
    } else {
        Some(format!("{name}.png").into())
    }
}

fn document_panel(
    bandpass: &BandpassPanel,
    panel_number: usize,
    display: &DisplayOptions<'_>,
) -> (PlotDocumentPanel, OverlayLegend) {
    let phase = bandpass.ordinate == super::Ordinate::Phase;
    let (points, lines) = match (display.show_points, display.show_lines) {
        (None, None) => (phase, !phase),
        (None, Some(lines)) => (!lines, lines),
        (Some(false), _) => (false, true),
        (Some(true), lines) => (true, lines.unwrap_or(!phase)),
    };
    let delay = bandpass.ordinate == super::Ordinate::Delay;
    let markersize = if delay {
        display.markersize.max(8.0)
    } else {
        display.markersize
    };
    let (points, lines) = if delay {
        (true, false)
    } else {
        (points, lines)
    };
    let mut paint = PanelColors::new(bandpass, display.layout);
    let identity_overlay = bandpass.overlay.antenna || bandpass.overlay.time;
    let combined = bandpass.overlay.antenna && bandpass.overlay.time;
    let b_comparison = bandpass
        .series
        .iter()
        .any(|series| !series.polynomial && series.table_index > 0);
    let dual_receptor = bandpass.series.iter().any(|series| series.receptor == 0)
        && bandpass.series.iter().any(|series| series.receptor == 1);
    let mut annotations = Vec::new();
    let mut labels = std::collections::BTreeSet::new();
    let layers = bandpass
        .series
        .iter()
        .enumerate()
        .map(|(index, data)| {
            let color = paint.color(data, index);
            let points = points && !data.polynomial;
            let lines = lines || data.polynomial;
            let dashed = identity_overlay && dual_receptor && data.receptor == 1 && lines;
            let marker = if points {
                if data.table_index > 0 || (identity_overlay && data.receptor == 1) {
                    "o"
                } else {
                    "."
                }
            } else {
                "None"
            };
            let table_id = if b_comparison { data.table_index } else { 0 };
            if !combined && labels.insert((&data.label, table_id)) {
                let rows = f64::from(display.layout[0]);
                let text = if b_comparison {
                    format!("{}-c{}", data.label, table_id + 1)
                } else if identity_overlay && lines {
                    format!("{} {}", data.label, if dashed { "dashed" } else { "solid" })
                } else {
                    data.label.clone()
                };
                let y = 1.0 - (0.04 + 0.03 * (data.receptor + 2 * table_id) as f64) * rows;
                if identity_overlay && points && !lines {
                    let offset = if phase {
                        0.015
                            * if bandpass.overlay.antenna && data.receptor == 0 {
                                1.0
                            } else {
                                rows
                            }
                    } else {
                        0.0
                    };
                    paint.legend.markers.push(LegendMarker {
                        marker,
                        x: if phase { 0.05 } else { 0.04 },
                        y: y + offset,
                    });
                }
                annotations.push(PlotDocumentAnnotation {
                    id: format!("polarization-{table_id}-{}", data.receptor),
                    x: if identity_overlay && points && !lines {
                        0.07
                    } else {
                        0.05
                    },
                    y,
                    text,
                    style: Some(PlotTextStyle {
                        axes_relative: true,
                        horizontal_alignment: Some("left".into()),
                        vertical_alignment: Some("top".into()),
                        font_size: None,
                        background: None,
                    }),
                });
            }
            PlotDocumentLayer {
                id: format!("series-{index}"),
                title: String::new(),
                kind: PlotLayerKind::Line,
                x_axis_id: "x".into(),
                y_axis_id: "y".into(),
                x_values: data.curve.x.clone(),
                y_values: data.curve.y.clone(),
                point_labels: Vec::new(),
                point_symbol_sizes: Vec::new(),
                interval_x_start: Vec::new(),
                interval_x_end: Vec::new(),
                interval_y: Vec::new(),
                interval_height: Vec::new(),
                interval_labels: Vec::new(),
                provenance: Vec::new(),
                color_group: color.into(),
                symbol_size: markersize,
                line_width: 1.0,
                opacity: 1.0,
                source_sample_count: data.curve.x.len() as u64,
                payload_strategy: PlotPayloadStrategy::PointCloud,
                provenance_summary: String::new(),
                data: None,
                style: Some(PlotLayerStyle {
                    color: Some(color.into()),
                    marker: Some(marker.into()),
                    line_style: Some(
                        (if dashed {
                            "--"
                        } else if lines {
                            "-"
                        } else {
                            "None"
                        })
                        .into(),
                    ),
                }),
            }
        })
        .collect();
    (
        PlotDocumentPanel {
            id: format!("panel-{panel_number}"),
            title: bandpass.title.clone(),
            title_font_size: None,
            axes: vec![
                axis("x", &bandpass.x_label, x_limits(bandpass, display)),
                axis(
                    "y",
                    bandpass.y_label,
                    y_limits(bandpass, display).unwrap_or([0.0, 1.0]),
                ),
            ],
            layers,
            annotations,
            aspect: None,
            frame_visible: None,
        },
        paint.legend,
    )
}

struct PanelColors<'a> {
    bandpass: &'a BandpassPanel,
    antennas: Vec<&'a str>,
    first_time: usize,
    legend: OverlayLegend,
}

impl<'a> PanelColors<'a> {
    fn new(bandpass: &'a BandpassPanel, layout: [u32; 2]) -> Self {
        let mut antennas = Vec::new();
        for series in &bandpass.series {
            let name = series.antenna_name.as_str();
            if !antennas.contains(&name) {
                antennas.push(name);
            }
        }
        let mut legend = OverlayLegend::default();
        if bandpass.overlay.antenna && !bandpass.overlay.time {
            legend
                .identities
                .extend(
                    antennas
                        .iter()
                        .enumerate()
                        .map(|(index, name)| LegendIdentity {
                            text: (*name).into(),
                            color: identity_color(index),
                            x: 0.07 + 0.05 * index.min(17) as f64,
                            y: 0.931 - 0.018 * index.saturating_sub(17) as f64,
                        }),
                );
        } else if bandpass.overlay.time {
            legend.identities.push(LegendIdentity {
                text: "UT".into(),
                color: "k",
                x: 0.04,
                y: 0.931,
            });
            let spacing = if layout[1] == 1 { 0.078 } else { 0.065 };
            legend
                .identities
                .extend(bandpass.legend_times.iter().enumerate().map(
                    |(position, &(index, epoch))| {
                        let date = format_time(epoch / 86_400.0, YMD, 0);
                        LegendIdentity {
                            text: date.get(11..).unwrap_or(&date).into(),
                            color: if bandpass.overlay.antenna {
                                "k"
                            } else {
                                identity_color(index)
                            },
                            x: 0.07 + spacing * position.min(12) as f64,
                            y: 0.931 - 0.018 * position.saturating_sub(12) as f64,
                        }
                    },
                ));
        }
        let first_time = bandpass.legend_times.first().map_or(0, |(index, _)| *index);
        Self {
            bandpass,
            antennas,
            first_time,
            legend,
        }
    }

    fn color(&self, series: &BandpassSeries, index: usize) -> &'static str {
        let overlay = self.bandpass.overlay;
        let antenna = || {
            self.antennas
                .iter()
                .position(|name| *name == series.antenna_name)
                .unwrap_or(0)
        };
        if overlay.antenna && overlay.time {
            if series.time_index == self.first_time {
                return ["b", "g", "r", "c"][index % 4];
            }
            return identity_color(antenna());
        }
        if overlay.antenna {
            return identity_color(antenna());
        }
        if overlay.time {
            return identity_color(series.time_index);
        }
        [["b", "g"], ["k", "c"], ["m", "r"]][series.table_index][series.receptor % 2]
    }
}

fn axis(id: &str, label: &str, [lower, upper]: [f64; 2]) -> PlotDocumentAxis {
    PlotDocumentAxis {
        id: id.into(),
        label: label.into(),
        unit: String::new(),
        lower,
        upper,
        scale: PlotAxisScale::Linear,
        lane_labels: Vec::new(),
        draws_on_trailing_edge: false,
    }
}

fn x_limits(panel: &BandpassPanel, display: &DisplayOptions<'_>) -> [f64; 2] {
    let requested = display
        .limit_channels
        .then(|| {
            display
                .channel_range
                .and_then(|range| range.frequencies(&panel.frequency_channels))
        })
        .flatten();
    let [lower, upper] = requested.unwrap_or([display.limits[0], display.limits[1]]);
    if lower == 0.0 && upper == 0.0 {
        return panel.x_limits;
    }
    let range = (upper - lower).abs();
    let pad = if range == 0.0 { 1e-5 } else { 0.01 * range };
    [lower.min(upper) - pad, lower.max(upper) + pad]
}

fn y_limits(panel: &BandpassPanel, display: &DisplayOptions<'_>) -> Option<[f64; 2]> {
    let [_, _, lower, upper] = display.limits;
    if lower < upper {
        return Some([lower, upper]);
    }
    if panel.y_label == "Phase (deg)" {
        if let Some(limits) = display.phase_limits {
            return Some(limits);
        }
        if let Some(first) = panel.series.first()
            && first.curve.y.iter().all(|sample| sample.abs() < 0.2)
        {
            return Some([-0.2, 0.2]);
        }
    }
    let channel_limits = display
        .channel_range
        .and_then(|range| range.frequencies(&panel.frequency_channels));
    let requested = channel_limits.or_else(|| {
        let [lower, upper, ..] = display.limits;
        (!panel.frequency_channels.is_empty() && (lower != 0.0 || upper != 0.0))
            .then_some([lower, upper])
    })?;
    let lower = requested[0].min(requested[1]);
    let upper = requested[0].max(requested[1]);
    let mut values = panel.series.iter().flat_map(|series| {
        series
            .curve
            .x
            .iter()
            .zip(&series.curve.y)
            .filter_map(move |(&x, &y)| (x >= lower && x <= upper && y.is_finite()).then_some(y))
    });
    let first = values.next()?;
    let [min, max] = values.fold([first, first], |[min, max], y| [min.min(y), max.max(y)]);
    (min < max).then_some([min, max])
}
