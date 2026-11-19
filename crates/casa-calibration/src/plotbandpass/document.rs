// SPDX-License-Identifier: LGPL-3.0-or-later
//! Pack calibration panels and file names into the shared plot document.

use std::path::PathBuf;

use casa_ms::plot_document::{
    PlotAxisScale, PlotDocument, PlotDocumentAnnotation, PlotDocumentAxis, PlotDocumentLayer,
    PlotDocumentPanel, PlotFigure, PlotLayerKind, PlotLayerStyle, PlotLayout, PlotPayloadStrategy,
    PlotTextStyle,
};
use serde::Serialize;

use super::{BandpassPanel, BandpassPlots};

pub(crate) struct DisplayOptions<'a> {
    pub(crate) layout: [u32; 2],
    pub(crate) figfile: &'a str,
    pub(crate) sequential_names: bool,
    pub(crate) density: u32,
    pub(crate) markersize: f64,
    pub(crate) show_points: Option<bool>,
    pub(crate) show_lines: Option<bool>,
    pub(crate) interactive: bool,
    pub(crate) limits: [f64; 4],
    pub(crate) phase_limits: Option<[f64; 2]>,
    pub(crate) channel_range: Option<ChannelRange>,
    pub(crate) limit_channels: bool,
    pub(crate) resample: u32,
}

#[derive(Clone, Copy)]
pub(crate) enum ChannelRange {
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
pub(crate) struct BandpassPage {
    figure: PlotFigure,
    density: u32,
    y_limits: Vec<Option<[f64; 2]>>,
}

pub(crate) fn pages(
    plot: BandpassPlots,
    display: &DisplayOptions<'_>,
) -> Result<Vec<BandpassPage>, String> {
    let [rows, columns] = display.layout;
    let capacity = (rows as usize)
        .checked_mul(columns as usize)
        .filter(|&capacity| capacity > 0)
        .ok_or("bandpass subplot needs a positive grid size")?;
    let count = plot.panels.len().div_ceil(capacity);
    let subtitle = plot
        .measurement_set
        .as_deref()
        .and_then(|path| path.file_name())
        .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
    plot.panels
        .chunks(capacity)
        .enumerate()
        .map(|(index, source)| {
            let panels = source
                .iter()
                .enumerate()
                .map(|(index, panel)| document_panel(panel, index, display))
                .collect();
            let y_limits = source
                .iter()
                .map(|panel| y_limits(panel, display))
                .collect();
            let document = PlotDocument {
                id: format!("plotbandpass-{index:03}"),
                title: String::new(),
                subtitle: subtitle.clone(),
                header_lines: vec![plot.caltable.display().to_string()],
                axes: Vec::new(),
                layers: Vec::new(),
                annotations: Vec::new(),
                panels,
                show_legend: false,
                layout: Some(PlotLayout {
                    rows,
                    columns,
                    aspect: None,
                    margins: None,
                    font_size: None,
                }),
            };
            document
                .validate_payload(0)
                .map_err(|error| error.to_string())?;
            let output_file = output_file(display, &source[source.len() - 1].suffix, index, count);
            Ok(BandpassPage {
                figure: PlotFigure {
                    document,
                    output_file,
                    show: display.interactive,
                },
                density: display.density,
                y_limits,
            })
        })
        .collect()
}

fn output_file(
    display: &DisplayOptions<'_>,
    suffix: &str,
    page: usize,
    count: usize,
) -> Option<PathBuf> {
    if display.figfile.is_empty() {
        return None;
    }
    let name = display
        .figfile
        .split_once(".png")
        .map_or(display.figfile, |(base, _)| base);
    let name = if count == 1 && display.sequential_names {
        name.to_string()
    } else if display.sequential_names {
        format!("{name}.{page:03}")
    } else {
        format!("{name}.{suffix}")
    };
    if display.resample > 1 && (count > 1 || !display.sequential_names) {
        Some(format!("{name}.resample{}.png", display.resample).into())
    } else {
        Some(format!("{name}.png").into())
    }
}

fn document_panel(
    bandpass: &BandpassPanel,
    panel_number: usize,
    display: &DisplayOptions<'_>,
) -> PlotDocumentPanel {
    let phase = bandpass.y_label.to_ascii_lowercase().starts_with("phase");
    let points = display.show_points.unwrap_or(phase);
    let lines = display.show_lines.unwrap_or(!phase);
    let palette = [
        "black", "red", "blue", "green", "#7f3f98", "#a57a24", "#228a84",
    ];
    let mut annotations = Vec::new();
    let mut labels = std::collections::BTreeSet::new();
    let layers = bandpass
        .series
        .iter()
        .enumerate()
        .map(|(index, data)| {
            let color = palette[index % palette.len()];
            if labels.insert(&data.label) {
                annotations.push(PlotDocumentAnnotation {
                    id: format!("polarization-{}", data.receptor),
                    x: 0.05,
                    y: 0.95 - 0.07 * data.receptor as f64,
                    text: data.label.clone(),
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
                symbol_size: display.markersize,
                line_width: 1.0,
                opacity: 1.0,
                source_sample_count: data.curve.x.len() as u64,
                payload_strategy: PlotPayloadStrategy::PointCloud,
                provenance_summary: String::new(),
                data: None,
                style: Some(PlotLayerStyle {
                    color: Some(color.into()),
                    marker: Some(if points { "." } else { "None" }.into()),
                    line_style: Some(if lines { "-" } else { "None" }.into()),
                }),
            }
        })
        .collect();
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
