// SPDX-License-Identifier: LGPL-3.0-or-later
//! Make calibration layers for the shared PlotDocument renderer.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::axis::Axis;
use super::iteration::{Panel, PlotPanels};
use super::samples::Point;
use crate::plot_document::{
    PlotAxisScale, PlotDocument, PlotDocumentAxis, PlotDocumentLayer, PlotDocumentPanel,
    PlotFigure, PlotLayerData, PlotLayerKind, PlotLayerStyle, PlotLayout, PlotPayloadStrategy,
    PlotPayloadWriter,
};

pub(super) fn prepare(
    task: &TaskInvocation<'_>,
    payload: &Path,
    destination: Option<PathBuf>,
    show: bool,
    overwrite: bool,
    panels: &PlotPanels,
    header: impl Fn(&[Panel]) -> Vec<String>,
) -> Result<Vec<PlotFigure>, TaskFailure> {
    let pages = panels.pages().collect::<Vec<_>>();
    let destinations = pages
        .iter()
        .enumerate()
        .map(|(page, page_panels)| {
            destination
                .as_deref()
                .map(|path| panels.filename(path, page, page_panels))
        })
        .collect::<Vec<_>>();
    let output_paths = destinations
        .iter()
        .map(|path| ("plotfile", path.as_deref()))
        .collect::<Vec<_>>();
    crate::msexplore::task_contract::refuse_unwritable_outputs(
        &output_paths,
        overwrite,
        "set overwrite=True to replace it",
    )
    .map_err(TaskFailure::validation)?;
    let mut payload = PlotPayloadWriter::open(payload).map_err(write_failure)?;
    let ranges = AxisRanges::new(task, panels)?;
    let title = task.text("title")?.unwrap_or_default();
    let mut figures = Vec::new();
    for (page, (source, destination)) in pages.iter().zip(destinations).enumerate() {
        let content = source
            .iter()
            .enumerate()
            .map(|(index, panel)| make_panel(task, &mut payload, panels, panel, index, &ranges))
            .collect::<Result<Vec<_>, _>>()?;
        let [rows, columns] = panels.grid;
        let mut document = PlotDocument {
            id: format!("plotms-calibration-{page}"),
            title: title.clone(),
            subtitle: String::new(),
            header_lines: header(source),
            axes: Vec::new(),
            layers: Vec::new(),
            annotations: Vec::new(),
            panels: content,
            show_legend: task.flag("showlegend")?,
            layout: Some(PlotLayout {
                rows,
                columns,
                first_panel: None,
                aspect: None,
                margins: None,
                font_size: None,
            }),
        };
        if rows == 1 && columns == 1 && panels.axis_name().is_none() {
            let panel = document.panels.pop().expect("one panel");
            document.axes = panel.axes;
            document.layers = panel.layers;
            document.annotations = panel.annotations;
        }
        figures.push(PlotFigure {
            output_file: destination,
            show: show && page == 0,
            document,
        });
    }
    let bytes = payload.finish().map_err(write_failure)?;
    for figure in &figures {
        figure
            .document
            .validate_payload(bytes)
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
    }
    Ok(figures)
}

fn make_panel(
    task: &TaskInvocation<'_>,
    payload: &mut PlotPayloadWriter,
    panels: &PlotPanels,
    panel: &Panel,
    index: usize,
    ranges: &AxisRanges<'_>,
) -> Result<PlotDocumentPanel, TaskFailure> {
    let series = &panel.series;
    let xlabel = task.text("xlabel")?.filter(|label| !label.is_empty());
    let ylabel = task.text("ylabel")?.filter(|label| !label.is_empty());
    let manual = plot_limits(task.value("plotrange"))?;
    let color = task
        .text("coloraxis")?
        .filter(|value| !value.is_empty())
        .map(|value| Axis::parse(&value))
        .transpose()
        .map_err(TaskFailure::validation)?;
    let on_right = |index| right_side(&ranges.locations, index);
    let x = &series[0].x;
    let [x_range, left_range, right_range] = ranges.for_panel(panel);
    let mut axes = vec![axis(
        "x",
        *x,
        xlabel.as_deref(),
        x_range,
        [manual[0], manual[1]],
    )];
    for right in [false, true] {
        let selected = series
            .iter()
            .enumerate()
            .filter(|(index, _)| on_right(*index) == right)
            .map(|(_, series)| series)
            .collect::<Vec<_>>();
        let Some(first) = selected.first() else {
            continue;
        };
        let mut ordinate = axis(
            ordinate_id(right),
            first.y,
            ylabel.as_deref(),
            if right { right_range } else { left_range },
            [manual[2], manual[3]],
        );
        if selected.len() > 1 && ylabel.is_none() {
            ordinate.label = selected
                .iter()
                .map(|line| line.y.label())
                .collect::<Vec<_>>()
                .join(", ");
        }
        ordinate.draws_on_trailing_edge = right;
        axes.push(ordinate);
    }
    let connect = task
        .text("xconnector")?
        .is_some_and(|name| name == "line" || name == "step");
    let connect_times = task.flag("timeconnector")?;
    let mut layers = Vec::new();
    for (id, series) in series.iter().enumerate() {
        let yid = ordinate_id(on_right(id));
        let mut groups = BTreeMap::<(bool, String, u64), Vec<&Point>>::new();
        for point in series.points.iter().filter(|point| {
            point.x.is_finite()
                && point.y.is_finite()
                && (!point.flagged || shows_flagged(ranges.flagged_symbols, id))
        }) {
            let label = color.map_or_else(
                || series.y.label().to_string(),
                |color| {
                    format!(
                        "{} {}",
                        color.label(),
                        color.value(&point.coordinates, Default::default(), 0.0)
                    )
                },
            );
            let epoch = if connect && !connect_times {
                point.coordinates.time.to_bits()
            } else {
                0
            };
            groups
                .entry((point.flagged, label, epoch))
                .or_default()
                .push(point);
        }
        for ((flagged, label, epoch), points) in groups {
            layers.push(layer(
                payload,
                (index, id, yid, epoch),
                flagged,
                label,
                points,
                connect,
            )?);
        }
    }
    let colorize_overlay = if task.flag("colorizeoverlay")? {
        color
    } else {
        None
    };
    append_atmosphere(
        payload,
        panel,
        index,
        AtmosphereStyle {
            flagged_symbols: ranges.flagged_symbols,
            color: colorize_overlay,
            connect,
        },
        &mut axes,
        &mut layers,
    )?;
    let title = panels
        .axis_name()
        .map_or_else(String::new, |axis| format!("{axis} {}", panel.label));
    Ok(PlotDocumentPanel {
        id: format!("calibration-panel-{index}"),
        title,
        title_font_size: None,
        axes,
        layers,
        annotations: Vec::new(),
        aspect: None,
        frame_visible: None,
    })
}

struct AtmosphereStyle<'a> {
    flagged_symbols: Option<&'a ParameterValue>,
    color: Option<Axis>,
    connect: bool,
}

fn append_atmosphere(
    payload: &mut PlotPayloadWriter,
    panel: &Panel,
    index: usize,
    style: AtmosphereStyle<'_>,
    axes: &mut Vec<PlotDocumentAxis>,
    layers: &mut Vec<PlotDocumentLayer>,
) -> Result<(), TaskFailure> {
    let Some(first) = panel.overlay.first() else {
        return Ok(());
    };
    let mut bounds = [f64::INFINITY, f64::NEG_INFINITY];
    for (overlay_id, series) in panel.overlay.iter().enumerate() {
        let series_index = panel.series.len() + overlay_id;
        let show_flagged = shows_flagged(style.flagged_symbols, series_index);
        let mut groups = BTreeMap::<String, Vec<&Point>>::new();
        for point in series.points.iter().filter(|point| {
            point.x.is_finite() && point.y.is_finite() && (show_flagged || !point.flagged)
        }) {
            bounds = [bounds[0].min(point.y), bounds[1].max(point.y)];
            let group = style.color.map_or_else(
                || series.y.label().to_string(),
                |axis| {
                    format!(
                        "{} {}",
                        axis.label(),
                        axis.value(&point.coordinates, Default::default(), 0.0)
                    )
                },
            );
            groups.entry(group).or_default().push(point);
        }
        for (group, points) in groups {
            let mut curve = layer(
                payload,
                (index, series_index, "atmosphere", 0),
                false,
                group,
                points,
                style.connect,
            )?;
            curve.title = series.y.label().into();
            curve.provenance_summary = "Atmospheric model over the selected channels".into();
            if let Some(layer_style) = &mut curve.style {
                layer_style.color = style.color.is_none().then(|| {
                    crate::msexplore::plotms_figure::default_overlay_color(overlay_id).into()
                });
            }
            layers.push(curve);
        }
    }
    let mut overlay_axis = axis("atmosphere", first.y, None, bounds, [0.0; 2]);
    overlay_axis.draws_on_trailing_edge = true;
    axes.push(overlay_axis);
    Ok(())
}

fn ordinate_id(right: bool) -> &'static str {
    if right { "right" } else { "left" }
}

fn layer(
    payload: &mut PlotPayloadWriter,
    (panel, index, ordinate, epoch): (usize, usize, &str, u64),
    flagged: bool,
    label: String,
    points: Vec<&Point>,
    connect: bool,
) -> Result<PlotDocumentLayer, TaskFailure> {
    let x = payload
        .values(points.iter().map(|point| point.x))
        .map_err(write_failure)?;
    let y = payload
        .values(points.iter().map(|point| point.y))
        .map_err(write_failure)?;
    let kind = if connect {
        PlotLayerKind::Line
    } else {
        PlotLayerKind::Scatter
    };
    Ok(PlotDocumentLayer {
        id: format!("{index}-{panel}-{ordinate}-{flagged}-{epoch}-{label}"),
        title: label.clone(),
        kind,
        x_axis_id: "x".into(),
        y_axis_id: ordinate.into(),
        x_values: Vec::new(),
        y_values: Vec::new(),
        point_labels: Vec::new(),
        point_symbol_sizes: Vec::new(),
        interval_x_start: Vec::new(),
        interval_x_end: Vec::new(),
        interval_y: Vec::new(),
        interval_height: Vec::new(),
        interval_labels: Vec::new(),
        provenance: Vec::new(),
        color_group: if flagged { "flagged".into() } else { label },
        symbol_size: 2.0,
        line_width: 1.0,
        opacity: 1.0,
        source_sample_count: points.len() as u64,
        payload_strategy: PlotPayloadStrategy::BinaryArrays,
        provenance_summary: "Calibration table solutions".into(),
        data: Some(PlotLayerData::Points { x, y }),
        style: Some(PlotLayerStyle {
            color: flagged.then(|| "#ff0000".into()),
            marker: Some("o".into()),
            line_style: connect.then(|| "-".into()),
        }),
    })
}

struct AxisRanges<'a> {
    locations: Vec<&'a str>,
    flagged_symbols: Option<&'a ParameterValue>,
    global: [[f64; 2]; 3],
    local_x: bool,
    local_y: bool,
}

impl<'a> AxisRanges<'a> {
    fn new(task: &'a TaskInvocation<'_>, layout: &PlotPanels) -> Result<Self, TaskFailure> {
        let locations =
            super::axis_names(task.value("yaxislocation")).map_err(TaskFailure::validation)?;
        if locations
            .iter()
            .any(|location| !matches!(*location, "" | "left" | "right"))
        {
            return Err(TaskFailure::validation(
                "yaxislocation accepts left or right",
            ));
        }
        let flagged_symbols = task.value("customflaggedsymbol");
        Ok(Self {
            global: data_ranges(&layout.panels, &locations, flagged_symbols),
            locations,
            flagged_symbols,
            local_x: task.flag("xselfscale")? && !task.flag("xsharedaxis")?,
            local_y: task.flag("yselfscale")? && !task.flag("ysharedaxis")?,
        })
    }

    fn for_panel(&self, panel: &Panel) -> [[f64; 2]; 3] {
        let mut ranges = self.global;
        if self.local_x || self.local_y {
            let local = data_ranges(
                std::slice::from_ref(panel),
                &self.locations,
                self.flagged_symbols,
            );
            if self.local_x {
                ranges[0] = local[0];
            }
            if self.local_y {
                ranges[1..].copy_from_slice(&local[1..]);
            }
        }
        ranges
    }
}

fn right_side(locations: &[&str], index: usize) -> bool {
    locations
        .get(index)
        .or_else(|| (locations.len() == 1).then(|| &locations[0]))
        == Some(&"right")
}

pub(in crate::msexplore) fn shows_flagged(symbols: Option<&ParameterValue>, index: usize) -> bool {
    let enabled = match symbols {
        Some(ParameterValue::Array(values)) => values.get(index),
        other => other,
    };
    matches!(enabled, Some(ParameterValue::Bool(true)))
}

fn data_ranges(
    panels: &[Panel],
    locations: &[&str],
    flagged_symbols: Option<&ParameterValue>,
) -> [[f64; 2]; 3] {
    let mut ranges = [[f64::INFINITY, f64::NEG_INFINITY]; 3];
    let add = |range: &mut [f64; 2], value: f64| {
        if value.is_finite() {
            *range = [range[0].min(value), range[1].max(value)];
        }
    };
    for panel in panels {
        for (index, series) in panel.series.iter().enumerate() {
            let y_index = if right_side(locations, index) { 2 } else { 1 };
            let show_flags = shows_flagged(flagged_symbols, index);
            for point in &series.points {
                if show_flags || !point.flagged {
                    add(&mut ranges[0], point.x);
                    add(&mut ranges[y_index], point.y);
                }
            }
        }
    }
    ranges
}

fn axis(
    id: &str,
    axis: Axis,
    label: Option<&str>,
    [mut lower, mut upper]: [f64; 2],
    limits: [f64; 2],
) -> PlotDocumentAxis {
    if lower > upper {
        [lower, upper] = [0.0, 1.0];
    }
    let padding = if lower == upper {
        if axis == Axis::Time {
            0.5
        } else {
            lower.abs().mul_add(0.01, 0.5)
        }
    } else {
        0.05 * (upper - lower)
    };
    if limits != [0.0, 0.0] {
        [lower, upper] = limits;
    } else {
        lower -= padding;
        upper += padding;
    }
    let unit = axis.unit();
    let label = label.map_or_else(
        || {
            if unit.is_empty() {
                axis.label().into()
            } else {
                format!("{} ({unit})", axis.label())
            }
        },
        str::to_owned,
    );
    PlotDocumentAxis {
        id: id.into(),
        label,
        unit: unit.into(),
        lower,
        upper,
        scale: PlotAxisScale::Linear,
        lane_labels: Vec::new(),
        draws_on_trailing_edge: false,
    }
}

pub(in crate::msexplore) fn plot_limits(
    value: Option<&ParameterValue>,
) -> Result<[f64; 4], TaskFailure> {
    let Some(ParameterValue::Array(values)) = value else {
        return Ok([0.0; 4]);
    };
    if values.is_empty() {
        return Ok([0.0; 4]);
    }
    let limits = values
        .iter()
        .map(|value| match value {
            ParameterValue::Float(value) if value.is_finite() => Ok(*value),
            ParameterValue::Integer(value) => Ok(*value as f64),
            _ => Err(TaskFailure::validation(
                "plotrange requires finite axis bounds",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    limits
        .try_into()
        .map_err(|_| TaskFailure::validation("plotrange requires four axis bounds"))
}

fn write_failure(error: std::io::Error) -> TaskFailure {
    TaskFailure::execution(format!("write PlotMS arrays: {error}"))
}
