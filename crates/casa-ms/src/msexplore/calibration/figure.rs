// SPDX-License-Identifier: LGPL-3.0-or-later
//! Make calibration layers for the shared PlotDocument renderer.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::PlotSeries;
use super::axis::Axis;
use super::samples::Point;
use crate::plot_document::{
    PlotAxisScale, PlotDocument, PlotDocumentAxis, PlotDocumentLayer, PlotFigure, PlotLayerData,
    PlotLayerKind, PlotLayerStyle, PlotPayloadStrategy, PlotPayloadWriter,
};

pub(super) fn prepare(
    task: &TaskInvocation<'_>,
    payload: &Path,
    destination: Option<PathBuf>,
    show: bool,
    series: &[PlotSeries],
) -> Result<PlotFigure, TaskFailure> {
    let mut payload = PlotPayloadWriter::open(payload).map_err(write_failure)?;
    let title = task.text("title")?.unwrap_or_default();
    let xlabel = task.text("xlabel")?.filter(|label| !label.is_empty());
    let ylabel = task.text("ylabel")?.filter(|label| !label.is_empty());
    let manual = plot_limits(task.value("plotrange"))?;
    let color = task
        .text("coloraxis")?
        .filter(|value| !value.is_empty())
        .map(|value| Axis::parse(&value))
        .transpose()
        .map_err(TaskFailure::validation)?;
    let ordinate_locations =
        super::axis_names(task.value("yaxislocation")).map_err(TaskFailure::validation)?;
    if ordinate_locations
        .iter()
        .any(|location| !matches!(*location, "" | "left" | "right"))
    {
        return Err(TaskFailure::validation(
            "yaxislocation accepts left or right",
        ));
    }
    let on_right = |index| {
        ordinate_locations
            .get(index)
            .or_else(|| (ordinate_locations.len() == 1).then(|| &ordinate_locations[0]))
            == Some(&"right")
    };
    let x = &series[0].x;
    let mut axes = vec![axis(
        "x",
        *x,
        xlabel.as_deref(),
        series
            .iter()
            .flat_map(|line| line.points.iter().map(|point| point.x)),
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
            selected
                .iter()
                .flat_map(|line| line.points.iter().map(|point| point.y)),
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
    let show_flagged = matches!(
        task.value("customflaggedsymbol"),
        Some(ParameterValue::Bool(true))
    );
    let connect = task
        .text("xconnector")?
        .is_some_and(|name| name == "line" || name == "step");
    let connect_times = task.flag("timeconnector")?;
    let mut layers = Vec::new();
    for (id, series) in series.iter().enumerate() {
        let yid = ordinate_id(on_right(id));
        let mut groups = BTreeMap::<(bool, String, u64), Vec<&Point>>::new();
        for point in series.points.iter().filter(|point| {
            point.x.is_finite() && point.y.is_finite() && (!point.flagged || show_flagged)
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
                &mut payload,
                (id, yid, epoch),
                flagged,
                label,
                points,
                connect,
            )?);
        }
    }
    let document = PlotDocument {
        id: "plotms-calibration".into(),
        title,
        subtitle: String::new(),
        header_lines: Vec::new(),
        axes,
        layers,
        annotations: Vec::new(),
        panels: Vec::new(),
        show_legend: task.flag("showlegend")?,
        layout: None,
    };
    let bytes = payload.finish().map_err(write_failure)?;
    document
        .validate_payload(bytes)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    Ok(PlotFigure {
        output_file: destination,
        show,
        document,
    })
}

fn ordinate_id(right: bool) -> &'static str {
    if right { "right" } else { "left" }
}

fn layer(
    payload: &mut PlotPayloadWriter,
    (index, ordinate, epoch): (usize, &str, u64),
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
        id: format!("{index}-{ordinate}-{flagged}-{epoch}-{label}"),
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

fn axis(
    id: &str,
    axis: Axis,
    label: Option<&str>,
    values: impl Iterator<Item = f64>,
    limits: [f64; 2],
) -> PlotDocumentAxis {
    let [mut lower, mut upper] = values
        .filter(|value| value.is_finite())
        .fold([f64::INFINITY, f64::NEG_INFINITY], |[lo, hi], value| {
            [lo.min(value), hi.max(value)]
        });
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
    if limits[0] != limits[1] {
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

fn plot_limits(value: Option<&ParameterValue>) -> Result<[f64; 4], TaskFailure> {
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
