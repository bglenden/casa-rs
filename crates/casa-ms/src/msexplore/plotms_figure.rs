// SPDX-License-Identifier: LGPL-3.0-or-later
//! Build figures for MeasurementSet visibilities and atmospheric curves.

use std::collections::BTreeMap;
use std::path::Path;

use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::calibration::figure::plot_limits;
use super::plotms_atmosphere::Curve;
use super::{MsAxis, MsDataColumn, MsPlotPayload, MsScatterGridPayload, plotms_text};
use crate::plot_document::{
    PlotAxisScale, PlotDocument, PlotDocumentAxis, PlotDocumentLayer, PlotFigure, PlotLayerData,
    PlotLayerKind, PlotLayerStyle, PlotLayout, PlotPayloadStrategy, PlotPayloadWriter,
    plotms_visibility_document,
};

pub(super) fn prepare(
    task: &TaskInvocation<'_>,
    payload: &MsPlotPayload,
    arrays: &Path,
    output: Option<&Path>,
    show: bool,
    overlays: &[Vec<Curve>],
    columns: Option<[MsDataColumn; 2]>,
) -> Result<Vec<PlotFigure>, TaskFailure> {
    let (x, y, second, grid) = match payload {
        MsPlotPayload::Scatter(plot) => (plot.x_axis, plot.y_axis, plot.secondary_y_axis, None),
        MsPlotPayload::ScatterGrid(grid) => (grid.x_axis, grid.y_axis, None, Some(grid)),
        _ => {
            return Err(TaskFailure::validation(
                "PlotMS figures require visibility scatter data",
            ));
        }
    };
    let mut document = plotms_visibility_document(payload);
    if let Some(columns) = columns {
        label_visibility_columns(task, &mut document, [x, y], second, columns)?;
    }
    document.show_legend = task.flag("showlegend")?;
    let limits = plot_limits(task.value("plotrange"))?;
    let style = PanelStyle {
        frequency: [
            x == MsAxis::Frequency,
            y == MsAxis::Frequency,
            second == Some(MsAxis::Frequency),
        ],
        limits,
        connect: task.flag("timeconnector")?
            || task
                .text("xconnector")?
                .as_deref()
                .is_some_and(|value| matches!(value, "line" | "step")),
        colorize: task.flag("colorizeoverlay")?
            && task
                .text("coloraxis")?
                .is_some_and(|value| !value.is_empty() && !value.eq_ignore_ascii_case("none")),
    };
    let mut writer = PlotPayloadWriter::open(arrays).map_err(write_failure)?;
    let figures = if let Some(grid) = grid {
        for (index, panel) in document.panels.iter_mut().enumerate() {
            style.prepare(
                index,
                &mut writer,
                &mut panel.axes,
                &mut panel.layers,
                overlays.get(index).map(Vec::as_slice),
            )?;
        }
        paginate(task, document, grid, output, show)?
    } else {
        style.prepare(
            0,
            &mut writer,
            &mut document.axes,
            &mut document.layers,
            overlays.first().map(Vec::as_slice),
        )?;
        vec![PlotFigure {
            output_file: output.map(Path::to_owned),
            show,
            document,
        }]
    };
    let bytes = writer.finish().map_err(write_failure)?;
    for figure in &figures {
        figure
            .document
            .validate_payload(bytes)
            .map_err(|error| TaskFailure::execution(error.to_string()))?;
    }
    Ok(figures)
}

fn label_visibility_columns(
    task: &TaskInvocation<'_>,
    document: &mut PlotDocument,
    [x, y]: [MsAxis; 2],
    second: Option<MsAxis>,
    [x_column, y_column]: [MsDataColumn; 2],
) -> Result<(), TaskFailure> {
    let has_label =
        |name| Ok::<_, TaskFailure>(task.text(name)?.is_some_and(|text| !text.is_empty()));
    let x_name = column_axis_name(x, x_column);
    let mut y_name = column_axis_name(y, y_column);
    if let Some(second) = second {
        y_name.push_str(", ");
        y_name.push_str(&column_axis_name(second, y_column));
    }
    if !has_label("title")? {
        document.title = format!("{y_name} vs. {x_name}");
    }
    let explicit_x = has_label("xlabel")?;
    let explicit_y = has_label("ylabel")?;
    let axes = std::iter::once(&mut document.axes)
        .chain(document.panels.iter_mut().map(|panel| &mut panel.axes));
    for axis in axes.flatten() {
        let (kind, column) = match axis.id.as_str() {
            "x" if !explicit_x => (x, x_column),
            "y" if !explicit_y => (y, y_column),
            "y-secondary" => match second {
                Some(second) => (second, y_column),
                None => continue,
            },
            _ => continue,
        };
        if kind.is_visibility_math() {
            axis.label = column_axis_name(kind, column);
            if kind == MsAxis::Phase {
                axis.label.push_str(" (degrees)");
            }
        }
    }
    Ok(())
}

fn column_axis_name(axis: MsAxis, column: MsDataColumn) -> String {
    let name = plotms_text::name(axis);
    if axis.is_visibility_math() && column != MsDataColumn::Data {
        let label = match column {
            MsDataColumn::DataMinusModel => "data-model (vector)",
            MsDataColumn::CorrectedMinusModel => "corrected-model (vector)",
            MsDataColumn::DataMinusModelScalar => "data-model (scalar)",
            MsDataColumn::CorrectedMinusModelScalar => "corrected-model (scalar)",
            MsDataColumn::DataDivModel => "data/model (vector)",
            MsDataColumn::CorrectedDivModel => "corrected/model (vector)",
            MsDataColumn::DataDivModelScalar => "data/model (scalar)",
            MsDataColumn::CorrectedDivModelScalar => "corrected/model (scalar)",
            other => other.as_str(),
        };
        format!("{name}:{label}")
    } else {
        name.into()
    }
}

struct PanelStyle {
    frequency: [bool; 3],
    limits: [f64; 4],
    connect: bool,
    colorize: bool,
}

impl PanelStyle {
    fn frequency_axis(&self, id: &str) -> bool {
        match id {
            "x" => self.frequency[0],
            "y" => self.frequency[1],
            "y-secondary" => self.frequency[2],
            _ => false,
        }
    }

    fn prepare(
        &self,
        panel: usize,
        writer: &mut PlotPayloadWriter,
        axes: &mut Vec<PlotDocumentAxis>,
        layers: &mut Vec<PlotDocumentLayer>,
        overlays: Option<&[Curve]>,
    ) -> Result<(), TaskFailure> {
        for axis in axes.iter_mut() {
            if self.frequency_axis(&axis.id) {
                axis.lower /= 1e9;
                axis.upper /= 1e9;
                axis.unit = "GHz".into();
                if axis.label == "Frequency (Hz)" {
                    axis.label = "Frequency (GHz)".into();
                }
            }
            let offset = if axis.id == "x" { 0 } else { 2 };
            if self.limits[offset] != 0.0 || self.limits[offset + 1] != 0.0 {
                axis.lower = self.limits[offset];
                axis.upper = self.limits[offset + 1];
            }
        }
        for (index, layer) in layers.iter_mut().enumerate() {
            let (x_scale, y_scale) = (
                if self.frequency_axis(&layer.x_axis_id) {
                    1e9
                } else {
                    1.0
                },
                if self.frequency_axis(&layer.y_axis_id) {
                    1e9
                } else {
                    1.0
                },
            );
            let x = writer
                .values(
                    std::mem::take(&mut layer.x_values)
                        .into_iter()
                        .map(|v| v / x_scale),
                )
                .map_err(write_failure)?;
            let y = writer
                .values(
                    std::mem::take(&mut layer.y_values)
                        .into_iter()
                        .map(|v| v / y_scale),
                )
                .map_err(write_failure)?;
            layer.id = format!("{index}-visibility-{panel}");
            layer.data = Some(PlotLayerData::Points { x, y });
            layer.payload_strategy = PlotPayloadStrategy::BinaryArrays;
            layer.style = Some(PlotLayerStyle {
                color: None,
                marker: Some("o".into()),
                line_style: self.connect.then(|| "-".into()),
            });
        }
        if let Some(curves) = overlays {
            self.append_curves(panel, writer, axes, layers, curves)?;
        }
        Ok(())
    }

    fn append_curves(
        &self,
        panel: usize,
        writer: &mut PlotPayloadWriter,
        axes: &mut Vec<PlotDocumentAxis>,
        layers: &mut Vec<PlotDocumentLayer>,
        curves: &[Curve],
    ) -> Result<(), TaskFailure> {
        let Some(first) = curves.first() else {
            return Ok(());
        };
        let mut bounds = [f64::INFINITY, f64::NEG_INFINITY];
        for (index, curve) in curves.iter().enumerate() {
            let mut groups = BTreeMap::<&str, Vec<_>>::new();
            for sample in curve
                .samples
                .iter()
                .filter(|sample| sample.y.is_finite() && sample.x.is_finite())
            {
                bounds = [bounds[0].min(sample.y), bounds[1].max(sample.y)];
                let group = if self.colorize {
                    sample.color_group.as_str()
                } else {
                    ""
                };
                groups.entry(group).or_default().push(sample);
            }
            for (group_index, (group, points)) in groups.into_iter().enumerate() {
                let xscale = if self.frequency[0] { 1e9 } else { 1.0 };
                let x = writer
                    .values(points.iter().map(|sample| sample.x / xscale))
                    .map_err(write_failure)?;
                let y = writer
                    .values(points.iter().map(|sample| sample.y))
                    .map_err(write_failure)?;
                layers.push(PlotDocumentLayer {
                    id: format!("atmosphere-{panel}-{index}-{group_index}"),
                    title: curve.axis.label().into(),
                    kind: if self.connect {
                        PlotLayerKind::Line
                    } else {
                        PlotLayerKind::Scatter
                    },
                    x_axis_id: "x".into(),
                    y_axis_id: "atmosphere".into(),
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
                    color_group: group.into(),
                    symbol_size: 2.0,
                    line_width: 1.0,
                    opacity: 1.0,
                    source_sample_count: points.len() as u64,
                    payload_strategy: PlotPayloadStrategy::BinaryArrays,
                    provenance_summary: "Atmospheric model over the selected channels".into(),
                    data: Some(PlotLayerData::Points { x, y }),
                    style: Some(PlotLayerStyle {
                        color: (!self.colorize).then(|| default_overlay_color(index).into()),
                        marker: Some("o".into()),
                        line_style: self.connect.then(|| "-".into()),
                    }),
                });
            }
        }
        if bounds[0].is_finite() {
            if bounds[0] == bounds[1] {
                bounds = [bounds[0] - 0.5, bounds[1] + 0.5];
            }
            axes.push(PlotDocumentAxis {
                id: "atmosphere".into(),
                label: format!("{} ({})", first.axis.label(), first.axis.unit()),
                unit: first.axis.unit().into(),
                lower: bounds[0],
                upper: bounds[1],
                scale: PlotAxisScale::Linear,
                lane_labels: Vec::new(),
                draws_on_trailing_edge: true,
            });
        }
        Ok(())
    }
}

fn write_failure(error: std::io::Error) -> TaskFailure {
    TaskFailure::execution(format!("write PlotMS arrays: {error}"))
}

fn paginate(
    task: &TaskInvocation<'_>,
    mut document: PlotDocument,
    grid: &MsScatterGridPayload,
    output: Option<&Path>,
    show: bool,
) -> Result<Vec<PlotFigure>, TaskFailure> {
    let [rows, columns] = requested_grid(task)?;
    document.layout = Some(PlotLayout {
        rows,
        columns,
        aspect: None,
        margins: None,
        font_size: None,
    });
    let capacity = (rows as usize).saturating_mul(columns as usize);
    let panels = std::mem::take(&mut document.panels);
    let pages = if task.text("exprange")?.as_deref() == Some("all") {
        panels.len().div_ceil(capacity)
    } else {
        1
    };
    let mut figures = Vec::new();
    for (page, page_panels) in panels.chunks(capacity).take(pages).enumerate() {
        let mut document = document.clone();
        document.panels.extend_from_slice(page_panels);
        let labels = grid.panels[page * capacity..page * capacity + page_panels.len()]
            .iter()
            .map(|panel| match grid.iteraxis {
                super::MsIterationAxis::Scan => {
                    panel.label.strip_prefix("Scan ").unwrap_or(&panel.label)
                }
                super::MsIterationAxis::SpectralWindow => {
                    panel.key.strip_prefix("spw-").unwrap_or(&panel.label)
                }
                _ => &panel.label,
            });
        let output_file = output
            .map(|path| iteration_filename(path, iteration_name(grid.iteraxis), page, labels));
        figures.push(PlotFigure {
            output_file,
            show: show && page == 0,
            document,
        });
    }
    let paths = figures
        .iter()
        .map(|figure| ("plotfile", figure.output_file.as_deref()))
        .collect::<Vec<_>>();
    super::task_contract::refuse_unwritable_outputs(
        &paths,
        task.flag("overwrite")?,
        "set overwrite=True to replace it",
    )
    .map_err(TaskFailure::validation)?;
    Ok(figures)
}

pub(in crate::msexplore) fn default_overlay_color(index: usize) -> &'static str {
    if index == 0 { "#d88400" } else { "#b32673" }
}

pub(super) fn iteration_name(axis: super::MsIterationAxis) -> &'static str {
    match axis {
        super::MsIterationAxis::SpectralWindow => "Spw",
        super::MsIterationAxis::Correlation => "Corr",
        _ => axis.display_name(),
    }
}

pub(super) fn requested_grid(task: &TaskInvocation<'_>) -> Result<[u32; 2], TaskFailure> {
    let count = |name| {
        u32::try_from(task.integer(name)?.max(1))
            .map_err(|_| TaskFailure::validation(format!("{name} is too large")).at(name))
    };
    Ok([count("gridrows")?, count("gridcols")?])
}

pub(in crate::msexplore) fn iteration_filename<'a>(
    destination: &Path,
    axis: &str,
    page: usize,
    labels: impl Iterator<Item = &'a str>,
) -> std::path::PathBuf {
    let labels = labels
        .map(|label| label.replace(" & ", "_with_").replace('/', "-"))
        .collect::<Vec<_>>()
        .join(",");
    let suffix = if page == 0 {
        String::new()
    } else {
        format!("_{}", page + 1)
    };
    let mut name = destination.file_stem().unwrap_or_default().to_os_string();
    name.push(format!("_{axis}{labels}{suffix}"));
    if let Some(extension) = destination.extension() {
        name.push(".");
        name.push(extension);
    }
    destination.with_file_name(name)
}
