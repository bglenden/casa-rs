// SPDX-License-Identifier: LGPL-3.0-or-later
//! Build figures for MeasurementSet visibilities and atmospheric curves.

use std::collections::BTreeMap;
use std::path::Path;

use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::calibration::figure::plot_limits;
use super::plotms_atmosphere::Curve;
use super::{
    MsAxis, MsDataColumn, MsIterationAxis, MsPlotPayload, MsScatterGridPayload,
    MsScatterPanelPayload, MsScatterSeries, plotms_text,
};
use crate::plot_document::{
    PlotAxisScale, PlotDocument, PlotDocumentAxis, PlotDocumentLayer, PlotFigure, PlotLayerData,
    PlotLayerKind, PlotLayerStyle, PlotLayout, PlotPayloadStrategy, PlotPayloadWriter, label_unit,
    plotms_visibility_document, series_y_bounds,
};

pub(super) struct VisibilityAxisColumns<'a> {
    pub(super) base: [MsDataColumn; 3],
    pub(super) secondary_axis: Option<MsAxis>,
    pub(super) additional: &'a [(MsAxis, MsDataColumn)],
    pub(super) right_ordinates: &'a [bool],
}

pub(super) fn prepare(
    task: &TaskInvocation<'_>,
    payload: &MsPlotPayload,
    arrays: &Path,
    output: Option<&Path>,
    show: bool,
    overlays: &[Vec<Curve>],
    columns: Option<VisibilityAxisColumns<'_>>,
) -> Result<Vec<PlotFigure>, TaskFailure> {
    let (x, y, second, grid) = match payload {
        MsPlotPayload::Scatter(plot) => (plot.x_axis, plot.y_axis, plot.secondary_y_axis, None),
        MsPlotPayload::ScatterGrid(grid) => (
            grid.x_axis,
            grid.y_axis,
            columns.as_ref().and_then(|columns| columns.secondary_axis),
            Some(grid),
        ),
        _ => {
            return Err(TaskFailure::validation(
                "PlotMS figures require visibility scatter data",
            ));
        }
    };
    let mut document = plotms_visibility_document(payload);
    if let Some(columns) = columns {
        label_and_group_ordinates(task, &mut document, payload, [x, y], second, columns)?;
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

fn label_and_group_ordinates(
    task: &TaskInvocation<'_>,
    document: &mut PlotDocument,
    payload: &MsPlotPayload,
    [x, y]: [MsAxis; 2],
    second: Option<MsAxis>,
    columns: VisibilityAxisColumns<'_>,
) -> Result<(), TaskFailure> {
    label_visibility_columns(task, document, [x, y], second, columns.base)?;
    let Some(second) = second else {
        return Ok(());
    };
    if columns.additional.is_empty() && !matches!(payload, MsPlotPayload::ScatterGrid(_)) {
        return Ok(());
    }
    let choices = [(y, columns.base[1]), (second, columns.base[2])]
        .into_iter()
        .chain(columns.additional.iter().copied())
        .collect::<Vec<_>>();
    if !task.text("title")?.is_some_and(|title| !title.is_empty()) {
        let ordinates = choices
            .iter()
            .map(|&(axis, column)| column_axis_name(axis, column))
            .collect::<Vec<_>>()
            .join(", ");
        let x_name = column_axis_name(x, columns.base[0]);
        document.title = format!("{ordinates} vs. {x_name}");
    }
    let supplied_label = task.text("ylabel")?.filter(|label| !label.is_empty());
    let group = |axes: &mut Vec<PlotDocumentAxis>,
                 layers: &mut [PlotDocumentLayer],
                 series: &[MsScatterSeries]| {
        group_ordinate_sides(
            axes,
            layers,
            series,
            &choices,
            columns.right_ordinates,
            supplied_label.as_deref(),
        );
    };
    match payload {
        MsPlotPayload::Scatter(plot) => {
            group(&mut document.axes, &mut document.layers, &plot.series)
        }
        MsPlotPayload::ScatterGrid(grid) => {
            let title = std::mem::take(&mut document.title);
            let font = task.integer("titlefont")?;
            let iteration = iteration_name(grid.iteraxis);
            for (panel, plot) in document.panels.iter_mut().zip(&grid.panels) {
                group(&mut panel.axes, &mut panel.layers, &plot.series);
                let label = iteration_value(grid.iteraxis, plot);
                panel.title = format!("{title} {iteration}: {label}");
                panel.title_font_size = (font > 0).then(|| font.to_string());
            }
            if task.flag("xsharedaxis")? {
                unite_panel_bounds(document, "x");
            }
            if task.flag("ysharedaxis")? {
                unite_panel_bounds(document, "y");
                unite_panel_bounds(document, "y-secondary");
            }
        }
        _ => {}
    }
    Ok(())
}

fn unite_panel_bounds(document: &mut PlotDocument, axis_id: &str) {
    let bounds = document
        .panels
        .iter()
        .flat_map(|panel| &panel.axes)
        .filter(|axis| axis.id == axis_id)
        .fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(lower, upper), axis| (lower.min(axis.lower), upper.max(axis.upper)),
        );
    for axis in document
        .panels
        .iter_mut()
        .flat_map(|panel| &mut panel.axes)
        .filter(|axis| axis.id == axis_id)
    {
        (axis.lower, axis.upper) = bounds;
    }
}

fn group_ordinate_sides(
    axes: &mut Vec<PlotDocumentAxis>,
    layers: &mut [PlotDocumentLayer],
    series: &[MsScatterSeries],
    choices: &[(MsAxis, MsDataColumn)],
    right_ordinates: &[bool],
    supplied_label: Option<&str>,
) {
    let right = |index| right_ordinates.get(index).copied().unwrap_or(false);
    let axis_id = |right| if right { "y-secondary" } else { "y" };
    axes.truncate(1);
    for (series, layer) in series.iter().zip(layers) {
        layer.y_axis_id = axis_id(right(series.y_index)).into();
    }
    for side in [false, true] {
        let selected = choices
            .iter()
            .copied()
            .enumerate()
            .filter(|(index, _)| right(*index) == side)
            .map(|(_, choice)| choice)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            continue;
        }
        let mut labels = selected
            .iter()
            .map(|&(axis, column)| {
                if !axis.is_visibility_math() {
                    return super::axis_label(axis);
                }
                let mut label = column_axis_name(axis, column);
                if axis == MsAxis::Phase {
                    label.push_str(" (degrees)");
                }
                label
            })
            .collect::<Vec<_>>();
        labels.dedup();
        let unit_for = |axis| {
            if axis == MsAxis::Phase {
                "degrees".into()
            } else {
                label_unit(&super::axis_label(axis))
            }
        };
        let unit = unit_for(selected[0].0);
        let unit = if selected.iter().all(|&(axis, _)| unit_for(axis) == unit) {
            unit
        } else {
            String::new()
        };
        let (lower, upper) = if selected
            .iter()
            .all(|&(axis, column)| axis == MsAxis::Phase && !column.has_unbounded_phase())
        {
            (-180.0, 180.0)
        } else {
            series_y_bounds(
                series
                    .iter()
                    .filter(|series| right(series.y_index) == side)
                    .flat_map(|series| series.points.iter().map(|&(_, y)| y)),
            )
        };
        axes.push(PlotDocumentAxis {
            id: axis_id(side).into(),
            label: supplied_label.map_or_else(|| labels.join(", "), str::to_owned),
            unit,
            lower,
            upper,
            scale: PlotAxisScale::Linear,
            lane_labels: Vec::new(),
            draws_on_trailing_edge: side,
        });
    }
}

fn label_visibility_columns(
    task: &TaskInvocation<'_>,
    document: &mut PlotDocument,
    [x, y]: [MsAxis; 2],
    second: Option<MsAxis>,
    [x_column, y_column, secondary_y_column]: [MsDataColumn; 3],
) -> Result<(), TaskFailure> {
    let has_label =
        |name| Ok::<_, TaskFailure>(task.text(name)?.is_some_and(|text| !text.is_empty()));
    let x_name = column_axis_name(x, x_column);
    let mut y_name = column_axis_name(y, y_column);
    if let Some(second) = second {
        y_name.push_str(", ");
        y_name.push_str(&column_axis_name(second, secondary_y_column));
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
                Some(second) => (second, secondary_y_column),
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

pub(super) fn column_axis_name(axis: MsAxis, column: MsDataColumn) -> String {
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
            layer.id = if layer.color_group == "flagged" {
                format!("0-flagged-visibility-{panel}-{index}")
            } else {
                format!("{index}-visibility-{panel}")
            };
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
    let position = requested_position(task)?;
    let layout = PlotLayout {
        rows,
        columns,
        first_panel: None,
        aspect: None,
        margins: None,
        font_size: None,
    };
    let capacity = (rows as usize).saturating_mul(columns as usize);
    let offset = position[0] as usize * columns as usize + position[1] as usize;
    let panels = std::mem::take(&mut document.panels);
    let first_end = panels.len().min(capacity - offset);
    let (first, remaining) = panels.split_at(first_end);
    let pages = std::iter::once(first)
        .chain(remaining.chunks(capacity))
        .filter(|page| !page.is_empty());
    let all = task.text("exprange")?.as_deref() == Some("all");
    let mut figures = Vec::new();
    let mut start = 0;
    for (page, page_panels) in pages.enumerate() {
        if !all && page > 0 {
            break;
        }
        // CASA advances through the existing pages before resizing its canvas grid.
        // When the iteration count initially fits on one page, its extra export
        // still uses the first page's cell and has no iteration labels in the filename.
        let reuse_first_canvas = offset != 0 && page > 0 && panels.len() <= capacity;
        let first_panel = if page == 0 || reuse_first_canvas {
            position
        } else {
            [0, 0]
        };
        let mut document = document.clone();
        document.layout = Some(PlotLayout {
            first_panel: (offset != 0).then_some(first_panel),
            ..layout.clone()
        });
        document.panels.extend_from_slice(page_panels);
        let end = start + page_panels.len();
        let labels = grid.panels[start..end]
            .iter()
            .map(|panel| iteration_value(grid.iteraxis, panel));
        if offset != 0 {
            for (panel, scan) in document.panels.iter_mut().zip(labels.clone()) {
                panel.title = format!("{} Scan: {scan}", document.title);
            }
        }
        let axis = if reuse_first_canvas {
            ""
        } else {
            iteration_name(grid.iteraxis)
        };
        let output_file = output.map(|path| iteration_filename(path, axis, page, labels));
        figures.push(PlotFigure {
            output_file,
            show: show && page == 0,
            document,
        });
        start = end;
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

fn iteration_value(axis: MsIterationAxis, panel: &MsScatterPanelPayload) -> &str {
    match axis {
        MsIterationAxis::Scan => panel.label.strip_prefix("Scan ").unwrap_or(&panel.label),
        MsIterationAxis::SpectralWindow => panel.key.strip_prefix("spw-").unwrap_or(&panel.label),
        _ => &panel.label,
    }
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

pub(super) fn static_grid(
    task: &TaskInvocation<'_>,
    iterated: bool,
    offset_scan_grid: bool,
) -> Result<Option<PlotLayout>, TaskFailure> {
    let [rows, columns] = requested_grid(task)?;
    let [row, column] = requested_position(task)?;
    if row >= rows || column >= columns {
        return Err(TaskFailure::validation("row/col index out of range"));
    }
    if iterated && [row, column] != [0, 0] && !offset_scan_grid {
        return Err(TaskFailure::validation(
            "nondefault grid placement with PlotMS iteration is pending #1032",
        ));
    }
    Ok(
        (!iterated && (rows > 1 || columns > 1)).then_some(PlotLayout {
            rows,
            columns,
            first_panel: Some([row, column]),
            aspect: None,
            margins: None,
            font_size: None,
        }),
    )
}

pub(super) fn requested_position(task: &TaskInvocation<'_>) -> Result<[u32; 2], TaskFailure> {
    let index = |name| {
        u32::try_from(task.integer(name)?)
            .map_err(|_| TaskFailure::validation("row/col index out of range").at(name))
    };
    Ok([index("rowindex")?, index("colindex")?])
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
    if !axis.is_empty() {
        name.push(format!("_{axis}{labels}"));
    }
    name.push(suffix);
    if let Some(extension) = destination.extension() {
        name.push(".");
        name.push(extension);
    }
    destination.with_file_name(name)
}
