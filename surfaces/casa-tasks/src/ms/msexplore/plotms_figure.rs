// SPDX-License-Identifier: LGPL-3.0-or-later
//! Build figures for MeasurementSet visibilities and atmospheric curves.

use std::collections::BTreeMap;
use std::path::Path;

use crate::{TaskFailure, TaskInvocation};
use casa_types::measures::frequency::FrequencyRef;

use super::calibration::figure::plot_limits;
use super::plotms_atmosphere::Curve;
use super::plotms_pointing::PointingAxes;
use casa_ms::columns::frequency_columns::ChanFreqColumn;
use casa_ms::subtables::SubTable;
use casa_plot_data::msexplore::{
    MsAxis, MsColorAxis, MsDataColumn, MsIterationAxis, MsPlotPayload, MsPlotSpec,
    MsScatterGridPayload, MsScatterPanelPayload, MsScatterSeries, axis_label, column_axis_name,
    plotms_averaged_colors, plotms_row_axes, reference_midnight, selected_i32_values,
};
use casa_plot_data::plot_document::{
    PlotAxisScale, PlotDocument, PlotDocumentAxis, PlotDocumentLayer, PlotFigure, PlotLayerData,
    PlotLayerKind, PlotLayerStyle, PlotLayout, PlotPayloadStrategy, PlotPayloadWriter, label_unit,
    plotms_visibility_document, series_y_bounds,
};

pub(super) struct VisibilityAxisColumns<'a> {
    pub(super) base: [MsDataColumn; 3],
    pub(super) secondary_axis: Option<MsAxis>,
    pub(super) additional: &'a [(MsAxis, MsDataColumn)],
    pub(super) right_ordinates: &'a [bool],
    pub(super) pointing: Option<&'a PointingAxes>,
}

pub(super) struct AxisPresentation<'a> {
    pub(super) columns: Option<VisibilityAxisColumns<'a>>,
    pub(super) spec: &'a MsPlotSpec,
    pub(super) clock_reference: Option<i64>,
    pub(super) frequency_frame: Option<FrequencyRef>,
}

pub(super) fn prepare(
    task: &TaskInvocation<'_>,
    payload: &MsPlotPayload,
    arrays: &Path,
    output: Option<&Path>,
    show: bool,
    overlays: &[Vec<Curve>],
    presentation: AxisPresentation<'_>,
) -> Result<Vec<PlotFigure>, TaskFailure> {
    let AxisPresentation {
        columns,
        spec,
        clock_reference,
        frequency_frame,
    } = presentation;
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
    if let Some(reference) = clock_reference {
        prepare_clock_axes(&mut document, spec, reference);
    }
    let limits = plot_limits(task.value("plotrange"))?;
    if let Some(grid) = grid {
        label_iteration_panels(task, &mut document, grid)?;
        let auto_visibility = |axis: MsAxis| axis.is_visibility_math() && axis != MsAxis::Phase;
        if task.flag("xselfscale")? && limits[..2] == [0.0, 0.0] {
            unite_panel_bounds(&mut document, "x", auto_visibility(spec.x_axis));
        }
        if task.flag("yselfscale")? && limits[2..] == [0.0, 0.0] {
            let autoscale = spec.y_axes.iter().copied().all(auto_visibility);
            unite_panel_bounds(&mut document, "y", autoscale);
            unite_panel_bounds(&mut document, "y-secondary", autoscale);
        }
    }
    document.show_legend = task.flag("showlegend")?;
    let style = PanelStyle {
        frequency: [
            x == MsAxis::Frequency,
            y == MsAxis::Frequency,
            second == Some(MsAxis::Frequency),
        ],
        limits,
        x_axis: x,
        frequency_frame,
        label_frame: [
            !task.text("xlabel")?.is_some_and(|s| !s.is_empty()),
            !task.text("ylabel")?.is_some_and(|s| !s.is_empty()),
        ],
        averaged_antenna_color: plotms_averaged_colors::axis(spec),
        colorize: task.flag("colorizeoverlay")?
            && task
                .text("coloraxis")?
                .is_some_and(|value| !value.is_empty() && !value.eq_ignore_ascii_case("none")),
    };
    let mut writer = PlotPayloadWriter::open(arrays).map_err(write_failure)?;
    let figures = if let Some(grid) = grid {
        for (index, (panel, source)) in document.panels.iter_mut().zip(&grid.panels).enumerate() {
            style.prepare(
                index,
                &mut writer,
                &mut panel.axes,
                &mut panel.layers,
                &source.series,
                overlays.get(index).map(Vec::as_slice),
            )?;
        }
        paginate(task, document, grid, output, show)?
    } else {
        let MsPlotPayload::Scatter(plot) = payload else {
            unreachable!("visibility figures use scatter payloads");
        };
        style.prepare(
            0,
            &mut writer,
            &mut document.axes,
            &mut document.layers,
            &plot.series,
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

pub(super) fn reporting_frame(
    ms: &casa_ms::MeasurementSet,
    spec: &MsPlotSpec,
    payload: &MsPlotPayload,
) -> Option<FrequencyRef> {
    if !std::iter::once(spec.x_axis)
        .chain(spec.y_axes.iter().copied())
        .any(|axis| matches!(axis, MsAxis::Frequency | MsAxis::Velocity))
    {
        return None;
    }
    if let Some(frame) = spec.transforms.freqframe.as_deref() {
        return frame.parse().ok();
    }
    let windows = ms.spectral_window().ok()?;
    let frequencies = ChanFreqColumn::new(windows.table());
    let mut frames = (0..windows.row_count()).map(|row| frequencies.frequency_reference(row).ok());
    if let Some(Some(frame)) = frames.next()
        && frames.all(|other| other == Some(frame))
    {
        return Some(frame);
    }
    let series = match payload {
        MsPlotPayload::Scatter(plot) => vec![plot.series.as_slice()],
        MsPlotPayload::ScatterGrid(grid) => {
            grid.panels.iter().map(|p| p.series.as_slice()).collect()
        }
        _ => return None,
    };
    let rows = series
        .into_iter()
        .flatten()
        .flat_map(|series| series.provenance.iter().map(|point| point.row))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let descriptions = ms.data_description().ok()?;
    let ids = selected_i32_values(ms.main_table(), "DATA_DESC_ID", &rows).ok()?;
    let mut unique = ids
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|id| {
            let spw = descriptions.spectral_window_id(id.try_into().ok()?).ok()?;
            frequencies.frequency_reference(spw.try_into().ok()?).ok()
        });
    let first = unique.next()??;
    unique.all(|frame| frame == Some(first)).then_some(first)
}

fn prepare_clock_axes(document: &mut PlotDocument, spec: &MsPlotSpec, reference: i64) {
    let time_axis = if spec.x_axis == MsAxis::Time {
        "x"
    } else {
        "y"
    };
    let reference = if spec.averaging.avgtime.is_some() {
        let times = axis_samples(&document.layers, time_axis).chain(
            document
                .panels
                .iter()
                .flat_map(|panel| axis_samples(&panel.layers, time_axis)),
        );
        reference_midnight(times).unwrap_or(reference)
    } else {
        reference
    };
    let axes = std::iter::once((&mut document.axes, &document.layers)).chain(
        document
            .panels
            .iter_mut()
            .map(|panel| (&mut panel.axes, &panel.layers)),
    );
    for (axes, layers) in axes {
        for axis in axes {
            let x = axis.id == "x";
            let clock = if x {
                spec.x_axis == MsAxis::Time
            } else {
                axis.id == "y" && spec.y_axes.as_slice() == [MsAxis::Time]
            };
            if !clock {
                continue;
            }
            axis.scale = PlotAxisScale::MjdSecondsClock {
                reference_mjd_seconds: reference,
            };
            if let Some((lower, upper)) = plotms_clock_bounds(axis_samples(layers, &axis.id)) {
                axis.lower = lower;
                axis.upper = upper;
            }
        }
    }
}

fn plotms_clock_bounds(values: impl Iterator<Item = f64>) -> Option<(f64, f64)> {
    let (lower, upper) = data_bounds(values)?;
    let span = upper - lower;
    // PlotMS sets a time range before Qwt autoscales other axes.
    let padding = if span == 0.0 {
        2.0
    } else if span < 1.0 {
        span / 4.0
    } else if span < 120.0 {
        1.0
    } else {
        0.0
    };
    Some((lower - padding, upper + padding))
}

fn axis_samples<'a>(
    layers: &'a [PlotDocumentLayer],
    id: &'a str,
) -> impl Iterator<Item = f64> + 'a {
    layers
        .iter()
        .filter_map(move |layer| {
            if layer.x_axis_id == id {
                Some(&layer.x_values)
            } else if layer.y_axis_id == id {
                Some(&layer.y_values)
            } else {
                None
            }
        })
        .flatten()
        .copied()
}

fn data_bounds(values: impl Iterator<Item = f64>) -> Option<(f64, f64)> {
    let (lower, upper) = values
        .filter(|value| value.is_finite())
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(low, high), value| {
            (low.min(value), high.max(value))
        });
    (lower <= upper).then_some((lower, upper))
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
        if (columns.pointing.is_some() || plotms_row_axes::metadata_coordinate_pair(x, y).is_some())
            && columns.right_ordinates.first() == Some(&true)
        {
            let axes = std::iter::once(&mut document.axes)
                .chain(document.panels.iter_mut().map(|panel| &mut panel.axes));
            for axis in axes.flatten().filter(|axis| axis.id == "y") {
                axis.draws_on_trailing_edge = true;
            }
        }
        return Ok(());
    };
    if columns.pointing.is_none()
        && columns.additional.is_empty()
        && !matches!(payload, MsPlotPayload::ScatterGrid(_))
    {
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
            columns.pointing,
        );
    };
    match payload {
        MsPlotPayload::Scatter(plot) => {
            group(&mut document.axes, &mut document.layers, &plot.series)
        }
        MsPlotPayload::ScatterGrid(grid) => {
            for (panel, plot) in document.panels.iter_mut().zip(&grid.panels) {
                group(&mut panel.axes, &mut panel.layers, &plot.series);
            }
        }
        _ => {}
    }
    Ok(())
}

fn label_iteration_panels(
    task: &TaskInvocation<'_>,
    document: &mut PlotDocument,
    grid: &MsScatterGridPayload,
) -> Result<(), TaskFailure> {
    let title = std::mem::take(&mut document.title);
    let font = task.integer("titlefont")?;
    let iteration = iteration_name(grid.iteraxis);
    for (panel, plot) in document.panels.iter_mut().zip(&grid.panels) {
        let label = iteration_value(grid.iteraxis, plot);
        panel.title = if grid.iteraxis == MsIterationAxis::Time {
            format!("{title} : {label}")
        } else {
            format!("{title} {iteration}: {label}")
        };
        panel.title_font_size = (font > 0).then(|| font.to_string());
    }
    Ok(())
}

fn unite_panel_bounds(document: &mut PlotDocument, axis_id: &str, autoscale: bool) {
    let raw = document
        .panels
        .iter()
        .flat_map(|panel| axis_samples(&panel.layers, axis_id));
    let automatic = autoscale.then(|| plotms_auto_bounds(raw)).flatten();
    let bounds = automatic.unwrap_or_else(|| {
        document
            .panels
            .iter()
            .filter(|panel| {
                panel.layers.iter().any(|layer| {
                    !layer.x_values.is_empty()
                        && (layer.x_axis_id == axis_id || layer.y_axis_id == axis_id)
                })
            })
            .flat_map(|panel| &panel.axes)
            .filter(|axis| axis.id == axis_id)
            .fold(
                (f64::INFINITY, f64::NEG_INFINITY),
                |(lower, upper), axis| (lower.min(axis.lower), upper.max(axis.upper)),
            )
    });
    if !bounds.0.is_finite() || !bounds.1.is_finite() {
        return;
    }
    for axis in document
        .panels
        .iter_mut()
        .flat_map(|panel| &mut panel.axes)
        .filter(|axis| axis.id == axis_id)
    {
        (axis.lower, axis.upper) = bounds;
    }
}

fn plotms_auto_bounds(values: impl Iterator<Item = f64>) -> Option<(f64, f64)> {
    let (lower, upper) = data_bounds(values)?;
    // Qwt's linear scale uses at most eight major steps and aligns to
    // powers of ten and their 2x and 5x multiples.
    let interval = (upper - lower) * (1.0 - 1e-6) / 8.0;
    if !interval.is_normal() {
        return None;
    }
    let decade = 10.0_f64.powf(interval.log10().floor());
    let step = [decade, 2.0 * decade, 5.0 * decade, 10.0 * decade]
        .into_iter()
        .find(|&step| interval <= step)?;
    let tolerance = step * 1e-6;
    Some((
        ((lower + tolerance) / step).floor() * step,
        ((upper - tolerance) / step).ceil() * step,
    ))
}

fn group_ordinate_sides(
    axes: &mut Vec<PlotDocumentAxis>,
    layers: &mut [PlotDocumentLayer],
    series: &[MsScatterSeries],
    choices: &[(MsAxis, MsDataColumn)],
    right_ordinates: &[bool],
    supplied_label: Option<&str>,
    pointing: Option<&PointingAxes>,
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
            .collect::<Vec<_>>();
        if selected.is_empty() {
            continue;
        }
        let mut labels = selected
            .iter()
            .map(|&(index, (axis, column))| {
                if let Some(label) = pointing.and_then(|axes| axes.label(index, 1)) {
                    return label;
                }
                if !axis.is_visibility_math() {
                    return axis_label(axis);
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
            if axis == MsAxis::Phase || (pointing.is_some() && MsAxis::is_pointing(axis)) {
                "degrees".into()
            } else {
                label_unit(&axis_label(axis))
            }
        };
        let unit = unit_for(selected[0].1.0);
        let unit = if selected
            .iter()
            .all(|&(_, (axis, _))| unit_for(axis) == unit)
        {
            unit
        } else {
            String::new()
        };
        let (lower, upper) = if selected
            .iter()
            .all(|&(_, (axis, column))| axis == MsAxis::Phase && !column.has_unbounded_phase())
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

struct PanelStyle {
    frequency: [bool; 3],
    limits: [f64; 4],
    x_axis: MsAxis,
    frequency_frame: Option<FrequencyRef>,
    label_frame: [bool; 2],
    averaged_antenna_color: Option<MsColorAxis>,
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
        series: &[MsScatterSeries],
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
            if let Some(frame) = self.frequency_frame
                && self.label_frame[usize::from(axis.id != "x")]
                && (axis.label.starts_with("Frequency (") || axis.label.starts_with("Velocity ("))
            {
                axis.label.push(' ');
                axis.label.push_str(frame.as_str());
            }
            let offset = if axis.id == "x" { 0 } else { 2 };
            if self.limits[offset] != 0.0 || self.limits[offset + 1] != 0.0 {
                axis.lower = self.limits[offset];
                axis.upper = self.limits[offset + 1];
            }
        }
        let averaged_colors = self
            .averaged_antenna_color
            .map_or_else(BTreeMap::new, |axis| {
                plotms_averaged_colors::palette(axis, series)
            });
        for (index, (layer, source)) in layers.iter_mut().zip(series).enumerate() {
            layer.id = format!("{}-visibility-{panel}-{index}", source.y_index);
            let color = averaged_colors
                .get(&source.color_group)
                .map(|color| (*color).into());
            if color.is_some() && layer.x_values.is_empty() {
                // Draw a legend key for this masked cache category without coordinates.
                layer.kind = PlotLayerKind::Line;
                layer.style = Some(PlotLayerStyle {
                    color,
                    marker: Some("o".into()),
                    line_style: Some("None".into()),
                });
                continue;
            }
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
            let conjugate = self.x_axis.is_uv_plane_axis() && source.y_axis.is_uv_plane_axis();
            let mut values = |data: &mut Vec<f64>, scale| {
                let data = std::mem::take(data);
                writer
                    .values(
                        data.iter().map(|v| v / scale).chain(
                            data.iter()
                                .take(if conjugate { data.len() } else { 0 })
                                .map(|v| -v / scale),
                        ),
                    )
                    .map_err(write_failure)
            };
            let x = values(&mut layer.x_values, x_scale)?;
            let y = values(&mut layer.y_values, y_scale)?;
            layer.data = Some(PlotLayerData::Points { x, y });
            layer.payload_strategy = PlotPayloadStrategy::BinaryArrays;
            layer.style = Some(PlotLayerStyle {
                color,
                marker: Some("o".into()),
                line_style: None,
            });
        }
        if let Some(curves) = overlays {
            self.append_curves(panel, writer, axes, layers, curves)?;
        }
        self.uv_bounds(axes, layers, series);
        Ok(())
    }

    fn uv_bounds(
        &self,
        axes: &mut [PlotDocumentAxis],
        layers: &[PlotDocumentLayer],
        series: &[MsScatterSeries],
    ) {
        let signed = |axis: &PlotDocumentAxis| {
            if axis.id == "x" {
                self.x_axis.is_uv_plane_axis()
            } else {
                layers.iter().zip(series).any(|(layer, entry)| {
                    layer.y_axis_id == axis.id && entry.y_axis.is_uv_plane_axis()
                })
            }
        };
        let extent = |axis: &PlotDocumentAxis| axis.lower.abs().max(axis.upper.abs());
        let common = (self.limits == [0.0; 4]
            && self.x_axis.is_uv_plane_axis()
            && axes.iter().any(|axis| axis.id != "x" && signed(axis)))
        .then(|| {
            axes.iter()
                .filter(|axis| signed(axis))
                .map(extent)
                .fold(0.0, f64::max)
        });
        for axis in axes.iter_mut().filter(|axis| signed(axis)) {
            let offset = if axis.id == "x" { 0 } else { 2 };
            if self.limits[offset..offset + 2] == [0.0; 2] {
                let limit = common.unwrap_or_else(|| extent(axis));
                let limit = limit.max(1.0) * 1.05;
                axis.lower = -limit;
                axis.upper = limit;
            }
        }
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
                    kind: PlotLayerKind::Scatter,
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
                        line_style: None,
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

pub(in crate::ms::msexplore) fn default_overlay_color(index: usize) -> &'static str {
    if index == 0 { "#d88400" } else { "#b32673" }
}

pub(super) fn iteration_name(axis: MsIterationAxis) -> &'static str {
    match axis {
        MsIterationAxis::SpectralWindow => "Spw",
        MsIterationAxis::Correlation => "Corr",
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
    offset_grid: bool,
) -> Result<Option<PlotLayout>, TaskFailure> {
    let [rows, columns] = requested_grid(task)?;
    let [row, column] = requested_position(task)?;
    if row >= rows || column >= columns {
        return Err(TaskFailure::validation("row/col index out of range"));
    }
    if iterated && [row, column] != [0, 0] && !offset_grid {
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

pub(in crate::ms::msexplore) fn iteration_filename<'a>(
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
