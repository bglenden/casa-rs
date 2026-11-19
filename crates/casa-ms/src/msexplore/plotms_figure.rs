// SPDX-License-Identifier: LGPL-3.0-or-later
//! Render calibrated MeasurementSet visibility data with the PlotMS presenter.

use std::path::Path;

use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::calibration::figure::plot_limits;
use super::{MsAxis, MsPlotPayload};
use crate::plot_document::{
    PlotFigure, PlotLayerData, PlotLayerStyle, PlotPayloadStrategy, PlotPayloadWriter,
    plotms_visibility_document,
};

pub(super) fn prepare(
    task: &TaskInvocation<'_>,
    payload: &MsPlotPayload,
    arrays: &Path,
    output: Option<&Path>,
    show: bool,
) -> Result<PlotFigure, TaskFailure> {
    let MsPlotPayload::Scatter(scatter) = payload else {
        return Err(TaskFailure::validation(
            "iterated MeasurementSet callibrary figures are pending #1526",
        ));
    };
    let mut document = plotms_visibility_document(payload);
    document.show_legend = task.flag("showlegend")?;
    let limits = plot_limits(task.value("plotrange"))?;
    let frequency_axis = |id: &str| match id {
        "x" => scatter.x_axis == MsAxis::Frequency,
        "y" => scatter.y_axis == MsAxis::Frequency,
        "y-secondary" => scatter.secondary_y_axis == Some(MsAxis::Frequency),
        _ => false,
    };
    for axis in &mut document.axes {
        if frequency_axis(&axis.id) {
            axis.lower /= 1e9;
            axis.upper /= 1e9;
            axis.unit = "GHz".into();
            if axis.label == "Frequency (Hz)" {
                axis.label = "Frequency (GHz)".into();
            }
        }
        let offset = if axis.id == "x" { 0 } else { 2 };
        if limits[offset] != 0.0 || limits[offset + 1] != 0.0 {
            axis.lower = limits[offset];
            axis.upper = limits[offset + 1];
        }
    }
    let io =
        |error: std::io::Error| TaskFailure::execution(format!("write PlotMS arrays: {error}"));
    let mut writer = PlotPayloadWriter::open(arrays).map_err(io)?;
    let connect = task.flag("timeconnector")?
        || task
            .text("xconnector")?
            .as_deref()
            .is_some_and(|value| matches!(value, "line" | "step"));
    for (index, layer) in document.layers.iter_mut().enumerate() {
        let (x_scale, y_scale) = (
            if frequency_axis(&layer.x_axis_id) {
                1e9
            } else {
                1.0
            },
            if frequency_axis(&layer.y_axis_id) {
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
            .map_err(io)?;
        let y = writer
            .values(
                std::mem::take(&mut layer.y_values)
                    .into_iter()
                    .map(|v| v / y_scale),
            )
            .map_err(io)?;
        layer.id = format!("{index}-visibility");
        layer.data = Some(PlotLayerData::Points { x, y });
        layer.payload_strategy = PlotPayloadStrategy::BinaryArrays;
        if connect {
            layer.style = Some(PlotLayerStyle {
                color: None,
                marker: None,
                line_style: Some("-".into()),
            });
        }
    }
    writer.finish().map_err(io)?;
    Ok(PlotFigure {
        output_file: output.map(Path::to_owned),
        show,
        document,
    })
}
