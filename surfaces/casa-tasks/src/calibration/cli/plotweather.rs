// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA plotweather parameter and result adapter.

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::{CasaFloat, CasaValue};
use casa_plot_data::calibration::weather_figure::{WeatherFigure, prepare};
use serde::Serialize;

#[derive(Serialize)]
struct WeatherReport {
    opacity: Vec<CasaFloat>,
    windows_ghz: Vec<CasaFloat>,
    plot: Option<WeatherFigure>,
}

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let vis = task.text("vis")?.unwrap_or_default();
    let plot_name = task.text("plotName")?.unwrap_or_default();
    let estimate = casa_calibration::weather::estimate_weather(
        &path,
        task.number("seasonal_weight")?,
        task.flag("doPlot")?,
    )
    .map_err(TaskFailure::execution)?;
    let value = CasaValue::List {
        items: estimate
            .opacity
            .iter()
            .copied()
            .map(|value| CasaValue::Float {
                value: CasaFloat(value),
            })
            .collect(),
    };
    let plot = estimate
        .plot_data
        .as_ref()
        .map(|data| prepare(data, &vis, &plot_name))
        .transpose()
        .map_err(TaskFailure::execution)?;
    let report = WeatherReport {
        opacity: estimate.opacity.into_iter().map(CasaFloat).collect(),
        windows_ghz: estimate.windows_ghz.into_iter().map(CasaFloat).collect(),
        plot,
    };
    let success = TaskSuccess::new(value, &report)?;
    if report.plot.is_some() {
        Ok(success.with_note("Use casars.tasks.plotweather for the weather figure."))
    } else {
        Ok(success)
    }
}
