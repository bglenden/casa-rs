// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA plotweather parameter and result adapter.

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let vis = task.text("vis")?.unwrap_or_default();
    let plot_name = task.text("plotName")?.unwrap_or_default();
    let ms = MeasurementSet::open_retained_read(&path)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let estimate = crate::weather::estimate_weather(
        &ms,
        &path,
        task.number("seasonal_weight")?,
        task.flag("doPlot")?,
        &vis,
        &plot_name,
    )
    .map_err(TaskFailure::execution)?;
    let value = CasaValue::List {
        items: estimate
            .opacity
            .iter()
            .copied()
            .map(CasaValue::from)
            .collect(),
    };
    TaskSuccess::new(value, &estimate)
}
