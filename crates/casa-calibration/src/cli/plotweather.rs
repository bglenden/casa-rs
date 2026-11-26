// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA plotweather parameter and result adapter.

use casa_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let vis = task.text("vis")?.unwrap_or_default();
    let plot_name = task.text("plotName")?.unwrap_or_default();
    let estimate = crate::weather::estimate_weather(
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
            .map(|value| CasaValue::Float { value })
            .collect(),
    };
    let success = TaskSuccess::new(value, &estimate)?;
    if estimate.plot.is_some() {
        Ok(success.with_note("Use casars.tasks.plotweather for the weather figure."))
    } else {
        Ok(success)
    }
}
