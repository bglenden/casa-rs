// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA plotweather parameter and result adapter.

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let path = task.input_path("vis")?;
    let ms = MeasurementSet::open_retained_read(&path)
        .map_err(|error| TaskFailure::execution(error.to_string()))?;
    let estimate = crate::weather::estimate_weather(
        &ms,
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
            .map(CasaValue::float)
            .collect(),
    };
    TaskSuccess::new(value, &estimate)
}
