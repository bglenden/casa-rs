// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `rerefant` input and return contract.

use casa_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let input = task.input_path("tablein")?;
    let destination = task
        .paths("caltable")?
        .into_iter()
        .find(|path| !path.as_os_str().is_empty());
    let flexible = match task.text("refantmode")?.as_deref() {
        Some("flex") => true,
        Some("strict") => false,
        _ => {
            return Err(
                TaskFailure::validation("refantmode must be flex or strict").at("refantmode")
            );
        }
    };
    let selector = task.text("refant")?.unwrap_or_default();
    crate::rerefant::rereference_calibration_table(
        &vis,
        &input,
        destination.as_deref().unwrap_or(&input),
        flexible,
        &selector,
    )
    .map_err(TaskFailure::execution)?;
    TaskSuccess::new(CasaValue::None, &())
}
