// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `smoothcal` input and return contract.

use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::smooth::{Smoother, smooth_calibration_table};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let input = task.input_path("tablein")?;
    let destination = task
        .paths("caltable")?
        .into_iter()
        .find(|path| !path.as_os_str().is_empty());
    let destination = destination.as_deref().unwrap_or(&input);
    let field = task.texts("field")?.join(",");
    let smoother = match task.text("smoothtype")?.as_deref() {
        Some("mean") => Smoother::Mean,
        Some("median") => Smoother::Median,
        _ => {
            return Err(
                TaskFailure::validation("smoothtype must be mean or median").at("smoothtype")
            );
        }
    };
    let seconds = task.number("smoothtime")?;
    let ratesmooth = task.flag("ratesmooth")?;
    smooth_calibration_table(
        &vis,
        &input,
        destination,
        &field,
        smoother,
        seconds,
        ratesmooth,
    )
    .map_err(TaskFailure::execution)?;
    TaskSuccess::new(CasaValue::None, &())
}
