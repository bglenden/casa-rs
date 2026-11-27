// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `smoothcal` input and return contract.

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::CasaValue;

use casa_calibration::smooth::{Smoother, smooth_calibration_table};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let input = task.input_path("tablein")?;
    let destination = task
        .paths("caltable")?
        .into_iter()
        .find(|path| !path.as_os_str().is_empty());
    let destination = destination.as_deref().unwrap_or(&input);
    let fields = task.texts("field")?;
    if fields.len() > 1 && fields.iter().any(|field| field.trim().is_empty()) {
        return Err(TaskFailure::execution(
            "a field list cannot contain empty selectors",
        ));
    }
    let field = fields.join(",");
    let smoother = task
        .text("smoothtype")?
        .as_deref()
        .and_then(Smoother::from_name)
        .ok_or_else(|| {
            TaskFailure::validation("smoothtype must be mean or median").at("smoothtype")
        })?;
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
