// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA PlotMS ordinate column selection.

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::{MsAxis, MsDataColumn};

pub(super) fn ordinate_columns(
    task: &TaskInvocation<'_>,
    y_axis: MsAxis,
    secondary_axis: Option<MsAxis>,
) -> Result<[MsDataColumn; 2], TaskFailure> {
    let supplied = task.value("ydatacolumn");
    let names = super::calibration::axis_names(supplied)
        .map_err(TaskFailure::validation)?
        .into_iter()
        .map(canonical_column)
        .collect::<Result<Vec<_>, _>>()?;
    let multiple_axes = matches!(task.value("yaxis"), Some(ParameterValue::Array(_)));
    let multiple_columns = matches!(supplied, Some(ParameterValue::Array(_)));
    // CASA validates all supplied columns, then applies them only to a matching axis form.
    let names = if multiple_axes == multiple_columns {
        names.as_slice()
    } else {
        &[]
    };
    let name = |index| names.get(index).copied().unwrap_or("data");
    if secondary_axis == Some(y_axis) && name(0) == name(1) {
        return Err(TaskFailure::validation("Remove duplicate y-axes."));
    }
    let column = |index| MsDataColumn::parse(name(index)).map_err(TaskFailure::validation);
    Ok([column(0)?, column(1)?])
}

fn canonical_column(name: &str) -> Result<&str, TaskFailure> {
    match name {
        "data"
        | "corrected"
        | "model"
        | "float"
        | "data-model_vector"
        | "data-model_scalar"
        | "corrected-model_vector"
        | "corrected-model_scalar"
        | "data/model_vector"
        | "data/model_scalar"
        | "corrected/model_vector"
        | "corrected/model_scalar" => Ok(name),
        "residual" | "corrected-model" => Ok("corrected-model_vector"),
        "data-model" => Ok("data-model_vector"),
        "data/model" => Ok("data/model_vector"),
        "corrected/model" => Ok("corrected/model_vector"),
        _ => Err(TaskFailure::validation(format!(
            "invalid ydatacolumn value '{name}'"
        ))),
    }
}
