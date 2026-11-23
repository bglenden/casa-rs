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
    let columns = task.value("ydatacolumn");
    let names = match (task.value("yaxis"), columns) {
        (Some(ParameterValue::Array(_)), Some(ParameterValue::Array(_))) => {
            super::calibration::axis_names(columns).map_err(TaskFailure::validation)?
        }
        (Some(ParameterValue::Array(_)), _) | (_, Some(ParameterValue::Array(_))) => Vec::new(),
        _ => super::calibration::axis_names(columns).map_err(TaskFailure::validation)?,
    };
    let name = |index| names.get(index).copied().unwrap_or("");
    if secondary_axis == Some(y_axis) && name(0) == name(1) {
        return Err(TaskFailure::validation("Remove duplicate y-axes."));
    }
    let column = |index| match name(index) {
        "" => Ok(MsDataColumn::Data),
        name => MsDataColumn::parse(name).map_err(TaskFailure::validation),
    };
    Ok([column(0)?, column(1)?])
}
