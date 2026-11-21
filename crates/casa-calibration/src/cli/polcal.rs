// SPDX-License-Identifier: LGPL-3.0-or-later
//! The public polarization calibration task.

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::solve::grouping::leakage::solve_polarization;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let request = super::casa_read::leakage_request(task, &vis)?;
    if crate::paths::overlap(&vis, &request.output)
        .map_err(|error| TaskFailure::validation(error.to_string()).at("caltable"))?
    {
        return Err(
            TaskFailure::validation("output caltable overlaps the MeasurementSet").at("caltable"),
        );
    }
    let ms = MeasurementSet::open(&vis)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    let rows = solve_polarization(&ms, &request).map_err(TaskFailure::execution)?;
    TaskSuccess::new(
        CasaValue::None,
        &serde_json::json!({ "solution_rows": rows }),
    )
}
