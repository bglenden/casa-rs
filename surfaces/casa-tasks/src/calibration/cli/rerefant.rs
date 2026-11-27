// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA `rerefant` input and return contract.

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_calibration::rerefant::{RereferenceError, rereference_calibration_table};
use casa_contracts::CasaValue;
use casa_observation::ObservationError;

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
    rereference_calibration_table(
        &vis,
        &input,
        destination.as_deref().unwrap_or(&input),
        flexible,
        &selector,
    )
    .map_err(|error| match error {
        RereferenceError::Observation(ObservationError::Unsupported { .. }) => {
            TaskFailure::validation(error.to_string()).at("vis")
        }
        _ => TaskFailure::execution(error.to_string()),
    })?;
    TaskSuccess::new(CasaValue::None, &())
}
