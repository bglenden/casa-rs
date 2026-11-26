// SPDX-License-Identifier: LGPL-3.0-or-later
//! `importfits` - FITS to CASA image import, on the shared task front end.

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::ParameterValue;
use casa_images::{ImportFitsRequest, ImportFitsValue, importfits};

use super::front_end::{image_task_front_end, image_task_success};
use super::task_contract::ImageAnalysisTaskResult;

/// The front end of the `importfits` binary.
pub fn importfits_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("importfits", &["importfits"], run)
}

fn to_importfits_values(items: &[ParameterValue]) -> Vec<ImportFitsValue> {
    items
        .iter()
        .map(|item| match item {
            ParameterValue::String(value) => ImportFitsValue::String(value.clone()),
            ParameterValue::Integer(value) => ImportFitsValue::Integer(*value),
            ParameterValue::Float(value) => ImportFitsValue::Float(*value),
            ParameterValue::Bool(value) => ImportFitsValue::Bool(*value),
            _ => ImportFitsValue::Other,
        })
        .collect()
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let items = |name| match task.value(name) {
        Some(ParameterValue::Array(items)) => Ok(to_importfits_values(items)),
        _ => Err(TaskFailure::validation("expected a list").at(name)),
    };
    let request = ImportFitsRequest {
        fitsimage: task.input_path("fitsimage")?,
        imagename: task.path("imagename")?,
        whichrep: task.integer("whichrep")?,
        whichhdu: task.integer("whichhdu")?,
        zeroblanks: task.flag("zeroblanks")?,
        overwrite: task.flag("overwrite")?,
        defaultaxes: task.flag("defaultaxes")?,
        defaultaxesvalues: items("defaultaxesvalues")?,
        beam: items("beam")?,
    };
    image_task_success(importfits(&request).map(ImageAnalysisTaskResult::Importfits))
}
