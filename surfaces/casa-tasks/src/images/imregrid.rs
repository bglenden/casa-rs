// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imregrid` - CASA-style regridding onto a template, on the shared task
//! front end.

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::{
    CasaValue, ParameterValue, casacore_value_to_casa, parameter_value_to_casacore,
};
use casa_core::Value;
use casa_imageanalysis::{
    ImregridOutcome, ImregridRequest, ImregridTemplate, imregrid_with_cancellation,
};
use casa_images::ImageError;

use super::front_end::{image_task_front_end, image_task_returning};
use super::task_contract::{ImageAnalysisTaskResult, ImregridTaskOutcome};

/// The front end of the `imregrid` binary.
pub fn imregrid_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("imregrid", &["imregrid"], run)
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let template = match task.value("template") {
        Some(ParameterValue::String(template)) if ImregridTemplate::is_selector(template) => {
            ImregridTemplate::Image(template.into())
        }
        Some(ParameterValue::String(template)) if !template.is_empty() => {
            ImregridTemplate::Image(task.io_path(template))
        }
        Some(ParameterValue::Table(fields)) => {
            let Some(csys_value) = fields.get("csys") else {
                return Err(TaskFailure::execution("template record has no csys field"));
            };
            let Value::Record(csys) =
                parameter_value_to_casacore(csys_value).map_err(TaskFailure::execution)?
            else {
                return Err(TaskFailure::execution(
                    "template csys must be a coordinate record",
                ));
            };
            let Some(ParameterValue::Array(values)) = fields.get("shap") else {
                return Err(TaskFailure::execution(
                    "template shap must be an array of axis lengths",
                ));
            };
            let shape = values
                .iter()
                .map(|value| match value {
                    ParameterValue::Integer(length) if *length > 0 => usize::try_from(*length).ok(),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    TaskFailure::execution("template shape must contain positive integers")
                })?;
            ImregridTemplate::Record { csys, shape }
        }
        _ => {
            return Err(TaskFailure::validation(
                "template must be an image path or coordinate record",
            )
            .at("template"));
        }
    };
    let request = ImregridRequest {
        imagename: task.input_path("imagename")?,
        template,
        output: task.file_text("output")?.unwrap_or_default().into(),
        interpolation: task.required_text("interpolation")?,
        asvelocity: task.flag("asvelocity")?,
        axes: task.integers("axes")?,
        shape: task.integers("shape")?,
        decimate: task.integer("decimate")?,
        replicate: task.flag("replicate")?,
        overwrite: task.flag("overwrite")?,
    };
    let cancellation = task.native_options().map(|options| &options.cancel);
    let outcome = imregrid_with_cancellation(&request, cancellation).and_then(|item| match item {
        ImregridOutcome::Product(product) => Ok(ImregridTaskOutcome::Product(product)),
        ImregridOutcome::Template(value) => casacore_value_to_casa(&value)
            .map(ImregridTaskOutcome::Template)
            .map_err(|error| ImageError::InvalidMetadata(error.into())),
    });
    if matches!(outcome, Err(ImageError::Interrupted)) {
        return Err(TaskFailure::interrupted("imregrid interrupted"));
    }
    let value = match &outcome {
        Ok(ImregridTaskOutcome::Template(value)) => value.clone(),
        _ => CasaValue::None,
    };
    image_task_returning(value, outcome.map(ImageAnalysisTaskResult::Imregrid))
}
