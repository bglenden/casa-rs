// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imregrid` - CASA-style regridding onto a template, on the shared task
//! front end.

use std::process::ExitCode;

use casa_contracts::{CasaValue, ParameterValue};
use casa_images::{
    ImageAnalysisTaskResult, ImregridOutcome, ImregridRequest, ImregridTemplate,
    image_task_front_end, image_task_returning, imregrid,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("imregrid", &["imregrid"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let template = match task.value("template") {
        Some(ParameterValue::String(template)) if !template.is_empty() => {
            ImregridTemplate::Image(template.into())
        }
        Some(ParameterValue::Table(record)) => ImregridTemplate::Record(record.clone()),
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
        output: task.text("output")?.unwrap_or_default().into(),
        interpolation: task.required_text("interpolation")?,
        asvelocity: task.flag("asvelocity")?,
        axes: task.integers("axes")?,
        shape: task.integers("shape")?,
        decimate: task.integer("decimate")?,
        replicate: task.flag("replicate")?,
        overwrite: task.flag("overwrite")?,
    };
    let outcome = imregrid(&request);
    let value = match &outcome {
        Ok(ImregridOutcome::Template(value)) => value.clone(),
        _ => CasaValue::None,
    };
    image_task_returning(value, outcome.map(ImageAnalysisTaskResult::Imregrid))
}
