// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imregrid` - CASA-style regridding onto a template, on the shared task
//! front end.

use std::process::ExitCode;

use casa_images::{
    ImageAnalysisTaskResult, ImregridRequest, image_task_front_end, image_task_success, imregrid,
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
    let template = task.path("template")?;
    // CASA's default, `template='get'`, returns the input's coordinate
    // system and shape instead of regridding.
    if template.as_os_str().eq_ignore_ascii_case("get") {
        return Err(TaskFailure::validation(
            "template='get' is not implemented by casa-rs yet (#977); give a template image",
        )
        .at("template"));
    }
    let request = ImregridRequest {
        imagename: task.path("imagename")?,
        template,
        output: task.path("output")?,
        interpolation: task.required_text("interpolation")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(imregrid(&request).map(ImageAnalysisTaskResult::Imregrid))
}
