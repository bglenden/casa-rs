// SPDX-License-Identifier: LGPL-3.0-or-later
//! `impv` - CASA-style position-velocity images, on the shared task front
//! end.

use std::process::ExitCode;

use casa_images::{
    ImageAnalysisTaskResult, ImpvRequest, image_task_front_end, image_task_success, impv,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("impv", &["impv"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let width = task.integer("width")?;
    let request = ImpvRequest {
        imagename: task.input_path("imagename")?,
        outfile: task.path("outfile")?,
        mode: "coords".to_string(),
        start: task.required_text("start")?,
        end: task.required_text("end")?,
        width: usize::try_from(width).map_err(|_| {
            TaskFailure::validation(format!("width must be at least 1, found {width}")).at("width")
        })?,
        chans: task.text("chans")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(impv(&request).map(ImageAnalysisTaskResult::Impv))
}
