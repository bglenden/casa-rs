// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imsubimage` - CASA-style image section extraction, on the shared task
//! front end.

use std::process::ExitCode;

use casa_images::{
    ImageAnalysisTaskResult, ImsubimageRequest, image_task_front_end, image_task_success,
    imsubimage,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("imsubimage", &["imsubimage"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = ImsubimageRequest {
        imagename: task.path("imagename")?,
        outfile: task.path("outfile")?,
        box_pixels: task.text("box")?,
        chans: task.text("chans")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(imsubimage(&request).map(ImageAnalysisTaskResult::Imsubimage))
}
