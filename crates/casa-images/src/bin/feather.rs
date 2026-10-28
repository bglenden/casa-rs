// SPDX-License-Identifier: LGPL-3.0-or-later
//! `feather` - CASA-style feathering of single-dish and interferometer
//! images, on the shared task front end.

use std::process::ExitCode;

use casa_images::{
    FeatherRequest, ImageAnalysisTaskResult, feather, image_task_front_end, image_task_success,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("feather", &["feather"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = FeatherRequest {
        imagename: task.path("imagename")?,
        highres: task.path("highres")?,
        lowres: task.path("lowres")?,
        sdfactor: task.number("sdfactor")? as f32,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(feather(&request).map(ImageAnalysisTaskResult::Feather))
}
