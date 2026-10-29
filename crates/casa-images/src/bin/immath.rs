// SPDX-License-Identifier: LGPL-3.0-or-later
//! `immath` - CASA-style image arithmetic, on the shared task front end.

use std::process::ExitCode;

use casa_images::{
    ImageAnalysisTaskResult, ImmathRequest, image_task_front_end, image_task_returning, immath,
};
use casa_task_runtime::{CasaValue, TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("immath", &["immath"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = ImmathRequest {
        imagename: task.paths("imagename")?,
        outfile: task.path("outfile")?,
        expr: task.required_text("expr")?,
        overwrite: task.flag("overwrite")?,
    };
    // CASA's immath returns True.
    image_task_returning(
        CasaValue::Bool { value: true },
        immath(&request).map(ImageAnalysisTaskResult::Immath),
    )
}
