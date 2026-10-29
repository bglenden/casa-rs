// SPDX-License-Identifier: LGPL-3.0-or-later
//! `exportfits` - CASA image to FITS export, on the shared task front end.

use std::process::ExitCode;

use casa_images::{ImageAnalysisTaskResult, export_fits, image_task_front_end, image_task_success};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("exportfits", &["exportfits"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let result = export_fits(
        task.input_path("imagename")?,
        task.path("fitsimage")?,
        task.flag("velocity")?,
        task.flag("overwrite")?,
    );
    image_task_success(result.map(ImageAnalysisTaskResult::Exportfits))
}
