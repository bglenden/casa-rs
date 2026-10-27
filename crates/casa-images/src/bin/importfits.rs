// SPDX-License-Identifier: LGPL-3.0-or-later
//! `importfits` - FITS to CASA image import, on the shared task front end.

use std::process::ExitCode;

use casa_images::{ImageAnalysisTaskResult, image_task_front_end, image_task_success, import_fits};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("importfits", &["importfits"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let result = import_fits(
        task.path("fitsimage")?,
        task.path("imagename")?,
        task.flag("overwrite")?,
    );
    image_task_success(result.map(ImageAnalysisTaskResult::Importfits))
}
