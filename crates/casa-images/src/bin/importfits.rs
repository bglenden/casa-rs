// SPDX-License-Identifier: LGPL-3.0-or-later
//! `importfits` - FITS to CASA image import, on the shared task front end.

use std::process::ExitCode;

use casa_contracts::ParameterValue;
use casa_images::{
    ImageAnalysisTaskResult, ImportFitsRequest, image_task_front_end, image_task_success,
    importfits,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("importfits", &["importfits"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let items = |name| match task.value(name) {
        Some(ParameterValue::Array(items)) => Ok(items.clone()),
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
