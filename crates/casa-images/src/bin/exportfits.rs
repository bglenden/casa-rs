// SPDX-License-Identifier: LGPL-3.0-or-later
//! `exportfits` - CASA image to FITS export, on the shared task front end.

use std::process::ExitCode;

use casa_images::{
    ExportFitsRequest, ImageAnalysisTaskResult, exportfits, image_task_front_end,
    image_task_success,
};
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
    let bitpix = task.integer("bitpix")?;
    if bitpix != -32 && bitpix != 16 {
        return Err(TaskFailure::validation(format!(
            "unsupported bitpix {bitpix}: allowed values are -32 and 16"
        ))
        .at("bitpix"));
    }
    let minpix = task.number("minpix")?;
    let maxpix = task.number("maxpix")?;
    if !minpix.is_finite() {
        return Err(TaskFailure::validation("minpix must be finite").at("minpix"));
    }
    if !maxpix.is_finite() {
        return Err(TaskFailure::validation("maxpix must be finite").at("maxpix"));
    }
    let request = ExportFitsRequest {
        imagename: task.input_path("imagename")?,
        fitsimage: task.path("fitsimage")?,
        velocity: task.flag("velocity")?,
        optical: task.flag("optical")?,
        bitpix: bitpix as i32,
        minpix,
        maxpix,
        overwrite: task.flag("overwrite")?,
        dropstokes: task.flag("dropstokes")?,
        stokeslast: task.flag("stokeslast")?,
        history: task.flag("history")?,
        dropdeg: task.flag("dropdeg")?,
    };
    image_task_success(exportfits(&request).map(ImageAnalysisTaskResult::Exportfits))
}
