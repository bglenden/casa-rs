// SPDX-License-Identifier: LGPL-3.0-or-later
//! `impbcor` - CASA-style primary-beam correction, on the shared task front
//! end.

use std::path::PathBuf;
use std::process::ExitCode;

use casa_images::{
    ImageAnalysisTaskResult, ImpbcorRequest, image_task_bool_text, image_task_front_end,
    image_task_success, impbcor,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("impbcor", &["impbcor"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = ImpbcorRequest {
        imagename: task.path("imagename")?,
        pbimage: task.path("pbimage")?,
        outfile: task.path("outfile")?,
        mode: task.required_text("mode")?,
        cutoff: task.number("cutoff")?,
        box_selection: task.text("box")?,
        region: task.text("region")?.map(PathBuf::from),
        chans: task.text("chans")?,
        stokes: task.text("stokes")?,
        mask: task.text("mask")?,
        stretch: image_task_bool_text(task, "stretch")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(impbcor(&request).map(ImageAnalysisTaskResult::Impbcor))
}
