// SPDX-License-Identifier: LGPL-3.0-or-later
//! `immoments` - CASA-style spectral moment maps, on the shared task front
//! end.

use std::process::ExitCode;

use casa_images::{
    ImageAnalysisTaskResult, ImmomentsRequest, image_task_front_end, image_task_success, immoments,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("immoments", &["immoments"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let moments = task.required_text("moments")?;
    let request = ImmomentsRequest {
        imagename: task.path("imagename")?,
        outfile: task.path("outfile")?,
        moments: moments.parse().map_err(|error| {
            TaskFailure::validation(format!("invalid moments {moments:?}: {error}")).at("moments")
        })?,
        chans: task.text("chans")?,
        includepix: task
            .text("includepix")?
            .map(|text| parse_range(&text))
            .transpose()?,
        mask: task.text("mask")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(immoments(&request).map(ImageAnalysisTaskResult::Immoments))
}

/// `min,max` as a pixel range.
fn parse_range(text: &str) -> Result<[f64; 2], TaskFailure> {
    let invalid = |reason: String| {
        TaskFailure::validation(format!("invalid includepix {text:?}: {reason}")).at("includepix")
    };
    let values = text
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<f64>()
                .map_err(|error| invalid(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    match values[..] {
        [min, max] => Ok([min, max]),
        _ => Err(invalid("expected min,max".to_string())),
    }
}
