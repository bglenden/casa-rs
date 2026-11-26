// SPDX-License-Identifier: LGPL-3.0-or-later
//! `impbcor` - CASA-style primary-beam correction, on the shared task front
//! end.

use std::process::ExitCode;

use casa_contracts::ParameterValue;
use casa_images::{
    ImageAnalysisTaskResult, ImpbcorRequest, PbcorBeam, PbcorRegion, image_task_front_end,
    image_task_success, impbcor,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

fn main() -> ExitCode {
    image_task_front_end("impbcor", &["impbcor"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let pbimage = match task.value("pbimage") {
        Some(ParameterValue::String(name)) if !name.is_empty() => {
            PbcorBeam::Image(task.input_path("pbimage")?)
        }
        Some(ParameterValue::Array(pixels)) if !pixels.is_empty() => {
            PbcorBeam::Pixels(pixels.clone())
        }
        _ => {
            return Err(TaskFailure::validation(
                "pbimage must name an image or supply floating-point beam pixels",
            )
            .at("pbimage"));
        }
    };
    let region = match task.value("region") {
        Some(ParameterValue::String(text)) if text.trim().is_empty() => None,
        Some(ParameterValue::String(text)) => Some(PbcorRegion::Text(text.clone())),
        Some(ParameterValue::Table(fields)) if fields.is_empty() => None,
        Some(ParameterValue::Table(fields)) => Some(PbcorRegion::Record(fields.clone())),
        None => None,
        _ => {
            return Err(TaskFailure::validation(
                "region must be CRTF text, a filename, or a CASA region record",
            )
            .at("region"));
        }
    };
    let mask = task.text("mask")?;
    // CASA reads stretch only with a mask, and ignores it otherwise.
    let stretch = mask.is_some() && task.flag("stretch")?;
    let request = ImpbcorRequest {
        imagename: task.input_path("imagename")?,
        pbimage,
        outfile: task.path("outfile")?,
        mode: task.required_text("mode")?,
        cutoff: cutoff(task.value("cutoff"))?,
        box_selection: task.text("box")?,
        region,
        chans: task.text("chans")?,
        stokes: task.text("stokes")?,
        mask,
        stretch,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(impbcor(&request).map(ImageAnalysisTaskResult::Impbcor))
}

fn cutoff(value: Option<&ParameterValue>) -> Result<f64, TaskFailure> {
    let value = match value {
        Some(ParameterValue::NumpyScalar { value, .. }) => Some(value.as_ref()),
        value => value,
    };
    let invalid = || TaskFailure::validation("cutoff must be numeric").at("cutoff");
    match value {
        Some(ParameterValue::Float(number)) => Ok(*number),
        Some(ParameterValue::Integer(number)) => Ok(*number as f64),
        Some(ParameterValue::String(text)) => text.parse().map_err(|_| invalid()),
        _ => Err(invalid()),
    }
}
