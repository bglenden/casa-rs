// SPDX-License-Identifier: LGPL-3.0-or-later
//! `impbcor` - CASA-style primary-beam correction, on the shared task front
//! end.

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::{ParameterValue, parameter_array_to_casa};
use casa_core::ArrayValue;
use casa_images::{ImpbcorRequest, PbcorBeam, impbcor};

use super::front_end::{image_task_front_end, image_task_success, parse_region_parameter};
use super::task_contract::ImageAnalysisTaskResult;

/// The front end of the `impbcor` binary.
pub fn impbcor_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("impbcor", &["impbcor"], run)
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let pbimage = match task.value("pbimage") {
        Some(ParameterValue::String(name)) if !name.is_empty() => {
            PbcorBeam::Image(task.input_path("pbimage")?)
        }
        Some(ParameterValue::Array(pixels)) if !pixels.is_empty() => {
            let values = parameter_array_to_casa(pixels).map_err(TaskFailure::execution)?;
            let ArrayValue::Float64(array) = values else {
                return Err(TaskFailure::execution(
                    "pbimage pixels must be a floating-point array",
                ));
            };
            PbcorBeam::Pixels(array)
        }
        _ => {
            return Err(TaskFailure::validation(
                "pbimage must name an image or supply floating-point beam pixels",
            )
            .at("pbimage"));
        }
    };
    let region = parse_region_parameter(task.value("region"), false)?;
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
