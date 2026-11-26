// SPDX-License-Identifier: LGPL-3.0-or-later
//! `impv` - CASA-style position-velocity images, on the shared task front
//! end.

use std::process::ExitCode;

use casa_contracts::ParameterValue;
use casa_images::{
    ImageAnalysisTaskResult, ImpvRequest, PvAmount, image_task_front_end, image_task_success, impv,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::de::DeserializeOwned;

fn main() -> ExitCode {
    image_task_front_end("impv", &["impv"], run).main(
        std::env::args_os().skip(1),
        casa_task_runtime::CasaLoggingConfig::from_env(),
        std::env::set_current_dir,
        &mut std::io::stdout(),
        std::io::stderr(),
    )
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = ImpvRequest {
        imagename: task.input_path("imagename")?,
        outfile: task.path("outfile")?,
        mode: task.required_text("mode")?,
        start: optional(task, "start")?,
        end: optional(task, "end")?,
        center: optional(task, "center")?,
        length: amount(task, "length")?,
        pa: amount(task, "pa")?,
        width: amount(task, "width")?.unwrap_or(PvAmount::Whole(1)),
        unit: task.required_text("unit")?,
        region: task.value("region").cloned(),
        chans: task.text("chans")?,
        stokes: task.text("stokes")?,
        mask: task.text("mask")?,
        stretch: task.flag("stretch")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(impv(&request).map(ImageAnalysisTaskResult::Impv))
}

fn optional<T: DeserializeOwned>(
    task: &TaskInvocation<'_>,
    name: &str,
) -> Result<Option<T>, TaskFailure> {
    let Some(value) = task.value(name) else {
        return Ok(None);
    };
    if matches!(value, ParameterValue::String(text) if text.trim().is_empty())
        || matches!(value, ParameterValue::Array(values) if values.is_empty())
    {
        return Ok(None);
    }
    serde_json::from_value(value.to_plain_json())
        .map(Some)
        .map_err(|error| TaskFailure::validation(format!("invalid {name}: {error}")).at(name))
}

fn amount(task: &TaskInvocation<'_>, name: &str) -> Result<Option<PvAmount>, TaskFailure> {
    if let Some(ParameterValue::Table(record)) = task.value(name) {
        return Ok(Some(PvAmount::Quantity(record.clone())));
    }
    if let Some(ParameterValue::String(text)) = task.value(name)
        && let Ok(pixels) = text.parse::<i64>()
    {
        return Ok(Some(PvAmount::Whole(pixels)));
    }
    optional(task, name)
}
