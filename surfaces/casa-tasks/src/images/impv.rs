// SPDX-License-Identifier: LGPL-3.0-or-later
//! `impv` - CASA-style position-velocity images, on the shared task front
//! end.

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::ParameterValue;
use casa_images::{ImpvRequest, PvAmount, impv};
use serde::de::DeserializeOwned;

use super::front_end::{
    image_task_front_end, image_task_success, parse_region_parameter, table_fields_to_record,
};
use super::task_contract::{ImageAnalysisTaskResult, PvTaskAmount};

/// The front end of the `impv` binary.
pub fn impv_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("impv", &["impv"], run)
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
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
        region: parse_region_parameter(task.value("region"), true)?,
        chans: task.text("chans")?,
        stokes: task.text("stokes")?,
        mask: super::front_end::mask_in_workspace(task)?,
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
        return Ok(Some(PvAmount::Record(table_fields_to_record(record)?)));
    }
    if let Some(ParameterValue::String(text)) = task.value(name)
        && let Ok(pixels) = text.parse::<i64>()
    {
        return Ok(Some(PvAmount::Whole(pixels)));
    }
    let Some(parsed) = optional::<PvTaskAmount>(task, name)? else {
        return Ok(None);
    };
    Ok(Some(match parsed {
        PvTaskAmount::Whole(v) => PvAmount::Whole(v),
        PvTaskAmount::Fractional(v) => PvAmount::Fractional(v),
        PvTaskAmount::Angular(v) => PvAmount::Angular(v),
        PvTaskAmount::Quantity(record) => PvAmount::Record(table_fields_to_record(&record)?),
    }))
}
