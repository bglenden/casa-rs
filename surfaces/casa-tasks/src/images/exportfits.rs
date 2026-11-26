// SPDX-License-Identifier: LGPL-3.0-or-later
//! `exportfits` - CASA image to FITS export, on the shared task front end.

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_images::{ExportFitsRequest, exportfits};

use super::front_end::{image_task_front_end, image_task_success};
use super::task_contract::ImageAnalysisTaskResult;

/// The front end of the `exportfits` binary.
pub fn exportfits_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("exportfits", &["exportfits"], run)
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
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
