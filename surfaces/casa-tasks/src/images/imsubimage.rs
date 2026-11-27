// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imsubimage` - CASA-style image section extraction, on the shared task
//! front end.

use std::path::PathBuf;

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_imageanalysis::{ImsubimageRequest, imsubimage};

use super::front_end::{image_task_front_end, image_task_success};
use super::task_contract::ImageAnalysisTaskResult;

/// The front end of the `imsubimage` binary.
pub fn imsubimage_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("imsubimage", &["imsubimage"], run)
}

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let outfile = task
        .text("outfile")?
        .map_or_else(PathBuf::new, |path| task.io_path(path));
    let request = ImsubimageRequest {
        imagename: task.input_path("imagename")?,
        outfile,
        box_pixels: task.text("box")?,
        region: super::front_end::region_in_workspace(task)?,
        chans: task.text("chans")?,
        stokes: task.text("stokes")?,
        mask: task.text("mask")?,
        dropdeg: task.flag("dropdeg")?,
        overwrite: task.flag("overwrite")?,
        verbose: task.flag("verbose")?,
        stretch: task.flag("stretch")?,
        keepaxes: task.integers("keepaxes")?,
    };
    let summary =
        imsubimage(&request).map_err(|error| TaskFailure::execution(error.to_string()))?;
    let msg = format!(
        "Created subimage '{}' with shape {:?}",
        summary.outfile, summary.shape
    );
    let mut success = image_task_success(Ok(ImageAnalysisTaskResult::Imsubimage(summary)))?;
    if request.verbose {
        let seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |elapsed| elapsed.as_secs_f64());
        let mjd_seconds = seconds + 40_587.0 * 86_400.0;
        success = success.with_replayed_info(mjd_seconds, "imsubimage", &msg, false);
    }
    Ok(success)
}
