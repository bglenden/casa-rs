// SPDX-License-Identifier: LGPL-3.0-or-later
//! `feather` - CASA-style feathering of single-dish and interferometer
//! images, on the shared task front end.

use crate::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_images::{FeatherRequest, feather};

use super::front_end::{image_task_front_end, image_task_success};
use super::task_contract::ImageAnalysisTaskResult;

/// The front end of the `feather` binary.
pub fn feather_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    image_task_front_end("feather", &["feather"], run)
}

fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let request = FeatherRequest {
        imagename: task.path("imagename")?,
        highres: task.input_path("highres")?,
        lowres: task.input_path("lowres")?,
        sdfactor: task.number("sdfactor")? as f32,
        effdishdiam: task.number("effdishdiam")? as f32,
        lowpassfiltersd: task.flag("lowpassfiltersd")?,
        overwrite: task.flag("overwrite")?,
    };
    image_task_success(feather(&request).map(ImageAnalysisTaskResult::Feather))
}
