// SPDX-License-Identifier: LGPL-3.0-or-later
//! The image task binaries on the shared task front end (#931).
//!
//! Each binary's `main` builds its front end here, passing the surfaces it
//! hosts and a callback that reads the resolved run into its typed request.
//! The front end owns the command line, `--json-run`, Last and logging.

use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

use crate::{ImageAnalysisTaskResult, ImageError, image_analysis_task_schema_bundle};

/// The front end of the image binary `program`, which hosts `surfaces`.
pub fn image_task_front_end<F>(program: &str, surfaces: &[&str], execute: F) -> TaskFrontEnd<F>
where
    F: Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>,
{
    TaskFrontEnd::new(
        program,
        image_analysis_task_schema_bundle(),
        surfaces,
        execute,
    )
    .expect("the image-analysis contract carries every image surface")
}

/// A product-writing image task's reply: its summary as the result, which a
/// command line prints as JSON.
///
/// The return value is `None` until the CASA compatibility work gives each
/// task CASA's own (#931 PR 4b).
pub fn image_task_success(
    result: Result<ImageAnalysisTaskResult, ImageError>,
) -> Result<TaskSuccess, TaskFailure> {
    let result = result.map_err(|error| TaskFailure::execution(error.to_string()))?;
    let text = serde_json::to_string_pretty(&result)
        .map_err(|error| TaskFailure::execution(format!("serialize the result: {error}")))?;
    Ok(TaskSuccess::new(CasaValue::None, &result)?.with_text(text + "\n"))
}

/// Read `name`, a CASA boolean held as text (`"true"`, `"false"`, `"T"`,
/// `"F"`, any case).
pub fn image_task_bool_text(task: &TaskInvocation<'_>, name: &str) -> Result<bool, TaskFailure> {
    let text = task.required_text(name)?;
    match text.to_ascii_lowercase().as_str() {
        "true" | "t" => Ok(true),
        "false" | "f" => Ok(false),
        _ => Err(TaskFailure::validation(format!(
            "parameter {name:?} must be true or false, found {text:?}"
        ))
        .at(name)),
    }
}
