// SPDX-License-Identifier: LGPL-3.0-or-later
//! `calibrate`, the calibration task provider, on the shared task front end
//! (#931).
//!
//! One binary hosts the general `calibrate` surface, whose `mode` picks the
//! workflow, and the CASA-named `applycal`, `gaincal`, `bandpass`,
//! `fluxscale` and `gencal`, one workflow each. The command line's first
//! word names the surface (`calibrate gaincal --ms ...`); `calibrate` runs
//! otherwise. The front end owns the command line, `--json-run`, Last and
//! logging; this module reads the resolved run into its
//! [`CalibrationTaskRequest`], runs it, and writes its report.
//!
//! A run's reply carries the tagged [`CalibrationTaskResult`]. A command line
//! prints the workflow's report in `format`, or writes it to `output`; a
//! report that would replace an existing file is refused before the task
//! runs unless `overwrite` is set. The calibration tasks return None for
//! now; CASA's own return values come with the CASA-exact surfaces.
//!
//! [`CalibrationTaskRequest`]: crate::CalibrationTaskRequest
//! [`CalibrationTaskResult`]: crate::CalibrationTaskResult

mod read;
mod render;

use std::path::Path;

use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

use crate::calibration_task_schema_bundle;

/// The surfaces `calibrate` hosts; the first runs when the command line
/// names none.
pub const CALIBRATION_SURFACES: [&str; 6] = [
    "calibrate",
    "applycal",
    "gaincal",
    "bandpass",
    "fluxscale",
    "gencal",
];

/// The front end of the `calibrate` binary, hosting
/// [`CALIBRATION_SURFACES`].
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new(
        "calibrate",
        calibration_task_schema_bundle(),
        &CALIBRATION_SURFACES,
        run_task,
    )
    .expect("the calibration contract carries its surfaces")
}

/// Run one resolved calibration request and write its report.
fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let report = read::report(task)?;
    let request = read::task_request(task)?;
    // Checked before the task runs, so a refused report changes nothing.
    if let Some(path) = report.output.as_deref() {
        refuse_existing_report(path, report.overwrite)?;
    }
    let result = request.execute().map_err(TaskFailure::execution)?;
    let rendered = render::render(report.format, &result).map_err(TaskFailure::execution)?;
    let text = match report.output.as_deref() {
        Some(path) => {
            std::fs::write(path, rendered).map_err(|error| {
                TaskFailure::execution(format!("write {}: {error}", path.display()))
            })?;
            String::new()
        }
        None => rendered,
    };
    Ok(TaskSuccess::new(casa_task_runtime::CasaValue::None, &result)?.with_text(text))
}

fn refuse_existing_report(path: &Path, overwrite: bool) -> Result<(), TaskFailure> {
    if path.exists() && !overwrite {
        return Err(TaskFailure::validation(format!(
            "refusing to overwrite existing output {}; set overwrite to replace it",
            path.display()
        ))
        .at("output"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
