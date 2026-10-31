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
//! A run's reply carries the tagged [`CalibrationTaskResult`] and the value
//! CASA's task returns: `gaincal` and `bandpass` their tables, selected data
//! and interval counts ([`solve_record`]), `fluxscale` its flux densities
//! ([`fluxscale_record`]), and `applycal`, `gencal` and `calibrate`
//! `None`. A command line prints the workflow's report in `format`, or
//! writes it to `output`; a report that would replace an existing file is
//! refused before the task runs unless `overwrite` is set.
//!
//! [`CalibrationTaskRequest`]: crate::CalibrationTaskRequest
//! [`CalibrationTaskResult`]: crate::CalibrationTaskResult

mod casa_read;
mod read;
mod render;

use std::path::Path;

use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

use crate::{
    ApplyCalibrationTableSpec, CalibrationTaskRequest, CalibrationTaskResult,
    calibration_task_schema_bundle, fluxscale_record, solve_record,
};

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
    let applied = match &request {
        CalibrationTaskRequest::SolveGain(solve) => solve.prior_calibration_tables.clone(),
        CalibrationTaskRequest::SolveBandpass(solve) => solve.prior_calibration_tables.clone(),
        _ => Vec::new(),
    };
    let result = request.execute().map_err(TaskFailure::execution)?;
    let value = casa_value(task.surface(), &applied, &result);
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
    Ok(TaskSuccess::new(value, &result)?.with_text(text))
}

/// What `surface` returns for `result`, as CASA's task of that name does;
/// `applied` is the chain a solve pre-applied.
fn casa_value(
    surface: &str,
    applied: &[ApplyCalibrationTableSpec],
    result: &CalibrationTaskResult,
) -> CasaValue {
    match (surface, result) {
        ("gaincal", CalibrationTaskResult::SolveGain(report)) => solve_record(
            applied,
            &report.selected_data,
            &report.output_table,
            &report.solve_stats,
        ),
        ("bandpass", CalibrationTaskResult::SolveBandpass(report)) => solve_record(
            applied,
            &report.selected_data,
            &report.output_table,
            &report.solve_stats,
        ),
        ("fluxscale", CalibrationTaskResult::FluxScale(report)) => fluxscale_record(report),
        _ => CasaValue::None,
    }
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
mod casa_tests;
#[cfg(test)]
mod tests;
