// SPDX-License-Identifier: LGPL-3.0-or-later
//! Double-circle single-dish gain task adapter.

use std::path::Path;

use casa_calibration::execute::SolvePriorApply;
use casa_calibration::session::resolve_casa_calibration_selection;
use casa_calibration::single_dish::gain::{Controls, GainGenerateError, generate};
use casa_calibration::single_dish::lock_solve_output;
use casa_calibration::{
    ApplyMode, ApplyPlanRequest, CalibrationOutputMode, GainSolveMode, plan_apply,
};
use casa_contracts::CasaValue;
use casa_ms::{MeasurementSet, MsSelection};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use super::{gain_options, selector};

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let outfile = task.text("outfile")?.unwrap_or_default();
    let overwrite = task.flag("overwrite")?;
    if outfile.is_empty() {
        return Err(TaskFailure::validation("outfile is empty.").at("outfile"));
    }
    let output = Path::new(&outfile);
    if output.exists() && !overwrite {
        return Err(TaskFailure::execution(format!("{outfile} exists.")).at("outfile"));
    }
    let infile = task.input_path("infile")?;
    let selection = MsSelection {
        selectdata: true,
        antenna: selector(task, "antenna")?.map(|antenna| format!("{antenna}&&&")),
        field: selector(task, "field")?,
        spw: selector(task, "spw")?,
        scan: selector(task, "scan")?,
        intent: selector(task, "intent")?,
        ..MsSelection::default()
    };
    let ms = MeasurementSet::open(&infile)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("infile"))?;
    let selected = resolve_casa_calibration_selection(&ms, &selection).map_err(|error| {
        TaskFailure::execution(format!("Error in data selection specification: {error}"))
    })?;
    let tables = gain_options::priors(task)?;
    let priors = if tables.is_empty() {
        None
    } else {
        let plan = plan_apply(
            &ms,
            &ApplyPlanRequest {
                selection,
                apply_mode: ApplyMode::CalFlag,
                parang: false,
                calibration_tables: tables,
            },
        )
        .map_err(|_| gain_options::setapply_error("applytable"))?;
        Some(
            SolvePriorApply::single_dish(&ms, plan)
                .map_err(|_| gain_options::setapply_error("applytable"))?,
        )
    };
    let controls = Controls {
        radius: gain_options::radius(task)?,
        smooth: task.flag("smooth")?,
        solve_mode: GainSolveMode::AmplitudePhase,
        correlation_dependent_flags: false,
    };
    if casa_calibration::paths::overlap(&infile, output)
        .map_err(|error| TaskFailure::execution(error.to_string()))?
    {
        return Err(TaskFailure::execution(
            "The MeasurementSet and output calibration table overlap.",
        )
        .at("outfile"));
    }
    let mode = if overwrite {
        CalibrationOutputMode::Replace
    } else {
        CalibrationOutputMode::Create
    };
    let _output_locks = lock_solve_output(output, mode)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("outfile"))?;
    let (count, _) = generate(
        &ms,
        &selected,
        priors.as_ref(),
        output,
        mode,
        controls,
        None,
    )
    .map_err(|error| match error {
        GainGenerateError::Spw(message) => TaskFailure::execution(message).at("spw"),
        GainGenerateError::Other(message) => TaskFailure::execution(message),
    })?;
    TaskSuccess::new(
        CasaValue::None,
        &serde_json::json!({"caltable": outfile, "row_count": count}),
    )
}
