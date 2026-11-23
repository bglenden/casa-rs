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

mod calstat;
mod casa_read;
mod ft;
mod getantposalma;
mod getcalmodvla;
mod getephemtable;
pub(crate) mod history;
mod initweights;
mod jyperk;
mod listcal;
mod model_columns;
mod multi_ms;
mod plotbandpass;
mod plotweather;
mod polcal;
mod polfromgain;
mod read;
mod render;
mod rerefant;
mod setjy;

pub use setjy::direct::{ImagerSetJyRequest, imager_setjy};
mod smoothcal;
mod statwt;
mod uvmodelfit;
mod wvrgcal;

use std::path::Path;

use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

use crate::{
    ApplyCalibrationTableSpec, CalibrationTaskRequest, CalibrationTaskResult,
    calibration_task_schema_bundle, fluxscale_record, solve_record,
};

pub use crate::task_contract::CALIBRATION_SURFACES;

/// The front end of the `calibrate` binary, hosting
/// [`CALIBRATION_SURFACES`].
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "calibrate",
        calibration_task_schema_bundle(),
        &CALIBRATION_SURFACES,
        run_task,
    )
    .expect("the calibration contract carries its surfaces")
}

/// Run one resolved calibration request and write its report.
fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    match task.surface() {
        "appendantab" => return crate::appendantab::run(task),
        "pccor" => return crate::pccor::run(task),
        "getephemtable" => return getephemtable::run(task),
        "sdcal" => return crate::single_dish::sdcal(task),
        "sdgaincal" => return crate::single_dish::gain::run(task),
        _ => {}
    }
    if task.surface() == "sdsmooth" {
        return crate::single_dish::preparation::run_sdsmooth(task);
    }
    if task.surface() == "sdatmcor" {
        return crate::single_dish::preparation::atmosphere::task::run(task);
    }
    if task.surface() == "sdfit" {
        return crate::single_dish::preparation::line_fit::task::run(task);
    }
    if task.surface() == "sdbaseline" {
        return crate::single_dish::preparation::baseline::task::run(task);
    }
    if task.surface() == "getjyperkalma"
        || (task.surface() == "gencal" && task.required_text("caltype")? == "jyperk")
    {
        return jyperk::run(task);
    }
    if task.surface() == "polcal"
        || (task.surface() == "gaincal"
            && matches!(task.required_text("gaintype")?.as_str(), "XY+QU" | "XYf+QU"))
    {
        return polcal::run(task);
    }
    if task.surface() == "polfromgain" {
        return polfromgain::run(task);
    }
    if task.surface() == "wvrgcal" {
        return wvrgcal::run(task);
    }
    if task.surface() == "listcal" {
        return listcal::run(task);
    }
    if task.surface() == "calstat" {
        return calstat::run(task);
    }
    if task.surface() == "getantposalma" {
        return getantposalma::run(task);
    }
    if task.surface() == "getcalmodvla" {
        return getcalmodvla::run(task);
    }
    if task.surface() == "ft" {
        return ft::run(task);
    }
    if task.surface() == "setjy" {
        return setjy::run(task);
    }
    if task.surface() == "rerefant" {
        return rerefant::run(task);
    }
    if task.surface() == "smoothcal" {
        return smoothcal::run(task);
    }
    if task.surface() == "plotbandpass" {
        return plotbandpass::run(task);
    }
    if task.surface() == "uvmodelfit" {
        return uvmodelfit::run(task);
    }
    if task.surface() == "initweights" {
        return initweights::run(task);
    }
    if task.surface() == "plotweather" {
        return plotweather::run(task);
    }
    if task.surface() == "statwt" {
        return statwt::run(task);
    }
    if matches!(task.surface(), "clearcal" | "delmod" | "uvsub") {
        return model_columns::run(task);
    }
    let report = read::report(task)?;
    let request = read::task_request(task)?;
    // Checked before the task runs, so a refused report changes nothing.
    if let Some(path) = report.output.as_deref() {
        refuse_existing_report(path, report.overwrite)?;
        refuse_report_in_product(path, &request, task)?;
    }
    if task.surface() == "fluxscale"
        && let CalibrationTaskRequest::FluxScale(scale) = &request
        && !scale.append
        && scale.output_table.exists()
    {
        return Err(TaskFailure::validation(format!("fluxtable {} exists. Please specify a different name. Or set append=True, to append the results to the table.",scale.output_table.display())).at("fluxtable"));
    }
    let applied = match &request {
        CalibrationTaskRequest::SolveGain(solve)
        | CalibrationTaskRequest::SolveAccor(solve)
        | CalibrationTaskRequest::SolveBaseline { gain: solve, .. } => {
            solve.prior_calibration_tables.clone()
        }
        CalibrationTaskRequest::SolveBandpass(solve) => solve.prior_calibration_tables.clone(),
        _ => Vec::new(),
    };
    let mode = if task.surface() == "calibrate" {
        Some(if report.overwrite {
            crate::CalibrationOutputMode::Replace
        } else {
            crate::CalibrationOutputMode::Create
        })
    } else {
        None
    };
    let multi = match &request {
        CalibrationTaskRequest::ExecuteApply(request) if task.surface() == "applycal" => {
            multi_ms::execute(task, request)?
        }
        _ => None,
    };
    let result = if let Some(report) = multi {
        CalibrationTaskResult::ApplyMultiMs(report)
    } else if let CalibrationTaskRequest::ExecuteApply(request) = &request {
        request
            .execute_apply()
            .map(CalibrationTaskResult::Apply)
            .map_err(|error| {
                let failure = TaskFailure::execution(error.to_string());
                match error {
                    crate::ApplyExecutionError::Plan(source) => match *source {
                        crate::ApplyPlanError::Argument(
                            crate::ApplyArgumentError::MissingCalwt,
                        ) => failure.at("calwt"),
                        crate::ApplyPlanError::Argument(crate::ApplyArgumentError::SpwMap(_)) => {
                            failure.at("spwmap")
                        }
                        _ => failure,
                    },
                    _ => failure,
                }
            })?
    } else if let CalibrationTaskRequest::Gencal(request) = &request {
        request
            .generate(mode.unwrap_or(crate::CalibrationOutputMode::Update))
            .map(CalibrationTaskResult::Gencal)
            .map_err(|error| match error {
                crate::GencalError::PositionInput(error) => position_input_failure(error),
                crate::GencalError::EopFile { path, source } => {
                    let parameter = match source.kind() {
                        std::io::ErrorKind::NotFound => "infile.missing",
                        std::io::ErrorKind::PermissionDenied => "infile.permission",
                        std::io::ErrorKind::IsADirectory => "infile.directory",
                        _ => "infile.io",
                    };
                    let message = source.to_string();
                    let message = if let Some(errno) = source.raw_os_error() {
                        let reason = message
                            .split_once(" (os error ")
                            .map_or(message.as_str(), |(reason, _)| reason);
                        let path = casa_provider_contracts::ParameterValue::String(
                            path.display().to_string(),
                        );
                        format!(
                            "[Errno {errno}] {reason}: {}",
                            casa_provider_contracts::python_repr(&path)
                        )
                    } else {
                        format!("{message}: {}", path.display())
                    };
                    TaskFailure::execution(message).at(parameter)
                }
                crate::GencalError::AutomaticPositionInput(reason) => {
                    TaskFailure::validation(reason).at("antenna")
                }
                ref error @ crate::GencalError::VlaArchive { ref source, .. } => {
                    let parameter = match source.as_ref() {
                        ureq::Error::StatusCode(code) => format!("antenna.archive.http.{code}"),
                        _ => "antenna.archive.url".to_string(),
                    };
                    TaskFailure::execution(error.to_string()).at(parameter)
                }
                error => TaskFailure::execution(error.to_string()),
            })?
    } else {
        request
            .execute_with_output_mode(mode)
            .map_err(TaskFailure::execution)?
    };
    if task.surface() == "fluxscale"
        && let (
            CalibrationTaskRequest::FluxScale(request),
            CalibrationTaskResult::FluxScale(report),
        ) = (&request, &result)
    {
        crate::fluxscale::history::record(task, request, report);
    }
    if task.surface() == "applycal"
        && matches!(&result, CalibrationTaskResult::Apply(_))
        && let CalibrationTaskRequest::ExecuteApply(request) = &request
    {
        history::record_applycal(task, &request.measurement_set);
    }
    let value = casa_value(task.surface(), &applied, &result);
    report_task_success(task.surface(), &report, &result, value)
}

fn report_task_success(
    surface: &str,
    report: &read::Report,
    result: &CalibrationTaskResult,
    value: CasaValue,
) -> Result<TaskSuccess, TaskFailure> {
    let compact_kind = match result {
        CalibrationTaskResult::Apply(_) => Some("apply"),
        CalibrationTaskResult::ApplyMultiMs(_) => Some("apply_multi_ms"),
        _ => None,
    };
    if let Some(kind) = compact_kind
        && surface == "applycal"
        && report.output.is_none()
        && std::env::var_os("CASARS_APPLYCAL_VALUE_ONLY").is_some_and(|value| value == "1")
    {
        // The completion decoder needs the kind, and the CASA-signature
        // wrapper consumes only the value. Avoid serializing the row plan.
        crate::timing::report_process_memory();
        return TaskSuccess::new(value, &serde_json::json!({ "kind": kind }));
    }
    report_success(report, result, value)
}

fn report_success(
    report: &read::Report,
    result: &CalibrationTaskResult,
    value: CasaValue,
) -> Result<TaskSuccess, TaskFailure> {
    let rendered = render::render(report.format, result).map_err(TaskFailure::execution)?;
    let text = match report.output.as_deref() {
        Some(path) => {
            std::fs::write(path, rendered).map_err(|error| {
                TaskFailure::execution(format!("write {}: {error}", path.display()))
            })?;
            String::new()
        }
        None => rendered,
    };
    crate::timing::report_process_memory();
    Ok(TaskSuccess::new(value, result)?.with_text(text))
}

fn position_input_failure(error: crate::AntposInputError) -> TaskFailure {
    use crate::AntposInputError;

    let parameter = match &error {
        AntposInputError::Invalid(_) => {
            return TaskFailure::validation(error.to_string()).at("infile");
        }
        AntposInputError::Io { source, .. } => match source.kind() {
            std::io::ErrorKind::NotFound => "infile.missing",
            std::io::ErrorKind::PermissionDenied => "infile.permission",
            std::io::ErrorKind::IsADirectory => "infile.directory",
            _ => "infile.io",
        },
        AntposInputError::Json(_) => "infile.json",
        AntposInputError::Key(_) => "infile.key",
        AntposInputError::Index(_) => "infile.index",
        AntposInputError::Type(_) => "infile.type",
        AntposInputError::Attribute => "infile.attribute",
    };
    TaskFailure::execution(error.to_string()).at(parameter)
}

fn refuse_report_in_product(
    path: &Path,
    request: &CalibrationTaskRequest,
    task: &TaskInvocation<'_>,
) -> Result<(), TaskFailure> {
    refuse_report_in_products(path, scientific_paths(request)?, task)
}

fn refuse_report_in_products(
    path: &Path,
    mut products: Vec<std::path::PathBuf>,
    task: &TaskInvocation<'_>,
) -> Result<(), TaskFailure> {
    if task.surface() == "fluxscale"
        && let Some(vis) = task.text("vis")?
    {
        products.push(vis.into());
    }
    if matches!(task.surface(), "applycal" | "gaincal" | "bandpass")
        && task.flag("docallib")?
        && let Some(callib) = task.text("callib")?
    {
        products.push(callib.into());
    }
    for product in products {
        if crate::paths::overlap(path, &product)
            .map_err(|e| TaskFailure::validation(e.to_string()).at("output"))?
        {
            return Err(
                TaskFailure::validation("report path overlaps a scientific product").at("output"),
            );
        }
    }
    Ok(())
}

fn scientific_paths(
    request: &CalibrationTaskRequest,
) -> Result<Vec<std::path::PathBuf>, TaskFailure> {
    let (mut paths, chain) = match request {
        CalibrationTaskRequest::SolveGain(r)
        | CalibrationTaskRequest::SolveAccor(r)
        | CalibrationTaskRequest::SolveBaseline { gain: r, .. } => (
            vec![r.measurement_set.clone(), r.output_table.clone()],
            r.prior_calibration_tables.as_slice(),
        ),
        CalibrationTaskRequest::SolveBandpass(r) => (
            vec![r.measurement_set.clone(), r.output_table.clone()],
            r.prior_calibration_tables.as_slice(),
        ),
        CalibrationTaskRequest::ExecuteApply(r) => (
            vec![r.measurement_set.clone()],
            r.calibration_tables.as_slice(),
        ),
        CalibrationTaskRequest::PlanApply(r) => (
            vec![r.measurement_set.clone()],
            r.calibration_tables.as_slice(),
        ),
        CalibrationTaskRequest::Summary(r) => (r.paths.clone(), &[][..]),
        CalibrationTaskRequest::Stats(r) => (vec![r.path.clone()], &[][..]),
        CalibrationTaskRequest::FluxScale(r) => {
            let mut paths = vec![r.input_table.clone(), r.output_table.clone()];
            paths.extend(r.options.listfile.iter().cloned());
            (paths, &[][..])
        }
        CalibrationTaskRequest::ExportCorrectedData(r) => {
            (vec![r.input_ms.clone(), r.output_ms.clone()], &[][..])
        }
        CalibrationTaskRequest::Gencal(r) => {
            let mut paths = vec![
                r.prior.measurement_set.clone(),
                r.prior.output_table.clone(),
            ];
            paths.extend(
                crate::gencal::prior_inputs_for_report(&r.prior, r.infile())
                    .map_err(|error| TaskFailure::validation(error.to_string()).at("vis"))?,
            );
            (paths, &[][..])
        }
    };
    paths.extend(chain.iter().map(|table| table.path.clone()));
    Ok(paths)
}

/// What `surface` returns for `result`, as CASA's task of that name does;
/// `applied` is the chain a solve pre-applied.
fn casa_value(
    surface: &str,
    applied: &[ApplyCalibrationTableSpec],
    result: &CalibrationTaskResult,
) -> CasaValue {
    match (surface, result) {
        ("gaincal", CalibrationTaskResult::SolveGain(report))
            if report.gain_type == crate::GainType::GSpline =>
        {
            crate::casa_return::polynomial_solve_record(
                applied,
                &report.selected_data,
                &report.output_table,
            )
        }
        ("gaincal", CalibrationTaskResult::SolveGain(report)) => solve_record(
            applied,
            &report.selected_data,
            &report.output_table,
            &report.solve_stats,
        ),
        ("bandpass", CalibrationTaskResult::SolveBandpass(report))
            if report.table_subtype == "BPOLY" && report.solution_row_count > 0 =>
        {
            crate::casa_return::polynomial_solve_record(
                applied,
                &report.selected_data,
                &report.output_table,
            )
        }
        ("bandpass", CalibrationTaskResult::SolveBandpass(report))
            if report.solve_stats.windows.is_empty() =>
        {
            crate::casa_return::skipped_solve_record(
                if report.selection_failed {
                    &[]
                } else {
                    applied
                },
                (!report.selected_data.fields.is_empty()).then_some(&report.selected_data),
            )
        }
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
