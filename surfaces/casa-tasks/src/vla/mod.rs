// SPDX-License-Identifier: LGPL-3.0-or-later
//! The disk-based `importvla` binary on the shared task front end (#931).
//!
//! The front end owns the command line, `--json-run`, Last and logging; the
//! callback here reads the resolved run into [`ImportVlaOptions`] and
//! imports.

use crate::{ParameterOrigin, TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};
use casa_contracts::{ParameterValue, python_repr};
use casa_ms::TransformTaskHistory;
use casa_vla::{ImportReport, ImportVlaOptions, VlaError, import_archive_with_history};

pub mod task_contract;
pub use task_contract::{
    IMPORTVLA_TASK_PROTOCOL_NAME, IMPORTVLA_TASK_PROTOCOL_VERSION, ImportVlaImportTaskRequest,
    ImportVlaScanTaskRequest, ImportVlaTaskRequest, ImportVlaTaskResult,
    importvla_protocol_descriptor, importvla_task_schema_bundle,
};

/// The front end of the `importvla` binary.
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new_with_deferred_schema(
        "importvla",
        importvla_task_schema_bundle(),
        &["importvla"],
        run_task,
    )
    .expect("the importvla contract carries its surface")
}

/// Import one resolved request; CASA's `importvla` returns None.
pub(crate) fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let options = options_from(task)?;
    let history = task_history(task, &options)?;
    let report = run(&options, Some(&history)).map_err(failure)?;
    let text = import_text(&report);
    Ok(
        TaskSuccess::new(crate::CasaValue::None, &ImportVlaTaskResult::Import(report))?
            .with_text(text),
    )
}

fn task_history(
    task: &TaskInvocation<'_>,
    options: &ImportVlaOptions,
) -> Result<TransformTaskHistory, TaskFailure> {
    let vis = options.effective_vis_for_import().map_err(failure)?;
    let paths = ParameterValue::Array(
        options
            .archivefiles
            .iter()
            .map(|path| ParameterValue::String(path.display().to_string()))
            .collect(),
    );
    let text = |name| {
        task.text(name)
            .map(|value| format!("{:?}", value.unwrap_or_default()))
    };
    let flag = |value| if value { "True".into() } else { "False".into() };
    Ok(TransformTaskHistory {
        task: "importvla".into(),
        parameters: [
            ("archivefiles", python_repr(&paths)),
            ("vis", format!("{:?}", vis.display().to_string())),
            ("bandname", text("bandname")?),
            ("frequencytol", python_repr(frequency_tolerance(task)?)),
            ("project", text("project")?),
            ("starttime", text("starttime")?),
            ("stoptime", text("stoptime")?),
            ("applytsys", flag(options.applytsys)),
            ("autocorr", flag(options.autocorr)),
            ("antnamescheme", text("antnamescheme")?),
            ("keepblanks", flag(options.keepblanks)),
            ("evlabands", flag(options.evlabands)),
        ]
        .into_iter()
        .map(|(name, value)| (name.into(), value))
        .collect(),
    })
}

fn frequency_tolerance<'a>(
    task: &'a TaskInvocation<'_>,
) -> Result<&'a ParameterValue, TaskFailure> {
    task.value("frequencytol")
        .ok_or_else(|| TaskFailure::validation("missing frequency tolerance").at("frequencytol"))
}

/// The resolved run as [`ImportVlaOptions`]. `archivefiles` arrives as the
/// list of archive paths: a comma-separated string is split before its paths
/// are normalized, on every route.
fn options_from(task: &TaskInvocation<'_>) -> Result<ImportVlaOptions, TaskFailure> {
    let archivefiles = task.paths("archivefiles")?;
    let mut vis = task.paths("vis")?;
    if vis.len() > 1 {
        return Err(TaskFailure::validation("importvla writes one vis").at("vis"));
    }
    let options = ImportVlaOptions {
        archivefiles,
        vis: vis.pop(),
        bandname: task
            .text("bandname")?
            .map(|text| text.parse())
            .transpose()
            .map_err(failure)?,
        frequencytol_hz: if task
            .session()
            .states()
            .get("frequencytol")
            .is_some_and(|state| state.origin == ParameterOrigin::Default)
        {
            // CASA's default adopts the first CDA channel width.
            0.0
        } else {
            match frequency_tolerance(task)? {
                ParameterValue::String(text) => {
                    ImportVlaOptions::parse_frequencytol(text).map_err(failure)?
                }
                ParameterValue::Integer(value) => *value as f64,
                ParameterValue::Float(value) => *value,
                _ => {
                    return Err(TaskFailure::validation("expected a frequency or quantity")
                        .at("frequencytol"));
                }
            }
        },
        project: task.text("project")?,
        starttime: task.text("starttime")?,
        stoptime: task.text("stoptime")?,
        applytsys: task.flag("applytsys")?,
        autocorr: task.flag("autocorr")?,
        antnamescheme: task
            .required_text("antnamescheme")?
            .parse()
            .map_err(failure)?,
        keepblanks: task.flag("keepblanks")?,
        evlabands: task.flag("evlabands")?,
    };
    options.require_archivefiles().map_err(failure)?;
    Ok(options)
}

/// A refused argument is a validation failure at that parameter; anything
/// else failed the import.
fn failure(error: VlaError) -> TaskFailure {
    match &error {
        VlaError::InvalidArgument { argument, .. } => {
            let argument = (*argument).to_string();
            TaskFailure::validation(error.to_string()).at(argument)
        }
        VlaError::NoArchiveFiles => TaskFailure::validation(error.to_string()).at("archivefiles"),
        _ => TaskFailure::execution(error.to_string()),
    }
}

fn run(
    options: &ImportVlaOptions,
    history: Option<&TransformTaskHistory>,
) -> Result<ImportReport, VlaError> {
    tracing::info!(
        archive_count = options.archivefiles.len(),
        vis = ?options.vis,
        "importvla run started"
    );
    let report = match import_archive_with_history(options, history) {
        Ok(report) => report,
        Err(error) => {
            tracing::error!(
                casa.priority = "SEVERE",
                error = %error,
                "importvla import failed"
            );
            return Err(error);
        }
    };
    tracing::info!(
        archive_count = options.archivefiles.len(),
        vis = ?report.vis,
        rows = report.main_rows_written,
        "importvla run completed"
    );
    Ok(report)
}

fn import_text(report: &ImportReport) -> String {
    format!(
        "importvla disk import\nvis: {}\nlogical records seen: {}\nlogical records imported: {}\nlogical records skipped: {}\nmain rows written: {}\n",
        report.vis.display(),
        report.logical_records_seen,
        report.logical_records_imported,
        report.logical_records_skipped,
        report.main_rows_written
    )
}

#[cfg(test)]
mod tests;
