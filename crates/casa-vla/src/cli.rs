// SPDX-License-Identifier: LGPL-3.0-or-later
//! The disk-based `importvla` binary on the shared task front end (#931).
//!
//! The front end owns the command line, `--json-run`, Last and logging; the
//! callback here reads the resolved run into [`ImportVlaOptions`] and
//! imports.

use std::fs;
use std::path::{Path, PathBuf};

use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

use crate::task_contract::{ImportVlaTaskResult, importvla_task_schema_bundle};
use crate::{
    ImportReport, ImportVlaOptions, VlaError, import_archive_files_to_measurement_set_from_options,
};
pub use casa_ms::presentation::UiCommandSchema;

/// The front end of the `importvla` binary.
pub fn task_front_end()
-> TaskFrontEnd<impl Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>> {
    TaskFrontEnd::new(
        "importvla",
        importvla_task_schema_bundle(),
        &["importvla"],
        run_task,
    )
    .expect("the importvla contract carries its surface")
}

/// Build the machine-readable command schema for `importvla`.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    let bundle = casa_provider_contracts::builtin_surface_bundle("importvla")
        .expect("built-in importvla parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_form(&bundle))
            .expect("canonical importvla UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}

/// Import one resolved request; CASA's `importvla` returns None.
fn run_task(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let options = options_from(task)?;
    let report = run(&options).map_err(failure)?;
    let text = import_text(&report);
    Ok(TaskSuccess::new(&(), &ImportVlaTaskResult::Import(report))?.with_text(text))
}

/// The resolved run as [`ImportVlaOptions`]. Each `archivefiles` entry may
/// itself list files separated by commas, as CASA's string form does.
fn options_from(task: &TaskInvocation<'_>) -> Result<ImportVlaOptions, TaskFailure> {
    let archivefiles = task
        .paths("archivefiles")?
        .iter()
        .flat_map(|path| {
            path.to_string_lossy()
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(PathBuf::from)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
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
        frequencytol_hz: ImportVlaOptions::parse_frequencytol(&task.required_text("frequencytol")?)
            .map_err(failure)?,
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

fn run(options: &ImportVlaOptions) -> Result<ImportReport, VlaError> {
    tracing::info!(
        archive_count = options.archivefiles.len(),
        vis = ?options.vis,
        "importvla run started"
    );
    // A failed import removes the default output it started, never one that
    // existed before the run: that one is a product it refused to replace.
    let cleanup_vis = if options.vis.is_none() {
        Some(options.effective_vis_for_import()?).filter(|path| !path.exists())
    } else {
        None
    };
    let report = match import_archive_files_to_measurement_set_from_options(options) {
        Ok(report) => report,
        Err(error) => {
            tracing::error!(
                casa.priority = "SEVERE",
                error = %error,
                cleanup_vis = ?cleanup_vis,
                "importvla import failed"
            );
            if let Some(path) = cleanup_vis.as_deref().filter(|path| path.exists()) {
                cleanup_failed_import_output(path).map_err(|cleanup_error| {
                    VlaError::import(format!(
                        "{error}; cleanup failed for {}: {cleanup_error}",
                        path.display()
                    ))
                })?;
            }
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

fn cleanup_failed_import_output(path: &Path) -> Result<(), std::io::Error> {
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
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
