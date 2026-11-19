// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's terminal and file listings of calibration solution values.

use std::io::Write;
use std::path::Path;

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;
use tempfile::NamedTempFile;

use crate::listing::{CalibrationListing, list_calibration};

#[derive(Serialize)]
struct Report {
    listing: Option<CalibrationListing>,
    listfile: Option<String>,
}

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let path = task.input_path("caltable")?;
    let field = task.text("field")?.unwrap_or_default();
    let antenna = task.text("antenna")?.unwrap_or_default();
    let spw = task.text("spw")?.unwrap_or_default();
    // CASA's Python bridge passes a long to Calibrater's signed Int.
    let page_rows = task.integer("pagerows")? as i32;
    let ms = MeasurementSet::open(&vis)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    let listing = list_calibration(&ms, &path, &field, &antenna, &spw, page_rows)
        .map_err(TaskFailure::execution)?;
    let listfile = task.text("listfile")?.filter(|path| !path.is_empty());
    let paged_reply = listfile.is_none()
        && page_rows > 0
        && std::env::var_os("CASARS_LISTCAL_PAGED_REPLY").is_some();
    if let Some(error) = &listing.error
        && (!paged_reply || listing.parts.is_empty())
    {
        return Err(TaskFailure::execution(error.clone()));
    }
    let text = listing.lines();
    if let Some(path) = listfile {
        publish_listing(Path::new(&path), &text)?;
        let message = format!("Writing output to file: {path}\n");
        return Ok(TaskSuccess::new(
            CasaValue::None,
            &Report {
                listing: None,
                listfile: Some(path),
            },
        )?
        .with_text(message));
    }
    Ok(TaskSuccess::new(
        CasaValue::None,
        &Report {
            listing: Some(listing),
            listfile: None,
        },
    )?
    .with_text(text))
}

fn publish_listing(path: &Path, text: &str) -> Result<(), TaskFailure> {
    let fail = |error: std::io::Error| {
        TaskFailure::execution(format!(
            "could not create listing file {}: {error}",
            path.display()
        ))
        .at("listfile")
    };
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut stage = NamedTempFile::new_in(directory).map_err(fail)?;
    stage.write_all(text.as_bytes()).map_err(fail)?;
    stage
        .persist_noclobber(path)
        .map_err(|error| fail(error.error))?;
    Ok(())
}
