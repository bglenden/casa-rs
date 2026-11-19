// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's terminal and file listings of calibration solution values.

use std::fs::OpenOptions;
use std::io::Write;

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;

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
    let page_rows = task.integer("pagerows")?;
    let ms = MeasurementSet::open(&vis)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    let listing = list_calibration(&ms, &path, &field, &antenna, &spw, page_rows)
        .map_err(TaskFailure::execution)?;
    let text = listing.lines();
    let listfile = task.text("listfile")?.filter(|path| !path.is_empty());
    if let Some(path) = listfile {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                TaskFailure::execution(format!("could not create listing file {path:?}: {error}"))
                    .at("listfile")
            })?;
        file.write_all(text.as_bytes())
            .map_err(|error| TaskFailure::execution(error.to_string()).at("listfile"))?;
        let message = format!("Writing output to file: {path}");
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
