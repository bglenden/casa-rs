// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's terminal and file listings of calibration solution values.

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::CasaValue;
use casa_ms::MeasurementSet;
use casa_ms::observation_io::{ObservationError, calibration_subtables, is_msv4_store};

use casa_calibration::listing::{
    CalibrationListingError, CalibrationListingReport, list_calibration, list_calibration_named,
    publish_listing,
};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let path = task.input_path("caltable")?;
    let field = task.text("field")?.unwrap_or_default();
    let antenna = task.text("antenna")?.unwrap_or_default();
    let spw = task.text("spw")?.unwrap_or_default();
    // CASA's Python bridge passes a long to Calibrater's signed Int.
    let page_rows = task.integer("pagerows")? as i32;
    let listfile = task.text("listfile")?.filter(|path| !path.is_empty());
    let metadata_file = listfile.is_some()
        && page_rows == 0
        && field.is_empty()
        && antenna.is_empty()
        && matches!(spw.as_str(), "" | "*");
    let listing = if metadata_file {
        let ms = calibration_subtables(&vis).map_err(|error| match error {
            ObservationError::Unsupported { .. } => {
                TaskFailure::validation(error.to_string()).at("vis")
            }
            ObservationError::Storage(source) if !is_msv4_store(&vis) => {
                TaskFailure::execution(source.to_string()).at("vis")
            }
            _ => TaskFailure::execution(error.to_string()).at("vis"),
        })?;
        list_calibration_named(&ms, Some(&vis), &path, &field, &antenna, &spw, page_rows).map_err(
            |error| match error {
                CalibrationListingError::WindowIdentity(_) => {
                    TaskFailure::validation(error.to_string()).at("vis")
                }
                _ => TaskFailure::execution(error.to_string()),
            },
        )?
    } else {
        if is_msv4_store(&vis) {
            return Err(TaskFailure::validation(
                "MSv4 listcal requires default selectors, pagerows=0 and a listing file",
            )
            .at("vis"));
        }
        let ms = MeasurementSet::open(&vis)
            .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
        list_calibration(&ms, &path, &field, &antenna, &spw, page_rows)
            .map_err(TaskFailure::execution)?
    };
    let paged_reply = listfile.is_none()
        && page_rows > 0
        && task.native_options().map_or_else(
            || std::env::var_os("CASARS_LISTCAL_PAGED_REPLY").is_some(),
            |options| options.listcal_paged_reply,
        );
    if let Some(error) = &listing.error
        && (!paged_reply || listing.parts.is_empty())
    {
        return Err(TaskFailure::execution(error.clone()));
    }
    let text = listing.lines();
    if let Some(path) = listfile {
        publish_listing(&task.io_path(&path), &text)
            .map_err(|error| TaskFailure::execution(error).at("listfile"))?;
        let message = format!("Writing output to file: {path}\n");
        return Ok(TaskSuccess::new(
            CasaValue::None,
            &CalibrationListingReport {
                listing: None,
                listfile: Some(path),
            },
        )?
        .with_text(message));
    }
    Ok(TaskSuccess::new(
        CasaValue::None,
        &CalibrationListingReport {
            listing: Some(listing),
            listfile: None,
        },
    )?
    .with_text(text))
}
