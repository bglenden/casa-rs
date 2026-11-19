// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only application of PlotMS callibrary inputs.

use std::path::Path;

use casa_ms::msexplore::{PlotMsCalibratedRow, PlotMsCalibration};
use casa_ms::{MeasurementSet, MsSelection};
use casa_task_runtime::TaskFailure;

use crate::callib::{load_apply_specs_from_callib, parse_apply_specs};
use crate::execute::evaluate_apply_rows;
use crate::{ApplyMode, ApplyPlanRequest, plan_apply};

/// Correct the selected plot rows through the applycal engine without saving them.
pub fn load_calibration(
    ms: &MeasurementSet,
    selection: &MsSelection,
    library: &[String],
) -> Result<PlotMsCalibration, TaskFailure> {
    let specs = match library {
        [filename] if !filename.contains('=') => {
            let path = Path::new(filename);
            if !path.exists() {
                return Err(TaskFailure::validation("callib file does not exist").at("callib"));
            }
            load_apply_specs_from_callib(path)
        }
        entries => parse_apply_specs("PlotMS callib", &entries.join(",")),
    }
    .map_err(|error| TaskFailure::validation(error.to_string()).at("callib"))?;
    let request = ApplyPlanRequest {
        selection: selection.clone(),
        apply_mode: ApplyMode::CalFlag,
        parang: false,
        calibration_tables: specs,
    };
    let plan = plan_apply(ms, &request)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("callib"))?;
    let rows = evaluate_apply_rows(ms, &plan, false)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("callib"))?;
    let row_order = plan
        .selected_rows
        .into_iter()
        .map(|row| row.row_index)
        .collect();
    let channels = plan
        .channel_selections
        .into_iter()
        .map(|(spw, selection)| (spw, selection.indices))
        .collect();
    let rows = rows
        .into_iter()
        .map(|(index, row)| {
            (
                index,
                PlotMsCalibratedRow {
                    corrected_data: row.corrected_data,
                    flags: row.flags,
                },
            )
        })
        .collect();
    Ok(PlotMsCalibration {
        row_order,
        rows,
        channels,
    })
}
