// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only application of PlotMS callibrary inputs.

use std::collections::BTreeMap;
use std::path::Path;

use casa_ms::msexplore::{PlotMsCalibratedRow, PlotMsCalibration};
use casa_ms::{MeasurementSet, MsSelection};
use casa_task_runtime::TaskFailure;

use crate::callib::{load_apply_specs_from_callib, parse_apply_specs};
use crate::execute::evaluate_plot_rows;
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
    let rows = evaluate_plot_rows(ms, &plan)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("callib"))?;
    let row_order = plan
        .selected_rows
        .into_iter()
        .map(|row| row.row_index)
        .collect();
    let mut channels = BTreeMap::<_, Vec<_>>::new();
    if let Some(spw) = selection.spw.as_deref().filter(|_| selection.selectdata) {
        let windows = ms
            .resolve_spw_selector(spw)
            .map_err(|error| TaskFailure::validation(error.to_string()).at("spw"))?;
        for &range in windows.channel_list() {
            channels.entry(range.spw_id as i32).or_default().push(range);
        }
    }
    let rows = rows
        .into_iter()
        .map(|(index, row)| {
            let weights = row.plot_weights.expect("PlotMS rows have weights");
            (
                index,
                PlotMsCalibratedRow {
                    corrected_data: row.corrected_data,
                    flags: row.flags,
                    weight: weights.scalar,
                    weight_spectrum: weights.spectrum,
                },
            )
        })
        .collect();
    Ok(PlotMsCalibration {
        row_order,
        rows,
        channels,
        x_data_column: None,
    })
}
