// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only PlotMS callibrary application and visibility model prediction.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crate::TaskFailure;
use casa_calibration::callib::{load_apply_specs_from_callib_at, parse_apply_specs_at};
use casa_calibration::execute::evaluate_plot_rows;
use casa_calibration::{ApplyMode, ApplyPlanRequest, plan_apply};
use casa_ms::{MeasurementSet, MsSelection};
use casa_plot_data::msexplore::{PlotMsCalibratedRow, PlotMsCalibration};

pub use casa_calibration::plotms::load_models;

/// Correct the selected plot rows through the applycal engine without saving them.
/// For channel-weighted ordinates, the calibration preview expands scalar sigma
/// to the stored weight spectrum grid when the MS has no sigma spectrum.
pub fn load_calibration(
    ms: &MeasurementSet,
    selection: &MsSelection,
    library: &[String],
    channel_weighting: bool,
    workspace: &Path,
) -> Result<PlotMsCalibration, TaskFailure> {
    let specs = match library {
        [filename] if !filename.contains('=') => {
            let path = workspace.join(filename);
            if !path.exists() {
                return Err(TaskFailure::validation("callib file does not exist").at("callib"));
            }
            load_apply_specs_from_callib_at(&path, workspace)
        }
        entries => parse_apply_specs_at("PlotMS callib", &entries.join(","), Some(workspace)),
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
    let rows = evaluate_plot_rows(ms, &plan, channel_weighting)
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
    let rows: HashMap<usize, PlotMsCalibratedRow> = rows
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
