// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only PlotMS callibrary application and visibility model prediction.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crate::TaskFailure;
use casa_calibration::apply::evaluate_plot_rows;
use casa_calibration::apply::{ApplyMode, ApplyPlanRequest, plan_apply};
use casa_calibration::callib::{load_apply_specs_from_callib_at, parse_apply_specs_at};
use casa_ms::{MeasurementSet, MsSelection, ResolvedMsSelectionRow};
use casa_plot_data::msexplore::{PlotMsCalibratedRow, PlotMsCalibration};

use casa_calibration::models::plotms_model_input;
use casa_core::ArrayValue;
use casa_mstransform::{PlotMsModelReferences, TransformInputView};

/// Predict model visibilities for the exact PlotMS row selection without
/// writing MODEL_DATA. Return None when no selected field has a virtual model
/// pointer, so PlotMS can use its physical or default model reader.
/// When supplied, `references` identifies the input buffer recipe and chunk
/// phase reference for each selected row.
pub fn load_models(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    references: Option<PlotMsModelReferences<'_>>,
) -> Result<Option<HashMap<usize, ArrayValue>>, String> {
    let fields = casa_ms::virtual_model_fields(ms);
    if !rows.iter().any(|row| fields.contains(&row.field_id)) {
        return Ok(None);
    }
    let indices = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let prepared = plotms_model_input(references)
        .prepare(ms, rows, &BTreeMap::new())
        .map_err(|error| error.to_string())?;
    let mut columns = prepared
        .read(ms.main_table(), &indices, &["MODEL_DATA"])
        .map_err(|error| error.to_string())?;
    let cells = columns
        .remove("MODEL_DATA")
        .ok_or_else(|| "virtual models did not supply MODEL_DATA".to_string())?;
    Ok(Some(
        indices
            .into_iter()
            .zip(cells)
            .filter_map(|(row, cell)| cell.map(|cell| (row, cell)))
            .collect(),
    ))
}

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
