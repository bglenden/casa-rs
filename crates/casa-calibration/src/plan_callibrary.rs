// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve CLPatchPanel data slices against the globally selected MS.
use super::*;
use std::collections::HashMap;

pub(super) fn selected_rows(
    ms: &MeasurementSet,
    spec: &ApplyCalibrationTableSpec,
    selected: &[ApplyRowPlan],
) -> Result<Option<Vec<usize>>, ApplyPlanError> {
    let Some(library) = spec
        .callibrary
        .as_ref()
        .filter(|library| library.selection != MsSelection::new())
    else {
        return Ok(None);
    };
    let resolved = resolve_calibration_selection(ms, &library.selection).map_err(|error| {
        ApplyPlanError::Selection {
            source: MsError::InvalidInput(error.to_string()),
        }
    })?;
    let mut rows = resolved.row_indices().collect::<Vec<_>>();
    rows.sort_unstable();
    rows.dedup();
    if library.selection.intent.is_some() || library.selection.observation.is_some() {
        // Calibrater::correct2 sorts ARRAY/SCAN/FIELD/DDID/TIME. VisCal::syncMeta2
        // uses the first row's STATE_ID and observation for the entire buffer.
        let indices = selected.iter().map(|row| row.row_index).collect::<Vec<_>>();
        let arrays = load_i32_column_rows(ms.main_table(), "ARRAY_ID", &indices)?;
        let mut groups = HashMap::new();
        let mut grouped = Vec::new();
        for (index, row) in selected.iter().enumerate() {
            let key = (
                value_at_i32(&arrays, index, "ARRAY_ID")?,
                row.scan_number,
                row.field_id,
                row.data_desc_id,
                row.time_seconds.to_bits(),
            );
            let applies = *groups
                .entry(key)
                .or_insert_with(|| rows.binary_search(&row.row_index).is_ok());
            if applies {
                grouped.push(row.row_index);
            }
        }
        rows = grouped;
        rows.sort_unstable();
    }
    Ok(Some(rows))
}
