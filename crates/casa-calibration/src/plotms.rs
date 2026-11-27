// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read-only PlotMS callibrary application and visibility model prediction.

use std::collections::{BTreeMap, HashMap};

use casa_core::ArrayValue;
use casa_ms::{MeasurementSet, ResolvedMsSelectionRow};
use casa_mstransform::{PlotMsModelReferences, TransformInputView};

use crate::transform_models::plotms_model_input;

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
