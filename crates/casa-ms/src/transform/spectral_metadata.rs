// SPDX-License-Identifier: LGPL-3.0-or-later
//! SPECTRAL_WINDOW metadata for channel selection without averaging.

use std::collections::BTreeMap;
use std::path::Path;

use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Array1;

use crate::SubtableId;

use super::{MsTransformError, TransformChannelSelection, all_channels_selected, f64_vector_cell};

pub(super) fn remap_associations(
    mut row: RecordValue,
    spw_map: &BTreeMap<i32, i32>,
) -> RecordValue {
    let Some(Value::Array(ArrayValue::Int32(ids))) = row.get("ASSOC_SPW_ID") else {
        return row;
    };
    let selected = ids
        .iter()
        .enumerate()
        .filter_map(|(index, id)| spw_map.get(id).map(|&id| (index, id)))
        .collect::<Vec<_>>();
    if let Some(Value::Array(ArrayValue::String(nature))) = row.get("ASSOC_NATURE") {
        let nature = nature.iter().collect::<Vec<_>>();
        let values = selected
            .iter()
            .map(|&(index, _)| {
                nature
                    .get(index)
                    .map_or_else(String::new, |&value| value.clone())
            })
            .collect();
        row.upsert(
            "ASSOC_NATURE",
            Value::Array(ArrayValue::from_string_vec(values)),
        );
    }
    row.upsert(
        "ASSOC_SPW_ID",
        Value::Array(ArrayValue::from_i32_vec(
            selected.into_iter().map(|(_, id)| id).collect(),
        )),
    );
    row
}

pub(super) fn write_channel_selection(
    output_ms: &Path,
    compact_ids: &BTreeMap<i32, i32>,
    selections: &BTreeMap<i32, TransformChannelSelection>,
) -> Result<(), MsTransformError> {
    let path = output_ms.join(SubtableId::SpectralWindow.name());
    let error = |source: casa_tables::TableError| MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut table = Table::open(TableOptions::new(&path)).map_err(error)?;
    let mut changed = false;
    for (old_id, &new_id) in compact_ids {
        let row = new_id as usize;
        let selection = &selections[old_id];
        let freq = f64_vector_cell(&table, row, "CHAN_FREQ", &path)?;
        if all_channels_selected(&selection.channels, freq.len()) {
            continue;
        }
        let widths = f64_vector_cell(&table, row, "CHAN_WIDTH", &path)?;
        let resolution = f64_vector_cell(&table, row, "RESOLUTION", &path)?;
        let (centres, widths, resolution) =
            selected_windows(selection, &freq, &widths, &resolution).ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: path.display().to_string(),
                    reason: format!("SPW {old_id} channel metadata does not cover the selection"),
                }
            })?;
        let reference = centres[0].min(centres[centres.len() - 1]);
        let total = widths.iter().map(|width| width.abs()).sum();
        for (name, values) in [
            ("CHAN_FREQ", centres),
            ("CHAN_WIDTH", widths),
            ("EFFECTIVE_BW", resolution.clone()),
            ("RESOLUTION", resolution),
        ] {
            let value = Value::Array(ArrayValue::Float64(Array1::from_vec(values).into_dyn()));
            table
                .column_accessor_mut(name)
                .and_then(|mut column| column.set(row, value))
                .map_err(error)?;
        }
        for (name, value) in [
            (
                "NUM_CHAN",
                ScalarValue::Int32(selection.channels.len() as i32),
            ),
            ("REF_FREQUENCY", ScalarValue::Float64(reference)),
            ("TOTAL_BANDWIDTH", ScalarValue::Float64(total)),
        ] {
            table
                .column_accessor_mut(name)
                .and_then(|mut column| column.set(row, Value::Scalar(value)))
                .map_err(error)?;
        }
        changed = true;
    }
    if changed {
        table
            .prepare_write()
            .save(TableOptions::new(&path))
            .map_err(error)?;
    }
    Ok(())
}

fn selected_windows(
    selection: &TransformChannelSelection,
    freq: &[f64],
    widths: &[f64],
    resolution: &[f64],
) -> Option<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    let sign = if freq.last()? < freq.first()? {
        -1.0
    } else {
        1.0
    };
    let mut centre = Vec::with_capacity(selection.channels.len());
    let mut width = Vec::with_capacity(selection.channels.len());
    let mut res = Vec::with_capacity(selection.channels.len());
    // MSTransformDataHandler::fillSPWTable uses the stride's span for the
    // metadata even though the visibility iterator copies just its first sample.
    for (&first, &last) in selection.channels.iter().zip(&selection.metadata_ends) {
        let separation = (freq.get(last)? - freq.get(first)?) * sign;
        centre.push((freq[first] + freq[last]) * 0.5);
        width.push((separation + (widths.get(first)? + widths.get(last)?).abs() * 0.5) * sign);
        res.push(separation + (resolution.get(first)? + resolution.get(last)?) * 0.5);
    }
    Some((centre, width, res))
}
