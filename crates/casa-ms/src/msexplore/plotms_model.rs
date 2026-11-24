// SPDX-License-Identifier: LGPL-3.0-or-later
//! Implicit model cells for MeasurementSet PlotMS.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Array2;

use super::selected_i32_values;
use crate::selection::ResolvedMsSelectionRow;
use crate::virtual_model::{VirtualModelEntry, selected_model_combinations, virtual_model};
use crate::{MeasurementSet, initial_model_visibility, scratch_columns::virtual_model_fields};

/// A selected virtual-model field overrides stored cells, including its
/// fallback model in windows without a recipe.
pub(super) fn refuse_averaged_virtual_fields(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
) -> Result<(), String> {
    for field in virtual_model_fields(ms) {
        if rows.iter().any(|row| row.field_id == field)
            && virtual_model(ms, field)
                .map_err(|error| error.to_string())?
                .is_some()
        {
            return Err("PlotMS virtual MeasurementSet models are pending #1032".into());
        }
    }
    Ok(())
}

/// Share one unit point model cell per selected data description.
pub(super) fn default_cells(
    ms: &MeasurementSet,
    rows: &[usize],
) -> Result<Vec<Arc<ArrayValue>>, String> {
    refuse_matching_virtual_model(ms, rows)?;
    let main = ms.main_table();
    let descriptions = ms.data_description().map_err(|error| error.to_string())?;
    let spectral_window = ms.spectral_window().map_err(|error| error.to_string())?;
    let polarization = ms.polarization().map_err(|error| error.to_string())?;
    let index =
        |value: i32| usize::try_from(value).map_err(|_| format!("invalid MS index {value}"));
    let mut cells = HashMap::<i32, Arc<ArrayValue>>::new();
    selected_i32_values(main, "DATA_DESC_ID", rows)?
        .into_iter()
        .map(|ddid| {
            if let Some(cell) = cells.get(&ddid) {
                return Ok(Arc::clone(cell));
            }
            let description = index(ddid)?;
            let spw_id = descriptions
                .spectral_window_id(description)
                .map_err(|error| error.to_string())?;
            let pol_id = descriptions
                .polarization_id(description)
                .map_err(|error| error.to_string())?;
            let channels = spectral_window
                .num_chan(index(spw_id)?)
                .map_err(|error| error.to_string())?;
            let correlations = polarization
                .corr_type(index(pol_id)?)
                .map_err(|error| error.to_string())?;
            let values =
                Array2::from_shape_fn((correlations.len(), index(channels)?), |(corr, _)| {
                    initial_model_visibility(correlations[corr])
                });
            let cell = Arc::new(ArrayValue::Complex32(values.into_dyn()));
            cells.insert(ddid, Arc::clone(&cell));
            Ok(cell)
        })
        .collect()
}

fn refuse_matching_virtual_model(ms: &MeasurementSet, rows: &[usize]) -> Result<(), String> {
    let fields = virtual_model_fields(ms);
    if fields.is_empty() {
        return Ok(());
    }
    let selected = selected_model_combinations(ms, rows).map_err(|error| error.to_string())?;
    for field in fields {
        let combinations = selected
            .index_combination
            .iter()
            .copied()
            .filter(|combination| combination[0] == field)
            .collect::<BTreeSet<_>>();
        if combinations.is_empty() {
            continue;
        }
        let Some(model) = virtual_model(ms, field).map_err(|error| error.to_string())? else {
            continue;
        };
        let matches_selection = if matches!(
            model.record.get("version"),
            Some(Value::Scalar(ScalarValue::Int32(2)))
        ) {
            VirtualModelEntry::entries(&model.record)
                .map_err(|error| error.to_string())?
                .into_iter()
                .flat_map(|entry| entry.index_combination)
                .any(|combination| combinations.contains(&combination))
        } else {
            matches_legacy_model(&model.record, &combinations)?
        };
        if matches_selection {
            return Err("PlotMS virtual MeasurementSet models are pending #1032".into());
        }
    }
    Ok(())
}

fn matches_legacy_model(
    record: &RecordValue,
    combinations: &BTreeSet<[i32; 4]>,
) -> Result<bool, String> {
    for (count_name, prefix) in [("numft", "ft"), ("numcl", "cl")] {
        let Some(Value::Scalar(ScalarValue::Int32(count))) = record.get(count_name) else {
            continue;
        };
        for index in 0..*count {
            let name = format!("{prefix}_{index}");
            let Some(Value::Record(entry)) = record.get(&name) else {
                return Err(format!("legacy virtual model has no {name} record"));
            };
            let ids = |name| match entry.get(name) {
                Some(Value::Array(ArrayValue::Int32(values))) if values.ndim() == 1 => Ok(values),
                _ => Err(format!("legacy virtual model has no {name} vector")),
            };
            let fields = ids("fields")?;
            let windows = ids("spws")?;
            if combinations.iter().any(|[field, window, _, _]| {
                fields.iter().any(|value| value == field)
                    && windows.iter().any(|value| value == window)
            }) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
