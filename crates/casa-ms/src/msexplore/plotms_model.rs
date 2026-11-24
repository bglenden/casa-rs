// SPDX-License-Identifier: LGPL-3.0-or-later
//! Implicit model cells for MeasurementSet PlotMS.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::Array2;

use super::{
    MsAxis, MsDataColumn, MsPlotSpec, PlotMsColumnSelection,
    cli::PlotMsModelLoader,
    plotms_columns::{
        supports_ordinary_time_axes, uses_channel_derived, uses_ordinary_time_averaging,
    },
    selected_i32_values, uses_extended_averaging,
};
use crate::selection::ResolvedMsSelectionRow;
use crate::virtual_model::{VirtualModelEntry, selected_model_combinations, virtual_model};
use crate::{MeasurementSet, initial_model_visibility, scratch_columns::virtual_model_fields};

/// Supply read-only predicted cells before a supported averaging transform.
pub(super) fn selected_model_cells(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    plot: Option<&MsPlotSpec>,
    columns: &PlotMsColumnSelection<'_>,
    load_models: Option<PlotMsModelLoader>,
) -> Result<Option<HashMap<usize, ArrayValue>>, String> {
    let Some(plot) = plot else {
        return Ok(None);
    };
    let axes = [
        Some(plot.x_axis),
        Some(plot.y_axis()),
        plot.secondary_y_axis(),
    ];
    let picks = [
        columns.x.unwrap_or(plot.data_column),
        plot.data_column,
        columns.secondary_y.unwrap_or(plot.data_column),
    ];
    let mut visibility_columns = axes
        .into_iter()
        .zip(picks)
        .filter_map(|(axis, column)| {
            axis.is_some_and(MsAxis::is_visibility_math)
                .then_some(column)
        })
        .chain(
            columns
                .additional_y
                .iter()
                .filter_map(|&(axis, column)| axis.is_visibility_math().then_some(column)),
        );
    if !visibility_columns.clone().any(MsDataColumn::needs_model) {
        return Ok(None);
    }
    if columns.phasecenter.is_some() {
        let fields = virtual_model_fields(ms);
        if rows.iter().any(|row| fields.contains(&row.field_id)) {
            return Err("PlotMS phasecenter with a virtual model is pending (#1032)".into());
        }
    }
    let extended = uses_extended_averaging(plot);
    let averaging = extended || plot.averaging.avgchannel.is_some_and(|width| width > 1);
    let ordinary_time = uses_ordinary_time_averaging(plot) && supports_ordinary_time_axes(plot);
    let standard_columns = visibility_columns.all(|column| {
        matches!(
            column,
            MsDataColumn::Data | MsDataColumn::Corrected | MsDataColumn::Model
        )
    });
    let derived_average = (columns.extended_derived && !plot.averaging.avgfield)
        || (!extended && uses_channel_derived(plot, columns.x));
    let can_predict =
        !averaging || ((!extended || ordinary_time) && standard_columns) || derived_average;
    if let Some(load) = load_models.filter(|_| can_predict) {
        return load(ms, rows);
    }
    if averaging || (plot.iteration.iteraxis.is_some() && plot.secondary_y_axis().is_some()) {
        let message = if can_predict {
            "use casars.tasks.plotms to predict virtual MeasurementSet models"
        } else {
            "PlotMS virtual MeasurementSet models are pending #1032"
        };
        refuse_selected_virtual_fields(ms, rows, message)?;
    }
    Ok(None)
}

/// A selected virtual-model field overrides stored cells, including its
/// fallback model in windows without a recipe.
fn refuse_selected_virtual_fields(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    message: &str,
) -> Result<(), String> {
    for field in virtual_model_fields(ms) {
        if rows.iter().any(|row| row.field_id == field)
            && virtual_model(ms, field)
                .map_err(|error| error.to_string())?
                .is_some()
        {
            return Err(message.into());
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
