// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA PlotMS CLI column validation and storage resolution.

use std::collections::HashSet;

use casa_contracts::ParameterValue;
use casa_ms::MeasurementSet;
pub use casa_plot_data::msexplore::plotms_columns::{
    supports_ordinary_time_axes, uses_antenna_visibility_average, uses_channel_derived,
    uses_channel_ordinate_masks, uses_ordinary_time_averaging, uses_scalar_visibility_average,
    uses_spectral_window_average, uses_vector_baseline_average,
};
use casa_plot_data::msexplore::{MsAxis, MsDataColumn};
use casa_task_runtime::{TaskFailure, TaskInvocation};

pub(super) fn check_averaged_derived_storage(
    ms: &MeasurementSet,
    columns: [MsDataColumn; 3],
    averaging_axis: &str,
    implicit_model: bool,
) -> Result<(), TaskFailure> {
    let schema = ms.main_table().schema();
    // The visibility reader supplies either a selected virtual model or the
    // default point source in supported plots. FLOAT_DATA requires a stored model.
    let stored_model =
        !implicit_model || schema.is_some_and(|schema| schema.contains_column("FLOAT_DATA"));
    let operands = columns
        .into_iter()
        .filter(|column| column.is_derived())
        .map(|column| {
            if column.is_corrected_model_expression() {
                "CORRECTED_DATA"
            } else {
                "DATA"
            }
        });
    for name in stored_model
        .then_some("MODEL_DATA")
        .into_iter()
        .chain(operands)
    {
        if !schema.is_some_and(|schema| schema.contains_column(name)) {
            return Err(TaskFailure::validation(format!(
                "{averaging_axis}-averaged residuals and ratios require stored {name} (#1032)"
            )));
        }
    }
    Ok(())
}

pub(super) fn weight_axis_columns(
    axes: [Option<MsAxis>; 3],
    columns: [MsDataColumn; 3],
) -> [Option<MsDataColumn>; 3] {
    std::array::from_fn(|index| {
        matches!(
            axes[index],
            Some(MsAxis::Weight | MsAxis::WeightSpectrum | MsAxis::Sigma | MsAxis::SigmaSpectrum)
        )
        .then_some(columns[index])
    })
}

pub(super) fn check_averaging(
    averaging: bool,
    extended_averaging_with_independent_ordinates: bool,
    supports_derived: bool,
    columns: [MsDataColumn; 3],
) -> Result<(), TaskFailure> {
    if extended_averaging_with_independent_ordinates {
        return Err(TaskFailure::validation(
            "time, baseline, antenna and SPW averaging of independent MeasurementSet ordinate columns is pending #1032",
        ));
    }
    if averaging && !supports_derived && columns.into_iter().any(MsDataColumn::is_derived) {
        return Err(TaskFailure::validation(
            "averaging derived data columns in this MeasurementSet plot is pending #1032",
        ));
    }
    Ok(())
}

pub(super) fn ordinate_columns(
    task: &TaskInvocation<'_>,
    ordinates: &[MsAxis],
) -> Result<Vec<MsDataColumn>, TaskFailure> {
    let supplied = task.value("ydatacolumn");
    let names = super::calibration::axis_names(supplied)
        .map_err(TaskFailure::validation)?
        .into_iter()
        .map(canonical_column)
        .collect::<Result<Vec<_>, _>>()?;
    let multiple_axes = matches!(task.value("yaxis"), Some(ParameterValue::Array(_)));
    let multiple_columns = matches!(supplied, Some(ParameterValue::Array(_)));
    // CASA validates all supplied columns, then applies them only to a matching axis form.
    let names = if multiple_axes == multiple_columns {
        names.as_slice()
    } else {
        &[]
    };
    let mut seen = HashSet::with_capacity(ordinates.len());
    ordinates
        .iter()
        .enumerate()
        .map(|(index, &axis)| {
            let name = names.get(index).copied().unwrap_or("data");
            let column = MsDataColumn::parse(name).map_err(TaskFailure::validation)?;
            if !seen.insert((axis, column)) {
                return Err(TaskFailure::validation("Remove duplicate y-axes."));
            }
            Ok(column)
        })
        .collect()
}

fn canonical_column(name: &str) -> Result<&str, TaskFailure> {
    match name {
        "data"
        | "corrected"
        | "model"
        | "float"
        | "data-model_vector"
        | "data-model_scalar"
        | "corrected-model_vector"
        | "corrected-model_scalar"
        | "data/model_vector"
        | "data/model_scalar"
        | "corrected/model_vector"
        | "corrected/model_scalar" => Ok(name),
        "residual" | "corrected-model" => Ok("corrected-model_vector"),
        "data-model" => Ok("data-model_vector"),
        "data/model" => Ok("data/model_vector"),
        "corrected/model" => Ok("corrected/model_vector"),
        _ => Err(TaskFailure::validation(format!(
            "invalid ydatacolumn value '{name}'"
        ))),
    }
}

pub(super) fn resolve_storage_columns<const N: usize>(
    ms: &MeasurementSet,
    requested: [MsDataColumn; N],
    axes: [Option<MsAxis>; N],
    averaging: bool,
    allow_float_average: bool,
) -> Result<([MsDataColumn; N], bool), TaskFailure> {
    use MsDataColumn::*;

    let schema = ms.main_table().schema();
    let has = |name| schema.is_some_and(|schema| schema.column(name).is_some());
    let has_data = has("DATA");
    let has_float = has("FLOAT_DATA");
    let has_corrected = has("CORRECTED_DATA");
    let observed = if !has_data && has_float { Float } else { Data };
    let columns = requested.map(|column| match column {
        Data => observed,
        Corrected if !has_corrected && (has_data || has_float) => observed,
        CorrectedMinusModel if !has_corrected && has_data => DataMinusModel,
        CorrectedMinusModelScalar if !has_corrected && has_data => DataMinusModelScalar,
        CorrectedDivModel if !has_corrected && has_data => DataDivModel,
        CorrectedDivModelScalar if !has_corrected && has_data => DataDivModelScalar,
        other => other,
    });
    for (axis, column) in axes.into_iter().zip(columns) {
        if !axis.is_some_and(MsAxis::is_visibility_math) {
            continue;
        }
        if has_float && !has_data && matches!(column, DataMinusModel | DataMinusModelScalar) {
            return Err(TaskFailure::validation(
                "Data residuals not valid; use FLOAT_DATA.",
            ));
        }
        if column == Float {
            if !has_float {
                return Err(TaskFailure::validation(
                    "FLOAT_DATA is unavailable. Use DATA for PlotMS.",
                ));
            }
            if matches!(axis, Some(MsAxis::Phase | MsAxis::Imaginary)) {
                return Err(TaskFailure::validation(
                    "PlotMS requires an amplitude or real axis for FLOAT_DATA.",
                ));
            }
        }
        let uses_observed = matches!(column, Data | Float)
            || (column.is_derived() && !column.is_corrected_model_expression());
        if averaging && has_float && uses_observed && (!allow_float_average || column.is_derived())
        {
            return Err(TaskFailure::validation(
                "averaging MeasurementSet FLOAT_DATA is pending #1032",
            ));
        }
    }
    if columns != requested {
        tracing::warn!(
            observed = observed.as_str(),
            "Using available observed columns for PlotMS."
        );
    }
    Ok((columns, has_float && (!averaging || allow_float_average)))
}
