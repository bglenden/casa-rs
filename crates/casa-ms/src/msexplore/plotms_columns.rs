// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA PlotMS ordinate column selection.

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::{FloatGrid, MsAxis, MsDataColumn, MsPlotSpec, PlotMsInput};
use crate::MeasurementSet;
use crate::schema::main_table::OptionalMainColumn;
use crate::transform::{WeightKind, visibility_weight_kind, weight_from_sigma};

pub(super) fn uses_model(axes: [Option<MsAxis>; 3], columns: [MsDataColumn; 3]) -> bool {
    axes.into_iter()
        .zip(columns)
        .any(|(axis, column)| axis.is_some_and(MsAxis::is_visibility_math) && column.needs_model())
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
    columns: [MsDataColumn; 3],
) -> Result<(), TaskFailure> {
    if extended_averaging_with_independent_ordinates {
        return Err(TaskFailure::validation(
            "time, baseline, antenna and SPW averaging of independent MeasurementSet ordinate columns is pending #1032",
        ));
    }
    if averaging && columns.into_iter().any(MsDataColumn::is_derived) {
        return Err(TaskFailure::validation(
            "averaging derived data columns in this MeasurementSet plot is pending #1032",
        ));
    }
    Ok(())
}

pub(super) fn channel_weight_kinds(
    ms: &MeasurementSet,
    spec: &MsPlotSpec,
    input: &PlotMsInput<'_>,
) -> [WeightKind; 3] {
    let primary = spec.data_column;
    let columns = [
        input.x_data_column.unwrap_or(primary),
        primary,
        input.secondary_y_data_column.unwrap_or(primary),
    ];
    let axes = [
        Some(spec.x_axis),
        Some(spec.y_axis()),
        spec.secondary_y_axis(),
    ];
    let schema = ms.main_table().schema();
    let has = |name| schema.is_some_and(|schema| schema.contains_column(name));
    let mut active = axes
        .into_iter()
        .zip(columns)
        .zip(input.weight_axis_columns)
        .filter_map(|((axis, column), weight_column)| {
            let weight_column = match weight_column {
                Some(MsDataColumn::Corrected) if !has("CORRECTED_DATA") => Some(MsDataColumn::Data),
                weight_column => weight_column,
            };
            weight_column.or_else(|| {
                axis.is_some_and(MsAxis::is_visibility_math)
                    .then_some(column)
            })
        });
    // PlotMS configures the transform to load all columns for a mixed-column
    // request, so a model uses the corrected or observed weighting when present.
    let all = active
        .next()
        .is_some_and(|first| active.any(|column| column != first));
    columns.map(|column| {
        let column = match column {
            MsDataColumn::Data => OptionalMainColumn::Data,
            MsDataColumn::Corrected => OptionalMainColumn::CorrectedData,
            MsDataColumn::Model => OptionalMainColumn::ModelData,
            _ => return WeightKind::Unit,
        };
        visibility_weight_kind(
            column,
            all && has("DATA"),
            all && has("CORRECTED_DATA"),
            false,
        )
    })
}

pub(super) fn inverse_variance(spectrum: &FloatGrid) -> FloatGrid {
    FloatGrid {
        corr_count: spectrum.corr_count,
        chan_count: spectrum.chan_count,
        values: spectrum
            .values
            .iter()
            .copied()
            .map(|sigma| f64::from(weight_from_sigma(sigma as f32)))
            .collect(),
    }
}

pub(super) fn ordinate_columns(
    task: &TaskInvocation<'_>,
    y_axis: MsAxis,
    secondary_axis: Option<MsAxis>,
) -> Result<[MsDataColumn; 2], TaskFailure> {
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
    let name = |index| names.get(index).copied().unwrap_or("data");
    if secondary_axis == Some(y_axis) && name(0) == name(1) {
        return Err(TaskFailure::validation("Remove duplicate y-axes."));
    }
    let column = |index| MsDataColumn::parse(name(index)).map_err(TaskFailure::validation);
    Ok([column(0)?, column(1)?])
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

pub(super) fn resolve_storage_columns(
    ms: &MeasurementSet,
    requested: [MsDataColumn; 3],
    axes: [Option<MsAxis>; 3],
    averaging: bool,
) -> Result<([MsDataColumn; 3], bool), TaskFailure> {
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
        if averaging && has_float && matches!(column, Data | Float) {
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
    Ok((columns, has_float && !averaging))
}
