// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA PlotMS ordinate column selection.

use std::collections::HashSet;

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::{ChannelBin, FloatGrid, MsAxis, MsDataColumn, MsPlotSpec, PlotMsInput};
use crate::MeasurementSet;
use crate::schema::main_table::OptionalMainColumn;
use crate::transform::{
    WeightKind, mean_weight_preferring_unflagged, visibility_weight_kind, weight_from_sigma,
};

pub(super) fn uses_vector_baseline_average(spec: &MsPlotSpec) -> bool {
    spec.averaging.avgbaseline
        && (spec.averaging.avgtime.is_none()
            || spec.averaging.avgchannel.is_none_or(|width| width <= 1))
        && !spec.averaging.avgscan
        && !spec.averaging.avgfield
        && !spec.averaging.avgantenna
        && !spec.averaging.avgspw
        && !spec.averaging.scalar
        && spec.y_axes.iter().copied().all(MsAxis::is_visibility_math)
        && (spec.x_axis.is_visibility_math()
            || matches!(spec.x_axis, MsAxis::Time | MsAxis::Channel)
            || (spec.x_axis == MsAxis::Frequency && spec.transforms.freqframe.is_none()))
}

pub(super) fn baseline_weight(weight: f64) -> f64 {
    let minimum = f64::from(f32::MIN_POSITIVE);
    if weight < minimum { minimum } else { weight }
}

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

#[derive(Clone, Copy)]
pub(super) struct PlotMsWeightKinds {
    pub(super) axes: [WeightKind; 3],
    pub(super) primary: WeightKind,
    pub(super) row_weight: WeightKind,
}

impl PlotMsWeightKinds {
    pub(super) fn uses(self, kind: WeightKind) -> bool {
        self.primary == kind || self.axes.contains(&kind)
    }
}

pub(super) fn weight_kinds(
    ms: &MeasurementSet,
    spec: &MsPlotSpec,
    input: &PlotMsInput<'_>,
) -> PlotMsWeightKinds {
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
    let first = active.next();
    let all = first.is_some_and(|first| active.any(|column| column != first));
    let has_observed = all && has("DATA");
    let has_corrected = all && has("CORRECTED_DATA");
    let primary = if has_observed {
        MsDataColumn::Data
    } else if has_corrected {
        MsDataColumn::Corrected
    } else {
        first.unwrap_or(primary)
    };
    let kind = |column| {
        let column = match column {
            MsDataColumn::Data => OptionalMainColumn::Data,
            MsDataColumn::Corrected => OptionalMainColumn::CorrectedData,
            MsDataColumn::Model => OptionalMainColumn::ModelData,
            _ => return WeightKind::Unit,
        };
        visibility_weight_kind(column, has_observed, has_corrected, false)
    };
    PlotMsWeightKinds {
        axes: columns.map(kind),
        primary: kind(primary),
        row_weight: if has_corrected {
            WeightKind::Calibrated
        } else {
            kind(primary)
        },
    }
}

/// The baseline reducer receives the transform's scalar weight, averaged
/// across the output channels of the first selected correlation.
pub(super) fn channel_averaged_row_weight(
    weights: Option<&FloatGrid>,
    correlation: usize,
    bins: &[ChannelBin],
    flags: &ndarray::ArrayView2<'_, bool>,
) -> f64 {
    f64::from(mean_weight_preferring_unflagged(bins.iter().map(|bin| {
        let mut sums = [0.0, 0.0];
        let mut all_flagged = true;
        for channel in bin.channels() {
            let flagged = flags[(correlation, channel)];
            all_flagged &= flagged;
            sums[usize::from(flagged)] += weights.map_or(1.0, |grid| {
                grid.values[correlation * grid.chan_count + channel] as f32
            });
        }
        (all_flagged, sums[usize::from(all_flagged)])
    })))
}

pub(super) fn bin_has_weight(
    spectrum: &FloatGrid,
    corr: usize,
    bin: &ChannelBin,
    flags: &ndarray::ArrayView2<'_, bool>,
) -> bool {
    bin_weight(spectrum, corr, bin, flags) > 0.0
}

pub(super) fn bin_weight(
    spectrum: &FloatGrid,
    corr: usize,
    bin: &ChannelBin,
    flags: &ndarray::ArrayView2<'_, bool>,
) -> f64 {
    bin.channels()
        .filter(|&channel| !flags[(corr, channel)])
        .map(|channel| spectrum.values[corr * spectrum.chan_count + channel])
        .sum()
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
