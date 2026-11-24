// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA PlotMS ordinate column selection.

use std::collections::HashSet;

use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::{ChannelBin, FloatGrid, MsAxis, MsDataColumn, MsPlotSpec, PlotMsInput};
use crate::MeasurementSet;
use crate::schema::main_table::OptionalMainColumn;
use crate::transform::{
    WeightKind, mean_weight_preferring_unflagged, sigma_from_weight, visibility_weight_kind,
    weight_from_sigma,
};

pub(super) fn uses_channel_ordinate_masks(
    x_axis: MsAxis,
    mut ordinates: impl Iterator<Item = MsAxis> + Clone,
) -> bool {
    x_axis == MsAxis::Channel
        && ordinates.clone().any(MsAxis::is_visibility_math)
        && ordinates.clone().any(|axis| !axis.is_visibility_math())
        && ordinates.all(|axis| {
            axis.is_visibility_math()
                || matches!(axis, MsAxis::U | MsAxis::V | MsAxis::W | MsAxis::Elevation)
        })
}

pub(super) fn uses_vector_baseline_average(spec: &MsPlotSpec) -> bool {
    spec.averaging.avgbaseline
        && !spec.averaging.avgscan
        && !spec.averaging.avgfield
        && !spec.averaging.avgantenna
        && !spec.averaging.avgspw
        && !spec.averaging.scalar
        && supports_reduced_visibility_axes(spec)
}

pub(super) fn uses_antenna_visibility_average(spec: &MsPlotSpec) -> bool {
    spec.averaging.avgantenna
        && !spec.averaging.avgbaseline
        && !spec.averaging.avgscan
        && !spec.averaging.avgfield
        && !spec.averaging.avgspw
        && supports_reduced_visibility_axes(spec)
}

pub(super) fn uses_weighted_time_uvw(spec: &MsPlotSpec) -> bool {
    spec.averaging.avgtime.is_some()
        && spec.averaging.avgchannel.is_none_or(|width| width <= 1)
        && !spec.averaging.scalar
        && !spec.averaging.avgscan
        && !spec.averaging.avgfield
        && !spec.averaging.avgbaseline
        && !spec.averaging.avgantenna
        && !spec.averaging.avgspw
        && ((spec.x_axis.uses_uvw_metres()
            && spec.y_axes.iter().copied().all(MsAxis::is_visibility_math))
            || (spec.x_axis.is_visibility_math()
                && spec.y_axes.len() == 1
                && spec.y_axis().uses_uvw_metres()))
}

pub(super) fn uses_scalar_visibility_average(spec: &MsPlotSpec) -> bool {
    spec.averaging.scalar
        && (spec.averaging.avgtime.is_some() || spec.averaging.avgbaseline)
        && !spec.averaging.avgantenna
        && !spec.averaging.avgspw
        && supports_reduced_visibility_axes(spec)
}

fn supports_reduced_visibility_axes(spec: &MsPlotSpec) -> bool {
    spec.y_axes.iter().copied().all(MsAxis::is_visibility_math)
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

pub(super) fn uses_channel_derived(spec: &MsPlotSpec, x: Option<MsDataColumn>) -> bool {
    spec.averaging.avgchannel.is_some_and(|width| width > 1)
        && [
            (spec.x_axis, x.unwrap_or(spec.data_column)),
            (spec.y_axis(), spec.data_column),
        ]
        .into_iter()
        .any(|(axis, column)| axis.is_visibility_math() && column.is_derived())
}

pub(super) fn check_averaged_derived_storage(
    ms: &MeasurementSet,
    columns: [MsDataColumn; 3],
    averaging_axis: &str,
) -> Result<(), TaskFailure> {
    let schema = ms.main_table().schema();
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
    for name in std::iter::once("MODEL_DATA").chain(operands) {
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

#[derive(Clone, Copy)]
pub(super) struct PlotMsWeightKinds {
    pub(super) axes: [WeightKind; 3],
    pub(super) model: Option<WeightKind>,
    pub(super) primary: WeightKind,
    pub(super) row_weight: WeightKind,
    pub(super) row_sigma: WeightKind,
}

impl PlotMsWeightKinds {
    pub(super) fn uses(self, kind: WeightKind) -> bool {
        self.primary == kind || self.model == Some(kind) || self.axes.contains(&kind)
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
    // A derived expression itself loads all columns, as does a mixed-column request.
    // A model uses corrected or observed weighting when present in that iterator.
    let first = active.next();
    let all = first.is_some_and(|first| first.is_derived() || active.any(|column| column != first));
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
            MsDataColumn::Data
            | MsDataColumn::DataMinusModel
            | MsDataColumn::DataMinusModelScalar
            | MsDataColumn::DataDivModel
            | MsDataColumn::DataDivModelScalar => OptionalMainColumn::Data,
            MsDataColumn::Corrected
            | MsDataColumn::CorrectedMinusModel
            | MsDataColumn::CorrectedMinusModelScalar
            | MsDataColumn::CorrectedDivModel
            | MsDataColumn::CorrectedDivModelScalar => OptionalMainColumn::CorrectedData,
            MsDataColumn::Model => OptionalMainColumn::ModelData,
            _ => return WeightKind::Unit,
        };
        visibility_weight_kind(column, has_observed, has_corrected, false)
    };
    PlotMsWeightKinds {
        axes: columns.map(kind),
        model: (input.extended_derived || uses_channel_derived(spec, input.x_data_column))
            .then(|| kind(MsDataColumn::Model)),
        primary: kind(primary),
        row_weight: if has_corrected {
            WeightKind::Calibrated
        } else {
            kind(primary)
        },
        row_sigma: if has_observed {
            WeightKind::Observed
        } else {
            kind(primary)
        },
    }
}

pub(super) struct ChannelStatistics {
    pub(super) weight: f64,
    pub(super) sigma: f64,
    spectra: Vec<(bool, f32, f32)>,
}

impl ChannelStatistics {
    pub(super) fn from_bins(
        correlation: usize,
        bins: &[ChannelBin],
        flags: &ndarray::ArrayView2<'_, bool>,
        weight: Option<&FloatGrid>,
        inverse_variance: Option<&FloatGrid>,
    ) -> Self {
        let sum = |grid: Option<&FloatGrid>, bin: &ChannelBin| {
            channel_bin_weight(grid, correlation, bin, flags, ChannelWeightMask::FlagsOnly)
        };
        let spectra: Vec<_> = bins
            .iter()
            .map(|bin| {
                let (flagged, weight) = sum(weight, bin);
                let variance_weight = sum(inverse_variance, bin).1;
                (flagged, weight, sigma_from_weight(variance_weight))
            })
            .collect();
        // The cache derives row statistics over the transformed spectra,
        // using channel flags before the visibility projection's weight mask.
        let weight = mean_weight_preferring_unflagged(
            spectra
                .iter()
                .map(|&(flagged, weight, _)| (flagged, weight)),
        );
        let sigma = mean_weight_preferring_unflagged(
            spectra.iter().map(|&(flagged, _, sigma)| (flagged, sigma)),
        );
        Self {
            weight: f64::from(weight),
            sigma: f64::from(sigma),
            spectra,
        }
    }

    pub(super) fn spectrum(&self, bin: usize) -> Option<(f64, f64)> {
        let (flagged, weight, sigma) = self.spectra[bin];
        (!flagged).then_some((f64::from(weight), f64::from(sigma)))
    }
}

/// The next reducer receives the transform's scalar weight, averaged
/// across the output channels of the first selected correlation.
pub(super) fn channel_averaged_row_weight(
    weights: Option<&FloatGrid>,
    correlation: usize,
    bins: &[ChannelBin],
    flags: &ndarray::ArrayView2<'_, bool>,
    mask: ChannelWeightMask,
) -> f64 {
    f64::from(mean_weight_preferring_unflagged(bins.iter().map(|bin| {
        channel_bin_weight(weights, correlation, bin, flags, mask)
    })))
}

#[derive(Clone, Copy)]
pub(super) enum ChannelWeightMask {
    FlagsOnly,
    Positive,
    Nonzero,
}

pub(super) fn channel_bin_weight(
    weights: Option<&FloatGrid>,
    correlation: usize,
    bin: &ChannelBin,
    flags: &ndarray::ArrayView2<'_, bool>,
    mask: ChannelWeightMask,
) -> (bool, f32) {
    sum_channel_weights(
        bin.channels().map(|channel| {
            let weight = weights.map_or(1.0, |grid| {
                grid.values[correlation * grid.chan_count + channel] as f32
            });
            (flags[(correlation, channel)], weight)
        }),
        mask,
    )
}

pub(super) fn sum_channel_weights(
    samples: impl Iterator<Item = (bool, f32)>,
    mask: ChannelWeightMask,
) -> (bool, f32) {
    let mut sums = [0.0, 0.0];
    let mut all_flagged = true;
    for (flagged, weight) in samples {
        all_flagged &= flagged;
        sums[usize::from(flagged)] += weight;
    }
    let sum = sums[usize::from(all_flagged)];
    let usable = !all_flagged
        && match mask {
            ChannelWeightMask::FlagsOnly => true,
            ChannelWeightMask::Positive => sum > 0.0,
            ChannelWeightMask::Nonzero => sum != 0.0 && !sum.is_nan(),
        };
    (!usable, sum)
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
        let uses_observed = matches!(column, Data | Float)
            || (column.is_derived() && !column.is_corrected_model_expression());
        if averaging && has_float && uses_observed {
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
