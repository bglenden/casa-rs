// SPDX-License-Identifier: LGPL-3.0-or-later
//! VisBufferImpl2::resetWeightsUsingSigma, before every calibration apply.

use casa_ms::MsError;
use casa_tables::Table;
use casa_types::ArrayValue;

use crate::model::{CalibrationParameterFamily, CalibrationTableSummary};

use super::{
    ApplyExecutionError, GainGrid, PlotWeights, correcting_gain, correlation_receptors, stokes_name,
};
use ndarray::{Array1, Array2, ArrayD, Axis};

pub(super) fn supports_calwt(
    summary: &CalibrationTableSummary,
    mixes_correlations: bool,
    cross_hand_phase: bool,
) -> bool {
    let subtype = summary.table_subtype.as_str();
    if super::baseline::is_subtype(subtype) || subtype == "X Mueller" {
        return false;
    }
    (summary.parameter_family == CalibrationParameterFamily::Complex
        && !mixes_correlations
        && !cross_hand_phase
        && !matches!(subtype, "GlinXph Jones" | "GlinXphf Jones"))
        || matches!(
            subtype,
            "TOpac" | "TfOpac" | "EGainCurve" | "EPowerCurve" | "B TSYS" | "G EVLASWPOW"
        )
        || crate::single_dish::is_sky_subtype(subtype)
}

/// Calibrater uses partialMedians without takeEvenMean: an even channel count
/// takes the lower central value, not the average of the two central values.
pub(super) fn lower_median(mut values: Vec<f32>) -> f32 {
    assert!(
        !values.is_empty(),
        "weight median requires at least one channel"
    );
    let middle = (values.len() - 1) / 2;
    *values.select_nth_unstable_by(middle, f32::total_cmp).1
}

pub(super) fn from_sigma(
    mut value: ArrayValue,
    column: &str,
) -> Result<ArrayValue, ApplyExecutionError> {
    let ArrayValue::Float32(values) = &mut value else {
        return Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("<measurement-set {column}>"),
            shape: value.shape().to_vec(),
        });
    };
    values.mapv_inplace(|sigma| if sigma > 0. { 1. / (sigma * sigma) } else { 0. });
    Ok(value)
}

pub(super) fn broadcast_spectrum(
    scalar: &ArrayValue,
    shape: &[usize],
) -> Result<ArrayValue, ApplyExecutionError> {
    match (scalar, shape) {
        (ArrayValue::Float32(values), [correlations, channels])
            if values.shape() == [*correlations] =>
        {
            Ok(ArrayValue::Float32(
                Array2::from_shape_fn((*correlations, *channels), |(correlation, _)| {
                    values[[correlation]]
                })
                .into_dyn(),
            ))
        }
        _ => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: "<measurement-set weight spectrum>".into(),
            shape: shape.to_vec(),
        }),
    }
}

pub(super) fn scalar_medians(
    spectrum: &ArrayValue,
    channels: Option<&[usize]>,
) -> Option<ArrayValue> {
    let ArrayValue::Float32(values) = spectrum else {
        return None;
    };
    if values.ndim() != 2 {
        return None;
    }
    let medians = values
        .axis_iter(Axis(0))
        .map(|row| {
            let selected = match channels {
                Some(channels) => channels
                    .iter()
                    .map(|&channel| row.get([channel]).copied())
                    .collect::<Option<Vec<_>>>()?,
                None => row.iter().copied().collect::<Vec<_>>(),
            };
            (!selected.is_empty()).then(|| lower_median(selected))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(ArrayValue::Float32(Array1::from_vec(medians).into_dyn()))
}

pub(super) fn has_spectrum(
    table: &Table,
    row: Option<usize>,
    column: &str,
) -> Result<bool, ApplyExecutionError> {
    if !table
        .schema()
        .is_some_and(|schema| schema.contains_column(column))
    {
        return Ok(false);
    }
    let Some(row) = row else { return Ok(false) };
    table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array_shape())
        .map(|shape| shape.is_some_and(|shape| shape.iter().all(|length| *length > 0)))
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: format!("<measurement-set {column}>"),
            source: MsError::from(source),
        })
}

#[derive(Clone, Copy)]
pub(super) struct PlotWeightColumns {
    scalar: usize,
    spectrum: Option<usize>,
    expand_sigma: bool,
}

pub(super) fn plot_weight_columns(
    table: &Table,
    first_row: Option<usize>,
    columns: &mut Vec<&str>,
    channel_weighting: bool,
) -> Result<PlotWeightColumns, ApplyExecutionError> {
    let scalar = columns.len();
    columns.push("SIGMA");
    let spectrum = if has_spectrum(table, first_row, "SIGMA_SPECTRUM")? {
        let slot = columns.len();
        columns.push("SIGMA_SPECTRUM");
        Some(slot)
    } else {
        None
    };
    let expand_sigma = channel_weighting
        && spectrum.is_none()
        && has_spectrum(table, first_row, "WEIGHT_SPECTRUM")?;
    Ok(PlotWeightColumns {
        scalar,
        spectrum,
        expand_sigma,
    })
}

impl PlotWeightColumns {
    pub(super) fn has_spectrum(self) -> bool {
        self.spectrum.is_some() || self.expand_sigma
    }

    pub(super) fn read<'a>(
        self,
        read_array: impl Fn(usize) -> Result<&'a ArrayValue, ApplyExecutionError>,
        shape: &[usize],
    ) -> Result<PlotWeights, ApplyExecutionError> {
        let read = |slot, name| from_sigma(read_array(slot)?.clone(), name);
        let scalar = read(self.scalar, "SIGMA")?;
        let spectrum = if self.expand_sigma {
            Some(broadcast_spectrum(&scalar, shape)?)
        } else {
            self.spectrum
                .map(|slot| read(slot, "SIGMA_SPECTRUM"))
                .transpose()?
        };
        Ok(PlotWeights { scalar, spectrum })
    }
}

/// Single-channel diagonal gains have one weight factor per correlation.
/// Apply it over the spectrum without sampling the antenna gains per bin.
/// EPowerCurve uses zero weight for a noninvertible receptor, even though
/// its Jones correction still applies any usable gain from the other antenna.
pub(super) fn apply_scalar_pair(
    first: &GainGrid,
    second: &GainGrid,
    correlations: &[i32],
    data_desc_id: i32,
    zero_weight_for_invalid_gain: bool,
    mut weights: Option<&mut ArrayD<f32>>,
    mut implicit_weights: Option<&mut ArrayD<f32>>,
) -> Result<(), ApplyExecutionError> {
    for (correlation, &code) in correlations.iter().enumerate() {
        let (left, right) = correlation_receptors(code).ok_or_else(|| {
            ApplyExecutionError::UnsupportedCorrelationLayout {
                data_desc_id,
                correlation_types: correlations
                    .iter()
                    .map(|&code| stokes_name(code).to_string())
                    .collect(),
            }
        })?;
        let left = left.min(first.receptor_count.saturating_sub(1));
        let right = right.min(second.receptor_count.saturating_sub(1));
        let (left, left_invalid) = correcting_gain(first.value_at(left, 0), first.flag_at(left, 0));
        let (right, right_invalid) =
            correcting_gain(second.value_at(right, 0), second.flag_at(right, 0));
        let factor = if zero_weight_for_invalid_gain && (left_invalid || right_invalid) {
            0.0
        } else {
            left.norm_sqr() * right.norm_sqr()
        };
        for spectrum in [weights.as_deref_mut(), implicit_weights.as_deref_mut()]
            .into_iter()
            .flatten()
        {
            spectrum
                .index_axis_mut(Axis(0), correlation)
                .mapv_inplace(|weight| weight * factor);
        }
    }
    Ok(())
}
