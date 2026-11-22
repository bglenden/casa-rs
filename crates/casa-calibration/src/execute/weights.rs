// SPDX-License-Identifier: LGPL-3.0-or-later
//! VisBufferImpl2::resetWeightsUsingSigma, before every calibration apply.

use casa_ms::MsError;
use casa_tables::Table;
use casa_types::ArrayValue;

use crate::model::{CalibrationParameterFamily, CalibrationTableSummary};

use super::{ApplyExecutionError, GainGrid, correcting_gain, correlation_receptors, stokes_name};
use ndarray::{ArrayD, Axis};

pub(super) fn supports_calwt(
    summary: &CalibrationTableSummary,
    is_leakage: bool,
    cross_hand_phase: bool,
) -> bool {
    let subtype = summary.table_subtype.as_str();
    if super::baseline::is_subtype(subtype) || subtype == "X Mueller" {
        return false;
    }
    (summary.parameter_family == CalibrationParameterFamily::Complex
        && !is_leakage
        && !cross_hand_phase
        && !matches!(subtype, "GlinXph Jones" | "GlinXphf Jones"))
        || matches!(
            subtype,
            "TOpac" | "EGainCurve" | "EPowerCurve" | "B TSYS" | "G EVLASWPOW"
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
