// SPDX-License-Identifier: LGPL-3.0-or-later
//! VisBufferImpl2::resetWeightsUsingSigma, before every calibration apply.

use casa_ms::MsError;
use casa_tables::Table;
use casa_types::ArrayValue;

use super::ApplyExecutionError;

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
