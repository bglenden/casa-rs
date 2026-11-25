// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scalar noise convention used by the legacy SubMS channel averager.

use casa_tables::TableError;
use casa_types::ArrayValue;

use crate::schema::main_table::OptionalMainColumn;

use super::visibility_columns::VisibilityColumns;

/// Scale each correlation by the width of the first bin. A shorter final
/// bin changes spectral weights but has no separate row WEIGHT or SIGMA.
pub(super) fn scale_channel_noise(
    columns: &VisibilityColumns,
    width: usize,
    weight: &mut ArrayValue,
    sigma: &mut ArrayValue,
) -> Result<(), TableError> {
    use OptionalMainColumn::{CorrectedData, Data, FloatData, ModelData};

    let contains = |name| columns.copies().iter().any(|column| column.source == name);
    let observed = contains(Data);
    let corrected = contains(CorrectedData);
    let model = contains(ModelData);
    let floating = contains(FloatData);
    let (ArrayValue::Float32(weight), ArrayValue::Float32(sigma)) = (weight, sigma) else {
        return Err(TableError::Schema(
            "WEIGHT and SIGMA must hold Float32".into(),
        ));
    };
    let width = width as f64;
    let root_width = width.sqrt();
    for (weight, sigma) in weight.iter_mut().zip(sigma) {
        if observed || floating {
            *sigma = (f64::from(*sigma) / root_width) as f32;
            if !corrected {
                *weight = if *sigma > 0.0 {
                    (1.0 / f64::from(*sigma).powi(2)) as f32
                } else {
                    f32::EPSILON
                };
            }
        }
        if corrected || model {
            *weight = (f64::from(*weight) * width) as f32;
            if !observed {
                *sigma = if *weight > 0.0 {
                    1.0 / weight.sqrt()
                } else {
                    f32::MAX
                };
            }
        }
    }
    Ok(())
}
