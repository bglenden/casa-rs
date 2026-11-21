// SPDX-License-Identifier: LGPL-3.0-or-later

use std::mem::size_of;

use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};

use crate::derived::engine::MsCalEngine;
use crate::{MeasurementSet, MsReadPlan, MsSelectionIoBudget};

use super::FlaggingError;

pub(super) fn rows_outside_limits(
    ms: &MeasurementSet,
    selected: &[usize],
    [lower, upper]: [f64; 2],
    path: &str,
) -> Result<Vec<usize>, FlaggingError> {
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let fail = |reason: String| FlaggingError::MutateFlags {
        path: path.to_owned(),
        reason,
    };
    let engine = MsCalEngine::new(ms)
        .and_then(|engine| engine.with_field_ephemerides(ms))
        .map_err(|error| fail(error.to_string()))?;
    let bytes = size_of::<f64>() + 3 * size_of::<i32>() + size_of::<usize>();
    let budget = MsSelectionIoBudget::from_system_memory(2, bytes, None)
        .map_err(|error| fail(error.to_string()))?;
    let plan = MsReadPlan::new(selected.len(), budget).map_err(|error| fail(error.to_string()))?;
    let (mut times, mut fields, mut first, mut second) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut outside = Vec::new();
    for rows in selected.chunks(plan.rows_per_block.max(1)) {
        use RequiredScalarColumnDestination as Column;
        use RequiredScalarColumnValuesMut::{Float64, Int32};

        ms.main_table()
            .required_scalar_columns_for_rows_into(
                rows,
                &mut [
                    Column::new("TIME", Float64(&mut times)),
                    Column::new("FIELD_ID", Int32(&mut fields)),
                    Column::new("ANTENNA1", Int32(&mut first)),
                    Column::new("ANTENNA2", Int32(&mut second)),
                ],
            )
            .map_err(|error| fail(error.to_string()))?;
        for (index, &row) in rows.iter().enumerate() {
            let time = times[index];
            if !time.is_finite() {
                return Err(fail(format!("TIME row {row} must be finite")));
            }
            let field = usize::try_from(fields[index])
                .map_err(|_| fail(format!("negative FIELD_ID at row {row}")))?;
            let elevation = |antenna| {
                let antenna = usize::try_from(antenna)
                    .map_err(|_| fail(format!("negative antenna ID at row {row}")))?;
                engine
                    .azel(time, field, antenna)
                    .map(|(_, elevation)| elevation.to_degrees())
                    .map_err(|error| fail(error.to_string()))
            };
            let left = elevation(first[index])?;
            let right = elevation(second[index])?;
            if left < lower || right < lower || left > upper || right > upper {
                outside.push(row);
            }
        }
    }
    Ok(outside)
}
