// SPDX-License-Identifier: LGPL-3.0-or-later
//! Place diagonal prior calibration on the correct side of a delay solve.

use super::{
    ApplyExecutionError, ApplyPlan, ArrayValue, Complex32, EvaluatedApplyRow, HashMap,
    MeasurementSet, evaluate_apply_rows, for_each_apply_row,
};

pub(crate) fn evaluate_delay_priors(
    ms: &MeasurementSet,
    mut plan: ApplyPlan,
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    // VisEquation::collapse2 corrects DATA with earlier terms and corrupts
    // MODEL_DATA with terms at or after K. This distinction matters even for
    // diagonal terms because CASA leaves data over a zero model undivided.
    let (data_tables, mut model_tables): (Vec<_>, Vec<_>) =
        std::mem::take(&mut plan.calibration_tables)
            .into_iter()
            .partition(|table| {
                matches!(
                    table.summary.table_subtype.as_str(),
                    "B Jones" | "BPOLY" | "KAntPos Jones"
                )
            });
    plan.calibration_tables = data_tables;
    let mut rows = evaluate_apply_rows(ms, &plan)?;
    if model_tables.is_empty() {
        return Ok(rows);
    }
    for table in &mut model_tables {
        table.calwt = false;
    }
    plan.calibration_tables = model_tables;
    plan.parang = false;
    for_each_apply_row(ms, &plan, true, |index, row| {
        let data = rows.get_mut(&index).expect("same selection for both sides");
        let ArrayValue::Bool(data_flags) = &mut data.flags else {
            unreachable!("apply flags are boolean")
        };
        let ArrayValue::Bool(model_flags) = row.flags else {
            unreachable!("model calibration flags are boolean")
        };
        data_flags.zip_mut_with(&model_flags, |data, &model| *data |= model);
        let ArrayValue::Complex32(mut inverse) = row.corrected_data else {
            unreachable!("unit visibilities are complex")
        };
        // Correction of a unit visibility yields the inverse diagonal
        // factor. Forward application to the model uses its reciprocal.
        inverse.mapv_inplace(|value| Complex32::new(1., 0.) / value);
        data.model_gains = Some(inverse);
    })?;
    Ok(rows)
}
