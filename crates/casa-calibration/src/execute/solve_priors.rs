// SPDX-License-Identifier: LGPL-3.0-or-later
//! Place diagonal prior calibration on the correct side of a solve.

use super::{
    ApplyExecutionError, ApplyPlan, ArrayValue, EvaluatedApplyRow, HashMap, MeasurementSet,
    PreviewWeightSource, evaluate_apply_rows, for_each_apply_row,
};

pub(crate) enum PriorSolveBoundary {
    Bandpass,
    Delay,
}

pub(crate) fn evaluate_solve_priors(
    ms: &MeasurementSet,
    mut plan: ApplyPlan,
    boundary: PriorSolveBoundary,
    share_input_flags: bool,
) -> Result<HashMap<usize, EvaluatedApplyRow>, ApplyExecutionError> {
    // VisEquation::collapse2 corrects DATA with earlier terms and corrupts
    // MODEL_DATA with terms at or after K. This distinction matters even for
    // diagonal terms because CASA leaves data over a zero model undivided.
    let (data_tables, mut model_tables): (Vec<_>, Vec<_>) =
        std::mem::take(&mut plan.calibration_tables)
            .into_iter()
            .partition(|table| match table.summary.table_subtype.as_str() {
                "KAntPos Jones" => true,
                "B Jones" | "BPOLY" => matches!(boundary, PriorSolveBoundary::Delay),
                _ => false,
            });
    plan.calibration_tables = data_tables;
    let mut rows = evaluate_apply_rows(ms, &plan, share_input_flags)?;
    if model_tables.is_empty() {
        return Ok(rows);
    }
    for table in &mut model_tables {
        table.calwt = false;
    }
    plan.calibration_tables = model_tables;
    plan.parang = false;
    for_each_apply_row(
        ms,
        &plan,
        true,
        share_input_flags,
        PreviewWeightSource::UnitFactors,
        |index, row| {
            let data = rows.get_mut(&index).expect("same selection for both sides");
            let ArrayValue::Bool(data_flags) = &mut data.flags else {
                unreachable!("apply flags are boolean")
            };
            let ArrayValue::Bool(model_flags) = row.flags else {
                unreachable!("model calibration flags are boolean")
            };
            data_flags.zip_mut_with(&model_flags, |data, &model| *data |= model);
            let ArrayValue::Complex32(gains) = row.corrected_data else {
                unreachable!("unit visibilities are complex")
            };
            data.model_gains = Some(gains);
        },
    )?;
    Ok(rows)
}
