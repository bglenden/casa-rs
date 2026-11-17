// SPDX-License-Identifier: LGPL-3.0-or-later
//! Traditional setapply selects calibration rows, then merges fields in time.

use super::*;

pub(super) fn load(
    plan: &ApplyCalibrationTablePlan,
    engine: Option<&Arc<MsCalEngine>>,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    let mut table = load_calibration_table(plan, engine)?;
    if matches!(plan.spec.gainfield, Some(crate::GainFieldSelector::Nearest)) {
        return Ok(table);
    }
    if !plan.resolved_gainfields.is_empty() {
        table.solutions.retain(|(field, _, _), _| {
            plan.resolved_gainfields
                .iter()
                .any(|selected| selected.field_id == *field)
        });
    }
    // CTPatchedInterp uses one field time series except for its explicit
    // nearest/map modes. Reuse the same merge used by simulator application.
    table.solutions = crate::lookup::across_fields(&table)
        .into_iter()
        .map(|((window, antenna), series)| ((-1, window, antenna), series))
        .collect();
    Ok(table)
}
