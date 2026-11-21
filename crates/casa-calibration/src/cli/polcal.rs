// SPDX-License-Identifier: LGPL-3.0-or-later
//! The public polarization calibration task.

use casa_ms::MeasurementSet;
use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::solve::grouping::leakage::solve_polarization;

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let request = super::casa_read::leakage_request(task, &vis)?;
    if crate::paths::overlap(&vis, &request.output)
        .map_err(|error| TaskFailure::validation(error.to_string()).at("caltable"))?
    {
        return Err(
            TaskFailure::validation("output caltable overlaps the MeasurementSet").at("caltable"),
        );
    }
    let ms = MeasurementSet::open(&vis)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    let outcome = solve_polarization(&ms, &request).map_err(TaskFailure::execution)?;
    let result = if outcome.source.is_empty() {
        CasaValue::None
    } else {
        CasaValue::dict(outcome.source.into_iter().map(|(field, windows)| {
            let mut average = num_complex::Complex32::ZERO;
            let vector = |qu: num_complex::Complex32| {
                CasaValue::float64_vector([1.0, f64::from(qu.re), f64::from(qu.im), 0.0])
            };
            let mut entries = windows
                .iter()
                .map(|(id, &qu)| {
                    average += qu;
                    (format!("Spw{id}"), vector(qu))
                })
                .collect::<Vec<_>>();
            average /= windows.len() as f32;
            entries.push(("SpwAve".into(), vector(average)));
            (field, CasaValue::dict(entries))
        }))
    };
    TaskSuccess::new(
        result,
        &serde_json::json!({ "solution_rows": outcome.rows }),
    )
}
