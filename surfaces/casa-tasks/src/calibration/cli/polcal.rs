// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public polarization and joint cross-hand calibration tasks.

use std::collections::BTreeMap;

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_calibration::solve::grouping::leakage::{LeakageSolveKind, solve_polarization};
use casa_contracts::CasaValue;
use casa_ms::MeasurementSet;
use casa_types::Complex32;

use crate::calibration::managed_output::{
    CalibrationTaskResult, CrossHandGainSolveReport, CrossHandGainType,
};

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let vis = task.input_path("vis")?;
    let request = super::casa_read::leakage_request(task, &vis)?;
    if casa_calibration::paths::overlap(&vis, &request.output)
        .map_err(|error| TaskFailure::validation(error.to_string()).at("caltable"))?
    {
        return Err(
            TaskFailure::validation("output caltable overlaps the MeasurementSet").at("caltable"),
        );
    }
    let report = request
        .kind
        .gain_phase()
        .then(|| super::read::report(task))
        .transpose()?;
    if let Some(report) = &report
        && let Some(path) = report.output.as_deref()
    {
        super::refuse_existing_report(path, report.overwrite)?;
        let mut paths = vec![vis.clone(), request.output.clone()];
        paths.extend(request.priors.iter().map(|table| table.path.clone()));
        super::refuse_report_in_products(path, paths, task)?;
    }
    let ms = MeasurementSet::open(&vis)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("vis"))?;
    let outcome = solve_polarization(&ms, &request).map_err(TaskFailure::execution)?;
    if let Some((stats, report)) = outcome.gain_stats.zip(report) {
        let result = crate::calibration::casa_return::solve_record(
            &request.priors,
            &outcome.selected,
            &request.output,
            &stats,
        );
        let result_report = CrossHandGainSolveReport {
            output_table: request.output,
            gain_type: if request.kind == LeakageSolveKind::XY {
                CrossHandGainType::Scalar
            } else {
                CrossHandGainType::Channelized
            },
            field_ids: outcome.solved_fields.into_iter().collect(),
            spectral_window_ids: outcome.solved_windows.into_iter().collect(),
            solution_row_count: outcome.rows,
            selected_data: outcome.selected,
            solve_stats: stats,
        };
        return super::report_success(
            &report,
            &CalibrationTaskResult::SolveCrossHandGain(result_report),
            result,
        );
    }
    let result = if outcome.source.is_empty() {
        CasaValue::None
    } else {
        fractional_stokes_record(outcome.source)
    };
    TaskSuccess::new(
        result,
        &serde_json::json!({ "solution_rows": outcome.rows }),
    )
}

fn fractional_stokes_record(source: BTreeMap<String, BTreeMap<i32, Complex32>>) -> CasaValue {
    CasaValue::dict(
        source
            .into_iter()
            .map(|(field, windows)| {
                let mut average = Complex32::ZERO;
                let vector = |qu: Complex32| {
                    CasaValue::float64_vector(vec![1.0, f64::from(qu.re), f64::from(qu.im), 0.0])
                };
                let mut entries = windows
                    .iter()
                    .map(|(id, &qu)| {
                        average += qu;
                        (format!("Spw{id}"), vector(qu))
                    })
                    .collect::<Vec<_>>();
                average /= windows.len() as f32;
                entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                entries.push(("SpwAve".to_string(), vector(average)));
                (field, CasaValue::dict(entries))
            })
            .collect::<Vec<_>>(),
    )
}
