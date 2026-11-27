// SPDX-License-Identifier: LGPL-3.0-or-later
//! `Calibrater::fluxscale` task `HISTORY` provenance adapter.

use crate::TaskInvocation;
use casa_calibration::fluxscale::history::append_messages;
use casa_calibration::fluxscale::{FluxScaleReport, FluxScaleRequest};
use casa_contracts::{ParameterValue, python_repr};

/// History failure is a warning after publication, as in the CASA task.
pub(crate) fn record(
    task: &TaskInvocation<'_>,
    request: &FluxScaleRequest,
    report: &FluxScaleReport,
) {
    let _phase = casa_calibration::timing::Phase::start("fluxscale_history");
    if let Err(error) = append_messages(request, messages(task, request, report), 2) {
        tracing::warn!("updating fluxscale HISTORY: {error}");
    }
}

fn messages(
    task: &TaskInvocation<'_>,
    request: &FluxScaleRequest,
    report: &FluxScaleReport,
) -> Vec<String> {
    let mut messages = casa_calibration::fluxscale::history::tool_messages(request);
    messages.extend([
        "taskname=fluxscale".into(),
        format!("version: casa-rs {}", env!("CARGO_PKG_VERSION")),
    ]);
    let mut bindings = task
        .session()
        .bundle()
        .surface
        .bindings()
        .iter()
        .filter(|binding| binding.order < 15)
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    for binding in bindings {
        // Record effective normalized values, as the task provider receives them.
        let value = task
            .session()
            .states()
            .get(&binding.name)
            .and_then(|state| state.value.as_ref());
        if let Some(value) = value {
            let text = if let ParameterValue::String(value) = value {
                format!("\"{value}\"")
            } else {
                python_repr(value)
            };
            messages.push(format!("{:<11} = {text}", binding.name));
        }
    }
    messages.push("Fluxscale results *****".into());
    for (id, field) in &report.fields {
        messages.push(format!("{}(field id={id}) ", field.field_name));
        let (mut last_base, mut last_unit) = (1., "Hz");
        for (index, spw) in report.spw_ids.iter().enumerate() {
            let frequency = report.frequencies_hz[index];
            let (base, unit) = if frequency > 1e9 {
                (1e9, "GHz")
            } else if frequency > 1e6 {
                (1e6, "MHz")
            } else if frequency > 1e3 {
                (1e3, "kHz")
            } else {
                (1., "Hz")
            };
            if frequency > 0. {
                last_base = base;
                last_unit = unit;
            }
            let result = &field.spw_results[spw];
            messages.push(if frequency < 0. {
                format!("  Spw {spw} insufficient data, flux density is not determined.")
            } else {
                format!(
                    "  Spw {spw}(freq = {:7.3}{unit}) Flux density = {:10.6}+/-{:10.6} Jy",
                    frequency / base,
                    result.fluxd[0],
                    result.fluxd_err[0]
                )
            });
        }
        if report.spw_ids.len() > 1 {
            let coefficients = |values: &[f64]| {
                format!(
                    "[{}]",
                    values
                        .iter()
                        .map(|value| format!("{value:5.3}"))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            };
            messages.push(format!(
                "  Spectral index fitting coefficients [zero-point, alpha, beta] with errors :{}+/-{}",
                coefficients(&field.spidx),
                coefficients(&field.spidx_err)
            ));
            messages.push(format!(
                "  Fitted flux density = {:10.6}+/-{:10.6} Jy (reference freq = {:7.3}{last_unit})",
                field.fit_fluxd,
                field.fit_fluxd_err,
                field.fit_ref_frequency_hz / last_base
            ));
        }
    }
    messages
}
