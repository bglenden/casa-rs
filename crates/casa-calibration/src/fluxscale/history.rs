// SPDX-License-Identifier: LGPL-3.0-or-later
//! `Calibrater::fluxscale`, `write_history` and `writeResultsHistory` provenance.

use std::path::Path;

use casa_contracts::{ParameterValue, python_repr};
use casa_tables::{Table, TableOptions};
use casa_task_runtime::TaskInvocation;
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

use super::{FluxScaleError, FluxScaleReport, FluxScaleRequest};

/// History failure is a warning after publication, as in the CASA task.
pub(crate) fn record(
    task: &TaskInvocation<'_>,
    request: &FluxScaleRequest,
    report: &FluxScaleReport,
) {
    let _phase = crate::timing::Phase::start("fluxscale_history");
    if let Err(error) = append(task, request, report) {
        tracing::warn!("updating fluxscale HISTORY: {error}");
    }
}

fn append(
    task: &TaskInvocation<'_>,
    request: &FluxScaleRequest,
    report: &FluxScaleReport,
) -> Result<(), Box<dyn std::error::Error>> {
    append_messages(request, messages(task, request, report), 2)
}

pub(super) fn record_failure(request: &FluxScaleRequest, caltype: &str) {
    let _phase = crate::timing::Phase::start("fluxscale_history");
    if let Err(error) = append_messages(request, vec![type_message(request, caltype)], 1) {
        tracing::warn!("updating fluxscale HISTORY: {error}");
    }
}

pub(super) fn record_tool_type(
    path: &Path,
    request: &FluxScaleRequest,
    caltype: &str,
) -> Result<(), FluxScaleError> {
    record_tool_message(path, type_message(request, caltype))
}

pub(super) fn record_tool_output(
    path: &Path,
    request: &FluxScaleRequest,
) -> Result<(), FluxScaleError> {
    record_tool_message(path, output_message(request))
}

fn record_tool_message(path: &Path, message: String) -> Result<(), FluxScaleError> {
    let _phase = crate::timing::Phase::start("fluxscale_history");
    append_to_path(path, vec![message], 1).map_err(|e| FluxScaleError::History(e.to_string()))
}

fn append_messages(
    request: &FluxScaleRequest,
    messages: Vec<String>,
    calibrater_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(vis) = &request.options.measurement_set else {
        return Ok(());
    };
    append_to_path(&vis.join("HISTORY"), messages, calibrater_count)
}

fn append_to_path(
    path: &Path,
    messages: Vec<String>,
    calibrater_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    let mut table = Table::open_with_lock(
        TableOptions::new(path),
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait),
    )?;
    #[cfg(not(unix))]
    let mut table = Table::open(TableOptions::new(path))?;
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs_f64()
        + 40_587. * 86_400.;
    for (index, message) in messages.into_iter().enumerate() {
        let calibrater = index < calibrater_count;
        let mut row = RecordValue::new(Vec::new());
        for (column, text) in [
            ("APPLICATION", if calibrater { "calibrater" } else { "ms" }),
            (
                "ORIGIN",
                if calibrater {
                    "calibrater::fluxscale()"
                } else {
                    "fluxscale"
                },
            ),
            ("PRIORITY", "INFO"),
            ("MESSAGE", message.as_str()),
        ] {
            row.upsert(column, Value::Scalar(ScalarValue::String(text.into())));
        }
        row.upsert("TIME", Value::Scalar(ScalarValue::Float64(time)));
        row.upsert("OBJECT_ID", Value::Scalar(ScalarValue::Int32(0)));
        row.upsert("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(-1)));
        for column in ["APP_PARAMS", "CLI_COMMAND"] {
            row.upsert(
                column,
                Value::Array(ArrayValue::from_string_vec(vec![String::new()])),
            );
        }
        table.add_row(row)?;
    }
    table.save(TableOptions::new(path))?;
    Ok(())
}

fn tool_messages(request: &FluxScaleRequest) -> Vec<String> {
    let caltype = Table::open(TableOptions::new(&request.input_table))
        .ok()
        .map(|table| table.info().sub_type.clone())
        .unwrap_or_default();
    vec![type_message(request, &caltype), output_message(request)]
}

fn type_message(request: &FluxScaleRequest, caltype: &str) -> String {
    format!(
        "Table {} is of type: {caltype}",
        request.input_table.display()
    )
}

fn output_message(request: &FluxScaleRequest) -> String {
    let action = if request.append {
        "Appending result to"
    } else {
        "Storing result in"
    };
    format!("{action} {}", request.output_table.display())
}

fn messages(
    task: &TaskInvocation<'_>,
    request: &FluxScaleRequest,
    report: &FluxScaleReport,
) -> Vec<String> {
    let mut messages = tool_messages(request);
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
            messages.push(format!("  Spectral index fitting coefficients [zero-point, alpha, beta] with errors :{}+/-{}", coefficients(&field.spidx), coefficients(&field.spidx_err)));
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
