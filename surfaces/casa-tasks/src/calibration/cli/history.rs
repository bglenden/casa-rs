// SPDX-License-Identifier: LGPL-3.0-or-later
//! The applycal task's `mstools.write_history` provenance.

use std::path::Path;

use casa_contracts::{ParameterValue, python_repr};
use casa_tables::{Table, TableOptions};
use casa_task_runtime::TaskInvocation;
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};

pub(super) fn record_applycal(task: &TaskInvocation<'_>, vis: &Path) {
    let _phase = casa_calibration::timing::Phase::start("applycal_history");
    record_task(task, vis);
}

pub(super) fn record_task(task: &TaskInvocation<'_>, vis: &Path) {
    record_messages(task, vis, messages(task));
}

pub(crate) fn record_sdcal(task: &TaskInvocation<'_>, vis: &Path) {
    // The pinned task inspects the decorated callable's code object, which
    // exposes zero positional parameters to its HISTORY writer.
    record_messages(
        task,
        vis,
        vec![
            "taskname=sdcal".into(),
            format!("version: casa-rs {}", env!("CARGO_PKG_VERSION")),
        ],
    );
}

fn record_messages(task: &TaskInvocation<'_>, vis: &Path, messages: Vec<String>) {
    if let Err(error) = append(vis, "ms", task.surface(), messages) {
        tracing::warn!(task = task.surface(), "updating HISTORY: {error}");
    }
}

pub(super) fn record_setjy_flux(vis: &Path, messages: Vec<String>) {
    if let Err(error) = append(vis, "setjy", "setjy", messages) {
        tracing::warn!("updating setjy flux HISTORY: {error}");
    }
}

pub(super) fn record_setjy_imager_flux(vis: &Path, messages: Vec<String>) {
    if !messages.is_empty()
        && let Err(error) = append(vis, "imager", "imager::setjy()", messages)
    {
        tracing::warn!("updating setjy imager flux HISTORY: {error}");
    }
}

fn append(
    vis: &Path,
    application: &str,
    origin: &str,
    messages: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = vis.join("HISTORY");
    #[cfg(unix)]
    let mut table = Table::open_with_lock(
        TableOptions::new(&path),
        casa_tables::LockOptions::new(casa_tables::LockMode::PermanentLockingWait),
    )?;
    #[cfg(not(unix))]
    let mut table = Table::open(TableOptions::new(&path))?;
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs_f64()
        + 40_587. * 86_400.;
    for message in messages {
        let mut row = RecordValue::new(Vec::new());
        for (name, value) in [
            ("APPLICATION", application),
            ("ORIGIN", origin),
            ("PRIORITY", "INFO"),
            ("MESSAGE", &message),
        ] {
            row.upsert(name, Value::Scalar(ScalarValue::String(value.into())));
        }
        row.upsert("TIME", Value::Scalar(ScalarValue::Float64(time)));
        row.upsert("OBJECT_ID", Value::Scalar(ScalarValue::Int32(0)));
        row.upsert("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(-1)));
        for name in ["APP_PARAMS", "CLI_COMMAND"] {
            row.upsert(
                name,
                Value::Array(ArrayValue::from_string_vec(vec![String::new()])),
            );
        }
        table.add_row(row)?;
    }
    table.save(TableOptions::new(path))?;
    Ok(())
}

fn value(task: &TaskInvocation<'_>, name: &str) -> Option<ParameterValue> {
    task.session().states().get(name)?.value.clone()
}

fn messages(task: &TaskInvocation<'_>) -> Vec<String> {
    if task.surface() == "ft" {
        return ft_messages(task);
    }
    let mut messages = vec![
        format!("taskname={}", task.surface()),
        format!("version: casa-rs {}", env!("CARGO_PKG_VERSION")),
    ];
    let mut bindings = task
        .session()
        .bundle()
        .surface
        .bindings()
        .iter()
        .filter(|binding| task.surface() != "applycal" || binding.order < 21)
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    for binding in bindings {
        if let Some(value) = effective(task, &binding.name) {
            let text = match value {
                ParameterValue::String(text) => format!("\"{text}\""),
                value => python_repr(&value),
            };
            messages.push(format!("{:<11} = {text}", binding.name));
        }
    }
    messages
}

fn ft_messages(task: &TaskInvocation<'_>) -> Vec<String> {
    let mut messages = vec!["taskname = ft".into()];
    for name in [
        "vis",
        "field",
        "spw",
        "timerange",
        "uvrange",
        "antenna",
        "scan",
        "observation",
        "intent",
        "model",
        "complist",
        "incremental",
    ] {
        let Some(value) = effective(task, name) else {
            continue;
        };
        let text = match value {
            ParameterValue::String(text) if name == "model" => {
                python_repr(&ParameterValue::Array(vec![ParameterValue::String(text)]))
            }
            ParameterValue::String(text) => text,
            other => python_repr(&other),
        };
        messages.push(format!("{name:<12}= \"{text}\""));
    }
    messages
}

fn effective(task: &TaskInvocation<'_>, name: &str) -> Option<ParameterValue> {
    if task.surface() == "statwt"
        && task.value("selectdata") == Some(&ParameterValue::Bool(false))
        && matches!(
            name,
            "field" | "spw" | "intent" | "array" | "observation" | "scan"
        )
    {
        return Some(ParameterValue::String(String::new()));
    }
    let mut current = value(task, name)?;
    // The provider uses an MS-path vector and "none" selector sentinels;
    // CASA's public applycal parameters and HISTORY use a path and empty strings.
    if name == "vis"
        && let ParameterValue::Array(paths) = &current
        && paths.len() == 1
    {
        return Some(paths[0].clone());
    }
    if matches!(
        name,
        "field"
            | "spw"
            | "intent"
            | "timerange"
            | "uvrange"
            | "antenna"
            | "scan"
            | "observation"
            | "msselect"
    ) && current == ParameterValue::String("none".into())
        && task
            .session()
            .bundle()
            .surface
            .bindings()
            .iter()
            .any(|binding| binding.name == name && binding.concept.semantic_revision.0 == 1)
    {
        current = ParameterValue::String(String::new());
    }

    if name == "applymode" && current == ParameterValue::String(String::new()) {
        return Some(ParameterValue::String("calflag".into()));
    }
    if value(task, "docallib") == Some(ParameterValue::Bool(true)) {
        // CASA wraps a flat spwmap only in its active traditional branch.
        return Some(if name == "spwmap" {
            task.session().given(name).cloned().unwrap_or(current)
        } else {
            current
        });
    }
    let tables = match value(task, "gaintable") {
        Some(ParameterValue::Array(tables)) => tables,
        _ => Vec::new(),
    };
    if let ParameterValue::Array(values) = &mut current {
        match name {
            "calwt" if values.len() == 1 => {
                let count = if tables == [ParameterValue::String(String::new())] {
                    0
                } else {
                    tables.len()
                };
                *values = vec![values[0].clone(); count];
            }
            "spwmap" if !values.is_empty() && !matches!(values[0], ParameterValue::Array(_)) => {
                current = ParameterValue::Array(vec![current]);
            }
            "interp" => {
                for (table, value) in tables.iter().zip(values) {
                    if table != &ParameterValue::String(String::new())
                        && value == &ParameterValue::String(String::new())
                    {
                        *value = ParameterValue::String("linear".into());
                    }
                }
            }
            _ => {}
        }
    }
    Some(current)
}
