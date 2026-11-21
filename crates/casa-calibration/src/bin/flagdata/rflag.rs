// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's scalar, matrix and file forms of RFlag threshold parameters.

use std::collections::BTreeMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use casa_ms::flagging::{FlagDataRequest, RFlagThresholdReport};
use casa_provider_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};

#[derive(Default)]
pub(super) struct ThresholdInput {
    pub(super) scalar: Option<f64>,
    pub(super) by_field_spw: BTreeMap<(i32, i32), f64>,
    pub(super) invalid: bool,
}

pub(super) fn threshold_input(
    task: &TaskInvocation<'_>,
    name: &str,
    calculate: bool,
) -> Result<ThresholdInput, TaskFailure> {
    let Some(value) = task.value(name) else {
        return Ok(ThresholdInput::default());
    };
    if let Some(scalar) = number(value) {
        return Ok(ThresholdInput {
            scalar: Some(scalar),
            ..ThresholdInput::default()
        });
    }
    match value {
        ParameterValue::String(path) if !path.is_empty() && !calculate => read_file(path, name),
        ParameterValue::String(_) | ParameterValue::EmptyArray { .. } => {
            Ok(ThresholdInput::default())
        }
        ParameterValue::Array(rows) => Ok(matrix(rows)),
        _ => Ok(ThresholdInput {
            invalid: !calculate,
            ..ThresholdInput::default()
        }),
    }
}

fn number(value: &ParameterValue) -> Option<f64> {
    match value {
        ParameterValue::Float(value) => Some(*value),
        ParameterValue::Integer(value) => Some(*value as f64),
        ParameterValue::NumpyScalar { value, .. } | ParameterValue::ScalarArray(value) => {
            number(value)
        }
        _ => None,
    }
}

fn matrix(rows: &[ParameterValue]) -> ThresholdInput {
    let by_field_spw = rows
        .iter()
        .map(|row| {
            let ParameterValue::Array(columns) = row else {
                return None;
            };
            let [field, spw, threshold] = columns.as_slice() else {
                return None;
            };
            Some((
                (number(field)? as i32, number(spw)? as i32),
                number(threshold)?,
            ))
        })
        .collect::<Option<BTreeMap<_, _>>>();
    match by_field_spw {
        Some(by_field_spw) => ThresholdInput {
            by_field_spw,
            ..ThresholdInput::default()
        },
        None => ThresholdInput {
            invalid: true,
            ..ThresholdInput::default()
        },
    }
}

fn read_file(path: &str, name: &str) -> Result<ThresholdInput, TaskFailure> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(ThresholdInput::default()),
        Err(error) => return Err(TaskFailure::execution(error.to_string()).at(name)),
    };
    let value = contents
        .lines()
        .rev()
        .filter(|line| line.len() > 5 && !line.starts_with('#'))
        .find_map(|line| {
            // CASA emits a one-line Python dictionary with single-quoted keys
            // and a nested numeric array. Parse the data without evaluation.
            let mut record = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
                &line.replace('\'', "\""),
            )
            .ok()?;
            record.remove(name)
        })
        .ok_or_else(|| {
            TaskFailure::execution(
            "cannot access local variable 'devthreshold' where it is not associated with a value",
        ).at("rflag-threshold-file")
        })?;
    Ok(match ParameterValue::from_plain_json(&value) {
        Some(ParameterValue::Array(rows)) => matrix(&rows),
        other => {
            let scalar = other.as_ref().and_then(number);
            ThresholdInput {
                scalar,
                invalid: scalar.is_none(),
                ..ThresholdInput::default()
            }
        }
    })
}

pub(super) fn write_files(
    task: &TaskInvocation<'_>,
    report: &RFlagThresholdReport,
) -> Result<(), TaskFailure> {
    if report.timedev.is_empty() && report.freqdev.is_empty() {
        return Ok(());
    }
    let mut outputs = Vec::new();
    for (name, rows) in [("timedev", &report.timedev), ("freqdev", &report.freqdev)] {
        let path = output_path(task.value(name), name);
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let failure = |error: std::io::Error| {
            TaskFailure::execution(format!("{}: {error}", path.display())).at(name)
        };
        let mut output = tempfile::NamedTempFile::new_in(parent).map_err(failure)?;
        let data = serde_json::to_string(rows)
            .map_err(|error| TaskFailure::execution(error.to_string()).at(name))?;
        writeln!(output, "{{'name':'Rflag','{name}':{data}}}\n").map_err(failure)?;
        outputs.push((name, path, output));
    }
    for (name, path, output) in outputs {
        output.persist(&path).map_err(|error| {
            TaskFailure::execution(format!("{}: {}", path.display(), error.error)).at(name)
        })?;
    }
    Ok(())
}

fn output_path(input: Option<&ParameterValue>, name: &str) -> PathBuf {
    if let Some(ParameterValue::String(path)) = input {
        let number_only = path.replace('.', "");
        if !path.is_empty()
            && !path.contains('[')
            && (number_only.is_empty() || !number_only.chars().all(char::is_numeric))
        {
            return PathBuf::from(path);
        }
    }
    PathBuf::from(format!("rflag_output_thresholds_{name}0.txt"))
}

pub(super) fn saved_threshold(
    original: Option<&ParameterValue>,
    name: &str,
    action: &str,
    request: Option<&FlagDataRequest>,
    report: Option<&RFlagThresholdReport>,
) -> Option<(ParameterValue, bool)> {
    let rows_value = |rows: Vec<[f64; 3]>| {
        ParameterValue::Array(
            rows.into_iter()
                .map(|row| {
                    ParameterValue::Array(row.into_iter().map(ParameterValue::Float).collect())
                })
                .collect(),
        )
    };
    let empty = match original {
        None | Some(ParameterValue::EmptyArray { .. }) => true,
        Some(ParameterValue::Array(rows)) => rows.is_empty(),
        Some(ParameterValue::String(value)) => value.is_empty(),
        _ => false,
    };
    if let Some(report) = report
        && (empty || matches!(original, Some(ParameterValue::Array(_))))
    {
        let rows = if name == "timedev" {
            &report.timedev
        } else {
            &report.freqdev
        };
        return Some((rows_value(rows.clone()), empty));
    }
    if action == "apply"
        && matches!(original, Some(ParameterValue::String(path)) if !path.is_empty())
        && let Some(request) = request
    {
        let (scalar, fields) = if name == "timedev" {
            (request.timedev, &request.timedev_by_field_spw)
        } else {
            (request.freqdev, &request.freqdev_by_field_spw)
        };
        if fields.is_empty() {
            return scalar.map(|value| (ParameterValue::Float(value), false));
        }
        return Some((
            rows_value(
                fields
                    .iter()
                    .map(|(&(field, spw), &value)| [f64::from(field), f64::from(spw), value])
                    .collect(),
            ),
            false,
        ));
    }
    original
        .filter(|_| !empty)
        .cloned()
        .map(|value| (value, false))
}
