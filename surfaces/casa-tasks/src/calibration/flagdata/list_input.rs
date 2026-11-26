// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read, validate, and serialize CASA flagdata list entries.

use std::fs;
use std::mem::discriminant;
use std::path::Path;

use crate::{TaskFailure, TaskInvocation};
use casa_contracts::{DefaultSpec, ParameterValue, python_repr};
use casa_types::quanta::format::{TIME, YMD, format_time};
use casa_types::quanta::{Quantity, Unit};

use super::super::command_values::{Assignments, parse_assignments};
use super::super::shadow;

pub(crate) fn tbuff_active(task: &TaskInvocation<'_>) -> bool {
    match task.value("tbuff") {
        Some(ParameterValue::Float(seconds)) => *seconds != 0.0,
        Some(ParameterValue::Integer(seconds)) => *seconds != 0,
        Some(ParameterValue::String(text)) => {
            text.parse::<f64>().is_ok_and(|seconds| seconds != 0.0)
        }
        Some(ParameterValue::Array(values)) => !values.is_empty(),
        _ => false,
    }
}

pub(super) fn read(task: &TaskInvocation<'_>) -> Result<Vec<Assignments>, TaskFailure> {
    let buffer = task.value("tbuff");
    let padded = tbuff_active(task);
    let padding = padding(buffer)?;
    let (lines, from_file) = match task.value("inpfile") {
        Some(ParameterValue::String(line)) if padded && !task.io_path(line).is_file() => {
            (vec![line.clone()], false)
        }
        Some(ParameterValue::String(path)) => (read_lines(&task.io_path(path))?, true),
        Some(ParameterValue::Array(values)) => {
            let inputs = strings(values)?;
            if inputs
                .first()
                .is_some_and(|path| task.io_path(path).is_file())
            {
                let mut lines = Vec::new();
                for path in inputs {
                    lines.extend(read_lines(&task.io_path(path))?);
                }
                (lines, true)
            } else {
                (inputs.into_iter().map(str::to_owned).collect(), false)
            }
        }
        _ => return Err(error()),
    };
    let reasons = match task.value("reason") {
        None | Some(ParameterValue::EmptyArray { .. }) => Vec::new(),
        Some(ParameterValue::String(value)) if value == "any" => Vec::new(),
        Some(ParameterValue::String(value)) => vec![value.as_str()],
        Some(ParameterValue::Array(values)) => strings(values)?,
        _ => return Err(error()),
    };
    let mut commands = Vec::new();
    for line in lines {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.trim().is_empty() {
            return Err(error());
        }
        let line = if from_file || !padded {
            line.replace("true", "True").replace("false", "False")
        } else {
            line
        };
        let mut values = parse_assignments(&line).ok_or_else(error)?;
        if let Some(padding) = padding {
            pad_timerange(&mut values, padding)?;
        }
        if !reasons.is_empty()
            && !matches!(values.get("reason"), Some(ParameterValue::String(reason)) if reasons.contains(&reason.as_str()))
        {
            continue;
        }
        if let Some(ParameterValue::String(path)) = values.get("addantenna")
            && !path.is_empty()
        {
            let antenna = shadow::read_list(task, path).map_err(|_| error())?;
            values.set_with_spelling("addantenna", antenna.value, antenna.command);
        }
        if values
            .iter()
            .any(|(key, value)| !valid_value(task, key, value))
        {
            return Err(error());
        }
        commands.push(values);
    }
    if commands.is_empty() {
        Err(error())
    } else {
        Ok(commands)
    }
}

fn valid_value(task: &TaskInvocation<'_>, key: &str, value: &ParameterValue) -> bool {
    match (key, value) {
        (
            "vis" | "inpfile" | "flagbackup" | "tbuff" | "cmdreason" | "savepars" | "outfile"
            | "display" | "action",
            _,
        ) => return false,
        ("ntime", ParameterValue::Float(_))
        | ("observation" | "quackinterval", ParameterValue::Integer(_))
        | ("addantenna", ParameterValue::Table(_))
        | ("timedev" | "freqdev", ParameterValue::Float(_) | ParameterValue::Array(_)) => {
            return true;
        }
        ("chanbin", _) => return matches!(value, ParameterValue::Integer(_)),
        _ => {}
    }
    task.session().bundle().surface.bindings().iter().find(|binding| binding.name == key)
        .is_some_and(|binding| matches!(&binding.default, DefaultSpec::Literal { value: default } if discriminant(default) == discriminant(value)))
}

fn strings(values: &[ParameterValue]) -> Result<Vec<&str>, TaskFailure> {
    values
        .iter()
        .map(|value| match value {
            ParameterValue::String(value) => Ok(value.as_str()),
            _ => Err(error()),
        })
        .collect()
}

fn read_lines(path: &Path) -> Result<Vec<String>, TaskFailure> {
    let text = fs::read_to_string(path).map_err(|_| error())?;
    Ok(text.lines().map(str::trim_end).map(str::to_owned).collect())
}

pub(super) fn error() -> TaskFailure {
    TaskFailure::execution("Error reading the input list. Make sure the syntax used in the list follows the rules given in the inline help of the task.")
        .at("flag-list-input")
}

fn padding(input: Option<&ParameterValue>) -> Result<Option<[f64; 2]>, TaskFailure> {
    let numeric = |value: &ParameterValue| match value {
        ParameterValue::Float(seconds) => Ok(*seconds),
        ParameterValue::Integer(seconds) => Ok(*seconds as f64),
        ParameterValue::String(text) => text.parse::<f64>().map_err(|_| error()),
        _ => Err(error()),
    };
    match input {
        None | Some(ParameterValue::EmptyArray { .. }) => Ok(None),
        Some(ParameterValue::Array(values)) => match values.as_slice() {
            [before] => Ok(Some([numeric(before)?; 2])),
            [before, after] => Ok(Some([numeric(before)?, numeric(after)?])),
            _ => Ok(None),
        },
        Some(value) => {
            let seconds = numeric(value)?;
            Ok((seconds != 0.0).then_some([seconds; 2]))
        }
    }
}

fn pad_timerange(values: &mut Assignments, [before, after]: [f64; 2]) -> Result<(), TaskFailure> {
    let Some(ParameterValue::String(range)) = values.get("timerange") else {
        return Ok(());
    };
    let Some((start, end)) = range.split_once('~') else {
        return Ok(());
    };
    let seconds = Unit::new("s").expect("seconds is a defined unit");
    let degrees = Unit::new("deg").expect("degrees is a defined unit");
    let shifted = |text: &str, offset: f64| {
        let quantity = Quantity::read(text).map_err(|_| error())?;
        let value = quantity
            .get_value_in(&seconds)
            .or_else(|_| {
                quantity
                    .get_value_in(&degrees)
                    .map(|degrees| degrees * 240.0)
            })
            .map_err(|_| error())?;
        let day = (value + offset) / 86400.0;
        if !day.is_finite() {
            return Err(error());
        }
        let format = if text.bytes().filter(|&byte| byte == b'/').count() == 3 {
            YMD
        } else {
            TIME
        };
        Ok(format_time(day, format, 9))
    };
    let range = format!("{}~{}", shifted(start, -before)?, shifted(end, after)?);
    values.set("timerange", ParameterValue::String(range));
    Ok(())
}

pub(super) fn command_line(values: &Assignments, reason: &str) -> String {
    render_command_line(values, Some(reason))
}

pub(super) fn command_line_without_reason(values: &Assignments) -> String {
    render_command_line(values, None)
}

fn render_command_line(values: &Assignments, reason: Option<&str>) -> String {
    let render = |key: &str, value: &ParameterValue| match value {
        ParameterValue::String(text) => format!("{key}='{}'", text.trim_matches('\'')),
        value => format!("{key}={}", python_repr(value)),
    };
    let replacement = reason.map(|text| ParameterValue::String(text.to_owned()));
    let mut arguments = values
        .iter()
        .filter(|(key, _)| reason.is_some() || key.as_str() != "reason")
        .map(|(key, value)| {
            if let Some(spelling) = values.spelling(key) {
                return format!("{key}={spelling}");
            }
            let rendered_value = match replacement.as_ref() {
                Some(replacement) if key == "reason" && !reason.unwrap_or_default().is_empty() => {
                    replacement
                }
                _ => value,
            };
            render(key, rendered_value)
        })
        .collect::<Vec<_>>();
    if let (Some(text), Some(replacement)) = (reason, replacement.as_ref())
        && !text.is_empty()
        && values.get("reason").is_none()
    {
        arguments.push(render("reason", replacement));
    }
    arguments.join(" ")
}
