// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::Path;

use casa_contracts::{ParameterValue, python_repr};
use casa_ms::flagging::{ShadowAntenna, ShadowOptions};
use casa_task_runtime::{TaskFailure, TaskInvocation};

pub(super) struct ShadowParameters {
    pub(super) options: ShadowOptions,
    pub(super) command_argument: Option<String>,
}

pub(super) fn parameters(task: &TaskInvocation<'_>) -> Result<ShadowParameters, TaskFailure> {
    let tolerance_m = task
        .value("tolerance")
        .and_then(number)
        .map_or_else(|| task.number("tolerance"), Ok)?;
    let (config, command_argument) = match task.value("addantenna") {
        Some(ParameterValue::String(path)) if !path.is_empty() => {
            let config = read_list(path)?;
            (config.value, config.command)
        }
        Some(value @ ParameterValue::Table(records)) if !records.is_empty() => (
            value.clone(),
            Some(
                task.argument_text("addantenna")
                    .map_or_else(|| python_repr(value), str::to_owned),
            ),
        ),
        Some(ParameterValue::String(_) | ParameterValue::Table(_)) | None => {
            (ParameterValue::Table(BTreeMap::new()), None)
        }
        _ => {
            return Err(
                TaskFailure::validation("addantenna must be a string or dictionary")
                    .at("addantenna"),
            );
        }
    };
    let additional = additional_antennas(&config);
    Ok(ShadowParameters {
        options: ShadowOptions {
            tolerance_m,
            enabled: additional.is_some(),
            additional: additional.unwrap_or_default(),
        },
        command_argument,
    })
}

fn additional_antennas(value: &ParameterValue) -> Option<Vec<ShadowAntenna>> {
    let ParameterValue::Table(records) = value else {
        tracing::warn!(
            "Cannot create agent Shadow: Parameter 'addantenna' must be of type 'record/dict'"
        );
        return None;
    };
    let mut antennas = Vec::with_capacity(records.len());
    for index in 0..records.len() {
        let Some(ParameterValue::Table(record)) = records.get(&index.to_string()) else {
            tracing::warn!(
                index,
                "Cannot create agent Shadow: invalid additional antenna record"
            );
            return None;
        };
        let coords = record.get("position").and_then(position);
        let diameter = record.get("diameter").and_then(number);
        if let (Some(position_m), Some(diameter_m)) = (coords, diameter) {
            antennas.push(ShadowAntenna {
                position_m,
                diameter_m,
            });
        } else {
            tracing::warn!(
                index,
                "NOT using additional antennas for shadow calculations: a dish needs a diameter and three ITRF coordinates"
            );
            return Some(Vec::new());
        }
    }
    Some(antennas)
}

fn number(value: &ParameterValue) -> Option<f64> {
    match value {
        ParameterValue::Float(value) => Some(*value),
        ParameterValue::Integer(value) => Some(*value as f64),
        ParameterValue::NumpyScalar { value, .. } => number(value),
        _ => None,
    }
}

fn position(value: &ParameterValue) -> Option<[f64; 3]> {
    let ParameterValue::Array(elements) = value else {
        return None;
    };
    let [x, y, z] = elements.as_slice() else {
        return None;
    };
    // CASA's additional antenna record admits a floating-point vector.
    if elements
        .iter()
        .all(|value| matches!(value, ParameterValue::Integer(_)))
    {
        return None;
    }
    Some([number(x)?, number(y)?, number(z)?])
}

pub(super) struct AntennaList {
    pub(super) value: ParameterValue,
    pub(super) command: Option<String>,
}

pub(super) fn read_list(path: &str) -> Result<AntennaList, TaskFailure> {
    let contents = std::fs::read_to_string(path).map_err(|_| {
        let message = if Path::new(path).exists() {
            format!("Error opening file {path}")
        } else {
            format!("File {path} not found - please verify the name")
        };
        TaskFailure::execution(message).at("addantenna")
    })?;
    let lines: Vec<_> = contents
        .lines()
        .map(str::trim_end)
        .filter(|line| line.len() >= 5 && !line.starts_with('#'))
        .collect();
    let (dishes, remainder) = lines.as_chunks::<3>();
    if !remainder.is_empty() {
        tracing::warn!("The file needs to have 3 entries per antenna, on separate lines");
        return Ok(AntennaList {
            value: ParameterValue::Bool(false),
            command: Some("False".to_owned()),
        });
    }
    let mut records = BTreeMap::new();
    let mut command = Vec::new();
    for (index, lines) in dishes.iter().enumerate() {
        let mut record = BTreeMap::new();
        let mut fields = Vec::new();
        for line in lines {
            let Some((key, raw)) = line.split_once('=') else {
                tracing::warn!(line, "Error in parsing additional antennas");
                return Ok(empty_list());
            };
            let key = key.trim_end();
            let raw = raw.trim();
            let value = match key {
                "name" => ParameterValue::String(
                    raw.split_whitespace()
                        .next()
                        .ok_or_else(|| {
                            TaskFailure::execution("list index out of range").at("addantenna")
                        })?
                        .to_owned(),
                ),
                "diameter" => ParameterValue::Float(parse_float(raw)?),
                "position" => {
                    let coordinates = raw
                        .strip_prefix('[')
                        .and_then(|raw| raw.strip_suffix(']'))
                        .unwrap_or(raw);
                    let terms: Vec<_> = coordinates.split(',').collect();
                    let [x, y, z] = terms.as_slice() else {
                        tracing::warn!(line, "Error in parsing additional antennas");
                        return Ok(empty_list());
                    };
                    ParameterValue::Array(
                        [x, y, z]
                            .into_iter()
                            .map(|text| parse_float(text.trim()).map(ParameterValue::Float))
                            .collect::<Result<_, _>>()?,
                    )
                }
                _ => continue,
            };
            fields.push(format!("'{key}': {}", python_repr(&value)));
            record.insert(key.to_owned(), value);
        }
        command.push(format!("'{index}': {{{}}}", fields.join(", ")));
        records.insert(index.to_string(), ParameterValue::Table(record));
    }
    Ok(AntennaList {
        value: ParameterValue::Table(records),
        command: Some(format!("{{{}}}", command.join(", "))),
    })
}

fn empty_list() -> AntennaList {
    AntennaList {
        value: ParameterValue::Table(BTreeMap::new()),
        command: Some("{}".to_owned()),
    }
}

fn parse_float(value: &str) -> Result<f64, TaskFailure> {
    value.parse().map_err(|_| {
        let value = python_repr(&ParameterValue::String(value.to_owned()));
        TaskFailure::execution(format!("could not convert string to float: {value}"))
            .at("addantenna")
    })
}
