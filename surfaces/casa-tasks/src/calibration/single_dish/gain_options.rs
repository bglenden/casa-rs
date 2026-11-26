// SPDX-License-Identifier: LGPL-3.0-or-later
//! Double-circle radius and prior-calibration task arguments.

use std::path::Path;

use casa_contracts::ParameterValue;
use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::quanta::{Quantity, Unit};

use casa_calibration::{ApplyCalibrationTableSpec, ApplyInterpolationMode};

pub(super) fn radius(task: &TaskInvocation<'_>) -> Result<Option<f64>, TaskFailure> {
    let invalid = || TaskFailure::execution("Error in Calibrater::setsolve.").at("radius");
    let radians = match task.value("radius") {
        None => return Ok(None),
        Some(ParameterValue::String(text)) if text.trim().is_empty() => return Ok(None),
        Some(ParameterValue::String(text)) => {
            if let Ok(value) = text.trim().parse::<f64>() {
                (value / 3600.).to_radians()
            } else {
                let quantity = text.parse::<Quantity>().map_err(|_| invalid())?;
                let unit = "rad".parse::<Unit>().expect("standard angle unit");
                quantity.get_value_in(&unit).map_err(|_| invalid())?
            }
        }
        Some(ParameterValue::Float(value)) => (value / 3600.).to_radians(),
        Some(ParameterValue::Integer(value)) => (*value as f64 / 3600.).to_radians(),
        _ => return Err(invalid()),
    };
    if !radians.is_finite() || radians < 0. {
        return Err(invalid());
    }
    Ok(Some(radians))
}

pub(super) fn priors(
    task: &TaskInvocation<'_>,
) -> Result<Vec<ApplyCalibrationTableSpec>, TaskFailure> {
    let tables = match task.value("applytable") {
        None => return Ok(Vec::new()),
        Some(ParameterValue::String(path)) if path.is_empty() => return Ok(Vec::new()),
        Some(value @ ParameterValue::String(_)) => std::slice::from_ref(value),
        Some(ParameterValue::Array(tables)) => tables.as_slice(),
        _ => {
            return Err(
                TaskFailure::execution("applytable must be a path or a list of paths.")
                    .at("applytable"),
            );
        }
    };
    tables
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let ParameterValue::String(path) = entry else {
                return Err(
                    TaskFailure::execution("applytable entries must be paths.").at("applytable")
                );
            };
            if path.is_empty() {
                return Err(
                    TaskFailure::execution("applytable entries must be nonempty paths.")
                        .at("applytable"),
                );
            }
            if !Path::new(path).exists() {
                return Err(
                    TaskFailure::execution(format!("Table {path} does not exist."))
                        .at("applytable"),
                );
            }
            let interp = match task.value("interp") {
                Some(ParameterValue::Array(values)) => values.get(index),
                value => value,
            };
            let mut spec = ApplyCalibrationTableSpec::new(path);
            spec.interp = match interp {
                None => ApplyInterpolationMode::Linear,
                Some(ParameterValue::String(value)) => ApplyInterpolationMode::parse_casa(value)
                    .map_err(|_| setapply_error("interp"))?,
                _ => return Err(TaskFailure::validation("Invalid interpolation.").at("interp")),
            };
            spec.spwmap = spwmap(task.value("spwmap"), index)?;
            Ok(spec)
        })
        .collect()
}

fn spwmap(value: Option<&ParameterValue>, index: usize) -> Result<Vec<i32>, TaskFailure> {
    let invalid = || TaskFailure::validation("Invalid spectral-window mapping.").at("spwmap");
    let values = match value {
        None => return Ok(Vec::new()),
        Some(ParameterValue::Table(values)) if values.is_empty() => return Ok(Vec::new()),
        Some(ParameterValue::Array(values)) => values.as_slice(),
        _ => return Err(invalid()),
    };
    let values = if values
        .iter()
        .all(|value| matches!(value, ParameterValue::Array(_)))
    {
        let Some(ParameterValue::Array(values)) = values.get(index) else {
            return Ok(Vec::new());
        };
        values.as_slice()
    } else {
        values
    };
    values
        .iter()
        .map(|value| match value {
            ParameterValue::Integer(id) => i32::try_from(*id).map_err(|_| invalid()),
            _ => Err(invalid()),
        })
        .collect()
}

pub(super) fn setapply_error(parameter: &'static str) -> TaskFailure {
    TaskFailure::execution("Error in Calibrater::setapply.").at(parameter)
}
