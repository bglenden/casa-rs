// SPDX-License-Identifier: LGPL-3.0-or-later
//! Single-dish calibration task entry points.

mod position_switch;

use std::path::{Path, PathBuf};

use casa_ms::{MeasurementSet, MsSelection};
use casa_provider_contracts::{CasaValue, ParameterValue};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use crate::CalibrationOutputMode;
use crate::gencal::{GencalRequest, GencalType, gencal_with_output_mode};
use crate::session::resolve_casa_calibration_selection;

pub(crate) fn sdcal(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let mode = task.required_text("calmode")?;
    let infile = task.input_path("infile")?;
    let outfile = task.text("outfile")?.unwrap_or_default();
    let overwrite = task.flag("overwrite")?;
    let selection = MsSelection {
        selectdata: true,
        field: selector(task, "field")?,
        spw: selector(task, "spw")?,
        scan: selector(task, "scan")?,
        ..MsSelection::default()
    };
    if mode == "tsys" && (selection.scan.is_some() || selection.spw.is_some()) {
        tracing::warn!("Scan and Spw input must be empty in calmode='tsys'.");
        return TaskSuccess::new(CasaValue::None, &serde_json::Value::Null);
    }
    let ms = MeasurementSet::open(&infile)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("infile"))?;
    let selected = resolve_casa_calibration_selection(&ms, &selection).map_err(|error| {
        TaskFailure::execution(format!("Error in data selection specification: {error}"))
    })?;
    if !outfile.is_empty() && Path::new(&outfile).exists() && !overwrite {
        return Err(TaskFailure::execution(format!(
            "overwrite is False and output file exists: {outfile}"
        ))
        .at("outfile"));
    }
    if outfile.is_empty() && matches!(mode.as_str(), "tsys" | "ps") {
        return Err(TaskFailure::execution("Output file name must be specified.").at("outfile"));
    }
    let count = match mode.as_str() {
        "ps" => {
            check_fraction(task)?;
            let rows = selected.row_indices().collect::<Vec<_>>();
            position_switch::generate(&ms, &rows, Path::new(&outfile), overwrite).map_err(
                |error| {
                    tracing::error!(%error, "Single-dish sky solve failed");
                    TaskFailure::execution("Error in Calibrater::solve.")
                },
            )?
        }
        "tsys" => tsys(infile, PathBuf::from(&outfile), overwrite)?,
        _ => {
            return Err(TaskFailure::validation(format!(
                "sdcal mode {mode:?} is pending in #1530"
            ))
            .at("calmode"));
        }
    };
    TaskSuccess::new(
        CasaValue::None,
        &serde_json::json!({
            "caltable": outfile,
            "row_count": count,
        }),
    )
}

fn selector(task: &TaskInvocation<'_>, key: &str) -> Result<Option<String>, TaskFailure> {
    task.text(key)
        .map(|selector| selector.filter(|text| !text.is_empty()))
}

fn tsys(infile: PathBuf, outfile: PathBuf, overwrite: bool) -> Result<usize, TaskFailure> {
    let output = if overwrite {
        CalibrationOutputMode::Replace
    } else {
        CalibrationOutputMode::Create
    };
    let request = GencalRequest {
        measurement_set: infile,
        output_table: outfile,
        caltype: GencalType::Tsys,
        antenna: String::new(),
        spw: String::new(),
        parameter: Vec::new(),
        gaincurve_table: None,
    };
    gencal_with_output_mode(&request, "", None, None, true, output)
        .map(|report| report.row_count)
        .map_err(|error| TaskFailure::execution(error.to_string()))
}

fn check_fraction(task: &TaskInvocation<'_>) -> Result<(), TaskFailure> {
    let fraction = match task.value("fraction") {
        Some(ParameterValue::String(text)) if text.trim().is_empty() => 0.1,
        Some(ParameterValue::String(text)) => {
            let (value, scale) = text
                .split_once('%')
                .map_or((text.as_str(), 1.), |(value, _)| (value, 0.01));
            value.trim().parse::<f64>().map_err(|_| {
                let text = casa_provider_contracts::python_repr(&ParameterValue::String(value.into()));
                TaskFailure::execution(format!(
                    "Invalid fraction value (original error message: \"could not convert string to float: {text}\")"
                ))
                .at("fraction")
            })? * scale
        }
        Some(ParameterValue::Float(value)) => *value,
        Some(ParameterValue::Integer(value)) => *value as f64,
        Some(ParameterValue::Bool(value)) => u8::from(*value) as f64,
        _ => return Err(TaskFailure::validation("Invalid fraction value.").at("fraction")),
    };
    if task.integer("noff")? <= 0 && fraction >= 0.5 {
        return Err(
            TaskFailure::validation("Too many edge points. fraction must be < 0.5.").at("fraction"),
        );
    }
    Ok(())
}
