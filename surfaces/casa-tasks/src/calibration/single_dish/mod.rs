// SPDX-License-Identifier: LGPL-3.0-or-later
//! Single-dish calibration task adapters (`sdcal`, `sdgaincal`, `sdsmooth`, `sdatmcor`, `sdfit`, `sdbaseline`).

mod application;
pub(crate) mod atmosphere;
pub(crate) mod baseline;
pub(crate) mod gain;
mod gain_options;
pub(crate) mod line_fit;
mod smooth;

use std::path::{Path, PathBuf};

use casa_calibration::single_dish::{edges, lock_solve_output, position_switch};
use casa_calibration::{
    CalibrationOutputMode, GencalRequest, GencalType, gencal_with_output_mode,
    session::resolve_casa_calibration_selection,
};
use casa_contracts::{CasaValue, ParameterValue};
use casa_ms::{MeasurementSet, MsError, MsSelection, MsSelectionError, ResolvedMsSelection};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

pub(crate) fn run_sdsmooth(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    smooth::run(task)
}

pub(crate) fn sdcal(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let mode = task.required_text("calmode")?;
    let generates_table = matches!(mode.as_str(), "ps" | "tsys" | "otfraster" | "otf");
    let infile = task.input_path("infile")?;
    let outfile = task.text("outfile")?.unwrap_or_default();
    let overwrite = task.flag("overwrite")?;
    let selection = MsSelection {
        selectdata: true,
        field: selector(task, "field")?,
        spw: selector(task, "spw")?,
        scan: selector(task, "scan")?,
        intent: if matches!(mode.as_str(), "ps" | "otf" | "otfraster") {
            selector(task, "intent")?
        } else {
            None
        },
        ..MsSelection::default()
    };
    if mode == "tsys" && (selection.scan.is_some() || selection.spw.is_some()) {
        tracing::warn!("Scan and Spw input must be empty in calmode='tsys'.");
        return TaskSuccess::new(CasaValue::None, &serde_json::Value::Null);
    }
    let ms = MeasurementSet::open(&infile)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("infile"))?;
    let selected = resolve_selection(&ms, &selection)?;
    if !outfile.is_empty() && Path::new(&outfile).exists() && !overwrite {
        return Err(TaskFailure::execution(format!(
            "overwrite is False and output file exists: {outfile}"
        ))
        .at("outfile"));
    }
    if matches!(
        mode.as_str(),
        "apply"
            | "ps,apply"
            | "tsys,apply"
            | "ps,tsys,apply"
            | "otfraster,apply"
            | "otfraster,tsys,apply"
            | "otf,apply"
            | "otf,tsys,apply"
    ) {
        return application::run(task, &mode, ms, selection, selected);
    }
    if outfile.is_empty() && generates_table {
        return Err(TaskFailure::execution("Output file name must be specified.").at("outfile"));
    }
    let output_path = Path::new(&outfile);
    if generates_table
        && casa_calibration::paths::overlap(&infile, output_path)
            .map_err(|error| TaskFailure::execution(error.to_string()))?
    {
        return Err(TaskFailure::execution(
            "The MeasurementSet and output calibration table overlap.",
        )
        .at("outfile"));
    }
    let output_mode = if overwrite {
        CalibrationOutputMode::Replace
    } else {
        CalibrationOutputMode::Create
    };
    let _output_locks = if overwrite && generates_table {
        lock_solve_output(output_path, output_mode)
            .map_err(|error| TaskFailure::execution(error.to_string()).at("outfile"))?
    } else {
        Vec::new()
    };
    let count = match mode.as_str() {
        "ps" => generate_position_switch(task, &ms, selected, output_path, output_mode)?,
        "otfraster" | "otf" => generate_edges(task, &ms, selected, output_path, output_mode)?,
        "tsys" => tsys(infile, PathBuf::from(&outfile), output_mode)?,
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

fn resolve_selection(
    ms: &MeasurementSet,
    selection: &MsSelection,
) -> Result<ResolvedMsSelection, TaskFailure> {
    resolve_casa_calibration_selection(ms, selection).map_err(|error| {
        if let MsSelectionError::Domain(MsError::VersionError(reason)) = &error
            && let Some(intent) = reason
                .strip_prefix("intent selector ")
                .and_then(|value| value.strip_suffix(" did not match any STATE rows"))
        {
            return TaskFailure::execution(format!(
                "Error in data selection specification: State Expression: No match found for {intent}\nState Expression: Partial or no match for State ID list []"
            ));
        }
        TaskFailure::execution(format!("Error in data selection specification: {error}"))
    })
}

fn generate_position_switch(
    task: &TaskInvocation<'_>,
    ms: &MeasurementSet,
    selected: ResolvedMsSelection,
    output: &Path,
    mode: CalibrationOutputMode,
) -> Result<usize, TaskFailure> {
    check_fraction(task)?;
    let rows = selected.row_indices().collect::<Vec<_>>();
    position_switch::generate(ms, &rows, output, mode).map_err(|error| {
        tracing::error!(%error, "Single-dish sky solve failed");
        TaskFailure::execution("Error in Calibrater::solve.")
    })
}

fn generate_edges(
    task: &TaskInvocation<'_>,
    ms: &MeasurementSet,
    selected: ResolvedMsSelection,
    output: &Path,
    mode: CalibrationOutputMode,
) -> Result<usize, TaskFailure> {
    let fraction = check_fraction(task)?;
    let selection = if task.required_text("calmode")?.starts_with("otfraster") {
        edges::Selection::Raster {
            fraction,
            noff: task.integer("noff")?,
        }
    } else {
        edges::Selection::Otf { fraction }
    };
    let rows = selected.row_indices().collect::<Vec<_>>();
    edges::generate(ms, &rows, output, mode, selection).map_err(|error| {
        tracing::error!(%error, "Single-dish map sky solve failed");
        TaskFailure::execution("Error in Calibrater::solve.")
    })
}

pub(crate) fn selector(
    task: &TaskInvocation<'_>,
    key: &str,
) -> Result<Option<String>, TaskFailure> {
    task.text(key)
        .map(|selector| selector.filter(|text| !text.is_empty()))
}

fn tsys(
    infile: PathBuf,
    outfile: PathBuf,
    mode: CalibrationOutputMode,
) -> Result<usize, TaskFailure> {
    let request = GencalRequest {
        measurement_set: infile,
        output_table: outfile,
        caltype: GencalType::Tsys,
        antenna: String::new(),
        spw: String::new(),
        parameter: Vec::new(),
        gaincurve_table: None,
    };
    gencal_with_output_mode(&request, "", None, None, true, mode)
        .map(|report| report.row_count)
        .map_err(|error| TaskFailure::execution(error.to_string()))
}

fn check_fraction(task: &TaskInvocation<'_>) -> Result<f64, TaskFailure> {
    let fraction = match task.value("fraction") {
        Some(ParameterValue::String(text)) if text.trim().is_empty() => 0.1,
        Some(ParameterValue::String(text)) => {
            // CASA uses the '%' position in the stripped string to index the
            // original argument, so leading whitespace affects that slice.
            let (value, scale) = if let Some(index) = text.trim().chars().position(|ch| ch == '%') {
                let limit = text
                    .char_indices()
                    .nth(index)
                    .map_or(text.len(), |(offset, _)| offset);
                (&text[..limit], 0.01)
            } else {
                (text.as_str(), 1.)
            };
            value.trim().parse::<f64>().map_err(|_| {
                let text = casa_contracts::python_repr(&ParameterValue::String(value.into()));
                invalid_fraction(&format!("could not convert string to float: {text}"))
            })? * scale
        }
        Some(ParameterValue::Float(value)) => *value,
        Some(ParameterValue::Integer(value)) => *value as f64,
        Some(ParameterValue::Bool(value)) => u8::from(*value) as f64,
        Some(ParameterValue::Array(_)) => {
            return Err(invalid_fraction(
                "float() argument must be a string or a real number, not 'list'",
            ));
        }
        Some(ParameterValue::Table(_)) => {
            return Err(invalid_fraction(
                "float() argument must be a string or a real number, not 'dict'",
            ));
        }
        _ => return Err(TaskFailure::validation("Invalid fraction value.").at("fraction")),
    };
    if task.integer("noff")? <= 0 && fraction >= 0.5 {
        return Err(
            TaskFailure::validation("Too many edge points. fraction must be < 0.5.").at("fraction"),
        );
    }
    Ok(fraction)
}

fn invalid_fraction(reason: &str) -> TaskFailure {
    TaskFailure::execution(format!(
        "Invalid fraction value (original error message: \"{reason}\")"
    ))
    .at("fraction")
}
