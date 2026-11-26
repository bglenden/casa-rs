// SPDX-License-Identifier: LGPL-3.0-or-later
//! The public sdbaseline task and its output-product conventions.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::{CasaValue, ParameterValue};
use casa_ms::transform::TransformTaskHistory;
use casa_ms::{MeasurementSet, MsSelection};

use casa_calibration::single_dish::preparation::baseline::sinusoid::{
    FftThreshold, INVALID_WAVE, WaveSelection,
};
use casa_calibration::{
    SingleDishBaseline, SingleDishBaselineBasis, SingleDishBaselineDiagnostics,
    SingleDishBaselineMode, SingleDishLineFinder, SingleDishSession, SingleDishSinusoid,
};

use super::selector;

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let input = task.input_path("infile")?;
    let output = task
        .text("outfile")?
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| format!("{}_bs", input.display().to_string().trim_end_matches('/')));
    let output = Path::new(&output);
    let overwrite = task.flag("overwrite")?;
    let subtract = task.flag("dosubtract")?;
    if subtract && output.exists() && !overwrite {
        return Err(TaskFailure::execution(format!(
            "outfile='{}' exists, and cannot overwrite it.",
            output.display()
        ))
        .at("outfile"));
    }
    let mode = task.required_text("blmode")?.to_ascii_lowercase();
    let family = task.required_text("blfunc")?.to_ascii_lowercase();
    if family == "variable" {
        let pa = task.text("blparam")?.unwrap_or_default();
        if !Path::new(&pa).exists() {
            return Err(
                TaskFailure::execution(format!("input file '{pa}' does not exists")).at("blparam"),
            );
        }
    }
    let mask = task
        .text("spw")?
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "*".into());
    let update_weight = task.flag("updateweight")?;
    let products = if mode == "fit" {
        DiagnosticPaths::from_task(task, &input, subtract, overwrite)?
    } else {
        DiagnosticPaths::default()
    };
    let maskmode = task.required_text("maskmode")?;
    let header = text_header(&input, output, &maskmode);
    let table = task.text("bltable")?.unwrap_or_default();
    let parameter_file = task.text("blparam")?.unwrap_or_default();
    let diagnostics = SingleDishBaselineDiagnostics {
        csv: products.csv.as_deref(),
        text: products.text.as_deref(),
        heading: Some(&header),
    };
    let baseline_mode = match mode.as_str() {
        "fit" if family == "variable" => SingleDishBaselineMode::Variable {
            parameters: Path::new(&parameter_file),
            verbose: task.flag("verbose")?,
            table: products.table.as_deref(),
            diagnostics,
        },
        "fit" => {
            let basis = match family.as_str() {
                "poly" | "chebyshev" => SingleDishBaselineBasis::Polynomial {
                    order: nonnegative(task, "order")?,
                    chebyshev: family == "chebyshev",
                },
                "cspline" => SingleDishBaselineBasis::CubicSpline {
                    pieces: usize::try_from(task.integer("npiece")?)
                        .ok()
                        .filter(|&pieces| pieces != 0)
                        .ok_or_else(|| {
                            TaskFailure::execution("npiece must be positive.").at("npiece")
                        })?,
                },
                "sinusoid" => SingleDishBaselineBasis::Sinusoid(sinusoid(task)?),
                _ => return Err(unsupported(&family, "blfunc")),
            };
            let line_finder = if maskmode == "auto" {
                Some(SingleDishLineFinder::new(
                    task.number("thresh")?,
                    task.integer("avg_limit")?,
                    task.integer("minwidth")?,
                    &task.integers("edge")?,
                ))
            } else {
                None
            };
            SingleDishBaselineMode::Fit {
                basis,
                clip_sigma: task.number("clipthresh")?,
                max_fits: nonnegative(task, "clipniter")? + 1,
                line_finder,
                table: products.table.as_deref(),
                diagnostics,
            }
        }
        "apply" => {
            let table = Path::new(&table);
            if !table.exists() {
                return Err(TaskFailure::execution(format!(
                    "file specified in bltable '{}' does not exist.",
                    table.display()
                ))
                .at("bltable"));
            }
            SingleDishBaselineMode::Apply { table }
        }
        _ => return TaskSuccess::new(CasaValue::None, &()),
    };
    let selection = selected_windows(&input, &mask)?;
    let mut session = SingleDishSession::default();
    session.open(&input);
    session
        .set_selection(
            MsSelection {
                antenna: selector(task, "antenna")?,
                field: selector(task, "field")?,
                spw: Some(selection),
                timerange: selector(task, "timerange")?,
                scan: selector(task, "scan")?,
                correlation: selector(task, "pol")?,
                intent: selector(task, "intent")?,
                ..MsSelection::default()
            },
            task.flag("reindex")?,
        )
        .map_err(TaskFailure::execution)?;
    let report = session
        .baseline(SingleDishBaseline {
            mode: baseline_mode,
            datacolumn: &task.required_text("datacolumn")?,
            mask: &mask,
            output: subtract.then_some(output),
            subtract: subtract || mode == "apply",
            overwrite,
            update_weight,
            sigma_value: &task.required_text("sigmavalue")?,
            history: subtract.then(|| TransformTaskHistory {
                task: "sdbaseline".into(),
                parameters: Vec::new(),
            }),
        })
        .map_err(TaskFailure::execution)?;
    TaskSuccess::new(CasaValue::None, &report)
}

fn sinusoid(task: &TaskInvocation<'_>) -> Result<SingleDishSinusoid, TaskFailure> {
    let include = wave_selection(task, "addwn")?;
    let exclude = wave_selection(task, "rejwn")?;
    let threshold = match task.value("fftthresh").map(numpy_scalar) {
        Some(ParameterValue::Integer(value)) => value.to_string(),
        Some(ParameterValue::Float(value)) => value.to_string(),
        Some(ParameterValue::String(value)) => value.to_ascii_lowercase(),
        _ => {
            return Err(
                TaskFailure::validation("fftthresh must be float or integer or string.")
                    .at("fftthresh"),
            );
        }
    };
    FftThreshold::validate(&threshold)
        .map_err(|message| TaskFailure::validation(message).at("fftthresh"))?;
    if matches!(&include, WaveSelection::Explicit(values) if values.is_empty()) {
        return Err(TaskFailure::execution("addwn must contain at least one element.").at("addwn"));
    }
    Ok(SingleDishSinusoid::new(
        include,
        exclude,
        task.flag("applyfft")?,
        &task.required_text("fftmethod")?.to_ascii_lowercase(),
        &threshold,
    ))
}

fn numpy_scalar(value: &ParameterValue) -> &ParameterValue {
    match value {
        ParameterValue::NumpyScalar { value, .. } => value,
        value => value,
    }
}

fn wave_selection(task: &TaskInvocation<'_>, name: &str) -> Result<WaveSelection, TaskFailure> {
    let selection = match task.value(name) {
        Some(ParameterValue::String(text)) => WaveSelection::from_task_text(text),
        Some(ParameterValue::Integer(value)) => WaveSelection::explicit([*value]),
        Some(ParameterValue::Array(values)) => wave_list(values),
        _ => Err(INVALID_WAVE.into()),
    };
    selection.map_err(|message| TaskFailure::validation(message).at(name))
}

fn wave_list(values: &[ParameterValue]) -> Result<WaveSelection, String> {
    if values
        .iter()
        .any(|value| matches!(value, ParameterValue::String(_)))
    {
        let mut text = Vec::with_capacity(values.len());
        for value in values {
            let ParameterValue::String(value) = value else {
                return Err("Cannot sort a wavenumber list containing strings and numbers.".into());
            };
            text.push(value.as_str());
        }
        text.sort_unstable();
        text.dedup();
        let indexes = text
            .into_iter()
            .map(|value| value.trim().parse::<i64>().map_err(|_| INVALID_WAVE))
            .collect::<Result<Vec<_>, _>>()?;
        return WaveSelection::explicit(indexes);
    }

    // CASA sorts and deduplicates the original Python values, then converts
    // the joined list to integer prefixes in the direct tool.
    let mut numbers = Vec::with_capacity(values.len());
    for value in values.iter().map(numpy_scalar) {
        let number = match value {
            ParameterValue::Float(value) if value.is_finite() => (*value, *value as i64),
            ParameterValue::Integer(value) => (*value as f64, *value),
            ParameterValue::Bool(value) => (f64::from(u8::from(*value)), 0),
            _ => return Err(INVALID_WAVE.into()),
        };
        if number.1 < 0 {
            return Err(INVALID_WAVE.into());
        }
        numbers.push(number);
    }
    numbers.sort_by(|left, right| left.0.partial_cmp(&right.0).expect("finite wave number"));
    numbers.dedup_by(|left, right| left.0 == right.0);
    WaveSelection::explicit(numbers.into_iter().map(|(_, index)| index))
}

pub(super) fn selected_windows(input: &Path, mask: &str) -> Result<String, TaskFailure> {
    let ms =
        MeasurementSet::open(input).map_err(|error| TaskFailure::execution(error.to_string()))?;
    let selection = ms
        .resolve_spw_selector(mask)
        .map_err(|error| TaskFailure::execution(error.to_string()).at("spw"))?;
    Ok(selection
        .spw_ids()
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(","))
}

fn nonnegative(task: &TaskInvocation<'_>, parameter: &str) -> Result<usize, TaskFailure> {
    usize::try_from(task.integer(parameter)?).map_err(|_| {
        TaskFailure::execution(format!("{parameter} must be positive or zero.")).at(parameter)
    })
}

fn unsupported(value: &str, name: &str) -> TaskFailure {
    TaskFailure::execution(format!(
        "sdbaseline {name}={value:?} is not yet implemented (#1532)"
    ))
    .at(name)
}

#[derive(Default)]
struct DiagnosticPaths {
    csv: Option<PathBuf>,
    text: Option<PathBuf>,
    table: Option<PathBuf>,
}

impl DiagnosticPaths {
    fn from_task(
        task: &TaskInvocation<'_>,
        input: &Path,
        subtract: bool,
        overwrite: bool,
    ) -> Result<Self, TaskFailure> {
        let formats = task.texts("blformat")?;
        if !subtract && formats.iter().all(String::is_empty) {
            return Err(TaskFailure::execution(
                "blformat must be specified when dosubtract is False",
            )
            .at("blformat"));
        }
        let mut outputs = task.texts("bloutput")?;
        if outputs == [""] {
            outputs.resize(formats.len(), String::new());
        }
        if outputs.len() != formats.len() {
            return Err(
                TaskFailure::execution("blformat and bloutput must have the same length.")
                    .at("bloutput"),
            );
        }
        let mut seen_formats = BTreeSet::new();
        let mut seen_outputs = BTreeSet::new();
        let mut paths = Self::default();
        for (format, output) in formats.iter().zip(&outputs) {
            if format.is_empty() {
                continue;
            }
            if !seen_formats.insert(format) {
                return Err(
                    TaskFailure::execution("duplicate elements in blformat.").at("blformat")
                );
            }
            if !output.is_empty() && !seen_outputs.insert(output) {
                return Err(
                    TaskFailure::execution("duplicate elements in bloutput.").at("bloutput")
                );
            }
            let (slot, extension) = match format.to_ascii_lowercase().as_str() {
                "csv" => (&mut paths.csv, "csv"),
                "text" => (&mut paths.text, "txt"),
                "table" => (&mut paths.table, "bltable"),
                _ => continue,
            };
            if slot.is_some() {
                continue;
            }
            let path = if output.is_empty() {
                PathBuf::from(format!("{}_blparam.{extension}", input.display()))
            } else {
                PathBuf::from(output)
            };
            if path.exists() && !overwrite {
                return Err(
                    TaskFailure::execution(format!("{} exists.", path.display())).at("bloutput"),
                );
            }
            *slot = Some(path);
        }
        Ok(paths)
    }
}

fn text_header(input: &Path, output: &Path, maskmode: &str) -> String {
    const SEPARATOR: &str = "############################################################";
    format!(
        "{SEPARATOR}\nSource Table: {}\n Output File: {}\n   Mask mode: {maskmode}\n{SEPARATOR}\n\n",
        input.display(),
        output.display()
    )
}
