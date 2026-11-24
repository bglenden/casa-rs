// SPDX-License-Identifier: LGPL-3.0-or-later
//! The sdatmcor task's public selection and ATM parameter conversion.

use std::collections::BTreeMap;
use std::path::Path;

use casa_ms::{MeasurementSet, MsSelection};
use casa_provider_contracts::{CasaValue, ParameterValue};
use casa_tables::{Table, TableOptions};
use casa_task_runtime::{CasaLoggingConfig, TaskFailure, TaskInvocation, TaskSuccess};
use casa_types::{
    ArrayValue, RecordValue, ScalarValue, Value,
    quanta::{Quantity, Unit},
};
use ndarray::Array1;

use super::{MainRows, SingleDishAtmosphere, SingleDishSession, inputs, thread_count};
use crate::single_dish::selector;

pub(crate) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let infile = task.required_text("infile")?;
    let outfile = task.required_text("outfile")?;
    let overwrite = task.flag("overwrite")?;
    let (input, output) = check_paths(&infile, &outfile, overwrite)?;
    let atmosphere = task.integer("atmtype")?;
    if !(1..=5).contains(&atmosphere) {
        return Err(TaskFailure::execution(format!(
            "atmtype (={atmosphere}) should be any one of (1, 2, 3, 4, 5)."
        ))
        .at("atmtype"));
    }
    let humidity = task.number("humidity")?;
    if humidity != -1. && !(0.0..=100.0).contains(&humidity) {
        return Err(TaskFailure::execution(format!(
            "humidity (={humidity}) should be in range 0~100"
        ))
        .at("humidity"));
    }
    let column = task.required_text("datacolumn")?;
    if super::data_column(&column).is_err() {
        return Err(TaskFailure::execution(format!(
            "Specified column name ({column}) Unacceptable."
        ))
        .at("datacolumn"));
    }

    let ms = MeasurementSet::open(input).map_err(execution)?;
    let antenna = selector(task, "antenna")?
        .map(|text| include_autocorrelations(&ms, text))
        .transpose()?;
    let output_spw = task.required_text("outputspw")?;
    let rows = MainRows::read(&ms).map_err(execution)?;
    let (config, messages) = configuration(task, &ms, &rows, &output_spw, atmosphere, humidity)?;
    let mut session = SingleDishSession::default();
    session.open(input);
    session
        .set_selection(
            MsSelection {
                spw: Some(output_spw),
                field: selector(task, "field")?,
                scan: selector(task, "scan")?,
                antenna,
                correlation: selector(task, "correlation")?,
                timerange: selector(task, "timerange")?,
                intent: selector(task, "intent")?,
                observation: selector(task, "observation")?,
                feed: selector(task, "feed")?,
                msselect: selector(task, "msselect")?,
                ..MsSelection::default()
            },
            false,
        )
        .map_err(execution)?;
    let report = session
        .atmcor_prepared(
            &ms,
            rows,
            SingleDishAtmosphere {
                config: &config,
                datacolumn: &column,
                outfile: output,
                overwrite,
            },
        )
        .map_err(execution)?;
    let fallback =
        !CasaLoggingConfig::output_was_configured(&std::env::args_os().skip(1).collect::<Vec<_>>());
    let unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64());
    let mjd_seconds = unix_seconds + 40_587.0 * 86_400.0;
    let mut result = TaskSuccess::new(CasaValue::None, &report)?;
    for message in messages {
        result = result.with_replayed_info(mjd_seconds, "sdatmcor", &message, fallback);
    }
    Ok(result)
}

fn include_autocorrelations(ms: &MeasurementSet, text: String) -> Result<String, TaskFailure> {
    let has_auto = |expression: &str| {
        let selected = ms.resolve_antenna_selector(expression).map_err(execution)?;
        Ok::<_, TaskFailure>(
            (0..selected.antenna_count()).any(|index| selected.holds(index, index)),
        )
    };
    if text.is_empty() || has_auto(&text)? {
        return Ok(text);
    }
    let rewritten = text
        .split(';')
        .flat_map(|clause| {
            if clause.is_empty() || clause.ends_with("&&") {
                return vec![clause.to_string()];
            }
            clause
                .split('&')
                .filter(|part| !part.is_empty())
                .map(|part| format!("{part}&&&"))
                .collect()
        })
        .collect::<Vec<_>>()
        .join(";");
    if !has_auto(&rewritten)? {
        return Err(TaskFailure::execution(
            "Cannot handle antenna selection properly. Abort.",
        ));
    }
    Ok(rewritten)
}

fn check_paths<'a>(
    infile: &'a str,
    outfile: &'a str,
    overwrite: bool,
) -> Result<(&'a Path, &'a Path), TaskFailure> {
    if infile.is_empty() {
        return Err(TaskFailure::execution("infile MUST BE specified.").at("infile"));
    }
    if outfile.is_empty() {
        return Err(TaskFailure::execution("outfile MUST BE specified.").at("outfile"));
    }
    let input = Path::new(infile);
    let output = Path::new(outfile);
    if crate::paths::overlap(input, output).map_err(execution)? {
        return Err(TaskFailure::execution(
            "You are attempting to write the output on your input file.",
        )
        .at("outfile"));
    }
    if !input.exists() {
        return Err(TaskFailure::execution("Specified infile does not exist.").at("infile"));
    }
    if output.exists() && !overwrite {
        return Err(TaskFailure::execution("Specified outfile already exist.").at("outfile"));
    }
    Ok((input, output))
}

fn configuration(
    task: &TaskInvocation<'_>,
    ms: &MeasurementSet,
    raw: &MainRows,
    output: &str,
    atmosphere: i64,
    humidity: f64,
) -> Result<(RecordValue, Vec<String>), TaskFailure> {
    let reference = inputs::default_antenna(ms, raw).map_err(execution)?;
    let site = inputs::site_altitude(ms, reference).map_err(execution)?;
    let detailed = task.flag("atmdetail")?;
    let processes = task.required_text("spw")?;
    let selected = resolve_windows(ms, &processes, "spw")?;
    let written = resolve_windows(ms, output, "outputspw")?;
    let (windows, uncorrected): (Vec<_>, Vec<_>) = written
        .into_iter()
        .filter(|window| raw.windows.contains(window))
        .partition(|window| selected.contains(window));
    let length = ms.spectral_window().map_err(execution)?.row_count();
    let gain = gain_factors(task.value("gainfactor"), length)?;
    let mut messages = vec![format!("processspw (input) = {windows:?}")];
    if let [window] = uncorrected.as_slice() {
        messages.push(format!("SPW {window} is output but not corrected"));
    } else if !uncorrected.is_empty() {
        let ids = uncorrected.iter().map(i32::to_string).collect::<Vec<_>>();
        messages.push(format!(
            "SPWs {} are output but not corrected",
            ids.join(" ")
        ));
    }
    let mut record = RecordValue::default();
    record.upsert(
        "processspw",
        Value::Array(ArrayValue::Int32(Array1::from_vec(windows).into_dyn())),
    );
    record.upsert("gainfactor", doubles(gain));
    record.upsert("refant", Value::Scalar(ScalarValue::Int32(reference)));
    record.upsert(
        "atmType",
        Value::Scalar(ScalarValue::Int32(atmosphere as i32)),
    );
    let threads = std::env::var("OMP_NUM_THREADS")
        .ok()
        .map(|text| text.trim().parse::<i32>())
        .transpose()
        .map_err(execution)?
        .unwrap_or(-1);
    messages.push(format!(
        "Setting numThreads_ to {}",
        thread_count(Some(f64::from(threads)))
    ));
    record.upsert("nthreads", Value::Scalar(ScalarValue::Int32(threads)));
    record.upsert("maxAltitude", scalar(120.));
    record.upsert(
        "lapseRate",
        scalar(quantity(task, "dtem_dh", "K/km")?.unwrap_or(-5.6)),
    );
    record.upsert(
        "scaleHeight",
        scalar(quantity(task, "h0", "km")?.unwrap_or(2.)),
    );
    record.upsert(
        "pressureStep",
        scalar(quantity(task, "dp", "mbar")?.unwrap_or(10.)),
    );
    let factor = task.number("dpm")?;
    record.upsert(
        "pressureStepFactor",
        scalar(if factor == -1. { 1.2 } else { factor }),
    );
    let altitude = if detailed {
        quantity(task, "altitude", "m")?.unwrap_or(site)
    } else {
        site
    };
    record.upsert("siteAltitude", scalar(altitude));
    for (name, unit) in [("temperature", "K"), ("pressure", "mbar"), ("pwv", "mm")] {
        if let Some(value) = quantity(task, name, unit)?
            && detailed
        {
            record.upsert(name, scalar(value));
        }
    }
    if detailed && humidity != -1. {
        record.upsert("humidity", scalar(humidity));
    }
    for (parameter, field, unit) in [
        ("layerboundaries", "layerBoundaries", "m"),
        ("layertemperature", "layerTemperatures", "K"),
    ] {
        if let Some(values) = layers(task, parameter, unit)?
            && detailed
        {
            record.upsert(field, doubles(values));
        }
    }
    Ok((record, messages))
}

fn resolve_windows(
    ms: &MeasurementSet,
    expression: &str,
    parameter: &str,
) -> Result<Vec<i32>, TaskFailure> {
    let expression = if expression.is_empty() {
        "*"
    } else {
        expression
    };
    ms.resolve_spw_selector(expression)
        .map(|selection| {
            selection
                .spw_ids()
                .iter()
                .map(|&window| window as i32)
                .collect()
        })
        .map_err(|error| execution(error).at(parameter))
}

fn gain_factors(value: Option<&ParameterValue>, windows: usize) -> Result<Vec<f64>, TaskFailure> {
    let value = value.map(scalar_value);
    let mut factors = vec![1.; windows];
    match value {
        Some(ParameterValue::Table(entries)) => {
            for (key, value) in entries {
                let window = key
                    .parse::<i32>()
                    .map_err(|error| execution(error).at("gainfactor"))?;
                if let Some(factor) = factors.get_mut(window as usize) {
                    *factor = numeric(value).ok_or_else(|| {
                        TaskFailure::execution("Non-numeric gain factor.").at("gainfactor")
                    })?;
                }
            }
        }
        Some(ParameterValue::String(path)) => {
            for (window, factor) in calibration_gains(Path::new(path))? {
                if let Some(value) = factors.get_mut(window as usize) {
                    *value = factor;
                }
            }
        }
        Some(value) => factors.fill(
            numeric(value)
                .ok_or_else(|| TaskFailure::execution("Invalid gainfactor.").at("gainfactor"))?,
        ),
        None => {}
    }
    Ok(factors)
}

fn calibration_gains(path: &Path) -> Result<BTreeMap<i32, f64>, TaskFailure> {
    if !path.exists() {
        return Err(
            TaskFailure::execution(format!("\"{}\" should exist.", path.display()))
                .at("gainfactor"),
        );
    }
    let table = Table::open(TableOptions::new(path)).map_err(execution)?;
    let (column, spw) = ["FPARAM", "CPARAM"]
        .iter()
        .find_map(|&name| {
            table
                .column_accessor(name)
                .ok()
                .zip(table.column_accessor("SPECTRAL_WINDOW_ID").ok())
        })
        .ok_or_else(|| {
            TaskFailure::execution(format!("{} is not a caltable", path.display())).at("gainfactor")
        })?;
    let mut sums = BTreeMap::<i32, (f64, usize)>::new();
    for first in (0..table.row_count()).step_by(512) {
        let rows = (first..(first + 512).min(table.row_count())).collect::<Vec<_>>();
        let values = column
            .array_cells_owned_uncached(&rows)
            .map_err(execution)?;
        let windows = spw.scalar_cells_owned_for_rows(&rows).map_err(execution)?;
        for (values, window) in values.into_iter().zip(windows) {
            let Some(ScalarValue::Int32(window)) = window else {
                return Err(TaskFailure::execution("Invalid calibration window."));
            };
            let (sum, count) = sums.entry(window).or_default();
            match values {
                Some(ArrayValue::Float32(values)) => {
                    *sum += reciprocal_squares(values.iter().copied());
                    *count += values.len();
                }
                Some(ArrayValue::Complex32(values)) => {
                    *sum += reciprocal_squares(values.iter().map(|value| value.re));
                    *count += values.len();
                }
                _ => {
                    return Err(
                        TaskFailure::execution("Invalid calibration gain cells.").at("gainfactor")
                    );
                }
            }
        }
    }
    Ok(sums
        .into_iter()
        .map(|(window, (sum, count))| (window, sum / count as f64))
        .collect())
}

fn reciprocal_squares(values: impl Iterator<Item = f32>) -> f64 {
    values.map(|value| f64::from((value * value).recip())).sum()
}

fn quantity(task: &TaskInvocation<'_>, name: &str, unit: &str) -> Result<Option<f64>, TaskFailure> {
    match task.value(name).map(scalar_value) {
        None => Ok(None),
        Some(ParameterValue::String(text)) if text.is_empty() => Ok(None),
        Some(value) => parse_quantity(value, unit)
            .map(Some)
            .map_err(|error| execution(error).at(name)),
    }
}

fn layers(
    task: &TaskInvocation<'_>,
    name: &str,
    unit: &str,
) -> Result<Option<Vec<f64>>, TaskFailure> {
    let numbers = match task.value(name).map(scalar_value) {
        None => return Ok(None),
        Some(ParameterValue::String(text)) if text.is_empty() => return Ok(None),
        Some(ParameterValue::String(text)) => text
            .split(',')
            .map(|item| ParameterValue::String(item.into()))
            .collect::<Vec<_>>(),
        Some(ParameterValue::Array(items)) => items.to_vec(),
        Some(ParameterValue::EmptyArray { .. }) => Vec::new(),
        Some(_) => {
            return Err(TaskFailure::execution(
                "user_param for parse_atm_list_params should be either list or str.",
            )
            .at(name));
        }
    };
    numbers
        .iter()
        .map(|value| parse_quantity(value, unit))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
        .map_err(|error| execution(error).at(name))
}

fn parse_quantity(value: &ParameterValue, unit: &str) -> Result<f64, String> {
    if let Some(value) = numeric(value) {
        return Ok(value);
    }
    let ParameterValue::String(text) = scalar_value(value) else {
        return Err("The atmosphere parameter must be a quantity.".into());
    };
    let quantity = Quantity::read(text).map_err(|error| error.to_string())?;
    if quantity.unit() == Quantity::dimensionless(0.).unit() {
        return Ok(quantity.value());
    }
    let expected = Unit::new(unit).map_err(|error| error.to_string())?;
    quantity.get_value_in(&expected).map_err(|_| {
        format!("User input \"{text}\" should have the unit compatible with \"{unit}\"")
    })
}

fn scalar_value(value: &ParameterValue) -> &ParameterValue {
    match value {
        ParameterValue::NumpyScalar { value, .. } | ParameterValue::ScalarArray(value) => {
            scalar_value(value)
        }
        value => value,
    }
}

fn numeric(value: &ParameterValue) -> Option<f64> {
    match scalar_value(value) {
        ParameterValue::Float(value) => Some(*value),
        ParameterValue::Integer(value) => Some(*value as f64),
        ParameterValue::Bool(value) => Some(f64::from(u8::from(*value))),
        _ => None,
    }
}

fn scalar(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn doubles(values: Vec<f64>) -> Value {
    Value::Array(ArrayValue::Float64(Array1::from_vec(values).into_dyn()))
}

fn execution(error: impl std::fmt::Display) -> TaskFailure {
    TaskFailure::execution(error.to_string())
}
