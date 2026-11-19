// SPDX-License-Identifier: LGPL-3.0-or-later
//! Adapt the installed plotbandpass signature to the native plot producer.

use std::path::Path;

use casa_provider_contracts::{CasaValue, ParameterValue};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;

use crate::plotbandpass::{
    BandpassPage, ChannelRange, DisplayOptions, Overlay, PlotRequest, Selectors, derivative_record,
    pages, prepare_bandpass,
};

#[derive(Serialize)]
struct Reply {
    pages: Vec<BandpassPage>,
}

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let caltable = task.input_path("caltable")?;
    check_remaining_controls(task)?;
    let axis = task.text("xaxis")?.unwrap_or_default();
    let frequency_axis = match axis.as_str() {
        "chan" | "channel" => false,
        "freq" | "frequency" => true,
        _ => {
            return Err(
                TaskFailure::validation("xaxis must name channel or frequency").at("xaxis"),
            );
        }
    };
    let polarization = selector(task, "poln")?;
    let polarizations = polarization
        .iter()
        .flat_map(|label| label.split(',').map(str::trim))
        .filter(|label| !label.is_empty())
        .collect::<Vec<_>>();
    let vis = task.text("vis")?.filter(|name| !name.is_empty());
    let y_axis = task.text("yaxis")?.unwrap_or_default();
    let caltable2 = task.text("caltable2")?.filter(|name| !name.is_empty());
    let caltable3 = task.text("caltable3")?.filter(|name| !name.is_empty());
    let request = PlotRequest {
        caltable: &caltable,
        vis: vis.as_deref().map(Path::new),
        companion_tables: [
            caltable2.as_deref().map(Path::new),
            caltable3.as_deref().map(Path::new),
        ],
        selectors: Selectors {
            antenna: selector(task, "antenna")?,
            field: selector(task, "field")?,
            spw: selector(task, "spw")?,
            time: selector(task, "timeranges")?,
            scan: selector(task, "scans")?,
            baseband: selector(task, "basebands")?,
        },
        overlay: overlay(&task.text("overlay")?.unwrap_or_default())?,
        frequency_axis,
        y_axis: &y_axis,
        polarizations: &polarizations,
        threshold_seconds: task.number("solutionTimeThresholdSeconds")?,
        show_flagged: task.flag("showflagged")?,
        show_baseband_number: task.flag("showBasebandNumber")?,
        derivative_sigma: numeric(task, "channeldiff")?,
        edge: unsigned(task, "edge")? as usize,
        resample: unsigned(task, "resample")?,
    };
    let figfile = task.text("figfile")?.unwrap_or_default();
    let display = DisplayOptions {
        layout: subplot(task)?,
        figfile: &figfile,
        sequential_names: task.flag("figfileSequential")?,
        density: unsigned(task, "density")?,
        markersize: task.integer("markersize")? as f64,
        show_points: automatic_flag(task, "showpoints")?,
        show_lines: automatic_flag(task, "showlines")?,
        interactive: task.flag("interactive")?,
        limits: task.numbers("plotrange")?.try_into().map_err(|_| {
            TaskFailure::validation("plotrange must contain four values").at("plotrange")
        })?,
        phase_limits: if y_axis.to_ascii_lowercase().contains("both") {
            phase_limits(task)?
        } else {
            None
        },
        channel_range: channel_range(task, frequency_axis)?,
        limit_channels: task.flag("chanrangeSetXrange")?,
        resample: unsigned(task, "resample")?,
    };
    let plan = prepare_bandpass(&request).map_err(TaskFailure::execution)?;
    let value = if request.derivative_sigma > 0.0 {
        derivative_record(&plan)
    } else {
        CasaValue::None
    };
    let reply = Reply {
        pages: pages(plan, &display).map_err(TaskFailure::execution)?,
    };
    TaskSuccess::new(value, &reply)
        .map(|success| success.with_note("Use casars.tasks.plotbandpass to render the figures."))
}

fn selector(task: &TaskInvocation<'_>, name: &str) -> Result<Vec<String>, TaskFailure> {
    let convert = |value: &ParameterValue| match value {
        ParameterValue::String(value) => Ok(value.clone()),
        ParameterValue::Integer(value) => Ok(value.to_string()),
        _ => Err(TaskFailure::validation("expected a name or an index").at(name)),
    };
    match task.value(name) {
        Some(ParameterValue::Array(values)) => values.iter().map(convert).collect(),
        Some(value) => Ok(vec![convert(value)?]),
        None => Ok(Vec::new()),
    }
}

fn overlay(text: &str) -> Result<Overlay, TaskFailure> {
    let mut mode = Overlay::default();
    for word in text.split(',').map(str::trim) {
        match word {
            "" => (),
            "antenna" => mode.antenna = true,
            "time" => mode.time = true,
            "spw" => mode.window = true,
            "baseband" => mode.baseband = true,
            _ => return Err(TaskFailure::validation("unknown bandpass overlay").at("overlay")),
        }
    }
    Ok(mode)
}

fn numeric(task: &TaskInvocation<'_>, name: &str) -> Result<f64, TaskFailure> {
    match task.value(name) {
        Some(ParameterValue::Bool(value)) => Ok(f64::from(u8::from(*value))),
        _ => task.number(name),
    }
}

fn unsigned(task: &TaskInvocation<'_>, name: &str) -> Result<u32, TaskFailure> {
    u32::try_from(task.integer(name)?)
        .map_err(|_| TaskFailure::validation("expected a nonnegative integer").at(name))
}

fn automatic_flag(task: &TaskInvocation<'_>, name: &str) -> Result<Option<bool>, TaskFailure> {
    match task.value(name) {
        Some(ParameterValue::Bool(value)) => Ok(Some(*value)),
        Some(ParameterValue::String(value)) if value == "auto" => Ok(None),
        _ => Err(TaskFailure::validation("expected a Boolean or auto").at(name)),
    }
}

fn subplot(task: &TaskInvocation<'_>) -> Result<[u32; 2], TaskFailure> {
    let value = selector(task, "subplot")?.join("");
    let mut size = value.chars().map(|digit| digit.to_digit(10));
    match (size.next().flatten(), size.next().flatten()) {
        (Some(rows @ 1..=8), Some(columns @ 1..=2)) => Ok([rows, columns]),
        _ => Err(TaskFailure::validation("invalid bandpass subplot grid").at("subplot")),
    }
}

fn phase_limits(task: &TaskInvocation<'_>) -> Result<Option<[f64; 2]>, TaskFailure> {
    let values = selector(task, "phase")?;
    if values.iter().all(String::is_empty) {
        return Ok(None);
    }
    let numbers = values
        .iter()
        .flat_map(|value| value.split(',').map(str::trim))
        .map(str::parse::<f64>)
        .collect::<Result<Vec<_>, _>>();
    let limits = numbers.ok().and_then(|values| values.try_into().ok());
    limits
        .map(Some)
        .ok_or_else(|| TaskFailure::validation("phase must provide two limits").at("phase"))
}

fn channel_range(
    task: &TaskInvocation<'_>,
    frequency_axis: bool,
) -> Result<Option<ChannelRange>, TaskFailure> {
    let values = selector(task, "chanrange")?;
    let error = || {
        TaskFailure::validation("chanrange must be a channel pair or a percentage").at("chanrange")
    };
    match values.as_slice() {
        [] => Ok(None),
        [value] if value.is_empty() => Ok(None),
        [value] if frequency_axis => {
            if let Some(percent) = value.strip_suffix('%') {
                let percent = percent.parse::<f64>().map_err(|_| error())?;
                return Ok((0.0 < percent && percent < 100.0)
                    .then_some(ChannelRange::CentralPercent(percent)));
            }
            let (start, stop) = value.split_once('~').ok_or_else(error)?;
            let start = start.trim().parse().map_err(|_| error())?;
            let stop = stop.trim().parse().map_err(|_| error())?;
            Ok(Some(ChannelRange::Indices([start, stop])))
        }
        [start, stop] => {
            let start = start.parse().map_err(|_| error())?;
            let stop = stop.parse().map_err(|_| error())?;
            Ok(Some(ChannelRange::Indices([start, stop])))
        }
        _ => Err(error()),
    }
}

fn check_remaining_controls(task: &TaskInvocation<'_>) -> Result<(), TaskFailure> {
    for name in [
        "buildpdf",
        "zoom",
        "showatm",
        "pwv",
        "gs",
        "convert",
        "showtsky",
        "showfdm",
        "showatmfield",
        "lo1",
        "showimage",
        "showatmpoints",
        "parentms",
        "pdftk",
        "platformingThreshold",
        "platformingSigma",
    ] {
        if task.is_set(name) {
            return Err(
                TaskFailure::validation("bandpass control in progress under #1526").at(name),
            );
        }
    }
    Ok(())
}
