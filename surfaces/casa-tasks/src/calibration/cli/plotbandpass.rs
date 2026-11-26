// SPDX-License-Identifier: LGPL-3.0-or-later
//! Adapt the installed plotbandpass signature to the native plot producer.

use std::path::Path;

use crate::{TaskFailure, TaskInvocation, TaskSuccess};
use casa_contracts::{CasaValue, ParameterValue};
use serde::Serialize;

use crate::calibration::plotbandpass_statistics::derivative_record;
use casa_plot_data::calibration::plotbandpass::{
    AtmosphericOptions, BandpassPage, ChannelRange, DisplayOptions, Overlay, PlotRequest,
    Selectors, pages, prepare_bandpass,
};

#[derive(Serialize)]
struct Reply {
    pages: Vec<BandpassPage>,
    pdf: Option<PdfOutput>,
    empty_tuple: bool,
    sequential: bool,
    polynomial: bool,
    resample: u32,
    diagnostics: Vec<String>,
}

#[derive(Serialize)]
struct PdfOutput {
    output: String,
    sequential: bool,
    density: u32,
    convert: String,
    pdftk: String,
    ghostscript: String,
}

pub(super) fn run(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let caltable = task.input_path("caltable")?;
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
    let raw_overlay = task.text("overlay")?.unwrap_or_default();
    let overlay = overlay(&raw_overlay)?;
    let y_axis = task.text("yaxis")?.unwrap_or_default();
    let both = y_axis.starts_with("ap") || y_axis.starts_with("both");
    let reference_field = selector_numeric_text(task, "showatmfield")?;
    let image_sideband = task.flag("showimage")?;
    let show_atmosphere = task.flag("showatm")?;
    let transmission = show_atmosphere
        || (!task.flag("showtsky")? && (image_sideband || !reference_field.is_empty()));
    let sky_temperature = !show_atmosphere && task.flag("showtsky")?;
    let lo = if image_sideband {
        match local_oscillator(task)? {
            Some(lo) => lo,
            None => {
                return plain(
                    CasaValue::None,
                    false,
                    "lo1 must be a float (entered as a string or number)",
                );
            }
        }
    } else {
        None
    };
    if !reference_field.is_empty() && !overlay.time {
        return plain(
            CasaValue::None,
            true,
            "The showatmfield only has meaning for overlay='time'.",
        );
    }
    if both && (overlay.antenna || overlay.time || overlay.window) {
        return plain(
            CasaValue::None,
            true,
            "Option 'overlay' is incompatible with yaxis='both'. Pick either amp or phase.",
        );
    }
    let show_fdm = task.flag("showfdm")?;
    if show_fdm && !frequency_axis {
        return plain(
            CasaValue::None,
            true,
            "The option showfdm=True requires xaxis='freq'.",
        );
    }
    let Some(layout) = subplot(task)? else {
        return plain(
            CasaValue::None,
            true,
            "Invalid subplot size. Use 11 through 81, or 22, 32, or 42.",
        );
    };
    let limits = match plot_limits(task)? {
        Some(limits) => limits,
        None => {
            return plain(
                CasaValue::None,
                true,
                "plotrange must be an array: e.g. [0,1,-180,180]",
            );
        }
    };
    let phase_limits = match phase_limits(task)? {
        Some(limits) => limits.filter(|_| both),
        None => {
            return plain(
                CasaValue::None,
                true,
                "phase must be either '' or 2 values: [x,y]",
            );
        }
    };
    let channel_range = match channel_range(task, frequency_axis) {
        Ok(range) if range.is_none() || limits == [0.0; 4] => range,
        _ => {
            return plain(
                CasaValue::None,
                true,
                "Use a channel range with xaxis='freq' and default plotrange.",
            );
        }
    };
    let edge = task.integer("edge")?;
    let resample = task.integer("resample")?;
    if edge < 0 || resample < 1 {
        let message = if edge < 0 {
            "edge must be >= 0"
        } else {
            "resample must be an integer >= 1"
        };
        return plain(
            CasaValue::String {
                value: String::new(),
            },
            false,
            message,
        );
    }
    let polarization = selector(task, "poln")?;
    let polarizations = polarization
        .iter()
        .flat_map(|label| label.split(',').map(str::trim))
        .filter(|label| !label.is_empty())
        .collect::<Vec<_>>();
    let vis = task.text("vis")?.filter(|name| !name.is_empty());
    let caltable2 = task.text("caltable2")?.filter(|name| !name.is_empty());
    let caltable3 = task.text("caltable3")?.filter(|name| !name.is_empty());
    for path in [&caltable2, &caltable3].into_iter().flatten() {
        if !Path::new(path).exists() {
            return plain(
                CasaValue::None,
                true,
                &format!("Could not find companion calibration table {path}"),
            );
        }
    }
    let water = selector_numeric_text(task, "pwv")?;
    if let Some(parameter) = selector_with_names_in_vector(task) {
        return plain(
            CasaValue::None,
            parameter != "spw",
            "Specify names in a comma-delimited string or pass an array of numeric indices.",
        );
    }
    let parent_ms = task.text("parentms")?.filter(|name| !name.is_empty());
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
        overlay,
        frequency_axis,
        y_axis: &y_axis,
        polarizations: &polarizations,
        threshold_seconds: task.number("solutionTimeThresholdSeconds")?,
        show_flagged: task.flag("showflagged")?,
        show_baseband_number: task.flag("showBasebandNumber")?,
        derivative_sigma: numeric(task, "channeldiff")?,
        edge: edge as usize,
        resample: unsigned(task, "resample")?,
        platforming_threshold: task.number("platformingThreshold")?,
        platforming_sigma: task.number("platformingSigma")?,
        atmosphere: AtmosphericOptions {
            transmission,
            sky_temperature,
            water: &water,
            reference_field: &reference_field,
            image_sideband,
            lo_ghz: lo,
            parent_ms: parent_ms.as_deref().map(Path::new),
            show_points: task.flag("showatmpoints")?,
            time_overlay: overlay.time,
        },
        show_fdm,
        zoom_intersect: task.text("zoom")?.as_deref() == Some("intersect"),
    };
    let figfile = task.text("figfile")?.unwrap_or_default();
    let display = DisplayOptions {
        layout,
        figfile: &figfile,
        sequential_names: task.flag("figfileSequential")?,
        density: unsigned(task, "density")?,
        markersize: task.integer("markersize")? as f64,
        show_points: automatic_flag(task, "showpoints")?,
        show_lines: automatic_flag(task, "showlines")?,
        interactive: task.flag("interactive")?,
        limits,
        phase_limits,
        channel_range,
        limit_channels: task.flag("chanrangeSetXrange")?,
        resample: unsigned(task, "resample")?,
    };
    run_request(task, &request, &display)
}

fn run_request(
    task: &TaskInvocation<'_>,
    request: &PlotRequest<'_>,
    display: &DisplayOptions<'_>,
) -> Result<TaskSuccess, TaskFailure> {
    let figfile = display.figfile;
    let build_pdf = task.flag("buildpdf")?;
    if figfile.is_empty()
        && (build_pdf || (!display.interactive && request.derivative_sigma == 0.0))
    {
        return plain(
            CasaValue::None,
            true,
            "Specify a figfile when building a PDF or running without display or derivative statistics.",
        );
    }
    let completed_pages = std::env::var("CASARS_BANDPASS_COMPLETED_PAGES")
        .ok()
        .and_then(|text| text.parse::<usize>().ok());
    let panel_limit = completed_pages
        .map(|count| count.saturating_mul(display.layout[0] as usize * display.layout[1] as usize));
    let plan = prepare_bandpass(request, panel_limit).map_err(TaskFailure::execution)?;
    if let Some(ChannelRange::Indices([start, end])) = display.channel_range
        && plan.panels.iter().any(|panel| {
            !panel.frequency_channels.is_empty()
                && (end > panel.frequency_channels.len()
                    || (display.limit_channels && start.max(end) >= panel.frequency_channels.len()))
        })
    {
        return plain(
            CasaValue::None,
            true,
            "Channel range falls outside the calibration spectral window.",
        );
    }
    let value = derivative_record(&plan).unwrap_or(CasaValue::None);
    let empty_tuple = !plan.returns_none;
    let pdf = if build_pdf {
        Some(PdfOutput {
            output: format!("{figfile}.pdf"),
            sequential: display.sequential_names,
            density: display.density,
            convert: task.text("convert")?.unwrap_or_default(),
            pdftk: task.text("pdftk")?.unwrap_or_default(),
            ghostscript: task.text("gs")?.unwrap_or_default(),
        })
    } else {
        None
    };
    let polynomial = plan.polynomial;
    let mut diagnostics = if completed_pages.is_none() && task.flag("debug")? {
        vec![
            format!("caltable = {}", plan.caltable.display()),
            format!(
                "msName = {}.",
                plan.measurement_set
                    .as_deref()
                    .map_or_else(String::new, |ms| ms.display().to_string())
            ),
            format!(
                "Preparing {} bandpass panels for {} versus {}, overlay={:?}",
                plan.panels.len(),
                request.y_axis,
                task.text("xaxis")?.unwrap_or_default(),
                task.text("overlay")?.unwrap_or_default(),
            ),
        ]
    } else {
        Vec::new()
    };
    diagnostics.extend(plan.selection_notice.iter().cloned());
    let reply = Reply {
        pages: if completed_pages.is_none() {
            pages(plan, display).map_err(TaskFailure::execution)?
        } else {
            Vec::new()
        },
        pdf,
        empty_tuple,
        sequential: display.sequential_names,
        polynomial,
        resample: display.resample,
        diagnostics,
    };
    TaskSuccess::new(value, &reply)
        .map(|success| success.with_note("Use casars.tasks.plotbandpass to render the figures."))
}

fn plain(value: CasaValue, empty_tuple: bool, message: &str) -> Result<TaskSuccess, TaskFailure> {
    TaskSuccess::new(
        value,
        &Reply {
            pages: Vec::new(),
            pdf: None,
            empty_tuple,
            sequential: false,
            polynomial: false,
            resample: 1,
            diagnostics: vec![message.to_string()],
        },
    )
}

/// A valid optional oscillator, or no result when CASA declines its spelling.
fn local_oscillator(task: &TaskInvocation<'_>) -> Result<Option<Option<f64>>, TaskFailure> {
    match task.value("lo1") {
        Some(ParameterValue::String(value)) if value.is_empty() => Ok(Some(None)),
        Some(ParameterValue::String(value)) => {
            let decimal = value.split_once('.').is_some_and(|(whole, part)| {
                !whole.is_empty()
                    && !part.is_empty()
                    && whole
                        .bytes()
                        .chain(part.bytes())
                        .all(|byte| byte.is_ascii_digit())
            });
            let value = decimal.then(|| value.parse::<f64>().ok()).flatten();
            Ok(value.map(|hz| Some(if hz > 1e6 { hz * 1e-9 } else { hz })))
        }
        Some(ParameterValue::Float(_)) | Some(ParameterValue::Integer(_)) => Err(
            TaskFailure::execution("expected string or bytes-like object, got 'float'"),
        ),
        _ => Ok(Some(None)),
    }
}

fn selector_with_names_in_vector(task: &TaskInvocation<'_>) -> Option<&'static str> {
    ["antenna", "field", "spw"].into_iter().find(|parameter| {
        matches!(task.value(parameter), Some(ParameterValue::Array(values))
        if values.iter().any(|value| {
            matches!(value, ParameterValue::String(text) if text.parse::<i32>().is_err())
        }))
    })
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

fn selector_numeric_text(task: &TaskInvocation<'_>, name: &str) -> Result<String, TaskFailure> {
    match task.value(name) {
        Some(ParameterValue::Float(value)) => Ok(value.to_string()),
        _ => Ok(selector(task, name)?.join("")),
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
        Some(ParameterValue::String(_)) => Ok(Some(name == "showlines")),
        _ => Err(TaskFailure::validation("expected a Boolean or auto").at(name)),
    }
}

fn subplot(task: &TaskInvocation<'_>) -> Result<Option<[u32; 2]>, TaskFailure> {
    let value = selector(task, "subplot")?.join("");
    let mut size = value.trim().parse::<i32>().map_err(|_| {
        TaskFailure::execution(format!("invalid literal for int() with base 10: '{value}'"))
    })?;
    if size > 100 {
        if matches!(task.value("subplot"), Some(ParameterValue::String(_))) {
            return Err(TaskFailure::execution(
                "unsupported operand type(s) for //=: 'str' and 'int'",
            ));
        }
        size /= 10;
    }
    Ok(match (size / 10, size % 10) {
        (rows @ 1..=8, 1) => Some([rows as u32, 1]),
        (rows @ 2..=4, 2) => Some([rows as u32, 2]),
        _ => None,
    })
}

fn plot_limits(task: &TaskInvocation<'_>) -> Result<Option<[f64; 4]>, TaskFailure> {
    let values = task.numbers("plotrange")?;
    if values.is_empty() {
        return Ok(Some([0.0; 4]));
    }
    Ok(values.get(..4).and_then(|limits| limits.try_into().ok()))
}

fn phase_limits(task: &TaskInvocation<'_>) -> Result<Option<Option<[f64; 2]>>, TaskFailure> {
    let values = selector(task, "phase")?;
    Ok(match values.as_slice() {
        [value] if value.is_empty() => Some(None),
        [lower, upper] => lower
            .parse()
            .ok()
            .zip(upper.parse().ok())
            .map(|(lo, hi)| (lo != 0.0 || hi != 0.0).then_some([lo, hi])),
        _ => None,
    })
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
            Ok(([start, stop] != [0, 0]).then_some(ChannelRange::Indices([start, stop])))
        }
        [start, stop] => {
            let start = start.parse().map_err(|_| error())?;
            let stop = stop.parse().map_err(|_| error())?;
            Ok(([start, stop] != [0, 0]).then_some(ChannelRange::Indices([start, stop])))
        }
        _ => Err(error()),
    }
}
