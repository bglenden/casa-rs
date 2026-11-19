// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration table input for the shared PlotMS provider.

mod averaging;
mod axis;
mod figure;
mod samples;
mod selection;
mod table_data;
mod text;

use std::ffi::OsStr;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use casa_provider_contracts::{CasaValue, ParameterValue};
use casa_tables::{Table, TableInfo, TableOptions};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;

use self::axis::Axis;
use self::samples::{Point, SeriesRequest};
use self::selection::{Expressions, Selection};
use self::table_data::CalibrationTable;

pub(super) fn input(task: &TaskInvocation<'_>) -> Result<Option<PathBuf>, TaskFailure> {
    let explicit = task.text("caltable")?.filter(|value| !value.is_empty());
    let vis = task.text("vis")?.filter(|value| !value.is_empty());
    if let Some(explicit) = explicit {
        if vis.is_some() {
            return Err(TaskFailure::execution(
                "Cannot set both vis and caltable parameters",
            ));
        }
        return Ok(Some(explicit.into()));
    }
    Ok(vis.map(PathBuf::from).filter(|path| {
        std::fs::read_to_string(path.join("table.info"))
            .is_ok_and(|info| TableInfo::parse(&info).table_type == "Calibration")
    }))
}

#[derive(Serialize)]
struct PlotResult {
    samples: usize,
    plots: Vec<crate::plot_document::PlotFigure>,
}

struct PlotSeries {
    x: Axis,
    y: Axis,
    points: Vec<Point>,
}

pub(super) fn run(
    task: &TaskInvocation<'_>,
    path: &Path,
    payload: Option<&Path>,
) -> Result<TaskSuccess, TaskFailure> {
    validate_pending_controls(task)?;
    let output = task
        .text("plotfile")?
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let overwrite = task.flag("overwrite")?;
    super::task_contract::refuse_unwritable_outputs(
        &[("plotfile", output.as_deref())],
        overwrite,
        "set overwrite=True to replace it",
    )
    .map_err(TaskFailure::validation)?;
    let native = Table::open(TableOptions::new(path)).map_err(|e| {
        TaskFailure::execution(format!("open calibration table {}: {e}", path.display()))
    })?;
    let table = CalibrationTable::read(native, path).map_err(TaskFailure::execution)?;
    let use_selection = task.flag("selectdata")?;
    let expression = |name| {
        if use_selection {
            Ok(task.text(name)?.unwrap_or_default())
        } else {
            Ok::<_, TaskFailure>(String::new())
        }
    };
    let antenna = expression("antenna")?;
    let field = expression("field")?;
    let spw = expression("spw")?;
    let scan = expression("scan")?;
    let timerange = expression("timerange")?;
    let observation = expression("observation")?;
    let mut polarization = expression("correlation")?;
    if polarization.is_empty() {
        polarization = expression("polarization")?;
    }
    let expressions = Expressions {
        antenna: &antenna,
        field: &field,
        spw: &spw,
        scan: &scan,
        timerange: &timerange,
        observation: &observation,
    };
    let selection = Selection::resolve(&table, &expressions).map_err(TaskFailure::validation)?;
    let averaging = averaging::options(task)?;
    let [default_x, default_y] = default_axes(&table.subtype);
    let x = task
        .text("xaxis")?
        .filter(|value| !value.is_empty())
        .map_or(Ok(default_x), |value| Axis::parse(&value))
        .map_err(TaskFailure::validation)?;
    let ys = axes(task.value("yaxis"), default_y).map_err(TaskFailure::validation)?;
    let series = ys
        .into_iter()
        .map(|y| {
            let request = SeriesRequest {
                x,
                y,
                polarization: &polarization,
                averaging: &averaging,
            };
            let points =
                samples::points(&table, &selection, &request).map_err(TaskFailure::validation)?;
            Ok(PlotSeries { x, y, points })
        })
        .collect::<Result<Vec<_>, TaskFailure>>()?;
    let expformat = task.text("expformat")?.unwrap_or_default();
    let mut figure_path = output;
    if let Some(destination) = &figure_path {
        let format = if expformat.is_empty() {
            destination
                .extension()
                .and_then(OsStr::to_str)
                .unwrap_or("")
        } else {
            &expformat
        };
        if format.eq_ignore_ascii_case("txt") {
            let plotindex = task.optional_number("plotindex")?.unwrap_or_default() as usize;
            let header = ExportHeader {
                path,
                expressions,
                polarization,
                averaging: &averaging,
                plotindex,
                verbose: task.flag("verbose")?,
            };
            write_export(destination, overwrite, &table, &header, &series).map_err(|e| {
                TaskFailure::execution(format!("write {}: {e}", destination.display()))
            })?;
            figure_path = None;
        }
    }
    let show = task.flag("showgui")?;
    let plots = match payload {
        Some(payload) => vec![figure::prepare(task, payload, figure_path, show, &series)?],
        None if figure_path.is_some() => {
            return Err(TaskFailure::validation(
                "use casars.tasks.plotms for rendered figures",
            ));
        }
        None => Vec::new(),
    };
    let samples = series.iter().map(|line| line.points.len()).sum();
    TaskSuccess::new(CasaValue::None, &PlotResult { samples, plots })
}

fn validate_pending_controls(task: &TaskInvocation<'_>) -> Result<(), TaskFailure> {
    let pending = |name| {
        TaskFailure::validation(format!(
            "PlotMS calibration control {name} is pending #1526"
        ))
        .at(name)
    };
    for name in ["iteraxis", "headeritems", "freqframe", "restfreq"] {
        if task.text(name)?.is_some_and(|value| !value.is_empty()) {
            return Err(pending(name));
        }
    }
    if task.flag("selectdata")? {
        for name in ["uvrange", "antpos", "array", "intent", "feed", "msselect"] {
            if task.text(name)?.is_some_and(|value| !value.is_empty()) {
                return Err(pending(name));
            }
        }
    }
    for name in ["gridrows", "gridcols"] {
        if task.integer(name)? != 1 {
            return Err(pending(name));
        }
    }
    Ok(())
}

fn axes(value: Option<&ParameterValue>, default: Axis) -> Result<Vec<Axis>, String> {
    let values = axis_names(value)?;
    if values.is_empty() {
        return Ok(vec![default]);
    }
    values.iter().map(|name| Axis::parse(name)).collect()
}

pub(super) fn axis_names(value: Option<&ParameterValue>) -> Result<Vec<&str>, String> {
    fn text(value: &ParameterValue) -> Result<&str, String> {
        match value {
            ParameterValue::String(text) => Ok(text),
            _ => Err("yaxis requires an axis name or a list of axis names".into()),
        }
    }
    match value {
        Some(ParameterValue::Array(items)) => items.iter().map(text).collect(),
        Some(ParameterValue::String(name)) if name.is_empty() => Ok(Vec::new()),
        Some(value) => Ok(vec![text(value)?]),
        None => Ok(Vec::new()),
    }
}

fn default_axes(subtype: &str) -> [Axis; 2] {
    use Axis::*;
    match subtype {
        "B Jones" | "BPOLY" => [Channel, Amplitude],
        "K Jones" => [Antenna1, Amplitude],
        subtype if subtype.contains("TSYS") => [Frequency, SystemTemperature],
        "TOpac" => [Time, Opacity],
        "F Jones" => [Time, Tec],
        _ => [Time, Amplitude],
    }
}

struct ExportHeader<'a> {
    path: &'a Path,
    expressions: Expressions<'a>,
    polarization: String,
    averaging: &'a crate::MsAverageSpec,
    plotindex: usize,
    verbose: bool,
}

fn write_export(
    destination: &Path,
    overwrite: bool,
    table: &CalibrationTable,
    header: &ExportHeader<'_>,
    series: &[PlotSeries],
) -> std::io::Result<()> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let file = tempfile::NamedTempFile::new_in(parent)?;
    let mut output = BufWriter::new(file.as_file());
    writeln!(
        output,
        "# file: {}",
        header
            .path
            .file_name()
            .unwrap_or_else(|| header.path.as_os_str())
            .to_string_lossy()
    )?;
    let selections = &header.expressions;
    for (name, value) in [
        ("field", selections.field),
        ("spw", selections.spw),
        ("timerange", selections.timerange),
        ("antenna", selections.antenna),
        ("scan", selections.scan),
        ("correlation/polarization", header.polarization.as_str()),
        ("observation", selections.observation),
    ] {
        if !value.is_empty() {
            writeln!(output, "# {name}: {value}")?;
        }
    }
    if averaging::active(header.averaging) {
        let average = header.averaging;
        match average.avgchannel {
            Some(channels) => writeln!(output, "# channel average: {channels}")?,
            None => writeln!(output, "# channel average: None")?,
        }
        if let Some(time) = average.avgtime {
            write!(output, "# time average: {time}s")?;
            if average.avgscan {
                write!(output, " scan: True")?;
            }
            if average.avgfield {
                write!(output, " field: True")?;
            }
            writeln!(output)?;
        } else {
            writeln!(output, "# time average: None")?;
        }
        for (name, enabled) in [
            ("baseline", average.avgbaseline),
            ("antenna", average.avgantenna),
            ("spw", average.avgspw),
            ("scalar", average.scalar),
        ] {
            if enabled {
                writeln!(output, "# {name} average: True")?;
            }
        }
    }
    writeln!(output, "# From plot {}", header.plotindex)?;
    for series in series {
        let mut points = series
            .points
            .iter()
            .filter(|point| !point.flagged)
            .peekable();
        text::axes_header(
            &mut output,
            series.x,
            series.y,
            header.verbose,
            points.peek().is_some(),
        )?;
        for point in points {
            text::point(
                &mut output,
                table,
                point,
                [series.x, series.y],
                header.verbose,
            )?;
        }
    }
    output.flush()?;
    drop(output);
    if overwrite {
        file.persist(destination)?;
    } else {
        file.persist_noclobber(destination)?;
    }
    Ok(())
}
