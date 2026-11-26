// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration table input for the shared PlotMS provider.

pub(super) mod atmosphere;
pub(super) mod averaging;
pub(super) mod axis;
pub(super) mod figure;
mod header;
mod iteration;
mod legacy;
mod samples;
mod selection;
mod table_data;
pub(super) mod text;

use std::ffi::OsStr;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use casa_contracts::{CasaValue, ParameterValue};
use casa_tables::{Table, TableInfo, TableOptions};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};
use serde::Serialize;

use self::axis::Axis;
use self::iteration::{IterationAxis, PlotPanels};
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
    text_file: Option<PathBuf>,
    plots: Vec<casa_plot_data::PlotFigure>,
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
    validate_calibration_controls(task)?;
    let header_items = header::items(task)?;
    figure::plot_limits(task.value("plotrange"))?;
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
    let taql = expression("msselect")?;
    let antenna_position = expression("antpos")?;
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
        taql: &taql,
    };
    let averaging = averaging::options(task)?;
    let (native, spline) = legacy::materialize(
        native,
        path,
        &expressions,
        averaging::active(&averaging),
        &polarization,
    )
    .map_err(TaskFailure::execution)?;
    let table = CalibrationTable::read(native, path).map_err(TaskFailure::execution)?;
    if table.position_parameters()
        && !matches!(
            antenna_position.as_str(),
            "" | "X" | "Y" | "Z" | "XY" | "XZ" | "YZ" | "XYZ"
        )
    {
        return Err(
            TaskFailure::validation("Invalid antpos selection for cal table.").at("antpos"),
        );
    }
    let receptor_selection = if table.position_parameters() && !antenna_position.is_empty() {
        &antenna_position
    } else {
        &polarization
    };
    // Legacy GSPLINE selection applies to the source solution rows.
    let spline_selection = Expressions::default();
    let selection = Selection::resolve(
        &table,
        if spline.is_some() {
            &spline_selection
        } else {
            &expressions
        },
    )
    .map_err(TaskFailure::validation)?;
    let [default_x, default_y] = default_axes(&table.subtype);
    let x = task
        .text("xaxis")?
        .filter(|value| !value.is_empty())
        .map_or(Ok(default_x), |value| Axis::parse(&value))
        .map_err(TaskFailure::validation)?;
    let ys = axes(task.value("yaxis"), task.value("ydatacolumn"), default_y)
        .map_err(TaskFailure::validation)?;
    if spline.is_some() {
        legacy::check_spline_axes(std::iter::once(x).chain(ys.iter().copied()))
            .map_err(TaskFailure::validation)?;
    }
    let iteration = IterationAxis::from_task(task)?;
    let series = ys
        .into_iter()
        .map(|y| {
            let request = SeriesRequest {
                x,
                y,
                polarization: receptor_selection,
                averaging: &averaging,
                spline,
                iteration,
            };
            let points =
                samples::points(&table, &selection, &request).map_err(TaskFailure::validation)?;
            Ok(PlotSeries { x, y, points })
        })
        .collect::<Result<Vec<_>, TaskFailure>>()?;
    let samples = series.iter().map(|line| line.points.len()).sum();
    let mut panels = PlotPanels::prepare(task, &table, series, iteration)?;
    if let Some(options) = atmosphere::Options::from_task(task)? {
        atmosphere::append_overlays(
            &table,
            path,
            &selection,
            &expressions,
            &options,
            &mut panels,
            averaging::active(&averaging),
        )?;
    }
    let expformat = task.text("expformat")?.unwrap_or_default();
    let mut figure_path = output;
    let mut text_file = None;
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
                antenna_position,
                averaging: &averaging,
                requested_channels: if task.flag("averagedata")? {
                    task.text("avgchannel")?
                        .and_then(|value| value.parse::<f64>().ok())
                        .filter(|&value| value > 0.0)
                } else {
                    None
                },
                plotindex,
                verbose: task.flag("verbose")?,
                flagged_symbols: task.value("customflaggedsymbol"),
            };
            write_export(destination, overwrite, &table, &header, &panels).map_err(|e| {
                TaskFailure::execution(format!("write {}: {e}", destination.display()))
            })?;
            text_file = figure_path.take();
        }
    }
    let show = task.flag("showgui")?;
    let plots = match payload {
        Some(payload) => figure::prepare(
            task,
            payload,
            figure_path,
            show,
            overwrite,
            &panels,
            |source| header::lines(&header_items, &table, path, source),
        )?,
        None if figure_path.is_some() => {
            return Err(TaskFailure::validation(
                "use casars.tasks.plotms for rendered figures",
            ));
        }
        None => Vec::new(),
    };
    TaskSuccess::new(
        CasaValue::None,
        &PlotResult {
            samples,
            plots,
            text_file,
        },
    )
}

fn validate_calibration_controls(task: &TaskInvocation<'_>) -> Result<(), TaskFailure> {
    if task.flag("transform")?
        && (task
            .text("freqframe")?
            .is_some_and(|value| !value.is_empty())
            || task
                .text("restfreq")?
                .is_some_and(|value| !value.is_empty())
            || !task.required_text("veldef")?.eq_ignore_ascii_case("RADIO"))
    {
        tracing::warn!("Transformations ignored: not supported for calibration tables.");
    }
    if task.flag("selectdata")? {
        for name in ["uvrange", "array", "intent", "feed"] {
            if task.text(name)?.is_some_and(|value| !value.is_empty()) {
                return Err(TaskFailure::validation(format!(
                    "Selection by {name} not supported for NewCalTable"
                ))
                .at(name));
            }
        }
    }
    Ok(())
}

fn axes(
    value: Option<&ParameterValue>,
    data_columns: Option<&ParameterValue>,
    default: Axis,
) -> Result<Vec<Axis>, String> {
    let values = axis_names(value)?;
    if values.is_empty() {
        return Ok(vec![default]);
    }
    let columns = if matches!(data_columns, Some(ParameterValue::Array(_))) {
        axis_names(data_columns)?
    } else {
        Vec::new()
    };
    let mut chosen = Vec::with_capacity(values.len());
    for (index, name) in values.iter().enumerate() {
        let ordinate = Axis::parse(name)?;
        let column = columns.get(index).copied().unwrap_or_default();
        if chosen.contains(&(ordinate, column)) {
            return Err("Remove duplicate y-axes.".into());
        }
        chosen.push((ordinate, column));
    }
    Ok(chosen.into_iter().map(|(ordinate, _)| ordinate).collect())
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
        "B Jones" => [Channel, Amplitude],
        "BPOLY" => [Frequency, Amplitude],
        "K Jones" => [Antenna1, Amplitude],
        subtype if subtype.starts_with("KAntPos") => [Antenna1, Amplitude],
        subtype if subtype.contains("TSYS") => [Channel, Amplitude],
        "TOpac" => [Time, Opacity],
        "F Jones" => [Time, Tec],
        _ => [Time, Amplitude],
    }
}

struct ExportHeader<'a> {
    path: &'a Path,
    expressions: Expressions<'a>,
    polarization: String,
    antenna_position: String,
    averaging: &'a casa_plot_data::MsAverageSpec,
    requested_channels: Option<f64>,
    plotindex: usize,
    verbose: bool,
    flagged_symbols: Option<&'a ParameterValue>,
}

fn write_export(
    destination: &Path,
    overwrite: bool,
    table: &CalibrationTable,
    header: &ExportHeader<'_>,
    panels: &PlotPanels,
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
            .unwrap_or(header.path.as_os_str())
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
        ("msselect", selections.taql),
        ("antenna position", header.antenna_position.as_str()),
    ] {
        if !value.is_empty() {
            writeln!(output, "# {name}: {value}")?;
        }
    }
    text::write_averaging_header(&mut output, header.averaging, header.requested_channels)?;
    if let Some(axis) = panels.axis_name() {
        writeln!(output, "# iteraxis: {axis}")?;
    }
    for (index, panel) in panels.panels.iter().take(panels.capacity()).enumerate() {
        if index == 0 {
            writeln!(output, "# From plot {}", header.plotindex)?;
        } else {
            writeln!(output, "# From plot {index} iteration {}", index + 1)?;
        }
        for (index, series) in panel.series.iter().chain(&panel.overlay).enumerate() {
            let show_flagged = figure::shows_flagged(header.flagged_symbols, index);
            let mut points = series
                .points
                .iter()
                .filter(|point| show_flagged || !point.flagged)
                .peekable();
            text::axes_header(
                &mut output,
                series.x,
                series.y,
                header.verbose,
                points.peek().is_some(),
                table,
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
