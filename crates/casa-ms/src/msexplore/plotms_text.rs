// SPDX-License-Identifier: LGPL-3.0-or-later
//! Text coordinates and provenance for MeasurementSet PlotMS.

use std::collections::{HashMap, hash_map::Entry};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use casa_task_runtime::{TaskFailure, TaskInvocation};

use super::calibration::{
    averaging,
    text::{significant, write_averaging_header},
};
use super::plotms_atmosphere::Curve;
use super::{
    MsAxis, MsPlotPayload, MsScatterPlotMsMetadata, MsScatterPointRef, MsScatterSeries, listobs,
};
use crate::MeasurementSet;
use crate::columns::main_ids;
use crate::columns::time_columns::TimeColumn;

pub(super) fn write_export(
    task: &TaskInvocation<'_>,
    ms: &MeasurementSet,
    payload: &MsPlotPayload,
    output: &Path,
    library: &[String],
    overlays: &[Vec<Curve>],
) -> Result<(), TaskFailure> {
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let io = |error: std::io::Error| {
        TaskFailure::execution(format!("write PlotMS export {}: {error}", output.display()))
    };
    let file = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
    let mut writer = BufWriter::new(file.as_file());
    write_header(&mut writer, task, &task.path("vis")?, library)?;
    let index = task.integer("plotindex")?;
    let verbose = task.flag("verbose")?;
    let mut metadata = Metadata::new(ms).map_err(io)?;
    metadata.averaged_baselines = task.flag("averagedata")? && task.flag("avgbaseline")?;
    match payload {
        MsPlotPayload::Scatter(plot) => {
            writeln!(writer, "# From plot {index}").map_err(io)?;
            if plot.series.is_empty() {
                for y in std::iter::once(plot.y_axis).chain(plot.secondary_y_axis) {
                    write_empty_section(&mut writer, name(plot.x_axis), name(y), verbose)
                        .map_err(io)?;
                }
            } else {
                write_series(
                    &mut writer,
                    &mut metadata,
                    plot.x_axis,
                    &plot.series,
                    verbose,
                    library.is_empty(),
                )
                .map_err(io)?;
            }
            if let Some(curves) = overlays.first() {
                write_curves(&mut writer, &mut metadata, plot.x_axis, curves, verbose)
                    .map_err(io)?;
            }
        }
        MsPlotPayload::ScatterGrid(grid) => {
            let axis = super::plotms_figure::iteration_name(grid.iteraxis);
            writeln!(writer, "# iteraxis: {axis}").map_err(io)?;
            let [rows, columns] = super::plotms_figure::requested_grid(task)?;
            let capacity = (rows as usize).saturating_mul(columns as usize);
            for (panel, plot) in grid.panels.iter().take(capacity).enumerate() {
                if panel == 0 {
                    writeln!(writer, "# From plot {index}").map_err(io)?;
                } else {
                    writeln!(writer, "# From plot {panel} iteration {}", panel + 1).map_err(io)?;
                }
                write_series(
                    &mut writer,
                    &mut metadata,
                    grid.x_axis,
                    &plot.series,
                    verbose,
                    library.is_empty(),
                )
                .map_err(io)?;
                if let Some(curves) = overlays.get(panel) {
                    write_curves(&mut writer, &mut metadata, grid.x_axis, curves, verbose)
                        .map_err(io)?;
                }
            }
        }
        _ => {
            return Err(TaskFailure::validation(
                "PlotMS calibration export requires scatter data",
            ));
        }
    }
    writer.flush().map_err(io)?;
    drop(writer);
    if task.flag("overwrite")? {
        file.persist(output).map_err(|error| io(error.error))?;
    } else {
        file.persist_noclobber(output)
            .map_err(|error| io(error.error))?;
    }
    Ok(())
}

fn write_header(
    output: &mut dyn Write,
    task: &TaskInvocation<'_>,
    vis: &Path,
    library: &[String],
) -> Result<(), TaskFailure> {
    let io = |error: std::io::Error| TaskFailure::execution(error.to_string());
    writeln!(
        output,
        "# file: {}",
        vis.file_name().unwrap_or(vis.as_os_str()).to_string_lossy()
    )
    .map_err(io)?;
    if task.flag("selectdata")? {
        for (name, label) in [
            ("field", "field"),
            ("spw", "spw"),
            ("timerange", "timerange"),
            ("uvrange", "uvrange"),
            ("antenna", "antenna"),
            ("scan", "scan"),
            ("correlation", "correlation/polarization"),
            ("polarization", "correlation/polarization"),
            ("array", "array"),
            ("observation", "observation"),
            ("intent", "intent"),
            ("feed", "feed"),
            ("msselect", "msselect"),
        ] {
            if let Some(value) = task.text(name)?.filter(|value| !value.is_empty()) {
                writeln!(output, "# {label}: {value}").map_err(io)?;
            }
        }
    }
    let mut average = averaging::options(task)?;
    let requested_channels = if task.flag("averagedata")? {
        // CASA records the original request even when zero disables binning.
        if average.avgtime.is_none() {
            average.avgtime = task
                .text("avgtime")?
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|&value| value == 0.0);
        }
        task.text("avgchannel")?
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|&value| value >= 0.0)
    } else {
        None
    };
    write_averaging_header(output, &average, requested_channels).map_err(io)?;
    if library.is_empty() {
        return Ok(());
    }
    let label = match library {
        [filename] if !filename.contains('=') => std::path::absolute(filename)
            .map_err(io)?
            .display()
            .to_string(),
        entries => entries.join(","),
    };
    writeln!(output, "# cal library: {label}").map_err(io)
}

fn write_series(
    output: &mut dyn Write,
    metadata: &mut Metadata<'_>,
    x_axis: MsAxis,
    data: &[MsScatterSeries],
    verbose: bool,
    channel_major: bool,
) -> std::io::Result<()> {
    for section in
        data.chunk_by(|left, right| left.y_index == right.y_index && left.y_axis == right.y_axis)
    {
        let y_axis = section[0].y_axis;
        write_section(output, name(x_axis), name(y_axis), verbose)?;
        let scalar_scan_fields = section[0].plotms_averaged_scan_fields.as_deref();
        let mut points = section.iter().flat_map(|series| {
            series
                .points
                .iter()
                .zip(&series.provenance)
                .enumerate()
                .map(|(index, (xy, point))| (xy, point, series.plotms_metadata.get(index).copied()))
        });
        let write = |(&(x, y), point, display)| {
            let display: Option<MsScatterPlotMsMetadata> = display;
            let scan_field =
                scalar_scan_fields.and_then(|ids| ids.get(display?.averaging_group?).copied());
            write_point(
                output,
                metadata,
                (coordinate(x_axis, x), coordinate(y_axis, y)),
                point,
                (display, scan_field),
                None,
                verbose,
            )
        };
        if channel_major
            || x_axis.is_interval_or_wavelength()
            || y_axis.is_interval_or_wavelength()
            || y_axis == MsAxis::UvDistance
        {
            let mut ordered = points.collect::<Vec<_>>();
            ordered.sort_unstable_by_key(|&(_, point, display)| {
                point.plotms_order(display.and_then(|metadata| metadata.averaging_group))
            });
            ordered.into_iter().try_for_each(write)?;
        } else {
            points.try_for_each(write)?;
        }
    }
    Ok(())
}

fn write_curves(
    output: &mut dyn Write,
    metadata: &mut Metadata<'_>,
    x_axis: MsAxis,
    curves: &[Curve],
    verbose: bool,
) -> std::io::Result<()> {
    for curve in curves {
        write_section(output, name(x_axis), curve.axis.label(), verbose)?;
        for sample in &curve.samples {
            write_point(
                output,
                metadata,
                (coordinate(x_axis, sample.x), significant(sample.y)),
                &sample.point,
                (Some(sample.display), None),
                Some(sample.frequency),
                verbose,
            )?;
        }
    }
    Ok(())
}

fn write_section(output: &mut dyn Write, x: &str, y: &str, verbose: bool) -> std::io::Result<()> {
    if verbose {
        writeln!(
            output,
            "# x y chan scan field ant1 ant2 ant1name ant2name time freq spw poln obs"
        )?;
        writeln!(
            output,
            "# {x} {y} None None None None None None None MJD(seconds) GHz None None None"
        )
    } else {
        writeln!(output, "# x y ")?;
        writeln!(output, "# {x} {y}")
    }
}

fn write_empty_section(
    output: &mut dyn Write,
    x: &str,
    y: &str,
    verbose: bool,
) -> std::io::Result<()> {
    if verbose {
        writeln!(output, "# x y scan field ant1 ant1name time spw obs")?;
        writeln!(
            output,
            "# {x} {y} None None None None MJD(seconds) None None None"
        )?;
    } else {
        writeln!(output, "# x y ")?;
        writeln!(output, "# {x} {y}")?;
    }
    writeln!(output, "# No data to export")
}

fn write_point(
    output: &mut dyn Write,
    metadata: &mut Metadata<'_>,
    (x, y): (String, String),
    point: &MsScatterPointRef,
    (display, scan_field): (Option<MsScatterPlotMsMetadata>, Option<(i32, i32)>),
    frequency: Option<f64>,
    verbose: bool,
) -> std::io::Result<()> {
    if !verbose {
        return writeln!(output, "{x} {y}");
    }
    let averaged_baselines = metadata.averaged_baselines;
    let row = metadata.row(point.row)?;
    let (scan, field) = scan_field.unwrap_or((row.scan, row.field));
    let (ant1, ant2, ant1name, ant2name) = if averaged_baselines {
        (-1, -1, "*", "*")
    } else {
        (
            row.ant1,
            row.ant2,
            row.ant1name.as_str(),
            row.ant2name.as_str(),
        )
    };
    let frequency = frequency.unwrap_or_else(|| {
        row.frequencies
            .get(point.chan_start..point.chan_end)
            .filter(|values| !values.is_empty())
            .map_or(f64::NAN, |values| {
                values.iter().sum::<f64>() / values.len() as f64
            })
    }) / 1e9;
    let pol = row
        .correlations
        .get(point.corr)
        .map_or("?", |&code| listobs::stokes_name(code));
    writeln!(
        output,
        "{x} {y} {} {scan} {field} {} {} {} {} {:.3} {frequency:.9} {} {pol} {}",
        display.map_or(point.chan_start, |point| point.channel),
        ant1,
        ant2,
        ant1name,
        ant2name,
        display
            .and_then(|point| point.average_time)
            .unwrap_or(row.time),
        row.spw,
        row.observation,
    )
}

pub(super) fn name(axis: MsAxis) -> &'static str {
    match axis {
        MsAxis::Amplitude => "Amp",
        MsAxis::Phase => "Phase",
        MsAxis::Real => "Real",
        MsAxis::Imaginary => "Imag",
        MsAxis::Time => "Time",
        MsAxis::Interval => "Interval",
        MsAxis::Channel => "Channel",
        MsAxis::Frequency => "Frequency",
        MsAxis::Velocity => "Velocity",
        MsAxis::Scan => "Scan",
        MsAxis::Field => "Field",
        MsAxis::SpectralWindow => "Spw",
        MsAxis::Correlation => "Corr",
        MsAxis::Baseline => "Baseline",
        MsAxis::Row
        | MsAxis::Antenna1
        | MsAxis::Antenna2
        | MsAxis::Observation
        | MsAxis::Intent
        | MsAxis::Feed1
        | MsAxis::Feed2 => axis.display_name(),
        MsAxis::Antenna => "Antenna",
        MsAxis::UvDistance => "UVdist",
        MsAxis::U => "U",
        MsAxis::V => "V",
        MsAxis::W => "W",
        MsAxis::UvWave => "UVwave",
        MsAxis::UWave => "Uwave",
        MsAxis::VWave => "Vwave",
        MsAxis::WWave => "Wwave",
        MsAxis::Azimuth => "Azimuth",
        MsAxis::Elevation => "Elevation",
        MsAxis::HourAngle => "HourAngle",
        MsAxis::ParallacticAngle => "ParAngle",
        MsAxis::Weight => "Wt",
        MsAxis::Sigma => "Sigma",
        MsAxis::WeightSpectrum => "WtSp",
        MsAxis::SigmaSpectrum => "SigmaSp",
        MsAxis::Flag => "Flag",
        MsAxis::FlagRow => "FlagRow",
        MsAxis::AntRa => "Ant-Ra",
        MsAxis::AntDec => "Ant-Dec",
    }
}

fn coordinate(axis: MsAxis, value: f64) -> String {
    match axis {
        MsAxis::Time => format!("{value:.3}"),
        MsAxis::Frequency => format!("{:.9}", value / 1e9),
        MsAxis::AntRa | MsAxis::AntDec => format!("{value:.5}"),
        _ => significant(value),
    }
}

pub(super) struct Metadata<'a> {
    ms: &'a MeasurementSet,
    time: TimeColumn<'a>,
    antennas: Vec<String>,
    averaged_baselines: bool,
    last_row: Option<(usize, Row)>,
    frequencies: HashMap<i32, Arc<[f64]>>,
    correlations: HashMap<i32, Arc<[i32]>>,
}

pub(super) struct Row {
    ant1: i32,
    ant2: i32,
    ant1name: String,
    ant2name: String,
    pub(super) field: i32,
    pub(super) scan: i32,
    pub(super) spw: i32,
    pub(super) observation: i32,
    pub(super) time: f64,
    frequencies: Arc<[f64]>,
    correlations: Arc<[i32]>,
}

impl<'a> Metadata<'a> {
    pub(super) fn new(ms: &'a MeasurementSet) -> std::io::Result<Self> {
        let antenna = ms.antenna().map_err(std::io::Error::other)?;
        let antennas = (0..antenna.row_count())
            .map(|row| Ok(format!("{}@{}", antenna.name(row)?, antenna.station(row)?)))
            .collect::<crate::MsResult<Vec<_>>>()
            .map_err(std::io::Error::other)?;
        Ok(Self {
            ms,
            time: TimeColumn::new(ms.main_table()),
            antennas,
            averaged_baselines: false,
            last_row: None,
            frequencies: HashMap::new(),
            correlations: HashMap::new(),
        })
    }

    pub(super) fn row(&mut self, row: usize) -> std::io::Result<&Row> {
        if self
            .last_row
            .as_ref()
            .is_none_or(|(index, _)| *index != row)
        {
            let metadata = self.read_row(row)?;
            self.last_row = Some((row, metadata));
        }
        Ok(&self.last_row.as_ref().expect("selected row loaded").1)
    }

    fn read_row(&mut self, row: usize) -> std::io::Result<Row> {
        let ms = self.ms;
        let table = ms.main_table();
        let get =
            |column: main_ids::ScalarIdColumn<'_>| column.get(row).map_err(std::io::Error::other);
        let ddid = get(main_ids::data_desc_id(table))? as usize;
        let dd = ms.data_description().map_err(std::io::Error::other)?;
        let spw = dd.spectral_window_id(ddid).map_err(std::io::Error::other)?;
        let pol = dd.polarization_id(ddid).map_err(std::io::Error::other)?;
        let frequencies = match self.frequencies.entry(spw) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => Arc::clone(
                entry.insert(
                    ms.spectral_window()
                        .and_then(|table| table.chan_freq(spw as usize))
                        .map_err(std::io::Error::other)?
                        .into(),
                ),
            ),
        };
        let correlations = match self.correlations.entry(pol) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => Arc::clone(
                entry.insert(
                    ms.polarization()
                        .and_then(|table| table.corr_type(pol as usize))
                        .map_err(std::io::Error::other)?
                        .into(),
                ),
            ),
        };
        let name = |id: i32| {
            self.antennas
                .get(id as usize)
                .cloned()
                .unwrap_or_else(|| "*".into())
        };
        let ant1 = get(main_ids::antenna1(table))?;
        let ant2 = get(main_ids::antenna2(table))?;
        Ok(Row {
            ant1,
            ant2,
            ant1name: name(ant1),
            ant2name: name(ant2),
            field: get(main_ids::field_id(table))?,
            scan: get(main_ids::scan_number(table))?,
            spw,
            observation: get(main_ids::observation_id(table))?,
            time: self
                .time
                .get_mjd_seconds(row)
                .map_err(std::io::Error::other)?,
            frequencies,
            correlations,
        })
    }
}
