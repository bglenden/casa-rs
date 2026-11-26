// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's public PlotMS text export for selected calibration samples.

use std::io::{self, Write};

use super::axis::Axis;
use super::samples::Point;
use super::table_data::CalibrationTable;

pub(super) fn axes_header(
    output: &mut dyn Write,
    x: Axis,
    y: Axis,
    verbose: bool,
    has_points: bool,
    table: &CalibrationTable,
) -> io::Result<()> {
    let (columns, units) = match (verbose, has_points) {
        (true, true) if table.subtype == "BPOLY" => (
            "chan scan field ant1 ant1name time freq spw poln obs",
            " None None None None None MJD(seconds) GHz None None None",
        ),
        (true, true) if table.subtype == "GSPLINE" => (
            "scan field ant1 ant1name time spw poln obs",
            " None None None None MJD(seconds) None None None",
        ),
        (true, true) if table.position_parameters() => (
            "chan scan field ant1 ant2 ant1name ant2name time freq spw antpos obs",
            " None None None None None None None MJD(seconds) GHz None None None",
        ),
        (true, true) => (
            "chan scan field ant1 ant2 ant1name ant2name time freq spw poln obs",
            " None None None None None None None MJD(seconds) GHz None None None",
        ),
        (true, false) => (
            "scan field ant1 ant1name time spw ",
            " None None None None MJD(seconds) None None None",
        ),
        (false, _) => ("", ""),
    };
    writeln!(output, "# x y {columns}")?;
    writeln!(output, "# {} {}{units}", x.label(), y.label())?;
    if !has_points {
        writeln!(output, "# No data to export")?;
    }
    Ok(())
}

pub(super) fn point(
    output: &mut dyn Write,
    table: &CalibrationTable,
    point: &Point,
    axes: [Axis; 2],
    verbose: bool,
) -> io::Result<()> {
    let x = coordinate(axes[0], point.x);
    let y = coordinate(axes[1], point.y);
    if !verbose {
        return writeln!(output, "{x} {y}");
    }
    let metadata = point.coordinates;
    let polarization = if table.position_parameters() {
        CalibrationTable::position_label(metadata.receptor).to_owned()
    } else if metadata.receptor < 0 {
        "/".to_string()
    } else {
        metadata.receptor.to_string()
    };
    if matches!(table.subtype.as_str(), "BPOLY" | "GSPLINE") {
        let name = usize::try_from(metadata.antennas[0])
            .ok()
            .and_then(|id| table.antennas.get(id))
            .map_or("*", |antenna| antenna.name.as_str());
        write!(output, "{x} {y}")?;
        if table.subtype == "BPOLY" {
            write!(output, " {}", metadata.channel)?;
        }
        write!(
            output,
            " {} {} {} {name} {:.3}",
            metadata.scan, metadata.field, metadata.antennas[0], metadata.time,
        )?;
        if table.subtype == "BPOLY" {
            write!(output, " {:.9}", metadata.frequency_ghz)?;
        }
        return writeln!(
            output,
            " {} {polarization} {}",
            metadata.window, metadata.observation,
        );
    }
    let names = metadata.antennas.map(|id| {
        usize::try_from(id)
            .ok()
            .and_then(|id| table.antennas.get(id))
            .map_or_else(|| "*".to_string(), |antenna| antenna.label())
    });
    writeln!(
        output,
        "{x} {y} {} {} {} {} {} {} {} {:.3} {:.9} {} {} {}",
        metadata.channel,
        metadata.scan,
        metadata.field,
        metadata.antennas[0],
        metadata.antennas[1],
        names[0],
        names[1],
        metadata.time,
        metadata.frequency_ghz,
        metadata.window,
        polarization,
        metadata.observation,
    )
}

fn coordinate(axis: Axis, value: f64) -> String {
    match axis {
        Axis::Time => format!("{value:.3}"),
        Axis::Frequency => format!("{value:.9}"),
        _ => significant(value),
    }
}

// PlotMS uses the default significant-digit precision of a C++ stream.
pub(in crate::ms::msexplore) fn significant(value: f64) -> String {
    if value.is_nan() {
        return if value.is_sign_negative() {
            "-nan"
        } else {
            "nan"
        }
        .into();
    }
    if !value.is_finite() || value == 0.0 {
        return value.to_string();
    }
    const DECIMAL_DIGITS: i32 = 5;
    let scientific = format!("{value:.5e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("scientific format");
    let exponent: i32 = exponent.parse().expect("scientific exponent");
    if !(-4..=DECIMAL_DIGITS).contains(&exponent) {
        format!("{}e{exponent:+03}", trim_trailing_zeroes(mantissa))
    } else {
        let digits = (DECIMAL_DIGITS - exponent) as usize;
        trim_trailing_zeroes(&format!("{value:.digits$}")).to_string()
    }
}

fn trim_trailing_zeroes(value: &str) -> &str {
    if value.contains('.') {
        value.trim_end_matches('0').trim_end_matches('.')
    } else {
        value
    }
}

pub(in crate::ms::msexplore) fn write_averaging_header(
    output: &mut dyn Write,
    average: &casa_plot_data::MsAverageSpec,
    requested_channels: Option<f64>,
) -> io::Result<()> {
    if requested_channels.is_some() || super::averaging::active(average) {
        match requested_channels {
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
    Ok(())
}
