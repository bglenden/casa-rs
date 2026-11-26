// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fmt::Write as _;

use casa_coordinates::display::{format_stream_value, format_stream_value_with_precision};
use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateType, DirectionCoordinate, QualityType, StokesType,
};
use casa_core::measures::direction::DirectionRef;
use casa_core::measures::doppler::DopplerRef;
use casa_core::measures::position::PositionRef;
use casa_core::quanta::Unit;
use casa_core::quanta::format::{ANGLE, DIG2, TIME, YMD, format_angle, format_time};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyBool;

use crate::convert::holder::{DOPPLER_CODES, doppler_ref};
use crate::tools::{LogLine, emit};

use super::conversion_frames::FrameName;
use super::doppler::VelocityConversion;
use super::inspect::coordinate_index;
use super::world_reference::nested_axis_display;
use super::{CoordinateState, PyCoordsys};

const COLUMNS: [&str; 9] = [
    "Axis",
    "Coord",
    "Type",
    "Name",
    "Proj",
    "Coord value",
    "at pixel",
    "Coord incr",
    " Units",
];

fn reference_frame(result: &mut String, label: &str, native: &str, current: &str) {
    let _ = write!(result, "{label} reference : {native}");
    if native != current {
        let _ = write!(result, " (-> {current})");
    }
    result.push('\n');
}

fn metadata(state: &CoordinateState, numbers: &[usize], name: &str) -> String {
    let coordinates = &state.coordinates;
    let obs = coordinates.obs_info();
    let first = |kind| {
        numbers
            .iter()
            .enumerate()
            .filter(|(index, _)| coordinates.coordinate(*index).coordinate_type() == kind)
            .min_by_key(|(_, number)| *number)
            .map(|(index, _)| coordinates.coordinate(index))
    };
    let direction = first(CoordinateType::Direction).and_then(|model| match model {
        CoordinateModel::Direction(direction) => Some(direction),
        _ => None,
    });
    let mut output = String::from("\n");
    if let Some(direction) = direction {
        reference_frame(
            &mut output,
            "Direction",
            direction.direction_ref().as_str(),
            direction.conversion_direction_ref().as_str(),
        );
    }
    if let Some(CoordinateModel::Spectral(spectral)) = first(CoordinateType::Spectral) {
        let native = spectral.frequency_ref();
        reference_frame(
            &mut output,
            "Spectral ",
            native.as_str(),
            spectral
                .conversion_frequency_ref()
                .unwrap_or(native)
                .as_str(),
        );
        let _ = writeln!(output, "Velocity  type      : {name}");
        let rest = spectral.rest_frequency();
        if rest > 0.0 {
            let _ = write!(
                output,
                "Rest frequency      : {}",
                format_stream_value(rest)
            );
            if spectral.rest_frequencies().len() > 1 {
                let others = spectral
                    .rest_frequencies()
                    .iter()
                    .filter(|&&value| (value - rest).abs() > 1e-13 * value.abs().max(rest))
                    .map(|&value| format_stream_value(value))
                    .collect::<Vec<_>>();
                let _ = write!(output, " [{}]", others.join(", "));
            }
            let _ = writeln!(output, " {}", spectral.axis_units()[0]);
        }
    }
    if let Some(direction) = direction
        && !obs.pointing_center_initial
    {
        let [longitude, latitude] = obs.pointing_center_rad;
        let _ = writeln!(
            output,
            "Pointing center     :  {}  {}",
            sky_reference(direction, 0, longitude, 6),
            sky_reference(direction, 1, latitude, 6),
        );
    }
    let _ = writeln!(output, "Telescope           : {}", obs.telescope);
    let _ = writeln!(output, "Observer            : {}", obs.observer);
    let date = obs
        .date
        .as_ref()
        .map(|epoch| epoch.value().as_mjd())
        .filter(|mjd| mjd.floor() != 0.0)
        .map_or_else(|| "UNKNOWN".into(), |mjd| format_time(mjd, YMD, 12));
    let _ = writeln!(output, "Date observation    : {date}");
    if let Some(position) = &obs.telescope_position {
        let [x, y, z] = position.values();
        let values = [x, y, z].map(format_stream_value);
        let units = if position.refer() == PositionRef::ITRF {
            ["m"; 3]
        } else {
            ["rad", "rad", "m"]
        };
        let _ = writeln!(
            output,
            "Telescope position: [{}{}, {}{}, {}{}] ({})",
            values[0],
            units[0],
            values[1],
            units[1],
            values[2],
            units[2],
            position.refer(),
        );
    }
    output.push('\n');
    output
}

fn sky_reference(
    direction: &DirectionCoordinate,
    axis: usize,
    value: f64,
    decimals: u32,
) -> String {
    let frame = direction.direction_ref();
    let degree_frame = matches!(
        frame,
        DirectionRef::GALACTIC
            | DirectionRef::SUPERGAL
            | DirectionRef::ECLIPTIC
            | DirectionRef::MECLIPTIC
            | DirectionRef::TECLIPTIC
            | DirectionRef::AZEL
            | DirectionRef::AZELSW
            | DirectionRef::AZELGEO
            | DirectionRef::AZELSWGEO
    );
    if degree_frame {
        let mut degrees = value.to_degrees();
        if axis == 0 {
            degrees = if matches!(
                frame,
                DirectionRef::ECLIPTIC | DirectionRef::MECLIPTIC | DirectionRef::TECLIPTIC
            ) {
                (degrees + 180.0).rem_euclid(360.0) - 180.0
            } else {
                degrees.rem_euclid(360.0)
            };
        }
        return format!("{degrees:.digits$}", digits = decimals as usize);
    }
    let format = match axis {
        0 if frame == DirectionRef::HADEC => TIME | DIG2,
        0 => TIME,
        _ => ANGLE | DIG2,
    };
    format_angle(value, format, 6 + decimals)
}

fn scientific(value: f64, precision: usize) -> String {
    if !value.is_finite() {
        return format_stream_value(value);
    }
    let scientific = format!("{value:.precision$e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("scientific notation");
    let exponent = exponent.parse::<i32>().expect("decimal exponent");
    format!("{mantissa}e{exponent:+03}")
}

fn coordinate_precision(value: f64, increment: f64) -> usize {
    if !value.is_finite() || !increment.is_finite() || value.fract() == 0.0 || increment == 0.0 {
        return 6;
    }
    let magnitude = value.abs();
    let step = increment.abs();
    let mut needed = 1;
    let mut threshold = 1.0;
    while magnitude > threshold {
        needed += 1;
        threshold *= 10.0;
    }
    if step < 1.0 {
        let mut threshold = 0.1;
        while step < threshold {
            needed += 1;
            threshold /= 10.0;
        }
    } else {
        let step_digits = step.log10().ceil() as usize + 1;
        if step_digits < needed {
            needed -= step_digits;
        }
    }
    if needed > 5 { needed + 1 } else { 6 }
}

struct AxisRow {
    fields: [String; 9],
    visible: bool,
    labels_only: bool,
}

impl AxisRow {
    fn write(&self, result: &mut String, widths: &[usize; 9]) {
        for (index, field) in self.fields.iter().enumerate() {
            if self.labels_only && index >= 6 {
                break;
            }
            let width = widths[index];
            let _ = if index < 4 {
                write!(result, "{field:<width$}")
            } else if index < 8 {
                write!(result, "{field:>width$}")
            } else {
                write!(result, "{field}")
            };
        }
        result.push('\n');
    }

    fn update_widths(&self, widths: &mut [usize; 9]) {
        for (width, field) in widths.iter_mut().zip(&self.fields) {
            *width = (*width).max(field.len());
        }
    }
}

fn axis_rows(
    state: &CoordinateState,
    numbers: &[usize],
    doppler: DopplerRef,
) -> PyResult<Vec<AxisRow>> {
    let cs = &state.coordinates;
    let mut rows = Vec::new();
    for pixel in 0..cs.n_pixel_axes() {
        let (index, local) = cs.pixel_axis_owner(pixel).expect("public pixel axis");
        let number = numbers[index];
        rows.extend(coordinate_rows(
            cs,
            index,
            number,
            local,
            Some(pixel),
            doppler,
        )?);
        let pixels = cs.coordinate_pixel_axes(index).expect("placed coordinate");
        if pixels.iter().flatten().copied().max() == Some(pixel) {
            let worlds = cs.coordinate_world_axes(index).expect("placed coordinate");
            for (local, (pixel, world)) in pixels.iter().zip(worlds).enumerate() {
                if pixel.is_none() && world.is_some() {
                    rows.extend(coordinate_rows(cs, index, number, local, None, doppler)?);
                }
            }
        }
    }
    for (index, &number) in numbers.iter().enumerate() {
        let pixels = cs.coordinate_pixel_axes(index).expect("placed coordinate");
        if pixels.iter().all(Option::is_none) {
            let worlds = cs.coordinate_world_axes(index).expect("placed coordinate");
            for (local, world) in worlds.into_iter().enumerate() {
                if world.is_some() {
                    let mut hidden = coordinate_rows(cs, index, number, local, None, doppler)?;
                    for row in &mut hidden {
                        row.visible = false;
                    }
                    rows.extend(hidden);
                }
            }
        }
    }
    Ok(rows)
}

fn coordinate_rows(
    cs: &casa_coordinates::CoordinateSystem,
    index: usize,
    number: usize,
    local: usize,
    pixel: Option<usize>,
    doppler: DopplerRef,
) -> PyResult<Vec<AxisRow>> {
    let model = cs.coordinate(index);
    if matches!(model, CoordinateModel::System(_)) && model.n_pixel_axes() != model.n_world_axes() {
        return Err(PyRuntimeError::new_err(
            "Cannot summarize a nested coordinate system with unequal pixel and world axes",
        ));
    }
    let mut fields = std::array::from_fn(|_| String::new());
    fields[0] = pixel.map_or_else(|| "..".into(), |axis| axis.to_string());
    fields[1] = number.to_string();
    fields[2] = model.coordinate_type().to_string();
    fields[3] = model.axis_names()[local].clone();
    let reference_pixel = model.reference_pixel();
    let fixed = cs
        .local_pixel_replacements(index)
        .unwrap_or(&reference_pixel);
    let ref_pixel = if pixel.is_some() {
        reference_pixel[local]
    } else {
        fixed[local]
    };
    let labels = match model {
        CoordinateModel::Stokes(stokes) => Some(if pixel.is_some() {
            stokes
                .stokes()
                .iter()
                .map(|label| label.name())
                .collect::<Vec<_>>()
                .join(" ")
        } else {
            model
                .to_world(fixed)
                .ok()
                .and_then(|world| StokesType::from_code(world[local] as i32))
                .map_or_else(|| "?".into(), |label| label.name().into())
        }),
        CoordinateModel::Quality(quality) => Some(if pixel.is_some() {
            quality
                .values()
                .iter()
                .map(|label| label.name())
                .collect::<Vec<_>>()
                .join(" ")
        } else {
            model.to_world(fixed).ok().map_or_else(
                || "?".into(),
                |world| QualityType::from_world(world[local]).name().into(),
            )
        }),
        _ => None,
    };
    if let Some(labels) = labels {
        fields[5] = labels;
        return Ok(vec![AxisRow {
            fields,
            visible: true,
            labels_only: true,
        }]);
    }
    let value = if pixel.is_some() {
        model.reference_value()[local]
    } else {
        model
            .to_world(fixed)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?[local]
    };
    let increment = model.increment()[local];
    fields[6] = format!("{ref_pixel:.2}");
    let mut row_increment = increment;
    let mut unit = model.axis_units()[local].clone();
    let mut precision = 6;
    let mut formatted_increment = None;
    match model {
        CoordinateModel::Direction(direction) => {
            fields[4] = if direction.is_ncp() {
                "NCP".into()
            } else {
                direction.projection().projection_type().to_string()
            };
            fields[5] = sky_reference(direction, local, value, 3);
            let hours = direction.direction_ref() == DirectionRef::HADEC && local == 0;
            row_increment = if hours {
                increment.to_degrees() / 15.0
            } else {
                increment.to_degrees() * 3600.0
            };
            unit = if hours { "h".into() } else { "arcsec".into() };
        }
        CoordinateModel::Spectral(spectral) => {
            if !spectral.has_linear_mapping() {
                fields[3].push_str(" (tab)");
            }
            fields[5] = format_stream_value(value);
        }
        _ => {
            fields[5] = format_stream_value_with_precision(
                value,
                coordinate_precision(value, model.increment()[0]),
            );
            precision = coordinate_precision(increment, model.increment()[0]);
            if let Some((value, increment, units)) =
                nested_axis_display(model, local, value, increment)?
            {
                fields[5] = value;
                formatted_increment = Some(increment);
                unit = units;
            }
        }
    }
    fields[7] = if pixel.is_some() {
        formatted_increment.unwrap_or_else(|| scientific(row_increment, precision))
    } else {
        String::new()
    };
    if pixel.is_none() && matches!(model, CoordinateModel::Direction(_)) {
        unit.clear();
    }
    fields[8] = format!(" {unit}");
    let mut rows = vec![AxisRow {
        fields,
        visible: true,
        labels_only: false,
    }];
    if let CoordinateModel::Spectral(spectral) = model
        && spectral.rest_frequency() > 0.0
    {
        let velocity_unit = Unit::new("km/s").expect("standard velocity unit");
        let conversion = VelocityConversion::for_spectral_axis(spectral, &velocity_unit, doppler)?;
        let mut fields = std::array::from_fn(|_| String::new());
        fields[3] = "Velocity".into();
        fields[5] = conversion
            .mixed_velocity(spectral.reference_value()[0])
            .map_or_else(|_| "Fail".into(), format_stream_value);
        if pixel.is_some() {
            fields[6] = format!("{ref_pixel:.2}");
            let velocity_at = |offset| {
                let world = spectral.to_world(&[ref_pixel + offset]).ok()?[0];
                conversion.mixed_velocity(world).ok()
            };
            fields[7] = velocity_at(0.5)
                .zip(velocity_at(-0.5))
                .map_or_else(|| "Fail".into(), |(high, low)| scientific(high - low, 6));
            fields[8] = " km/s".into();
        }
        rows.push(AxisRow {
            fields,
            visible: true,
            labels_only: false,
        });
    }
    Ok(rows)
}

fn coordinate_summary(
    state: &CoordinateState,
    doppler: DopplerRef,
    name: &str,
) -> PyResult<String> {
    let placements = state
        .coordinates
        .coordinates_in_record(&state.record)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
    let numbers = placements.iter().map(coordinate_index).collect::<Vec<_>>();
    let mut output = metadata(state, &numbers, name);
    let rows = axis_rows(state, &numbers, doppler)?;
    let header = AxisRow {
        fields: COLUMNS.map(str::to_owned),
        visible: true,
        labels_only: false,
    };
    let mut widths = COLUMNS.map(str::len);
    for row in &rows {
        row.update_widths(&mut widths);
    }
    for width in &mut widths[..8] {
        *width += 1;
    }
    header.write(&mut output, &widths);
    let _ = writeln!(output, "{} ", "-".repeat(widths.iter().sum::<usize>() - 1));
    for row in rows.into_iter().filter(|row| row.visible) {
        row.write(&mut output, &widths);
    }
    output.pop();
    Ok(output)
}

struct ListFlag(bool);

impl FromPyObject<'_, '_> for ListFlag {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        value
            .cast::<PyBool>()
            .map(|flag| Self(flag.is_true()))
            .map_err(|_| {
                PyTypeError::new_err("in method 'coordsys_summary', argument 3 of type 'bool'")
            })
    }
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (doppler=FrameName::radio_default(), list=ListFlag(true)), text_signature = "($self, doppler='RADIO', list=True)")]
    fn summary(&self, py: Python<'_>, doppler: FrameName, list: ListFlag) -> PyResult<Vec<String>> {
        let name = doppler.into_name("doppler")?;
        let canonical = DOPPLER_CODES.resolve(&name);
        let mut logs = Vec::new();
        if canonical.is_none() {
            logs.push(LogLine::warning("Illegal doppler type, using RADIO"));
        }
        let name = canonical.unwrap_or("RADIO");
        let convention = doppler_ref(name).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        let text = py.detach(|| coordinate_summary(&self.state(), convention, name))?;
        if list.0 {
            logs.push(LogLine::info(text));
            emit(py, "casars.tools.coordsys", &logs)?;
            Ok(vec!["T".into()])
        } else {
            emit(py, "casars.tools.coordsys", &logs)?;
            Ok(vec![text])
        }
    }
}
