// SPDX-License-Identifier: LGPL-3.0-or-later

//! Coordinate summary table formatting and metadata extraction for `ia.summary`.

use std::fmt::Write;

use casa_coordinates::display::format_stream_value;
use casa_coordinates::{
    Coordinate, CoordinateModel, CoordinateSystem, QualityType, SpectralCoordinate,
};
use casa_core::measures::doppler::{DopplerRef, MDoppler};
use casa_core::measures::frame::MeasFrame;
use casa_core::measures::position::PositionRef;
use casa_core::quanta::{MvTime, Unit};
use casa_core::{ArrayValue, RecordValue, Value};

use super::super::beams::subrecord;
use super::super::coords::{
    format_direction, format_number, format_number_with_precision, nested_axis_display,
    scientific_with_precision as scientific, spectral_display_value, spectral_format_unit,
};
use super::super::error::{ImageToolError, ImageToolResult};

struct Placement {
    index: usize,
    field: String,
    model: CoordinateModel,
    pixels: Vec<Option<usize>>,
    world: Vec<Option<usize>>,
}

fn placements(record: &RecordValue) -> ImageToolResult<Vec<Placement>> {
    CoordinateSystem::record_coordinates(record)?
        .into_iter()
        .map(|coordinate| {
            let index = coordinate
                .field
                .trim_start_matches(char::is_alphabetic)
                .parse::<usize>()
                .map_err(|err| ImageToolError::new(err.to_string()))?;
            Ok(Placement {
                index,
                model: coordinate.model(record)?,
                field: coordinate.field,
                pixels: coordinate.pixel_axes,
                world: coordinate.world_axes,
            })
        })
        .collect()
}

/// Extracted coordinate axis descriptors for `ia.summary`.
#[derive(Debug, Clone)]
pub struct SummaryCoordinateFields {
    /// 0-based reference pixel per pixel axis.
    pub refpix: Vec<f64>,
    /// Axis names in pixel or world order.
    pub axisnames: Vec<String>,
    /// Axis units in pixel or world order.
    pub axisunits: Vec<String>,
    /// Reference value per axis in declared units.
    pub refval: Vec<f64>,
    /// Increment per axis in declared units.
    pub incr: Vec<f64>,
}

/// Extracts coordinate summary descriptor fields from `record`.
pub fn summary_coordinate_fields(
    record: &RecordValue,
    pixel_order: bool,
) -> ImageToolResult<SummaryCoordinateFields> {
    let mut pixels = Vec::new();
    let mut descriptors = Vec::new();
    for coordinate in placements(record)? {
        let model = &coordinate.model;
        if matches!(model, CoordinateModel::System(_))
            && model.n_pixel_axes() != model.n_world_axes()
        {
            return Err(ImageToolError::new(
                "Cannot summarize a nested coordinate system with unequal pixel and world axes",
            ));
        }
        let names = model.axis_names();
        let units = model.declared_axis_units();
        let (values, increments) = model.reference_and_increment_in_declared_units();
        let references = model.reference_pixel();
        for local in 0..model.n_world_axes() {
            let pixel = coordinate.pixels[local];
            if let Some(index) = pixel {
                pixels.push((index, references[local]));
            }
            let axis = if pixel_order {
                pixel
            } else {
                coordinate.world[local]
            };
            if let Some(index) = axis {
                descriptors.push((
                    index,
                    names[local].clone(),
                    units[local].clone(),
                    values[local],
                    increments[local],
                ));
            }
        }
    }
    pixels.sort_by_key(|axis| axis.0);
    descriptors.sort_by_key(|axis| axis.0);
    Ok(SummaryCoordinateFields {
        refpix: pixels.into_iter().map(|axis| axis.1).collect(),
        axisnames: descriptors.iter().map(|axis| axis.1.clone()).collect(),
        axisunits: descriptors.iter().map(|axis| axis.2.clone()).collect(),
        refval: descriptors.iter().map(|axis| axis.3).collect(),
        incr: descriptors.iter().map(|axis| axis.4).collect(),
    })
}

/// Converts frequency into velocity in `km/s` using `doppler` and `rest` frequency.
pub(super) fn velocity(frequency: f64, rest: f64, doppler: DopplerRef) -> f64 {
    MDoppler::new(frequency / rest, DopplerRef::RATIO)
        .convert_to(doppler, &MeasFrame::default())
        .map_or(f64::NAN, |velocity| velocity.value() * 299_792.458)
}

const HEADERS: [&str; 11] = [
    "Axis",
    "Coord",
    "Type",
    "Name",
    "Proj",
    "Shape",
    "Tile",
    "Coord value",
    "at pixel",
    "Coord incr",
    " Units",
];

struct ReportRow {
    order: (usize, usize),
    fields: [String; HEADERS.len()],
    labels_only: bool,
}

fn rows(
    record: &RecordValue,
    shape: &[usize],
    tile: &[usize],
    doppler: DopplerRef,
) -> ImageToolResult<Vec<ReportRow>> {
    let mut rows = Vec::new();
    for coordinate in placements(record)? {
        let model = &coordinate.model;
        let names = model.axis_names();
        let units = model.axis_units();
        let values = model.reference_value();
        let increments = model.increment();
        let refs = model.reference_pixel();
        let pixel_replace = match record.get(&format!("pixelreplace{}", coordinate.index)) {
            Some(Value::Array(ArrayValue::Float64(pixels))) => {
                Some(pixels.iter().copied().collect::<Vec<_>>())
            }
            _ => None,
        };
        let last_pixel = coordinate.pixels.iter().flatten().copied().max();
        for local in 0..model.n_world_axes() {
            if coordinate.world[local].is_none() {
                continue;
            }
            let pixel = coordinate.pixels[local];
            let order = pixel.map_or((last_pixel.unwrap_or(usize::MAX), 2 + local), |axis| {
                (axis, 0)
            });
            let display_pixel = pixel.map_or_else(
                || {
                    pixel_replace
                        .as_ref()
                        .map_or(refs[local], |pixels| pixels[local])
                },
                |_| refs[local],
            );
            let reference = if let (None, Some(replacement)) = (pixel, &pixel_replace) {
                model.to_world(replacement)?[local]
            } else {
                values[local]
            };
            let field = |axis: Option<usize>, lengths: &[usize]| {
                axis.and_then(|axis| lengths.get(axis))
                    .map_or_else(|| " ".into(), usize::to_string)
            };
            let mut row = ReportRow {
                order,
                fields: [
                    pixel.map_or_else(|| "..".into(), |axis| axis.to_string()),
                    coordinate.index.to_string(),
                    model.coordinate_type().to_string(),
                    names[local].clone(),
                    " ".into(),
                    field(pixel, shape),
                    field(pixel, tile),
                    format_number(reference, increments[0], false),
                    format!("{display_pixel:.2}"),
                    if pixel.is_some() {
                        format_number_with_precision(increments[local], increments[0], true, 6)
                    } else {
                        " ".into()
                    },
                    format!(" {}", units[local]),
                ],
                labels_only: false,
            };
            let mut velocity_row = None;
            match model {
                CoordinateModel::Direction(direction) => {
                    row.fields[4] = if direction.is_ncp() {
                        "NCP"
                    } else {
                        direction.projection().name()
                    }
                    .into();
                    let frame = direction.direction_ref();
                    let (text, unit) = format_direction(frame, local, reference, true, false, 3);
                    row.fields[7] = text;
                    if pixel.is_some() {
                        let (text, unit) =
                            format_direction(frame, local, increments[local], false, true, 6);
                        row.fields[9] = text;
                        row.fields[10] = format!(" {unit}");
                    } else {
                        row.fields[10] = format!(" {unit}");
                    }
                }
                CoordinateModel::Stokes(stokes) => {
                    row.fields[7] = if pixel.is_some() {
                        stokes
                            .stokes()
                            .iter()
                            .map(|kind| kind.name())
                            .collect::<Vec<_>>()
                            .join(" ")
                    } else {
                        stokes
                            .stokes()
                            .get(display_pixel.round() as usize)
                            .map_or("?", |kind| kind.name())
                            .to_owned()
                    };
                    row.labels_only = true;
                }
                CoordinateModel::Quality(quality) => {
                    row.fields[7] = if pixel.is_some() {
                        quality
                            .values()
                            .iter()
                            .map(|label| label.name())
                            .collect::<Vec<_>>()
                            .join(" ")
                    } else {
                        QualityType::from_world(reference).name().into()
                    };
                    row.labels_only = true;
                }
                CoordinateModel::Spectral(spectral) => {
                    velocity_row = spectral_row(
                        &mut row,
                        spectral,
                        subrecord(record, &coordinate.field),
                        reference,
                        pixel.map(|_| display_pixel),
                        doppler,
                    )?;
                }
                CoordinateModel::System(_) => {
                    if let Some((value, increment, units)) =
                        nested_axis_display(model, local, reference, increments[local])?
                    {
                        row.fields[7] = value;
                        if pixel.is_some() {
                            row.fields[9] = increment;
                        }
                        row.fields[10] = format!(" {units}");
                    }
                }
                _ => (),
            }
            rows.push(row);
            rows.extend(velocity_row);
        }
    }
    rows.sort_by_key(|row| row.order);
    Ok(rows)
}

fn spectral_row(
    row: &mut ReportRow,
    spectral: &SpectralCoordinate,
    record: Option<&RecordValue>,
    reference: f64,
    pixel: Option<f64>,
    doppler: DopplerRef,
) -> ImageToolResult<Option<ReportRow>> {
    if !spectral.has_linear_mapping() {
        row.fields[3].push_str(" (tab)");
    }
    let native_units = spectral.axis_units();
    let native = &native_units[0];
    let requested = spectral_format_unit(record, native);
    let unit = Unit::new(requested)?;
    let convert =
        |value, absolute| spectral_display_value(spectral, record, value, native, &unit, absolute);
    let increment = convert(spectral.increment()[0], false)?;
    let frequency_units = unit.conformant(&Unit::new("Hz").expect("built-in frequency unit"));
    row.fields[7] = match convert(reference, true)? {
        Some(value) if frequency_units => {
            format_number(value, increment.unwrap_or_default(), false)
        }
        Some(value) => format_stream_value(value),
        None => "Fail".into(),
    };
    row.fields[10] = format!(" {requested}");
    if pixel.is_some() {
        row.fields[9] = match increment {
            Some(value) if frequency_units => format_number_with_precision(value, value, true, 6),
            Some(value) => scientific(value, 6),
            None => "Fail".into(),
        };
    }

    let rest = spectral.rest_frequency();
    if rest <= 0.0 {
        return Ok(None);
    }
    let mut fields: [String; HEADERS.len()] = std::array::from_fn(|_| String::new());
    fields[3] = "Velocity".into();
    fields[7] = format_stream_value(velocity(spectral.reference_value()[0], rest, doppler));
    if let Some(pixel) = pixel {
        fields[8] = row.fields[8].clone();
        let higher = spectral.to_world(&[pixel + 0.5]);
        let lower = spectral.to_world(&[pixel - 0.5]);
        fields[9] = match (higher, lower) {
            (Ok(hi), Ok(lo)) => scientific(
                velocity(hi[0], rest, doppler) - velocity(lo[0], rest, doppler),
                6,
            ),
            _ => "Fail".into(),
        };
        fields[10] = " km/s".into();
    } else {
        fields[10] = " ".into();
    }
    let (axis, offset) = row.order;
    Ok(Some(ReportRow {
        order: (axis, offset + 1),
        fields,
        labels_only: false,
    }))
}

/// Formats CASA's coordinate summary report section (reference frames, observatory, axis table).
pub fn format_coordinate_summary_report(
    record: &RecordValue,
    coords: &CoordinateSystem,
    shape: &[usize],
    tile: &[usize],
    doppler: DopplerRef,
) -> ImageToolResult<String> {
    let mut report = String::from("\n");
    let direction = CoordinateSystem::first_direction_in_record(record)?;
    let spectral = CoordinateSystem::first_spectral_in_record(record)?;
    let obs = coords.obs_info();
    if let Some((_, direction)) = &direction {
        let frame = direction.direction_ref();
        write!(report, "Direction reference : {frame}").unwrap();
        let conversion = direction.conversion_direction_ref();
        if conversion != frame {
            write!(report, " (-> {conversion})").unwrap();
        }
        report.push('\n');
    }
    if let Some((_, spectral)) = spectral {
        let frame = spectral.frequency_ref();
        write!(report, "Spectral  reference : {frame}").unwrap();
        let conversion = spectral.world_frequency_ref();
        if conversion != frame {
            write!(report, " (-> {conversion})").unwrap();
        }
        let doppler_name = match doppler {
            DopplerRef::Z => "OPTICAL",
            DopplerRef::BETA => "TRUE",
            other => other.as_str(),
        };
        writeln!(report, "\nVelocity  type      : {doppler_name}").unwrap();
        let rest = spectral.rest_frequency();
        if rest > 0.0 {
            let unit = &spectral.axis_units()[0];
            write!(
                report,
                "Rest frequency      : {}",
                format_stream_value(rest)
            )
            .unwrap();
            let alternatives = spectral.rest_frequencies();
            if alternatives.len() > 1 {
                let other = alternatives
                    .iter()
                    .copied()
                    .filter(|value| (value - rest).abs() > 1e-13 * rest.abs().max(value.abs()))
                    .map(format_stream_value)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(report, " [{other}]").unwrap();
            }
            writeln!(report, " {unit}").unwrap();
        }
    }
    if let Some((_, direction)) = &direction
        && !obs.pointing_center_initial
    {
        let frame = direction.direction_ref();
        let [longitude, latitude] = obs.pointing_center_rad;
        let (longitude, _) = format_direction(frame, 0, longitude, true, false, 6);
        let (latitude, _) = format_direction(frame, 1, latitude, true, false, 6);
        writeln!(report, "Pointing center     :  {longitude}  {latitude}").unwrap();
    }
    writeln!(report, "Telescope           : {}", obs.telescope).unwrap();
    writeln!(report, "Observer            : {}", obs.observer).unwrap();
    let date = obs
        .date
        .as_ref()
        .filter(|epoch| epoch.value().as_mjd() != 0.0)
        .map_or_else(
            || "UNKNOWN".into(),
            |epoch| {
                let date = MvTime::from_mjd_days(epoch.value().as_mjd());
                format!(
                    "{:04}/{:02}/{:02}/{}",
                    date.year(),
                    date.month(),
                    date.month_day(),
                    date.format_time(6)
                )
            },
        );
    writeln!(report, "Date observation    : {date}").unwrap();
    if let Some(position) = &obs.telescope_position {
        let position = position.convert_to(PositionRef::ITRF)?;
        let [x, y, z] = position.values().map(format_stream_value);
        writeln!(
            report,
            "Telescope position: [{x}m, {y}m, {z}m] ({})",
            position.refer()
        )
        .unwrap();
    }
    report.push('\n');
    let rows = rows(record, shape, tile, doppler)?;
    let widths = std::array::from_fn(|column| {
        let content = rows
            .iter()
            .filter(|row| !row.labels_only || column < 8)
            .map(|row| row.fields[column].chars().count())
            .max()
            .unwrap_or(0);
        HEADERS[column].len().max(content) + usize::from(column < 10)
    });
    write_table_row(&mut report, &HEADERS, &widths, false);
    let width = widths.iter().sum::<usize>();
    writeln!(report, "\n{} ", "-".repeat(width - 1)).unwrap();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            report.push('\n');
        }
        write_table_row(&mut report, &row.fields, &widths, row.labels_only);
    }
    Ok(report)
}

fn write_table_row(
    report: &mut String,
    fields: &[impl AsRef<str>; HEADERS.len()],
    widths: &[usize; HEADERS.len()],
    labels_only: bool,
) {
    for (column, (field, &width)) in fields.iter().zip(widths).enumerate() {
        let field = field.as_ref();
        match column {
            0..=3 => write!(report, "{field:<width$}").unwrap(),
            4..=7 => write!(report, "{field:>width$}").unwrap(),
            _ if labels_only => break,
            10 => report.push_str(field),
            _ => write!(report, "{field:>width$}").unwrap(),
        }
    }
}
