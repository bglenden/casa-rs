// SPDX-License-Identifier: LGPL-3.0-or-later
//! Input positions, strip widths and length-mode sky geometry.

use std::f64::consts::PI;

use casa_coordinates::{Coordinate, DirectionCoordinate};
use casa_types::measures::{
    MeasFrame,
    direction::{DirectionRef, MDirection},
};
use casa_types::quanta::{Quantity, Unit};

use crate::analysis::imsubimage::region::parse_world_angle;
use crate::{ImageError, ImpvRequest, PvAmount, PvPosition};

pub(super) fn endpoints(
    request: &ImpvRequest,
    coordinate: &DirectionCoordinate,
    allow_pixel_shorthand: bool,
) -> Result<[[f64; 2]; 2], ImageError> {
    let missing = |message: &str| ImageError::CasaSelection(message.into());
    match request
        .mode
        .trim()
        .as_bytes()
        .first()
        .map(u8::to_ascii_lowercase)
    {
        Some(b'c') => {
            let (Some(start), Some(end)) = (&request.start, &request.end) else {
                return Err(missing("coords mode requires start and end"));
            };
            if std::mem::discriminant(start) != std::mem::discriminant(end) {
                return Err(missing("start and end must use the same representation"));
            }
            Ok([
                to_pixel(start, coordinate, allow_pixel_shorthand)?,
                to_pixel(end, coordinate, allow_pixel_shorthand)?,
            ])
        }
        Some(b'l') => {
            let (Some(center), Some(length), Some(pa)) =
                (&request.center, &request.length, &request.pa)
            else {
                return Err(missing("length mode requires center, length and pa"));
            };
            let center = parse_position(center, allow_pixel_shorthand)?;
            let length = match length {
                PvAmount::Whole(n) => pixel_length(*n as f64, &center, coordinate)?,
                PvAmount::Fractional(n) => pixel_length(*n, &center, coordinate)?,
                value => radians(value)?,
            };
            let center = match center {
                Position::Pixel(pixel) => {
                    let [lon, lat] = coordinate.pixel_to_world(pixel)?;
                    MDirection::from_angles(lon, lat, coordinate.conversion_direction_ref())
                }
                Position::Sky(center) => center,
            };
            let (lon, lat) = center.as_angles();
            let pa = radians(pa)?;
            let shift = |angle: f64| {
                let (s, c) = (length / 2.0).sin_cos();
                let latitude = (lat.sin() * c + lat.cos() * s * angle.cos())
                    .clamp(-1.0, 1.0)
                    .asin();
                let longitude =
                    lon + (angle.sin() * s * lat.cos()).atan2(c - lat.sin() * latitude.sin());
                sky_to_pixel(
                    &MDirection::from_angles(longitude, latitude, center.refer()),
                    coordinate,
                )
            };
            Ok([shift(pa)?, shift(pa - PI)?])
        }
        _ => Err(missing("Unsupported value for mode")),
    }
}

fn pixel_length(
    length: f64,
    center: &Position,
    coordinate: &DirectionCoordinate,
) -> Result<f64, ImageError> {
    if !length.is_finite() || (length <= 0.0 && matches!(center, Position::Pixel(_))) {
        return Err(ImageError::CasaSelection("Length must be positive".into()));
    }
    Ok(length * coordinate.increment()[0].abs())
}

pub(super) fn width(
    value: &PvAmount,
    coordinate: &DirectionCoordinate,
) -> Result<usize, ImageError> {
    let width = match value {
        PvAmount::Whole(n) => *n,
        PvAmount::Fractional(_) => 0,
        value => {
            let pixels = radians(value)? / coordinate.increment()[0].abs();
            let nearest = pixels.round();
            let round_up = if nearest.rem_euclid(2.0) == 1.0 && (pixels - nearest).abs() <= 1e-13 {
                nearest
            } else {
                pixels.ceil()
            };
            let round_up = round_up.max(1.0) as i64;
            if round_up % 2 == 0 {
                round_up + 1
            } else {
                round_up
            }
        }
    };
    usize::try_from(width)
        .ok()
        .filter(|n| n % 2 == 1)
        .ok_or_else(|| ImageError::CasaSelection("width must be an odd integer >= 1".into()))
}

fn radians(value: &PvAmount) -> Result<f64, ImageError> {
    let invalid = || ImageError::CasaSelection("expected a value with angular units".into());
    let quantity = match value {
        PvAmount::Angular(text) => Quantity::read(text).map_err(|_| invalid())?,
        PvAmount::Record(record) => crate::beam::quantity_from_record(record)?,
        _ => return Err(invalid()),
    };
    let radians = quantity
        .get_value_in(&Unit::new("rad").expect("radians are built in"))
        .map_err(|_| invalid())?;
    if !radians.is_finite() {
        return Err(invalid());
    }
    Ok(radians)
}

fn to_pixel(
    position: &PvPosition,
    coordinate: &DirectionCoordinate,
    allow_pixel_shorthand: bool,
) -> Result<[f64; 2], ImageError> {
    match parse_position(position, allow_pixel_shorthand)? {
        Position::Pixel(pixel) => Ok(pixel),
        Position::Sky(direction) => sky_to_pixel(&direction, coordinate),
    }
}

enum Position {
    Pixel([f64; 2]),
    Sky(MDirection),
}

fn parse_position(
    position: &PvPosition,
    allow_pixel_shorthand: bool,
) -> Result<Position, ImageError> {
    let invalid =
        || ImageError::CasaSelection("expected two pixel coordinates or a sky direction".into());
    let (frame, [longitude, latitude]) = match position {
        PvPosition::Integral(pixel) => return Ok(Position::Pixel(pixel.map(|value| value as f64))),
        PvPosition::Pixel(pixel) => return Ok(Position::Pixel(*pixel)),
        PvPosition::World([lon, lat]) => (DirectionRef::J2000, [lon.as_str(), lat.as_str()]),
        PvPosition::Text(text) => {
            let fields: Vec<_> = text
                .split(|ch: char| ch == ',' || ch.is_whitespace())
                .filter(|part| !part.is_empty())
                .collect();
            match fields.as_slice() {
                [first, second] => {
                    // Keep the native CLI's original comma-separated pixel shorthand.
                    if allow_pixel_shorthand
                        && text.contains(',')
                        && let (Ok(x), Ok(y)) = (first.parse(), second.parse())
                    {
                        return Ok(Position::Pixel([x, y]));
                    }
                    (DirectionRef::J2000, [*first, *second])
                }
                [frame, lon, lat] => (frame.parse().map_err(|_| invalid())?, [*lon, *lat]),
                _ => return Err(invalid()),
            }
        }
    };
    Ok(Position::Sky(MDirection::from_angles(
        parse_world_angle(longitude)?,
        parse_world_angle(latitude)?,
        frame,
    )))
}

fn sky_to_pixel(
    sky: &MDirection,
    coordinate: &DirectionCoordinate,
) -> Result<[f64; 2], ImageError> {
    let native = sky
        .convert_to(coordinate.conversion_direction_ref(), &MeasFrame::default())
        .map_err(|error| ImageError::CasaSelection(error.to_string()))?;
    let (lon, lat) = native.as_angles();
    Ok(coordinate.world_to_pixel([lon, lat])?)
}
