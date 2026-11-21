// SPDX-License-Identifier: LGPL-3.0-or-later
//! Format extrema positions as CASA's ImageMetaDataBase does.

use casa_coordinates::{CoordinateModel, CoordinateSystem, StokesType};
use casa_types::measures::direction::DirectionRef;
use casa_types::quanta::format::{ANGLE, DIG2, TIME, format_angle};

use super::projection_header::cpp_number_precision;
use crate::ImageError;

pub(super) fn format_position(
    coordinates: &CoordinateSystem,
    position: &[usize],
    spectral_axis: Option<usize>,
) -> Result<String, ImageError> {
    let pixels: Vec<_> = position.iter().map(|&pixel| pixel as f64).collect();
    let world = coordinates.to_world(&pixels)?;
    let mut fields = Vec::with_capacity(world.len());
    for (axis, &value) in world.iter().enumerate() {
        let (index, local) = coordinates.world_axis(axis).expect("dense world axes");
        let coordinate = coordinates.coordinate(index);
        let text = match coordinate {
            CoordinateModel::Direction(direction) => {
                direction_value(direction.direction_ref(), local, value)
            }
            CoordinateModel::Stokes(_) => StokesType::from_code(value.round() as i32)
                .map_or("Undefined", StokesType::name)
                .to_string(),
            CoordinateModel::Spectral(_) => {
                let unit = if coordinates.world_axis_to_pixel(axis) == spectral_axis {
                    coordinate.axis_units().remove(local)
                } else {
                    String::new()
                };
                format!("{}{unit}", number(value, coordinate.increment()[local]))
            }
            _ => number(value, coordinate.increment()[0]),
        };
        fields.push(text);
    }
    Ok(fields.join(" "))
}

fn direction_value(frame: DirectionRef, axis: usize, radians: f64) -> String {
    match frame {
        DirectionRef::HADEC => {
            if axis == 0 {
                let angle = (radians + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU)
                    - std::f64::consts::PI;
                if angle == -std::f64::consts::PI {
                    "-12:00:00.000".into()
                } else {
                    format_angle(angle, TIME | DIG2, 9)
                }
            } else {
                format_angle(radians, ANGLE | DIG2, 9)
            }
        }
        DirectionRef::GALACTIC
        | DirectionRef::SUPERGAL
        | DirectionRef::ECLIPTIC
        | DirectionRef::MECLIPTIC
        | DirectionRef::TECLIPTIC
        | DirectionRef::AZEL
        | DirectionRef::AZELSW
        | DirectionRef::AZELGEO
        | DirectionRef::AZELSWGEO => {
            let mut degrees = radians.to_degrees();
            if axis == 0 {
                degrees = degrees.rem_euclid(360.0);
                if matches!(
                    frame,
                    DirectionRef::ECLIPTIC | DirectionRef::MECLIPTIC | DirectionRef::TECLIPTIC
                ) && degrees >= 180.0
                {
                    degrees -= 360.0;
                }
            }
            format!("{degrees:.6}")
        }
        _ => {
            if axis == 0 {
                format_angle(radians, TIME, 9)
            } else {
                format_angle(radians, ANGLE | DIG2, 9)
            }
        }
    }
}

/// `Coordinate::format` raises default precision to distinguish increments.
fn number(value: f64, increment: f64) -> String {
    let mut precision = 6usize;
    if value.is_finite() && increment.is_finite() && value.fract() != 0.0 {
        let mut needed = 1usize;
        let mut compare = 1.0;
        while value.abs() > compare {
            needed += 1;
            compare *= 10.0;
        }
        if increment.abs() < 1.0 {
            compare = 0.1;
            while increment.abs() < compare {
                needed += 1;
                compare /= 10.0;
            }
        } else {
            let mut adjust = 1usize;
            compare = 1.0;
            while increment.abs() > compare {
                adjust += 1;
                compare *= 10.0;
            }
            if adjust < needed {
                needed -= adjust;
            }
        }
        if needed > 5 {
            precision = needed + 1;
        }
    }
    cpp_number_precision(value, precision)
}
