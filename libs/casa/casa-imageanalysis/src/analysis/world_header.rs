// SPDX-License-Identifier: LGPL-3.0-or-later
//! Format extrema positions as CASA's ImageMetaDataBase does.

use casa_coordinates::{CoordinateModel, CoordinateSystem, StokesType};
use casa_core::measures::direction::DirectionRef;
use casa_core::quanta::format::{ANGLE, DIG2, TIME, format_angle};

use super::projection_header::cpp_number_precision;
use casa_images::ImageError;

pub(super) fn format_position(
    coordinates: &CoordinateSystem,
    position: &[usize],
    spectral_axis: Option<usize>,
) -> Result<String, ImageError> {
    let pixels: Vec<f64> = position
        .iter()
        .take(coordinates.n_pixel_axes())
        .map(|&value| value as f64)
        .collect();
    let world = coordinates.to_world(&pixels)?;
    let mut fields = Vec::with_capacity(world.len());
    for (axis, value) in world.into_iter().enumerate() {
        let (index, local) = coordinates
            .world_axis_owner(axis)
            .expect("existing world axis");
        let coordinate = coordinates.coordinate(index);
        let increments = coordinate.increment();
        let text = match coordinate {
            CoordinateModel::Direction(direction) => direction_value(
                direction.direction_ref(),
                direction.longitude_axis() == Some(local),
                value,
            ),
            CoordinateModel::Stokes(_) => StokesType::from_code(value.round() as i32)
                .map_or("Undefined", StokesType::name)
                .to_owned(),
            CoordinateModel::Spectral(_) => {
                let pixel = coordinates
                    .coordinate_pixel_axes(index)
                    .and_then(|axes| axes.get(local).copied().flatten());
                let unit = if pixel.is_some() && spectral_axis == pixel {
                    coordinate.axis_units()[0].clone()
                } else {
                    String::new()
                };
                format!("{}{unit}", number(value, increments[0]))
            }
            _ => number(value, increments[local]),
        };
        fields.push(text);
    }
    Ok(fields.join(" "))
}

fn direction_value(frame: DirectionRef, longitude: bool, radians: f64) -> String {
    match frame {
        DirectionRef::HADEC => {
            if longitude {
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
            if longitude {
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
            if longitude {
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use casa_coordinates::LinearCoordinate;
    use casa_core::Value;
    use casa_tables::{Table, TableOptions};

    use super::{CoordinateSystem, format_position};

    #[test]
    fn extrema_labels_match_casa_world_order_and_sliced_latitude() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../casacore/casa-images/tests/data/casa-6.7.6/axis_maps");
        for (name, position, expected) in [
            (
                "swapped.image",
                &[2, 3, 1, 4][..],
                "-22.55.03.860 04:35:00.961 Q 1.419e+09Hz",
            ),
            (
                "interleaved.image",
                &[3, 1, 2, 4][..],
                "04:35:00.961 Q -22.55.03.860 1.419e+09Hz",
            ),
            (
                "dropdeg_ra.image",
                &[2, 1, 4][..],
                "-08.44.33.971 Q 1.419e+09Hz",
            ),
        ] {
            let table = Table::open_metadata_only(TableOptions::new(root.join(name))).unwrap();
            let Some(Value::Record(record)) = table.keywords().get("coords") else {
                panic!("{name}: no coordinate record");
            };
            let mapped = CoordinateSystem::from_mapped_record(record).unwrap();
            let last = Some(position.len() - 1);
            assert_eq!(
                format_position(&mapped, position, last).unwrap(),
                expected,
                "{name}"
            );
            if name == "dropdeg_ra.image" {
                let legacy = CoordinateSystem::from_record(record).unwrap();
                assert_eq!(
                    format_position(&legacy, position, last).unwrap(),
                    expected,
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn extrema_use_the_increment_for_each_linear_axis() {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(
            LinearCoordinate::new(2, Vec::new(), Vec::new())
                .with_reference_value(vec![0.0, 1000.0])
                .with_increment(vec![1.0, 1e-5]),
        );
        let label = format_position(&coordinates, &[1, 1], None).unwrap();
        let last = label
            .split_whitespace()
            .last()
            .unwrap()
            .parse::<f64>()
            .unwrap();
        assert_eq!(last, 1000.00001);
    }
}
