// SPDX-License-Identifier: LGPL-3.0-or-later
//! The axes each coordinate of a casacore coordinate-system record owns.
//!
//! casacore's `CoordinateSystem::save` writes, for coordinate `i`, the
//! system world and pixel axes it owns as `worldmap<i>` and `pixelmap<i>`
//! (element `j` is the system axis of the coordinate's axis `j`), and a
//! removed axis as `-(axis + 1)`, with its value in `worldreplace<i>` or
//! `pixelreplace<i>`. `addCoordinate` gives each new coordinate the next
//! axes, so a system built coordinate by coordinate has consecutive maps.
//! `CoordinateSystem::transpose` (`imtrans`, `ia.transpose`,
//! `coordsys.transpose`) keeps the coordinates in place and permutes the
//! maps.
//!
//! A [`CoordinateSystem`](super::CoordinateSystem) holds its coordinates
//! over consecutive axes in coordinate order. [`coordinate_order`] finds the
//! order of a record's coordinates that gives that layout. It refuses a
//! record that no order can give: one where a coordinate's axes are out of
//! the coordinate's own order or not adjacent, where the world axes are
//! ordered unlike the pixel axes, or where a coordinate has removed axes.

use casa_types::RecordValue;

use crate::coordinate::CoordinateModel;
use crate::error::CoordinateError;
use crate::record_utils::get_required_vec_i32;

/// What casa-rs reads, and the issue for the rest, for every refusal.
const SUPPORTED: &str = "casa-rs reads a coordinate system only when each coordinate's axes are \
     adjacent and in the coordinate's own order (#1002)";

/// One coordinate's `pixelmap<i>` and `worldmap<i>`.
struct AxisMaps {
    pixel: Vec<i32>,
    world: Vec<i32>,
}

/// The order of `coordinates`, read from `rec` as `(record index, field
/// name, coordinate)` in record index order, in which they own consecutive
/// pixel and world axes.
///
/// A record without axis maps keeps its coordinate order. casa-rs's own
/// records always have them, and so do casacore's.
pub(super) fn coordinate_order(
    rec: &RecordValue,
    coordinates: &[(usize, String, CoordinateModel)],
) -> Result<Vec<usize>, CoordinateError> {
    let maps = coordinates
        .iter()
        .map(|(index, name, coordinate)| axis_maps(rec, *index, name, coordinate))
        .collect::<Result<Vec<_>, _>>()?;
    if maps.iter().all(Option::is_none) {
        return Ok((0..coordinates.len()).collect());
    }
    let maps = maps
        .into_iter()
        .zip(coordinates)
        .map(|(map, (index, name, _))| {
            map.ok_or_else(|| {
                CoordinateError::InvalidRecord(format!(
                    "{name} has no pixelmap{index} or worldmap{index}, which the record's \
                     other coordinates have"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    check_numbering(maps.iter().map(|map| map.pixel.as_slice()), "pixel")?;
    check_numbering(maps.iter().map(|map| map.world.as_slice()), "world")?;
    for (map, (_, name, coordinate)) in maps.iter().zip(coordinates) {
        check_run(name, coordinate, &map.pixel, "pixel")?;
        check_run(name, coordinate, &map.world, "world")?;
    }

    // Each map is now a run, so ordering the coordinates by their first
    // pixel axis makes the pixel maps consecutive. The world maps follow
    // only if the runs come in the same order.
    let mut order = (0..coordinates.len()).collect::<Vec<_>>();
    order.sort_by_key(|&k| maps[k].pixel.first().copied());
    let mut world_order = order.clone();
    world_order.sort_by_key(|&k| maps[k].world.first().copied());
    if world_order != order {
        let names = |order: &[usize]| {
            order
                .iter()
                .map(|&k| coordinates[k].1.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        return Err(CoordinateError::UnsupportedAxisLayout(format!(
            "the coordinates own the pixel axes in the order {} but the world axes in the \
             order {}; {SUPPORTED}, with the world axes in the order of the pixel axes",
            names(&order),
            names(&world_order)
        )));
    }
    Ok(order)
}

/// Coordinate `name`'s axis maps, `None` if the record has neither.
fn axis_maps(
    rec: &RecordValue,
    index: usize,
    name: &str,
    coordinate: &CoordinateModel,
) -> Result<Option<AxisMaps>, CoordinateError> {
    let pixel_field = format!("pixelmap{index}");
    let world_field = format!("worldmap{index}");
    match (rec.get(&pixel_field), rec.get(&world_field)) {
        (None, None) => return Ok(None),
        (Some(_), None) | (None, Some(_)) => {
            return Err(CoordinateError::InvalidRecord(format!(
                "{name} needs both {pixel_field} and {world_field}"
            )));
        }
        (Some(_), Some(_)) => {}
    }
    let pixel = get_required_vec_i32(rec, &pixel_field)?;
    let world = get_required_vec_i32(rec, &world_field)?;
    for (field, map, count) in [
        (&pixel_field, &pixel, coordinate.n_pixel_axes()),
        (&world_field, &world, coordinate.n_world_axes()),
    ] {
        if map.len() != count {
            return Err(CoordinateError::InvalidRecord(format!(
                "{field} names {} axes but {name} has {count}",
                map.len()
            )));
        }
    }
    if pixel.iter().chain(&world).any(|&axis| axis < 0) {
        return Err(CoordinateError::UnsupportedAxisLayout(format!(
            "{name} has removed axes (pixel axes {pixel:?}, world axes {world:?}; casacore \
             keeps a removed axis as -(axis + 1)); {SUPPORTED}, with none removed. In CASA, \
             make the image without dropping an axis of a coordinate it keeps (imsubimage \
             with dropdeg=False)"
        )));
    }
    Ok(Some(AxisMaps { pixel, world }))
}

/// The maps number the system's `kind` axes `0..n` once each.
fn check_numbering<'a>(
    maps: impl Iterator<Item = &'a [i32]> + Clone,
    kind: &str,
) -> Result<(), CoordinateError> {
    let mut axes = maps.clone().flatten().copied().collect::<Vec<_>>();
    axes.sort_unstable();
    if axes
        .iter()
        .zip(0..)
        .all(|(&axis, expected)| axis == expected)
    {
        return Ok(());
    }
    Err(CoordinateError::InvalidRecord(format!(
        "the {kind} maps {:?} do not number the system's {} {kind} axes from 0 once each",
        maps.collect::<Vec<_>>(),
        axes.len()
    )))
}

/// Coordinate `name`'s `kind` axes `map` are adjacent and in its own order.
fn check_run(
    name: &str,
    coordinate: &CoordinateModel,
    map: &[i32],
    kind: &str,
) -> Result<(), CoordinateError> {
    if map.windows(2).all(|pair| pair[1] == pair[0] + 1) {
        return Ok(());
    }
    let mut by_axis = map.iter().zip(coordinate.axis_names()).collect::<Vec<_>>();
    by_axis.sort_unstable();
    let adjacent = by_axis.windows(2).all(|pair| *pair[1].0 == *pair[0].0 + 1);
    let problem = if adjacent {
        let names = by_axis
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>()
            .join(", ");
        format!("which puts its axes in the order {names}")
    } else {
        "which are not adjacent".to_string()
    };
    Err(CoordinateError::UnsupportedAxisLayout(format!(
        "{name} owns {kind} axes {map:?}, {problem}; {SUPPORTED}. In CASA, transpose the \
         image (imtrans) to an order that keeps them so"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CoordinateSystem;
    use crate::coordinate::CoordinateType;
    use crate::direction::DirectionCoordinate;
    use crate::projection::{Projection, ProjectionType};
    use crate::spectral::SpectralCoordinate;
    use crate::stokes::{StokesCoordinate, StokesType};
    use casa_types::measures::direction::DirectionRef;
    use casa_types::measures::frequency::FrequencyRef;
    use casa_types::{ArrayValue, Value};

    /// casacore's `newcoordsys(direction, stokes I Q, spectral)`:
    /// `direction0`, `stokes1`, `spectral2` over consecutive axes.
    fn creation_order() -> CoordinateSystem {
        let mut cs = CoordinateSystem::new();
        cs.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [0.1, 0.2],
            [-1e-5, 1e-5],
            [3.0, 2.0],
        ));
        cs.add_coordinate(StokesCoordinate::new(vec![StokesType::I, StokesType::Q]));
        cs.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.415e9,
            1e6,
            0.0,
            1.420_405_752e9,
        ));
        cs
    }

    /// `creation_order()`'s record with each coordinate's world and pixel
    /// maps set to `maps`, as CASA writes them after a transpose.
    fn record_with_maps(maps: [&[i32]; 3]) -> RecordValue {
        let mut rec = creation_order().to_record();
        for (index, map) in maps.into_iter().enumerate() {
            for field in ["worldmap", "pixelmap"] {
                rec.upsert(
                    format!("{field}{index}"),
                    Value::Array(ArrayValue::from_i32_vec(map.to_vec())),
                );
            }
        }
        rec
    }

    fn types(cs: &CoordinateSystem) -> Vec<CoordinateType> {
        (0..cs.n_coordinates())
            .map(|index| cs.coordinate(index).coordinate_type())
            .collect()
    }

    fn refusal(rec: &RecordValue) -> String {
        match CoordinateSystem::from_record(rec) {
            Err(CoordinateError::UnsupportedAxisLayout(message)) => message,
            other => panic!("expected an unsupported axis layout, got {other:?}"),
        }
    }

    #[test]
    fn a_transposed_record_reads_with_its_coordinates_in_pixel_order() {
        // `cs.transpose([2, 0, 1, 3])`: Stokes, then the direction, then
        // the spectral axis.
        let rec = record_with_maps([&[1, 2], &[0], &[3]]);
        let cs = CoordinateSystem::from_record(&rec).unwrap();
        assert_eq!(
            types(&cs),
            [
                CoordinateType::Stokes,
                CoordinateType::Direction,
                CoordinateType::Spectral
            ]
        );
        assert_eq!(
            CoordinateSystem::record_coordinate_fields(&rec).unwrap(),
            ["stokes1", "direction0", "spectral2"]
        );
        // Pixel (Q, RA 3, Dec 2, channel 4) is world (Q, the reference
        // direction, 1.419 GHz), in the system's axis order.
        let world = cs.to_world(&[1.0, 3.0, 2.0, 4.0]).unwrap();
        assert_eq!(world[0], 2.0);
        assert!((world[1] - 0.1).abs() < 1e-12 && (world[2] - 0.2).abs() < 1e-12);
        assert!((world[3] - 1.419e9).abs() < 1e-3);
        assert!((cs.to_pixel(&world).unwrap()[3] - 4.0).abs() < 1e-9);

        // Written back, the coordinates are numbered in that order over
        // consecutive axes, and read the same.
        let again = cs.to_record();
        assert!(again.get("stokes0").is_some() && again.get("direction1").is_some());
        let cs_again = CoordinateSystem::from_record(&again).unwrap();
        assert_eq!(types(&cs_again), types(&cs));
    }

    #[test]
    fn consecutive_and_absent_maps_keep_the_coordinate_order() {
        let created = [
            CoordinateType::Direction,
            CoordinateType::Stokes,
            CoordinateType::Spectral,
        ];
        let rec = creation_order().to_record();
        assert_eq!(
            types(&CoordinateSystem::from_record(&rec).unwrap()),
            created
        );

        let mut bare = rec;
        for index in 0..3 {
            for field in ["worldmap", "pixelmap", "worldreplace", "pixelreplace"] {
                bare.remove(&format!("{field}{index}"));
            }
        }
        assert_eq!(
            types(&CoordinateSystem::from_record(&bare).unwrap()),
            created
        );
    }

    #[test]
    fn maps_read_from_python_as_int64_are_honoured() {
        let mut rec = record_with_maps([&[1, 2], &[3], &[0]]);
        for (index, map) in [vec![1i64, 2], vec![3], vec![0]].into_iter().enumerate() {
            for field in ["worldmap", "pixelmap"] {
                rec.upsert(
                    format!("{field}{index}"),
                    Value::Array(ArrayValue::Int64(
                        ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[map.len()]), map.clone())
                            .unwrap(),
                    )),
                );
            }
        }
        // `imtrans order='3012'`: the spectral axis first.
        assert_eq!(
            types(&CoordinateSystem::from_record(&rec).unwrap()),
            [
                CoordinateType::Spectral,
                CoordinateType::Direction,
                CoordinateType::Stokes
            ]
        );
    }

    #[test]
    fn axes_swapped_within_a_coordinate_are_refused() {
        // `imtrans order='1023'`.
        let message = refusal(&record_with_maps([&[1, 0], &[2], &[3]]));
        assert!(
            message.contains("direction0 owns pixel axes [1, 0]")
                && message.contains("Declination, Right Ascension")
                && message.contains("#1002"),
            "{message}"
        );
    }

    #[test]
    fn a_coordinate_interleaved_with_another_is_refused() {
        // `imtrans order='0213'`.
        let message = refusal(&record_with_maps([&[0, 2], &[1], &[3]]));
        assert!(
            message.contains("direction0 owns pixel axes [0, 2], which are not adjacent"),
            "{message}"
        );
    }

    #[test]
    fn removed_axes_are_refused() {
        // `imsubimage(dropdeg=True)` of an image with one RA pixel: casacore
        // keeps the removed RA axis as -1 with its replacement value.
        let mut rec = record_with_maps([&[-1, 0], &[1], &[2]]);
        rec.upsert(
            "pixelreplace0",
            Value::Array(ArrayValue::from_f64_vec(vec![0.0, 0.0])),
        );
        let message = refusal(&rec);
        assert!(
            message.contains("direction0 has removed axes") && message.contains("dropdeg"),
            "{message}"
        );
    }

    #[test]
    fn world_axes_ordered_unlike_pixel_axes_are_refused() {
        let mut rec = record_with_maps([&[1, 2], &[0], &[3]]);
        rec.upsert(
            "worldmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        );
        rec.upsert("worldmap1", Value::Array(ArrayValue::from_i32_vec(vec![2])));
        let message = refusal(&rec);
        assert!(
            message.contains("pixel axes in the order stokes1, direction0, spectral2")
                && message.contains("world axes in the order direction0, stokes1, spectral2"),
            "{message}"
        );
    }

    #[test]
    fn malformed_maps_are_invalid_records() {
        let invalid = |rec: &RecordValue| {
            matches!(
                CoordinateSystem::from_record(rec),
                Err(CoordinateError::InvalidRecord(_))
            )
        };
        // An axis owned twice, a map of the wrong length, and a coordinate
        // without maps among coordinates with them.
        assert!(invalid(&record_with_maps([&[0, 1], &[1], &[3]])));
        assert!(invalid(&record_with_maps([&[0], &[1], &[2]])));
        let mut rec = creation_order().to_record();
        rec.remove("worldmap1");
        rec.remove("pixelmap1");
        assert!(invalid(&rec));
        let mut rec = creation_order().to_record();
        rec.remove("worldmap2");
        assert!(invalid(&rec));
    }
}
