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
//! the coordinate's own order or not adjacent, or where the world axes are
//! ordered unlike the pixel axes.
//!
//! A removed axis is dropped from a linear coordinate whose other axes do
//! not depend on it, which leaves the system's conversions as casacore's.
//! `impv` writes its position-velocity images so: a two-axis linear
//! coordinate with the second axis removed. A direction slice retains its
//! projection and uses the record's replacement values for the removed axis.

use casa_core::{RecordValue, Value};

use super::{
    CoordinateSystem, RecordCoordinate, coordinate_record_entries, parse_coordinate_record,
};

use crate::coordinate::CoordinateModel;
use crate::error::CoordinateError;
use crate::obs_info::ObsInfo;
use crate::record_utils::{get_required_vec_f64, get_required_vec_i32};

impl CoordinateSystem {
    /// Restore a coordinate record made by a live image operation, retaining
    /// sampled axes' live references. A filesystem reopen uses `from_record`.
    ///
    /// # Errors
    /// The record or one of its live sampled references is invalid.
    pub fn from_live_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        match Self::restore_record(rec, true) {
            Err(CoordinateError::UnsupportedAxisLayout(_)) => {
                Self::restore_mapped_record(rec, true)
            }
            result => result,
        }
    }

    pub(super) fn restore_record(rec: &RecordValue, live: bool) -> Result<Self, CoordinateError> {
        let obs_info = match rec.get("obsinfo") {
            Some(Value::Record(obs_rec)) => ObsInfo::from_record(obs_rec)?,
            _ => ObsInfo::from_record(rec)?,
        };
        let mut coordinates = Vec::new();
        for (placement, mut coordinate) in read_coordinates(rec)? {
            if live {
                coordinate = live_coordinate(rec, &placement.field, coordinate)?;
            }
            coordinates.push(coordinate);
        }
        Ok(Self {
            coordinates,
            obs_info,
            maps: None,
        })
    }
}

pub(super) fn live_coordinate(
    rec: &RecordValue,
    field: &str,
    coordinate: CoordinateModel,
) -> Result<CoordinateModel, CoordinateError> {
    let Some(Value::Record(stored)) = rec.get(field) else {
        return Ok(coordinate);
    };
    Ok(match coordinate {
        CoordinateModel::Spectral(spectral) if !spectral.has_linear_mapping() => {
            spectral.with_live_reference(stored)?.into()
        }
        CoordinateModel::Tabular(table) => table.with_live_reference(stored)?.into(),
        other => other,
    })
}

/// `rec`'s coordinates with where they come from, in the order of their
/// axes.
pub(super) fn read_coordinates(
    rec: &RecordValue,
) -> Result<Vec<(RecordCoordinate, CoordinateModel)>, CoordinateError> {
    let mut parsed = read_models(rec)?;
    let placements = coordinate_order(rec, &mut parsed)?;
    let mut parsed = parsed.into_iter().map(Some).collect::<Vec<_>>();
    Ok(placements
        .into_iter()
        .map(|placement| {
            let (_, field, coordinate) = parsed[placement.index]
                .take()
                .expect("an order names each coordinate once");
            let placed = RecordCoordinate {
                field,
                pixel_axes: placement.pixel_axes,
                world_axes: placement.world_axes,
            };
            (placed, coordinate)
        })
        .collect())
}

pub(super) fn read_models(
    rec: &RecordValue,
) -> Result<Vec<(usize, String, CoordinateModel)>, CoordinateError> {
    let mut parsed = Vec::new();
    for (index, key) in coordinate_record_entries(rec) {
        let Some(Value::Record(coord_rec)) = rec.get(&key) else {
            return Err(CoordinateError::InvalidRecord(format!(
                "coordinate field '{key}' is not a record"
            )));
        };
        let coordinate = parse_coordinate_record(&key, coord_rec)?;
        parsed.push((index, key, coordinate));
    }
    Ok(parsed)
}

/// What casa-rs reads, and the issue for the rest, for every refusal.
const SUPPORTED: &str = "casa-rs reads a coordinate system only when each coordinate's axes are \
     adjacent and in the coordinate's own order (#1002)";

/// One coordinate's `pixelmap<i>` and `worldmap<i>` over the axes it keeps,
/// and both maps as the record has them.
struct AxisMaps {
    pixel: Vec<i32>,
    world: Vec<i32>,
    record_pixel: Vec<i32>,
    record_world: Vec<i32>,
}

/// Where one of a record's coordinates goes in the system.
pub(super) struct Placement {
    /// Its position in the record's coordinates.
    pub(super) index: usize,
    /// The system pixel axis of each of the record coordinate's own axes,
    /// `None` for a removed one.
    pub(super) pixel_axes: Vec<Option<usize>>,
    /// The system world axis of each of the record coordinate's own axes.
    pub(super) world_axes: Vec<Option<usize>>,
}

/// The order of `coordinates`, read from `rec` as `(record index, field
/// name, coordinate)` in record index order, in which they own consecutive
/// pixel and world axes. A linear coordinate with removed axes is replaced
/// by the coordinate over its other axes.
///
/// A record without axis maps keeps its coordinate order. casa-rs's own
/// records always have them, and so do casacore's.
pub(super) fn coordinate_order(
    rec: &RecordValue,
    coordinates: &mut [(usize, String, CoordinateModel)],
) -> Result<Vec<Placement>, CoordinateError> {
    let maps = coordinates
        .iter_mut()
        .map(|(index, name, coordinate)| axis_maps(rec, *index, name, coordinate))
        .collect::<Result<Vec<_>, _>>()?;
    if maps.iter().all(Option::is_none) {
        let (mut next_pixel, mut next_world) = (0, 0);
        return Ok(coordinates
            .iter()
            .enumerate()
            .map(|(index, (_, _, coordinate))| {
                let pixel_end = next_pixel + coordinate.n_pixel_axes();
                let world_end = next_world + coordinate.n_world_axes();
                let placement = Placement {
                    index,
                    pixel_axes: (next_pixel..pixel_end).map(Some).collect(),
                    world_axes: (next_world..world_end).map(Some).collect(),
                };
                next_pixel = pixel_end;
                next_world = world_end;
                placement
            })
            .collect());
    }
    let maps = maps
        .into_iter()
        .zip(coordinates.iter())
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
    for (map, (_, name, coordinate)) in maps.iter().zip(coordinates.iter()) {
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
    Ok(order
        .into_iter()
        .map(|index| Placement {
            index,
            pixel_axes: maps[index]
                .record_pixel
                .iter()
                .map(|&axis| usize::try_from(axis).ok())
                .collect(),
            world_axes: maps[index]
                .record_world
                .iter()
                .map(|&axis| usize::try_from(axis).ok())
                .collect(),
        })
        .collect())
}

/// Coordinate `name`'s axis maps, `None` if the record has neither, with a
/// linear coordinate's removed axes dropped from it and from its maps.
fn axis_maps(
    rec: &RecordValue,
    index: usize,
    name: &str,
    coordinate: &mut CoordinateModel,
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
    if pixel.iter().chain(&world).all(|&axis| axis >= 0) {
        return Ok(Some(AxisMaps {
            record_pixel: pixel.clone(),
            record_world: world.clone(),
            pixel,
            world,
        }));
    }
    let kept = (0..pixel.len())
        .filter(|&j| pixel[j] >= 0)
        .collect::<Vec<_>>();
    let removed_alike =
        pixel.len() == world.len() && pixel.iter().zip(&world).all(|(&p, &w)| (p < 0) == (w < 0));
    let supported = match coordinate {
        CoordinateModel::Linear(linear) if removed_alike => {
            if let Some(reduced) = linear.kept_axes(&kept) {
                *linear = reduced;
                true
            } else {
                false
            }
        }
        CoordinateModel::Direction(direction) if removed_alike && kept.len() == 1 => {
            let removed = 1 - kept[0];
            let replacement = |kind| {
                let field = format!("{kind}replace{index}");
                let values = get_required_vec_f64(rec, &field)?;
                values
                    .get(removed)
                    .copied()
                    .filter(|value| value.is_finite())
                    .ok_or_else(|| {
                        CoordinateError::InvalidRecord(format!(
                            "{field} has no finite replacement for axis {removed}"
                        ))
                    })
            };
            direction.remove_record_axis(removed, replacement("pixel")?, replacement("world")?)?;
            true
        }
        _ => false,
    };
    if !supported {
        return Err(CoordinateError::UnsupportedAxisLayout(format!(
            "{name} has removed axes (pixel axes {pixel:?}, world axes {world:?}; casacore \
             keeps a removed axis as -(axis + 1)); {SUPPORTED}, and drops a removed axis \
             from a direction slice or a linear coordinate whose other axes do not depend on it. In CASA, \
             make the image again from the one it was cut from without dropping an axis of a \
             coordinate it keeps (imsubimage with dropdeg=False)"
        )));
    }
    let keep = |map: &[i32]| kept.iter().map(|&j| map[j]).collect();
    Ok(Some(AxisMaps {
        pixel: keep(&pixel),
        world: keep(&world),
        record_pixel: pixel,
        record_world: world,
    }))
}

/// The maps number the system's `kind` axes `0..n` once each.
pub(super) fn check_numbering<'a>(
    maps: impl Iterator<Item = &'a [i32]> + Clone,
    kind: &str,
) -> Result<(), CoordinateError> {
    let mut axes = maps
        .clone()
        .flatten()
        .copied()
        .filter(|&axis| axis >= 0)
        .collect::<Vec<_>>();
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
    use crate::linear::LinearCoordinate;
    use crate::projection::{Projection, ProjectionType};
    use crate::spectral::SpectralCoordinate;
    use crate::stokes::{StokesCoordinate, StokesType};
    use casa_core::measures::direction::DirectionRef;
    use casa_core::measures::frequency::FrequencyRef;
    use casa_core::{ArrayValue, Value};

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

    /// `record_coordinates(rec)` are `expected`, as (field, pixel axes).
    fn assert_placed<const N: usize>(rec: &RecordValue, expected: [(&str, Vec<Option<usize>>); N]) {
        let placed = CoordinateSystem::record_coordinates(rec)
            .unwrap()
            .into_iter()
            .map(|placed| (placed.field, placed.pixel_axes))
            .collect::<Vec<_>>();
        let expected = expected
            .into_iter()
            .map(|(field, axes)| (field.to_string(), axes))
            .collect::<Vec<_>>();
        assert_eq!(placed, expected);
    }

    fn compact_layout_refusal(rec: &RecordValue) -> String {
        CoordinateSystem::from_record(rec).expect("standard restore uses the signed maps");
        match CoordinateSystem::restore_record(rec, false) {
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
        assert_placed(
            &rec,
            [
                ("stokes1", vec![Some(0)]),
                ("direction0", vec![Some(1), Some(2)]),
                ("spectral2", vec![Some(3)]),
            ],
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
    fn axes_swapped_within_a_coordinate_requires_signed_maps() {
        // `imtrans order='1023'`.
        let message = compact_layout_refusal(&record_with_maps([&[1, 0], &[2], &[3]]));
        assert!(
            message.contains("direction0 owns pixel axes [1, 0]")
                && message.contains("Declination, Right Ascension")
                && message.contains("#1002"),
            "{message}"
        );
    }

    #[test]
    fn a_coordinate_interleaved_with_another_requires_signed_maps() {
        // `imtrans order='0213'`.
        let message = compact_layout_refusal(&record_with_maps([&[0, 2], &[1], &[3]]));
        assert!(
            message.contains("direction0 owns pixel axes [0, 2], which are not adjacent"),
            "{message}"
        );
    }

    #[test]
    fn a_direction_slice_round_trips_its_replacement_values() {
        // `imsubimage(dropdeg=True)` of an image with one RA pixel: casacore
        // keeps the removed RA axis as -1 with its replacement value.
        let mut rec = record_with_maps([&[-1, 0], &[1], &[2]]);
        rec.upsert(
            "pixelreplace0",
            Value::Array(ArrayValue::from_f64_vec(vec![13.0, 0.0])),
        );
        let system = CoordinateSystem::from_record(&rec).unwrap();
        let direction = system.coordinate(0);
        assert_eq!(direction.n_pixel_axes(), 1);
        assert_eq!(direction.axis_names(), ["Declination"]);
        let world = system.to_world(&[3.0, 0.0, 0.0]).unwrap();
        let restored = CoordinateSystem::from_record(&system.to_record()).unwrap();
        assert_eq!(restored.to_world(&[3.0, 0.0, 0.0]).unwrap(), world);
        assert_eq!(
            restored.to_pixel(&world).unwrap(),
            system.to_pixel(&world).unwrap()
        );
    }

    /// impv's position-velocity system: a two-axis linear offset coordinate
    /// with its second axis removed (pixel and world maps `[0, -2]`), then
    /// Stokes I and the spectral axis.
    fn position_velocity_record(pc: ndarray::Array2<f64>) -> RecordValue {
        let mut cs = CoordinateSystem::new();
        cs.add_coordinate(
            LinearCoordinate::new(
                2,
                vec!["Offset".into(), "Offset".into()],
                vec!["arcsec".into(), "arcsec".into()],
            )
            .with_increment(vec![60.0, 60.0])
            .with_reference_pixel(vec![14.0, 14.0])
            .with_pc_matrix(pc),
        );
        cs.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        cs.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1.415e9,
            1e3,
            4.0,
            1.420_405_752e9,
        ));
        let mut rec = cs.to_record();
        for (index, map) in [[0, -2].as_slice(), &[1], &[2]].into_iter().enumerate() {
            for field in ["worldmap", "pixelmap"] {
                rec.upsert(
                    format!("{field}{index}"),
                    Value::Array(ArrayValue::from_i32_vec(map.to_vec())),
                );
            }
        }
        rec.upsert(
            "pixelreplace0",
            Value::Array(ArrayValue::from_f64_vec(vec![0.0, 14.016_666_62])),
        );
        rec
    }

    #[test]
    fn a_linear_coordinates_independent_removed_axis_is_dropped() {
        let cs = CoordinateSystem::from_record(&position_velocity_record(ndarray::Array2::eye(2)))
            .unwrap();
        assert_eq!(
            types(&cs),
            [
                CoordinateType::Linear,
                CoordinateType::Stokes,
                CoordinateType::Spectral
            ]
        );
        assert_eq!(cs.n_pixel_axes(), 3);
        assert_eq!(cs.coordinate(0).axis_names(), ["Offset"]);
        assert_eq!(cs.coordinate(0).axis_units(), ["arcsec"]);
        // CASA's `cs.toworld([3, 0, 2])` for impv's image: 11 offset pixels
        // before the reference, Stokes I, two channels below it.
        let world = cs.to_world(&[3.0, 0.0, 2.0]).unwrap();
        assert_eq!(world[..2], [-660.0, 1.0]);
        assert!((world[2] - 1.414_998e9).abs() < 1e-3);
        // The record's linear coordinate keeps its removed axis.
        assert_placed(
            &position_velocity_record(ndarray::Array2::eye(2)),
            [
                ("linear0", vec![Some(0), None]),
                ("stokes1", vec![Some(1)]),
                ("spectral2", vec![Some(2)]),
            ],
        );
    }

    #[test]
    fn a_record_without_maps_places_its_coordinates_over_consecutive_axes() {
        let mut rec = creation_order().to_record();
        for index in 0..3 {
            rec.remove(&format!("worldmap{index}"));
            rec.remove(&format!("pixelmap{index}"));
        }
        assert_placed(
            &rec,
            [
                ("direction0", vec![Some(0), Some(1)]),
                ("stokes1", vec![Some(2)]),
                ("spectral2", vec![Some(3)]),
            ],
        );
    }

    #[test]
    fn a_removed_axis_the_others_depend_on_requires_signed_maps() {
        let coupled = ndarray::array![[1.0, 0.5], [0.0, 1.0]];
        let message = compact_layout_refusal(&position_velocity_record(coupled));
        assert!(
            message.contains("linear0 has removed axes")
                && message.contains("whose other axes do not depend on it"),
            "{message}"
        );
    }

    #[test]
    fn world_axes_ordered_unlike_pixel_axes_requires_signed_maps() {
        let mut rec = record_with_maps([&[1, 2], &[0], &[3]]);
        rec.upsert(
            "worldmap0",
            Value::Array(ArrayValue::from_i32_vec(vec![0, 1])),
        );
        rec.upsert("worldmap1", Value::Array(ArrayValue::from_i32_vec(vec![2])));
        let message = compact_layout_refusal(&rec);
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
