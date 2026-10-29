// SPDX-License-Identifier: LGPL-3.0-or-later
//! MSv4 `field_and_source_ephemeris` datasets: the fields of a visibility
//! dataset that follow an ephemeris, and that ephemeris, as XRADIO's
//! converter lays them out (`create_field_and_source_xds.py`,
//! `extract_ephemeris_info`).
//!
//! The body's geocentric samples are `SOURCE_DIRECTION`, `SOURCE_DISTANCE`
//! and `SOURCE_RADIAL_VELOCITY`, in SI units, on their own times
//! (`time_ephemeris`, what XRADIO's `convert_msv2_to_processing_set`
//! writes by default) or interpolated onto the visibility times (`time`).
//! Either way their times are the ephemeris table's `MJD` as Unix seconds
//! labelled UTC, which the boundary reads back as the table's `MJD`
//! ([`FieldEphemeris::times_mjd`]), so that a store converted from an MSv2
//! is evaluated as the MSv2 is.
//!
//! `FIELD_PHASE_CENTER_DIRECTION` is per visibility time: the field's
//! offset from the body (MSv2 `PHASE_DIR`) added to the body's direction,
//! interpolated linearly in angle onto the time, and wrapped as XRADIO
//! wraps angles ([`wrap_to_pi`]). It is a label, not the direction casacore
//! derives (which shifts the body's direction by the offset's true angle),
//! so the reader recovers the offset by subtracting the body's direction
//! again, to the store's rounding, and the writer writes the sum as XRADIO
//! does.

use std::f64::consts::{PI, TAU};
use std::sync::Arc;

use casa_observation::{Catalogue, FieldEphemeris, ObservationError};
use casa_types::measures::radial_velocity::RadialVelocityRef;
use zarrs::filesystem::FilesystemStore;

use super::metadata::{
    UNIX_EPOCH_MJD, direction_frame, has_node, invalid, numbers, open_array, require_layout,
    require_units, strings, text, time_reference,
};

/// The `type` of a field dataset whose fields follow an ephemeris.
pub(super) const EPHEMERIS_DATASET: &str = "field_and_source_ephemeris";

/// Seconds per day.
pub(super) const SECONDS_PER_DAY: f64 = 86_400.0;

/// How far two recovered offsets of one field may differ, radians. The
/// store holds the offset only within its sum with the body's direction,
/// an angle of up to 2π whose rounding is about 1e-15 rad.
pub(super) const OFFSET_RESOLUTION_RAD: f64 = 1.0e-12;

/// What a `field_and_source_ephemeris` dataset states.
pub(super) struct MovingFields {
    /// The dataset's fields, in the order of their first time.
    pub(super) fields: Vec<MovingField>,
    /// Each visibility time's field: an index into [`Self::fields`].
    pub(super) per_time: Vec<usize>,
    /// The ephemeris every field of the dataset follows (XRADIO writes one
    /// per dataset).
    pub(super) ephemeris: FieldEphemeris,
}

/// One field of a [`MovingFields`].
pub(super) struct MovingField {
    pub(super) name: String,
    /// The source as stored, XRADIO's placeholder included.
    pub(super) source_name: String,
    /// The offset from the body, (longitude, latitude) radians: the first
    /// of its times', which agree to [`OFFSET_RESOLUTION_RAD`].
    pub(super) offset_rad: [f64; 2],
}

/// Read the `field_and_source_ephemeris` dataset at `path`, whose
/// visibility dataset has `times_s` (Unix seconds, UTC) and the fields
/// `per_time` names at them.
///
/// # Errors
///
/// [`ObservationError::InvalidStore`] when a variable is missing or not of
/// the profile's type, dimensions, shape and units, the per-time field
/// names differ from the visibility dataset's, a field names two sources
/// or its offsets from the body differ, or the samples are not an
/// ephemeris ([`FieldEphemeris::new`]); [`ObservationError::Unsupported`]
/// for an ephemeris in another frame than ICRS or J2000, a phase centre in
/// another frame than its ephemeris, or one stated from another observer
/// than the Earth's centre.
pub(super) fn read(
    store: &Arc<FilesystemStore>,
    path: &str,
    times_s: &[f64],
    per_time: &[String],
) -> Result<MovingFields, ObservationError> {
    let time_ephemeris = format!("{path}/time_ephemeris");
    let (sample_times_s, axis) = if has_node(store, &time_ephemeris)? {
        let (times, attributes) = numbers::<f64>(store, &time_ephemeris)?;
        time_reference(&attributes, "time_ephemeris")?;
        (times, "time_ephemeris")
    } else {
        tracing::warn!(
            "{path}: the ephemeris is interpolated onto the visibility times \
             (ephemeris_interpolate=True): its table's own samples are not in the store, and the \
             body's direction is evaluated from these, which end at the last time"
        );
        (times_s.to_vec(), "time")
    };
    let samples = sample_times_s.len();
    let (directions, attributes) = series(
        store,
        path,
        "SOURCE_DIRECTION",
        &[axis, "sky_dir_label"],
        &[samples, 2],
        "rad",
    )?;
    let frame = direction_frame(&attributes, "SOURCE_DIRECTION")?;
    let (distances, _) = series(
        store,
        path,
        "SOURCE_DISTANCE",
        &[axis, "sky_dis_label"],
        &[samples, 1],
        "m",
    )?;
    let (velocities, _) = series(
        store,
        path,
        "SOURCE_RADIAL_VELOCITY",
        &[axis],
        &[samples],
        "m/s",
    )?;
    require_geocentric(store, path)?;
    let directions: Vec<[f64; 2]> = directions.as_chunks::<2>().0.to_vec();
    let (centres, attributes) = series(
        store,
        path,
        "FIELD_PHASE_CENTER_DIRECTION",
        &["time", "sky_dir_label"],
        &[times_s.len(), 2],
        "rad",
    )?;
    if direction_frame(&attributes, "FIELD_PHASE_CENTER_DIRECTION")? != frame {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 moving phase centre in another frame than its ephemeris",
        });
    }
    let names = strings(store, &format!("{path}/field_name"))?;
    let sources = strings(store, &format!("{path}/source_name"))?;
    if names != per_time || sources.len() != names.len() {
        return Err(invalid(format!(
            "{path}'s field_name and source_name are not the visibility dataset's fields at its \
             times"
        )));
    }
    let bodies = at_times(&sample_times_s, &directions, times_s)
        .ok_or_else(|| invalid(format!("{path}: a time outside SOURCE_DIRECTION's samples")))?;
    let (fields, per_time) = moving_fields(path, &names, &sources, &centres, &bodies)?;
    let name = fields
        .first()
        .map(|field| {
            if super::is_unknown_source(&field.source_name) {
                field.name.clone()
            } else {
                field.source_name.clone()
            }
        })
        .unwrap_or_default();
    if samples < 2 {
        return Err(ObservationError::Unsupported {
            what: "an MSv4 ephemeris of fewer than two samples (as XRADIO interpolates one onto \
                   a dataset of one time)",
        });
    }
    let ephemeris = FieldEphemeris::new(
        name,
        sample_times_s
            .iter()
            .map(|seconds| UNIX_EPOCH_MJD + seconds / SECONDS_PER_DAY)
            .collect(),
        directions,
        frame,
        distances,
        (velocities, RadialVelocityRef::GEO),
    )
    .map_err(|error| invalid(format!("{path}: {error}")))?;
    Ok(MovingFields {
        fields,
        per_time,
        ephemeris,
    })
}

/// Variable `name` of the dataset at `path`: `float64` of `dimensions` and
/// `shape`, in `units`, with its attributes.
fn series(
    store: &Arc<FilesystemStore>,
    path: &str,
    name: &str,
    dimensions: &[&str],
    shape: &[usize],
    units: &str,
) -> Result<(Vec<f64>, serde_json::Map<String, serde_json::Value>), ObservationError> {
    let variable = format!("{path}/{name}");
    let array = open_array(store, &variable)?;
    require_layout(&array, &variable, "float64", dimensions)?;
    let expected: Vec<u64> = shape.iter().map(|n| *n as u64).collect();
    if array.shape() != expected {
        return Err(invalid(format!(
            "{variable} has shape {:?}, not {expected:?}",
            array.shape()
        )));
    }
    require_units(array.attributes(), units, name)?;
    numbers::<f64>(store, &variable)
}

/// Refuse an ephemeris stated from anywhere but the Earth's centre: XRADIO
/// converts only geocentric tables (`obsloc` `GEOCENTRIC`), and records
/// that as `OBSERVER_POSITION`'s `coordinate_system`. (Its position is not
/// the geocentre: XRADIO places the table's `GeoLong`, `GeoLat` and
/// `GeoDist` on the WGS84 ellipsoid, the distance as a height.) The schema
/// makes `OBSERVER_POSITION` optional; a dataset without one is read as
/// geocentric, the only kind XRADIO writes.
fn require_geocentric(store: &Arc<FilesystemStore>, path: &str) -> Result<(), ObservationError> {
    let observer = format!("{path}/OBSERVER_POSITION");
    if !has_node(store, &observer)? {
        return Ok(());
    }
    let array = open_array(store, &observer)?;
    if text(array.attributes(), "coordinate_system", "OBSERVER_POSITION")? == "geocentric" {
        Ok(())
    } else {
        Err(ObservationError::Unsupported {
            what: "an MSv4 ephemeris stated from another observer than the Earth's centre",
        })
    }
}

/// The distinct fields of `names` (each time's field), with their sources
/// and offsets from `bodies` (the body's direction at each time), and each
/// time's field.
fn moving_fields(
    path: &str,
    names: &[String],
    sources: &[String],
    centres: &[f64],
    bodies: &[[f64; 2]],
) -> Result<(Vec<MovingField>, Vec<usize>), ObservationError> {
    let mut fields: Vec<MovingField> = Vec::new();
    let mut per_time = Vec::with_capacity(names.len());
    for (time, name) in names.iter().enumerate() {
        let offset_rad =
            std::array::from_fn(|axis| wrap_to_pi(centres[2 * time + axis] - bodies[time][axis]));
        let index = match fields.iter().position(|field| field.name == *name) {
            Some(index) => {
                let field = &fields[index];
                if field.source_name != sources[time] {
                    return Err(invalid(format!("{path}: field {name} names two sources")));
                }
                if !same_angles(offset_rad, field.offset_rad) {
                    return Err(invalid(format!(
                        "{path}: field {name}'s phase centre is not one offset from \
                         SOURCE_DIRECTION"
                    )));
                }
                index
            }
            None => {
                fields.push(MovingField {
                    name: name.clone(),
                    source_name: sources[time].clone(),
                    offset_rad,
                });
                fields.len() - 1
            }
        };
        per_time.push(index);
    }
    Ok((fields, per_time))
}

/// `values` (at `sample_times`) at each of `times`, interpolated linearly
/// (in angle, for directions) as XRADIO interpolates them (numpy's
/// `interp`), or `None` for a time outside the samples. Times are Unix
/// seconds; one within [`TIME_RESOLUTION_S`] of an end sample is at it, as
/// a sample at a visibility time comes back from its MJD.
pub(super) fn at_times<const N: usize>(
    sample_times: &[f64],
    values: &[[f64; N]],
    times: &[f64],
) -> Option<Vec<[f64; N]>> {
    let last = sample_times.len().checked_sub(1)?;
    let (first_sample, last_sample) = (sample_times[0], sample_times[last]);
    times
        .iter()
        .map(|time| {
            if *time < first_sample - TIME_RESOLUTION_S || *time > last_sample + TIME_RESOLUTION_S {
                return None;
            }
            let time = &time.clamp(first_sample, last_sample);
            let upper = sample_times.partition_point(|sample| sample <= time);
            if upper > last {
                return Some(values[last]);
            }
            let lower = upper - 1;
            let span = sample_times[upper] - sample_times[lower];
            Some(std::array::from_fn(|axis| {
                let slope = (values[upper][axis] - values[lower][axis]) / span;
                slope * (time - sample_times[lower]) + values[lower][axis]
            }))
        })
        .collect()
}

/// An angle wrapped to [-π, π) as XRADIO wraps angles
/// (`coord_math.wrap_to_pi`).
pub(super) fn wrap_to_pi(angle: f64) -> f64 {
    (angle + PI).rem_euclid(TAU) - PI
}

/// How far apart two statements of one time may be, seconds: 1 ms.
/// Converters state a table's `MJD` as Unix seconds by their own
/// arithmetic, which rounds differently, and a time read back as MJD days
/// moves by up to a microsecond.
pub(super) const TIME_RESOLUTION_S: f64 = 1.0e-3;

/// [`TIME_RESOLUTION_S`] in days: how far apart two datasets' samples of
/// one body may be stated and still be one sample.
const SAMPLE_TIME_RESOLUTION_DAYS: f64 = TIME_RESOLUTION_S / SECONDS_PER_DAY;

/// How far two datasets' values of one sample may differ, relative to
/// the larger: their converters' unit conversions round differently, by
/// about 1e-16.
const SAMPLE_RESOLUTION: f64 = 1.0e-12;

/// One sample: time, direction, distance and radial velocity.
type Sample = (f64, [f64; 2], f64, f64);

/// How far past a time its TDB can be, seconds: 69.184 s since 2017 (TT −
/// TAI 32.184 s, TAI − UTC 37 s), TDB − TT within 2 ms, and room for two
/// more leap seconds. Opening a store takes no leap-second table, so it
/// bounds the TDB rather than computing it.
const TDB_LEAD_S: f64 = 72.0;

/// Extend each ephemeris the catalogue's fields follow whose samples cover
/// the last time of a partition one of its fields is in but end less than
/// [`TDB_LEAD_S`] after it, by one sample on the samples' grid at or past
/// that ([`FieldEphemeris::extrapolated_to`]), with a warning. Imaging
/// reads a body's direction at each time's TDB, and XRADIO keeps only the
/// samples that bracket a dataset's UTC times; the owner chose extending
/// them over refusing those times (#995). `last_mjd` is each partition's
/// last time, MJD UTC. An ephemeris that does not cover a last time is
/// left as it is.
///
/// # Errors
///
/// An extension [`FieldEphemeris`] refuses.
pub(super) fn reach_last_tdb(
    catalogue: &mut Catalogue,
    last_mjd: &[Option<f64>],
) -> Result<(), ObservationError> {
    let mut needed: Vec<(Arc<FieldEphemeris>, f64)> = Vec::new();
    for (entry, last) in catalogue.partitions.iter().zip(last_mjd) {
        let Some(last) = *last else { continue };
        for field in &entry.fields {
            let Some(ephemeris) = &catalogue.fields[field.0 as usize].ephemeris else {
                continue;
            };
            match needed
                .iter_mut()
                .find(|(known, _)| Arc::ptr_eq(known, ephemeris))
            {
                Some((_, end)) => *end = end.max(last),
                None => needed.push((Arc::clone(ephemeris), last)),
            }
        }
    }
    for (ephemeris, last) in needed {
        let times = ephemeris.times_mjd();
        let (before, end) = (times[times.len() - 2], times[times.len() - 1]);
        let tdb = last + TDB_LEAD_S / SECONDS_PER_DAY;
        if end >= tdb || end < last - SAMPLE_TIME_RESOLUTION_DAYS {
            continue;
        }
        let step = end - before;
        let at = end + ((tdb - end) / step).ceil() * step;
        let extended = Arc::new(ephemeris.extrapolated_to(at).map_err(|error| {
            invalid(format!(
                "extending the ephemeris of {}: {error}",
                ephemeris.name()
            ))
        })?);
        tracing::warn!(
            "the ephemeris of {}: its samples end {:.1} s after the last time, short of its TDB; \
             one sample is extrapolated, {:.1} s after it, along the line through the last two",
            ephemeris.name(),
            (end - last) * SECONDS_PER_DAY,
            (at - last) * SECONDS_PER_DAY
        );
        for field in &mut catalogue.fields {
            if field
                .ephemeris
                .as_ref()
                .is_some_and(|known| Arc::ptr_eq(known, &ephemeris))
            {
                field.ephemeris = Some(Arc::clone(&extended));
            }
        }
    }
    Ok(())
}

/// The ephemeris of `known` and `other` together, when they are one
/// body's samples: the same frames, and the same values (to
/// [`SAMPLE_RESOLUTION`]) at every time both hold (to
/// [`SAMPLE_TIME_RESOLUTION_DAYS`]), where `known`'s are kept. Two datasets
/// of one field each hold the samples their own times need; `None` when
/// they disagree.
pub(super) fn union(
    known: &Arc<FieldEphemeris>,
    other: &FieldEphemeris,
) -> Option<Arc<FieldEphemeris>> {
    if **known == *other {
        return Some(Arc::clone(known));
    }
    if known.frame() != other.frame() || known.velocity_frame() != other.velocity_frame() {
        return None;
    }
    let samples = |ephemeris: &FieldEphemeris| -> Vec<Sample> {
        (0..ephemeris.times_mjd().len())
            .map(|index| {
                (
                    ephemeris.times_mjd()[index],
                    ephemeris.directions_rad()[index],
                    ephemeris.distances_m()[index],
                    ephemeris.radial_velocities_m_s()[index],
                )
            })
            .collect()
    };
    let (left, right) = (samples(known), samples(other));
    let (mut a, mut b) = (0, 0);
    let mut merged: Vec<Sample> = Vec::with_capacity(left.len() + right.len());
    while a < left.len() || b < right.len() {
        let next = match (left.get(a), right.get(b)) {
            (Some(mine), Some(theirs))
                if (mine.0 - theirs.0).abs() <= SAMPLE_TIME_RESOLUTION_DAYS =>
            {
                if !same_sample(mine, theirs) {
                    return None;
                }
                a += 1;
                b += 1;
                *mine
            }
            (Some(mine), Some(theirs)) if mine.0 < theirs.0 => {
                a += 1;
                *mine
            }
            (Some(mine), None) => {
                a += 1;
                *mine
            }
            (_, Some(theirs)) => {
                b += 1;
                *theirs
            }
            (None, None) => unreachable!("the loop ends when both are exhausted"),
        };
        merged.push(next);
    }
    FieldEphemeris::new(
        known.name(),
        merged.iter().map(|sample| sample.0).collect(),
        merged.iter().map(|sample| sample.1).collect(),
        known.frame(),
        merged.iter().map(|sample| sample.2).collect(),
        (
            merged.iter().map(|sample| sample.3).collect(),
            known.velocity_frame(),
        ),
    )
    .ok()
    .map(Arc::new)
}

/// Whether two samples at one time hold one body's values
/// ([`SAMPLE_RESOLUTION`]; angles to [`OFFSET_RESOLUTION_RAD`]).
fn same_sample(a: &Sample, b: &Sample) -> bool {
    let close = |x: f64, y: f64| (x - y).abs() <= SAMPLE_RESOLUTION * x.abs().max(y.abs());
    same_angles(a.1, b.1) && close(a.2, b.2) && (close(a.3, b.3) || (a.3 - b.3).abs() <= 1.0e-9)
}

/// Whether the angles `a` and `b` (longitude, latitude) are one, to the
/// store's resolution ([`OFFSET_RESOLUTION_RAD`]), whatever turn each is
/// stated in: a recovered offset is wrapped to [-π, π), and
/// [`MDirection::as_angles`](casa_types::measures::direction::MDirection::as_angles)
/// states a longitude in [0, 2π).
pub(super) fn same_angles(a: [f64; 2], b: [f64; 2]) -> bool {
    (0..2).all(|axis| wrap_to_pi(a[axis] - b[axis]).abs() <= OFFSET_RESOLUTION_RAD)
}

#[cfg(test)]
mod tests {
    use casa_types::measures::direction::DirectionRef;

    use super::*;

    #[test]
    fn angles_wrap_as_xradio_wraps_them() {
        assert_eq!(wrap_to_pi(0.25), 0.25);
        assert!((wrap_to_pi(1.5 * PI) + 0.5 * PI).abs() < 1e-15);
        assert_eq!(wrap_to_pi(-PI), -PI);
        assert!((wrap_to_pi(PI) + PI).abs() < 1e-15);
    }

    #[test]
    fn angles_are_one_whatever_turn_they_are_stated_in() {
        let offset = [-1.0e-5, 2.0e-6];
        assert!(same_angles(offset, [TAU - 1.0e-5, 2.0e-6]));
        assert!(same_angles([-1.0e-17, 0.0], [TAU - 1.0e-17, 0.0]));
        assert!(same_angles([PI - 1.0e-13, 0.0], [-PI + 1.0e-13, 0.0]));
        assert!(!same_angles(offset, [1.0e-5, 2.0e-6]));
    }

    #[test]
    fn directions_interpolate_linearly_in_angle_and_not_beyond_the_samples() {
        let times = [0.0, 10.0, 20.0];
        let directions = [[1.0, 0.5], [2.0, 0.25], [4.0, 0.0]];
        let at = at_times(&times, &directions, &[0.0, 5.0, 10.0, 15.0, 20.0]).expect("inside");
        assert_eq!(
            at,
            vec![
                [1.0, 0.5],
                [1.5, 0.375],
                [2.0, 0.25],
                [3.0, 0.125],
                [4.0, 0.0]
            ]
        );
        assert!(at_times(&times, &directions, &[20.5]).is_none());
        assert!(at_times(&times, &directions, &[-0.5]).is_none());
        // A time a rounding outside an end sample is at it.
        let ends = at_times(&times, &directions, &[-1.0e-7, 20.0 + 1.0e-7]).expect("at the ends");
        assert_eq!(ends, [directions[0], directions[directions.len() - 1]]);
    }

    fn ephemeris(times: &[f64]) -> FieldEphemeris {
        FieldEphemeris::new(
            "body",
            times.to_vec(),
            times.iter().map(|time| [time * 1e-3, 0.1]).collect(),
            DirectionRef::ICRS,
            times.iter().map(|time| 1e8 + time).collect(),
            (times.to_vec(), RadialVelocityRef::GEO),
        )
        .expect("an ephemeris")
    }

    #[test]
    fn two_datasets_samples_of_one_body_unite_and_conflicting_ones_do_not() {
        let known = Arc::new(ephemeris(&[1.0, 2.0, 3.0]));
        assert!(Arc::ptr_eq(
            &union(&known, &ephemeris(&[1.0, 2.0, 3.0])).expect("equal"),
            &known
        ));
        let united = union(&known, &ephemeris(&[3.0, 4.0, 6.0])).expect("one body");
        assert_eq!(*united, ephemeris(&[1.0, 2.0, 3.0, 4.0, 6.0]));
        // Another converter's statement of a sample's time, 0.1 ms off, is
        // the same sample.
        let restated = ephemeris(&[3.0, 4.0]);
        let shifted = FieldEphemeris::new(
            "body",
            vec![3.0 + 1.0e-4 / SECONDS_PER_DAY, 4.0],
            restated.directions_rad().to_vec(),
            DirectionRef::ICRS,
            restated.distances_m().to_vec(),
            (
                restated.radial_velocities_m_s().to_vec(),
                RadialVelocityRef::GEO,
            ),
        )
        .expect("an ephemeris");
        let united = union(&known, &shifted).expect("one body");
        assert_eq!(united.times_mjd(), [1.0, 2.0, 3.0, 4.0]);
        let mut moved = ephemeris(&[2.0, 5.0]);
        moved = FieldEphemeris::new(
            "body",
            moved.times_mjd().to_vec(),
            vec![[2.0e-3, 0.2], [5.0e-3, 0.1]],
            DirectionRef::ICRS,
            moved.distances_m().to_vec(),
            (
                moved.radial_velocities_m_s().to_vec(),
                RadialVelocityRef::GEO,
            ),
        )
        .expect("an ephemeris");
        assert!(union(&known, &moved).is_none());
    }
}
