// SPDX-License-Identifier: LGPL-3.0-or-later
//! A dataset's field dataset (`field_and_source_<group>_xds`): its fields'
//! fixed phase centres (`field_and_source`), or, when they follow an
//! ephemeris, the ephemeris and their phase centre at each time
//! (`field_and_source_ephemeris`, [`super::super::ephemeris`]), as XRADIO's
//! converter writes them.

use std::ops::Range;
use std::sync::Arc;

use casa_observation::{
    Catalogue, FieldEntry, FieldEphemeris, FieldId, ObservationError, TimeBaselineGrid,
};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::radial_velocity::RadialVelocityRef;
use serde_json::{Value, json};
use zarrs::array::ArraySubset;
use zarrs::filesystem::FilesystemStore;

use super::super::ephemeris::{
    EPHEMERIS_DATASET, SECONDS_PER_DAY, TIME_RESOLUTION_S, at_times, wrap_to_pi,
};
use super::super::metadata::{UNIX_EPOCH_MJD, direction_name, storage, time_attributes};
use super::{ArraySpec, Msv4Codec, codecs, coordinate, create_array, create_group, strings};

/// How far past a dataset's last time its ephemeris samples reach,
/// seconds. Imaging evaluates a moving field's direction at a time's TDB,
/// which is 69.2 s after its UTC since 2017 (TT − TAI is 32.184 s, TAI −
/// UTC 37 s) and has never been 70 s; XRADIO keeps the samples either side
/// of the UTC times only.
const EVALUATION_REACH_S: f64 = 120.0;

/// Write the field dataset at `path` of the fields `grid`'s times name, in
/// `catalogue`, for data whose UVW is in `frame`. `times_s` are the grid's
/// times in Unix seconds.
///
/// # Errors
///
/// As [`check_fields`]; any error writing the store.
pub(super) fn write_fields(
    store: &Arc<FilesystemStore>,
    path: &str,
    catalogue: &Catalogue,
    grid: &TimeBaselineGrid,
    times_s: &[f64],
    frame: DirectionRef,
) -> Result<(), ObservationError> {
    match plan(catalogue, grid, times_s, frame)? {
        Plan::Fixed(fields) => fixed_fields(store, path, &fields),
        Plan::Moving(ephemeris) => {
            let per_time: Vec<&FieldEntry> = grid
                .field_per_time
                .iter()
                .map(|field| &catalogue.fields[field.0 as usize])
                .collect();
            moving_fields(store, path, ephemeris, &per_time, times_s)
        }
    }
}

/// Refuse what [`write_fields`] cannot write of the fields `grid`'s times
/// name (at `times_s`, Unix seconds) for data whose UVW is in `frame`, so
/// that a conversion refuses it before anything is written.
///
/// # Errors
///
/// [`ObservationError::Unsupported`] for a phase centre in another frame
/// than `frame`, fields of which only some follow an ephemeris or that
/// follow different ones (XRADIO's dataset holds one), or an ephemeris
/// whose radial velocities are not geocentric;
/// [`ObservationError::InvalidRequest`] for an ephemeris that does not
/// cover the times.
pub(in crate::observation_io) fn check_fields(
    catalogue: &Catalogue,
    grid: &TimeBaselineGrid,
    times_s: &[f64],
    frame: DirectionRef,
) -> Result<(), ObservationError> {
    plan(catalogue, grid, times_s, frame).map(|_| ())
}

/// What a dataset's field dataset holds.
enum Plan<'a> {
    /// The fields' fixed phase centres, in catalogue order.
    Fixed(Vec<&'a FieldEntry>),
    /// The ephemeris every field follows.
    Moving(&'a FieldEphemeris),
}

/// [`check_fields`], and what the field dataset then holds.
fn plan<'a>(
    catalogue: &'a Catalogue,
    grid: &TimeBaselineGrid,
    times_s: &[f64],
    frame: DirectionRef,
) -> Result<Plan<'a>, ObservationError> {
    // The fields the grid uses, in catalogue order.
    let mut used: Vec<FieldId> = grid.field_per_time.clone();
    used.sort();
    used.dedup();
    let fields: Vec<&FieldEntry> = used
        .iter()
        .map(|field| &catalogue.fields[field.0 as usize])
        .collect();
    if fields
        .iter()
        .any(|field| field.phase_centre.refer() != frame)
    {
        return Err(ObservationError::Unsupported {
            what: "writing phase centres in another frame than the data group's UVW",
        });
    }
    let followed = fields.first().and_then(|field| field.ephemeris.as_deref());
    if fields
        .iter()
        .any(|field| field.ephemeris.as_deref() != followed)
    {
        return Err(ObservationError::Unsupported {
            what: "writing an MSv4 dataset whose fields follow different ephemerides, or only \
                   some one (a field_and_source_ephemeris dataset holds one)",
        });
    }
    let Some(ephemeris) = followed else {
        return Ok(Plan::Fixed(fields));
    };
    if ephemeris.velocity_frame() != RadialVelocityRef::GEO {
        return Err(ObservationError::Unsupported {
            what: "writing an MSv4 ephemeris whose radial velocities are not geocentric",
        });
    }
    if bodies(ephemeris, times_s).is_none() {
        return Err(ObservationError::InvalidRequest {
            reason: format!(
                "the ephemeris of {} does not cover the dataset's times, at each of which MSv4 \
                 states the phase centre",
                ephemeris.name()
            ),
        });
    }
    Ok(Plan::Moving(ephemeris))
}

/// The samples of an ephemeris a dataset keeps ([`kept`]), and the body's
/// direction and distance at each of the dataset's times, interpolated as
/// XRADIO interpolates them.
struct Bodies {
    kept: Range<usize>,
    sample_times: Vec<f64>,
    directions: Vec<[f64; 2]>,
    distances: Vec<[f64; 1]>,
}

/// [`Bodies`] of `ephemeris` at `times_s` (Unix seconds), or `None` when
/// its samples do not cover the times.
fn bodies(ephemeris: &FieldEphemeris, times_s: &[f64]) -> Option<Bodies> {
    let sample_times = unix_seconds(ephemeris);
    let kept = kept(&sample_times, times_s);
    let sample_times = sample_times[kept.clone()].to_vec();
    let distances: Vec<[f64; 1]> = ephemeris.distances_m()[kept.clone()]
        .iter()
        .map(|distance| [*distance])
        .collect();
    Some(Bodies {
        directions: at_times(
            &sample_times,
            &ephemeris.directions_rad()[kept.clone()],
            times_s,
        )?,
        distances: at_times(&sample_times, &distances, times_s)?,
        kept,
        sample_times,
    })
}

/// `fields`' phase centres, one per field (`field_and_source`).
fn fixed_fields(
    store: &Arc<FilesystemStore>,
    path: &str,
    fields: &[&FieldEntry],
) -> Result<(), ObservationError> {
    create_group(store, path, json!({"type": "field_and_source"}))?;
    let count = fields.len() as u64;
    names(store, path, fields, "field_name")?;
    let centres: Vec<f64> = fields
        .iter()
        .flat_map(|field| {
            let (longitude, latitude) = field.phase_centre.as_angles();
            [longitude, latitude]
        })
        .collect();
    let frame = fields
        .first()
        .map_or(DirectionRef::J2000, |field| field.phase_centre.refer());
    variable(
        store,
        &format!("{path}/FIELD_PHASE_CENTER_DIRECTION"),
        &[count, 2],
        &["field_name", "sky_dir_label"],
        &centres,
        json!({"type": "sky_coord", "units": "rad", "frame": direction_name(frame)?, "coordinates": "source_name"}),
    )
}

/// `ephemeris`, which the field of each time (`per_time`, at `times_s`)
/// follows (`field_and_source_ephemeris`): the samples on `time_ephemeris`
/// from the last before the first time to the first at or after
/// [`EVALUATION_REACH_S`] past the last ([`kept`]), and each time's phase
/// centre as XRADIO states it, the field's offset added to the body's
/// direction interpolated linearly in angle, wrapped.
fn moving_fields(
    store: &Arc<FilesystemStore>,
    path: &str,
    ephemeris: &FieldEphemeris,
    per_time: &[&FieldEntry],
    times_s: &[f64],
) -> Result<(), ObservationError> {
    let Bodies {
        kept,
        sample_times,
        directions: bodies,
        distances: body_distances,
    } = bodies(ephemeris, times_s).ok_or_else(|| ObservationError::InvalidRequest {
        reason: format!(
            "the ephemeris of {} does not cover the dataset's times",
            ephemeris.name()
        ),
    })?;
    let directions = &ephemeris.directions_rad()[kept.clone()];
    let distances: Vec<[f64; 1]> = ephemeris.distances_m()[kept.clone()]
        .iter()
        .map(|distance| [*distance])
        .collect();
    create_group(store, path, json!({"type": EPHEMERIS_DATASET}))?;
    names(store, path, per_time, "time")?;
    let frame = direction_name(ephemeris.frame())?;
    let samples = sample_times.len() as u64;
    // XRADIO's schema types the axis by its name (its converter writes
    // `time`, which its own schema check refuses).
    let mut time = time_attributes(casa_types::measures::EpochRef::UTC)?;
    time.insert("type".into(), "time_ephemeris".into());
    coordinate(
        store,
        &format!("{path}/time_ephemeris"),
        json!("float64"),
        json!("NaN"),
        "time_ephemeris",
        &sample_times,
        Value::Object(time),
    )?;
    for (name, labels) in [
        ("sky_dis_label", &["dist"][..]),
        ("cartesian_pos_label", &["x", "y", "z"]),
        ("ellipsoid_dir_label", &["lon", "lat"]),
        ("ellipsoid_dis_label", &["dist"]),
    ] {
        let labels: Vec<String> = labels.iter().map(|label| (*label).to_string()).collect();
        strings(
            store,
            &format!("{path}/{name}"),
            &[labels.len() as u64],
            &[name],
            &labels,
        )?;
    }
    variable(
        store,
        &format!("{path}/SOURCE_DIRECTION"),
        &[samples, 2],
        &["time_ephemeris", "sky_dir_label"],
        directions.as_flattened(),
        json!({"type": "sky_coord", "frame": frame, "units": "rad"}),
    )?;
    variable(
        store,
        &format!("{path}/SOURCE_DISTANCE"),
        &[samples, 1],
        &["time_ephemeris", "sky_dis_label"],
        distances.as_flattened(),
        json!({"type": "sky_coord", "frame": frame, "units": "m"}),
    )?;
    variable(
        store,
        &format!("{path}/SOURCE_RADIAL_VELOCITY"),
        &[samples],
        &["time_ephemeris"],
        &ephemeris.radial_velocities_m_s()[kept],
        json!({"type": "quantity", "units": "m/s"}),
    )?;
    // The ephemeris is the body seen from the Earth's centre.
    variable(
        store,
        &format!("{path}/OBSERVER_POSITION"),
        &[3],
        &["cartesian_pos_label"],
        &[0.0; 3],
        json!({"type": "location", "units": "m", "frame": "ITRS", "origin_object_name": "Earth", "coordinate_system": "geocentric"}),
    )?;
    let times = times_s.len() as u64;
    let centres: Vec<f64> = per_time
        .iter()
        .zip(&bodies)
        .flat_map(|(field, body)| {
            let (longitude, latitude) = field.phase_centre.as_angles();
            [
                wrap_to_pi(longitude + body[0]),
                wrap_to_pi(latitude + body[1]),
            ]
        })
        .collect();
    variable(
        store,
        &format!("{path}/FIELD_PHASE_CENTER_DIRECTION"),
        &[times, 2],
        &["time", "sky_dir_label"],
        &centres,
        json!({"type": "sky_coord", "frame": frame, "units": "rad", "coordinates": "field_name source_name"}),
    )?;
    variable(
        store,
        &format!("{path}/FIELD_PHASE_CENTER_DISTANCE"),
        &[times, 1],
        &["time", "sky_dis_label"],
        body_distances.as_flattened(),
        json!({"type": "sky_coord", "frame": frame, "units": "m"}),
    )
}

/// The samples of `ephemeris` a dataset of `times_s` (Unix seconds) holds
/// ([`kept`]).
pub(in crate::observation_io) fn kept_samples(
    ephemeris: &FieldEphemeris,
    times_s: &[f64],
) -> Range<usize> {
    kept(&unix_seconds(ephemeris), times_s)
}

/// `ephemeris`'s sample times, Unix seconds.
fn unix_seconds(ephemeris: &FieldEphemeris) -> Vec<f64> {
    ephemeris
        .times_mjd()
        .iter()
        .map(|mjd| (mjd - UNIX_EPOCH_MJD) * SECONDS_PER_DAY)
        .collect()
}

/// The samples (at `sample_times`) a dataset of `times` keeps: from the
/// last before its first time to the first at or after
/// [`EVALUATION_REACH_S`] past its last, and at least two. XRADIO keeps
/// those either side of the times themselves (`project_min_max_sorted`):
/// the last before the first time, and the first after the last. A sample
/// within [`TIME_RESOLUTION_S`] of the first time is at it, not before it.
fn kept(sample_times: &[f64], times: &[f64]) -> Range<usize> {
    let last = sample_times.len() - 1;
    let (Some(first), Some(end)) = (times.first(), times.last()) else {
        return 0..sample_times.len();
    };
    let start = sample_times
        .partition_point(|time| *time < first - TIME_RESOLUTION_S)
        .saturating_sub(1)
        .min(last - 1);
    let end = sample_times
        .partition_point(|time| *time < end + EVALUATION_REACH_S)
        .clamp(start + 1, last);
    start..end + 1
}

/// The `field_name` and `source_name` coordinates of `fields`, along
/// `dimension` (XRADIO names an absent source `Unknown`), and the
/// direction labels.
fn names(
    store: &Arc<FilesystemStore>,
    path: &str,
    fields: &[&FieldEntry],
    dimension: &str,
) -> Result<(), ObservationError> {
    let count = [fields.len() as u64];
    let column =
        |f: &dyn Fn(&FieldEntry) -> String| fields.iter().map(|field| f(field)).collect::<Vec<_>>();
    strings(
        store,
        &format!("{path}/field_name"),
        &count,
        &[dimension],
        &column(&|f| f.name.clone()),
    )?;
    strings(
        store,
        &format!("{path}/source_name"),
        &count,
        &[dimension],
        &column(&|f| {
            f.source_name
                .clone()
                .unwrap_or_else(|| super::super::UNKNOWN_SOURCE.to_string())
        }),
    )?;
    strings(
        store,
        &format!("{path}/sky_dir_label"),
        &[2],
        &["sky_dir_label"],
        &["ra", "dec"].map(String::from),
    )?;
    Ok(())
}

/// A small `float64` variable written whole, in one chunk.
fn variable(
    store: &Arc<FilesystemStore>,
    path: &str,
    shape: &[u64],
    dimensions: &[&str],
    values: &[f64],
    attributes: Value,
) -> Result<(), ObservationError> {
    let array = create_array(
        store,
        path,
        ArraySpec {
            shape,
            chunks: shape,
            data_type: json!("float64"),
            codecs: codecs(Msv4Codec::Zstd, 8),
            fill: json!("NaN"),
            dimensions,
            attributes,
        },
    )?;
    array
        .store_array_subset(&ArraySubset::new_with_shape(shape.to_vec()), values)
        .map_err(storage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dataset_keeps_the_samples_either_side_and_its_last_times_tdb() {
        let samples: Vec<f64> = (0..10).map(|step| f64::from(step) * 60.0).collect();
        // Times from 130 s to 250 s: kept from the sample at 120 s to the
        // first at or after 370 s, 420 s.
        assert_eq!(kept(&samples, &[130.0, 190.0, 250.0]), 2..8);
        // A time on a sample keeps the sample before it too: casacore's
        // MeasComet indexes a time on a table's first row as row -1 for
        // some roundings.
        assert_eq!(kept(&samples, &[120.0, 130.0]), 1..6);
        // So does one a rounding past a sample.
        assert_eq!(kept(&samples, &[120.0 + 1.0e-7, 130.0]), 1..6);
        // Times beyond the samples keep the last two.
        assert_eq!(kept(&samples, &[1_000.0]), 8..10);
        // Times before them keep the first two, or as far as they reach.
        assert_eq!(kept(&samples, &[-500.0]), 0..2);
        assert_eq!(kept(&samples, &[-500.0, 10.0]), 0..4);
    }
}
