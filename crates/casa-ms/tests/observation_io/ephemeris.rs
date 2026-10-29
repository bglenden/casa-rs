// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fields that follow an ephemeris in MSv4 (#942): XRADIO's two layouts of
//! `field_and_source_ephemeris` read as the MSv2 they were converted from
//! holds the ephemeris, and the converter carries the ephemeris both ways.
//!
//! The fixtures are the trial MSv2 with its field following a Moon-like
//! J2000 body sampled every minute (`ms_trial_ephemeris.ms`) and XRADIO's
//! conversions of it (`tools/ms-trial/make_ephemeris_fixtures.sh`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_ms::observation_io::convert::{LossKind, to_msv2, to_msv4};
use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{Catalogue, FieldEntry, FieldEphemeris, FieldId, ObservationError};
use casa_test_support::ephemeris_table::{EphemerisFrame, EphemerisRow, write_field_ephemeris_in};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::radial_velocity::RadialVelocityRef;
use casa_types::{ArrayValue, ScalarValue, Value};

use super::fixture;

/// The losses converting the fixtures takes.
const ACCEPT: [LossKind; 3] = [LossKind::Sigma, LossKind::Tables, LossKind::Columns];

/// Seconds a day.
const DAY_S: f64 = 86_400.0;

fn catalogue(path: &Path) -> Arc<Catalogue> {
    open(path, &OpenOptions::default())
        .expect("open")
        .catalogue()
        .clone()
}

/// The one field of a fixture's catalogue, which follows an ephemeris.
fn moving_field(catalogue: &Catalogue) -> (&FieldEntry, &FieldEphemeris) {
    let [field] = catalogue.fields.as_slice() else {
        panic!("one field, not {}", catalogue.fields.len());
    };
    (field, field.ephemeris.as_deref().expect("an ephemeris"))
}

/// Every time of a store's single partition, MJD days, and its fields.
fn times(path: &Path) -> (Vec<f64>, Vec<FieldId>) {
    let observation = open(path, &OpenOptions::default()).expect("open");
    let id = observation.catalogue().partition_ids()[0];
    let grid = observation.grid(id, "base").expect("grid");
    let origin = grid.time_origin.day() + grid.time_origin.frac();
    (
        grid.times_s
            .iter()
            .map(|time| origin + time / DAY_S)
            .collect(),
        grid.field_per_time,
    )
}

fn assert_close(what: &str, a: f64, b: f64, tolerance: f64) {
    assert!(
        (a - b).abs() <= tolerance,
        "{what}: {a} and {b} differ by {:e}",
        (a - b).abs()
    );
}

/// `ephemeris`'s samples equal `reference`'s from sample `first` on, to
/// the rounding of converting units and times twice.
fn assert_samples(ephemeris: &FieldEphemeris, reference: &FieldEphemeris, first: usize) {
    for index in 0..ephemeris.times_mjd().len() {
        assert_sample(ephemeris, index, reference, first + index);
    }
}

/// `ephemeris`'s sample `index` equals `reference`'s sample `at`, as
/// [`assert_samples`] compares them.
fn assert_sample(ephemeris: &FieldEphemeris, index: usize, reference: &FieldEphemeris, at: usize) {
    assert_close(
        "time",
        ephemeris.times_mjd()[index],
        reference.times_mjd()[at],
        1.0e-6 / DAY_S,
    );
    for axis in 0..2 {
        assert_close(
            "direction",
            ephemeris.directions_rad()[index][axis],
            reference.directions_rad()[at][axis],
            1.0e-15,
        );
    }
    let distance = reference.distances_m()[at];
    assert_close(
        "distance",
        ephemeris.distances_m()[index],
        distance,
        1.0e-15 * distance,
    );
    let velocity = reference.radial_velocities_m_s()[at];
    assert_close(
        "velocity",
        ephemeris.radial_velocities_m_s()[index],
        velocity,
        1.0e-13 * velocity.abs(),
    );
}

/// The index of `reference`'s sample at `time` (MJD).
fn sample_at(reference: &FieldEphemeris, time: f64) -> usize {
    reference
        .times_mjd()
        .iter()
        .position(|known| (known - time).abs() <= 1.0e-6 / DAY_S)
        .expect("a sample of the reference")
}

/// XRADIO's `time_ephemeris` layout reads as the MSv2's own ephemeris: the
/// samples from the last before the first time to the first after
/// the last, the offset recovered from its sum with the body's direction,
/// and the frames.
#[test]
fn xradios_ephemeris_samples_read_as_the_msv2_holds_them() {
    let root = tempfile::tempdir().expect("root");
    let v2 = catalogue(&fixture(root.path(), "ms_trial_ephemeris.ms"));
    let store = fixture(root.path(), "ms_trial_ephemeris.ps.zarr");
    let v4 = catalogue(&store);
    let (reference_field, reference) = moving_field(&v2);
    let (field, ephemeris) = moving_field(&v4);
    assert_eq!(field.name, "trial_0");
    assert_eq!(ephemeris.frame(), DirectionRef::J2000);
    assert_eq!(ephemeris.velocity_frame(), RadialVelocityRef::GEO);
    let (offset, reference_offset) = (
        field.phase_centre.as_angles(),
        reference_field.phase_centre.as_angles(),
    );
    assert_close("offset", offset.0, reference_offset.0, 1.0e-15);
    assert_close("offset", offset.1, reference_offset.1, 1.0e-15);
    let (times, fields) = times(&store);
    assert!(fields.iter().all(|id| *id == FieldId(0)));
    let first = sample_at(reference, ephemeris.times_mjd()[0]);
    assert_samples(ephemeris, reference, first);
    let samples = ephemeris.times_mjd();
    assert!(samples[0] < times[0] && samples[1] >= times[0]);
    let last = samples[samples.len() - 1];
    assert!(last > times[times.len() - 1] && samples[samples.len() - 2] <= times[times.len() - 1]);
}

/// XRADIO's interpolated layout (`ephemeris_interpolate=True`), the samples
/// interpolated onto the visibility
/// times, reads as those values: the MSv2's samples interpolated linearly
/// in angle (and in distance) at each time. The test interpolates in MJD
/// days, whose rounding (0.6 µs) moves this body by up to 2e-12 rad and
/// 1e-4 m; XRADIO interpolates in Unix seconds.
#[test]
fn xradios_interpolated_ephemeris_reads_as_its_values_at_the_times() {
    let root = tempfile::tempdir().expect("root");
    let v2 = catalogue(&fixture(root.path(), "ms_trial_ephemeris.ms"));
    let store = fixture(root.path(), "ms_trial_ephemeris_interpolated.ps.zarr");
    let v4 = catalogue(&store);
    let (reference_field, reference) = moving_field(&v2);
    let (field, ephemeris) = moving_field(&v4);
    let (offset, reference_offset) = (
        field.phase_centre.as_angles(),
        reference_field.phase_centre.as_angles(),
    );
    assert_close("offset", offset.0, reference_offset.0, 1.0e-15);
    assert_close("offset", offset.1, reference_offset.1, 1.0e-15);
    let (times, _) = times(&store);
    assert_eq!(ephemeris.times_mjd().len(), times.len());
    for (index, time) in times.iter().enumerate() {
        assert_close("time", ephemeris.times_mjd()[index], *time, 1.0e-6 / DAY_S);
        let after = reference
            .times_mjd()
            .partition_point(|sample| sample <= time);
        let (t0, t1) = (
            reference.times_mjd()[after - 1],
            reference.times_mjd()[after],
        );
        let factor = (time - t0) / (t1 - t0);
        let between = |a: f64, b: f64| a + factor * (b - a);
        for axis in 0..2 {
            let expected = between(
                reference.directions_rad()[after - 1][axis],
                reference.directions_rad()[after][axis],
            );
            assert_close(
                "direction",
                ephemeris.directions_rad()[index][axis],
                expected,
                1.0e-11,
            );
        }
        let distance = between(
            reference.distances_m()[after - 1],
            reference.distances_m()[after],
        );
        assert_close(
            "distance",
            ephemeris.distances_m()[index],
            distance,
            1.0e-12 * distance,
        );
    }
}

/// The converter carries a moving field both ways: into an MSv4 the
/// samples from the last before the first time to the first two
/// minutes past the last (so the last time's TDB is covered), reported for
/// the rest; back into an MSv2 the same samples as the table casacore
/// reads, the field's `EPHEMERIS_ID`, its offset as `PHASE_DIR` and its
/// first time as `TIME`.
#[test]
fn a_moving_field_converts_both_ways() {
    let root = tempfile::tempdir().expect("root");
    let source = fixture(root.path(), "ms_trial_ephemeris.ms");
    let v2 = catalogue(&source);
    let (reference_field, reference) = moving_field(&v2);
    let v4_path = root.path().join("ours.ps.zarr");
    let report = to_msv4(&source, &v4_path, &super::convert::options(&ACCEPT)).expect("to MSv4");
    let v4 = catalogue(&v4_path);
    let (field, ephemeris) = moving_field(&v4);
    let (times, _) = times(&v4_path);
    let first = sample_at(reference, ephemeris.times_mjd()[0]);
    assert_samples(ephemeris, reference, first);
    let samples = ephemeris.times_mjd();
    let reach = times[times.len() - 1] + 120.0 / DAY_S;
    assert!(samples[0] < times[0] && samples[1] >= times[0]);
    assert!(samples[samples.len() - 1] >= reach && samples[samples.len() - 2] < reach);
    let unwritten = reference.times_mjd().len() - samples.len();
    let losses: Vec<String> = report.losses.iter().map(ToString::to_string).collect();
    assert!(
        losses
            .iter()
            .any(|loss| loss.contains(&format!("samples no dataset keeps ({unwritten};"))),
        "{losses:?}"
    );
    assert!(
        losses.iter().any(|loss| loss.starts_with("FIELD TIME")),
        "{losses:?}"
    );
    let offset = field.phase_centre.as_angles();
    let reference_offset = reference_field.phase_centre.as_angles();
    assert_close("offset", offset.0, reference_offset.0, 1.0e-15);
    assert_close("offset", offset.1, reference_offset.1, 1.0e-15);
    assert_eq!(field.phase_centre.refer(), DirectionRef::J2000);

    let back_path = root.path().join("back.ms");
    to_msv2(&v4_path, &back_path, &super::convert::options(&ACCEPT)).expect("to MSv2");
    let back = catalogue(&back_path);
    let (back_field, back_ephemeris) = moving_field(&back);
    assert_samples(back_ephemeris, reference, first);
    assert_eq!(back_ephemeris.times_mjd().len(), samples.len());
    assert_eq!(back_field.phase_centre.as_angles(), offset);
    let field_time = back_field
        .reference_epoch
        .as_ref()
        .expect("FIELD TIME")
        .value()
        .as_mjd();
    assert_close("FIELD TIME", field_time, times[0], 1.0e-6 / DAY_S);
    assert!(back_path.join("FIELD/EPHEM0_trial_0.tab").is_dir());

    // An MSv2 into an MSv2 keeps every sample and the FIELD row's TIME.
    let again_path = root.path().join("again.ms");
    to_msv2(&source, &again_path, &super::convert::options(&ACCEPT)).expect("MSv2 to MSv2");
    let again = catalogue(&again_path);
    let (again_field, again_ephemeris) = moving_field(&again);
    assert_samples(again_ephemeris, reference, 0);
    assert_eq!(
        again_ephemeris.times_mjd().len(),
        reference.times_mjd().len()
    );
    assert_close(
        "FIELD TIME",
        again_field
            .reference_epoch
            .as_ref()
            .expect("TIME")
            .value()
            .as_mjd(),
        reference_field
            .reference_epoch
            .as_ref()
            .expect("TIME")
            .value()
            .as_mjd(),
        1.0e-6 / DAY_S,
    );
}

/// The converter's dataset and XRADIO's of the same MSv2 are one field
/// with one ephemeris: in one processing set the reader unites their
/// samples, which agree where both hold them.
#[test]
fn two_converters_datasets_of_one_field_are_one_field() {
    let root = tempfile::tempdir().expect("root");
    let source = fixture(root.path(), "ms_trial_ephemeris.ms");
    let ours = root.path().join("ours.ps.zarr");
    to_msv4(&source, &ours, &super::convert::options(&ACCEPT)).expect("to MSv4");
    let ours_samples = moving_field(&catalogue(&ours)).1.times_mjd().len();
    let theirs = fixture(root.path(), "ms_trial_ephemeris.ps.zarr");
    let dataset = std::fs::read_dir(&theirs)
        .expect("XRADIO's store")
        .filter_map(Result::ok)
        .find(|entry| entry.path().join("zarr.json").is_file() && entry.path().is_dir())
        .expect("a dataset")
        .path();
    let copied = ours.join("xradio_0");
    copy_dir(&dataset, &copied);
    let both = catalogue(&ours);
    let (_, ephemeris) = moving_field(&both);
    assert_eq!(ephemeris.times_mjd().len(), ours_samples);
    assert_eq!(both.partitions.len(), 2);
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("directory");
    for entry in std::fs::read_dir(from)
        .expect("read")
        .filter_map(Result::ok)
    {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy");
        }
    }
}

/// Seconds [`two_sessions_two_fields`] moves its second session later:
/// past the samples the first session's dataset keeps.
const APART_S: f64 = 600.0;

/// The second field's offset from the body, radians: west of it, so that
/// its right ascension wraps.
const WEST_RAD: [f64; 2] = [-2.0e-5, 3.0e-6];

/// The ephemeris fixture with a second field, `west`, following the same
/// body at [`WEST_RAD`] and observed at every other time of the first
/// session, and its later times a second session moved [`APART_S`] later. The body's table is
/// replaced by one, sampled as often, that moves linearly in angle and
/// reaches past the moved times.
fn two_sessions_two_fields(root: &Path) -> PathBuf {
    let path = fixture(root, "ms_trial_ephemeris.ms");
    let original = catalogue(&path);
    let (_, body) = moving_field(&original);
    let step_s = (body.times_mjd()[1] - body.times_mjd()[0]) * DAY_S;
    let rows: Vec<EphemerisRow> = (0..body.times_mjd().len() + (APART_S / step_s) as usize)
        .map(|sample| {
            let t = sample as f64 * step_s;
            [
                body.times_mjd()[0] + t / DAY_S,
                120.0 + 1.5e-4 * t,
                18.0 - 4.0e-5 * t,
                0.00257,
                1.0e-5,
            ]
        })
        .collect();
    std::fs::remove_dir_all(path.join("FIELD/EPHEM0_Moonish.tab")).expect("the old table");
    write_field_ephemeris_in(&path, 0, "Moonish", &rows, EphemerisFrame::J2000);
    super::convert::split_sessions(&path, false);

    let mut ms = casa_ms::MeasurementSet::open(&path).expect("ms");
    let mut west = ms
        .subtable(casa_ms::SubtableId::Field)
        .and_then(|table| table.rows().ok()?.first().cloned())
        .expect("a field");
    west.upsert(
        "NAME",
        Value::Scalar(ScalarValue::String("west".to_string())),
    );
    let offset = Value::Array(ArrayValue::Float64(
        ndarray::ArrayD::from_shape_vec(vec![2, 1], WEST_RAD.to_vec()).expect("shape"),
    ));
    for column in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"] {
        west.upsert(column, offset.clone());
    }
    ms.subtable_mut(casa_ms::SubtableId::Field)
        .expect("FIELD")
        .add_row(west)
        .expect("a second field");
    let scalar = |ms: &casa_ms::MeasurementSet, row: usize, column: &str| {
        ms.main_table()
            .cell_accessor(row, column)
            .and_then(|cell| cell.scalar().cloned())
            .expect("a cell")
    };
    let times: Vec<f64> = (0..ms.main_table().row_count())
        .map(|row| match scalar(&ms, row, "TIME") {
            ScalarValue::Float64(time) => time,
            other => panic!("TIME {other:?}"),
        })
        .collect();
    let mut distinct = times.clone();
    distinct.sort_by(f64::total_cmp);
    distinct.dedup();
    for (row, time) in times.iter().enumerate() {
        let index = distinct.partition_point(|known| known < time);
        let later = scalar(&ms, row, "OBSERVATION_ID") == ScalarValue::Int32(1);
        let mut cells = vec![(
            "FIELD_ID",
            ScalarValue::Int32(i32::from(!later && index % 2 == 1)),
        )];
        if later {
            for column in ["TIME", "TIME_CENTROID"] {
                let ScalarValue::Float64(value) = scalar(&ms, row, column) else {
                    panic!("{column}");
                };
                cells.push((column, ScalarValue::Float64(value + APART_S)));
            }
        }
        for (column, value) in cells {
            ms.main_table_mut()
                .cell_accessor_mut(row, column)
                .and_then(|mut cell| cell.set(Value::Scalar(value)))
                .expect("a cell");
        }
    }
    ms.save().expect("save");
    path
}

/// The difference of two angles, radians, whatever turn each is stated in.
fn angle_between(a: f64, b: f64) -> f64 {
    (a - b + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU) - std::f64::consts::PI
}

/// Two fields of one body in two datasets whose samples have a gap between
/// them, one field in both and the other, west of the body, in the first
/// only: the reader makes each one catalogue field, the one whose offset
/// wraps as well as the other, and both follow one ephemeris, the union of
/// the datasets' samples (a body's, not a field's). Into an MSv2 that union
/// becomes one evenly spaced table, its gap filled by the ephemeris's
/// interpolation and reported.
#[test]
fn one_bodys_fields_in_two_datasets_follow_their_united_samples() {
    let root = tempfile::tempdir().expect("root");
    let source = two_sessions_two_fields(root.path());
    let v2 = catalogue(&source);
    let reference = v2.fields[0].ephemeris.as_deref().expect("an ephemeris");
    let v4_path = root.path().join("apart.ps.zarr");
    let options = super::convert::options(&ACCEPT);
    to_msv4(&source, &v4_path, &options).expect("to MSv4");
    let v4 = catalogue(&v4_path);
    assert_eq!(v4.partitions.len(), 2);
    let [first, second] = v4.fields.as_slice() else {
        panic!("two fields, not {:?}", v4.fields);
    };
    // An MSv4 names a field by its MSv2 name and row, as XRADIO does.
    for (field, stated) in [first, second].into_iter().zip(&v2.fields) {
        assert!(field.name.starts_with(&stated.name), "{}", field.name);
        let (offset, expected) = (
            field.phase_centre.as_angles(),
            stated.phase_centre.as_angles(),
        );
        assert_close("offset", angle_between(offset.0, expected.0), 0.0, 4.0e-15);
        assert_close("offset", angle_between(offset.1, expected.1), 0.0, 4.0e-15);
    }
    let united = first.ephemeris.as_ref().expect("an ephemeris");
    assert!(Arc::ptr_eq(
        united,
        second.ephemeris.as_ref().expect("an ephemeris")
    ));
    let step = reference.times_mjd()[1] - reference.times_mjd()[0];
    let samples = united.times_mjd();
    for (index, time) in samples.iter().enumerate() {
        assert_sample(united, index, reference, sample_at(reference, *time));
    }
    let span = ((samples[samples.len() - 1] - samples[0]) / step).round() as usize + 1;
    let filled = span - samples.len();
    assert!(filled > 0, "the datasets' samples have a gap");

    let back_path = root.path().join("apart.ms");
    let report = to_msv2(&v4_path, &back_path, &options).expect("to MSv2");
    let losses: Vec<String> = report.losses.iter().map(ToString::to_string).collect();
    assert!(
        losses.iter().any(|loss| loss.contains(&format!(
            "the ephemeris of {}: that its samples have gaps ({filled} rows filled",
            united.name()
        ))),
        "{losses:?}"
    );
    let back = catalogue(&back_path);
    let back_ephemeris = back.fields[0].ephemeris.as_deref().expect("an ephemeris");
    assert!(
        back.fields
            .iter()
            .all(|field| field.ephemeris.as_deref() == Some(back_ephemeris))
    );
    assert_eq!(back_ephemeris.times_mjd().len(), span);
    // A row in the gap is the united ephemeris's own interpolation there
    // (of Cartesian positions, which leaves this body's path, linear in
    // angle, by up to 1.3e-8 rad); every other row is a sample.
    for (index, time) in back_ephemeris.times_mjd().iter().enumerate() {
        assert_close(
            "time",
            *time,
            samples[0] + index as f64 * step,
            1.0e-6 / DAY_S,
        );
        if let Some(at) = samples
            .iter()
            .position(|sample| (sample - time).abs() <= 1.0e-6 / DAY_S)
        {
            assert_sample(back_ephemeris, index, united, at);
            continue;
        }
        let [x, y, z] = united.sample(*time).expect("inside the samples").position_m;
        let distance = (x * x + y * y + z * z).sqrt();
        let direction = [y.atan2(x), (z / distance).asin()];
        for (axis, expected) in direction.into_iter().enumerate() {
            let found = back_ephemeris.directions_rad()[index][axis];
            assert_close("direction", angle_between(found, expected), 0.0, 1.0e-14);
        }
        assert_close(
            "distance",
            back_ephemeris.distances_m()[index],
            distance,
            1.0e-14 * distance,
        );
    }
}

/// An ephemeris that does not cover a dataset's times cannot state its
/// phase centre at them, so writing it is refused before anything is
/// written; a field whose direction is a polynomial in time is refused as
/// XRADIO refuses it.
#[test]
fn uncovered_and_polynomial_moving_fields_are_refused() {
    let root = tempfile::tempdir().expect("root");
    let uncovered = fixture(root.path(), "ms_trial.ms");
    super::follow_ephemerides(&uncovered, &[0]);
    let out = root.path().join("uncovered.ps.zarr");
    let refused = to_msv4(&uncovered, &out, &super::convert::options(&ACCEPT));
    assert!(
        matches!(&refused, Err(ObservationError::InvalidRequest { reason }) if reason.contains("does not cover")),
        "{refused:?}"
    );
    assert!(!out.exists());

    let polynomial_root = tempfile::tempdir().expect("root");
    let polynomial = fixture(polynomial_root.path(), "ms_trial.ms");
    let mut ms = casa_ms::MeasurementSet::open(&polynomial).expect("ms");
    ms.field_mut()
        .and_then(|mut fields| fields.set_i32(0, "NUM_POLY", 1))
        .expect("a polynomial");
    ms.save().expect("save");
    drop(ms);
    let out = polynomial_root.path().join("moving.ps.zarr");
    let refused = to_msv4(&polynomial, &out, &super::convert::options(&ACCEPT));
    assert!(
        matches!(refused, Err(ObservationError::Unsupported { what }) if what.contains("polynomial")),
        "{refused:?}"
    );
    assert!(!out.exists());
}
