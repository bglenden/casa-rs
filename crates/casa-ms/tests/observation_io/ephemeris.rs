// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fields that follow an ephemeris in MSv4 (#942): XRADIO's two layouts of
//! `field_and_source_ephemeris` read as the MSv2 they were converted from
//! holds the ephemeris, and the converter carries the ephemeris both ways.
//!
//! The fixtures are the trial MSv2 with its field following a Moon-like
//! J2000 body sampled every minute (`ms_trial_ephemeris.ms`) and XRADIO's
//! conversions of it (`tools/ms-trial/make_ephemeris_fixtures.sh`).

use std::path::Path;
use std::sync::Arc;

use casa_ms::observation_io::convert::{LossKind, to_msv2, to_msv4};
use casa_ms::observation_io::{OpenOptions, open};
use casa_observation::{Catalogue, FieldEntry, FieldEphemeris, FieldId, ObservationError};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::radial_velocity::RadialVelocityRef;

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
    for (index, time) in ephemeris.times_mjd().iter().enumerate() {
        let at = first + index;
        assert_close("time", *time, reference.times_mjd()[at], 1.0e-6 / DAY_S);
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
/// samples from the last at or before the first time to the first after
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
    assert!(samples[0] <= times[0] && samples[1] > times[0]);
    let last = samples[samples.len() - 1];
    assert!(last > times[times.len() - 1] && samples[samples.len() - 2] <= times[times.len() - 1]);
}

/// XRADIO's default layout, the samples interpolated onto the visibility
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
    let (_, reference) = moving_field(&v2);
    let (field, ephemeris) = moving_field(&v4);
    assert!(field.phase_centre.as_angles().0.abs() > 0.0);
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
/// samples from the last at or before the first time to the first two
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
    assert!(samples[0] <= times[0] && samples[1] > times[0]);
    assert!(samples[samples.len() - 1] >= reach && samples[samples.len() - 2] < reach);
    let unwritten = reference.times_mjd().len() - samples.len();
    let losses: Vec<String> = report.losses.iter().map(ToString::to_string).collect();
    assert!(
        losses.iter().any(|loss| loss.contains(&format!(
            "samples beyond every partition's times and those either side ({unwritten})"
        ))),
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
