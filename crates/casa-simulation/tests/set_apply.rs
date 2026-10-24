// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setapply` then `sm.corrupt` (#841 Phase 3): a calibration table
//! corrupts the model as the simulated term of its type does, its
//! solutions found and interpolated in time as `casa-calibration` applies
//! them. Agreement with CASA is the corruption scenarios'
//! (`tools/simulation/sm_corrupt_scenarios.json`).

mod predict_support;

use std::path::{Path, PathBuf};

use casa_calibration::{
    ApplyInterpolationMode, CalibrationTableLookup, SimulatedJones, SimulatedSolution,
    SolutionValues, write_simulated_jones_table,
};
use casa_simulation::{
    ApplySpec, CasaOutcome, GainSpec, LeakageSpec, PredictSpec, Simulator, SimulatorOptions,
    SimulatorResult,
};
use casa_types::Complex32;
use casa_types::measures::{EpochRef, MEpoch};
use predict_support::{
    ANTENNAS, CENTRE_RAD, REFERENCE_MJD, Row, bits, data, read_main, resources, scan, session,
    write_point_list,
};

fn apply(table: &Path, interp: &str) -> ApplySpec {
    ApplySpec {
        table: table.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.0,
        interp: interp.to_string(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.0,
    }
}

/// A session of `stokes` observing `target` twice in integrations of
/// `integration_s`, with a 1 Jy point at its centre predicted.
fn observed(path: &Path, stokes: &str, point: &Path, integration_s: f64) -> Simulator {
    let mut sm = session(path, "ALMA", stokes, resources(2, 2 << 30));
    sm.set_times(
        integration_s,
        false,
        MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    )
    .expect("settimes");
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    sm.observe(&scan("target", 120.0, 180.0))
        .expect("a second scan");
    sm.predict(&PredictSpec {
        images: Vec::new(),
        component_list: Some(point.to_path_buf()),
        incremental: false,
    })
    .expect("predict");
    sm
}

fn point(directory: &Path) -> PathBuf {
    let path = directory.join("point.cl");
    write_point_list(&path, CENTRE_RAD, 1.0);
    path
}

fn corrupted(
    path: &Path,
    stokes: &str,
    point: &Path,
    steps: &dyn Fn(&mut Simulator) -> SimulatorResult<()>,
) -> Vec<Row> {
    let mut sm = observed(path, stokes, point, 10.0);
    steps(&mut sm).expect("the steps");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");
    read_main(path).0
}

/// A gain table and a leakage table written by `setgain` and `setleakage`
/// corrupt, through `setapply`, as the terms that wrote them did: at the
/// tables' own times every interpolation gives the solution itself, so the
/// DATA agree bit for bit, the leakage applied before the gains in both.
/// After `openfromms` too.
#[test]
fn applying_the_tables_of_simulated_terms_corrupts_as_they_did() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let (gains, leakage) = (path("g.cal"), path("d.cal"));
    let simulated = corrupted(&path("simulated.ms"), "XX XY YX YY", &point, &|sm| {
        sm.set_gain(&GainSpec {
            mode: "fbm".to_string(),
            table: gains.to_string_lossy().into_owned(),
            amplitude: vec![0.05],
        })?;
        sm.set_leakage(&LeakageSpec {
            table: leakage.to_string_lossy().into_owned(),
            amplitude: vec![0.05],
            offset: vec![0.02],
        })
    });
    for interp in ["linear", "nearest"] {
        let applied = corrupted(
            &path(&format!("{interp}.ms")),
            "XX XY YX YY",
            &point,
            &|sm| {
                sm.set_apply(&apply(&gains, interp))?;
                sm.set_apply(&ApplySpec {
                    kind: "D".to_string(),
                    ..apply(&leakage, interp)
                })
            },
        );
        assert_eq!(bits(&data(&applied)), bits(&data(&simulated)), "{interp}");
    }
    observed(&path("opened.ms"), "XX XY YX YY", &point, 10.0)
        .close()
        .expect("close");
    let mut sm = Simulator::open_from_ms(path("opened.ms"), options()).expect("openfromms");
    sm.set_apply(&apply(&gains, "linear")).expect("setapply G");
    sm.set_apply(&apply(&leakage, "linear"))
        .expect("setapply D");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close the opened MeasurementSet");
    let opened = read_main(&path("opened.ms")).0;
    assert_eq!(
        bits(&data(&opened)),
        bits(&data(&simulated)),
        "after openfromms"
    );
}

/// Between a table's solutions each row takes the gains interpolated in
/// amplitude and phase at its time, as CASA's `setapply` does (measured):
/// a table of 30 s solutions applied to 10 s integrations.
#[test]
fn rows_between_solutions_take_the_interpolated_gains() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let gains = path("coarse.cal");
    let mut sm = observed(&path("coarse.ms"), "XX YY", &point, 30.0);
    sm.set_gain(&GainSpec {
        mode: "fbm".to_string(),
        table: gains.to_string_lossy().into_owned(),
        amplitude: vec![0.1],
    })
    .expect("setgain");
    sm.close().expect("close");
    let rows = corrupted(&path("fine.ms"), "XX YY", &point, &|sm| {
        sm.set_apply(&apply(&gains, "linear"))
    });
    let lookup =
        CalibrationTableLookup::open(&gains, ApplyInterpolationMode::Linear).expect("open");
    let times = read_times(&path("fine.ms"));
    let mut between = 0;
    for (row, &time) in rows.iter().zip(&times) {
        let gain = |antenna: u32| {
            lookup
                .solution(0, 0, antenna as i32, time)
                .expect("lookup")
                .expect("a solution")
                .value(0, 0)
        };
        let [p, q] = row.antennas;
        let expected: Complex32 = gain(p) * gain(q).conj();
        assert!(
            (row.data[0] - expected).norm() < 1e-5,
            "{} against {expected}",
            row.data[0]
        );
        between += usize::from((time - REFERENCE_MJD * 86_400.0).rem_euclid(30.0) > 1.0);
    }
    assert!(between > 0, "some rows fall between the table's solutions");
}

/// A scan observed after `setapply` takes the table's gains at its own
/// times, as CASA's does (measured): the table is evaluated at `corrupt`,
/// over the scans observed by then.
#[test]
fn a_scan_observed_after_setapply_takes_the_tables_gains() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let gains = path("coarse.cal");
    let mut sm = observed(&path("coarse.ms"), "XX YY", &point, 30.0);
    sm.set_gain(&GainSpec {
        mode: "fbm".to_string(),
        table: gains.to_string_lossy().into_owned(),
        amplitude: vec![0.1],
    })
    .expect("setgain");
    sm.close().expect("close");

    let later = path("later.ms");
    let mut sm = session(&later, "ALMA", "XX YY", resources(2, 2 << 30));
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    sm.set_apply(&apply(&gains, "linear")).expect("setapply");
    sm.observe(&scan("target", 120.0, 180.0))
        .expect("a scan after the setapply");
    sm.predict(&PredictSpec {
        images: Vec::new(),
        component_list: Some(point.clone()),
        incremental: false,
    })
    .expect("predict");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");

    let lookup =
        CalibrationTableLookup::open(&gains, ApplyInterpolationMode::Linear).expect("open");
    let rows = read_main(&later).0;
    let times = read_times(&later);
    let first_scan_end = times[0] + 60.0;
    let mut after = 0;
    for (row, &time) in rows.iter().zip(&times) {
        let gain = |antenna: u32| {
            lookup
                .solution(0, 0, antenna as i32, time)
                .expect("lookup")
                .expect("a solution")
                .value(0, 0)
        };
        let [p, q] = row.antennas;
        let expected: Complex32 = gain(p) * gain(q).conj();
        assert!(
            (row.data[0] - expected).norm() < 1e-5,
            "{} against {expected}",
            row.data[0]
        );
        after += usize::from(time > first_scan_end);
    }
    assert!(after > 0, "rows of the later scan");
}

/// What `setapply` refuses, as CASA raises or as not yet implemented.
#[test]
fn setapply_refuses_what_casa_or_casa_rs_cannot_apply() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let gains = directory.path().join("g.cal");
    let mut sm = observed(&directory.path().join("g.ms"), "XX YY", &point, 10.0);
    sm.set_gain(&GainSpec {
        mode: "fbm".to_string(),
        table: gains.to_string_lossy().into_owned(),
        amplitude: vec![0.05],
    })
    .expect("setgain");
    sm.close().expect("close");
    let mut sm = observed(&directory.path().join("x.ms"), "XX YY", &point, 10.0);
    let outcome = |sm: &mut Simulator, spec: ApplySpec| {
        let error = sm.set_apply(&spec).expect_err("refused");
        (error.outcome(), error.message().to_string())
    };
    for (spec, expected, says) in [
        (apply(&gains, "aipslin"), CasaOutcome::Raises, "deprecated"),
        (
            apply(&directory.path().join("missing.cal"), "linear"),
            CasaOutcome::Raises,
            "Error in Simulator::setapply",
        ),
        (
            ApplySpec {
                kind: "D".to_string(),
                ..apply(&gains, "linear")
            },
            CasaOutcome::Raises,
            "not D",
        ),
        (
            ApplySpec {
                calwt: true,
                ..apply(&gains, "linear")
            },
            CasaOutcome::NotYetImplemented,
            "calwt",
        ),
        (
            apply(&gains, "cubic"),
            CasaOutcome::NotYetImplemented,
            "cubic",
        ),
    ] {
        let (outcome, message) = outcome(&mut sm, spec);
        assert_eq!(outcome, expected, "{message}");
        assert!(message.contains(says), "{message}");
    }
    sm.close().expect("close");
}

/// A gain table of `solutions` (time, field, gain) for every antenna,
/// written against the MeasurementSet at `ms`.
fn write_gains(ms: &Path, output: &Path, solutions: &[(f64, i32, f32)]) {
    let ms = casa_ms::MeasurementSet::open(ms).expect("open the MeasurementSet");
    let rows = solutions.iter().flat_map(|&(time_s, field_id, gain)| {
        (0..ANTENNAS as i32).map(move |antenna| SimulatedSolution {
            time_s,
            field_id,
            spectral_window_id: 0,
            antenna,
            values: SolutionValues::Receptors([Complex32::new(gain, 0.0); 2]),
        })
    });
    write_simulated_jones_table(&ms, output, SimulatedJones::Gain, rows).expect("write the table");
}

/// The first and last integration times of a session `observed` makes.
fn observed_times(directory: &Path, point: &Path) -> (PathBuf, [f64; 2]) {
    let reference = directory.join("reference.ms");
    observed(&reference, "XX YY", point, 10.0)
        .close()
        .expect("close");
    let times = read_times(&reference);
    let first = times.iter().copied().fold(f64::INFINITY, f64::min);
    let last = times.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    (reference, [first, last])
}

/// CASA's `setapply` takes every field's solutions as one series in time
/// (#893 review, whose CASA 6.7.6 reproduction this is): a table solved on
/// FIELD 0 early (gains 1) and on FIELD 1 late (gains 2) leaves the first
/// scan of rows on FIELD 0 as it was and multiplies the second by
/// |2|² = 4.
#[test]
fn setapply_takes_every_fields_solutions_as_one_series() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let (reference, [first, last]) = observed_times(directory.path(), &point);
    write_gains(
        &reference,
        &path("fields.cal"),
        &[(first, 0, 1.0), (last, 1, 2.0)],
    );
    let rows = corrupted(&path("applied.ms"), "XX YY", &point, &|sm| {
        sm.set_apply(&apply(&path("fields.cal"), "nearest"))
    });
    let times = read_times(&path("applied.ms"));
    let middle = (first + last) / 2.0;
    for (row, time) in rows.iter().zip(&times) {
        assert_eq!(row.field, 0, "the rows are on FIELD 0");
        let expected = if *time < middle { 1.0 } else { 4.0 };
        assert!(
            (row.data[0] - Complex32::new(expected, 0.0)).norm() < 1e-5,
            "at {time}: {} for {expected}",
            row.data[0]
        );
    }
}

/// `setapply` loads the table, as CASA's does (#893 review): moving the
/// table after the call, and writing another in its place, leave the term
/// as it was.
#[test]
fn setapply_keeps_the_table_it_loaded() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let (reference, [first, last]) = observed_times(directory.path(), &point);
    let table = path("gains.cal");
    write_gains(&reference, &table, &[(first, 0, 1.0), (last, 0, 2.0)]);
    let expected = corrupted(&path("expected.ms"), "XX YY", &point, &|sm| {
        sm.set_apply(&apply(&table, "linear"))
    });
    let moved = corrupted(&path("moved.ms"), "XX YY", &point, &|sm| {
        sm.set_apply(&apply(&table, "linear"))?;
        std::fs::rename(&table, path("moved.cal")).expect("move the table");
        write_gains(&reference, &table, &[(first, 0, 3.0), (last, 0, 3.0)]);
        Ok(())
    });
    assert_eq!(bits(&data(&moved)), bits(&data(&expected)));
}

/// Loading a table is admitted before the table is read (#893 review,
/// whose probe this is): within a 2 MiB budget a table of 9,996 rows is
/// refused as an admission.
#[test]
fn a_table_beyond_the_budget_is_refused_before_it_is_loaded() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let (reference, [first, _]) = observed_times(directory.path(), &point);
    let solutions: Vec<(f64, i32, f32)> = (0..9_996 / ANTENNAS)
        .map(|index| (first + index as f64, 0, 1.0))
        .collect();
    write_gains(&reference, &path("large.cal"), &solutions);
    let mut sm = session(&path("small.ms"), "ALMA", "XX YY", resources(2, 2 << 20));
    sm.observe(&scan("target", 0.0, 60.0)).expect("observe");
    let refused = sm
        .set_apply(&apply(&path("large.cal"), "linear"))
        .expect_err("a table beyond the budget");
    assert!(
        refused
            .message()
            .contains("loading a setapply calibration table"),
        "{refused}"
    );
}

/// The options `session` opens with.
fn options() -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: resources(2, 2 << 30),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

/// MAIN's TIME, row by row.
fn read_times(path: &Path) -> Vec<f64> {
    use casa_tables::{Table, TableOptions};
    use casa_types::{ScalarValue, Value};
    let table = Table::open(TableOptions::new(path)).expect("open MAIN");
    table
        .rows()
        .expect("the rows")
        .iter()
        .map(|row| match row.get("TIME") {
            Some(&Value::Scalar(ScalarValue::Float64(value))) => value,
            other => panic!("TIME: {other:?}"),
        })
        .collect()
}
