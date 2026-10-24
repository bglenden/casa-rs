// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setapply` then `sm.corrupt` (#841 Phase 3): a calibration table
//! corrupts the model as the simulated term of its type does, its
//! solutions found and interpolated in time as `casa-calibration` applies
//! them. Agreement with CASA is the corruption scenarios'
//! (`tools/simulation/sm_corrupt_scenarios.json`).

mod predict_support;

use std::path::{Path, PathBuf};

use casa_calibration::{ApplyInterpolationMode, CalibrationTableLookup};
use casa_simulation::{
    ApplySpec, CasaOutcome, GainSpec, LeakageSpec, PredictSpec, Simulator, SimulatorOptions,
    SimulatorResult,
};
use casa_types::Complex32;
use casa_types::measures::{EpochRef, MEpoch};
use predict_support::{
    CENTRE_RAD, REFERENCE_MJD, Row, bits, data, read_main, resources, scan, session,
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
