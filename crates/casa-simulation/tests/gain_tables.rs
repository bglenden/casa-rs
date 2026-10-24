// SPDX-License-Identifier: LGPL-3.0-or-later
//! `sm.setgain` and `sm.setleakage` with `table=`, and after `openfromms`
//! (#841 Phase 3): the calibration table `close` writes holds the term
//! `corrupt` applied, and a term set on an opened MeasurementSet corrupts
//! it as the session that observed would have. Agreement with CASA is the
//! corruption scenarios' (`tools/simulation/sm_corrupt_scenarios.json`).

mod predict_support;

use std::path::{Path, PathBuf};

use casa_simulation::{
    GainSpec, LeakageSpec, PredictSpec, Simulator, SimulatorOptions, SimulatorResult,
};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use predict_support::{
    ANTENNAS, CENTRE_RAD, REFERENCE_MJD, bits, data, read_main, resources, scan, session,
    write_point_list,
};

fn gain(table: &Path) -> GainSpec {
    GainSpec {
        mode: "fbm".to_string(),
        table: table.to_string_lossy().into_owned(),
        amplitude: vec![0.05],
    }
}

fn leakage(table: &Path) -> LeakageSpec {
    LeakageSpec {
        table: table.to_string_lossy().into_owned(),
        amplitude: vec![0.05],
        offset: vec![0.02],
    }
}

/// A session of `stokes` observing `target` twice, with a 1 Jy point at its
/// centre predicted.
fn observed(path: &Path, stokes: &str, point: &Path) -> Simulator {
    let mut sm = session(path, "ALMA", stokes, resources(2, 2 << 30));
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

/// A calibration table's MAIN rows as (TIME, FIELD_ID, SPECTRAL_WINDOW_ID,
/// ANTENNA1, CPARAM).
fn read_table(path: &Path) -> Vec<(f64, i32, i32, i32, Vec<Complex32>)> {
    let table = Table::open(TableOptions::new(path)).expect("open the calibration table");
    table
        .rows()
        .expect("the rows")
        .iter()
        .map(|row| {
            let int = |name: &str| match row.get(name) {
                Some(&Value::Scalar(ScalarValue::Int32(value))) => value,
                other => panic!("{name}: {other:?}"),
            };
            let time = match row.get("TIME") {
                Some(&Value::Scalar(ScalarValue::Float64(value))) => value,
                other => panic!("TIME: {other:?}"),
            };
            let cparam = match row.get("CPARAM") {
                Some(Value::Array(ArrayValue::Complex32(values))) => {
                    assert_eq!(values.shape(), [2, 1]);
                    values.iter().copied().collect()
                }
                other => panic!("CPARAM: {other:?}"),
            };
            assert_eq!(int("ANTENNA2"), -1);
            assert_eq!(int("SCAN_NUMBER"), -1);
            (
                time,
                int("FIELD_ID"),
                int("SPECTRAL_WINDOW_ID"),
                int("ANTENNA1"),
                cparam,
            )
        })
        .collect()
}

/// `setgain(table=…)` writes CASA's `G Jones` table at `close`: a row per
/// integration and antenna, by time and antenna, holding the gains
/// `corrupt` applied (on a 1 Jy point, XX is `g_p[0]·conj(g_q[0])`), which
/// casa-calibration reads as a complex gain table. A table already there
/// is replaced, as CASA replaces it.
#[test]
fn the_gain_table_holds_the_gains_corrupt_applied() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let table = directory.path().join("g.cal");
    std::fs::create_dir(&table).expect("a stale table");
    std::fs::write(table.join("table.dat"), b"stale").expect("its table.dat");
    let path = directory.path().join("gains.ms");
    let mut sm = observed(&path, "XX YY", &point);
    sm.set_gain(&gain(&table)).expect("setgain");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");

    let solutions = read_table(&table);
    let rows = read_main(&path).0;
    let integrations = rows.len() / (ANTENNAS * (ANTENNAS + 1) / 2);
    assert_eq!(solutions.len(), integrations * ANTENNAS);
    assert!(
        solutions
            .windows(2)
            .all(|pair| (pair[0].0, pair[0].3) < (pair[1].0, pair[1].3)),
        "by time and antenna"
    );
    let times = read_times(&path);
    for (row, time) in rows.iter().zip(&times) {
        let at = |antenna: u32| {
            &solutions
                .iter()
                .find(|solution| solution.0 == *time && solution.3 == antenna as i32)
                .expect("the antenna's solution at the row's time")
                .4
        };
        let [p, q] = row.antennas;
        let expected = at(p)[0] * at(q)[0].conj();
        assert!(
            (row.data[0] - expected).norm() < 1e-6,
            "{} against {expected}",
            row.data[0]
        );
    }
    let summary = casa_calibration::summarize_table(&table).expect("casa-calibration reads it");
    assert_eq!(summary.table_subtype, "G Jones");
    assert!(summary.supported_for_v1_apply(), "{:?}", summary.issues);
}

/// `setleakage(table=…)` writes a `D Jones` table of one solution per
/// window and antenna, at the window's first TIME.
#[test]
fn the_leakage_table_has_a_solution_per_window_and_antenna() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let table = directory.path().join("d.cal");
    let path = directory.path().join("leak.ms");
    let mut sm = observed(&path, "XX XY YX YY", &point);
    sm.set_leakage(&leakage(&table)).expect("setleakage");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");
    let solutions = read_table(&table);
    assert_eq!(solutions.len(), ANTENNAS);
    let first = read_times(&path).into_iter().fold(f64::INFINITY, f64::min);
    assert!(solutions.iter().all(|solution| solution.0 == first));
    let summary = casa_calibration::summarize_table(&table).expect("casa-calibration reads it");
    assert_eq!(summary.table_subtype, "D Jones");
}

/// A path that is something other than a table is not replaced: `close`
/// raises, having written the MeasurementSet.
#[test]
fn a_table_path_that_is_not_a_table_is_refused() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let table = directory.path().join("notes");
    std::fs::create_dir(&table).expect("a directory");
    let mut sm = observed(&directory.path().join("x.ms"), "XX YY", &point);
    sm.set_gain(&gain(&table)).expect("setgain");
    sm.corrupt().expect("corrupt");
    let error = sm.close().expect_err("not a table");
    assert!(error.message().contains("not a table"), "{error}");
    assert!(table.exists() && !table.join("table.dat").exists());
}

/// The options `session` opens with.
fn options() -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: resources(2, 2 << 30),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

/// Gains and leakage set after `openfromms` corrupt as in the session that
/// observed, bit for bit, and write the same tables: the opened
/// MeasurementSet's integrations are the solution chunks, and each row
/// takes its solution at its TIME.
#[test]
fn gains_and_leakage_after_openfromms_corrupt_as_before() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let point = point(directory.path());
    let path = |name: &str| directory.path().join(name);
    let steps = |sm: &mut Simulator, tables: &str| -> SimulatorResult<()> {
        sm.set_leakage(&leakage(&path(&format!("{tables}.dcal"))))?;
        sm.set_gain(&gain(&path(&format!("{tables}.gcal"))))?;
        sm.corrupt()
    };
    let mut sm = observed(&path("one.ms"), "XX XY YX YY", &point);
    steps(&mut sm, "one").expect("the steps");
    sm.close().expect("close");
    observed(&path("two.ms"), "XX XY YX YY", &point)
        .close()
        .expect("close");
    let mut sm = Simulator::open_from_ms(path("two.ms"), options()).expect("openfromms");
    steps(&mut sm, "two").expect("the steps after openfromms");
    sm.close().expect("close the opened MeasurementSet");

    let (one, two) = (read_main(&path("one.ms")).0, read_main(&path("two.ms")).0);
    assert!(one.iter().any(|row| row.data[1].norm() > 0.0), "leaked");
    assert_eq!(bits(&data(&one)), bits(&data(&two)));
    for table in ["dcal", "gcal"] {
        let cparams = |name: &str| -> Vec<Vec<Complex32>> {
            read_table(&path(&format!("{name}.{table}")))
                .into_iter()
                .map(|solution| solution.4)
                .collect()
        };
        assert_eq!(cparams("one"), cparams("two"), "{table}");
    }
}

/// MAIN's TIME, row by row.
fn read_times(path: &Path) -> Vec<f64> {
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
