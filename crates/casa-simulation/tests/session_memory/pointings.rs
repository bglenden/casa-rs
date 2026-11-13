// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stored POINTING payload and pending zero-interval rows stay admitted.
use super::polynomial_fields::measured;
use super::{
    ALLOWANCE_BYTES, EpochRef, LIVE, MEpoch, Ordering, PEAK, PredictSpec, REFERENCE_MJD, Simulator,
    SimulatorOptions, VoltagePatternSpec, alone, charge_process_caches, configured, resources,
    scan, write_components,
};
use casa_tables::{Table, TableOptions};
use casa_types::{ScalarValue, Value};

#[test]
fn stored_pointing_rows_are_admitted_before_beam_preparation() {
    if !alone("pointings::stored_pointing_rows_are_admitted_before_beam_preparation") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let list = directory.path().join("point.cl");
    write_components(&list, 1);
    let spec = PredictSpec {
        images: Vec::new(),
        component_lists: vec![list],
        incremental: false,
    };
    for count in [64usize, 20_000, 200_000] {
        let path = directory.path().join(format!("pointings-{count}.ms"));
        let mut sm = configured(&path, &measures, 512 << 20, 4);
        sm.observe(&scan(1)).unwrap();
        sm.close().unwrap();
        let mut table = Table::open(TableOptions::new(path.join("POINTING"))).unwrap();
        let mut row = table.row_accessor().row(0).unwrap().clone();
        *row.get_mut("INTERVAL").unwrap() = Value::Scalar(ScalarValue::Float64(0.));
        while table.row_count() < count {
            table.add_row(row.clone()).unwrap();
        }
        table
            .save(TableOptions::new(path.join("POINTING")))
            .unwrap();
        drop(table);
        let options = |budget| SimulatorOptions {
            measures: measures.clone(),
            resources: resources(budget),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        };
        let mut sm = Simulator::open_from_ms(&path, options(128 << 20)).unwrap();
        sm.set_vp(&VoltagePatternSpec::default()).unwrap();
        if count == 64 {
            sm.predict(&spec).unwrap();
        } else {
            drop(measured(&format!("{count} stored POINTING rows"), || {
                sm.predict(&spec).unwrap();
                sm
            }));
        }
        if count == 200_000 {
            let mut sm = Simulator::open_from_ms(&path, options(2 << 20)).unwrap();
            sm.set_vp(&VoltagePatternSpec::default()).unwrap();
            let baseline = LIVE.load(Ordering::Relaxed);
            PEAK.store(baseline, Ordering::Relaxed);
            let error = sm
                .predict(&spec)
                .expect_err("refuse the pending POINTING payload before decoding it");
            assert_eq!(
                error.admission_refused().unwrap().phase,
                "reading stored simulation POINTING"
            );
            assert!(PEAK.load(Ordering::Relaxed).saturating_sub(baseline) <= ALLOWANCE_BYTES);
        }
    }
}

#[test]
fn extending_stored_integrations_admits_growth_and_append_copies() {
    if !alone("pointings::extending_stored_integrations_admits_growth_and_append_copies") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for count in [64usize, 20_000, 200_000] {
        let path = directory.path().join(format!("integrations-{count}.ms"));
        let rows = super::polynomial_fields::chunked_ms(&path, count, &measures);
        let options = |budget| SimulatorOptions {
            measures: measures.clone(),
            resources: resources(budget),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        };
        if count == 200_000 {
            let before = std::fs::read(path.join("table.dat")).unwrap();
            let mut tight = Simulator::open_from_ms(&path, options(74 << 20)).unwrap();
            let error = tight
                .observe(&scan(1))
                .expect_err("admit the transient scan reallocation before append");
            assert_eq!(
                error.admission_refused().unwrap().phase,
                "extending stored simulation integrations"
            );
            drop(tight);
            assert_eq!(std::fs::read(path.join("table.dat")).unwrap(), before);
        }
        let mut sm = Simulator::open_from_ms(&path, options(256 << 20)).unwrap();
        sm.set_noise(&super::NoiseSpec::simple(0.1)).unwrap();
        sm.corrupt().unwrap();
        if count == 64 {
            sm.observe(&scan(1)).unwrap();
            sm.close().unwrap();
        } else {
            let sm = measured(&format!("{rows} integrations plus one observe"), || {
                sm.observe(&scan(1)).unwrap();
                sm
            });
            let (closed, _) = super::measured_close(sm, &path, true);
            println!(
                "{rows} integration append: heap {}, admitted {}",
                closed.heap, closed.admitted
            );
            assert!(closed.heap <= closed.admitted + ALLOWANCE_BYTES);
        }
    }
}
