// SPDX-License-Identifier: LGPL-3.0-or-later
//! Captured row masks and wide referenced cells own their memory charges.
use super::polynomial_fields::measured;
use super::{
    ALLOWANCE_BYTES, EpochRef, LIVE, MEpoch, Ordering, PEAK, REFERENCE_MJD, Simulator,
    SimulatorOptions, alone, charge_process_caches, configured, resources, scan,
};

#[test]
fn captured_masks_keep_their_reservation() {
    if !alone("captured_selection::captured_masks_keep_their_reservation") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for count in [64usize, 20_000, 200_000] {
        let path = directory.path().join(format!("rows-{count}.ms"));
        let mut sm = configured(&path, &measures, 512 << 20, 4);
        sm.observe(&scan(count.div_ceil(21))).unwrap();
        sm.close().unwrap();
        let mut sm = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: measures.clone(),
                resources: resources(128 << 20),
                now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .unwrap();
        drop(measured(
            &format!("{count} captured selection rows"),
            || {
                sm.set_data(&[], &[], "FLAG_ROW == FALSE").unwrap();
                sm
            },
        ));
    }
}

#[test]
fn referenced_cells_are_admitted_before_decoding() {
    if !alone("captured_selection::referenced_cells_are_admitted_before_decoding") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for channels in [64usize, 20_000, 200_000] {
        let path = directory.path().join(format!("channels-{channels}.ms"));
        let mut sm = configured(&path, &measures, 8 << 30, channels);
        sm.observe(&scan(1)).unwrap();
        sm.close().unwrap();
        let options = |budget| SimulatorOptions {
            measures: measures.clone(),
            resources: resources(budget),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        };
        let mut sm = Simulator::open_from_ms(&path, options(512 << 20)).unwrap();
        drop(measured(&format!("{channels} selection channels"), || {
            sm.set_data(&[], &[], "real(DATA[1,1]) == 0").unwrap();
            sm
        }));
        let mut nested = Simulator::open_from_ms(&path, options(2 << 30)).unwrap();
        drop(measured(
            &format!("{channels} nested selection channels"),
            || {
                nested
                    .set_data(&[], &[], "any(DATA == DATA+(DATA+(DATA+(DATA+DATA))))")
                    .unwrap();
                nested
            },
        ));
        if channels == 200_000 {
            let before = std::fs::read(path.join("table.dat")).unwrap();
            let mut tight = Simulator::open_from_ms(&path, options(32 << 20)).unwrap();
            let baseline = LIVE.load(Ordering::Relaxed);
            PEAK.store(baseline, Ordering::Relaxed);
            let error = tight
                .set_data(&[], &[], "real(DATA[1,1]) == 0")
                .expect_err("refuse the referenced array before decoding it");
            assert_eq!(
                error.admission_refused().unwrap().phase,
                "capturing simulator TaQL rows"
            );
            assert!(PEAK.load(Ordering::Relaxed).saturating_sub(baseline) <= ALLOWANCE_BYTES);
            drop(tight);
            assert_eq!(std::fs::read(path.join("table.dat")).unwrap(), before);
        }
    }
}
