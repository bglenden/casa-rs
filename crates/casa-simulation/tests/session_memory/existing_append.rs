// SPDX-License-Identifier: LGPL-3.0-or-later
//! Appending simulated rows keeps copying and writer scratch admitted.
use super::{
    ALLOWANCE_BYTES, EpochRef, MEpoch, REFERENCE_MJD, Simulator, SimulatorOptions, alone,
    charge_process_caches, configured, measured_close, resources, scan, scan_from,
};
const BUDGET: u64 = 512 << 20;

#[test]
fn existing_rows_are_copied_within_append_admission() {
    if !alone("existing_append::existing_rows_are_copied_within_append_admission") {
        return;
    }
    // As the existing patch laws: the process cache is charged separately,
    // so measure this close's buffers with that shared cache disabled.
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(BUDGET);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for (name, integrations, measure) in [
        ("warm", 60, false),
        ("short", 600, true),
        ("long", 6000, true),
    ] {
        let path = directory.path().join(format!("{name}.ms"));
        let mut initial = configured(&path, &measures, BUDGET, 32);
        initial.observe(&scan(integrations)).unwrap();
        initial.close().unwrap();
        let mut sm = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: measures.clone(),
                resources: resources(BUDGET),
                now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .unwrap();
        sm.set_times(10., false, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC))
            .unwrap();
        sm.observe(&scan_from(integrations, 1)).unwrap();
        if measure {
            let started = std::time::Instant::now();
            let (closed, _) = measured_close(sm, &path, false);
            println!(
                "append after {integrations} integrations: heap {}, admitted {}, wall {:.3}s",
                closed.heap,
                closed.admitted,
                started.elapsed().as_secs_f64()
            );
            assert!(
                closed.heap <= closed.admitted + ALLOWANCE_BYTES,
                "append copying and writer scratch exceed admission"
            );
        } else {
            sm.close().unwrap();
        }
    }
}
