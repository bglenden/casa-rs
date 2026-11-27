// SPDX-License-Identifier: LGPL-3.0-or-later
//! Multi-feed MAIN geometry and duplicated POINTING rows remain streamed.
use super::apply::{check, measured};
use super::{BUDGET_32, alone, charge_process_caches, configured, resources, scan};
use casa_simulation::NoiseSpec;

#[test]
fn multi_feed_geometry_and_pointing_stay_within_admission() {
    if !alone("feeds::multi_feed_geometry_and_pointing_stay_within_admission") {
        return;
    }
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    for measured_run in [false, true] {
        let path = root.path().join(format!("{measured_run}.ms"));
        let mut sm = configured(&path, &measures, BUDGET_32, 32);
        sm.set_feed(
            "list",
            &[0., 0.0002, -0.0001],
            &[0., -0.0001, 0.0003],
            &["X".into(), "X".into(), "X".into()],
        )
        .unwrap();
        sm.observe(&scan(200)).unwrap();
        sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
        sm.corrupt().unwrap();
        if measured_run {
            check(
                "streaming three feeds and their pointing rows",
                &measured(|| sm.close().unwrap()),
            );
        } else {
            sm.close().unwrap();
        }
    }
}
