// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tsys output snapshots and their table writer hold their own admission.
use super::apply::{check, measured};
use super::{alone, charge_process_caches, configured, reserved_memory, resources, scan};
use casa_simulation::NoiseSpec;

const BUDGET: u64 = 1 << 30;

fn parameters(mode: &str, path: &std::path::Path) -> NoiseSpec {
    NoiseSpec {
        mode: mode.into(),
        table: path.to_string_lossy().into_owned(),
        water_vapour_mm: 1.,
        ..NoiseSpec::default()
    }
}

#[test]
fn tsys_output_snapshots_remain_charged_after_reset() {
    if !alone("noise_tables::tsys_output_snapshots_remain_charged_after_reset") {
        return;
    }
    let policy = resources(BUDGET);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    for (mode, integrations, channels) in [("tsys-manual", 8192, 4), ("tsys-atm", 16, 4096)] {
        let mut sm = configured(
            &root.path().join(format!("{mode}.ms")),
            &measures,
            BUDGET,
            channels,
        );
        sm.observe(&scan(integrations)).unwrap();
        let mut spec = parameters(mode, &root.path().join(format!("{mode}.cal")));
        let name = std::mem::take(&mut spec.table);
        sm.set_noise(&spec).unwrap();
        sm.reset();
        spec.table = name;
        let unset = reserved_memory();
        let call = measured(|| sm.set_noise(&spec).unwrap());
        check(mode, &call);
        sm.reset();
        assert!(
            reserved_memory() > unset + 64 * 1024,
            "reset keeps the pending table charged"
        );
        drop(sm);
        assert_eq!(
            reserved_memory(),
            unset,
            "dropping the table releases its charge"
        );
    }
}

#[test]
fn wide_tsys_output_writes_hold_their_admission() {
    if !alone("noise_tables::wide_tsys_output_writes_hold_their_admission") {
        return;
    }
    let policy = resources(BUDGET);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    let mut sm = configured(&root.path().join("wide.ms"), &measures, BUDGET, 4096);
    sm.observe(&scan(16)).unwrap();
    sm.set_noise(&parameters("tsys-atm", &root.path().join("wide.cal")))
        .unwrap();
    sm.reset();
    let call = measured(|| sm.close().unwrap());
    check("writing a wide Tf table", &call);
}

#[test]
fn later_tsys_snapshots_hold_their_admission_until_the_plan_is_dropped() {
    if !alone("noise_tables::later_tsys_snapshots_hold_their_admission_until_the_plan_is_dropped") {
        return;
    }
    let policy = resources(BUDGET);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    for mode in ["tsys-manual", "tsys-atm"] {
        let mut sm = configured(
            &root.path().join(format!("later-{mode}.ms")),
            &measures,
            BUDGET,
            32,
        );
        sm.observe(&scan(1000)).unwrap();
        // Warm the physical model before measuring the additional snapshot.
        let spec = NoiseSpec {
            mode: mode.into(),
            water_vapour_mm: 1.,
            ..NoiseSpec::default()
        };
        sm.set_noise(&spec).unwrap();
        sm.reset();
        let baseline = reserved_memory();
        sm.set_noise(&spec).unwrap();
        sm.observe(&super::scan_from(1000, 1)).unwrap();
        let call = measured(|| sm.corrupt().unwrap());
        check("original scales for later Tsys observations", &call);
        let kept = reserved_memory();
        assert!(
            kept >= baseline + (1000 * super::ANTENNAS * size_of::<f32>()) as u64,
            "the original per-antenna scale payload remains charged"
        );
        sm.reset();
        let reset = reserved_memory();
        assert!(
            reset <= kept,
            "reset may release the original physical model"
        );
        assert!(
            reset >= baseline + (1000 * super::ANTENNAS * size_of::<f32>()) as u64,
            "recorded corruption keeps its original scale payload"
        );
        drop(sm);
        assert_eq!(reserved_memory(), baseline);
    }
}
