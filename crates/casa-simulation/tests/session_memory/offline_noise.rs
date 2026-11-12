// SPDX-License-Identifier: LGPL-3.0-or-later
//! Saved additive fields and their four-parameter tables retain their admission.
use super::apply::{check, measured};
use super::{alone, charge_process_caches, configured, reserved_memory, resources, scan};
use casa_simulation::NoiseSpec;
const BUDGET: u64 = 512 << 20;

#[test]
fn additive_fields_hold_their_admission_and_reset_releases_unsaved_fields() {
    if !alone(
        "offline_noise::additive_fields_hold_their_admission_and_reset_releases_unsaved_fields",
    ) {
        return;
    }
    let policy = resources(BUDGET);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    for named in [false, true] {
        let mut sm = configured(
            &root.path().join(format!("{named}.ms")),
            &measures,
            BUDGET,
            32,
        );
        sm.observe(&scan(1000)).unwrap();
        let before = reserved_memory();
        let call = measured(|| {
            sm.set_noise(&NoiseSpec {
                simple_noise_jy: 0.25,
                on_the_fly: false,
                table: if named {
                    root.path().join("noise.cal").to_string_lossy().into_owned()
                } else {
                    String::new()
                },
                ..NoiseSpec::default()
            })
            .unwrap()
        });
        check("a saved additive field", &call);
        sm.reset();
        if named {
            assert!(reserved_memory() > before + 1_000_000);
        } else {
            assert_eq!(reserved_memory(), before);
        }
        drop(sm);
        assert_eq!(reserved_memory(), before);
    }
}

#[test]
fn four_parameter_additive_tables_write_within_their_admission() {
    if !alone("offline_noise::four_parameter_additive_tables_write_within_their_admission") {
        return;
    }
    let policy = resources(BUDGET);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    let mut sm = configured(&root.path().join("wide.ms"), &measures, BUDGET, 2048);
    sm.observe(&scan(8)).unwrap();
    sm.set_noise(&NoiseSpec {
        simple_noise_jy: 0.25,
        on_the_fly: false,
        table: root.path().join("noise.cal").to_string_lossy().into_owned(),
        ..NoiseSpec::default()
    })
    .unwrap();
    sm.reset();
    let call = measured(|| sm.close().unwrap());
    check("writing a four-parameter A Noise table", &call);
}
