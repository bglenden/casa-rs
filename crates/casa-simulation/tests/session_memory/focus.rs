// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected finite-distance geometry remains charged with its prediction.
use super::polynomial_fields::measured;
use super::{PredictSpec, alone, charge_process_caches, configured, resources, scan};
use casa_types::measures::direction::{DirectionRef, MDirection};

#[test]
fn finite_distance_positions_hold_their_charge() {
    if !alone("focus::finite_distance_positions_hold_their_charge") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let model = directory.path().join("model.image");
    super::write_model(&model, 4);
    for integrations in [2, 1_000, 10_000] {
        let mut sm = configured(&directory.path().join("focus.ms"), &measures, 512 << 20, 4);
        sm.observe(&scan(integrations)).unwrap();
        sm.set_field(
            "near",
            &MDirection::from_angles(
                super::CENTRE_RAD[0],
                super::CENTRE_RAD[1],
                DirectionRef::J2000,
            ),
            "",
            1e6,
        )
        .unwrap();
        let predict = || {
            sm.predict(&PredictSpec {
                images: vec![model.clone()],
                component_lists: Vec::new(),
                incremental: false,
            })
            .unwrap();
            sm
        };
        if integrations == 2 {
            drop(predict());
        } else {
            drop(measured(
                &format!("{} finite-distance rows", integrations * 21),
                predict,
            ));
        }
    }
}
