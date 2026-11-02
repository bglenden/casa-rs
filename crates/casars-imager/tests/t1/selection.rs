// SPDX-License-Identifier: LGPL-3.0-or-later

//! Field-name selection reaches production imaging (#54/#980).

use casa_imaging_application::product_law;
use serde_json::json;

use super::fixture::{Component, Observation, PHASE_CENTER_RAD, Setup};

#[test]
fn field_names_patterns_and_mixed_lists_image_the_same_rows_as_ids() {
    let mut setup = Setup::vla_q_band(2);
    setup.fields = vec![PHASE_CENTER_RAD; 2];
    let point = Component {
        offset_px: [12, 8],
        flux_jy: 1.0,
        fwhm_px: None,
        spectral_index: 0.0,
    };
    let observation = Observation::synthesise_setup(setup, &[("point", point)]);
    for (index, ids, names) in [(0, "0", "t1-0"), (1, "1", "t1-1*"), (2, "0,1", "0,t1-1")] {
        let reference = format!("fields-{index}-ids");
        let named = format!("fields-{index}-names");
        let controls = |field, selectdata| {
            json!({
                "field": field, "selectdata": selectdata, "niter": 0, "write_pb": true,
            })
        };
        let (_, a) = observation.image(&reference, controls(ids, true));
        let (_, b) = observation.image(&named, controls(names, false));
        assert_eq!(a.suffixes(), b.suffixes());
        for suffix in a.suffixes() {
            let nrms = product_law::image_nrms(
                &observation.scratch(&format!("{reference}{suffix}")),
                &observation.scratch(&format!("{named}{suffix}")),
            )
            .unwrap();
            assert!(nrms <= 1e-6, "{names} {suffix}: {nrms:e}");
        }
    }
    let error = observation
        .try_image("missing-field", json!({"field":"NO_SUCH_FIELD","niter":0}))
        .err()
        .expect("an unknown field is refused");
    assert!(error.contains("did not match any FIELD rows"), "{error}");
    assert!(!observation.scratch("missing-field.psf").exists());
}
