// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-facet W-kernel admission (#1063).

use serde_json::json;

use super::{AMPLE, alone, check, observation, request, warm};

/// Every facet retains its own W-kernel set until the passes finish. The
/// allocator sees the kernels, chart state, and model refresh together.
#[test]
fn w_facet_kernels_stay_within_their_admission() {
    if !alone("w_facets::w_facet_kernels_stay_within_their_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 4);
    warm(&measurement_set, root.path());
    let mut breaches = Vec::new();
    check(
        &mut breaches,
        "16 W-facet kernel sets",
        &request(
            &measurement_set,
            &root.path().join("w-facets"),
            1024,
            json!({"gridder": "wproject", "wprojplanes": 32, "facets": 4,
                   "deconvolver": "hogbom", "niter": 20}),
        ),
        2,
        AMPLE,
    );
    assert!(breaches.is_empty(), "{breaches:#?}");
}
