// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product staging admission laws.

use super::*;

/// The product owner keeps its staging admission through publication, even
/// when a single product plane is larger than the fixed process allowance.
#[test]
fn a_large_product_plane_stays_within_its_staging_admission() {
    if !alone("staging::a_large_product_plane_stays_within_its_staging_admission") {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let measurement_set = observation(root.path(), 2);
    warm(&measurement_set, root.path());
    let dirty = request(
        &measurement_set,
        &root.path().join("large-product"),
        1024,
        json!({
            "specmode": "cube", "outframe": "TOPO", "spw": "0",
            "channel_start": 0, "channel_count": 2,
            "start": "0", "width": "1", "niter": 0,
        }),
    );
    let measured = measure(&dirty, 2, AMPLE);
    assert!(
        measured.outcome.is_ok(),
        "large cube: {:?}",
        measured.outcome.err()
    );
    assert!(
        admission_breach("large product staging", &measured).is_none(),
        "{}",
        admission_breach("large product staging", &measured).unwrap_or_default()
    );
}
