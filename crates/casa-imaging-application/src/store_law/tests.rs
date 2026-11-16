// SPDX-License-Identifier: LGPL-3.0-or-later
//! The store law where the other store reverses a window (#1014): CASA's
//! ALMA test MeasurementSet (`mssel_test_small.ms`, its window 1's 64
//! channels descending at −31.25 MHz) and XRADIO's conversion of it, which
//! writes that window ascending (casa-ms's fixtures).

use std::path::{Path, PathBuf};

use serde_json::json;

use super::compare_stores;
use crate::{Cancel, HostResources, RunContext};

/// Unpack casa-ms fixture archive `archive` into `root`; the path of
/// `name` in it.
fn unpack(root: &Path, archive: &str, name: &str) -> PathBuf {
    let archive = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../casa-ms/tests/fixtures")
        .join(archive);
    tar::Archive::new(flate2::read::GzDecoder::new(
        std::fs::File::open(archive).expect("fixture"),
    ))
    .unpack(root)
    .expect("unpack");
    root.join(name)
}

/// Every MAIN row of the MeasurementSet matches its sample in XRADIO's
/// conversion, and every row's geometry agrees to the law's tolerance,
/// each channel's frequency included: window 1's channel `c` is compared
/// with XRADIO's channel `63 - c`, the same channel, not the one at the
/// same position, which lies elsewhere in the band.
#[test]
fn a_window_another_store_reverses_holds_the_store_law() {
    let root = tempfile::tempdir().expect("root");
    let measurement_set = unpack(
        root.path(),
        "mssel_test_small.ms.tgz",
        "mssel_test_small.ms",
    );
    let converted = unpack(
        root.path(),
        "ms_trial/mssel_test_small.ps.zarr.tgz",
        "mssel_test_small.ps.zarr",
    );
    let request = crate::request::tests::request(json!({
        "vis": measurement_set,
        "imagename": root.path().join("law"),
        "imsize": [64, 64],
        "cell": "2arcsec",
        "backend": "cpu",
    }));
    let context = RunContext {
        host: HostResources::detect().expect("host resources"),
        policy: request.resource_policy(),
        cancel: Cancel::default(),
        summary: None,
    };
    let comparison = compare_stores(&request, &context, &converted).expect("both stores' geometry");
    assert_eq!(comparison.rows, 1058, "every MAIN row: {comparison:?}");
    assert_eq!(comparison.unmatched, 0, "{comparison:?}");
    assert!(comparison.holds(), "{comparison:?}");
    // 529 rows of 64 channels and 529 of one.
    assert_eq!(
        comparison.compared.get("frequency"),
        Some(&(529 * 64 + 529))
    );
}
