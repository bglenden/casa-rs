// SPDX-License-Identifier: LGPL-3.0-or-later

//! The MAIN rows imaging reads under a field, antenna, scan, time or
//! observation selection, against the rows CASA 6.7.6.14's MeasurementSet
//! selection names on the same MeasurementSet
//! (`resources/casa-compat/evidence/tclean-selection-casa-20261031.json`).
//! A selection CASA resolves to no rows (`DA41&DA56`: no such baseline) has
//! nothing to image, which imaging refuses.
//!
//! The rows are those of compile's partitions and the run's fields that
//! the run's per-time scan test keeps, gathered by the store law
//! ([`store_law::row_pointing`]); the run's own sample packing is checked
//! by T1's selection tests. Every selector reaches the request through the
//! catalog, as tclean's provider passes it.

use std::fs::File;
use std::path::{Path, PathBuf};

use casa_imaging_application::ImagingRequest;
use casa_imaging_application::store_law::row_pointing;
use flate2::read::GzDecoder;
use serde_json::{Value, json};

#[path = "common/imaging.rs"]
mod imaging;

const ORACLE: &str =
    include_str!("../../../resources/casa-compat/evidence/tclean-selection-casa-20261031.json");

/// The CASA cases this law covers: every recorded selection.
const CASES: [&str; 16] = [
    "all",
    "field_id",
    "field_name",
    "field_glob",
    "field_id_and_name",
    "field_both_names",
    "field_range",
    "antenna_id",
    "antenna_name",
    "baseline",
    "scan1",
    "scan3",
    "time_before",
    "time_after",
    "observation0",
    "combined",
];

/// Field names CASA's selection rejects, which imaging refuses.
const REFUSED: [&str; 2] = ["no_such_field", "mixed_missing_name"];

fn fixture(root: &Path) -> PathBuf {
    let archive = File::open(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../casa-ms/tests/fixtures/mssel_test_small.ms.tgz"),
    )
    .expect("the mssel fixture archive");
    tar::Archive::new(GzDecoder::new(archive))
        .unpack(root)
        .expect("unpack the mssel fixture");
    root.join("mssel_test_small.ms")
}

/// A dirty MFS request over `ms` with CASA's `selectors`.
fn request(ms: &Path, imagename: PathBuf, selectors: &Value) -> ImagingRequest {
    let mut values = json!({
        "vis": ms,
        "imagename": imagename,
        "imsize": 64,
        "cell": "1arcsec",
        "specmode": "mfs",
        "niter": 0,
        "datacolumn": "data",
    });
    for (name, value) in selectors.as_object().expect("the case's selectors") {
        values[name] = value.clone();
    }
    imaging::request(values)
}

#[test]
fn every_selector_reads_casas_main_rows() {
    let oracle: Value = serde_json::from_str(ORACLE).expect("oracle JSON");
    assert_eq!(oracle["casa_tasks"], "6.7.6.14");
    assert_eq!(oracle["casa_tools"], "6.7.6-14");
    let root = tempfile::tempdir().expect("test root");
    let ms = fixture(root.path());
    for name in CASES {
        let case = &oracle["cases"][name];
        let mut expected = case["rows"]
            .as_array()
            .expect("the case's rows")
            .iter()
            .map(|row| row.as_u64().expect("a MAIN row"))
            .collect::<Vec<_>>();
        expected.sort_unstable();
        let request = request(&ms, root.path().join(name), &case["selectors"]);
        let rows = row_pointing(&request, &imaging::context(request.resource_policy()));
        if expected.is_empty() {
            assert!(rows.is_err(), "{name}: an empty selection is refused");
            continue;
        }
        let rows = rows.unwrap_or_else(|error| panic!("{name}: {error}"));
        let actual = rows.into_keys().collect::<Vec<_>>();
        assert_eq!(
            actual.len(),
            expected.len(),
            "{name}: {} rows, CASA {}",
            actual.len(),
            expected.len()
        );
        assert_eq!(actual, expected, "{name}: the rows differ from CASA's");
    }
    for name in REFUSED {
        let case = &oracle["cases"][name];
        let casa = case["error"]["message"].as_str().expect("CASA's error");
        assert!(casa.contains("No match found for name"), "{name}: {casa}");
        let request = request(&ms, root.path().join(name), &case["selectors"]);
        let error = row_pointing(&request, &imaging::context(request.resource_policy()))
            .err()
            .unwrap_or_else(|| panic!("{name}: a selection CASA refuses is refused"))
            .to_string();
        assert!(
            error.contains("did not match any FIELD rows"),
            "{name}: {error}"
        );
    }
}
