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
use casa_ms::{MeasurementSet, SubtableId};
use casa_types::{ScalarValue, Value as TableValue};
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

fn physical_rows(case: &Value) -> Vec<u64> {
    let mut rows: Vec<_> = case["rows"]
        .as_array()
        .expect("the case's physical rows")
        .iter()
        .map(|row| row.as_u64().expect("a MAIN row"))
        .collect();
    rows.sort_unstable();
    rows
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
        let expected = physical_rows(case);
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

#[test]
fn quoted_fields_read_casas_physical_rows() {
    let root = tempfile::tempdir().expect("test root");
    let vis = fixture(root.path());
    let mut metadata = [
        ("G55 East, ridge", "phase calibrator, G55", false),
        ("G55  West", "G55 East, ridge", false),
    ];
    rewrite_fields(&vis, metadata);
    let ms = MeasurementSet::open(&vis).expect("reopen the fixture");

    // Stock CASA selects the same physical rows as these existing oracle
    // cases on the input whose FIELD metadata was renamed above.
    let oracle: Value = serde_json::from_str(ORACLE).expect("oracle JSON");
    for (name, field, baseline, ids) in [
        ("comma", r#""G55 East, ridge""#, "field_id", &[0][..]),
        ("spaces", r#""G55  West""#, "field_glob", &[1][..]),
        ("code", r#""phase calibrator, G55""#, "field_id", &[0][..]),
        ("braces", r#""G55{ East*,  West}""#, "all", &[0, 1][..]),
        ("mixed", r#"1,"G55 East, ridge""#, "all", &[0, 1][..]),
    ] {
        let request = request(&vis, root.path().join(name), &json!({"field": field}));
        let rows = row_pointing(&request, &imaging::context(request.resource_policy()))
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        let expected = physical_rows(&oracle["cases"][baseline]);
        assert_eq!(rows.into_keys().collect::<Vec<_>>(), expected, "{name}");
        assert_eq!(ms.resolve_field_selector(field).unwrap(), ids, "{name}");
    }

    for (field, reason) in [
        (r#""G55 West""#, "did not match any field names or codes"),
        (r#""G55 East, ridge"#, "unterminated quoted field selector"),
        (r#""G55 East, ridge","#, "empty term in field selector"),
    ] {
        let request = request(&vis, root.path().join("refused"), &json!({"field": field}));
        let error = row_pointing(&request, &imaging::context(request.resource_policy()))
            .expect_err("CASA refuses this field expression")
            .to_string();
        assert!(error.contains(reason), "{field:?}: {error}");
    }

    // With the matching NAME flagged, CASA instead finds field 1 by CODE.
    drop(ms);
    metadata[0].2 = true;
    rewrite_fields(&vis, metadata);
    let fallback = request(
        &vis,
        root.path().join("fallback"),
        &json!({"field": r#""G55 East, ridge""#}),
    );
    let rows = row_pointing(&fallback, &imaging::context(fallback.resource_policy())).unwrap();
    assert_eq!(
        rows.into_keys().collect::<Vec<_>>(),
        physical_rows(&oracle["cases"]["field_glob"]),
        "flagged names do not shadow an unflagged code"
    );
    let flagged_code = request(
        &vis,
        root.path().join("flagged-code"),
        &json!({"field": r#""phase calibrator, G55""#}),
    );
    assert!(
        row_pointing(
            &flagged_code,
            &imaging::context(flagged_code.resource_policy())
        )
        .is_err(),
        "a flagged code is refused"
    );
}

fn rewrite_fields(vis: &Path, metadata: [(&str, &str, bool); 2]) {
    let mut ms = MeasurementSet::open(vis).expect("open the fixture");
    let fields = ms.subtable_mut(SubtableId::Field).expect("FIELD");
    for (row, (name, code, flagged)) in metadata.into_iter().enumerate() {
        for (column, value) in [
            ("NAME", ScalarValue::String(name.to_owned())),
            ("CODE", ScalarValue::String(code.to_owned())),
            ("FLAG_ROW", ScalarValue::Bool(flagged)),
        ] {
            fields
                .cell_accessor_mut(row, column)
                .unwrap()
                .set(TableValue::Scalar(value))
                .unwrap();
        }
    }
    ms.save().expect("write private FIELD metadata");
}

#[test]
fn bare_field_codes_and_numeric_aliases_read_casas_physical_rows() {
    let root = tempfile::tempdir().expect("test root");
    let vis = fixture(root.path());
    let ordinary = [
        ("target  East", "  phase  ", false),
        ("phase", "  scan CAL  ", false),
    ];
    let flagged_name = [ordinary[0], ("phase", "  scan CAL  ", true)];
    let numeric = [("42", "unused", false), ("0", "37", false)];
    let numeric_conflict = [numeric[0], ("0", "42", false)];
    let numeric_padded = [("0042", "unused", false), numeric[1]];
    let numeric_duplicate = [numeric[0], ("42", "37", false)];
    let numeric_flagged = [("42", "unused", true), numeric_duplicate[1]];
    let oracle: Value = serde_json::from_str(ORACLE).expect("oracle JSON");
    let groups = [
        (
            ordinary,
            &[
                ("target  East", Some("field_id")),
                ("target East", None),
                ("phase", Some("field_glob")),
                ("scan CAL", Some("field_glob")),
                ("*scan*", Some("field_glob")),
                ("scan*", None),
            ][..],
        ),
        (
            flagged_name,
            &[("phase", Some("field_id")), ("scan CAL", None)][..],
        ),
        (
            numeric,
            &[
                ("0", Some("field_id")),
                (r#""0""#, Some("field_glob")),
                ("42", Some("field_id")),
                ("0042", Some("field_id")),
                ("37", Some("field_glob")),
                ("41~43", Some("field_id")),
                ("1,42", Some("all")),
                ("0,42", Some("field_id")),
            ][..],
        ),
        (numeric_conflict, &[("42", Some("field_id"))][..]),
        (numeric_padded, &[("42", None)][..]),
        (numeric_duplicate, &[("42", Some("field_id"))][..]),
        (
            numeric_flagged,
            &[("42", Some("field_glob")), ("0", Some("field_id"))][..],
        ),
    ];
    for (group, (metadata, cases)) in groups.into_iter().enumerate() {
        rewrite_fields(&vis, metadata);
        for &(field, expected) in cases {
            let selection = request(&vis, root.path().join("rows"), &json!({"field": field}));
            let result = row_pointing(&selection, &imaging::context(selection.resource_policy()));
            if let Some(case) = expected {
                let rows =
                    result.unwrap_or_else(|error| panic!("group {group}, {field:?}: {error}"));
                assert_eq!(
                    rows.into_keys().collect::<Vec<_>>(),
                    physical_rows(&oracle["cases"][case]),
                    "group {group}, {field:?}"
                );
            } else {
                assert!(result.is_err(), "CASA refuses group {group}, {field:?}");
            }
        }
    }
}
