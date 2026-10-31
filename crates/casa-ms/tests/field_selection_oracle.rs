// SPDX-License-Identifier: LGPL-3.0-or-later

//! Physical MAIN-row identity against the pinned CASA field-selection oracle.

use std::{fs::File, path::Path};

use casa_ms::{
    MeasurementSet, MsSelection, MsSelectionIoBudget, ResolvedMsSelectionRow, SubtableId,
};
use casa_types::{ScalarValue, Value as Cell};
use flate2::read::GzDecoder;
use serde_json::Value;

#[test]
fn field_names_patterns_and_mixed_ids_select_casas_physical_rows_in_order() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for record in [
        include_str!("../../../resources/casa-compat/evidence/tclean-selection-casa-20261031.json"),
        include_str!(
            "../../../resources/casa-compat/evidence/tclean-selection-metadata-casa-20261031.json"
        ),
    ] {
        let oracle: Value = serde_json::from_str(record).unwrap();
        assert_eq!(oracle["casa_tasks"], "6.7.6.14");
        assert_eq!(oracle["casa_tools"], "6.7.6-14");
        let temp = tempfile::tempdir().unwrap();
        let archive = File::open(root.join("tests/fixtures/mssel_test_small.ms.tgz")).unwrap();
        tar::Archive::new(GzDecoder::new(archive))
            .unpack(temp.path())
            .unwrap();
        let mut ms = MeasurementSet::open(temp.path().join("mssel_test_small.ms")).unwrap();
        for change in oracle["field_metadata"].as_array().unwrap() {
            let value = match &change["value"] {
                Value::String(text) => ScalarValue::String(text.clone()),
                Value::Bool(value) => ScalarValue::Bool(*value),
                value => panic!("unexpected FIELD metadata {value}"),
            };
            ms.subtable_mut(SubtableId::Field)
                .unwrap()
                .row_accessor_mut()
                .set_cell(
                    change["row"].as_u64().unwrap() as usize,
                    change["column"].as_str().unwrap(),
                    Cell::Scalar(value),
                )
                .unwrap();
        }
        let io = MsSelectionIoBudget {
            available_bytes: 1 << 20,
            maximum_live_blocks: 1,
            requested_bytes_per_row: std::mem::size_of::<ResolvedMsSelectionRow>(),
            storage_alignment_rows: None,
        };
        for (name, case) in oracle["cases"].as_object().unwrap() {
            // The other row selectors are a later imaging slice.
            if case["selectors"]
                .as_object()
                .unwrap()
                .keys()
                .any(|key| key != "field")
            {
                continue;
            }
            let selector = case["selectors"]["field"].as_str().unwrap_or("");
            let ids = ms.resolve_field_selector(selector);
            if case.get("error").is_some() {
                assert!(ids.is_err(), "{name}: CASA refuses {selector:?}");
                continue;
            }
            let expected = case["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row.as_u64().unwrap() as usize)
                .collect::<Vec<_>>();
            let request = MsSelection::new().field(&ids.unwrap());
            let resolved = ms.resolve_selection(&request, io).unwrap();
            assert_eq!(
                resolved.row_indices().collect::<Vec<_>>(),
                expected,
                "{name}: metadata resolver"
            );
            let request = MsSelection {
                field: Some(selector.to_string()),
                ..MsSelection::new()
            };
            let resolved = ms.resolve_selection(&request, io).unwrap();
            assert_eq!(
                resolved.row_indices().collect::<Vec<_>>(),
                expected,
                "{name}: shared task parser"
            );
        }
    }
}
