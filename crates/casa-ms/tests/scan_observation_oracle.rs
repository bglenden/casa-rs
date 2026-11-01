// SPDX-License-Identifier: LGPL-3.0-or-later
//! Exact physical MAIN rows against the pinned CASA scan/observation oracle.

use casa_ms::subtables::SubTable;
use casa_ms::{MeasurementSet, MsSelectionIoBudget, SelectedObservationRow};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::{fs::File, path::Path};

#[test]
fn numeric_scan_and_observation_select_casas_physical_rows_in_order() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let oracle: Value = serde_json::from_str(include_str!(
        "../../../resources/casa-compat/evidence/tclean-selection-casa-20261031.json"
    ))
    .unwrap();
    assert_eq!(oracle["casa_tasks"], "6.7.6.14");
    assert_eq!(oracle["casa_tools"], "6.7.6-14");
    let temp = tempfile::tempdir().unwrap();
    let archive = File::open(root.join("tests/fixtures/mssel_test_small.ms.tgz")).unwrap();
    tar::Archive::new(GzDecoder::new(archive))
        .unpack(temp.path())
        .unwrap();
    let ms = MeasurementSet::open(temp.path().join("mssel_test_small.ms")).unwrap();
    let ddids = (0..ms.data_description().unwrap().table().row_count())
        .map(|id| id as i32)
        .collect::<Vec<_>>();
    for (name, count) in [("scan1", 322), ("scan3", 736), ("observation0", 1058)] {
        let case = &oracle["cases"][name];
        let selected = ms
            .selected_observation_row_selection(&ddids, None, None, None)
            .unwrap()
            .with_numeric_row_selectors(
                case["selectors"]["scan"].as_str(),
                case["selectors"]["observation"].as_str(),
            )
            .unwrap();
        let expected = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row.as_u64().unwrap() as usize)
            .collect::<Vec<_>>();
        assert_eq!(expected.len(), count);
        for rows_per_block in [1, 17, 128] {
            let mut actual = Vec::new();
            ms.visit_selected_observation_rows(
                &selected,
                MsSelectionIoBudget {
                    available_bytes: rows_per_block * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                    maximum_live_blocks: 1,
                    requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                    storage_alignment_rows: Some(rows_per_block),
                },
                |row| actual.push(row.physical_row()),
            )
            .unwrap();
            assert_eq!(actual, expected, "{name}: {rows_per_block}-row blocks");
        }
    }
}
