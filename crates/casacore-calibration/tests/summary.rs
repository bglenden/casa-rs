// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use tempfile::TempDir;

use casacore_calibration::{CalibrationParameterFamily, summarize_table};

#[test]
fn summarize_synthetic_complex_caltable() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_minimal_complex_caltable(&dir.path().join("synthetic.gcal"));
    let summary = summarize_table(&table_path).expect("summary");

    assert_eq!(summary.table_type, "Calibration");
    assert_eq!(summary.table_subtype, "G Jones");
    assert_eq!(
        summary.parameter_family,
        CalibrationParameterFamily::Complex
    );
    assert_eq!(summary.row_count, 1);
    assert_eq!(summary.field_ids, vec![0]);
    assert_eq!(summary.spectral_window_ids, vec![3]);
    assert_eq!(summary.antenna1_ids, vec![1]);
    assert_eq!(summary.antenna2_ids, vec![-1]);
    assert_eq!(summary.observation_ids, vec![0]);
    assert_eq!(
        summary.parameter_column.parameter_column.as_deref(),
        Some("CPARAM")
    );
    assert!(summary.supported_for_v1_apply());
    assert!(summary.subtables.iter().all(|subtable| subtable.exists));
}
