// SPDX-License-Identifier: LGPL-3.0-or-later
#![cfg(feature = "slow-tests")]

mod common;

use tempfile::TempDir;

use casacore_calibration::{CalibrationParameterFamily, summarize_table};

#[test]
fn summarize_casa_generated_gain_t_and_bandpass_tables() {
    let dir = TempDir::new().expect("tempdir");
    let (g_path, t_path, b_path) = match common::generate_casa_exemplars(dir.path()) {
        Ok(paths) => paths,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let g_summary = summarize_table(&g_path).expect("summarize G");
    let t_summary = summarize_table(&t_path).expect("summarize T");
    let b_summary = summarize_table(&b_path).expect("summarize B");

    for summary in [&g_summary, &t_summary, &b_summary] {
        assert_eq!(summary.table_type, "Calibration");
        assert_eq!(
            summary.parameter_family,
            CalibrationParameterFamily::Complex
        );
        assert!(summary.row_count > 0);
        assert!(
            summary.columns.iter().any(|column| column == "CPARAM"),
            "missing CPARAM in {}",
            summary.path.display()
        );
    }

    assert_eq!(g_summary.table_subtype, "G Jones");
    assert_eq!(t_summary.table_subtype, "T Jones");
    assert_eq!(b_summary.table_subtype, "B Jones");
}
