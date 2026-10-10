// SPDX-License-Identifier: LGPL-3.0-or-later
//! C++ casacore locks MeasurementSet tables that casa-rs wrote in place.
//!
//! A table's `table.lock` publishes sync data that C++ casacore trusts when
//! it locks a table: it takes the row count from it, refuses a lock whose
//! column count differs from the table's, and asserts one change counter per
//! data manager (`ColumnSet::resync`). Each test opens a table in C++ with
//! `UserLocking` and takes, releases and retakes an explicit read lock.

#![cfg(feature = "cpp-interop-tests")]

mod common;

use casa_test_support::table_sync::{persisted_table_shape, published_table_sync};
use casa_test_support::{TableOracle, casacore_oracle_available};

/// C++ locks, unlocks and relocks `table`, which reports the persisted row
/// and column counts.
#[track_caller]
fn assert_cpp_relocks(table: &std::path::Path) {
    let persisted = persisted_table_shape(table);
    let (rows, columns) =
        TableOracle::lock_read_relock(table).expect("C++ locks, unlocks and relocks");
    assert_eq!(rows, persisted.rows, "{}", table.display());
    assert_eq!(columns as usize, persisted.columns, "{}", table.display());
}

/// A MeasurementSet saved in place under the mixed storage policy, whose
/// MAIN has many data managers, publishes sync data C++ can lock against.
#[test]
fn cpp_relocks_a_measurement_set_saved_in_place() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    assert!(persisted_table_shape(&ms_path).data_managers > 1);
    assert!(published_table_sync(&ms_path).is_some());
    for table in [
        ms_path.clone(),
        ms_path.join("ANTENNA"),
        ms_path.join("SPECTRAL_WINDOW"),
    ] {
        assert_cpp_relocks(&table);
    }
}
