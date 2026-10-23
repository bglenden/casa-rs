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

/// A save that fails while writing MAIN publishes nothing for a subtable it
/// never reached: C++ still finds ANTENNA's persisted rows, not the row that
/// was pending in memory.
#[test]
fn cpp_reads_an_unwritten_subtable_as_persisted_after_a_failed_save() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let antenna = ms_path.join("ANTENNA");
    let rows_before = persisted_table_shape(&antenna).rows;
    let mut measurement_set = casa_ms::MeasurementSet::open(&ms_path).expect("open MeasurementSet");
    common::stage_an_antenna_row(&mut measurement_set);
    common::make_main_unwritable(&ms_path);
    assert!(
        measurement_set.save().is_err(),
        "the save of MAIN must fail"
    );
    drop(measurement_set);
    assert_eq!(persisted_table_shape(&antenna).rows, rows_before);
    assert_cpp_relocks(&antenna);
}

/// After a column installation refused before it wrote anything, and after
/// one that installed a column before the next failed, C++ locks MAIN with
/// the columns actually on disk: no column that never reached disk is
/// published.
#[test]
fn cpp_relocks_main_after_failed_column_installations() {
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let columns = persisted_table_shape(&ms_path).columns;

    assert!(
        common::install_cloned_columns(&ms_path, &[("NEVER_INSTALLED", "MISSING_SOURCE")]).is_err()
    );
    assert_eq!(persisted_table_shape(&ms_path).columns, columns);
    assert_cpp_relocks(&ms_path);

    assert!(
        common::install_cloned_columns(&ms_path, &[("CLONE_A", "DATA"), ("CLONE_B", "TIME")])
            .is_err()
    );
    assert_eq!(persisted_table_shape(&ms_path).columns, columns + 1);
    assert_cpp_relocks(&ms_path);
}

/// What CASA 6.7.6's simulator leaves after a mosaic predict (#880): a
/// `TiledShapeStMan` table whose storage manager and sync data hold four
/// rows while `table.dat` gives one. Made from two casa-rs saves of the
/// same schema: the four-row table publishes its rows, then takes the
/// one-row table's `table.dat`.
fn stale_tiled_table(root: &std::path::Path) -> std::path::PathBuf {
    use casa_tables::{ColumnSchema, DataManagerKind, Table, TableOptions, TableSchema};
    let save = |path: &std::path::Path, rows: usize| {
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "DATA",
            casa_types::PrimitiveType::Complex32,
            Some(2),
        )])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        for row in 0..rows {
            table.add_row(stale_row(row as f32)).expect("append a row");
        }
        table
            .save(TableOptions::new(path).with_data_manager(DataManagerKind::TiledShapeStMan))
            .expect("save the tiled table");
    };
    let table = root.join("stale.tab");
    let one_row = root.join("one_row.tab");
    save(&table, 4);
    save(&one_row, 1);
    let mut lock = casa_tables::TableWriteLock::acquire(&table, 1).expect("write lock");
    lock.record_write();
    lock.release().expect("publish four rows");
    std::fs::copy(one_row.join("table.dat"), table.join("table.dat"))
        .expect("take the one-row table.dat");
    assert_eq!(persisted_table_shape(&table).rows, 1);
    assert_eq!(published_table_sync(&table).expect("sync").shape.rows, 4);
    table
}

fn stale_row(value: f32) -> casa_types::RecordValue {
    let cell = ndarray::ArrayD::from_shape_vec(
        vec![2, 2],
        vec![casa_types::Complex32::new(value, 0.0); 4],
    )
    .expect("2x2 cell");
    casa_types::RecordValue::new(vec![casa_types::RecordField::new(
        "DATA",
        casa_types::Value::Array(casa_types::ArrayValue::Complex32(cell)),
    )])
}

/// C++ opens and locks `table` with four rows.
#[track_caller]
fn assert_cpp_relocks_four_rows(table: &std::path::Path, case: &str) {
    let (rows, _) = TableOracle::lock_read_relock(table)
        .unwrap_or_else(|error| panic!("{case}: C++ refuses the table: {error}"));
    assert_eq!(rows, 4, "{case}");
}

/// C++ opens the stale table as CASA does, and still opens it with all its
/// rows after casa-rs writes a row in place, holds the write lock and writes
/// nothing, refuses a partial save of a handle holding an unsaved row, or
/// saves only the keywords of such a handle. Each once left a row count in
/// `table.dat` or the sync data that the storage manager does not hold.
#[test]
fn cpp_relocks_a_stale_table_after_casa_rs_writes_it() {
    use casa_tables::{Table, TableOptions, TableWriteLock};
    if !casacore_oracle_available() {
        eprintln!("skipping: C++ casacore not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let fresh = |case: &str| {
        let root = dir.path().join(case);
        std::fs::create_dir(&root).expect("case directory");
        stale_tiled_table(&root)
    };

    let table = fresh("as-left");
    assert_cpp_relocks_four_rows(&table, "as CASA left it");

    let table = fresh("row-written");
    let mut handle = Table::open(TableOptions::new(&table)).expect("open");
    {
        let mut prepared = handle
            .row_accessor_mut()
            .prepare(&["DATA"])
            .expect("prepare DATA");
        prepared.seek(2).expect("row 2");
        prepared
            .set_value_at(0, stale_row(20.0).get("DATA").expect("cell").clone())
            .expect("set row 2");
    }
    let mut lock = TableWriteLock::acquire(&table, 1).expect("write lock");
    lock.record_write();
    handle
        .prepare_write()
        .save_selected_rows(&["DATA"], &[2])
        .expect("write row 2 in place");
    lock.release().expect("publish");
    assert_cpp_relocks_four_rows(&table, "a row written in place");

    let table = fresh("nothing-written");
    let mut lock = TableWriteLock::acquire(&table, 1).expect("write lock");
    lock.record_write();
    lock.release().expect("publish");
    assert_cpp_relocks_four_rows(&table, "a write lock that wrote nothing");

    let table = fresh("refused");
    let mut handle = Table::open(TableOptions::new(&table)).expect("open");
    handle
        .add_row(stale_row(9.0))
        .expect("append an unsaved row");
    let mut lock = TableWriteLock::acquire(&table, 1).expect("write lock");
    lock.record_write();
    assert!(
        handle
            .prepare_write()
            .save_selected_rows(&["DATA"], &[2])
            .is_err()
    );
    lock.release().expect("publish");
    assert_cpp_relocks_four_rows(&table, "a refused partial save");

    let table = fresh("keywords");
    let mut handle = Table::open(TableOptions::new(&table)).expect("open");
    handle
        .add_row(stale_row(9.0))
        .expect("append an unsaved row");
    let mut lock = TableWriteLock::acquire(&table, 1).expect("write lock");
    lock.record_write();
    handle
        .save_metadata_only(TableOptions::new(&table))
        .expect("save the keywords");
    lock.release().expect("publish");
    assert_cpp_relocks_four_rows(&table, "a keyword save");
}
