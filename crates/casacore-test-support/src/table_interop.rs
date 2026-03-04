// SPDX-License-Identifier: LGPL-3.0-or-later
use std::path::Path;

use casacore_tables::{EndianFormat, Table, TableError, TableOptions, TableSchema};
use casacore_types::{RecordValue, Value};

use crate::CppTableFixture;

/// Which storage manager to use for the fixture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagerKind {
    StManAipsIO,
    StandardStMan,
    IncrementalStMan,
    TiledColumnStMan,
    TiledShapeStMan,
    TiledCellStMan,
}

/// A complete table fixture: schema, rows, table keywords, and column keywords.
///
/// Column keywords are stored as `(column_name, RecordValue)` pairs.
/// `cpp_fixture` maps this to the C++ shim fixture enum for interop tests.
/// `tile_shape` is used for tiled storage managers.
#[derive(Debug, Clone)]
pub struct TableFixture {
    pub schema: TableSchema,
    pub rows: Vec<RecordValue>,
    pub table_keywords: RecordValue,
    pub column_keywords: Vec<(String, RecordValue)>,
    pub cpp_fixture: Option<CppTableFixture>,
    pub tile_shape: Option<Vec<usize>>,
}

/// Result of one cell in the 2x2 interop matrix.
#[derive(Debug)]
pub struct MatrixCellResult {
    pub label: &'static str,
    pub passed: bool,
    pub error: Option<String>,
}

/// Map a `ManagerKind` to `casacore_tables::DataManagerKind`.
fn to_dm_kind(manager: ManagerKind) -> casacore_tables::DataManagerKind {
    match manager {
        ManagerKind::StManAipsIO => casacore_tables::DataManagerKind::StManAipsIO,
        ManagerKind::StandardStMan => casacore_tables::DataManagerKind::StandardStMan,
        ManagerKind::IncrementalStMan => casacore_tables::DataManagerKind::IncrementalStMan,
        ManagerKind::TiledColumnStMan => casacore_tables::DataManagerKind::TiledColumnStMan,
        ManagerKind::TiledShapeStMan => casacore_tables::DataManagerKind::TiledShapeStMan,
        ManagerKind::TiledCellStMan => casacore_tables::DataManagerKind::TiledCellStMan,
    }
}

/// Build `TableOptions` for saving with the given manager, dir, and fixture tile shape.
fn save_opts(fixture: &TableFixture, manager: ManagerKind, dir: &Path) -> TableOptions {
    let mut opts = TableOptions::new(dir).with_data_manager(to_dm_kind(manager));
    if let Some(ref ts) = fixture.tile_shape {
        opts = opts.with_tile_shape(ts.clone());
    }
    opts
}

/// Build `TableOptions` for saving with explicit endian format.
fn save_opts_endian(
    fixture: &TableFixture,
    manager: ManagerKind,
    dir: &Path,
    endian: EndianFormat,
) -> TableOptions {
    let mut opts = TableOptions::new(dir)
        .with_data_manager(to_dm_kind(manager))
        .with_endian_format(endian);
    if let Some(ref ts) = fixture.tile_shape {
        opts = opts.with_tile_shape(ts.clone());
    }
    opts
}

/// Write a table from the fixture using Rust, then read it back and compare.
pub fn rust_write_rust_read(
    fixture: &TableFixture,
    manager: ManagerKind,
    dir: &Path,
) -> Result<(), String> {
    let table = build_table_from_fixture(fixture).map_err(|e| format!("build table: {e}"))?;
    table
        .save(save_opts(fixture, manager, dir))
        .map_err(|e| format!("save: {e}"))?;

    let reopened = Table::open(TableOptions::new(dir)).map_err(|e| format!("open: {e}"))?;
    compare_table_to_fixture(&reopened, fixture)
}

/// Read a table (written by C++ or Rust) and compare it to the expected fixture.
pub fn read_and_verify(
    fixture: &TableFixture,
    _manager: ManagerKind,
    dir: &Path,
) -> Result<(), String> {
    let table = Table::open(TableOptions::new(dir)).map_err(|e| format!("open: {e}"))?;
    compare_table_to_fixture(&table, fixture)
}

/// Run RR (Rust write, Rust read) test only.
pub fn run_table_cross_matrix(
    fixture: &TableFixture,
    manager: ManagerKind,
) -> Vec<MatrixCellResult> {
    vec![run_rr(fixture, manager)]
}

/// Run CC (C++ write, C++ verify) test only. Skipped if C++ unavailable.
pub fn run_cc_only(fixture: &TableFixture) -> Option<MatrixCellResult> {
    if !crate::cpp_backend_available() {
        return None;
    }
    fixture.cpp_fixture.map(run_cc)
}

/// Run the full 2x2 matrix including CR and RC.
/// Only call this once the Rust storage format is casacore-compatible.
pub fn run_full_cross_matrix(
    fixture: &TableFixture,
    manager: ManagerKind,
) -> Vec<MatrixCellResult> {
    let mut results = Vec::with_capacity(4);

    // RR: Rust write, Rust read
    eprintln!("[cross-matrix] starting RR");
    results.push(run_rr(fixture, manager));
    eprintln!(
        "[cross-matrix] RR done: passed={}",
        results.last().unwrap().passed
    );

    if crate::cpp_backend_available() {
        if let Some(cpp_fix) = fixture.cpp_fixture {
            eprintln!("[cross-matrix] starting CC");
            results.push(run_cc(cpp_fix));
            eprintln!(
                "[cross-matrix] CC done: passed={}",
                results.last().unwrap().passed
            );

            eprintln!("[cross-matrix] starting CR");
            results.push(run_cr(fixture, manager, cpp_fix));
            eprintln!(
                "[cross-matrix] CR done: passed={}",
                results.last().unwrap().passed
            );

            eprintln!("[cross-matrix] starting RC");
            results.push(run_rc(fixture, manager, cpp_fix));
            eprintln!(
                "[cross-matrix] RC done: passed={}",
                results.last().unwrap().passed
            );
        }
    }

    results
}

/// Run the endian-aware cross-matrix: RR-BE, RR-LE, and (when C++ is available)
/// RC-BE and RC-LE.
///
/// StManAipsIO always stores data in big-endian, but the table.dat endian
/// marker still varies. StandardStMan stores bucket data in the requested
/// endian. In both cases C++ casacore should read the table transparently.
pub fn run_endian_cross_matrix(
    fixture: &TableFixture,
    manager: ManagerKind,
) -> Vec<MatrixCellResult> {
    let mut results = Vec::with_capacity(4);

    results.push(run_rr_with_endian(
        fixture,
        manager,
        EndianFormat::BigEndian,
    ));
    results.push(run_rr_with_endian(
        fixture,
        manager,
        EndianFormat::LittleEndian,
    ));

    if crate::cpp_backend_available() {
        if let Some(cpp_fix) = fixture.cpp_fixture {
            results.push(run_rc_with_endian(
                fixture,
                manager,
                cpp_fix,
                EndianFormat::BigEndian,
            ));
            results.push(run_rc_with_endian(
                fixture,
                manager,
                cpp_fix,
                EndianFormat::LittleEndian,
            ));
        }
    }

    results
}

fn run_rr_with_endian(
    fixture: &TableFixture,
    manager: ManagerKind,
    endian: EndianFormat,
) -> MatrixCellResult {
    let label = match endian {
        EndianFormat::BigEndian => "RR-BE",
        EndianFormat::LittleEndian => "RR-LE",
        EndianFormat::LocalEndian => "RR-Local",
    };
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("rr_endian_table");

    let table = match build_table_from_fixture(fixture) {
        Ok(t) => t,
        Err(e) => {
            return MatrixCellResult {
                label: leak_label(label),
                passed: false,
                error: Some(format!("build table: {e}")),
            };
        }
    };

    if let Err(e) = table.save(save_opts_endian(fixture, manager, &table_path, endian)) {
        return MatrixCellResult {
            label: leak_label(label),
            passed: false,
            error: Some(format!("save: {e}")),
        };
    }

    let reopened = match Table::open(TableOptions::new(&table_path)) {
        Ok(t) => t,
        Err(e) => {
            return MatrixCellResult {
                label: leak_label(label),
                passed: false,
                error: Some(format!("open: {e}")),
            };
        }
    };

    match compare_table_to_fixture(&reopened, fixture) {
        Ok(()) => MatrixCellResult {
            label: leak_label(label),
            passed: true,
            error: None,
        },
        Err(msg) => MatrixCellResult {
            label: leak_label(label),
            passed: false,
            error: Some(msg),
        },
    }
}

fn run_rc_with_endian(
    fixture: &TableFixture,
    manager: ManagerKind,
    cpp_fix: CppTableFixture,
    endian: EndianFormat,
) -> MatrixCellResult {
    let label = match endian {
        EndianFormat::BigEndian => "RC-BE",
        EndianFormat::LittleEndian => "RC-LE",
        EndianFormat::LocalEndian => "RC-Local",
    };
    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("rc_endian_table");

    let table = match build_table_from_fixture(fixture) {
        Ok(t) => t,
        Err(e) => {
            return MatrixCellResult {
                label: leak_label(label),
                passed: false,
                error: Some(format!("build table: {e}")),
            };
        }
    };

    if let Err(e) = table.save(save_opts_endian(fixture, manager, &table_path, endian)) {
        return MatrixCellResult {
            label: leak_label(label),
            passed: false,
            error: Some(format!("Rust save: {e}")),
        };
    }

    match crate::cpp_table_verify(cpp_fix, &table_path) {
        Ok(()) => MatrixCellResult {
            label: leak_label(label),
            passed: true,
            error: None,
        },
        Err(msg) => MatrixCellResult {
            label: leak_label(label),
            passed: false,
            error: Some(format!("C++ verify failed: {msg}")),
        },
    }
}

/// Leak a short string to get a `&'static str` for `MatrixCellResult::label`.
/// These are only test labels, so the tiny leak is harmless.
fn leak_label(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

fn run_rr(fixture: &TableFixture, manager: ManagerKind) -> MatrixCellResult {
    let dir = tempfile::tempdir().expect("create temp dir for RR");
    let table_path = dir.path().join("rr_table");

    match rust_write_rust_read(fixture, manager, &table_path) {
        Ok(()) => MatrixCellResult {
            label: "RR",
            passed: true,
            error: None,
        },
        Err(msg) => MatrixCellResult {
            label: "RR",
            passed: false,
            error: Some(msg),
        },
    }
}

fn run_cc(cpp_fix: CppTableFixture) -> MatrixCellResult {
    let dir = tempfile::tempdir().expect("create temp dir for CC");
    let table_path = dir.path().join("cc_table");

    let write_result = crate::cpp_table_write(cpp_fix, &table_path);
    if let Err(msg) = write_result {
        return MatrixCellResult {
            label: "CC",
            passed: false,
            error: Some(format!("C++ write failed: {msg}")),
        };
    }

    match crate::cpp_table_verify(cpp_fix, &table_path) {
        Ok(()) => MatrixCellResult {
            label: "CC",
            passed: true,
            error: None,
        },
        Err(msg) => MatrixCellResult {
            label: "CC",
            passed: false,
            error: Some(format!("C++ verify failed: {msg}")),
        },
    }
}

fn run_cr(
    fixture: &TableFixture,
    manager: ManagerKind,
    cpp_fix: CppTableFixture,
) -> MatrixCellResult {
    let dir = tempfile::tempdir().expect("create temp dir for CR");
    let table_path = dir.path().join("cr_table");

    let write_result = crate::cpp_table_write(cpp_fix, &table_path);
    if let Err(msg) = write_result {
        return MatrixCellResult {
            label: "CR",
            passed: false,
            error: Some(format!("C++ write failed: {msg}")),
        };
    }

    match read_and_verify(fixture, manager, &table_path) {
        Ok(()) => MatrixCellResult {
            label: "CR",
            passed: true,
            error: None,
        },
        Err(msg) => MatrixCellResult {
            label: "CR",
            passed: false,
            error: Some(msg),
        },
    }
}

fn run_rc(
    fixture: &TableFixture,
    manager: ManagerKind,
    cpp_fix: CppTableFixture,
) -> MatrixCellResult {
    let dir = tempfile::tempdir().expect("create temp dir for RC");
    let table_path = dir.path().join("rc_table");

    let table = match build_table_from_fixture(fixture) {
        Ok(t) => t,
        Err(e) => {
            return MatrixCellResult {
                label: "RC",
                passed: false,
                error: Some(format!("build table: {e}")),
            };
        }
    };

    if let Err(e) = table.save(save_opts(fixture, manager, &table_path)) {
        return MatrixCellResult {
            label: "RC",
            passed: false,
            error: Some(format!("Rust save: {e}")),
        };
    }

    match crate::cpp_table_verify(cpp_fix, &table_path) {
        Ok(()) => MatrixCellResult {
            label: "RC",
            passed: true,
            error: None,
        },
        Err(msg) => MatrixCellResult {
            label: "RC",
            passed: false,
            error: Some(format!("C++ verify failed: {msg}")),
        },
    }
}

fn build_table_from_fixture(fixture: &TableFixture) -> Result<Table, TableError> {
    let mut table = Table::from_rows_with_schema(fixture.rows.clone(), fixture.schema.clone())?;

    // Copy table keywords
    for field in fixture.table_keywords.fields() {
        table.keywords_mut().push(casacore_types::RecordField::new(
            field.name.clone(),
            field.value.clone(),
        ));
    }

    // Copy column keywords
    for (col_name, kw) in &fixture.column_keywords {
        table.set_column_keywords(col_name.clone(), kw.clone());
    }

    Ok(table)
}

fn compare_table_to_fixture(table: &Table, fixture: &TableFixture) -> Result<(), String> {
    // Compare schema
    let schema = table.schema().ok_or("reopened table has no schema")?;

    if schema != &fixture.schema {
        return Err(format!(
            "schema mismatch:\n  expected: {:?}\n  found:    {:?}",
            fixture.schema, schema
        ));
    }

    // Compare row count
    if table.row_count() != fixture.rows.len() {
        return Err(format!(
            "row count mismatch: expected {}, found {}",
            fixture.rows.len(),
            table.row_count()
        ));
    }

    // Compare per-cell values
    for (row_idx, expected_row) in fixture.rows.iter().enumerate() {
        for col_schema in fixture.schema.columns() {
            let col_name = col_schema.name();
            let expected = expected_row.get(col_name);
            let actual = table.cell(row_idx, col_name);

            match (expected, actual) {
                (None, None) => {} // Both undefined — OK
                (Some(e), Some(a)) => {
                    if !values_equal(e, a) {
                        return Err(format!(
                            "cell mismatch at row={row_idx}, col=\"{col_name}\":\n  expected: {e:?}\n  found:    {a:?}"
                        ));
                    }
                }
                (Some(e), None) => {
                    return Err(format!(
                        "cell missing at row={row_idx}, col=\"{col_name}\": expected {e:?}"
                    ));
                }
                (None, Some(a)) => {
                    return Err(format!(
                        "unexpected cell at row={row_idx}, col=\"{col_name}\": found {a:?}"
                    ));
                }
            }
        }
    }

    // Compare table keywords
    for field in fixture.table_keywords.fields() {
        let actual = table.keywords().get(&field.name);
        match actual {
            Some(a) => {
                if !values_equal(&field.value, a) {
                    return Err(format!(
                        "table keyword \"{}\" mismatch:\n  expected: {:?}\n  found:    {:?}",
                        field.name, field.value, a
                    ));
                }
            }
            None => {
                return Err(format!("table keyword \"{}\" missing", field.name));
            }
        }
    }

    // Compare column keywords
    for (col_name, expected_kw) in &fixture.column_keywords {
        let actual_kw = table.column_keywords(col_name);
        match actual_kw {
            Some(actual) => {
                for field in expected_kw.fields() {
                    let actual_val = actual.get(&field.name);
                    match actual_val {
                        Some(a) => {
                            if !values_equal(&field.value, a) {
                                return Err(format!(
                                    "column keyword \"{col_name}\".\"{}\": expected {:?}, found {:?}",
                                    field.name, field.value, a
                                ));
                            }
                        }
                        None => {
                            return Err(format!(
                                "column keyword \"{col_name}\".\"{}\" missing",
                                field.name
                            ));
                        }
                    }
                }
            }
            None => {
                return Err(format!("column keywords missing for column \"{col_name}\""));
            }
        }
    }

    Ok(())
}

/// Compare two values with tolerance for floating-point.
fn values_equal(a: &Value, b: &Value) -> bool {
    a == b
}
