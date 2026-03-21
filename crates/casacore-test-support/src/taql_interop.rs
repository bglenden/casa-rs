// SPDX-License-Identifier: LGPL-3.0-or-later
//! TaQL cross-matrix interop infrastructure.
//!
//! Provides helpers to run a TaQL query on both Rust and C++ casacore
//! implementations and compare the results across a 2x2 matrix:
//! RR (Rust write, Rust query), CC (C++ write, C++ query),
//! CR (C++ write, Rust query), RC (Rust write, C++ query).

#[cfg(has_casacore_cpp)]
use std::path::Path;

#[cfg(has_casacore_cpp)]
use casacore_tables::TableOptions;
use casacore_tables::taql::format as taql_fmt;
use casacore_tables::{ColumnSchema, Table, TableSchema};
use casacore_types::*;

/// String-grid result from a TaQL query.
#[derive(Debug, Clone)]
pub struct TaqlQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Result of one cell in the TaQL 2x2 cross-matrix.
#[derive(Debug)]
pub struct TaqlCrossResult {
    pub label: &'static str,
    pub passed: bool,
    pub error: Option<String>,
}

/// Which fixture to use for a cross-matrix test.
#[derive(Debug, Clone, Copy)]
pub enum TaqlFixtureKind {
    Simple,
    Array,
    VarShape,
}

/// Result from the C++ query side, including timing.
#[derive(Debug)]
pub struct CppTaqlQueryResult {
    pub grid: TaqlQueryResult,
    pub nrow: u64,
    pub ncol: u64,
    pub elapsed_ns: u64,
}

// ── Rust fixture builders ──

/// Build the 50-row simple fixture (matches C++ `write_simple_fixture_impl`).
///
/// Schema: id(Int32), name(String), ra(Float64), dec(Float64), flux(Float64),
///         category(String), flag(Bool), bigid(Int64), vis(Complex64/DComplex)
pub fn build_simple_fixture() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("ra", PrimitiveType::Float64),
        ColumnSchema::scalar("dec", PrimitiveType::Float64),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::scalar("category", PrimitiveType::String),
        ColumnSchema::scalar("flag", PrimitiveType::Bool),
        ColumnSchema::scalar("bigid", PrimitiveType::Int64),
        ColumnSchema::scalar("vis", PrimitiveType::Complex64),
    ])
    .unwrap();

    let categories = ["star", "galaxy", "pulsar", "quasar", "nebula"];
    let mut table = Table::with_schema(schema);
    for i in 0..50 {
        let cat = categories[i % categories.len()];
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(i as i32))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String(format!("SRC_{i:03}"))),
                ),
                RecordField::new("ra", Value::Scalar(ScalarValue::Float64(i as f64 * 7.2))),
                RecordField::new(
                    "dec",
                    Value::Scalar(ScalarValue::Float64(-45.0 + i as f64 * 1.8)),
                ),
                RecordField::new(
                    "flux",
                    Value::Scalar(ScalarValue::Float64(0.1 + i as f64 * 0.5)),
                ),
                RecordField::new(
                    "category",
                    Value::Scalar(ScalarValue::String(cat.to_string())),
                ),
                RecordField::new("flag", Value::Scalar(ScalarValue::Bool((i % 3) != 0))),
                RecordField::new(
                    "bigid",
                    Value::Scalar(ScalarValue::Int64(i as i64 * 1_000_000)),
                ),
                RecordField::new(
                    "vis",
                    Value::Scalar(ScalarValue::Complex64(casacore_types::Complex64::new(
                        i as f64 * 0.1,
                        i as f64 * -0.2,
                    ))),
                ),
            ]))
            .unwrap();
    }
    table
}

/// Build the 10-row array fixture (matches C++ `write_array_fixture_impl`).
pub fn build_array_fixture() -> Table {
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    let schema = TableSchema::new(vec![
        ColumnSchema::array_fixed("idata", PrimitiveType::Int32, vec![2, 3]),
        ColumnSchema::array_fixed("fdata", PrimitiveType::Float64, vec![3, 2]),
    ])
    .unwrap();

    let mut table = Table::with_schema(schema);
    for row in 0..10u32 {
        // idata: [2,3], values = row*6 + flat_idx (column-major storage order)
        // Use Fortran layout (.f()) so sequential fill matches C++ casacore's
        // `iarr.data()[j] = row*6 + j` which fills raw Fortran storage.
        let idata: Vec<i32> = (0..6).map(|j| (row as i32) * 6 + j).collect();
        let iarr = ArrayD::from_shape_vec(IxDyn(&[2, 3]).f(), idata).unwrap();

        // fdata: [3,2], values = row*6.0 + idx + 0.5
        let fdata: Vec<f64> = (0..6)
            .map(|j| (row as f64) * 6.0 + j as f64 + 0.5)
            .collect();
        let farr = ArrayD::from_shape_vec(IxDyn(&[3, 2]).f(), fdata).unwrap();

        table
            .add_row(RecordValue::new(vec![
                RecordField::new("idata", Value::Array(ArrayValue::Int32(iarr))),
                RecordField::new("fdata", Value::Array(ArrayValue::Float64(farr))),
            ]))
            .unwrap();
    }
    table
}

// ── Rust query execution ──

/// Execute a TaQL statement via `execute_taql()`, returning `Ok(())` on success.
///
/// Use this for queries that produce computed columns (GROUP BY, expressions in
/// SELECT, etc.) where `table.query()` would fail because the result is not a
/// simple `RefTable`.
pub fn rust_taql_exec_ok(table: &mut Table, query: &str) -> Result<(), String> {
    table
        .execute_taql(query)
        .map_err(|e| format!("Rust exec error: {e}"))?;
    Ok(())
}

/// Execute a TaQL SELECT on a Rust table and return the result as a string grid.
///
/// The query should omit `FROM` — it operates on the table directly.
/// Handles both simple SELECTs (RefTable view) and computed/aggregate
/// SELECTs (materialized table) via `Table::query_result()`.
pub fn rust_taql_query(table: &mut Table, query: &str) -> Result<TaqlQueryResult, String> {
    let qr = table
        .query_result(query)
        .map_err(|e| format!("Rust query error: {e}"))?;
    let columns = qr.column_names();
    let mut rows = Vec::with_capacity(qr.row_count());
    for i in 0..qr.row_count() {
        let row_rec = qr.row(i).map_err(|e| format!("row {i}: {e}"))?;
        let mut cells = Vec::with_capacity(columns.len());
        for col in &columns {
            let val = row_rec
                .get(col)
                .ok_or_else(|| format!("missing column {col} in row {i}"))?;
            cells.push(taql_fmt::format_value(val));
        }
        rows.push(cells);
    }
    Ok(TaqlQueryResult { columns, rows })
}

// ── C++ fixture writing (FFI wrappers) ──

/// Write the simple fixture using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_write_simple_fixture(path: &Path) -> Result<(), String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe { crate::cpp_taql_write_simple_fixture(c_path.as_ptr(), &mut error) };
    check_cpp_result(rc, error)
}

/// Write the array fixture using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_write_array_fixture(path: &Path) -> Result<(), String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe { crate::cpp_taql_write_array_fixture(c_path.as_ptr(), &mut error) };
    check_cpp_result(rc, error)
}

/// Write the variable-shape array fixture using C++ casacore.
#[cfg(has_casacore_cpp)]
pub fn cpp_write_varshape_fixture(path: &Path) -> Result<(), String> {
    let c_path = std::ffi::CString::new(path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString: {e}"))?;
    let mut error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe { crate::cpp_taql_write_varshape_fixture(c_path.as_ptr(), &mut error) };
    check_cpp_result(rc, error)
}

// ── C++ query execution ──

/// Execute a TaQL query via C++ casacore.
///
/// The query should use `$1` to reference the table at `table_path`.
#[cfg(has_casacore_cpp)]
pub fn cpp_taql_query(table_path: &Path, query: &str) -> Result<CppTaqlQueryResult, String> {
    use std::ffi::{CStr, CString};

    let c_path = CString::new(table_path.to_str().ok_or("non-utf8 path")?)
        .map_err(|e| format!("CString path: {e}"))?;
    let c_query = CString::new(query).map_err(|e| format!("CString query: {e}"))?;

    let mut out_result: *mut std::ffi::c_char = std::ptr::null_mut();
    let mut out_nrow: u64 = 0;
    let mut out_ncol: u64 = 0;
    let mut out_elapsed_ns: u64 = 0;
    let mut out_error: *mut std::ffi::c_char = std::ptr::null_mut();

    let rc = unsafe {
        crate::cpp_taql_query(
            c_path.as_ptr(),
            c_query.as_ptr(),
            &mut out_result,
            &mut out_nrow,
            &mut out_ncol,
            &mut out_elapsed_ns,
            &mut out_error,
        )
    };

    if rc != 0 {
        let msg = if out_error.is_null() {
            "unknown C++ TaQL error".to_string()
        } else {
            let s = unsafe { CStr::from_ptr(out_error).to_string_lossy().into_owned() };
            unsafe { crate::cpp_table_free_error(out_error) };
            s
        };
        return Err(msg);
    }

    let result_str = if out_result.is_null() {
        String::new()
    } else {
        let s = unsafe { CStr::from_ptr(out_result).to_string_lossy().into_owned() };
        unsafe { crate::cpp_taql_free_result(out_result) };
        s
    };

    let grid = parse_tsv_grid(&result_str);

    Ok(CppTaqlQueryResult {
        grid,
        nrow: out_nrow,
        ncol: out_ncol,
        elapsed_ns: out_elapsed_ns,
    })
}

// ── Result comparison ──

/// Compare two query result grids with optional floating-point tolerance.
///
/// Numeric strings (parseable as f64) are compared within `tolerance`.
/// Other strings are compared exactly.
///
/// Column names must match unless one side uses auto-generated names (like
/// C++ casacore's `Col_1`, `Col_2`, etc.). In that case, only the column
/// count is checked.
pub fn results_match(
    a: &TaqlQueryResult,
    b: &TaqlQueryResult,
    tolerance: f64,
) -> Result<(), String> {
    // Check column count
    if a.columns.len() != b.columns.len() {
        return Err(format!(
            "column count mismatch: {:?} vs {:?}",
            a.columns, b.columns
        ));
    }
    // Check column names — but allow mismatches when one side has auto-names
    if a.columns != b.columns
        && !has_auto_column_names(&a.columns)
        && !has_auto_column_names(&b.columns)
    {
        return Err(format!(
            "column mismatch: {:?} vs {:?}",
            a.columns, b.columns
        ));
    }
    if a.rows.len() != b.rows.len() {
        return Err(format!(
            "row count mismatch: {} vs {}",
            a.rows.len(),
            b.rows.len()
        ));
    }
    for (ri, (row_a, row_b)) in a.rows.iter().zip(&b.rows).enumerate() {
        if row_a.len() != row_b.len() {
            return Err(format!(
                "row {ri}: column count mismatch: {} vs {}",
                row_a.len(),
                row_b.len()
            ));
        }
        for (ci, (va, vb)) in row_a.iter().zip(row_b).enumerate() {
            if !values_close(va, vb, tolerance) {
                return Err(format!(
                    "row {ri}, col {} ({}): '{}' vs '{}'",
                    ci,
                    a.columns.get(ci).map(|s| s.as_str()).unwrap_or("?"),
                    va,
                    vb,
                ));
            }
        }
    }
    Ok(())
}

/// Returns true if any column name looks like a C++ auto-generated name.
fn has_auto_column_names(columns: &[String]) -> bool {
    columns
        .iter()
        .any(|c| c.starts_with("Col_") && c[4..].parse::<u32>().is_ok())
}

/// Run the full 2x2 cross-matrix for a single query.
///
/// `rust_query` omits FROM (operates on self).
/// `cpp_query` uses `$1` to reference the table.
#[allow(unused_variables)]
pub fn run_taql_cross_matrix(
    fixture: TaqlFixtureKind,
    rust_query: &str,
    cpp_query: &str,
    tolerance: f64,
) -> Vec<TaqlCrossResult> {
    let mut results = Vec::with_capacity(4);

    // RR: Rust write + Rust query
    let rr = run_rr(fixture, rust_query);
    eprintln!("[taql-cross] RR: passed={}", rr.passed);
    results.push(rr);

    #[cfg(has_casacore_cpp)]
    {
        let tmp = tempfile::tempdir().expect("tempdir");

        // CC: C++ write + C++ query
        let cc = run_cc(fixture, cpp_query, tmp.path());
        eprintln!("[taql-cross] CC: passed={}", cc.passed);
        results.push(cc);

        // CR: C++ write + Rust query
        let cr = run_cr(fixture, rust_query, tmp.path());
        eprintln!("[taql-cross] CR: passed={}", cr.passed);
        results.push(cr);

        // RC: Rust write + C++ query
        let rc = run_rc(fixture, cpp_query, tmp.path(), tolerance);
        eprintln!("[taql-cross] RC: passed={}", rc.passed);
        results.push(rc);
    }

    results
}

// ── Internal helpers ──

fn to_cross_result(label: &'static str, result: Result<(), String>) -> TaqlCrossResult {
    TaqlCrossResult {
        label,
        passed: result.is_ok(),
        error: result.err(),
    }
}

fn run_rr(fixture: TaqlFixtureKind, query: &str) -> TaqlCrossResult {
    let mut table = build_fixture(fixture);
    let result = rust_taql_query(&mut table, query).map(|_| ());
    to_cross_result("RR", result)
}

#[cfg(has_casacore_cpp)]
fn run_cc(fixture: TaqlFixtureKind, query: &str, base: &Path) -> TaqlCrossResult {
    let result = run_cc_inner(fixture, query, base);
    to_cross_result("CC", result)
}

#[cfg(has_casacore_cpp)]
fn run_cc_inner(fixture: TaqlFixtureKind, query: &str, base: &Path) -> Result<(), String> {
    let path = base.join("cc_table");
    write_cpp_fixture(fixture, &path)?;
    cpp_taql_query(&path, query)?;
    Ok(())
}

#[cfg(has_casacore_cpp)]
fn run_cr(fixture: TaqlFixtureKind, rust_query: &str, base: &Path) -> TaqlCrossResult {
    let result = run_cr_inner(fixture, rust_query, base);
    to_cross_result("CR", result)
}

#[cfg(has_casacore_cpp)]
fn run_cr_inner(fixture: TaqlFixtureKind, rust_query: &str, base: &Path) -> Result<(), String> {
    let path = base.join("cr_table");
    write_cpp_fixture(fixture, &path)?;
    let mut table = Table::open(TableOptions::new(&path)).map_err(|e| format!("Rust open: {e}"))?;
    rust_taql_query(&mut table, rust_query)?;
    Ok(())
}

#[cfg(has_casacore_cpp)]
fn run_rc(
    fixture: TaqlFixtureKind,
    cpp_query: &str,
    base: &Path,
    _tolerance: f64,
) -> TaqlCrossResult {
    let result = run_rc_inner(fixture, cpp_query, base);
    to_cross_result("RC", result)
}

#[cfg(has_casacore_cpp)]
fn run_rc_inner(fixture: TaqlFixtureKind, cpp_query: &str, base: &Path) -> Result<(), String> {
    let path = base.join("rc_table");
    let table = build_fixture(fixture);
    table
        .save(TableOptions::new(&path))
        .map_err(|e| format!("Rust save: {e}"))?;
    cpp_taql_query(&path, cpp_query)?;
    Ok(())
}

/// Run a cross-matrix comparing Rust vs C++ results with tolerance.
#[allow(unused_variables)]
pub fn run_taql_cross_matrix_compare(
    fixture: TaqlFixtureKind,
    rust_query: &str,
    cpp_query: &str,
    tolerance: f64,
) -> Vec<TaqlCrossResult> {
    let mut results = Vec::with_capacity(4);

    // RR: Rust write + Rust query (baseline)
    let rr_result = {
        let mut table = build_fixture(fixture);
        rust_taql_query(&mut table, rust_query)
    };
    match &rr_result {
        Ok(_) => results.push(TaqlCrossResult {
            label: "RR",
            passed: true,
            error: None,
        }),
        Err(e) => {
            results.push(TaqlCrossResult {
                label: "RR",
                passed: false,
                error: Some(e.clone()),
            });
            return results;
        }
    }
    let rr_grid = rr_result.unwrap();

    #[cfg(has_casacore_cpp)]
    {
        let tmp = tempfile::tempdir().expect("tempdir");

        // CC: C++ write + C++ query
        let cc_path = tmp.path().join("cc_table");
        let cc_result = compare_cc(&rr_grid, fixture, cpp_query, &cc_path, tolerance);
        results.push(to_cross_result("CC", cc_result));

        // CR: C++ write + Rust query
        let cr_path = tmp.path().join("cr_table");
        let cr_result = compare_cr(&rr_grid, fixture, rust_query, &cr_path, tolerance);
        results.push(to_cross_result("CR", cr_result));

        // RC: Rust write + C++ query
        let rc_path = tmp.path().join("rc_table");
        let rc_result = compare_rc(&rr_grid, fixture, cpp_query, &rc_path, tolerance);
        results.push(to_cross_result("RC", rc_result));
    }

    results
}

/// Run the 2x2 cross-matrix using `execute_taql()` on the Rust side.
///
/// Checks all 4 cells execute without error, but does NOT compare result grids.
/// Use this for queries that produce computed columns (GROUP BY, expressions in
/// SELECT, array indexing in SELECT, etc.) where `table.query()` returns
/// something other than a `RefTable`.
#[allow(unused_variables)]
pub fn run_taql_cross_matrix_exec(
    fixture: TaqlFixtureKind,
    rust_query: &str,
    cpp_query: &str,
) -> Vec<TaqlCrossResult> {
    let mut results = Vec::with_capacity(4);

    // RR: Rust write + Rust exec
    let rr = {
        let mut table = build_fixture(fixture);
        let result = rust_taql_exec_ok(&mut table, rust_query);
        to_cross_result("RR", result)
    };
    eprintln!("[taql-cross-exec] RR: passed={}", rr.passed);
    results.push(rr);

    #[cfg(has_casacore_cpp)]
    {
        let tmp = tempfile::tempdir().expect("tempdir");

        // CC: C++ write + C++ query
        let cc = run_cc(fixture, cpp_query, tmp.path());
        eprintln!("[taql-cross-exec] CC: passed={}", cc.passed);
        results.push(cc);

        // CR: C++ write + Rust exec
        let cr = {
            let cr_path = tmp.path().join("cr_exec_table");
            let result = (|| -> Result<(), String> {
                write_cpp_fixture(fixture, &cr_path)?;
                let mut table = Table::open(TableOptions::new(&cr_path))
                    .map_err(|e| format!("Rust open: {e}"))?;
                rust_taql_exec_ok(&mut table, rust_query)
            })();
            to_cross_result("CR", result)
        };
        eprintln!("[taql-cross-exec] CR: passed={}", cr.passed);
        results.push(cr);

        // RC: Rust write + C++ query
        let rc = run_rc(fixture, cpp_query, tmp.path(), 0.0);
        eprintln!("[taql-cross-exec] RC: passed={}", rc.passed);
        results.push(rc);
    }

    results
}

#[cfg(has_casacore_cpp)]
fn compare_cc(
    rr_grid: &TaqlQueryResult,
    fixture: TaqlFixtureKind,
    cpp_query: &str,
    path: &Path,
    tolerance: f64,
) -> Result<(), String> {
    write_cpp_fixture(fixture, path)?;
    let cpp_res = cpp_taql_query(path, cpp_query)?;
    results_match(rr_grid, &cpp_res.grid, tolerance)
}

#[cfg(has_casacore_cpp)]
fn compare_cr(
    rr_grid: &TaqlQueryResult,
    fixture: TaqlFixtureKind,
    rust_query: &str,
    path: &Path,
    tolerance: f64,
) -> Result<(), String> {
    write_cpp_fixture(fixture, path)?;
    let mut table = Table::open(TableOptions::new(path)).map_err(|e| format!("Rust open: {e}"))?;
    let cr_grid = rust_taql_query(&mut table, rust_query)?;
    results_match(rr_grid, &cr_grid, tolerance)
}

#[cfg(has_casacore_cpp)]
fn compare_rc(
    rr_grid: &TaqlQueryResult,
    fixture: TaqlFixtureKind,
    cpp_query: &str,
    path: &Path,
    tolerance: f64,
) -> Result<(), String> {
    let table = build_fixture(fixture);
    table
        .save(TableOptions::new(path))
        .map_err(|e| format!("Rust save: {e}"))?;
    let cpp_res = cpp_taql_query(path, cpp_query)?;
    results_match(rr_grid, &cpp_res.grid, tolerance)
}

fn build_fixture(kind: TaqlFixtureKind) -> Table {
    match kind {
        TaqlFixtureKind::Simple => build_simple_fixture(),
        TaqlFixtureKind::Array => build_array_fixture(),
        TaqlFixtureKind::VarShape => build_varshape_fixture(),
    }
}

#[cfg(has_casacore_cpp)]
fn write_cpp_fixture(kind: TaqlFixtureKind, path: &Path) -> Result<(), String> {
    match kind {
        TaqlFixtureKind::Simple => cpp_write_simple_fixture(path),
        TaqlFixtureKind::Array => cpp_write_array_fixture(path),
        TaqlFixtureKind::VarShape => cpp_write_varshape_fixture(path),
    }
}

#[cfg(has_casacore_cpp)]
fn check_cpp_result(rc: i32, error: *mut std::ffi::c_char) -> Result<(), String> {
    if rc == 0 {
        return Ok(());
    }
    let msg = if error.is_null() {
        "unknown C++ error".to_string()
    } else {
        let s = unsafe {
            std::ffi::CStr::from_ptr(error)
                .to_string_lossy()
                .into_owned()
        };
        unsafe { crate::cpp_table_free_error(error) };
        s
    };
    Err(msg)
}

/// Parse a tab-separated grid string into a `TaqlQueryResult`.
#[cfg(has_casacore_cpp)]
fn parse_tsv_grid(tsv: &str) -> TaqlQueryResult {
    let mut lines = tsv.lines();
    let columns: Vec<String> = match lines.next() {
        Some(header) => header.split('\t').map(|s| s.to_string()).collect(),
        None => {
            return TaqlQueryResult {
                columns: vec![],
                rows: vec![],
            };
        }
    };

    let rows: Vec<Vec<String>> = lines
        .filter(|line| !line.is_empty())
        .map(|line| line.split('\t').map(|s| s.to_string()).collect())
        .collect();

    TaqlQueryResult { columns, rows }
}

/// Check if two value strings are "close enough".
///
/// If both parse as f64, compare within tolerance.
/// Otherwise compare as exact strings.
fn values_close(a: &str, b: &str, tolerance: f64) -> bool {
    if a == b {
        return true;
    }
    // Try numeric comparison
    if let (Ok(fa), Ok(fb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        return (fa - fb).abs() <= tolerance;
    }
    false
}

/// Build a large simple fixture with `n` rows for benchmarking.
pub fn build_simple_fixture_n(n: usize) -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("ra", PrimitiveType::Float64),
        ColumnSchema::scalar("dec", PrimitiveType::Float64),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::scalar("category", PrimitiveType::String),
        ColumnSchema::scalar("flag", PrimitiveType::Bool),
        ColumnSchema::scalar("bigid", PrimitiveType::Int64),
        ColumnSchema::scalar("vis", PrimitiveType::Complex64),
    ])
    .unwrap();

    let categories = ["star", "galaxy", "pulsar", "quasar", "nebula"];
    let mut table = Table::with_schema(schema);
    for i in 0..n {
        let cat = categories[i % categories.len()];
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(i as i32))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String(format!("SRC_{i:03}"))),
                ),
                RecordField::new("ra", Value::Scalar(ScalarValue::Float64(i as f64 * 7.2))),
                RecordField::new(
                    "dec",
                    Value::Scalar(ScalarValue::Float64(-45.0 + i as f64 * 1.8)),
                ),
                RecordField::new(
                    "flux",
                    Value::Scalar(ScalarValue::Float64(0.1 + i as f64 * 0.5)),
                ),
                RecordField::new(
                    "category",
                    Value::Scalar(ScalarValue::String(cat.to_string())),
                ),
                RecordField::new("flag", Value::Scalar(ScalarValue::Bool((i % 3) != 0))),
                RecordField::new(
                    "bigid",
                    Value::Scalar(ScalarValue::Int64(i as i64 * 1_000_000)),
                ),
                RecordField::new(
                    "vis",
                    Value::Scalar(ScalarValue::Complex64(casacore_types::Complex64::new(
                        i as f64 * 0.1,
                        i as f64 * -0.2,
                    ))),
                ),
            ]))
            .unwrap();
    }
    table
}

/// Build the 10-row variable-shape fixture (matches C++ `write_varshape_fixture_impl`).
///
/// Schema: vardata(Float64, variable-shape), label(String)
/// Row i gets a 1D array of length i+1.
pub fn build_varshape_fixture() -> Table {
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    let schema = TableSchema::new(vec![
        ColumnSchema::array_variable("vardata", PrimitiveType::Float64, Some(1)),
        ColumnSchema::scalar("label", PrimitiveType::String),
    ])
    .unwrap();

    let mut table = Table::with_schema(schema);
    for row in 0..10u32 {
        let len = row as usize + 1;
        let data: Vec<f64> = (0..len)
            .map(|j| row as f64 * 10.0 + j as f64 + 0.5)
            .collect();
        let arr = ArrayD::from_shape_vec(IxDyn(&[len]).f(), data).unwrap();
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("vardata", Value::Array(ArrayValue::Float64(arr))),
                RecordField::new(
                    "label",
                    Value::Scalar(ScalarValue::String(format!("R{row}"))),
                ),
            ]))
            .unwrap();
    }
    table
}

/// Build a large array fixture with `n` rows for benchmarking.
/// Each row has an array column of the given shape.
pub fn build_array_fixture_n(n: usize, shape: &[usize]) -> Table {
    use ndarray::{ArrayD, IxDyn};

    let nelems: usize = shape.iter().product();
    let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
        "data",
        PrimitiveType::Float64,
        shape.to_vec(),
    )])
    .unwrap();

    let mut table = Table::with_schema(schema);
    for row in 0..n {
        let vals: Vec<f64> = (0..nelems)
            .map(|j| (row * nelems + j) as f64 * 0.1)
            .collect();
        let arr = ArrayD::from_shape_vec(IxDyn(shape), vals).unwrap();
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(arr)),
            )]))
            .unwrap();
    }
    table
}
