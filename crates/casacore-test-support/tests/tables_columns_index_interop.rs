// SPDX-License-Identifier: LGPL-3.0-or-later
//! Interop and performance tests for `ColumnsIndex`.
//!
//! # CR correctness test
//!
//! C++ writes a 50-row `antenna_id` table; Rust opens it, builds a
//! `ColumnsIndex`, and verifies that exact and range lookups return the
//! expected rows.  This catches any mismatch between how C++ encodes the
//! column data and how Rust decodes it.
//!
//! # Performance test
//!
//! Both the Rust and C++ `ColumnsIndex` implementations are timed on the same
//! 100 000-row table.  The test fails if Rust is more than 10× slower than
//! C++ (a generous budget for debug builds; gross regressions will still be
//! caught).

use casacore_tables::{
    ColumnSchema, ColumnsIndex, DataManagerKind, Table, TableOptions, TableSchema,
};
use casacore_test_support::{
    CppTableFixture, cpp_backend_available, cpp_columns_index_time_lookups, cpp_table_write,
};
use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

// ── CR correctness ────────────────────────────────────────────────────────────

/// C++ writes a 50-row table (antenna_id = row_index % 10).
/// Rust opens it, indexes it, and verifies exact + range lookups.
#[test]
fn cr_columns_index() {
    if !cpp_backend_available() {
        eprintln!("skipping cr_columns_index: C++ casacore not available");
        return;
    }

    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("antenna.tbl");

    cpp_table_write(CppTableFixture::ColumnsIndex, &table_path)
        .expect("C++ should write columns_index fixture");

    let table =
        Table::open(TableOptions::new(&table_path)).expect("Rust should open C++-written table");
    assert_eq!(table.row_count(), 50);

    let idx = ColumnsIndex::new(&table, &["antenna_id"]).expect("build index");

    // Exact lookup: antenna_id == 3 → rows 3, 13, 23, 33, 43 (5 rows)
    let mut rows = idx.lookup(&[("antenna_id", &ScalarValue::Int32(3))]);
    rows.sort_unstable();
    assert_eq!(rows.len(), 5, "antenna_id=3 should match 5 rows");
    for &r in &rows {
        let val = table
            .cell(r, "antenna_id")
            .expect("cell lookup")
            .expect("cell exists");
        assert_eq!(val, &Value::Scalar(ScalarValue::Int32(3)));
    }

    // Edge: antenna_id == 0 → rows 0, 10, 20, 30, 40
    let mut rows = idx.lookup(&[("antenna_id", &ScalarValue::Int32(0))]);
    rows.sort_unstable();
    assert_eq!(rows, vec![0, 10, 20, 30, 40]);

    // Edge: antenna_id == 9 → rows 9, 19, 29, 39, 49
    let mut rows = idx.lookup(&[("antenna_id", &ScalarValue::Int32(9))]);
    rows.sort_unstable();
    assert_eq!(rows, vec![9, 19, 29, 39, 49]);

    // Not found: antenna_id == 99 → empty
    let rows = idx.lookup(&[("antenna_id", &ScalarValue::Int32(99))]);
    assert!(rows.is_empty(), "antenna_id=99 should not match any row");

    // Range [2, 4] inclusive: antenna_id ∈ {2, 3, 4} → 15 rows
    let range = idx.lookup_range(
        &[("antenna_id", &ScalarValue::Int32(2))],
        &[("antenna_id", &ScalarValue::Int32(4))],
        true,
        true,
    );
    assert_eq!(range.len(), 15, "antenna_id in [2,4] should yield 15 rows");
    for &r in &range {
        let val = table
            .cell(r, "antenna_id")
            .expect("cell lookup")
            .expect("cell exists");
        if let Value::Scalar(ScalarValue::Int32(v)) = val {
            assert!(*v >= 2 && *v <= 4, "row {r} value {v} outside [2,4]");
        } else {
            panic!("expected Int32 at row {r}");
        }
    }

    // is_unique is false (each key appears 5 times)
    assert!(!idx.is_unique());
}

// ── Performance: Rust vs C++ ──────────────────────────────────────────────────

/// Build the same 100 000-row table (`id = i % 1000`) with Rust,
/// then time 1 000 exact lookups with both the C++ and Rust `ColumnsIndex`.
///
/// Fails if Rust takes more than 10× as long as C++ (allows generous margin
/// for debug builds and CI variance; the intent is to catch gross regressions).
#[test]
fn columns_index_perf_vs_cpp() {
    if !cpp_backend_available() {
        eprintln!("skipping columns_index_perf_vs_cpp: C++ casacore not available");
        return;
    }

    const NROWS: usize = 100_000;
    const NQUERIES: u64 = 1_000;
    const KEY: i32 = 42;
    // KEY % 1000 == 42, so exactly NROWS / 1000 == 100 rows match.
    const EXPECTED_MATCHES: usize = NROWS / 1000;

    let dir = tempfile::tempdir().expect("create temp dir");
    let table_path = dir.path().join("perf.tbl");

    // Write the table once with Rust (StandardStMan so C++ reads it cleanly).
    let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    for i in 0..NROWS {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32((i % 1000) as i32)),
            )]))
            .unwrap();
    }
    table
        .save(TableOptions::new(&table_path).with_data_manager(DataManagerKind::StandardStMan))
        .unwrap();

    // ── C++ timing ──────────────────────────────────────────────────────────
    let (cpp_elapsed_ns, cpp_match_count) =
        cpp_columns_index_time_lookups(&table_path, KEY, NQUERIES)
            .expect("C++ timing should succeed");

    assert_eq!(
        cpp_match_count as usize, EXPECTED_MATCHES,
        "C++ match count mismatch"
    );

    // ── Rust timing ─────────────────────────────────────────────────────────
    let table = Table::open(TableOptions::new(&table_path)).unwrap();
    let idx = ColumnsIndex::new(&table, &["id"]).unwrap();

    let t0 = std::time::Instant::now();
    let mut rust_match_count = 0usize;
    for _ in 0..NQUERIES {
        rust_match_count = idx.lookup(&[("id", &ScalarValue::Int32(KEY))]).len();
    }
    let rust_elapsed_ns = t0.elapsed().as_nanos() as u64;

    assert_eq!(
        rust_match_count, EXPECTED_MATCHES,
        "Rust match count mismatch"
    );

    // ── Report ───────────────────────────────────────────────────────────────
    let cpp_per_q = cpp_elapsed_ns / NQUERIES;
    let rust_per_q = rust_elapsed_ns / NQUERIES;

    eprintln!(
        "ColumnsIndex perf ({NQUERIES} lookups, {NROWS} rows, key={KEY}, {EXPECTED_MATCHES} matches/query):\n  \
         C++:  {} ns/query  (total {} ms)\n  \
         Rust: {} ns/query  (total {} ms)\n  \
         ratio Rust/C++: {:.1}×",
        cpp_per_q,
        cpp_elapsed_ns / 1_000_000,
        rust_per_q,
        rust_elapsed_ns / 1_000_000,
        rust_elapsed_ns as f64 / cpp_elapsed_ns.max(1) as f64,
    );

    assert!(
        rust_elapsed_ns <= cpp_elapsed_ns.saturating_mul(10),
        "Rust ColumnsIndex is >10× slower than C++: \
         Rust={rust_per_q}ns/q  C++={cpp_per_q}ns/q"
    );
}
