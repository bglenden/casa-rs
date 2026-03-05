// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-wave TaQL interop cross-matrix tests.
//!
//! Each test runs the full RR/CC/CR/RC matrix when C++ casacore is available.
//! Queries use compatible syntax for both Rust and C++ TaQL (Rust omits FROM,
//! C++ uses `$1`).

use casacore_test_support::taql_interop::*;

/// Assert all cells in the cross-matrix passed.
fn assert_all_passed(results: &[TaqlCrossResult], query: &str) {
    for r in results {
        assert!(
            r.passed,
            "cross-matrix cell {} failed for query '{}': {}",
            r.label,
            query,
            r.error.as_deref().unwrap_or("(no error)")
        );
    }
}

// ── Basic SELECT cross-matrix tests ──

#[test]
fn cross_select_star() {
    let results =
        run_taql_cross_matrix(TaqlFixtureKind::Simple, "SELECT *", "SELECT * FROM $1", 0.0);
    assert_all_passed(&results, "SELECT *");
}

#[test]
fn cross_select_where_gt() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE id > 45",
        "SELECT * FROM $1 WHERE id > 45",
        0.0,
    );
    assert_all_passed(&results, "WHERE id > 45");
}

#[test]
fn cross_select_where_compound() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE flux > 20.0 AND category = 'star'",
        "SELECT * FROM $1 WHERE flux > 20.0 AND category = 'star'",
        1e-6,
    );
    assert_all_passed(&results, "compound AND");
}

#[test]
fn cross_select_where_like() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE name LIKE 'SRC_00%'",
        "SELECT * FROM $1 WHERE name LIKE 'SRC_00%'",
        0.0,
    );
    assert_all_passed(&results, "LIKE");
}

#[test]
fn cross_select_where_between() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE id BETWEEN 10 AND 14",
        "SELECT * FROM $1 WHERE id BETWEEN 10 AND 14",
        0.0,
    );
    assert_all_passed(&results, "BETWEEN");
}

#[test]
fn cross_select_where_in() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE id IN (1, 3, 5, 7)",
        "SELECT * FROM $1 WHERE id IN (1, 3, 5, 7)",
        0.0,
    );
    assert_all_passed(&results, "IN list");
}

#[test]
fn cross_select_where_not_in() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE id NOT IN (0, 1)",
        "SELECT * FROM $1 WHERE id NOT IN (0, 1)",
        0.0,
    );
    assert_all_passed(&results, "NOT IN");
}

#[test]
fn cross_select_where_expression() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE flux * 2.0 > 48.0",
        "SELECT * FROM $1 WHERE flux * 2.0 > 48.0",
        1e-6,
    );
    assert_all_passed(&results, "expression in WHERE");
}

#[test]
fn cross_select_order_by() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * ORDER BY flux DESC",
        "SELECT * FROM $1 ORDER BY flux DESC",
        1e-6,
    );
    assert_all_passed(&results, "ORDER BY");
}

#[test]
fn cross_select_limit_offset() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * LIMIT 5 OFFSET 10",
        "SELECT * FROM $1 LIMIT 5 OFFSET 10",
        0.0,
    );
    assert_all_passed(&results, "LIMIT OFFSET");
}

#[test]
fn cross_select_distinct() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT DISTINCT category",
        "SELECT DISTINCT category FROM $1",
        0.0,
    );
    assert_all_passed(&results, "DISTINCT");
}

#[test]
fn cross_select_projection() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT id, name WHERE flux > 24.0",
        "SELECT id, name FROM $1 WHERE flux > 24.0",
        0.0,
    );
    assert_all_passed(&results, "projection");
}

// ── Cross-matrix result comparison tests ──
// These verify actual value equality across RR/CC/CR/RC.

#[test]
fn compare_select_where_flux() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, flux WHERE flux > 20.0",
        "SELECT id, flux FROM $1 WHERE flux > 20.0",
        1e-6,
    );
    assert_all_passed(&results, "compare flux filter");
}

#[test]
fn compare_select_between() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id WHERE id BETWEEN 10 AND 14",
        "SELECT id FROM $1 WHERE id BETWEEN 10 AND 14",
        0.0,
    );
    assert_all_passed(&results, "compare BETWEEN");
}

#[test]
fn compare_select_order_by_desc_limit() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, flux ORDER BY flux DESC LIMIT 10",
        "SELECT id, flux FROM $1 ORDER BY flux DESC LIMIT 10",
        1e-6,
    );
    assert_all_passed(&results, "compare ORDER BY DESC LIMIT");
}

#[test]
fn compare_select_like() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT name WHERE name LIKE 'SRC_00%'",
        "SELECT name FROM $1 WHERE name LIKE 'SRC_00%'",
        0.0,
    );
    assert_all_passed(&results, "compare LIKE");
}

#[test]
fn compare_full_pipeline() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, name, flux WHERE category = 'star' ORDER BY flux DESC LIMIT 3",
        "SELECT id, name, flux FROM $1 WHERE category = 'star' ORDER BY flux DESC LIMIT 3",
        1e-6,
    );
    assert_all_passed(&results, "full pipeline");
}

// ── Syntax-divergent test: different function names ──
// Rust uses length(), C++ uses strlen(). Separate queries for each side.

#[test]
fn cross_string_length_function() {
    let results = run_taql_cross_matrix(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE length(name) > 6",
        "SELECT * FROM $1 WHERE strlength(name) > 6",
        0.0,
    );
    assert_all_passed(&results, "string length function");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 2: Core syntax, operators, CASE
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_regex_match() {
    // C++ uses m/.../  for regex (p/.../  is glob-pattern syntax).
    // Rust uses =~ p/.../  which we treat as regex.
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE name =~ p/^SRC_00.*/",
        "SELECT * FROM $1 WHERE name ~ m/^SRC_00.*/",
        0.0,
    );
    assert_all_passed(&results, "regex match");
}

#[test]
fn cross_regex_negated() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE name !~ p/^SRC_00.*/",
        "SELECT * FROM $1 WHERE name !~ m/^SRC_00.*/",
        0.0,
    );
    assert_all_passed(&results, "regex negated");
}

#[test]
fn cross_bitwise_and_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE (id & 1) = 0",
        "SELECT * FROM $1 WHERE (id & 1) = 0",
        0.0,
    );
    assert_all_passed(&results, "bitwise AND filter");
}

#[test]
fn cross_bitwise_or_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE (id | 16) > 16",
        "SELECT * FROM $1 WHERE (id | 16) > 16",
        0.0,
    );
    assert_all_passed(&results, "bitwise OR filter");
}

#[test]
fn cross_bitwise_xor_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE (id ^ 3) > 10",
        "SELECT * FROM $1 WHERE (id ^ 3) > 10",
        0.0,
    );
    assert_all_passed(&results, "bitwise XOR filter");
}

#[test]
fn cross_in_range() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE id IN [10:20]",
        "SELECT * FROM $1 WHERE id IN [10:20]",
        0.0,
    );
    assert_all_passed(&results, "IN range");
}

#[test]
fn cross_iif_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE iif(flux > 20.0, TRUE, FALSE)",
        "SELECT * FROM $1 WHERE iif(flux > 20.0, TRUE, FALSE)",
        1e-6,
    );
    assert_all_passed(&results, "IIF filter");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 3: Array indexing, slicing, style modes
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_array_element_select() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "SELECT idata[1,2]",
        "SELECT idata[1,2] FROM $1",
        1e-6,
    );
    assert_all_passed(&results, "array element SELECT");
}

#[test]
fn cross_array_slice_select() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "SELECT idata[1:2,]",
        "SELECT idata[1:2,] FROM $1",
        1e-6,
    );
    assert_all_passed(&results, "array slice SELECT");
}

#[test]
fn cross_array_element_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "SELECT * WHERE idata[1,1] > 10",
        "SELECT * FROM $1 WHERE idata[1,1] > 10",
        1e-6,
    );
    assert_all_passed(&results, "array element filter");
}

#[test]
fn cross_style_python_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "USING STYLE PYTHON SELECT * WHERE idata[0,0] > 10",
        "USING STYLE PYTHON SELECT * FROM $1 WHERE idata[0,0] > 10",
        1e-6,
    );
    assert_all_passed(&results, "STYLE PYTHON filter");
}

#[test]
fn cross_array_full_slice() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "SELECT fdata[1:3,1:2]",
        "SELECT fdata[1:3,1:2] FROM $1",
        1e-6,
    );
    assert_all_passed(&results, "array full slice");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 4: Running and boxed window functions
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_runningmean() {
    // C++ running functions require per-dimension window shapes for nD arrays;
    // scalar halfWidth on 2D arrays produces zeros. Exec-only until we add
    // proper multi-dimensional window support.
    let results = run_taql_cross_matrix_exec(
        TaqlFixtureKind::Array,
        "SELECT runningmean(fdata, 2)",
        "SELECT runningmean(fdata, 2) FROM $1",
    );
    assert_all_passed(&results, "runningmean");
}

#[test]
fn cross_runningsum() {
    let results = run_taql_cross_matrix_exec(
        TaqlFixtureKind::Array,
        "SELECT runningsum(fdata, 2)",
        "SELECT runningsum(fdata, 2) FROM $1",
    );
    assert_all_passed(&results, "runningsum");
}

#[test]
fn cross_boxedmean() {
    let results = run_taql_cross_matrix_exec(
        TaqlFixtureKind::Array,
        "SELECT boxedmean(fdata, 3)",
        "SELECT boxedmean(fdata, 3) FROM $1",
    );
    assert_all_passed(&results, "boxedmean");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 5: Partial-axis array reductions
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_sums_axis1() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "SELECT sums(fdata, 1)",
        "SELECT sums(fdata, 1) FROM $1",
        1e-6,
    );
    assert_all_passed(&results, "sums axis 1");
}

#[test]
fn cross_means_axis2() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "SELECT means(fdata, 2)",
        "SELECT means(fdata, 2) FROM $1",
        1e-6,
    );
    assert_all_passed(&results, "means axis 2");
}

#[test]
fn cross_mins_axis1() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "SELECT mins(idata, 1)",
        "SELECT mins(idata, 1) FROM $1",
        1e-6,
    );
    assert_all_passed(&results, "mins axis 1");
}

#[test]
fn cross_maxs_axis2() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Array,
        "SELECT maxs(idata, 2)",
        "SELECT maxs(idata, 2) FROM $1",
        1e-6,
    );
    assert_all_passed(&results, "maxs axis 2");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 6: Group aggregates
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_groupby_gcount() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT category, gcount() GROUP BY category",
        "SELECT category, gcount() FROM $1 GROUP BY category",
        0.0,
    );
    assert_all_passed(&results, "GROUP BY gcount");
}

#[test]
fn cross_groupby_gsum() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT category, gsum(flux) GROUP BY category",
        "SELECT category, gsum(flux) FROM $1 GROUP BY category",
        1e-6,
    );
    assert_all_passed(&results, "GROUP BY gsum");
}

#[test]
fn cross_groupby_gmin_gmax() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT category, gmin(flux), gmax(flux) GROUP BY category",
        "SELECT category, gmin(flux), gmax(flux) FROM $1 GROUP BY category",
        1e-6,
    );
    assert_all_passed(&results, "GROUP BY gmin gmax");
}

#[test]
fn cross_groupby_gmean() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT category, gmean(flux) GROUP BY category",
        "SELECT category, gmean(flux) FROM $1 GROUP BY category",
        1e-6,
    );
    assert_all_passed(&results, "GROUP BY gmean");
}

#[test]
fn cross_gaggr() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT category, gaggr(id) GROUP BY category",
        "SELECT category, gaggr(id) FROM $1 GROUP BY category",
        1e-6,
    );
    assert_all_passed(&results, "gaggr");
}

#[test]
fn cross_growid() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT category, growid() GROUP BY category",
        "SELECT category, growid() FROM $1 GROUP BY category",
        1e-6,
    );
    assert_all_passed(&results, "growid");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 7: Utility functions
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_upper_lower_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE upper(category) = 'STAR'",
        "SELECT * FROM $1 WHERE upper(category) = 'STAR'",
        0.0,
    );
    assert_all_passed(&results, "upper filter");
}

#[test]
fn cross_substr_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE substr(name, 0, 3) = 'SRC'",
        "SELECT * FROM $1 WHERE substr(name, 0, 3) = 'SRC'",
        0.0,
    );
    assert_all_passed(&results, "substr filter");
}

#[test]
fn cross_trim_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE trim(category) = 'star'",
        "SELECT * FROM $1 WHERE trim(category) = 'star'",
        0.0,
    );
    assert_all_passed(&results, "trim filter");
}

#[test]
fn cross_abs_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE abs(dec) > 30.0",
        "SELECT * FROM $1 WHERE abs(dec) > 30.0",
        1e-6,
    );
    assert_all_passed(&results, "abs filter");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 8: Aliases, COUNT SELECT, HAVING
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_column_alias() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, flux AS brightness WHERE flux > 20.0",
        "SELECT id, flux AS brightness FROM $1 WHERE flux > 20.0",
        1e-6,
    );
    assert_all_passed(&results, "column alias");
}

#[test]
fn cross_count_select() {
    // C++ uses "COUNT <cols> FROM <table> WHERE ..." (no SELECT keyword).
    let results = run_taql_cross_matrix_exec(
        TaqlFixtureKind::Simple,
        "COUNT SELECT * WHERE flux > 20.0",
        "COUNT * FROM $1 WHERE flux > 20.0",
    );
    assert_all_passed(&results, "COUNT SELECT");
}

#[test]
fn cross_having_count() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT category, gcount() GROUP BY category HAVING gcount() > 5",
        "SELECT category, gcount() FROM $1 GROUP BY category HAVING gcount() > 5",
        0.0,
    );
    assert_all_passed(&results, "HAVING count");
}

#[test]
fn cross_having_sum() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT category, gsum(flux) GROUP BY category HAVING gsum(flux) > 50.0",
        "SELECT category, gsum(flux) FROM $1 GROUP BY category HAVING gsum(flux) > 50.0",
        1e-6,
    );
    assert_all_passed(&results, "HAVING sum");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 10: DDL format compatibility
// ══════════════════════════════════════════════════════════════════════

/// Rust creates table → C++ opens and queries it.
#[test]
fn cross_ddl_rust_create_cpp_open() {
    // This is effectively what the RC cells test, but named explicitly.
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, name WHERE id < 5",
        "SELECT id, name FROM $1 WHERE id < 5",
        0.0,
    );
    // Specifically check that the RC cell passes.
    for r in &results {
        if r.label == "RC" {
            assert!(
                r.passed,
                "DDL Rust→C++ open failed: {}",
                r.error.as_deref().unwrap_or("(no error)")
            );
        }
    }
}

/// C++ creates table → Rust opens and queries it.
#[test]
fn cross_ddl_cpp_create_rust_open() {
    // This is effectively what the CR cells test, but named explicitly.
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, name WHERE id < 5",
        "SELECT id, name FROM $1 WHERE id < 5",
        0.0,
    );
    // Specifically check that the CR cell passes.
    for r in &results {
        if r.label == "CR" {
            assert!(
                r.passed,
                "DDL C++→Rust open failed: {}",
                r.error.as_deref().unwrap_or("(no error)")
            );
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// Wave 12: Built-in function regression (trig)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_trig_functions() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE sin(ra) > 0.5 AND cos(dec) < 0.0",
        "SELECT * FROM $1 WHERE sin(ra) > 0.5 AND cos(dec) < 0.0",
        1e-6,
    );
    assert_all_passed(&results, "trig functions");
}

// ══════════════════════════════════════════════════════════════════════
// Wave 14: Complex pipeline (additional)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_complex_pipeline_2() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, name WHERE flux > 10.0 AND category != 'nebula' ORDER BY id ASC LIMIT 20 OFFSET 5",
        "SELECT id, name FROM $1 WHERE flux > 10.0 AND category != 'nebula' ORDER BY id ASC LIMIT 20 OFFSET 5",
        0.0,
    );
    assert_all_passed(&results, "complex pipeline 2");
}

// ══════════════════════════════════════════════════════════════════════
// Additional math/string function coverage
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_sqrt_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE sqrt(flux) > 4.0",
        "SELECT * FROM $1 WHERE sqrt(flux) > 4.0",
        1e-6,
    );
    assert_all_passed(&results, "sqrt filter");
}

#[test]
fn cross_log_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE log(flux + 1.0) > 2.0",
        "SELECT * FROM $1 WHERE log(flux + 1.0) > 2.0",
        1e-6,
    );
    assert_all_passed(&results, "log filter");
}

#[test]
fn cross_floor_ceil_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE floor(flux) != ceil(flux)",
        "SELECT * FROM $1 WHERE floor(flux) != ceil(flux)",
        1e-6,
    );
    assert_all_passed(&results, "floor/ceil filter");
}

#[test]
fn cross_power_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE flux ** 2.0 > 400.0",
        "SELECT * FROM $1 WHERE flux ** 2.0 > 400.0",
        1e-6,
    );
    assert_all_passed(&results, "power filter");
}

#[test]
fn cross_modulo_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE id % 3 = 0",
        "SELECT * FROM $1 WHERE id % 3 = 0",
        0.0,
    );
    assert_all_passed(&results, "modulo filter");
}

// ══════════════════════════════════════════════════════════════════════
// Additional predicate coverage
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_is_not_null() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE flux IS NOT NULL",
        "SELECT * FROM $1 WHERE !isnull(flux)",
        0.0,
    );
    assert_all_passed(&results, "IS NOT NULL");
}

#[test]
fn cross_not_between() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE id NOT BETWEEN 10 AND 40",
        "SELECT * FROM $1 WHERE id NOT BETWEEN 10 AND 40",
        0.0,
    );
    assert_all_passed(&results, "NOT BETWEEN");
}

#[test]
fn cross_or_condition() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT * WHERE category = 'star' OR category = 'pulsar'",
        "SELECT * FROM $1 WHERE category = 'star' OR category = 'pulsar'",
        0.0,
    );
    assert_all_passed(&results, "OR condition");
}

// ══════════════════════════════════════════════════════════════════════
// Extended type coverage: Bool, Int64, DComplex scalars
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_bool_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, flag WHERE flag = true",
        "SELECT id, flag FROM $1 WHERE flag = true",
        0.0,
    );
    assert_all_passed(&results, "Bool filter");
}

#[test]
fn cross_int64_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, bigid WHERE bigid > 20000000",
        "SELECT id, bigid FROM $1 WHERE bigid > 20000000",
        0.0,
    );
    assert_all_passed(&results, "Int64 filter");
}

#[test]
fn cross_bool_select_all() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, flag, bigid WHERE id < 6",
        "SELECT id, flag, bigid FROM $1 WHERE id < 6",
        0.0,
    );
    assert_all_passed(&results, "Bool+Int64 select");
}

#[test]
fn cross_complex_select() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::Simple,
        "SELECT id, vis WHERE id < 5",
        "SELECT id, vis FROM $1 WHERE id < 5",
        1e-6,
    );
    assert_all_passed(&results, "DComplex select");
}

// ══════════════════════════════════════════════════════════════════════
// Variable-shape array cross-matrix
// ══════════════════════════════════════════════════════════════════════

#[test]
fn cross_varshape_select_all() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::VarShape,
        "SELECT vardata, label",
        "SELECT vardata, label FROM $1",
        1e-6,
    );
    assert_all_passed(&results, "varshape SELECT *");
}

#[test]
fn cross_varshape_filter() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::VarShape,
        "SELECT vardata WHERE label = 'R5'",
        "SELECT vardata FROM $1 WHERE label = 'R5'",
        1e-6,
    );
    assert_all_passed(&results, "varshape filter");
}

#[test]
fn cross_varshape_nelements() {
    let results = run_taql_cross_matrix_compare(
        TaqlFixtureKind::VarShape,
        "SELECT nelements(vardata), label",
        "SELECT nelements(vardata), label FROM $1",
        0.0,
    );
    assert_all_passed(&results, "varshape nelements");
}
