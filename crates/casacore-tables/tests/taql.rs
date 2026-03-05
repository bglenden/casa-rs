// SPDX-License-Identifier: LGPL-3.0-or-later
//! Integration tests for TaQL (Table Query Language).
//!
//! Builds realistic tables and runs end-to-end queries across all
//! statement types.

use casacore_tables::taql::{self, TaqlResult};
use casacore_tables::{ColumnSchema, Table, TableSchema};
use casacore_types::*;

/// Build a 50-row astronomy catalog table for testing.
fn catalog_table() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("ra", PrimitiveType::Float64),
        ColumnSchema::scalar("dec", PrimitiveType::Float64),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ColumnSchema::scalar("category", PrimitiveType::String),
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
            ]))
            .unwrap();
    }
    table
}

// ── SELECT tests ──

#[test]
fn select_star_returns_all_rows() {
    let mut table = catalog_table();
    let view = table.query("SELECT *").unwrap();
    assert_eq!(view.row_count(), 50);
}

#[test]
fn select_where_simple() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE id > 45").unwrap();
    assert_eq!(view.row_count(), 4); // ids 46,47,48,49
}

#[test]
fn select_where_compound_and_or() {
    let mut table = catalog_table();
    let view = table
        .query("SELECT * WHERE flux > 20.0 AND category = 'star'")
        .unwrap();
    // flux > 20.0 means id >= 40 (flux = 0.1 + id * 0.5), stars are id % 5 == 0
    // ids: 40, 45
    assert_eq!(view.row_count(), 2);
}

#[test]
fn select_where_like() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE name LIKE 'SRC_00%'").unwrap();
    assert_eq!(view.row_count(), 10); // SRC_000 through SRC_009
}

#[test]
fn select_where_between() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE id BETWEEN 10 AND 14").unwrap();
    assert_eq!(view.row_count(), 5);
}

#[test]
fn select_where_in() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE id IN (1, 3, 5, 7)").unwrap();
    assert_eq!(view.row_count(), 4);
}

#[test]
fn select_where_not_in() {
    let mut table = catalog_table();
    // Everything except ids 0 and 1
    let view = table.query("SELECT * WHERE id NOT IN (0, 1)").unwrap();
    assert_eq!(view.row_count(), 48);
}

#[test]
fn select_order_by_single() {
    let mut table = catalog_table();
    let view = table.query("SELECT * ORDER BY flux DESC").unwrap();
    assert_eq!(view.row_count(), 50);
    // First row should have the highest flux (id=49)
    let first = view.cell(0, "id").unwrap();
    assert_eq!(first, &Value::Scalar(ScalarValue::Int32(49)));
}

#[test]
fn select_order_by_multiple() {
    let mut table = catalog_table();
    let view = table
        .query("SELECT * ORDER BY category ASC, flux DESC")
        .unwrap();
    assert_eq!(view.row_count(), 50);
    // First row should be a galaxy with the highest flux
    let cat = view.cell(0, "category").unwrap();
    assert_eq!(
        cat,
        &Value::Scalar(ScalarValue::String("galaxy".to_string()))
    );
}

#[test]
fn select_limit_offset() {
    let mut table = catalog_table();
    let view = table.query("SELECT * LIMIT 5 OFFSET 10").unwrap();
    assert_eq!(view.row_count(), 5);
    let first_id = view.cell(0, "id").unwrap();
    assert_eq!(first_id, &Value::Scalar(ScalarValue::Int32(10)));
}

#[test]
fn select_distinct() {
    let mut table = catalog_table();
    let view = table.query("SELECT DISTINCT category").unwrap();
    assert_eq!(view.row_count(), 5);
}

#[test]
fn select_column_projection() {
    let mut table = catalog_table();
    let view = table.query("SELECT id, name WHERE flux > 24.0").unwrap();
    assert_eq!(view.column_names(), &["id", "name"]);
}

#[test]
fn select_expression_where() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE flux * 2.0 > 48.0").unwrap();
    // flux * 2 > 48 means flux > 24, id * 0.5 + 0.1 > 24, id > 47.8
    assert_eq!(view.row_count(), 2); // ids 48, 49
}

#[test]
fn select_empty_result() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE id > 1000").unwrap();
    assert_eq!(view.row_count(), 0);
}

#[test]
fn select_with_function_in_where() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE length(name) > 6").unwrap();
    // All names are "SRC_NNN" which is 7 chars, so > 6 matches all
    assert_eq!(view.row_count(), 50);
}

#[test]
fn select_with_rowid() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE ROWID() < 3").unwrap();
    assert_eq!(view.row_count(), 3);
}

// ── UPDATE tests ──

#[test]
fn update_with_where() {
    let mut table = catalog_table();
    let result = table
        .execute_taql("UPDATE SET flux = 999.0 WHERE id = 25")
        .unwrap();
    match result {
        TaqlResult::Update { rows_affected } => assert_eq!(rows_affected, 1),
        _ => panic!("expected Update"),
    }
    let val = table.cell(25, "flux").unwrap();
    assert_eq!(val, &Value::Scalar(ScalarValue::Float64(999.0)));
}

#[test]
fn update_expression() {
    let mut table = catalog_table();
    let result = table
        .execute_taql("UPDATE SET flux = flux * 10.0 WHERE id = 0")
        .unwrap();
    match result {
        TaqlResult::Update { rows_affected } => assert_eq!(rows_affected, 1),
        _ => panic!("expected Update"),
    }
    let val = table.cell(0, "flux").unwrap();
    assert_eq!(val, &Value::Scalar(ScalarValue::Float64(1.0))); // 0.1 * 10.0
}

#[test]
fn update_multiple_rows() {
    let mut table = catalog_table();
    let result = table
        .execute_taql("UPDATE SET category = 'unknown' WHERE id > 47")
        .unwrap();
    match result {
        TaqlResult::Update { rows_affected } => assert_eq!(rows_affected, 2),
        _ => panic!("expected Update"),
    }
}

// ── INSERT tests ──

#[test]
fn insert_single_row() {
    let mut table = catalog_table();
    let result = table
        .execute_taql(
            "INSERT INTO (id, name, ra, dec, flux, category) VALUES (50, 'NEW_SRC', 0.0, 0.0, 100.0, 'star')",
        )
        .unwrap();
    match result {
        TaqlResult::Insert { rows_inserted } => assert_eq!(rows_inserted, 1),
        _ => panic!("expected Insert"),
    }
    assert_eq!(table.row_count(), 51);
}

#[test]
fn insert_multiple_rows() {
    let mut table = catalog_table();
    let result = table
        .execute_taql(
            "INSERT INTO (id, name, ra, dec, flux, category) VALUES (50, 'A', 0.0, 0.0, 1.0, 'star'), (51, 'B', 1.0, 1.0, 2.0, 'galaxy')",
        )
        .unwrap();
    match result {
        TaqlResult::Insert { rows_inserted } => assert_eq!(rows_inserted, 2),
        _ => panic!("expected Insert"),
    }
    assert_eq!(table.row_count(), 52);
}

// ── DELETE tests ──

#[test]
fn delete_with_where() {
    let mut table = catalog_table();
    let result = table.execute_taql("DELETE FROM WHERE id > 47").unwrap();
    match result {
        TaqlResult::Delete { rows_deleted } => assert_eq!(rows_deleted, 2),
        _ => panic!("expected Delete"),
    }
    assert_eq!(table.row_count(), 48);
}

#[test]
fn delete_with_limit() {
    let mut table = catalog_table();
    let result = table
        .execute_taql("DELETE FROM WHERE category = 'star' LIMIT 3")
        .unwrap();
    match result {
        TaqlResult::Delete { rows_deleted } => assert_eq!(rows_deleted, 3),
        _ => panic!("expected Delete"),
    }
    assert_eq!(table.row_count(), 47);
}

// ── GROUP BY / Aggregate tests ──

#[test]
fn select_count_star() {
    let mut table = catalog_table();
    let stmt = taql::parse("SELECT COUNT(*)").unwrap();
    let result = taql::execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Materialized { table } => {
            // One group (the whole table)
            assert_eq!(table.row_count(), 1);
        }
        _ => panic!("expected Materialized"),
    }
}

#[test]
fn select_group_by_category() {
    let mut table = catalog_table();
    let stmt = taql::parse("SELECT category, COUNT(*) GROUP BY category").unwrap();
    let result = taql::execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Materialized { table } => {
            assert_eq!(table.row_count(), 5); // 5 categories
        }
        _ => panic!("expected Materialized"),
    }
}

// ── Parse round-trip tests ──

#[test]
fn parse_roundtrip_complex_select() {
    let query = "SELECT id, name WHERE (flux > 1.0) ORDER BY id ASC LIMIT 10";
    let ast1 = taql::parse(query).unwrap();
    let displayed = ast1.to_string();
    let ast2 = taql::parse(&displayed).unwrap();
    assert_eq!(ast1, ast2);
}

#[test]
fn parse_roundtrip_update() {
    let query = "UPDATE SET flux = (flux * 2.0) WHERE (id = 5)";
    let ast1 = taql::parse(query).unwrap();
    let displayed = ast1.to_string();
    let ast2 = taql::parse(&displayed).unwrap();
    assert_eq!(ast1, ast2);
}

// ── Error handling tests ──

#[test]
fn error_unknown_column() {
    let mut table = catalog_table();
    let result = table.query("SELECT nonexistent_col");
    assert!(result.is_err());
}

#[test]
fn error_invalid_syntax() {
    let result = taql::parse("SELECTA *");
    assert!(result.is_err());
}

#[test]
fn error_query_rejects_update() {
    let mut table = catalog_table();
    let result = table.query("UPDATE SET flux = 1.0");
    assert!(result.is_err());
}

// ── Built-in function integration tests ──

#[test]
fn function_sqrt_in_query() {
    let mut table = catalog_table();
    // sqrt is evaluated per-row via function call
    let view = table.query("SELECT * WHERE sqrt(flux) > 4.5").unwrap();
    // sqrt(flux) > 4.5 means flux > 20.25, i.e. id * 0.5 + 0.1 > 20.25, id >= 41
    assert_eq!(view.row_count(), 9); // ids 41-49
}

#[test]
fn function_upper_in_query() {
    let mut table = catalog_table();
    let view = table
        .query("SELECT * WHERE upper(category) = 'STAR'")
        .unwrap();
    assert_eq!(view.row_count(), 10); // 50/5 = 10 stars
}

#[test]
fn function_abs_in_query() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE abs(dec) < 10.0").unwrap();
    // dec = -45 + i * 1.8
    // |dec| < 10 means -10 < -45 + i*1.8 < 10, i.e. 35/1.8 < i < 55/1.8
    // 19.4 < i < 30.5, so ids 20..30 = 11 rows
    assert_eq!(view.row_count(), 11);
}

// ── Case-insensitivity tests ──

#[test]
fn case_insensitive_keywords() {
    let mut table = catalog_table();
    let view = table.query("select * where id < 5").unwrap();
    assert_eq!(view.row_count(), 5);
}

#[test]
fn case_insensitive_functions() {
    let mut table = catalog_table();
    let view = table.query("SELECT * WHERE SQRT(flux) > 4.5").unwrap();
    assert_eq!(view.row_count(), 9);
}

// ── Comment handling ──

#[test]
fn comments_in_query() {
    let mut table = catalog_table();
    let view = table
        .query("# Select bright sources\nSELECT * WHERE flux > 24.0")
        .unwrap();
    assert_eq!(view.row_count(), 2);
}
