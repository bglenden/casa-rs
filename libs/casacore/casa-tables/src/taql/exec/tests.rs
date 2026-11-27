// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::Table;
use crate::schema::{ColumnSchema, TableSchema};
use casa_core::PrimitiveType;

fn test_table() -> Table {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("id", PrimitiveType::Int32),
        ColumnSchema::scalar("name", PrimitiveType::String),
        ColumnSchema::scalar("flux", PrimitiveType::Float64),
    ])
    .unwrap();

    let mut table = Table::with_schema(schema);
    for i in 0..10 {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
                RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String(format!("source_{i}"))),
                ),
                RecordField::new("flux", Value::Scalar(ScalarValue::Float64(i as f64 * 1.5))),
            ]))
            .unwrap();
    }
    table
}

#[test]
fn select_star() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT *").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select {
            row_indices,
            columns,
        } => {
            assert_eq!(row_indices.len(), 10);
            assert!(columns.is_empty()); // all columns
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_where_gt() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT * WHERE id > 5").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            assert_eq!(row_indices.len(), 4); // rows 6,7,8,9
            assert_eq!(row_indices, vec![6, 7, 8, 9]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_order_by_desc() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT * ORDER BY id DESC").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            assert_eq!(row_indices, vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_limit() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT * LIMIT 3").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            assert_eq!(row_indices.len(), 3);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_offset() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT * OFFSET 8").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            assert_eq!(row_indices.len(), 2);
            assert_eq!(row_indices, vec![8, 9]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_limit_offset() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT * ORDER BY id DESC LIMIT 3 OFFSET 2").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            assert_eq!(row_indices, vec![7, 6, 5]); // rows 9,8,7,6,5... skip 2, take 3
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_empty_result() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT * WHERE id > 100").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            assert!(row_indices.is_empty());
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_expression_where() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT * WHERE flux * 2.0 > 20.0").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            // flux = i * 1.5, so flux * 2 > 20 means i * 3 > 20, i.e. i >= 7
            assert_eq!(row_indices, vec![7, 8, 9]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_column_projection() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT id, name WHERE flux > 1.0").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { columns, .. } => {
            assert_eq!(columns, vec!["id", "name"]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn select_distinct() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar(
        "category",
        PrimitiveType::String,
    )])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for cat in &["A", "B", "A", "C", "B"] {
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "category",
                Value::Scalar(ScalarValue::String(cat.to_string())),
            )]))
            .unwrap();
    }

    let stmt = crate::taql::parse("SELECT DISTINCT category").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            assert_eq!(row_indices.len(), 3); // A, B, C
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn update_basic() {
    let mut table = test_table();
    let stmt = crate::taql::parse("UPDATE SET flux = 99.0 WHERE id = 5").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Update { rows_affected } => {
            assert_eq!(rows_affected, 1);
        }
        _ => panic!("expected Update"),
    }
    let val = table
        .cell_accessor(5, "flux")
        .and_then(|cell| cell.value())
        .unwrap();
    assert_eq!(val, Some(&Value::Scalar(ScalarValue::Float64(99.0))));
}

#[test]
fn update_expression_rhs() {
    let mut table = test_table();
    let stmt = crate::taql::parse("UPDATE SET flux = flux * 2.0 WHERE id = 3").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Update { rows_affected } => {
            assert_eq!(rows_affected, 1);
        }
        _ => panic!("expected Update"),
    }
    let val = table
        .cell_accessor(3, "flux")
        .and_then(|cell| cell.value())
        .unwrap();
    assert_eq!(val, Some(&Value::Scalar(ScalarValue::Float64(9.0)))); // 3 * 1.5 * 2.0
}

#[test]
fn insert_basic() {
    let mut table = test_table();
    let stmt = crate::taql::parse("INSERT INTO (id, name, flux) VALUES (10, 'new', 42.0)").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Insert { rows_inserted } => {
            assert_eq!(rows_inserted, 1);
        }
        _ => panic!("expected Insert"),
    }
    assert_eq!(table.row_count(), 11);
    let val = table
        .cell_accessor(10, "id")
        .and_then(|cell| cell.value())
        .unwrap();
    assert_eq!(val, Some(&Value::Scalar(ScalarValue::Int32(10))));
}

#[test]
fn insert_multiple() {
    let mut table = test_table();
    let stmt =
        crate::taql::parse("INSERT INTO (id, name, flux) VALUES (10, 'a', 1.0), (11, 'b', 2.0)")
            .unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Insert { rows_inserted } => {
            assert_eq!(rows_inserted, 2);
        }
        _ => panic!("expected Insert"),
    }
    assert_eq!(table.row_count(), 12);
}

#[test]
fn delete_with_where() {
    let mut table = test_table();
    let stmt = crate::taql::parse("DELETE FROM WHERE id > 7").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Delete { rows_deleted } => {
            assert_eq!(rows_deleted, 2); // rows 8,9
        }
        _ => panic!("expected Delete"),
    }
    assert_eq!(table.row_count(), 8);
}

#[test]
fn delete_with_limit() {
    let mut table = test_table();
    let stmt = crate::taql::parse("DELETE FROM WHERE id > 5 LIMIT 2").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Delete { rows_deleted } => {
            assert_eq!(rows_deleted, 2);
        }
        _ => panic!("expected Delete"),
    }
    assert_eq!(table.row_count(), 8);
}

// ── CALC tests ──

#[test]
fn calc_simple() {
    let mut table = test_table();
    let stmt = crate::taql::parse("CALC 1 + 2").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select {
            row_indices,
            columns,
        } => {
            assert_eq!(columns, vec!["result"]);
            assert_eq!(row_indices, vec![0]);
        }
        _ => panic!("expected Select from CALC"),
    }
}

#[test]
fn calc_empty_table() {
    let schema = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
    let mut table = Table::with_schema(schema);
    let stmt = crate::taql::parse("CALC 42").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            assert!(row_indices.is_empty());
        }
        _ => panic!("expected Select from CALC"),
    }
}

// ── ALTER TABLE tests ──

#[test]
fn alter_add_column_exec() {
    let mut table = test_table();
    let orig_cols = table.schema().unwrap().columns().len();
    let stmt = crate::taql::parse("ALTER TABLE ADD COLUMN new_col FLOAT64").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    assert!(matches!(result, TaqlResult::Update { rows_affected: 0 }));
    assert_eq!(table.schema().unwrap().columns().len(), orig_cols + 1);
}

#[test]
fn alter_drop_column_exec() {
    let mut table = test_table();
    let stmt = crate::taql::parse("ALTER TABLE DROP COLUMN flux").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    assert!(matches!(result, TaqlResult::Update { rows_affected: 0 }));
    assert!(
        table
            .schema()
            .unwrap()
            .columns()
            .iter()
            .all(|c| c.name() != "flux")
    );
}

#[test]
fn alter_rename_column_exec() {
    let mut table = test_table();
    let stmt = crate::taql::parse("ALTER TABLE RENAME COLUMN flux TO brightness").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    assert!(matches!(result, TaqlResult::Update { rows_affected: 0 }));
    assert!(
        table
            .schema()
            .unwrap()
            .columns()
            .iter()
            .any(|c| c.name() == "brightness")
    );
}

#[test]
fn alter_add_row_exec() {
    let mut table = test_table();
    assert_eq!(table.row_count(), 10);
    let stmt = crate::taql::parse("ALTER TABLE ADD ROW").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    assert!(matches!(result, TaqlResult::Insert { rows_inserted: 1 }));
    assert_eq!(table.row_count(), 11);
}

#[test]
fn alter_set_keyword_exec() {
    let mut table = test_table();
    let stmt = crate::taql::parse("ALTER TABLE SET KEYWORD mykey = 42").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    assert!(matches!(result, TaqlResult::Update { rows_affected: 0 }));
    assert!(table.keywords().get("mykey").is_some());
}

#[test]
fn alter_add_column_bad_type() {
    let mut table = test_table();
    let stmt = crate::taql::parse("ALTER TABLE ADD COLUMN bad_col FOOBAR").unwrap();
    let result = execute(&stmt, &mut table);
    assert!(result.is_err());
}

// ── default_value_for_type tests ──

#[test]
fn default_values_all_types() {
    use casa_core::PrimitiveType as PT;
    for pt in [
        PT::Bool,
        PT::UInt8,
        PT::UInt16,
        PT::UInt32,
        PT::Int16,
        PT::Int32,
        PT::Int64,
        PT::Float32,
        PT::Float64,
        PT::Complex32,
        PT::Complex64,
        PT::String,
    ] {
        let val = default_value_for_type(pt);
        // Just verify it produces a valid Value without panicking
        assert!(matches!(val, Value::Scalar(_)));
    }
}

// ── Wave 8: Aliases, COUNT SELECT, HAVING ───────────────────

#[test]
fn alias_propagation() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT id AS source_id, flux AS brightness").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { columns, .. } => {
            assert_eq!(columns, vec!["source_id", "brightness"]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn alias_mixed_with_bare_columns() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT id, flux AS brightness").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { columns, .. } => {
            assert_eq!(columns, vec!["id", "brightness"]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn count_select_all() {
    let mut table = test_table();
    let stmt = crate::taql::parse("COUNT SELECT *").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Count { count } => assert_eq!(count, 10),
        _ => panic!("expected Count"),
    }
}

#[test]
fn count_select_with_where() {
    let mut table = test_table();
    let stmt = crate::taql::parse("COUNT SELECT * WHERE id > 5").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Count { count } => assert_eq!(count, 4),
        _ => panic!("expected Count"),
    }
}

#[test]
fn count_select_empty() {
    let mut table = test_table();
    let stmt = crate::taql::parse("COUNT SELECT * WHERE id > 100").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Count { count } => assert_eq!(count, 0),
        _ => panic!("expected Count"),
    }
}

#[test]
fn having_filters_groups() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("category", PrimitiveType::String),
        ColumnSchema::scalar("val", PrimitiveType::Int32),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    // category A: 3 rows, category B: 1 row, category C: 2 rows
    for (cat, v) in &[("A", 1), ("A", 2), ("A", 3), ("B", 10), ("C", 5), ("C", 6)] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    "category",
                    Value::Scalar(ScalarValue::String(cat.to_string())),
                ),
                RecordField::new("val", Value::Scalar(ScalarValue::Int32(*v))),
            ]))
            .unwrap();
    }

    let stmt =
        crate::taql::parse("SELECT category, COUNT(*) GROUP BY category HAVING COUNT(*) > 1")
            .unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Materialized { table } => {
            // Only A (3 rows) and C (2 rows) pass HAVING COUNT(*) > 1
            assert_eq!(table.row_count(), 2);
        }
        _ => panic!("expected Materialized"),
    }
}

#[test]
fn having_sum_threshold() {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("grp", PrimitiveType::String),
        ColumnSchema::scalar("amount", PrimitiveType::Float64),
    ])
    .unwrap();
    let mut table = Table::with_schema(schema);
    for (g, a) in &[("X", 10.0), ("X", 20.0), ("Y", 1.0), ("Y", 2.0)] {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("grp", Value::Scalar(ScalarValue::String(g.to_string()))),
                RecordField::new("amount", Value::Scalar(ScalarValue::Float64(*a))),
            ]))
            .unwrap();
    }

    let stmt = crate::taql::parse("SELECT grp, SUM(amount) GROUP BY grp HAVING SUM(amount) > 5.0")
        .unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Materialized { table } => {
            // Only X (sum=30) passes; Y (sum=3) does not
            assert_eq!(table.row_count(), 1);
        }
        _ => panic!("expected Materialized"),
    }
}

// ── Wave 9: GIVING, subqueries ──────────────

#[test]
fn giving_clause_parses() {
    let stmt = crate::taql::parse("SELECT * GIVING output_table").unwrap();
    match stmt {
        Statement::Select(s) => {
            let g = s.giving.as_ref().unwrap();
            assert_eq!(g.table_name, "output_table");
            assert!(g.output_type.is_none());
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn giving_clause_with_type() {
    let stmt = crate::taql::parse("SELECT * GIVING output AS MEMORY").unwrap();
    match stmt {
        Statement::Select(s) => {
            let g = s.giving.as_ref().unwrap();
            assert_eq!(g.table_name, "output");
            assert_eq!(g.output_type.as_deref(), Some("MEMORY"));
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn subquery_in_expr_parses() {
    let stmt = crate::taql::parse("SELECT * WHERE id IN (SELECT id WHERE flux > 5.0)").unwrap();
    match stmt {
        Statement::Select(s) => {
            assert!(s.where_clause.is_some());
        }
        _ => panic!("expected Select"),
    }
}

// ── Wave 10: CREATE TABLE, DROP TABLE ───────────────────────

#[test]
fn create_table_basic() {
    let stmt = crate::taql::parse("CREATE TABLE mytab (col1 INT32, col2 FLOAT64)").unwrap();
    match &stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.table_name, "mytab");
            assert_eq!(ct.columns.len(), 2);
            assert_eq!(ct.columns[0].name, "col1");
            assert_eq!(ct.columns[0].data_type, "INT32");
            assert_eq!(ct.columns[1].name, "col2");
            assert_eq!(ct.columns[1].data_type, "FLOAT64");
        }
        _ => panic!("expected CreateTable"),
    }
    // Execute validates types
    let mut table = test_table();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::CreateTable { table_name } => assert_eq!(table_name, "mytab"),
        _ => panic!("expected CreateTable result"),
    }
}

#[test]
fn create_table_invalid_type() {
    let stmt = crate::taql::parse("CREATE TABLE t (col BADTYPE)").unwrap();
    let mut table = test_table();
    let result = execute(&stmt, &mut table);
    assert!(result.is_err());
}

#[test]
fn drop_table_basic() {
    let stmt = crate::taql::parse("DROP TABLE mytab").unwrap();
    match &stmt {
        Statement::DropTable(dt) => assert_eq!(dt.table_name, "mytab"),
        _ => panic!("expected DropTable"),
    }
    let mut table = test_table();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::DropTable { table_name } => assert_eq!(table_name, "mytab"),
        _ => panic!("expected DropTable result"),
    }
}

#[test]
fn create_table_roundtrip() {
    let stmt = crate::taql::parse("CREATE TABLE t (a INT32, b STRING)").unwrap();
    let displayed = stmt.to_string();
    let reparsed = crate::taql::parse(&displayed).unwrap();
    assert!(matches!(reparsed, Statement::CreateTable(_)));
}

// ── Wave 11: JOIN execution ─────────────────────────────────

#[test]
fn inner_join_self() {
    let mut table = test_table();
    // Self-join: every row joins with itself on matching id
    let stmt =
        crate::taql::parse("SELECT * FROM t JOIN t AS t2 ON id = t2.id WHERE id < 3").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            // Each row joins with itself, so rows 0,1,2 all match
            assert_eq!(row_indices, vec![0, 1, 2]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn cross_join() {
    let mut table = test_table();
    let stmt = crate::taql::parse("SELECT * FROM t CROSS JOIN t AS t2 WHERE id < 3").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            // Cross join includes all left rows that pass WHERE (0,1,2)
            assert_eq!(row_indices, vec![0, 1, 2]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn left_join_preserves_unmatched() {
    let mut table = test_table();
    // LEFT JOIN with an ON condition that never matches
    let stmt =
        crate::taql::parse("SELECT * FROM t LEFT JOIN t AS t2 ON id = 999 WHERE id < 3").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    match result {
        TaqlResult::Select { row_indices, .. } => {
            // LEFT JOIN: unmatched rows are still included
            assert_eq!(row_indices, vec![0, 1, 2]);
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn join_parse_roundtrip() {
    let stmt = crate::taql::parse("SELECT * FROM t JOIN t AS t2 ON id = t2.id").unwrap();
    let displayed = stmt.to_string();
    let reparsed = crate::taql::parse(&displayed).unwrap();
    assert!(matches!(reparsed, Statement::Select(_)));
}

#[test]
fn alter_set_keyword_accepts_quantity_and_rejects_non_scalar() {
    let mut table = test_table();
    let stmt = crate::taql::parse("ALTER TABLE SET KEYWORD speed = 1 'km/s'").unwrap();
    let result = execute(&stmt, &mut table).unwrap();
    assert!(matches!(result, TaqlResult::Update { rows_affected: 0 }));
    assert_eq!(
        table.keywords().get("speed"),
        Some(&Value::Scalar(ScalarValue::Float64(1.0)))
    );

    let stmt = crate::taql::parse("ALTER TABLE SET KEYWORD arr = meas.j2000(0, 0)").unwrap();
    assert!(execute(&stmt, &mut table).is_err());
}

#[test]
fn expr_value_materialization_handles_quantities() {
    let quantity = ExprValue::Quantity(casa_core::quanta::Quantity::new(2.5, "rad").unwrap());
    assert!(matches!(
        expr_value_to_literal(&quantity),
        Literal::Quantity { value, ref unit }
            if (value - 2.5).abs() < 1e-12 && unit == "rad"
    ));
    assert_eq!(
        expr_value_to_value_untyped(&quantity),
        Value::Scalar(ScalarValue::Float64(2.5))
    );
}

#[test]
fn parse_data_type_accepts_aliases() {
    assert_eq!(parse_data_type("BOOLEAN").unwrap(), PrimitiveType::Bool);
    assert_eq!(parse_data_type("short").unwrap(), PrimitiveType::Int16);
    assert_eq!(parse_data_type("integer").unwrap(), PrimitiveType::Int32);
    assert_eq!(parse_data_type("long").unwrap(), PrimitiveType::Int64);
    assert_eq!(parse_data_type("float").unwrap(), PrimitiveType::Float32);
    assert_eq!(parse_data_type("double").unwrap(), PrimitiveType::Float64);
    assert_eq!(
        parse_data_type("complex").unwrap(),
        PrimitiveType::Complex32
    );
    assert_eq!(
        parse_data_type("dcomplex").unwrap(),
        PrimitiveType::Complex64
    );
    assert_eq!(parse_data_type("text").unwrap(), PrimitiveType::String);
}
