// SPDX-License-Identifier: LGPL-3.0-or-later
//! Query execution engine for TaQL.
//!
//! Executes parsed [`Statement`] nodes against
//! a [`Table`](crate::Table), producing [`TaqlResult`] values.
//!
//! The SELECT pipeline: WHERE filter → ORDER BY sort → DISTINCT → OFFSET → LIMIT → projection.
//!
//! # C++ reference
//!
//! `TaQLNode.cc` — `TaQLNode::process()`.

use std::collections::HashSet;

use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

use super::ast::*;
use super::error::TaqlError;
use super::eval::{EvalContext, ExprValue, eval_expr};

/// The result of executing a TaQL statement.
#[derive(Debug)]
pub enum TaqlResult {
    /// SELECT result: row indices and column names for building a RefTable.
    Select {
        /// Matching row indices in the source table.
        row_indices: Vec<usize>,
        /// Column names to include (empty = all columns).
        columns: Vec<String>,
    },
    /// Aggregate SELECT result (GROUP BY or aggregate functions).
    Aggregate {
        /// Number of result groups/rows.
        row_count: usize,
    },
    /// UPDATE result.
    Update {
        /// Number of rows modified.
        rows_affected: usize,
    },
    /// INSERT result.
    Insert {
        /// Number of rows inserted.
        rows_inserted: usize,
    },
    /// DELETE result.
    Delete {
        /// Number of rows deleted.
        rows_deleted: usize,
    },
}

/// Execute a parsed TaQL statement against a table.
pub fn execute(stmt: &Statement, table: &mut crate::Table) -> Result<TaqlResult, TaqlError> {
    match stmt {
        Statement::Select(sel) => execute_select(sel, table),
        Statement::Update(upd) => execute_update(upd, table),
        Statement::Insert(ins) => execute_insert(ins, table),
        Statement::Delete(del) => execute_delete(del, table),
    }
}

/// Execute a SELECT statement.
fn execute_select(
    sel: &SelectStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    // Check for GROUP BY / aggregates
    if !sel.group_by.is_empty() || has_aggregates_in_columns(&sel.columns) {
        return execute_group_by(sel, table);
    }

    let row_count = table.row_count();

    // 1. WHERE filter
    let mut row_indices: Vec<usize> = if let Some(ref where_clause) = sel.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            if let Some(row) = table.row(i) {
                let ctx = EvalContext { row, row_index: i };
                let val = eval_expr(where_clause, &ctx)?;
                if val.to_bool()? {
                    indices.push(i);
                }
            }
        }
        indices
    } else {
        (0..row_count).collect()
    };

    // 2. ORDER BY
    if !sel.order_by.is_empty() {
        sort_rows(&mut row_indices, &sel.order_by, table)?;
    }

    // 3. DISTINCT
    if sel.distinct {
        deduplicate_rows(&mut row_indices, &sel.columns, table)?;
    }

    // 4. OFFSET
    if let Some(ref offset_expr) = sel.offset {
        let offset = eval_const_int(offset_expr)? as usize;
        if offset < row_indices.len() {
            row_indices = row_indices[offset..].to_vec();
        } else {
            row_indices.clear();
        }
    }

    // 5. LIMIT
    if let Some(ref limit_expr) = sel.limit {
        let limit = eval_const_int(limit_expr)? as usize;
        row_indices.truncate(limit);
    }

    // 6. Column projection
    let columns = extract_column_names(&sel.columns, table)?;

    Ok(TaqlResult::Select {
        row_indices,
        columns,
    })
}

/// Execute an UPDATE statement.
fn execute_update(
    upd: &UpdateStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let row_count = table.row_count();

    // Find matching rows
    let mut matching_rows: Vec<usize> = if let Some(ref where_clause) = upd.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            if let Some(row) = table.row(i) {
                let ctx = EvalContext { row, row_index: i };
                let val = eval_expr(where_clause, &ctx)?;
                if val.to_bool()? {
                    indices.push(i);
                }
            }
        }
        indices
    } else {
        (0..row_count).collect()
    };

    // Apply LIMIT
    if let Some(ref limit_expr) = upd.limit {
        let limit = eval_const_int(limit_expr)? as usize;
        matching_rows.truncate(limit);
    }

    // Evaluate all RHS expressions before applying (correct UPDATE semantics)
    let mut updates: Vec<Vec<(String, Value)>> = Vec::with_capacity(matching_rows.len());
    for &row_idx in &matching_rows {
        let row = table
            .row(row_idx)
            .ok_or_else(|| TaqlError::Table(format!("row {row_idx} disappeared during UPDATE")))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
        };
        let mut row_updates = Vec::with_capacity(upd.assignments.len());
        for assignment in &upd.assignments {
            let val = eval_expr(&assignment.value, &ctx)?;
            let value = expr_value_to_table_value(&val, table, &assignment.column)?;
            row_updates.push((assignment.column.clone(), value));
        }
        updates.push(row_updates);
    }

    // Apply updates
    let count = matching_rows.len();
    for (row_idx, row_updates) in matching_rows.into_iter().zip(updates) {
        for (column, value) in row_updates {
            table
                .set_cell(row_idx, &column, value)
                .map_err(|e| TaqlError::Table(e.to_string()))?;
        }
    }

    Ok(TaqlResult::Update {
        rows_affected: count,
    })
}

/// Execute an INSERT statement.
fn execute_insert(
    ins: &InsertStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let schema_columns: Vec<String> = if !ins.columns.is_empty() {
        ins.columns.clone()
    } else {
        // Use all columns from schema
        table
            .schema()
            .map(|s| s.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default()
    };

    let mut count = 0;
    for value_row in &ins.values {
        if !ins.columns.is_empty() && value_row.len() != ins.columns.len() {
            return Err(TaqlError::InsertColumnMismatch {
                columns: ins.columns.len(),
                values: value_row.len(),
            });
        }

        // Build a RecordValue for the row
        let empty_row = RecordValue::new(vec![]);
        let ctx = EvalContext {
            row: &empty_row,
            row_index: 0,
        };

        let mut fields = Vec::new();
        for (i, expr) in value_row.iter().enumerate() {
            let val = eval_expr(expr, &ctx)?;
            let col_name = schema_columns
                .get(i)
                .ok_or(TaqlError::InsertColumnMismatch {
                    columns: schema_columns.len(),
                    values: value_row.len(),
                })?;
            let value = expr_value_to_table_value(&val, table, col_name)?;
            fields.push(RecordField::new(col_name.as_str(), value));
        }

        table
            .add_row(RecordValue::new(fields))
            .map_err(|e| TaqlError::Table(e.to_string()))?;
        count += 1;
    }

    Ok(TaqlResult::Insert {
        rows_inserted: count,
    })
}

/// Execute a DELETE statement.
fn execute_delete(
    del: &DeleteStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let row_count = table.row_count();

    // Find matching rows
    let mut matching_rows: Vec<usize> = if let Some(ref where_clause) = del.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            if let Some(row) = table.row(i) {
                let ctx = EvalContext { row, row_index: i };
                let val = eval_expr(where_clause, &ctx)?;
                if val.to_bool()? {
                    indices.push(i);
                }
            }
        }
        indices
    } else {
        (0..row_count).collect()
    };

    // Apply LIMIT
    if let Some(ref limit_expr) = del.limit {
        let limit = eval_const_int(limit_expr)? as usize;
        matching_rows.truncate(limit);
    }

    // Sort and deduplicate (remove_rows requires sorted, unique indices)
    matching_rows.sort_unstable();
    matching_rows.dedup();

    let count = matching_rows.len();
    table
        .remove_rows(&matching_rows)
        .map_err(|e| TaqlError::Table(e.to_string()))?;

    Ok(TaqlResult::Delete {
        rows_deleted: count,
    })
}

/// Execute a SELECT with GROUP BY or aggregates.
fn execute_group_by(
    sel: &SelectStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let row_count = table.row_count();

    // 1. WHERE filter
    let row_indices: Vec<usize> = if let Some(ref where_clause) = sel.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            if let Some(row) = table.row(i) {
                let ctx = EvalContext { row, row_index: i };
                let val = eval_expr(where_clause, &ctx)?;
                if val.to_bool()? {
                    indices.push(i);
                }
            }
        }
        indices
    } else {
        (0..row_count).collect()
    };

    // 2. Group rows by GROUP BY keys
    let groups = if sel.group_by.is_empty() {
        // No GROUP BY: entire result set is one group
        vec![row_indices]
    } else {
        group_rows(&row_indices, &sel.group_by, table)?
    };

    // 3. For each group, compute aggregates and evaluate columns
    // Build result rows
    let mut result_rows: Vec<Vec<ExprValue>> = Vec::new();
    for group in &groups {
        if group.is_empty() {
            continue;
        }
        // Compute aggregate values for this group
        let mut col_values: Vec<ExprValue> = Vec::new();
        for col in &sel.columns {
            let val = eval_aggregate_column(&col.expr, group, table)?;
            col_values.push(val);
        }

        // HAVING filter
        if let Some(ref having) = sel.having {
            let having_val = eval_aggregate_expr(having, group, table)?;
            if !having_val.to_bool()? {
                continue;
            }
        }

        result_rows.push(col_values);
    }

    Ok(TaqlResult::Aggregate {
        row_count: result_rows.len(),
    })
}

/// Group rows by GROUP BY expressions, returning groups of row indices.
fn group_rows(
    row_indices: &[usize],
    group_by: &[Expr],
    table: &crate::Table,
) -> Result<Vec<Vec<usize>>, TaqlError> {
    use std::collections::HashMap;

    let mut groups: HashMap<GroupKey, Vec<usize>> = HashMap::new();
    let mut key_order: Vec<GroupKey> = Vec::new();

    for &row_idx in row_indices {
        let row = table
            .row(row_idx)
            .ok_or_else(|| TaqlError::Table(format!("row {row_idx} not found")))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
        };
        let key = GroupKey(
            group_by
                .iter()
                .map(|expr| eval_expr(expr, &ctx).map(ExprValueKey))
                .collect::<Result<_, _>>()?,
        );

        if !groups.contains_key(&key) {
            key_order.push(key.clone());
        }
        groups.entry(key).or_default().push(row_idx);
    }

    // Return groups in insertion order
    Ok(key_order
        .into_iter()
        .filter_map(|k| groups.remove(&k))
        .collect())
}

/// A wrapper around ExprValue that implements Hash+Eq for use as HashMap keys.
#[derive(Debug, Clone)]
struct ExprValueKey(ExprValue);

impl PartialEq for ExprValueKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (ExprValue::Bool(a), ExprValue::Bool(b)) => a == b,
            (ExprValue::Int(a), ExprValue::Int(b)) => a == b,
            (ExprValue::Float(a), ExprValue::Float(b)) => a.to_bits() == b.to_bits(),
            (ExprValue::Complex(a), ExprValue::Complex(b)) => {
                a.re.to_bits() == b.re.to_bits() && a.im.to_bits() == b.im.to_bits()
            }
            (ExprValue::String(a), ExprValue::String(b)) => a == b,
            (ExprValue::Null, ExprValue::Null) => true,
            _ => false,
        }
    }
}

impl Eq for ExprValueKey {}

impl std::hash::Hash for ExprValueKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            ExprValue::Bool(b) => b.hash(state),
            ExprValue::Int(n) => n.hash(state),
            ExprValue::Float(v) => v.to_bits().hash(state),
            ExprValue::Complex(c) => {
                c.re.to_bits().hash(state);
                c.im.to_bits().hash(state);
            }
            ExprValue::String(s) => s.hash(state),
            ExprValue::Null => {}
        }
    }
}

/// Evaluate an aggregate column expression for a group of rows.
fn eval_aggregate_column(
    expr: &Expr,
    group: &[usize],
    table: &crate::Table,
) -> Result<ExprValue, TaqlError> {
    match expr {
        Expr::Aggregate { func, arg } => {
            use super::aggregate::Accumulator;
            let mut acc = Accumulator::new(*func);
            for &row_idx in group {
                let row = table
                    .row(row_idx)
                    .ok_or_else(|| TaqlError::Table(format!("row {row_idx} not found")))?;
                let ctx = EvalContext {
                    row,
                    row_index: row_idx,
                };
                let val = if matches!(**arg, Expr::Star) {
                    ExprValue::Int(1) // COUNT(*)
                } else {
                    eval_expr(arg, &ctx)?
                };
                acc.accumulate(&val);
            }
            Ok(acc.finish())
        }
        Expr::ColumnRef(_) => {
            // For non-aggregate columns in GROUP BY, use the first row's value
            if let Some(&first_row) = group.first() {
                let row = table
                    .row(first_row)
                    .ok_or_else(|| TaqlError::Table(format!("row {first_row} not found")))?;
                let ctx = EvalContext {
                    row,
                    row_index: first_row,
                };
                eval_expr(expr, &ctx)
            } else {
                Ok(ExprValue::Null)
            }
        }
        _ => {
            // For expression columns, try evaluating with first row
            if let Some(&first_row) = group.first() {
                let row = table
                    .row(first_row)
                    .ok_or_else(|| TaqlError::Table(format!("row {first_row} not found")))?;
                let ctx = EvalContext {
                    row,
                    row_index: first_row,
                };
                eval_expr(expr, &ctx)
            } else {
                Ok(ExprValue::Null)
            }
        }
    }
}

/// Evaluate an aggregate expression (used for HAVING).
fn eval_aggregate_expr(
    expr: &Expr,
    group: &[usize],
    table: &crate::Table,
) -> Result<ExprValue, TaqlError> {
    match expr {
        Expr::Aggregate { .. } => eval_aggregate_column(expr, group, table),
        Expr::Binary { left, op, right } => {
            let lval = eval_aggregate_expr(left, group, table)?;
            let rval = eval_aggregate_expr(right, group, table)?;
            super::eval::eval_expr(
                &Expr::Binary {
                    left: Box::new(Expr::Literal(expr_value_to_literal(&lval))),
                    op: *op,
                    right: Box::new(Expr::Literal(expr_value_to_literal(&rval))),
                },
                &EvalContext {
                    row: &RecordValue::new(vec![]),
                    row_index: 0,
                },
            )
        }
        _ => eval_aggregate_column(expr, group, table),
    }
}

fn expr_value_to_literal(val: &ExprValue) -> Literal {
    match val {
        ExprValue::Bool(b) => Literal::Bool(*b),
        ExprValue::Int(n) => Literal::Int(*n),
        ExprValue::Float(v) => Literal::Float(*v),
        ExprValue::Complex(c) => Literal::Complex(*c),
        ExprValue::String(s) => Literal::String(s.clone()),
        ExprValue::Null => Literal::Null,
    }
}

// ── Helper functions ──

/// Check if any column in the SELECT list contains aggregate functions.
fn has_aggregates_in_columns(columns: &[SelectColumn]) -> bool {
    columns.iter().any(|c| expr_has_aggregate(&c.expr))
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate { .. } => true,
        Expr::Binary { left, right, .. } => expr_has_aggregate(left) || expr_has_aggregate(right),
        Expr::Unary { operand, .. } => expr_has_aggregate(operand),
        Expr::FunctionCall { args, .. } => args.iter().any(expr_has_aggregate),
        _ => false,
    }
}

/// Sort row indices by ORDER BY expressions.
fn sort_rows(
    row_indices: &mut [usize],
    order_by: &[OrderBySpec],
    table: &crate::Table,
) -> Result<(), TaqlError> {
    // Pre-evaluate all sort keys for all rows to avoid repeated evaluation.
    let mut sort_keys: Vec<Vec<ExprValue>> = Vec::with_capacity(row_indices.len());
    for &row_idx in row_indices.iter() {
        let row = table
            .row(row_idx)
            .ok_or_else(|| TaqlError::Table(format!("row {row_idx} not found during sort")))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
        };
        let keys: Vec<ExprValue> = order_by
            .iter()
            .map(|spec| eval_expr(&spec.expr, &ctx))
            .collect::<Result<_, _>>()?;
        sort_keys.push(keys);
    }

    // Build index array and sort
    let mut indices: Vec<usize> = (0..row_indices.len()).collect();
    indices.sort_by(|&a, &b| {
        for (i, spec) in order_by.iter().enumerate() {
            let cmp = sort_keys[a][i]
                .compare(&sort_keys[b][i])
                .unwrap_or(std::cmp::Ordering::Equal);
            let cmp = if spec.ascending { cmp } else { cmp.reverse() };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    // Reorder row_indices according to sorted order
    let original: Vec<usize> = row_indices.to_vec();
    for (i, &sorted_idx) in indices.iter().enumerate() {
        row_indices[i] = original[sorted_idx];
    }
    Ok(())
}

/// Remove duplicate rows based on projected column values.
fn deduplicate_rows(
    row_indices: &mut Vec<usize>,
    columns: &[SelectColumn],
    table: &crate::Table,
) -> Result<(), TaqlError> {
    let mut seen: HashSet<GroupKey> = HashSet::new();
    let mut deduped = Vec::with_capacity(row_indices.len());

    for &row_idx in row_indices.iter() {
        let row = table
            .row(row_idx)
            .ok_or_else(|| TaqlError::Table(format!("row {row_idx} not found during DISTINCT")))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
        };

        let key = GroupKey(if columns.is_empty() {
            // SELECT DISTINCT * — use all columns
            row.fields()
                .iter()
                .map(|f| ExprValueKey(ExprValue::from(&f.value)))
                .collect()
        } else {
            columns
                .iter()
                .map(|c| eval_expr(&c.expr, &ctx).map(ExprValueKey))
                .collect::<Result<_, _>>()?
        });

        if seen.insert(key) {
            deduped.push(row_idx);
        }
    }

    *row_indices = deduped;
    Ok(())
}

/// A hashable key made from a vector of ExprValueKey values.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GroupKey(Vec<ExprValueKey>);

impl std::hash::Hash for GroupKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.len().hash(state);
        for k in &self.0 {
            k.hash(state);
        }
    }
}

/// Extract column names from SELECT column list.
fn extract_column_names(
    columns: &[SelectColumn],
    table: &crate::Table,
) -> Result<Vec<String>, TaqlError> {
    if columns.is_empty() {
        // SELECT * — return empty to signal "all columns"
        return Ok(vec![]);
    }
    let mut names = Vec::new();
    for col in columns {
        let name = if let Some(ref alias) = col.alias {
            alias.clone()
        } else {
            match &col.expr {
                Expr::ColumnRef(cr) => cr.column.clone(),
                _ => col.expr.to_string(),
            }
        };
        names.push(name);
    }
    // Verify all simple column refs exist in schema
    if let Some(schema) = table.schema() {
        for col in columns {
            if let Expr::ColumnRef(cr) = &col.expr {
                if cr.table.is_none() && !schema.columns().iter().any(|c| c.name() == cr.column) {
                    return Err(TaqlError::ColumnNotFound {
                        name: cr.column.clone(),
                    });
                }
            }
        }
    }
    Ok(names)
}

/// Evaluate a constant integer expression (for LIMIT/OFFSET).
fn eval_const_int(expr: &Expr) -> Result<i64, TaqlError> {
    let empty = RecordValue::new(vec![]);
    let ctx = EvalContext {
        row: &empty,
        row_index: 0,
    };
    let val = eval_expr(expr, &ctx)?;
    val.to_int()
}

/// Convert an ExprValue back to a table Value, matching the column's schema type.
fn expr_value_to_table_value(
    val: &ExprValue,
    table: &crate::Table,
    column: &str,
) -> Result<Value, TaqlError> {
    // Try to match schema type
    if let Some(schema) = table.schema() {
        if let Some(col_schema) = schema.columns().iter().find(|c| c.name() == column) {
            use casacore_types::PrimitiveType;
            if let Some(ptype) = col_schema.data_type() {
                return Ok(Value::Scalar(match (ptype, val) {
                    (PrimitiveType::Bool, ExprValue::Bool(b)) => ScalarValue::Bool(*b),
                    (PrimitiveType::Int32, ExprValue::Int(n)) => ScalarValue::Int32(*n as i32),
                    (PrimitiveType::Int64, ExprValue::Int(n)) => ScalarValue::Int64(*n),
                    (PrimitiveType::Float32, ExprValue::Float(v)) => {
                        ScalarValue::Float32(*v as f32)
                    }
                    (PrimitiveType::Float64, ExprValue::Float(v)) => ScalarValue::Float64(*v),
                    (PrimitiveType::Float64, ExprValue::Int(n)) => ScalarValue::Float64(*n as f64),
                    (PrimitiveType::String, ExprValue::String(s)) => ScalarValue::String(s.clone()),
                    (PrimitiveType::Int32, ExprValue::Float(v)) => ScalarValue::Int32(*v as i32),
                    (PrimitiveType::Int64, ExprValue::Float(v)) => ScalarValue::Int64(*v as i64),
                    _ => return Ok(expr_value_to_value_untyped(val)),
                }));
            }
        }
    }
    Ok(expr_value_to_value_untyped(val))
}

/// Convert an ExprValue to a Value without schema type information.
fn expr_value_to_value_untyped(val: &ExprValue) -> Value {
    match val {
        ExprValue::Bool(b) => Value::Scalar(ScalarValue::Bool(*b)),
        ExprValue::Int(n) => Value::Scalar(ScalarValue::Int64(*n)),
        ExprValue::Float(v) => Value::Scalar(ScalarValue::Float64(*v)),
        ExprValue::Complex(c) => Value::Scalar(ScalarValue::Complex64(*c)),
        ExprValue::String(s) => Value::Scalar(ScalarValue::String(s.clone())),
        ExprValue::Null => Value::Scalar(ScalarValue::Bool(false)), // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Table;
    use crate::schema::{ColumnSchema, TableSchema};
    use casacore_types::PrimitiveType;

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
        let val = table.cell(5, "flux").unwrap();
        assert_eq!(val, &Value::Scalar(ScalarValue::Float64(99.0)));
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
        let val = table.cell(3, "flux").unwrap();
        assert_eq!(val, &Value::Scalar(ScalarValue::Float64(9.0))); // 3 * 1.5 * 2.0
    }

    #[test]
    fn insert_basic() {
        let mut table = test_table();
        let stmt =
            crate::taql::parse("INSERT INTO (id, name, flux) VALUES (10, 'new', 42.0)").unwrap();
        let result = execute(&stmt, &mut table).unwrap();
        match result {
            TaqlResult::Insert { rows_inserted } => {
                assert_eq!(rows_inserted, 1);
            }
            _ => panic!("expected Insert"),
        }
        assert_eq!(table.row_count(), 11);
        let val = table.cell(10, "id").unwrap();
        assert_eq!(val, &Value::Scalar(ScalarValue::Int32(10)));
    }

    #[test]
    fn insert_multiple() {
        let mut table = test_table();
        let stmt = crate::taql::parse(
            "INSERT INTO (id, name, flux) VALUES (10, 'a', 1.0), (11, 'b', 2.0)",
        )
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
}
