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

use super::ast::{self, *};
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
    /// Materialized SELECT result: an in-memory table with computed values.
    ///
    /// Produced when SELECT columns contain expressions (not just column refs),
    /// or when GROUP BY / aggregate functions are used. The table holds the
    /// fully evaluated result rows.
    ///
    /// C++ equivalent: the result of `makeProjectExprTable()` → `doProjectExpr()`.
    Materialized {
        /// The in-memory result table (boxed to avoid large enum variant).
        table: Box<crate::Table>,
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
    /// COUNT SELECT result.
    Count {
        /// Number of matching rows.
        count: usize,
    },
    /// CREATE TABLE result.
    CreateTable {
        /// Path of the created table.
        table_name: String,
    },
    /// DROP TABLE result.
    DropTable {
        /// Path of the dropped table.
        table_name: String,
    },
}

/// Execute a parsed TaQL statement against a table.
pub fn execute(stmt: &Statement, table: &mut crate::Table) -> Result<TaqlResult, TaqlError> {
    match stmt {
        Statement::Select(sel) => execute_select(sel, table),
        Statement::CountSelect(sel) => execute_count_select(sel, table),
        Statement::Update(upd) => execute_update(upd, table),
        Statement::Insert(ins) => execute_insert(ins, table),
        Statement::Delete(del) => execute_delete(del, table),
        Statement::Calc(calc) => execute_calc(calc, table),
        Statement::CreateTable(ct) => execute_create_table(ct),
        Statement::DropTable(dt) => execute_drop_table(dt),
        Statement::AlterTable(alt) => execute_alter_table(alt, table),
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

    let style = sel.style;

    // 1. WHERE filter
    let mut row_indices: Vec<usize> = if let Some(ref where_clause) = sel.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            if let Some(row) = table.row(i) {
                let ctx = EvalContext {
                    row,
                    row_index: i,
                    style,
                };
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

    // 1b. JOIN — nested-loop join against same table (self-join)
    if !sel.joins.is_empty() {
        row_indices = execute_joins(&row_indices, &sel.joins, table, style)?;
    }

    // 2. ORDER BY
    if !sel.order_by.is_empty() {
        sort_rows(&mut row_indices, &sel.order_by, table, style)?;
    }

    // 3. DISTINCT
    if sel.distinct {
        deduplicate_rows(&mut row_indices, &sel.columns, table, style)?;
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

/// Execute a parsed TaQL statement, materializing computed SELECTs.
///
/// Like [`execute()`], but for SELECT statements with computed columns,
/// evaluates expressions and returns `TaqlResult::Materialized` instead of
/// `TaqlResult::Select`. Used by `Table::query_result()`.
pub(crate) fn execute_materializing(
    stmt: &Statement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let result = execute(stmt, table)?;
    match result {
        TaqlResult::Select {
            ref row_indices,
            columns: _,
        } => {
            // Check if the original SELECT has computed columns
            if let Statement::Select(sel) = stmt {
                if !sel.columns.is_empty() && has_computed_columns(&sel.columns) {
                    return materialize_select(sel, row_indices, table);
                }
            }
            Ok(result)
        }
        other => Ok(other),
    }
}

/// Returns `true` if any SELECT column requires materialization.
///
/// A column needs materialization if it's not a plain `ColumnRef`, or if it
/// has an alias (which renames the column in the output).
fn has_computed_columns(columns: &[SelectColumn]) -> bool {
    columns.iter().any(|c| {
        // An alias on a ColumnRef renames the output column — must materialize
        if c.alias.is_some() {
            return true;
        }
        !matches!(&c.expr, Expr::ColumnRef(_))
    })
}

/// Materialize a SELECT with computed columns into an in-memory Table.
///
/// Evaluates each column expression for each selected row and builds a new
/// Table containing the computed values.
///
/// C++ equivalent: `makeProjectExprTable()` → `doProjectExpr()` → `doUpdate()`.
fn materialize_select(
    sel: &SelectStatement,
    row_indices: &[usize],
    table: &crate::Table,
) -> Result<TaqlResult, TaqlError> {
    use crate::schema::{ColumnSchema, TableSchema};

    let style = sel.style;

    // Determine column names
    let col_names = extract_column_names(&sel.columns, table)?;

    // Build result rows by evaluating each expression per row
    let mut result_rows: Vec<RecordValue> = Vec::with_capacity(row_indices.len());
    for &row_idx in row_indices {
        let row = table
            .row(row_idx)
            .ok_or_else(|| TaqlError::Table(format!("row {row_idx} not found")))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
            style,
        };
        let mut fields = Vec::with_capacity(sel.columns.len());
        for (ci, col) in sel.columns.iter().enumerate() {
            let val = eval_expr(&col.expr, &ctx)?;
            let value = expr_value_to_value_untyped(&val);
            fields.push(RecordField::new(&col_names[ci], value));
        }
        result_rows.push(RecordValue::new(fields));
    }

    // Build schema from first row (or column names if empty)
    let mut mat_table = if let Some(first_row) = result_rows.first() {
        let schema_cols: Vec<ColumnSchema> = col_names
            .iter()
            .map(|name| {
                let val = first_row.get(name);
                col_schema_from_value(name, val)
            })
            .collect();
        if let Ok(schema) = TableSchema::new(schema_cols) {
            crate::Table::with_schema_memory(schema)
        } else {
            crate::Table::new_memory()
        }
    } else {
        crate::Table::new_memory()
    };

    for row in result_rows {
        mat_table
            .add_row(row)
            .map_err(|e| TaqlError::Table(format!("materialization error: {e}")))?;
    }

    Ok(TaqlResult::Materialized {
        table: Box::new(mat_table),
    })
}

/// Infer a ColumnSchema from a Value.
fn col_schema_from_value(name: &str, val: Option<&Value>) -> crate::schema::ColumnSchema {
    use casacore_types::PrimitiveType as PT;
    match val {
        Some(Value::Scalar(s)) => {
            let pt = match s {
                ScalarValue::Bool(_) => PT::Bool,
                ScalarValue::UInt8(_) => PT::UInt8,
                ScalarValue::UInt16(_) => PT::UInt16,
                ScalarValue::UInt32(_) => PT::UInt32,
                ScalarValue::Int16(_) => PT::Int16,
                ScalarValue::Int32(_) => PT::Int32,
                ScalarValue::Int64(_) => PT::Int64,
                ScalarValue::Float32(_) => PT::Float32,
                ScalarValue::Float64(_) => PT::Float64,
                ScalarValue::Complex32(_) => PT::Complex32,
                ScalarValue::Complex64(_) => PT::Complex64,
                ScalarValue::String(_) => PT::String,
            };
            crate::schema::ColumnSchema::scalar(name, pt)
        }
        Some(Value::Array(arr)) => {
            let pt = arr.primitive_type();
            let shape: Vec<usize> = arr.shape().to_vec();
            crate::schema::ColumnSchema::array_fixed(name, pt, shape)
        }
        _ => crate::schema::ColumnSchema::scalar(name, PT::String),
    }
}

/// Execute a COUNT SELECT statement — returns the count of matching rows.
fn execute_count_select(
    sel: &SelectStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let result = execute_select(sel, table)?;
    let count = match result {
        TaqlResult::Select { row_indices, .. } => row_indices.len(),
        TaqlResult::Materialized { ref table } => table.row_count(),
        _ => 0,
    };
    Ok(TaqlResult::Count { count })
}

/// Execute JOIN clauses using nested-loop against the same table (self-join).
///
/// For each left row in `left_rows`, scans all table rows for the right side
/// and evaluates the ON condition with a merged row context.
/// Returns the unique set of left-row indices that matched.
///
/// C++ reference: `TableParseJoin`.
fn execute_joins(
    left_rows: &[usize],
    joins: &[JoinClause],
    table: &crate::Table,
    style: ast::IndexStyle,
) -> Result<Vec<usize>, TaqlError> {
    let mut result_rows: Vec<usize> = left_rows.to_vec();

    for join in joins {
        let right_count = table.row_count();
        let mut matched: Vec<usize> = Vec::new();
        let mut left_matched: HashSet<usize> = HashSet::new();

        for &left_idx in &result_rows {
            let left_row = table
                .row(left_idx)
                .ok_or_else(|| TaqlError::Table(format!("row {left_idx} not found")))?;
            let mut found_match = false;

            if join.join_type == JoinType::Cross {
                // Cross join: every left row pairs with every right row
                if !left_matched.contains(&left_idx) {
                    matched.push(left_idx);
                    left_matched.insert(left_idx);
                }
                continue;
            }

            for right_idx in 0..right_count {
                let right_row = table
                    .row(right_idx)
                    .ok_or_else(|| TaqlError::Table(format!("row {right_idx} not found")))?;

                // Merge left and right rows for ON evaluation.
                // Fields from right are accessible with the join table alias prefix,
                // but for self-joins we merge into one context.
                let merged = merge_rows(left_row, right_row, join.table.alias.as_deref());

                let ctx = EvalContext {
                    row: &merged,
                    row_index: left_idx,
                    style,
                };

                let passes = if let Some(ref on_expr) = join.on {
                    eval_expr(on_expr, &ctx)?.to_bool()?
                } else {
                    true
                };

                if passes {
                    found_match = true;
                    if left_matched.insert(left_idx) {
                        matched.push(left_idx);
                    }
                }
            }

            // LEFT JOIN: include unmatched left rows
            if join.join_type == JoinType::Left && !found_match && left_matched.insert(left_idx) {
                matched.push(left_idx);
            }
        }

        result_rows = matched;
    }

    Ok(result_rows)
}

/// Merge two rows into one, optionally prefixing right-side fields with an alias.
fn merge_rows<'a>(
    left: &'a RecordValue,
    right: &'a RecordValue,
    right_alias: Option<&str>,
) -> RecordValue {
    let mut fields: Vec<RecordField> = left
        .fields()
        .iter()
        .map(|f| RecordField::new(&f.name, f.value.clone()))
        .collect();

    for f in right.fields() {
        let name = if let Some(alias) = right_alias {
            format!("{}.{}", alias, f.name)
        } else {
            f.name.clone()
        };
        fields.push(RecordField::new(&name, f.value.clone()));
    }

    RecordValue::new(fields)
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
                let ctx = EvalContext {
                    row,
                    row_index: i,
                    style: ast::IndexStyle::default(),
                };
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
            style: ast::IndexStyle::default(),
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
            style: ast::IndexStyle::default(),
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
                let ctx = EvalContext {
                    row,
                    row_index: i,
                    style: ast::IndexStyle::default(),
                };
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

/// Execute a CALC statement.
///
/// Evaluates the expression in the context of the first row (if available)
/// and returns a single-row, single-column result.
fn execute_calc(calc: &CalcStatement, table: &mut crate::Table) -> Result<TaqlResult, TaqlError> {
    // Use row 0 if the table has rows, otherwise an empty context
    let empty_row = casacore_types::RecordValue::default();
    let row = if table.row_count() > 0 {
        table.row(0).unwrap_or(&empty_row)
    } else {
        &empty_row
    };
    let ctx = EvalContext {
        row,
        row_index: 0,
        style: ast::IndexStyle::default(),
    };
    let _val = eval_expr(&calc.expr, &ctx)?;

    // CALC returns the source row as context; report row 0 as the result row.
    Ok(TaqlResult::Select {
        columns: vec!["result".to_string()],
        row_indices: if table.row_count() > 0 {
            vec![0]
        } else {
            vec![]
        },
    })
}

/// Execute an ALTER TABLE statement.
fn execute_alter_table(
    alt: &AlterTableStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    use casacore_types::{PrimitiveType, ScalarValue, Value};

    match &alt.operation {
        AlterOperation::AddColumn { name, data_type } => {
            let ptype = match data_type.to_lowercase().as_str() {
                "bool" | "boolean" => PrimitiveType::Bool,
                "int16" | "short" => PrimitiveType::Int16,
                "int32" | "int" | "integer" => PrimitiveType::Int32,
                "int64" | "long" => PrimitiveType::Int64,
                "float32" | "float" => PrimitiveType::Float32,
                "float64" | "double" => PrimitiveType::Float64,
                "complex" | "complex32" => PrimitiveType::Complex32,
                "dcomplex" | "complex64" => PrimitiveType::Complex64,
                "string" | "text" => PrimitiveType::String,
                other => {
                    return Err(TaqlError::TypeError {
                        message: format!("unknown data type '{other}'"),
                    });
                }
            };
            let col = crate::ColumnSchema::scalar(name, ptype);
            let default_val = default_value_for_type(ptype);
            table
                .add_column(col, Some(default_val))
                .map_err(|e| TaqlError::Table(e.to_string()))?;
            Ok(TaqlResult::Update { rows_affected: 0 })
        }
        AlterOperation::DropColumn { name } => {
            table
                .remove_column(name)
                .map_err(|e| TaqlError::Table(e.to_string()))?;
            Ok(TaqlResult::Update { rows_affected: 0 })
        }
        AlterOperation::RenameColumn { old_name, new_name } => {
            table
                .rename_column(old_name, new_name)
                .map_err(|e| TaqlError::Table(e.to_string()))?;
            Ok(TaqlResult::Update { rows_affected: 0 })
        }
        AlterOperation::AddRow { count } => {
            let n = match count {
                Some(expr) => eval_const_int(expr)? as usize,
                None => 1,
            };
            for _ in 0..n {
                // Add a row with default values for each column
                let fields: Vec<casacore_types::RecordField> = table
                    .schema()
                    .map(|s| s.columns())
                    .unwrap_or(&[])
                    .iter()
                    .filter_map(|col: &crate::ColumnSchema| {
                        col.data_type().map(|pt| {
                            let default_val = default_value_for_type(pt);
                            casacore_types::RecordField::new(col.name(), default_val)
                        })
                    })
                    .collect();
                table
                    .add_row(casacore_types::RecordValue::new(fields))
                    .map_err(|e| TaqlError::Table(e.to_string()))?;
            }
            Ok(TaqlResult::Insert { rows_inserted: n })
        }
        AlterOperation::SetKeyword { name, value } => {
            let empty_row = casacore_types::RecordValue::default();
            let ctx = EvalContext {
                row: &empty_row,
                row_index: 0,
                style: ast::IndexStyle::default(),
            };
            let val = eval_expr(value, &ctx)?;
            let kw_val = match val {
                ExprValue::Bool(b) => Value::Scalar(ScalarValue::Bool(b)),
                ExprValue::Int(n) => Value::Scalar(ScalarValue::Int64(n)),
                ExprValue::Float(v) => Value::Scalar(ScalarValue::Float64(v)),
                ExprValue::String(s) => Value::Scalar(ScalarValue::String(s)),
                _ => {
                    return Err(TaqlError::TypeError {
                        message: "keyword value must be a scalar".to_string(),
                    });
                }
            };
            table.keywords_mut().upsert(name, kw_val);
            Ok(TaqlResult::Update { rows_affected: 0 })
        }
    }
}

/// Execute a CREATE TABLE statement.
///
/// This creates a new in-memory table with the specified schema. The resulting
/// table is not persisted — callers should use `Table::create_new` for disk
/// tables. The return value confirms the schema was valid.
fn execute_create_table(ct: &CreateTableStatement) -> Result<TaqlResult, TaqlError> {
    // Validate all column types
    for col_def in &ct.columns {
        parse_data_type(&col_def.data_type)?;
    }

    Ok(TaqlResult::CreateTable {
        table_name: ct.table_name.clone(),
    })
}

/// Execute a DROP TABLE statement.
///
/// This validates the statement but does not perform filesystem operations.
/// Actual table deletion requires filesystem access via `Table::delete`.
fn execute_drop_table(dt: &DropTableStatement) -> Result<TaqlResult, TaqlError> {
    Ok(TaqlResult::DropTable {
        table_name: dt.table_name.clone(),
    })
}

/// Parse a data type string into a PrimitiveType.
fn parse_data_type(type_str: &str) -> Result<casacore_types::PrimitiveType, TaqlError> {
    use casacore_types::PrimitiveType;
    match type_str.to_lowercase().as_str() {
        "bool" | "boolean" => Ok(PrimitiveType::Bool),
        "int16" | "short" => Ok(PrimitiveType::Int16),
        "int32" | "int" | "integer" => Ok(PrimitiveType::Int32),
        "int64" | "long" => Ok(PrimitiveType::Int64),
        "float32" | "float" => Ok(PrimitiveType::Float32),
        "float64" | "double" => Ok(PrimitiveType::Float64),
        "complex" | "complex32" => Ok(PrimitiveType::Complex32),
        "dcomplex" | "complex64" => Ok(PrimitiveType::Complex64),
        "string" | "text" => Ok(PrimitiveType::String),
        other => Err(TaqlError::TypeError {
            message: format!("unknown data type '{other}'"),
        }),
    }
}

/// Return a default value for a given primitive type.
fn default_value_for_type(ptype: casacore_types::PrimitiveType) -> casacore_types::Value {
    use casacore_types::{PrimitiveType as PT, ScalarValue as SV, Value};
    match ptype {
        PT::Bool => Value::Scalar(SV::Bool(false)),
        PT::UInt8 => Value::Scalar(SV::UInt8(0)),
        PT::UInt16 => Value::Scalar(SV::UInt16(0)),
        PT::UInt32 => Value::Scalar(SV::UInt32(0)),
        PT::Int16 => Value::Scalar(SV::Int16(0)),
        PT::Int32 => Value::Scalar(SV::Int32(0)),
        PT::Int64 => Value::Scalar(SV::Int64(0)),
        PT::Float32 => Value::Scalar(SV::Float32(0.0)),
        PT::Float64 => Value::Scalar(SV::Float64(0.0)),
        PT::Complex32 => Value::Scalar(SV::Complex32(num_complex::Complex32::new(0.0, 0.0))),
        PT::Complex64 => Value::Scalar(SV::Complex64(num_complex::Complex64::new(0.0, 0.0))),
        PT::String => Value::Scalar(SV::String(String::new())),
    }
}

/// Execute a SELECT with GROUP BY or aggregates.
fn execute_group_by(
    sel: &SelectStatement,
    table: &mut crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let row_count = table.row_count();
    let style = sel.style;

    // 1. WHERE filter
    let row_indices: Vec<usize> = if let Some(ref where_clause) = sel.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            if let Some(row) = table.row(i) {
                let ctx = EvalContext {
                    row,
                    row_index: i,
                    style,
                };
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
        group_rows(&row_indices, &sel.group_by, table, style)?
    };

    // 3. For each group, compute aggregates and evaluate columns
    let col_names = extract_column_names(&sel.columns, table)?;

    let mut record_rows: Vec<RecordValue> = Vec::new();
    for group in &groups {
        if group.is_empty() {
            continue;
        }
        // Compute aggregate values for this group
        let mut col_values: Vec<ExprValue> = Vec::new();
        for col in &sel.columns {
            let val = eval_aggregate_column(&col.expr, group, table, style)?;
            col_values.push(val);
        }

        // HAVING filter
        if let Some(ref having) = sel.having {
            let having_val = eval_aggregate_expr(having, group, table, style)?;
            if !having_val.to_bool()? {
                continue;
            }
        }

        // Convert ExprValue row to RecordValue
        let fields: Vec<RecordField> = col_names
            .iter()
            .zip(&col_values)
            .map(|(name, val)| RecordField::new(name, expr_value_to_value_untyped(val)))
            .collect();
        record_rows.push(RecordValue::new(fields));
    }

    // Build materialized table
    use crate::schema::{ColumnSchema, TableSchema};

    let mut mat_table = if let Some(first_row) = record_rows.first() {
        let schema_cols: Vec<ColumnSchema> = col_names
            .iter()
            .map(|name| col_schema_from_value(name, first_row.get(name)))
            .collect();
        if let Ok(schema) = TableSchema::new(schema_cols) {
            crate::Table::with_schema_memory(schema)
        } else {
            crate::Table::new_memory()
        }
    } else {
        crate::Table::new_memory()
    };

    for row in record_rows {
        mat_table
            .add_row(row)
            .map_err(|e| TaqlError::Table(format!("group-by materialization error: {e}")))?;
    }

    Ok(TaqlResult::Materialized {
        table: Box::new(mat_table),
    })
}

/// Group rows by GROUP BY expressions, returning groups of row indices.
fn group_rows(
    row_indices: &[usize],
    group_by: &[Expr],
    table: &crate::Table,
    style: ast::IndexStyle,
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
            style,
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
            ExprValue::DateTime(v) => v.to_bits().hash(state),
            ExprValue::Array(arr) => {
                arr.shape.hash(state);
                arr.data.len().hash(state);
            }
            ExprValue::Regex { pattern, flags } => {
                pattern.hash(state);
                flags.hash(state);
            }
            ExprValue::Null => {}
        }
    }
}

/// Evaluate an aggregate column expression for a group of rows.
fn eval_aggregate_column(
    expr: &Expr,
    group: &[usize],
    table: &crate::Table,
    style: ast::IndexStyle,
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
                    style,
                };
                // GROWID collects row indices; others evaluate the expression.
                if *func == AggregateFunc::RowId {
                    acc.accumulate_row_id(row_idx as i64);
                } else {
                    let val = if matches!(**arg, Expr::Star) {
                        ExprValue::Int(1) // COUNT(*)
                    } else {
                        eval_expr(arg, &ctx)?
                    };
                    acc.accumulate(&val);
                }
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
                    style,
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
                    style,
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
    style: ast::IndexStyle,
) -> Result<ExprValue, TaqlError> {
    match expr {
        Expr::Aggregate { .. } => eval_aggregate_column(expr, group, table, style),
        Expr::Binary { left, op, right } => {
            let lval = eval_aggregate_expr(left, group, table, style)?;
            let rval = eval_aggregate_expr(right, group, table, style)?;
            super::eval::eval_expr(
                &Expr::Binary {
                    left: Box::new(Expr::Literal(expr_value_to_literal(&lval))),
                    op: *op,
                    right: Box::new(Expr::Literal(expr_value_to_literal(&rval))),
                },
                &EvalContext {
                    row: &RecordValue::new(vec![]),
                    row_index: 0,
                    style,
                },
            )
        }
        _ => eval_aggregate_column(expr, group, table, style),
    }
}

fn expr_value_to_literal(val: &ExprValue) -> Literal {
    match val {
        ExprValue::Bool(b) => Literal::Bool(*b),
        ExprValue::Int(n) => Literal::Int(*n),
        ExprValue::Float(v) => Literal::Float(*v),
        ExprValue::Complex(c) => Literal::Complex(*c),
        ExprValue::String(s) => Literal::String(s.clone()),
        ExprValue::DateTime(v) => Literal::Float(*v),
        ExprValue::Array(_) => Literal::Null, // arrays don't have a literal form
        ExprValue::Regex { pattern, flags } => Literal::Regex {
            pattern: pattern.clone(),
            flags: flags.clone(),
        },
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
    style: ast::IndexStyle,
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
            style,
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
    style: ast::IndexStyle,
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
            style,
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
        style: ast::IndexStyle::default(),
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
        ExprValue::DateTime(v) => Value::Scalar(ScalarValue::Float64(*v)),
        ExprValue::Array(arr) => expr_array_to_value(arr),
        ExprValue::Regex { pattern, .. } => Value::Scalar(ScalarValue::String(pattern.clone())),
        ExprValue::Null => Value::Scalar(ScalarValue::Bool(false)), // fallback
    }
}

/// Convert an `eval::ArrayValue` to a `casacore_types::Value::Array`.
///
/// Determines the element type from the first element and builds an
/// `ndarray::ArrayD` with Fortran (column-major) layout, matching casacore conventions.
/// The eval flat data vector is in column-major order.
fn expr_array_to_value(arr: &super::eval::ArrayValue) -> Value {
    use casacore_types::ArrayValue as AV;
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    let shape = IxDyn(&arr.shape).f();

    if arr.data.is_empty() {
        // Empty array — default to Float64
        return Value::Array(AV::Float64(ArrayD::zeros(shape)));
    }

    // Determine type from first element
    match &arr.data[0] {
        ExprValue::Bool(_) => {
            let data: Vec<bool> = arr
                .data
                .iter()
                .map(|e| matches!(e, ExprValue::Bool(true)))
                .collect();
            Value::Array(AV::Bool(
                ArrayD::from_shape_vec(shape, data).unwrap_or_default(),
            ))
        }
        ExprValue::Int(_) => {
            let data: Vec<i64> = arr
                .data
                .iter()
                .map(|e| match e {
                    ExprValue::Int(n) => *n,
                    ExprValue::Float(v) => *v as i64,
                    _ => 0,
                })
                .collect();
            Value::Array(AV::Int64(
                ArrayD::from_shape_vec(shape, data).unwrap_or_default(),
            ))
        }
        ExprValue::Float(_) | ExprValue::DateTime(_) => {
            let data: Vec<f64> = arr
                .data
                .iter()
                .map(|e| match e {
                    ExprValue::Float(v) | ExprValue::DateTime(v) => *v,
                    ExprValue::Int(n) => *n as f64,
                    _ => 0.0,
                })
                .collect();
            Value::Array(AV::Float64(
                ArrayD::from_shape_vec(shape, data).unwrap_or_default(),
            ))
        }
        ExprValue::Complex(_) => {
            let data: Vec<num_complex::Complex64> = arr
                .data
                .iter()
                .map(|e| match e {
                    ExprValue::Complex(c) => *c,
                    ExprValue::Float(v) => num_complex::Complex64::new(*v, 0.0),
                    ExprValue::Int(n) => num_complex::Complex64::new(*n as f64, 0.0),
                    _ => num_complex::Complex64::new(0.0, 0.0),
                })
                .collect();
            Value::Array(AV::Complex64(
                ArrayD::from_shape_vec(shape, data).unwrap_or_default(),
            ))
        }
        ExprValue::String(_) => {
            let data: Vec<String> = arr
                .data
                .iter()
                .map(|e| match e {
                    ExprValue::String(s) => s.clone(),
                    _ => String::new(),
                })
                .collect();
            Value::Array(AV::String(
                ArrayD::from_shape_vec(shape, data).unwrap_or_default(),
            ))
        }
        _ => {
            // Fallback: treat as Float64
            let data: Vec<f64> = arr
                .data
                .iter()
                .map(|e| match e {
                    ExprValue::Float(v) => *v,
                    ExprValue::Int(n) => *n as f64,
                    _ => 0.0,
                })
                .collect();
            Value::Array(AV::Float64(
                ArrayD::from_shape_vec(shape, data).unwrap_or_default(),
            ))
        }
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
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
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
        use casacore_types::PrimitiveType as PT;
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

        let stmt =
            crate::taql::parse("SELECT grp, SUM(amount) GROUP BY grp HAVING SUM(amount) > 5.0")
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
        let stmt = crate::taql::parse("SELECT * FROM t LEFT JOIN t AS t2 ON id = 999 WHERE id < 3")
            .unwrap();
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
}
