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

use casa_core::{RecordField, RecordValue, ScalarValue, Value};

use super::ast::{self, *};
use super::error::TaqlError;
use super::eval::{EvalContext, ExprValue, eval_expr};

mod mutate;

use mutate::*;

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

/// Extended execution outcome carrying modified/inserted row indices.
#[derive(Debug)]
pub struct TaqlExecution {
    /// Primary statement result.
    pub result: TaqlResult,
    /// Affected row indices in the target table (`UPDATE`, `INSERT`).
    pub affected_rows: Vec<usize>,
}

/// Execute a parsed TaQL statement against a mutable table, returning both
/// the [`TaqlResult`] and any affected row indices.
pub fn execute_with_rows(
    stmt: &Statement,
    table: &mut crate::Table,
) -> Result<TaqlExecution, TaqlError> {
    match stmt {
        Statement::Select(sel) => Ok(TaqlExecution {
            result: execute_select(sel, table)?,
            affected_rows: Vec::new(),
        }),
        Statement::CountSelect(sel) => Ok(TaqlExecution {
            result: execute_count_select(sel, table)?,
            affected_rows: Vec::new(),
        }),
        Statement::Count(cnt) => Ok(TaqlExecution {
            result: execute_count(cnt, table)?,
            affected_rows: Vec::new(),
        }),
        Statement::Update(upd) => execute_update_with_rows(upd, table),
        Statement::Insert(ins) => execute_insert_with_rows(ins, table),
        Statement::Delete(del) => Ok(TaqlExecution {
            result: execute_delete(del, table)?,
            affected_rows: Vec::new(),
        }),
        Statement::Calc(calc) => Ok(TaqlExecution {
            result: execute_calc(calc, table)?,
            affected_rows: Vec::new(),
        }),
        Statement::CreateTable(ct) => Ok(TaqlExecution {
            result: execute_create_table(ct)?,
            affected_rows: Vec::new(),
        }),
        Statement::DropTable(dt) => Ok(TaqlExecution {
            result: execute_drop_table(dt)?,
            affected_rows: Vec::new(),
        }),
        Statement::AlterTable(alt) => Ok(TaqlExecution {
            result: execute_alter_table(alt, table)?,
            affected_rows: Vec::new(),
        }),
    }
}

/// Execute a parsed TaQL statement against a mutable table, materializing
/// computed `SELECT` projections and returning affected row indices.
pub fn execute_for_tool(
    stmt: &Statement,
    table: &mut crate::Table,
) -> Result<TaqlExecution, TaqlError> {
    let exec = execute_with_rows(stmt, table)?;
    if let TaqlResult::Select {
        ref row_indices, ..
    } = exec.result
        && let Statement::Select(sel) = stmt
        && !sel.columns.is_empty()
        && has_computed_columns(&sel.columns)
    {
        return Ok(TaqlExecution {
            result: materialize_select(sel, row_indices, table)?,
            affected_rows: Vec::new(),
        });
    }
    Ok(exec)
}

/// Read-only counterpart to [`execute_for_tool`] for `SELECT` and `COUNT`.
pub fn execute_readonly_for_tool(
    stmt: &Statement,
    table: &crate::Table,
) -> Result<TaqlResult, TaqlError> {
    execute_materializing_readonly(stmt, table)
}

/// Execute a parsed TaQL statement against a table.
pub fn execute(stmt: &Statement, table: &mut crate::Table) -> Result<TaqlResult, TaqlError> {
    execute_with_rows(stmt, table).map(|exec| exec.result)
}

/// Execute a parsed TaQL statement against a table without allowing mutation.
pub fn execute_readonly(stmt: &Statement, table: &crate::Table) -> Result<TaqlResult, TaqlError> {
    match stmt {
        Statement::Select(sel) => execute_select(sel, table),
        Statement::CountSelect(sel) => execute_count_select(sel, table),
        Statement::Count(cnt) => execute_count(cnt, table),
        Statement::Calc(calc) => execute_calc(calc, table),
        _ => Err(TaqlError::Table(
            "statement requires mutable table access".to_string(),
        )),
    }
}

/// Evaluate WHERE, JOIN, ORDER BY, NODUPLICATES, DISTINCT, OFFSET, and LIMIT for a SELECT.
pub(super) fn select_row_indices(
    sel: &SelectStatement,
    table: &crate::Table,
) -> Result<Vec<usize>, TaqlError> {
    select_rows_impl(sel, table, false)
}

/// Evaluate a row-view selection. A GROUP BY keeps one source row per group
/// instead of constructing a new table of aggregate columns.
///
/// # Errors
/// A query expression is invalid or a referenced cell cannot be read.
pub fn select_view_rows(
    sel: &SelectStatement,
    table: &crate::Table,
) -> Result<Vec<usize>, TaqlError> {
    select_rows_impl(sel, table, true)
}

fn select_rows_impl(
    sel: &SelectStatement,
    table: &crate::Table,
    project_groups: bool,
) -> Result<Vec<usize>, TaqlError> {
    let row_count = table.row_count();
    let style = sel.style;

    // 1. WHERE filter
    let mut row_indices: Vec<usize> = if let Some(ref where_clause) = sel.where_clause {
        let mut where_clause = where_clause.clone();
        super::mscal_lookup::bind(&mut where_clause, table, style)?;
        super::mscal_help::fold_filter(&mut where_clause, table, style)?;
        let mut indices = Vec::new();
        for i in 0..row_count {
            let row = table
                .row_accessor()
                .row(i)
                .map_err(|err| TaqlError::Table(err.to_string()))?;
            let ctx = EvalContext {
                row,
                row_index: i,
                style,
                measures: table.measures_provider(),
            };
            let val = eval_expr(&where_clause, &ctx)?;
            if val.to_bool()? {
                indices.push(i);
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

    if project_groups && !sel.group_by.is_empty() {
        let groups = group_rows(&row_indices, &sel.group_by, table, style)?;
        row_indices.clear();
        for group in groups {
            if let Some(having) = &sel.having
                && !eval_aggregate_expr(having, &group, table, style)?.to_bool()?
            {
                continue;
            }
            if let Some(&last) = group.last() {
                row_indices.push(last);
            }
        }
    }

    // 2. ORDER BY
    if !sel.order_by.is_empty() {
        sort_rows(&mut row_indices, &sel.order_by, table, style)?;
        if sel.order_distinct {
            let order_exprs: Vec<Expr> = sel.order_by.iter().map(|s| s.expr.clone()).collect();
            deduplicate_by_exprs(&mut row_indices, &order_exprs, table, style)?;
        }
    }

    // 3. DISTINCT
    if sel.distinct {
        deduplicate_rows(&mut row_indices, &sel.columns, table, style)?;
    }

    if project_groups {
        return apply_view_limits(row_indices, sel.offset.as_ref(), sel.limit.as_ref());
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

    Ok(row_indices)
}

/// Apply casacore's signed row-view OFFSET and LIMIT to selected row indices.
/// A zero limit preserves the rows after the offset.
///
/// # Errors
/// An offset or limit is not a constant integer expression.
pub fn apply_view_limits(
    mut rows: Vec<usize>,
    offset: Option<&Expr>,
    limit: Option<&Expr>,
) -> Result<Vec<usize>, TaqlError> {
    let count = rows.len() as i64;
    let from_end = |value: i64| {
        if value < 0 {
            (count + value).max(0)
        } else {
            value
        }
    };
    let offset = offset.map(eval_const_int).transpose()?.unwrap_or(0);
    let limit = limit.map(eval_const_int).transpose()?.unwrap_or(0);
    let start = from_end(offset).min(count);
    let end = if limit == 0 {
        count
    } else {
        start.saturating_add(from_end(limit)).min(count)
    };
    rows.truncate(end as usize);
    rows.drain(..start as usize);
    Ok(rows)
}

pub(super) fn select_columns_expr_list(
    columns: &[SelectColumn],
    table: &crate::Table,
) -> Result<Vec<Expr>, TaqlError> {
    if columns.is_empty() {
        let cols: Vec<Expr> = table
            .schema()
            .into_iter()
            .flat_map(|s| s.columns())
            .map(|c| {
                Expr::ColumnRef(ColumnRef {
                    table: None,
                    column: c.name().to_string(),
                })
            })
            .collect();
        return Ok(cols);
    }
    Ok(columns.iter().map(|c| c.expr.clone()).collect())
}

/// Execute a SELECT statement.
fn execute_select(sel: &SelectStatement, table: &crate::Table) -> Result<TaqlResult, TaqlError> {
    if sel.giving_expr.is_some() {
        return Err(TaqlError::TypeError {
            message:
                "Error in select expression: A query in a FROM can only have 'GIVING tablename'"
                    .to_string(),
        });
    }
    // Check for GROUP BY / aggregates
    if !sel.group_by.is_empty() || has_aggregates_in_columns(&sel.columns) {
        return execute_group_by(sel, table);
    }

    let row_indices = select_row_indices(sel, table)?;

    // 6. Column projection
    let columns = extract_column_names(&sel.columns, table)?;

    Ok(TaqlResult::Select {
        row_indices,
        columns,
    })
}

/// Execute a parsed TaQL statement against a table, materializing computed SELECTs.
pub(crate) fn execute_materializing_readonly(
    stmt: &Statement,
    table: &crate::Table,
) -> Result<TaqlResult, TaqlError> {
    let result = execute_readonly(stmt, table)?;
    match result {
        TaqlResult::Select {
            ref row_indices,
            columns: _,
        } => {
            if let Statement::Select(sel) = stmt
                && !sel.columns.is_empty()
                && has_computed_columns(&sel.columns)
            {
                return materialize_select(sel, row_indices, table);
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
            .row_accessor()
            .row(row_idx)
            .map_err(|err| TaqlError::Table(err.to_string()))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
            style,
            measures: table.measures_provider(),
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
    use casa_core::PrimitiveType as PT;
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
    table: &crate::Table,
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
                .row_accessor()
                .row(left_idx)
                .map_err(|err| TaqlError::Table(err.to_string()))?;
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
                    .row_accessor()
                    .row(right_idx)
                    .map_err(|err| TaqlError::Table(err.to_string()))?;

                // Merge left and right rows for ON evaluation.
                // Fields from right are accessible with the join table alias prefix,
                // but for self-joins we merge into one context.
                let merged = merge_rows(left_row, right_row, join.table.alias.as_deref());

                let ctx = EvalContext {
                    row: &merged,
                    row_index: left_idx,
                    style,
                    measures: table.measures_provider(),
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

/// Execute a SELECT with GROUP BY or aggregates.
fn execute_group_by(sel: &SelectStatement, table: &crate::Table) -> Result<TaqlResult, TaqlError> {
    let row_count = table.row_count();
    let style = sel.style;

    // 1. WHERE filter
    let row_indices: Vec<usize> = if let Some(ref where_clause) = sel.where_clause {
        let mut indices = Vec::new();
        for i in 0..row_count {
            let row = table
                .row_accessor()
                .row(i)
                .map_err(|err| TaqlError::Table(err.to_string()))?;
            let ctx = EvalContext {
                row,
                row_index: i,
                style,
                measures: table.measures_provider(),
            };
            let val = eval_expr(where_clause, &ctx)?;
            if val.to_bool()? {
                indices.push(i);
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
            .row_accessor()
            .row(row_idx)
            .map_err(|err| TaqlError::Table(err.to_string()))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
            style,
            measures: table.measures_provider(),
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
            (ExprValue::Quantity(a), ExprValue::Quantity(b)) => {
                a.unit().conformant(b.unit())
                    && a.get_si_value().to_bits() == b.get_si_value().to_bits()
            }
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
            ExprValue::Quantity(q) => {
                q.unit().name().hash(state);
                q.get_si_value().to_bits().hash(state);
            }
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
            ExprValue::MaskedArray(arr, mask) => {
                arr.shape.hash(state);
                arr.data.len().hash(state);
                mask.hash(state);
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
                    .map_err(|err| TaqlError::Table(err.to_string()))?;
                let ctx = EvalContext {
                    row,
                    row_index: row_idx,
                    style,
                    measures: table.measures_provider(),
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
                    .row_accessor()
                    .row(first_row)
                    .map_err(|err| TaqlError::Table(err.to_string()))?;
                let ctx = EvalContext {
                    row,
                    row_index: first_row,
                    style,
                    measures: table.measures_provider(),
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
                    .row_accessor()
                    .row(first_row)
                    .map_err(|err| TaqlError::Table(err.to_string()))?;
                let ctx = EvalContext {
                    row,
                    row_index: first_row,
                    style,
                    measures: table.measures_provider(),
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
                    measures: table.measures_provider(),
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
        ExprValue::Quantity(q) => Literal::Quantity {
            value: q.value(),
            unit: q.unit().name().to_string(),
        },
        ExprValue::Complex(c) => Literal::Complex(*c),
        ExprValue::String(s) => Literal::String(s.clone()),
        ExprValue::DateTime(v) => Literal::Float(*v),
        ExprValue::Array(_) | ExprValue::MaskedArray(..) => Literal::Null, // arrays have no literal form
        ExprValue::Regex { pattern, flags } => Literal::Regex {
            pattern: pattern.clone(),
            flags: flags.clone(),
        },
        ExprValue::Null => Literal::Null,
    }
}

// ── Helper functions ──

/// Check if any column in the SELECT list contains aggregate functions.
pub(super) fn has_aggregates_in_columns(columns: &[SelectColumn]) -> bool {
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
            .row_accessor()
            .row(row_idx)
            .map_err(|err| TaqlError::Table(err.to_string()))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
            style,
            measures: table.measures_provider(),
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
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr.clone()).collect();
    deduplicate_by_exprs(row_indices, &exprs, table, style)
}

/// Remove duplicate rows based on key expressions (`columns` empty -> all row columns).
fn deduplicate_by_exprs(
    row_indices: &mut Vec<usize>,
    exprs: &[Expr],
    table: &crate::Table,
    style: ast::IndexStyle,
) -> Result<(), TaqlError> {
    let mut seen: HashSet<GroupKey> = HashSet::new();
    let mut deduped = Vec::with_capacity(row_indices.len());

    for &row_idx in row_indices.iter() {
        let row = table
            .row_accessor()
            .row(row_idx)
            .map_err(|err| TaqlError::Table(err.to_string()))?;
        let ctx = EvalContext {
            row,
            row_index: row_idx,
            style,
            measures: table.measures_provider(),
        };

        let key = GroupKey(if exprs.is_empty() {
            // SELECT DISTINCT * — use all columns
            row.fields()
                .iter()
                .map(|f| ExprValueKey(ExprValue::from(&f.value)))
                .collect()
        } else {
            exprs
                .iter()
                .map(|expr| eval_expr(expr, &ctx).map(ExprValueKey))
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
            if let Expr::ColumnRef(cr) = &col.expr
                && cr.table.is_none()
                && !schema.columns().iter().any(|c| c.name() == cr.column)
            {
                return Err(TaqlError::ColumnNotFound {
                    name: cr.column.clone(),
                });
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
        measures: None,
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
    use crate::schema::ColumnType;

    if let Some(schema) = table.schema()
        && let Some(col_schema) = schema.columns().iter().find(|c| c.name() == column)
        && let Some(ptype) = col_schema.data_type()
    {
        match (col_schema.column_type(), val) {
            (ColumnType::Array(_), ExprValue::Array(arr)) => {
                return Ok(expr_array_to_typed_value(arr, ptype));
            }
            (ColumnType::Scalar, _) => {
                if let Some(scalar) = expr_scalar_to_type(val, ptype) {
                    return Ok(Value::Scalar(scalar));
                }
            }
            _ => {}
        }
    }
    Ok(expr_value_to_value_untyped(val))
}

fn expr_scalar_as_i64(val: &ExprValue) -> Option<i64> {
    match val {
        ExprValue::Int(n) => Some(*n),
        ExprValue::Float(v) | ExprValue::DateTime(v) => Some(*v as i64),
        ExprValue::Quantity(q) => Some(q.value() as i64),
        ExprValue::Bool(b) => Some(i64::from(*b)),
        _ => None,
    }
}

fn expr_scalar_as_f64(val: &ExprValue) -> Option<f64> {
    match val {
        ExprValue::Float(v) | ExprValue::DateTime(v) => Some(*v),
        ExprValue::Quantity(q) => Some(q.value()),
        ExprValue::Int(n) => Some(*n as f64),
        ExprValue::Bool(b) => Some(f64::from(*b)),
        _ => None,
    }
}

fn expr_scalar_as_c64(val: &ExprValue) -> Option<num_complex::Complex64> {
    match val {
        ExprValue::Complex(c) => Some(*c),
        _ => expr_scalar_as_f64(val).map(|v| num_complex::Complex64::new(v, 0.0)),
    }
}

fn expr_scalar_to_type(val: &ExprValue, ptype: casa_core::PrimitiveType) -> Option<ScalarValue> {
    use casa_core::PrimitiveType as PT;
    let as_i64 = expr_scalar_as_i64(val);
    let as_f64 = expr_scalar_as_f64(val);
    let as_c64 = expr_scalar_as_c64(val);
    match ptype {
        PT::Bool => match val {
            ExprValue::Bool(b) => Some(ScalarValue::Bool(*b)),
            ExprValue::Int(n) => Some(ScalarValue::Bool(*n != 0)),
            _ => None,
        },
        PT::UInt8 => as_i64.map(|n| ScalarValue::UInt8(n as u8)),
        PT::UInt16 => as_i64.map(|n| ScalarValue::UInt16(n as u16)),
        PT::UInt32 => as_i64.map(|n| ScalarValue::UInt32(n as u32)),
        PT::Int16 => as_i64.map(|n| ScalarValue::Int16(n as i16)),
        PT::Int32 => as_i64.map(|n| ScalarValue::Int32(n as i32)),
        PT::Int64 => as_i64.map(ScalarValue::Int64),
        PT::Float32 => as_f64.map(|v| ScalarValue::Float32(v as f32)),
        PT::Float64 => as_f64.map(ScalarValue::Float64),
        PT::Complex32 => as_c64
            .map(|c| ScalarValue::Complex32(num_complex::Complex32::new(c.re as f32, c.im as f32))),
        PT::Complex64 => as_c64.map(ScalarValue::Complex64),
        PT::String => match val {
            ExprValue::String(s) => Some(ScalarValue::String(s.clone())),
            _ => None,
        },
    }
}

/// Convert an ExprValue to a Value without schema type information.
fn expr_value_to_value_untyped(val: &ExprValue) -> Value {
    match val {
        ExprValue::Bool(b) => Value::Scalar(ScalarValue::Bool(*b)),
        ExprValue::Int(n) => Value::Scalar(ScalarValue::Int64(*n)),
        ExprValue::Float(v) => Value::Scalar(ScalarValue::Float64(*v)),
        ExprValue::Quantity(q) => Value::Scalar(ScalarValue::Float64(q.value())),
        ExprValue::Complex(c) => Value::Scalar(ScalarValue::Complex64(*c)),
        ExprValue::String(s) => Value::Scalar(ScalarValue::String(s.clone())),
        ExprValue::DateTime(v) => Value::Scalar(ScalarValue::Float64(*v)),
        ExprValue::Array(arr) => expr_array_to_value(arr),
        ExprValue::MaskedArray(arr, _) => expr_array_to_value(arr),
        ExprValue::Regex { pattern, .. } => Value::Scalar(ScalarValue::String(pattern.clone())),
        ExprValue::Null => Value::Scalar(ScalarValue::Bool(false)), // fallback
    }
}

/// Convert an `eval::ArrayValue` to a `casa_core::Value::Array` matching `ptype`.
fn expr_array_to_typed_value(
    arr: &super::eval::ArrayValue,
    ptype: casa_core::PrimitiveType,
) -> Value {
    use casa_core::{ArrayValue as AV, PrimitiveType as PT};
    use ndarray::{ArrayD, IxDyn, ShapeBuilder};

    let shape = IxDyn(&arr.shape).f();
    let to_i64 = |e: &ExprValue| expr_scalar_as_i64(e).unwrap_or(0);
    let to_f64 = |e: &ExprValue| expr_scalar_as_f64(e).unwrap_or(0.0);
    let to_c64 = |e: &ExprValue| {
        expr_scalar_as_c64(e).unwrap_or_else(|| num_complex::Complex64::new(0.0, 0.0))
    };
    macro_rules! map_array {
        ($variant:ident, $map:expr) => {{
            let data: Vec<_> = arr.data.iter().map($map).collect();
            Value::Array(AV::$variant(
                ArrayD::from_shape_vec(shape, data).unwrap_or_default(),
            ))
        }};
    }
    match ptype {
        PT::Bool => map_array!(Bool, |e| matches!(e, ExprValue::Bool(true))),
        PT::UInt8 => map_array!(UInt8, |e| to_i64(e) as u8),
        PT::UInt16 => map_array!(UInt16, |e| to_i64(e) as u16),
        PT::UInt32 => map_array!(UInt32, |e| to_i64(e) as u32),
        PT::Int16 => map_array!(Int16, |e| to_i64(e) as i16),
        PT::Int32 => map_array!(Int32, |e| to_i64(e) as i32),
        PT::Int64 => map_array!(Int64, to_i64),
        PT::Float32 => map_array!(Float32, |e| to_f64(e) as f32),
        PT::Float64 => map_array!(Float64, to_f64),
        PT::Complex32 => map_array!(Complex32, |e| {
            let c = to_c64(e);
            num_complex::Complex32::new(c.re as f32, c.im as f32)
        }),
        PT::Complex64 => map_array!(Complex64, to_c64),
        PT::String => map_array!(String, |e| match e {
            ExprValue::String(s) => s.clone(),
            _ => String::new(),
        }),
    }
}

/// Convert an `eval::ArrayValue` to a `casa_core::Value::Array`.
///
/// Determines the element type from the first element and builds an
/// `ndarray::ArrayD` with Fortran (column-major) layout, matching casacore conventions.
fn expr_array_to_value(arr: &super::eval::ArrayValue) -> Value {
    use casa_core::PrimitiveType as PT;
    let inferred = match arr.data.first() {
        Some(ExprValue::Bool(_)) => PT::Bool,
        Some(ExprValue::Int(_)) => PT::Int64,
        Some(ExprValue::Complex(_)) => PT::Complex64,
        Some(ExprValue::String(_)) => PT::String,
        _ => PT::Float64,
    };
    expr_array_to_typed_value(arr, inferred)
}

#[cfg(test)]
mod tests;
