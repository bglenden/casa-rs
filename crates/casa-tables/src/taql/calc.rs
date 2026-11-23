// SPDX-License-Identifier: LGPL-3.0-or-later
//! Evaluation engine for TaQL `CALC` expressions and subqueries.
//!
//! `CALC` evaluates scalar or array expressions standalone, against an open or
//! on-disk table (`FROM table`), or over bracketed subqueries
//! (`[SELECT ... FROM table GIVING [expr]]`).

use std::path::Path;

use casa_types::{PrimitiveType, RecordValue};

use super::ast::{
    AggregateFunc, CalcStatement, Expr, InSetElement, IndexElement, Literal, SelectStatement,
    Statement,
};
use super::error::TaqlError;
use super::eval::{
    ArrayValue, EvalContext, ExprValue, eval_array_index, eval_binary, eval_expr, eval_unary,
};
use super::exec::{
    TaqlResult, execute_readonly, has_aggregates_in_columns, select_columns_expr_list,
    select_row_indices,
};
use super::functions::call_function;
use crate::schema::ColumnType;
use crate::{Table, TableOptions};

/// Evaluated values from a `CALC` statement.
#[derive(Debug, Clone)]
pub struct CalcOutput {
    /// One value per evaluated table row (or a single value for constant/subquery
    /// expressions without table-row dependence).
    pub values: Vec<ExprValue>,
    /// Inferred scalar element type when `FROM` points to a 0-row table and the
    /// expression depends on table rows.
    pub empty_scalar_type: Option<PrimitiveType>,
    /// Present when the top-level expression directly indexes a column array
    /// with only single-element subscripts (e.g. `f1[0]` -> `Some(1)`,
    /// `m2[0,1]` -> `Some(2)`). Casacore preserves leading length-1 dimensions
    /// `(1, ..., 1, nrows)` for this case in `TableProxy::calc`.
    pub single_index_ndim: Option<usize>,
}

enum ResolvedTable<'a> {
    Borrowed(&'a Table),
    Owned(Box<Table>),
}

impl ResolvedTable<'_> {
    fn as_ref(&self) -> &Table {
        match self {
            Self::Borrowed(t) => t,
            Self::Owned(t) => t,
        }
    }
}

/// Evaluates a `CALC` statement, optionally resolving `FROM` against `open_table`.
pub fn evaluate_calc(
    calc: &CalcStatement,
    open_table: Option<(&Path, &Table)>,
) -> Result<CalcOutput, TaqlError> {
    validate_no_calc_aggregates(&calc.expr)?;

    if let Some(ref from_ref) = calc.from {
        let resolved = resolve_table(&from_ref.name, open_table)?;
        let table = resolved.as_ref();
        if !expr_uses_table_rows(&calc.expr) {
            let empty_row = RecordValue::default();
            let ctx = EvalContext {
                row: &empty_row,
                row_index: 0,
                style: calc.style,
                measures: table.measures_provider(),
            };
            let val = eval_calc_expr(&calc.expr, &ctx, open_table)?;
            return Ok(CalcOutput {
                values: vec![val],
                empty_scalar_type: None,
                single_index_ndim: None,
            });
        }
        let nrows = table.row_count();
        if nrows == 0 {
            validate_calc_columns(&calc.expr, table)?;
            return Ok(CalcOutput {
                values: Vec::new(),
                empty_scalar_type: infer_empty_scalar_type(&calc.expr, table),
                single_index_ndim: None,
            });
        }
        let mut values = Vec::with_capacity(nrows);
        for row_index in 0..nrows {
            let row = table
                .row_accessor()
                .row(row_index)
                .map_err(|err| TaqlError::Table(err.to_string()))?;
            let ctx = EvalContext {
                row,
                row_index,
                style: calc.style,
                measures: table.measures_provider(),
            };
            values.push(eval_calc_expr(&calc.expr, &ctx, open_table)?);
        }
        let single_index_ndim = if !matches!(values.first(), Some(ExprValue::Array(_))) {
            direct_column_single_index_ndim(&calc.expr)
        } else {
            None
        };
        return Ok(CalcOutput {
            values,
            empty_scalar_type: None,
            single_index_ndim,
        });
    }

    if expr_has_column_ref(&calc.expr) {
        return Err(TaqlError::TypeError {
            message: "Error in select expression: Shorthand  has not been defined in FROM clause"
                .to_string(),
        });
    }

    let empty_row = RecordValue::default();
    let ctx = EvalContext {
        row: &empty_row,
        row_index: 0,
        style: calc.style,
        measures: open_table.and_then(|(_, t)| t.measures_provider()),
    };
    let val = eval_calc_expr(&calc.expr, &ctx, open_table)?;
    Ok(CalcOutput {
        values: vec![val],
        empty_scalar_type: None,
        single_index_ndim: None,
    })
}

fn resolve_table<'a>(
    name: &str,
    open_table: Option<(&'a Path, &'a Table)>,
) -> Result<ResolvedTable<'a>, TaqlError> {
    if let Some((open_path, table)) = open_table
        && paths_match(Path::new(name), open_path)
    {
        return Ok(ResolvedTable::Borrowed(table));
    }
    let table =
        Table::open(TableOptions::new(name)).map_err(|e| TaqlError::Table(e.to_string()))?;
    Ok(ResolvedTable::Owned(Box::new(table)))
}

fn paths_match(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    let abs_a = std::path::absolute(a).ok();
    let abs_b = std::path::absolute(b).ok();
    match (abs_a, abs_b) {
        (Some(pa), Some(pb)) => pa == pb,
        _ => false,
    }
}

fn direct_column_single_index_ndim(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::ArrayIndex { array, indices }
            if matches!(array.as_ref(), Expr::ColumnRef(_))
                && !indices.is_empty()
                && indices
                    .iter()
                    .all(|elem| matches!(elem, IndexElement::Single(_))) =>
        {
            Some(indices.len())
        }
        _ => None,
    }
}

fn validate_no_calc_aggregates(expr: &Expr) -> Result<(), TaqlError> {
    match expr {
        Expr::Aggregate { func, arg } => {
            if matches!(
                func,
                AggregateFunc::Count
                    | AggregateFunc::First
                    | AggregateFunc::Last
                    | AggregateFunc::Fractile
                    | AggregateFunc::Aggr
                    | AggregateFunc::RowId
                    | AggregateFunc::Hist
            ) {
                return Err(TaqlError::TypeError {
                    message: "Error in select expression: Invalid use of an aggregate function \
                              (only use in SELECT or HAVING clause)"
                        .to_string(),
                });
            }
            validate_no_calc_aggregates(arg)
        }
        Expr::Unary { operand, .. } | Expr::BoundRowLookup { index: operand, .. } => {
            validate_no_calc_aggregates(operand)
        }
        Expr::Binary { left, right, .. } => {
            validate_no_calc_aggregates(left)?;
            validate_no_calc_aggregates(right)
        }
        Expr::FunctionCall { args, .. } | Expr::ArrayLiteral(args) => {
            for arg in args {
                validate_no_calc_aggregates(arg)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_no_calc_aggregates(expr)?;
            validate_no_calc_aggregates(low)?;
            validate_no_calc_aggregates(high)
        }
        Expr::In { expr, values, .. } => {
            validate_no_calc_aggregates(expr)?;
            for v in values {
                validate_no_calc_aggregates(v)?;
            }
            Ok(())
        }
        Expr::Like { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            validate_no_calc_aggregates(expr)?;
            validate_no_calc_aggregates(pattern)
        }
        Expr::IsNull { expr, .. } => validate_no_calc_aggregates(expr),
        Expr::ArrayIndex { array, indices } => {
            validate_no_calc_aggregates(array)?;
            for idx in indices {
                match idx {
                    IndexElement::Single(e) => validate_no_calc_aggregates(e)?,
                    IndexElement::Slice { start, end, step } => {
                        if let Some(s) = start {
                            validate_no_calc_aggregates(s)?;
                        }
                        if let Some(e) = end {
                            validate_no_calc_aggregates(e)?;
                        }
                        if let Some(st) = step {
                            validate_no_calc_aggregates(st)?;
                        }
                    }
                }
            }
            Ok(())
        }
        // Do not recurse into subqueries: aggregates inside a subquery belong to that SELECT.
        Expr::Subquery(_)
        | Expr::Literal(_)
        | Expr::PreparedConstant(_)
        | Expr::ColumnRef(_)
        | Expr::Star
        | Expr::BoundRowFunction(_)
        | Expr::RowNumber => Ok(()),
        Expr::InSet { expr, .. } => validate_no_calc_aggregates(expr),
    }
}

fn expr_has_column_ref(expr: &Expr) -> bool {
    match expr {
        Expr::ColumnRef(_) | Expr::BoundRowFunction(_) => true,
        Expr::Unary { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            expr_has_column_ref(operand)
        }
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::RegexMatch {
            expr: left,
            pattern: right,
            ..
        } => expr_has_column_ref(left) || expr_has_column_ref(right),
        Expr::Aggregate { arg, .. } | Expr::BoundRowLookup { index: arg, .. } => {
            expr_has_column_ref(arg)
        }
        Expr::FunctionCall { args, .. } | Expr::ArrayLiteral(args) => {
            args.iter().any(expr_has_column_ref)
        }
        Expr::Between {
            expr, low, high, ..
        } => expr_has_column_ref(expr) || expr_has_column_ref(low) || expr_has_column_ref(high),
        Expr::In { expr, values, .. } => {
            expr_has_column_ref(expr) || values.iter().any(expr_has_column_ref)
        }
        Expr::InSet { expr, .. } => expr_has_column_ref(expr),
        Expr::ArrayIndex { array, indices } => {
            expr_has_column_ref(array)
                || indices.iter().any(|el| match el {
                    IndexElement::Single(e) => expr_has_column_ref(e),
                    IndexElement::Slice { start, end, step } => {
                        start.as_ref().is_some_and(expr_has_column_ref)
                            || end.as_ref().is_some_and(expr_has_column_ref)
                            || step.as_ref().is_some_and(expr_has_column_ref)
                    }
                })
        }
        Expr::Subquery(_)
        | Expr::Literal(_)
        | Expr::PreparedConstant(_)
        | Expr::Star
        | Expr::RowNumber => false,
    }
}

fn expr_uses_table_rows(expr: &Expr) -> bool {
    if expr_has_column_ref(expr) {
        return true;
    }
    match expr {
        Expr::RowNumber => true,
        Expr::FunctionCall { name, args } => {
            matches!(
                name.to_ascii_lowercase().as_str(),
                "rownumber" | "rownr" | "rowid" | "rand"
            ) || args.iter().any(expr_uses_table_rows)
        }
        Expr::Unary { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            expr_uses_table_rows(operand)
        }
        Expr::Binary { left, right, .. } => {
            expr_uses_table_rows(left) || expr_uses_table_rows(right)
        }
        Expr::ArrayLiteral(items) => items.iter().any(expr_uses_table_rows),
        Expr::Aggregate { arg, .. } | Expr::BoundRowLookup { index: arg, .. } => {
            expr_uses_table_rows(arg)
        }
        Expr::ArrayIndex { array, indices } => {
            expr_uses_table_rows(array)
                || indices.iter().any(|el| match el {
                    IndexElement::Single(e) => expr_uses_table_rows(e),
                    IndexElement::Slice { start, end, step } => {
                        start.as_ref().is_some_and(expr_uses_table_rows)
                            || end.as_ref().is_some_and(expr_uses_table_rows)
                            || step.as_ref().is_some_and(expr_uses_table_rows)
                    }
                })
        }
        _ => false,
    }
}

fn validate_calc_columns(expr: &Expr, table: &Table) -> Result<(), TaqlError> {
    let Some(schema) = table.schema() else {
        return Ok(());
    };
    match expr {
        Expr::ColumnRef(cr) => {
            if schema.column(&cr.column).is_none() {
                return Err(TaqlError::ColumnNotFound {
                    name: cr.to_string(),
                });
            }
            Ok(())
        }
        Expr::Aggregate { arg, .. } => {
            validate_calc_columns(arg, table)?;
            if matches!(
                arg.as_ref(),
                Expr::ColumnRef(cr)
                    if schema
                        .column(&cr.column)
                        .is_some_and(|c| *c.column_type() == ColumnType::Scalar)
            ) {
                return Err(TaqlError::TypeError {
                    message: "Error in select expression: Invalid use of an aggregate function \
                              (only use in SELECT or HAVING clause)"
                        .to_string(),
                });
            }
            Ok(())
        }
        Expr::Unary { operand, .. }
        | Expr::IsNull { expr: operand, .. }
        | Expr::BoundRowLookup { index: operand, .. } => validate_calc_columns(operand, table),
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::RegexMatch {
            expr: left,
            pattern: right,
            ..
        } => {
            validate_calc_columns(left, table)?;
            validate_calc_columns(right, table)
        }
        Expr::FunctionCall { args, .. } | Expr::ArrayLiteral(args) => {
            for arg in args {
                validate_calc_columns(arg, table)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_calc_columns(expr, table)?;
            validate_calc_columns(low, table)?;
            validate_calc_columns(high, table)
        }
        Expr::In { expr, values, .. } => {
            validate_calc_columns(expr, table)?;
            for v in values {
                validate_calc_columns(v, table)?;
            }
            Ok(())
        }
        Expr::InSet { expr, elements, .. } => {
            validate_calc_columns(expr, table)?;
            for elem in elements {
                match elem {
                    InSetElement::Value(e) => validate_calc_columns(e, table)?,
                    InSetElement::Range { start, end, step } => {
                        if let Some(e) = start {
                            validate_calc_columns(e, table)?;
                        }
                        if let Some(e) = end {
                            validate_calc_columns(e, table)?;
                        }
                        if let Some(e) = step {
                            validate_calc_columns(e, table)?;
                        }
                    }
                }
            }
            Ok(())
        }
        Expr::ArrayIndex { array, indices } => {
            validate_calc_columns(array, table)?;
            for el in indices {
                match el {
                    IndexElement::Single(e) => validate_calc_columns(e, table)?,
                    IndexElement::Slice { start, end, step } => {
                        if let Some(e) = start {
                            validate_calc_columns(e, table)?;
                        }
                        if let Some(e) = end {
                            validate_calc_columns(e, table)?;
                        }
                        if let Some(e) = step {
                            validate_calc_columns(e, table)?;
                        }
                    }
                }
            }
            Ok(())
        }
        Expr::Subquery(_)
        | Expr::Literal(_)
        | Expr::PreparedConstant(_)
        | Expr::Star
        | Expr::BoundRowFunction(_)
        | Expr::RowNumber => Ok(()),
    }
}

fn infer_empty_scalar_type(expr: &Expr, table: &Table) -> Option<PrimitiveType> {
    let schema = table.schema()?;
    match expr {
        Expr::ColumnRef(cr) => match schema.column(&cr.column)?.column_type() {
            ColumnType::Scalar | ColumnType::Array(_) => schema.column(&cr.column)?.data_type(),
            ColumnType::Record => None,
        },
        Expr::ArrayIndex { array, .. } => infer_empty_scalar_type(array, table),
        _ => None,
    }
}

fn eval_calc_expr(
    expr: &Expr,
    ctx: &EvalContext<'_>,
    open_table: Option<(&Path, &Table)>,
) -> Result<ExprValue, TaqlError> {
    match expr {
        Expr::Subquery(sel) => eval_subquery(sel, open_table),
        Expr::Aggregate { func, arg } => eval_calc_aggregate(*func, arg, ctx, open_table),
        Expr::Unary { op, operand } if has_subquery_or_agg(operand) => {
            let val = eval_calc_expr(operand, ctx, open_table)?;
            eval_unary(*op, val)
        }
        Expr::Binary { left, op, right }
            if has_subquery_or_agg(left) || has_subquery_or_agg(right) =>
        {
            let lval = eval_calc_expr(left, ctx, open_table)?;
            let rval = eval_calc_expr(right, ctx, open_table)?;
            eval_binary(*op, lval, rval)
        }
        Expr::ArrayIndex { array, indices }
            if has_subquery_or_agg(array) || indices.iter().any(index_has_subquery_or_agg) =>
        {
            let arr_val = eval_calc_expr(array, ctx, open_table)?;
            let resolved_indices = indices
                .iter()
                .map(|el| resolve_index_subexprs(el, ctx, open_table))
                .collect::<Result<Vec<_>, _>>()?;
            match arr_val {
                ExprValue::Array(arr) => eval_array_index(&arr, None, &resolved_indices, ctx),
                ExprValue::MaskedArray(arr, mask) => {
                    eval_array_index(&arr, Some(&mask), &resolved_indices, ctx)
                }
                other => Err(TaqlError::TypeError {
                    message: format!("array indexing requires Array, got {}", other.type_name()),
                }),
            }
        }
        Expr::FunctionCall { name, args } if args.iter().any(has_subquery_or_agg) => {
            let evaluated: Vec<ExprValue> = args
                .iter()
                .map(|a| eval_calc_expr(a, ctx, open_table))
                .collect::<Result<_, _>>()?;
            call_function(name, &evaluated, ctx)
        }
        Expr::ArrayLiteral(items) if items.iter().any(has_subquery_or_agg) => {
            let data: Vec<ExprValue> = items
                .iter()
                .map(|item| eval_calc_expr(item, ctx, open_table))
                .collect::<Result<_, _>>()?;
            let shape = vec![data.len()];
            Ok(ExprValue::Array(ArrayValue { shape, data }))
        }
        Expr::Between { .. }
        | Expr::In { .. }
        | Expr::InSet { .. }
        | Expr::Like { .. }
        | Expr::RegexMatch { .. }
        | Expr::IsNull { .. }
            if has_subquery_or_agg(expr) =>
        {
            let resolved = resolve_calc_subexprs(expr, ctx, open_table)?;
            eval_expr(&resolved, ctx)
        }
        _ => eval_expr(expr, ctx),
    }
}

fn index_has_subquery_or_agg(el: &IndexElement) -> bool {
    match el {
        IndexElement::Single(e) => has_subquery_or_agg(e),
        IndexElement::Slice { start, end, step } => {
            start.as_ref().is_some_and(has_subquery_or_agg)
                || end.as_ref().is_some_and(has_subquery_or_agg)
                || step.as_ref().is_some_and(has_subquery_or_agg)
        }
    }
}

fn resolve_index_subexprs(
    el: &IndexElement,
    ctx: &EvalContext<'_>,
    open_table: Option<(&Path, &Table)>,
) -> Result<IndexElement, TaqlError> {
    match el {
        IndexElement::Single(e) => Ok(IndexElement::Single(resolve_calc_subexprs(
            e, ctx, open_table,
        )?)),
        IndexElement::Slice { start, end, step } => Ok(IndexElement::Slice {
            start: start
                .as_ref()
                .map(|e| resolve_calc_subexprs(e, ctx, open_table))
                .transpose()?,
            end: end
                .as_ref()
                .map(|e| resolve_calc_subexprs(e, ctx, open_table))
                .transpose()?,
            step: step
                .as_ref()
                .map(|e| resolve_calc_subexprs(e, ctx, open_table))
                .transpose()?,
        }),
    }
}

fn expr_value_to_expr(val: ExprValue) -> Result<Expr, TaqlError> {
    match val {
        ExprValue::Bool(b) => Ok(Expr::Literal(Literal::Bool(b))),
        ExprValue::Int(n) => Ok(Expr::Literal(Literal::Int(n))),
        ExprValue::Float(v) => Ok(Expr::Literal(Literal::Float(v))),
        ExprValue::Quantity(q) => Ok(Expr::Literal(Literal::Quantity {
            value: q.value(),
            unit: q.unit().name().to_string(),
        })),
        ExprValue::Complex(c) => Ok(Expr::Literal(Literal::Complex(c))),
        ExprValue::String(s) => Ok(Expr::Literal(Literal::String(s))),
        ExprValue::Regex { pattern, flags } => Ok(Expr::Literal(Literal::Regex { pattern, flags })),
        ExprValue::Null => Ok(Expr::Literal(Literal::Null)),
        ExprValue::Array(arr) => {
            let items = arr
                .data
                .into_iter()
                .map(expr_value_to_expr)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::ArrayLiteral(items))
        }
        masked @ ExprValue::MaskedArray(..) => {
            Ok(Expr::PreparedConstant(std::sync::Arc::new(masked)))
        }
        ExprValue::DateTime(v) => Ok(Expr::Literal(Literal::Float(v))),
    }
}

fn resolve_calc_subexprs(
    expr: &Expr,
    ctx: &EvalContext<'_>,
    open_table: Option<(&Path, &Table)>,
) -> Result<Expr, TaqlError> {
    if !has_subquery_or_agg(expr) {
        return Ok(expr.clone());
    }
    match expr {
        Expr::Subquery(_) | Expr::Aggregate { .. } => {
            let val = eval_calc_expr(expr, ctx, open_table)?;
            expr_value_to_expr(val)
        }
        Expr::Unary { op, operand } => Ok(Expr::Unary {
            op: *op,
            operand: Box::new(resolve_calc_subexprs(operand, ctx, open_table)?),
        }),
        Expr::Binary { left, op, right } => Ok(Expr::Binary {
            left: Box::new(resolve_calc_subexprs(left, ctx, open_table)?),
            op: *op,
            right: Box::new(resolve_calc_subexprs(right, ctx, open_table)?),
        }),
        Expr::FunctionCall { name, args } => Ok(Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| resolve_calc_subexprs(a, ctx, open_table))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Expr::BoundRowLookup {
            original,
            index,
            values,
            missing,
        } => Ok(Expr::BoundRowLookup {
            original: original.clone(),
            index: Box::new(resolve_calc_subexprs(index, ctx, open_table)?),
            values: values.clone(),
            missing: missing.clone(),
        }),
        Expr::ArrayLiteral(items) => Ok(Expr::ArrayLiteral(
            items
                .iter()
                .map(|a| resolve_calc_subexprs(a, ctx, open_table))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(Expr::Between {
            expr: Box::new(resolve_calc_subexprs(expr, ctx, open_table)?),
            low: Box::new(resolve_calc_subexprs(low, ctx, open_table)?),
            high: Box::new(resolve_calc_subexprs(high, ctx, open_table)?),
            negated: *negated,
        }),
        Expr::In {
            expr,
            values,
            negated,
        } => Ok(Expr::In {
            expr: Box::new(resolve_calc_subexprs(expr, ctx, open_table)?),
            values: values
                .iter()
                .map(|v| resolve_calc_subexprs(v, ctx, open_table))
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        }),
        Expr::InSet {
            expr,
            elements,
            negated,
        } => {
            let resolved_elements = elements
                .iter()
                .map(|el| match el {
                    InSetElement::Value(e) => Ok(InSetElement::Value(resolve_calc_subexprs(
                        e, ctx, open_table,
                    )?)),
                    InSetElement::Range { start, end, step } => Ok(InSetElement::Range {
                        start: start
                            .as_ref()
                            .map(|e| resolve_calc_subexprs(e, ctx, open_table))
                            .transpose()?,
                        end: end
                            .as_ref()
                            .map(|e| resolve_calc_subexprs(e, ctx, open_table))
                            .transpose()?,
                        step: step
                            .as_ref()
                            .map(|e| resolve_calc_subexprs(e, ctx, open_table))
                            .transpose()?,
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Expr::InSet {
                expr: Box::new(resolve_calc_subexprs(expr, ctx, open_table)?),
                elements: resolved_elements,
                negated: *negated,
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => Ok(Expr::Like {
            expr: Box::new(resolve_calc_subexprs(expr, ctx, open_table)?),
            pattern: Box::new(resolve_calc_subexprs(pattern, ctx, open_table)?),
            negated: *negated,
            case_insensitive: *case_insensitive,
        }),
        Expr::RegexMatch {
            expr,
            pattern,
            negated,
        } => Ok(Expr::RegexMatch {
            expr: Box::new(resolve_calc_subexprs(expr, ctx, open_table)?),
            pattern: Box::new(resolve_calc_subexprs(pattern, ctx, open_table)?),
            negated: *negated,
        }),
        Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
            expr: Box::new(resolve_calc_subexprs(expr, ctx, open_table)?),
            negated: *negated,
        }),
        Expr::ArrayIndex { array, indices } => Ok(Expr::ArrayIndex {
            array: Box::new(resolve_calc_subexprs(array, ctx, open_table)?),
            indices: indices
                .iter()
                .map(|el| resolve_index_subexprs(el, ctx, open_table))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Expr::Literal(_)
        | Expr::PreparedConstant(_)
        | Expr::ColumnRef(_)
        | Expr::Star
        | Expr::BoundRowFunction(_)
        | Expr::RowNumber => Ok(expr.clone()),
    }
}

fn has_subquery_or_agg(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) | Expr::Aggregate { .. } => true,
        Expr::Unary { operand, .. }
        | Expr::IsNull { expr: operand, .. }
        | Expr::BoundRowLookup { index: operand, .. } => has_subquery_or_agg(operand),
        Expr::Binary { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::RegexMatch {
            expr: left,
            pattern: right,
            ..
        } => has_subquery_or_agg(left) || has_subquery_or_agg(right),
        Expr::FunctionCall { args, .. } | Expr::ArrayLiteral(args) => {
            args.iter().any(has_subquery_or_agg)
        }
        Expr::Between {
            expr, low, high, ..
        } => has_subquery_or_agg(expr) || has_subquery_or_agg(low) || has_subquery_or_agg(high),
        Expr::In { expr, values, .. } => {
            has_subquery_or_agg(expr) || values.iter().any(has_subquery_or_agg)
        }
        Expr::InSet { expr, elements, .. } => {
            has_subquery_or_agg(expr)
                || elements.iter().any(|el| match el {
                    InSetElement::Value(e) => has_subquery_or_agg(e),
                    InSetElement::Range { start, end, step } => {
                        start.as_ref().is_some_and(has_subquery_or_agg)
                            || end.as_ref().is_some_and(has_subquery_or_agg)
                            || step.as_ref().is_some_and(has_subquery_or_agg)
                    }
                })
        }
        Expr::ArrayIndex { array, indices } => {
            has_subquery_or_agg(array) || indices.iter().any(index_has_subquery_or_agg)
        }
        _ => false,
    }
}

fn eval_calc_aggregate(
    func: AggregateFunc,
    arg: &Expr,
    ctx: &EvalContext<'_>,
    open_table: Option<(&Path, &Table)>,
) -> Result<ExprValue, TaqlError> {
    let fn_name = match func {
        AggregateFunc::Sum => "sum",
        AggregateFunc::Avg => "mean",
        AggregateFunc::Min => "min",
        AggregateFunc::Max => "max",
        AggregateFunc::Median => "median",
        AggregateFunc::NTrue => "ntrue",
        AggregateFunc::NFalse => "nfalse",
        AggregateFunc::Any => "any",
        AggregateFunc::All => "all",
        AggregateFunc::Variance => "variance",
        AggregateFunc::StdDev => "stddev",
        AggregateFunc::Rms => "rms",
        AggregateFunc::Product => "product",
        _ => {
            return Err(TaqlError::TypeError {
                message: "Error in select expression: Invalid use of an aggregate function \
                          (only use in SELECT or HAVING clause)"
                    .to_string(),
            });
        }
    };
    let val = eval_calc_expr(arg, ctx, open_table)?;
    match val {
        ExprValue::Array(_) => call_function(fn_name, &[val], ctx),
        _ => Err(TaqlError::TypeError {
            message: "Error in select expression: Invalid use of an aggregate function \
                      (only use in SELECT or HAVING clause)"
                .to_string(),
        }),
    }
}

fn eval_subquery(
    sel: &SelectStatement,
    open_table: Option<(&Path, &Table)>,
) -> Result<ExprValue, TaqlError> {
    let resolved = match sel.from {
        Some(ref tr) => resolve_table(&tr.name, open_table)?,
        None => {
            let Some((_, table)) = open_table else {
                return Err(TaqlError::TypeError {
                    message: "Error in select expression: Subquery requires a FROM table"
                        .to_string(),
                });
            };
            ResolvedTable::Borrowed(table)
        }
    };
    let table = resolved.as_ref();
    if sel.giving_expr.is_none()
        && (!sel.group_by.is_empty() || has_aggregates_in_columns(&sel.columns))
    {
        match execute_readonly(&Statement::Select(sel.clone()), table)? {
            TaqlResult::Materialized { table: out } => {
                let schema_cols = out.schema().map(|s| s.columns()).unwrap_or(&[]);
                if schema_cols.len() != 1 {
                    return Err(TaqlError::TypeError {
                        message: "Subquery must project exactly one expression".to_string(),
                    });
                }
                let col_name = schema_cols[0].name();
                let mut items = Vec::with_capacity(out.row_count());
                for i in 0..out.row_count() {
                    let cell = out
                        .row_accessor()
                        .row(i)
                        .map_err(|err| TaqlError::Table(err.to_string()))?
                        .get(col_name)
                        .map_or(ExprValue::Null, ExprValue::from);
                    items.push(cell);
                }
                return Ok(ExprValue::Array(ArrayValue {
                    shape: vec![items.len()],
                    data: items,
                }));
            }
            _ => {
                return Err(TaqlError::TypeError {
                    message: "Subquery did not materialize".to_string(),
                });
            }
        }
    }
    let row_indices = select_row_indices(sel, table)?;
    let col_expr = if let Some(ref expr) = sel.giving_expr {
        expr.clone()
    } else {
        let exprs = select_columns_expr_list(&sel.columns, table)?;
        if exprs.len() != 1 {
            return Err(TaqlError::TypeError {
                message: "Subquery must project exactly one expression".to_string(),
            });
        }
        exprs.into_iter().next().unwrap()
    };

    let mut items = Vec::with_capacity(row_indices.len());
    for row_index in row_indices {
        let row = table
            .row_accessor()
            .row(row_index)
            .map_err(|err| TaqlError::Table(err.to_string()))?;
        let ctx = EvalContext {
            row,
            row_index,
            style: sel.style,
            measures: table.measures_provider(),
        };
        items.push(eval_calc_expr(&col_expr, &ctx, open_table)?);
    }

    Ok(ExprValue::Array(ArrayValue {
        shape: vec![items.len()],
        data: items,
    }))
}
