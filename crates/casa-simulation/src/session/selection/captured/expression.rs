// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read only referenced cells and keep CASA's typed Boolean predicates.
use super::{SimulatorResult, error};
use casa_tables::{
    Table,
    taql::{
        ast::*,
        eval::{EvalContext, ExprValue, eval_expr},
    },
};
use casa_types::{RecordValue, Value};

pub(super) fn prepare(select: &mut SelectStatement) -> SimulatorResult<(Vec<String>, usize)> {
    let mut columns = Vec::new();
    let mut nodes = 0;
    let mut roots: Vec<_> = select
        .where_clause
        .iter_mut()
        .chain(select.group_by.iter_mut())
        .chain(select.having.iter_mut())
        .collect();
    roots.extend(select.order_by.iter_mut().map(|spec| &mut spec.expr));
    for root in roots {
        normalize(root, &mut columns, &mut nodes)?;
    }
    columns.sort();
    columns.dedup();
    Ok((columns, nodes))
}

fn normalize(expr: &mut Expr, columns: &mut Vec<String>, nodes: &mut usize) -> SimulatorResult<()> {
    if !matches!(expr, Expr::Literal(_) | Expr::RowNumber) {
        *nodes = nodes.saturating_add(1);
    }
    match expr {
        Expr::ColumnRef(column) => {
            if column.table.is_some() {
                return Err(error("unknown table qualifier"));
            }
            columns.push(column.column.clone());
        }
        Expr::FunctionCall { name, args } => {
            const FUNCTIONS: &[&str] = &[
                "abs",
                "near",
                "nearabs",
                "sqrt",
                "square",
                "pow",
                "sin",
                "cos",
                "tan",
                "asin",
                "acos",
                "atan",
                "atan2",
                "exp",
                "log",
                "log10",
                "floor",
                "ceil",
                "round",
                "sign",
                "fmod",
                "min",
                "max",
                "int",
                "real",
                "imag",
                "string",
                "upper",
                "lower",
                "trim",
                "ltrim",
                "rtrim",
                "length",
                "strlen",
                "substr",
                "shape",
                "ndim",
                "nelements",
                "isnan",
                "isinf",
                "isfinite",
                "iif",
                "any",
                "all",
                "ntrue",
                "nfalse",
                "sumsqr",
                "sumsquare",
                "rownumber",
                "rownr",
                "rowid",
                "rand",
                "pi",
                "e",
                "c",
            ];
            if name.eq_ignore_ascii_case("double") {
                return Err(error("unknown TaQL function double"));
            }
            if !FUNCTIONS
                .iter()
                .any(|function| name.eq_ignore_ascii_case(function))
            {
                return Err(super::super::query::unsupported(&format!(
                    "predicate function {name}"
                )));
            }
            for arg in args.iter_mut() {
                normalize(arg, columns, nodes)?;
            }
            if name.eq_ignore_ascii_case("square") && args.len() == 1 {
                let value = args[0].clone();
                *expr = Expr::Binary {
                    left: Box::new(value.clone()),
                    op: BinaryOp::Mul,
                    right: Box::new(value),
                };
            }
        }
        Expr::ArrayLiteral(values) => {
            for value in values {
                normalize(value, columns, nodes)?;
            }
        }
        Expr::Unary { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            normalize(operand, columns, nodes)?
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
        } => {
            normalize(left, columns, nodes)?;
            normalize(right, columns, nodes)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            normalize(expr, columns, nodes)?;
            normalize(low, columns, nodes)?;
            normalize(high, columns, nodes)?;
        }
        Expr::In { expr, values, .. } => {
            normalize(expr, columns, nodes)?;
            for value in values {
                normalize(value, columns, nodes)?;
            }
        }
        Expr::InSet { expr, elements, .. } => {
            normalize(expr, columns, nodes)?;
            for element in elements {
                match element {
                    InSetElement::Value(value) => normalize(value, columns, nodes)?,
                    InSetElement::Range { start, end, step } => {
                        for value in [start, end, step].into_iter().flatten() {
                            normalize(value, columns, nodes)?;
                        }
                    }
                }
            }
        }
        Expr::ArrayIndex { array, indices } => {
            normalize(array, columns, nodes)?;
            for index in indices {
                match index {
                    IndexElement::Single(value) => normalize(value, columns, nodes)?,
                    IndexElement::Slice { start, end, step } => {
                        for value in [start, end, step].into_iter().flatten() {
                            normalize(value, columns, nodes)?;
                        }
                    }
                }
            }
        }
        Expr::Literal(_) | Expr::RowNumber => {}
        _ => {
            return Err(super::super::query::unsupported(
                "this selection expression",
            ));
        }
    }
    Ok(())
}

pub(super) fn row_bytes(table: &Table, columns: &[String], nodes: usize) -> SimulatorResult<u64> {
    // Eager evaluation can retain one array per expression node, beside
    // the source cell and conversion/output buffers. Count the expression,
    // rather than assuming a constant number of live DATA arrays.
    let element_bytes = nodes
        .saturating_add(2)
        .saturating_mul(size_of::<ExprValue>())
        .saturating_add(16);
    let mut maximum = 0usize;
    for name in columns {
        let column = table.column_accessor(name).map_err(error)?;
        let schema = table
            .schema()
            .and_then(|schema| schema.column(name))
            .ok_or_else(|| error(format!("unknown column {name}")))?;
        if schema.data_type() == Some(casa_types::PrimitiveType::String) {
            return Err(super::super::query::unsupported("stored string columns"));
        }
        if matches!(schema.column_type(), casa_tables::ColumnType::Array(_)) {
            let mut elements = 0usize;
            for first in (0..table.row_count()).step_by(1024) {
                let rows: Vec<_> = (first..table.row_count().min(first + 1024)).collect();
                for shape in column
                    .array_shapes_uncached(&rows)
                    .map_err(error)?
                    .into_iter()
                    .flatten()
                {
                    elements = elements.max(
                        shape
                            .iter()
                            .try_fold(1usize, |n, size| n.checked_mul(*size))
                            .ok_or_else(|| error("selection cell size overflow"))?,
                    );
                }
            }
            maximum = maximum
                .checked_add(
                    elements
                        .checked_mul(element_bytes)
                        .ok_or_else(|| error("selection cell size overflow"))?,
                )
                .ok_or_else(|| error("selection cell size overflow"))?;
        } else {
            maximum = maximum.saturating_add(256);
        }
    }
    Ok(maximum as u64 + 4096)
}

pub(super) fn read_row(
    table: &Table,
    row: usize,
    columns: &[String],
    record: &mut RecordValue,
) -> SimulatorResult<()> {
    for (name, field) in columns.iter().zip(record.fields_mut()) {
        let column = table.column_accessor(name).map_err(error)?;
        field.value = if table
            .schema()
            .and_then(|schema| schema.column(name))
            .is_some_and(|schema| matches!(schema.column_type(), casa_tables::ColumnType::Array(_)))
        {
            let cell = column
                .array_cells_owned_uncached(&[row])
                .map_err(error)?
                .pop()
                .flatten()
                .ok_or_else(|| error(format!("undefined {name}")))?;
            Value::Array(cell)
        } else {
            Value::Scalar(
                column
                    .scalar_cells_owned_for_rows(&[row])
                    .map_err(error)?
                    .pop()
                    .flatten()
                    .ok_or_else(|| error(format!("undefined {name}")))?,
            )
        };
    }
    Ok(())
}

fn boolean(value: &ExprValue) -> bool {
    match value {
        ExprValue::Bool(_) => true,
        ExprValue::Array(array) => array.data.first().is_some_and(boolean),
        _ => false,
    }
}

/// Validate all arithmetic branches before a Boolean short circuit skips one.
pub(super) fn check(expr: &Expr, context: &EvalContext<'_>) -> SimulatorResult<()> {
    match expr {
        Expr::Binary { left, op, right } => {
            check(left, context)?;
            check(right, context)?;
            let a = eval_expr(left, context).map_err(error)?;
            let b = eval_expr(right, context).map_err(error)?;
            let (a, b) = (boolean(&a), boolean(&b));
            match op {
                BinaryOp::And | BinaryOp::Or if !a || !b => {
                    return Err(error("Boolean operands required"));
                }
                BinaryOp::Eq | BinaryOp::Ne if a != b => {
                    return Err(error("incompatible Boolean comparison"));
                }
                BinaryOp::And | BinaryOp::Or | BinaryOp::Eq | BinaryOp::Ne => {}
                _ if a || b => return Err(error("Boolean arithmetic is invalid")),
                _ => {}
            }
        }
        Expr::Unary { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            check(operand, context)?
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                check(arg, context)?;
            }
        }
        _ => {}
    }
    eval_expr(expr, context).map_err(error)?;
    Ok(())
}
