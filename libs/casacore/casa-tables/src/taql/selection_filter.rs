// SPDX-License-Identifier: LGPL-3.0-or-later
//! Physical first-row search for MeasurementSet time defaults.

use std::collections::HashSet;

use casa_core::{RecordField, RecordValue, Value};

use super::TaqlError;
use super::ast::{BinaryOp, Expr, SelectStatement};
use super::eval::{EvalContext, eval_expr};

/// Find the first physical row satisfying a prepared selection's WHERE
/// expression. This is used for MeasurementSet time defaults, for which
/// ORDER BY and LIMIT are not part of the selection grammar.
/// # Errors
/// The predicate cannot be prepared or evaluated for a candidate row.
#[doc(hidden)]
pub fn first_matching_row(
    sel: &SelectStatement,
    table: &crate::Table,
) -> Result<Option<usize>, TaqlError> {
    let Some(mut predicate) = sel.where_clause.clone() else {
        return Ok((table.row_count() > 0).then_some(0));
    };
    super::mscal_lookup::bind(&mut predicate, table, sel.style)?;
    super::mscal_help::fold_filter(&mut predicate, table, sel.style)?;
    let mut columns = HashSet::new();
    let mut all_columns = false;
    collect_filter_columns(&mut predicate, &mut columns, &mut all_columns);
    let schema = table.schema();
    let columns = schema.map(|schema| {
        schema
            .columns()
            .iter()
            .filter(|column| all_columns || columns.contains(column.name()))
            .collect::<Vec<_>>()
    });
    for row_index in 0..table.row_count() {
        let row;
        let fields = if let Some(columns) = &columns {
            row = read_expression_row(table, row_index, columns)?;
            &row
        } else {
            table
                .row_accessor()
                .row(row_index)
                .map_err(|error| TaqlError::Table(error.to_string()))?
        };
        let context = EvalContext {
            row: fields,
            row_index,
            style: sel.style,
            measures: table.measures_provider(),
        };
        if matches_where(&predicate, &context)? {
            return Ok(Some(row_index));
        }
    }
    Ok(None)
}

fn collect_filter_columns(expr: &mut Expr, names: &mut HashSet<String>, all_columns: &mut bool) {
    match expr {
        Expr::ColumnRef(column) => {
            names.insert(column.column.clone());
        }
        Expr::BoundRowFunction(_) => *all_columns = true,
        Expr::FunctionCall { name, .. } if super::functions::has_udf(name) => *all_columns = true,
        _ => {}
    }
    let Ok(()) = expr.try_for_each_child_mut(&mut |child| {
        collect_filter_columns(child, names, all_columns);
        Ok::<(), std::convert::Infallible>(())
    });
}

fn read_expression_row(
    table: &crate::Table,
    row_index: usize,
    columns: &[&crate::ColumnSchema],
) -> Result<RecordValue, TaqlError> {
    use crate::ColumnType;
    let convert_error = |error: crate::TableError| TaqlError::Table(error.to_string());
    let fields = columns
        .iter()
        .map(|column| {
            let name = column.name();
            let accessor = table.column_accessor(name).map_err(convert_error)?;
            let value = match column.column_type() {
                ColumnType::Scalar => accessor
                    .scalar_cells_owned_for_rows(&[row_index])
                    .map_err(convert_error)?
                    .pop()
                    .flatten()
                    .map(Value::Scalar),
                ColumnType::Array(_) => accessor
                    .array_cells_owned_uncached(&[row_index])
                    .map_err(convert_error)?
                    .pop()
                    .flatten()
                    .map(Value::Array),
                ColumnType::Record => Some(Value::Record(
                    accessor.record_cell(row_index).map_err(convert_error)?,
                )),
            };
            Ok(value.map(|value| RecordField {
                name: name.into(),
                value,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordValue::new(fields.into_iter().flatten().collect()))
}

fn matches_where(expr: &Expr, context: &EvalContext<'_>) -> Result<bool, TaqlError> {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => Ok(matches_where(left, context)? || matches_where(right, context)?),
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => Ok(matches_where(left, context)? && matches_where(right, context)?),
        _ => eval_expr(expr, context)?.to_bool(),
    }
}
