// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bind casacore's MSCAL selection functions to the shared MS row grammars.

use casa_tables::taql::ast::{BinaryOp, Expr, InSetElement, Literal, SelectStatement};
use casa_tables::taql::{self, IndexStyle, RecordExpressionType, Statement, TaqlError, TaqlResult};
use casa_tables::{ColumnType, RequiredScalarColumnValues, TableError};
use casa_types::PrimitiveType;

use super::intent::MatchMode;
use super::taql_geometry::GeometryBinder;
use super::{
    MsSelection, RowGrammar, default_selection_budget, int_list, parser, row_grammar,
    rows_per_block_for_budget,
};
use crate::{MeasurementSet, MsError, MsResult};

pub(super) fn select_rows(
    ms: &MeasurementSet,
    query: &str,
    rows_per_block: Option<usize>,
    first_only: bool,
) -> MsResult<Vec<usize>> {
    let statement = selection_query(ms, query, rows_per_block)?;
    if let Statement::Select(select) = &statement {
        if first_only {
            return taql::first_matching_row(select, ms.main_table())
                .map(|row| row.into_iter().collect())
                .map_err(taql_error);
        }
        if let Some(rows) = select_simple_rows(ms, select, rows_per_block)? {
            return Ok(rows);
        }
    }
    match taql::exec::execute_readonly(&statement, ms.main_table()).map_err(taql_error)? {
        TaqlResult::Select { row_indices, .. } => Ok(row_indices),
        _ => Err(MsError::InvalidInput(
            "expected a TaQL SELECT statement".into(),
        )),
    }
}

fn select_simple_rows(
    ms: &MeasurementSet,
    select: &SelectStatement,
    rows_per_block: Option<usize>,
) -> MsResult<Option<Vec<usize>>> {
    let SelectStatement {
        columns,
        from: None,
        joins,
        where_clause: Some(predicate),
        group_by,
        having: None,
        order_by,
        order_distinct: false,
        limit: None,
        offset: None,
        distinct: false,
        style: _,
        giving: None,
        giving_expr: None,
    } = select
    else {
        return Ok(None);
    };
    if !(columns.is_empty() && joins.is_empty() && group_by.is_empty() && order_by.is_empty()) {
        return Ok(None);
    }
    let table = ms.main_table();
    let row_count = table.row_count();
    if let Expr::Literal(Literal::Bool(include)) = predicate {
        return Ok(Some(if *include {
            (0..row_count).collect()
        } else {
            Vec::new()
        }));
    }
    if let Some(rows) = literal_row_id_set(predicate, row_count) {
        return Ok(Some(rows));
    }
    let Expr::Binary { left, op, right } = predicate else {
        return Ok(None);
    };
    let (operand, literal, reverse) = match (left.as_ref(), right.as_ref()) {
        (operand, Expr::Literal(Literal::Int(literal))) => (operand, *literal, false),
        (Expr::Literal(Literal::Int(literal)), operand) => (operand, *literal, true),
        _ => return Ok(None),
    };
    let matches: fn(i64, i64) -> bool = match (op, reverse) {
        (BinaryOp::Eq, _) => |cell, other| cell == other,
        (BinaryOp::Ne, _) => |cell, other| cell != other,
        (BinaryOp::Lt, false) | (BinaryOp::Gt, true) => |cell, other| cell < other,
        (BinaryOp::Le, false) | (BinaryOp::Ge, true) => |cell, other| cell <= other,
        (BinaryOp::Gt, false) | (BinaryOp::Lt, true) => |cell, other| cell > other,
        (BinaryOp::Ge, false) | (BinaryOp::Le, true) => |cell, other| cell >= other,
        _ => return Ok(None),
    };

    if matches!(operand, Expr::RowNumber) {
        return Ok(Some(
            (0..row_count)
                .filter(|&row| matches(row as i64, literal))
                .collect(),
        ));
    }
    let Expr::ColumnRef(column) = operand else {
        return Ok(None);
    };
    let name = column.column.as_str();
    if column.table.is_some()
        || !table
            .schema()
            .and_then(|schema| schema.column(name))
            .is_some_and(|schema| {
                schema.column_type() == &ColumnType::Scalar
                    && schema.data_type() == Some(PrimitiveType::Int32)
                    && !schema.options().undefined
            })
    {
        return Ok(None);
    }

    // Predicates on IDs need one scalar column, even when the MS has large visibility arrays.
    let rows_per_block = match rows_per_block {
        Some(rows) => rows,
        None => rows_per_block_for_budget(default_selection_budget()?)
            .map_err(|error| MsError::InvalidInput(error.to_string()))?,
    };
    let mut selected_rows = Vec::new();
    for start in (0..row_count).step_by(rows_per_block) {
        let end = start.saturating_add(rows_per_block).min(row_count);
        let rows = (start..end).collect::<Vec<_>>();
        let mut columns = table.required_scalar_columns_owned_for_rows(&[name], &rows)?;
        let Some(RequiredScalarColumnValues::Int32(values)) = columns.remove(name) else {
            return Err(MsError::InvalidInput(format!(
                "expected a scalar integer column {name}"
            )));
        };
        selected_rows.extend(
            rows.into_iter()
                .zip(values)
                .filter_map(|(row, value)| matches(i64::from(value), literal).then_some(row)),
        );
    }
    Ok(Some(selected_rows))
}

fn literal_row_id_set(predicate: &Expr, row_count: usize) -> Option<Vec<usize>> {
    let Expr::InSet {
        expr,
        elements,
        negated: false,
    } = predicate
    else {
        return None;
    };
    if !matches!(expr.as_ref(), Expr::RowNumber) {
        return None;
    }
    let mut rows = Vec::new();
    for element in elements {
        let InSetElement::Value(Expr::Literal(Literal::Int(id))) = element else {
            return None;
        };
        if let Ok(row) = usize::try_from(*id)
            && row < row_count
        {
            rows.push(row);
        }
    }
    rows.sort_unstable();
    rows.dedup();
    Some(rows)
}

pub(super) fn validate_predicate(
    ms: &MeasurementSet,
    query: &str,
    rows_per_block: Option<usize>,
) -> MsResult<(RecordExpressionType, Option<bool>)> {
    if let Statement::Select(select) = selection_query(ms, query, rows_per_block)?
        && let Some(predicate) = &select.where_clause
    {
        let kind =
            taql::validate_record_expression(predicate, ms.main_table()).map_err(taql_error)?;
        let constant = match taql::eval::eval_constant(predicate, "TaQL", select.style) {
            Ok(taql::eval::ExprValue::Bool(value)) => Some(value),
            _ => None,
        };
        return Ok((kind, constant));
    }
    Ok((RecordExpressionType::PossibleRowPredicate, None))
}

fn selection_query(
    ms: &MeasurementSet,
    query: &str,
    rows_per_block: Option<usize>,
) -> MsResult<Statement> {
    let mut statement = taql::parse(query).map_err(taql_error)?;
    if let Statement::Select(select) = &mut statement
        && let Some(predicate) = &mut select.where_clause
    {
        let mut geometry = GeometryBinder::new(ms);
        bind_selectors(predicate, ms, select.style, rows_per_block, &mut geometry)?;
    }
    Ok(statement)
}

fn bind_selectors(
    expr: &mut Expr,
    ms: &MeasurementSet,
    style: IndexStyle,
    rows_per_block: Option<usize>,
    geometry: &mut GeometryBinder<'_>,
) -> MsResult<()> {
    if let Expr::FunctionCall { name, args } = expr {
        let resolved = match resolve_selector(name, args, ms, style, rows_per_block)? {
            Some(predicate) => Some(predicate),
            None => geometry.bind(name, args, style)?,
        };
        if let Some(resolved) = resolved {
            *expr = resolved;
            return Ok(());
        }
    }
    expr.try_for_each_child_mut(&mut |child| {
        bind_selectors(child, ms, style, rows_per_block, geometry)
    })
}

fn resolve_selector(
    name: &str,
    args: &[Expr],
    ms: &MeasurementSet,
    style: IndexStyle,
    rows_per_block: Option<usize>,
) -> MsResult<Option<Expr>> {
    let lower = name.to_ascii_lowercase();
    let Some(function) = lower
        .strip_prefix("mscal.")
        .or_else(|| lower.strip_prefix("derivedmscal."))
    else {
        return Ok(None);
    };
    if !matches!(
        function,
        "baseline" | "field" | "spw" | "array" | "uvdist" | "obs" | "state" | "time" | "scan"
    ) {
        return Ok(None);
    }
    let [arg] = args else {
        return Err(taql_error(TaqlError::ArgumentCount {
            name: name.to_owned(),
            expected: "1".into(),
            got: args.len(),
        }));
    };
    let text = taql::eval::eval_constant_string(arg, name, style).map_err(taql_error)?;
    if text.is_empty() {
        return Err(MsError::InvalidInput(format!(
            "{name} has an empty selector"
        )));
    }

    let query = match function {
        "baseline" => {
            let baseline = ms.resolve_antenna_selector_with_warnings(&text)?;
            format!(
                "SELECT * WHERE {}",
                row_grammar::baseline_predicate(&baseline)
            )
        }
        "field" => {
            let ids = ms.resolve_field_selector(&text)?;
            format!("SELECT * WHERE FIELD_ID IN [{}]", int_list(&ids))
        }
        "state" => {
            let ids = parser::parse_state_selector_with_mode(ms, &text, MatchMode::Taql)?;
            if ids.is_empty() {
                return Ok(Some(Expr::Literal(Literal::Bool(false))));
            }
            format!("SELECT * WHERE STATE_ID IN [{}]", int_list(&ids))
        }
        _ => {
            let mut request = MsSelection::default();
            match function {
                "spw" => request.spw = Some(text),
                "array" => request.array = Some(text),
                "uvdist" => request.uvrange = Some(text),
                "obs" => request.observation = Some(text),
                "time" => request.timerange = Some(text),
                "scan" => request.scan = Some(text),
                _ => unreachable!("checked MSCAL selection function"),
            }
            request
                .compile_with_row_grammar(ms, RowGrammar::Transform, rows_per_block)?
                .to_taql()
        }
    };
    let Statement::Select(select) = taql::parse(&query).map_err(taql_error)? else {
        unreachable!("selector generates a TaQL SELECT")
    };
    Ok(Some(
        select
            .where_clause
            .unwrap_or(Expr::Literal(Literal::Bool(true))),
    ))
}

pub(super) fn taql_error(error: TaqlError) -> MsError {
    TableError::Taql(error.to_string()).into()
}
