// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bind casacore's MSCAL selection functions to the shared MS row grammars.

use casa_tables::taql::ast::{
    BinaryOp, Expr, InSetElement, IndexElement, Literal, SelectStatement, UnaryOp,
};
use casa_tables::taql::{self, IndexStyle, RecordExpressionType, Statement, TaqlError, TaqlResult};
use casa_tables::{ArrayShapeContract, ColumnType, RequiredScalarColumnValues, Table, TableError};
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
        if let Some(rows) = select_simple_rows(ms.main_table(), select, rows_per_block, None)? {
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

enum ScalarComparison {
    Integer {
        literal: i64,
        compare: fn(i64, i64) -> bool,
    },
    IntegerSet(Vec<i64>),
    Boolean(bool),
}

impl ScalarComparison {
    fn new(literal: &Literal, op: BinaryOp, reverse: bool) -> Option<Self> {
        match literal {
            Literal::Bool(value) => match op {
                BinaryOp::Eq => Some(Self::Boolean(*value)),
                BinaryOp::Ne => Some(Self::Boolean(!*value)),
                _ => None,
            },
            Literal::Int(literal) => {
                let compare: fn(i64, i64) -> bool = match (op, reverse) {
                    (BinaryOp::Eq, _) => |cell, other| cell == other,
                    (BinaryOp::Ne, _) => |cell, other| cell != other,
                    (BinaryOp::Lt, false) | (BinaryOp::Gt, true) => |cell, other| cell < other,
                    (BinaryOp::Le, false) | (BinaryOp::Ge, true) => |cell, other| cell <= other,
                    (BinaryOp::Gt, false) | (BinaryOp::Lt, true) => |cell, other| cell > other,
                    (BinaryOp::Ge, false) | (BinaryOp::Le, true) => |cell, other| cell >= other,
                    _ => return None,
                };
                Some(Self::Integer {
                    literal: *literal,
                    compare,
                })
            }
            _ => None,
        }
    }

    fn from_predicate(predicate: &Expr) -> Option<(&Expr, Self)> {
        match predicate {
            Expr::Binary { left, op, right } => match (left.as_ref(), right.as_ref()) {
                (operand, Expr::Literal(literal)) => {
                    Some((operand, Self::new(literal, *op, false)?))
                }
                (Expr::Literal(literal), operand) => {
                    Some((operand, Self::new(literal, *op, true)?))
                }
                _ => None,
            },
            Expr::InSet {
                expr,
                elements,
                negated: false,
            } => {
                let mut values = elements
                    .iter()
                    .map(|element| match element {
                        InSetElement::Value(value) => integer_literal(value),
                        InSetElement::Range { .. } => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                values.sort_unstable();
                values.dedup();
                Some((expr, Self::IntegerSet(values)))
            }
            _ => None,
        }
    }

    fn primitive(&self) -> PrimitiveType {
        match self {
            Self::Integer { .. } | Self::IntegerSet(_) => PrimitiveType::Int32,
            Self::Boolean(_) => PrimitiveType::Bool,
        }
    }

    fn row_numbers(&self, row_count: usize) -> Option<Vec<usize>> {
        match self {
            Self::Integer { literal, compare } => Some(
                (0..row_count)
                    .filter(|&row| compare(row as i64, *literal))
                    .collect(),
            ),
            Self::IntegerSet(values) => Some(
                values
                    .iter()
                    .filter_map(|&value| usize::try_from(value).ok())
                    .take_while(|&row| row < row_count)
                    .collect(),
            ),
            Self::Boolean(_) => None,
        }
    }
}

/// Try a scalar TaQL predicate on an ordered projection of a MeasurementSet's
/// MAIN table. Returned row numbers are relative to the projection.
/// A more general expression returns `None` for the full TaQL planner.
///
/// # Errors
/// The expression has invalid syntax or a selected scalar cell cannot be read.
pub fn select_projected_scalar_rows(
    table: &Table,
    expression: &str,
    source_rows: Option<&[usize]>,
) -> MsResult<Option<Vec<usize>>> {
    let statement = taql::parse(&format!("SELECT * WHERE ({expression})")).map_err(taql_error)?;
    let Statement::Select(select) = statement else {
        return Ok(None);
    };
    select_simple_rows(table, &select, None, source_rows)
}

fn select_simple_rows(
    table: &Table,
    select: &SelectStatement,
    rows_per_block: Option<usize>,
    source_rows: Option<&[usize]>,
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
        style,
        giving: None,
        giving_expr: None,
    } = select
    else {
        return Ok(None);
    };
    if !(columns.is_empty() && joins.is_empty() && group_by.is_empty() && order_by.is_empty()) {
        return Ok(None);
    }
    let row_count = source_rows.map_or(table.row_count(), <[usize]>::len);
    if let Expr::Literal(Literal::Bool(include)) = predicate {
        return Ok(Some(if *include {
            (0..row_count).collect()
        } else {
            Vec::new()
        }));
    }
    if source_rows.is_none()
        && let Some((axis, lower, upper)) = projected_uvw_range(predicate, *style)
        && table
            .schema()
            .and_then(|schema| schema.column("UVW"))
            .is_some_and(|schema| {
                matches!(
                    schema.column_type(),
                    ColumnType::Array(ArrayShapeContract::Fixed { shape }) if shape == &[3]
                ) && schema.data_type() == Some(PrimitiveType::Float64)
                    && !schema.options().undefined
            })
    {
        let rows = select_projected_uvw_rows(
            table,
            axis,
            lower..=upper,
            selection_block_size(rows_per_block)?,
        )?;
        return Ok(Some(rows));
    }
    let Some((operand, comparison)) = ScalarComparison::from_predicate(predicate) else {
        return Ok(None);
    };
    if matches!(operand, Expr::RowNumber) {
        return Ok(comparison.row_numbers(row_count));
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
                    && schema.data_type() == Some(comparison.primitive())
                    && !schema.options().undefined
            })
    {
        return Ok(None);
    }

    // These predicates need only one scalar column, even when the MS has large visibility arrays.
    let rows_per_block = selection_block_size(rows_per_block)?;
    let mut selected_rows = Vec::new();
    for start in (0..row_count).step_by(rows_per_block) {
        let end = start.saturating_add(rows_per_block).min(row_count);
        let direct_rows;
        let source = if let Some(source_rows) = source_rows {
            &source_rows[start..end]
        } else {
            direct_rows = (start..end).collect::<Vec<_>>();
            &direct_rows
        };
        let mut columns = table.required_scalar_columns_owned_for_rows(&[name], source)?;
        match (&comparison, columns.remove(name)) {
            (
                ScalarComparison::Integer { literal, compare },
                Some(RequiredScalarColumnValues::Int32(values)),
            ) => {
                selected_rows.extend(
                    (start..end).zip(values).filter_map(|(row, value)| {
                        compare(i64::from(value), *literal).then_some(row)
                    }),
                );
            }
            (
                ScalarComparison::IntegerSet(set),
                Some(RequiredScalarColumnValues::Int32(values)),
            ) => {
                selected_rows.extend((start..end).zip(values).filter_map(|(row, value)| {
                    set.binary_search(&i64::from(value)).is_ok().then_some(row)
                }));
            }
            (
                ScalarComparison::Boolean(expected),
                Some(RequiredScalarColumnValues::Bool(values)),
            ) => {
                selected_rows.extend(
                    (start..end)
                        .zip(values)
                        .filter_map(|(row, value)| (value == *expected).then_some(row)),
                );
            }
            _ => {
                return Err(MsError::InvalidInput(format!(
                    "unexpected scalar values in column {name}"
                )));
            }
        }
    }
    Ok(Some(selected_rows))
}

fn selection_block_size(rows_per_block: Option<usize>) -> MsResult<usize> {
    match rows_per_block {
        Some(rows) => Ok(rows),
        None => rows_per_block_for_budget(default_selection_budget()?)
            .map_err(|error| MsError::InvalidInput(error.to_string())),
    }
}

fn integer_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(Literal::Int(value)) => Some(*value),
        Expr::Unary {
            op: UnaryOp::Negate,
            operand,
        } => match operand.as_ref() {
            Expr::Literal(Literal::Int(value)) => value.checked_neg(),
            _ => None,
        },
        _ => None,
    }
}

fn real_literal(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Literal(Literal::Float(value)) => Some(*value),
        Expr::Unary {
            op: UnaryOp::Negate,
            operand,
        } if let Expr::Literal(Literal::Float(value)) = operand.as_ref() => Some(-value),
        _ => integer_literal(expr).map(|value| value as f64),
    }
}

fn projected_uvw_range(predicate: &Expr, style: IndexStyle) -> Option<(usize, f64, f64)> {
    let Expr::Binary {
        left,
        op: BinaryOp::And,
        right,
    } = predicate
    else {
        return None;
    };
    let Expr::Binary {
        left: projection,
        op: BinaryOp::Ge,
        right: lower,
    } = left.as_ref()
    else {
        return None;
    };
    let Expr::Binary {
        left: upper_projection,
        op: BinaryOp::Le,
        right: upper,
    } = right.as_ref()
    else {
        return None;
    };
    let Expr::ArrayIndex { array, indices } = projection.as_ref() else {
        return None;
    };
    let Expr::ColumnRef(column) = array.as_ref() else {
        return None;
    };
    let [IndexElement::Single(Expr::Literal(Literal::Int(index)))] = indices.as_slice() else {
        return None;
    };
    if column.table.is_some() || column.column != "UVW" || projection != upper_projection {
        return None;
    }
    let axis = usize::try_from(index.checked_sub(i64::from(style.is_one_based()))?).ok()?;
    (axis < 3).then_some((axis, real_literal(lower)?, real_literal(upper)?))
}

fn select_projected_uvw_rows(
    table: &Table,
    axis: usize,
    range: std::ops::RangeInclusive<f64>,
    rows_per_block: usize,
) -> MsResult<Vec<usize>> {
    let row_count = table.row_count();
    let mut selected = Vec::new();
    for start in (0..row_count).step_by(rows_per_block) {
        let end = start.saturating_add(rows_per_block).min(row_count);
        let rows = (start..end).collect::<Vec<_>>();
        let values = super::columns::load_uvw_column(table, &rows)?;
        selected.extend(
            rows.into_iter()
                .zip(values)
                .filter_map(|(row, uvw)| range.contains(&uvw[axis]).then_some(row)),
        );
    }
    Ok(selected)
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
