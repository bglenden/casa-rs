// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bind casacore's MSCAL selection functions to the shared MS row grammars.

use casa_tables::TableError;
use casa_tables::taql::ast::{Expr, Literal};
use casa_tables::taql::{self, IndexStyle, Statement, TaqlError, TaqlResult};

use super::intent::MatchMode;
use super::taql_geometry::GeometryBinder;
use super::{MsSelection, RowGrammar, int_list, parser, row_grammar};
use crate::{MeasurementSet, MsError, MsResult};

pub(super) fn select_rows(
    ms: &MeasurementSet,
    query: &str,
    rows_per_block: Option<usize>,
) -> MsResult<Vec<usize>> {
    let mut statement = taql::parse(query).map_err(taql_error)?;
    if let Statement::Select(select) = &mut statement
        && let Some(predicate) = &mut select.where_clause
    {
        let mut geometry = GeometryBinder::new(ms);
        bind_selectors(predicate, ms, select.style, rows_per_block, &mut geometry)?;
    }
    match taql::exec::execute_readonly(&statement, ms.main_table()).map_err(taql_error)? {
        TaqlResult::Select { row_indices, .. } => Ok(row_indices),
        _ => Err(MsError::InvalidInput(
            "expected a TaQL SELECT statement".into(),
        )),
    }
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
