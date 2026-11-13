// SPDX-License-Identifier: LGPL-3.0-or-later
//! Freeze selections requiring stored cells or global query clauses.
mod expression;
mod key;
use super::{DataSelection, SimulatorResult};
use crate::session::Simulator;
use crate::session::error::SimulatorError;
use casa_imaging_runtime::{Demand, Reservation, admit};
use casa_ms::MeasurementSet;
use casa_tables::taql::{
    self,
    ast::*,
    eval::{EvalContext, ExprValue, eval_expr},
};
use casa_types::{RecordField, RecordValue, Value};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct SelectedRows {
    words: Vec<u64>,
    _memory: Reservation,
}
impl PartialEq for SelectedRows {
    fn eq(&self, other: &Self) -> bool {
        self.words == other.words
    }
}
impl SelectedRows {
    pub(super) fn contains(&self, row: u64) -> bool {
        self.words
            .get(row as usize / 64)
            .is_some_and(|word| word & (1 << (row % 64)) != 0)
    }
}

fn error(value: impl std::fmt::Display) -> SimulatorError {
    SimulatorError::refused(format!("Caught exception: {value}"))
}

struct MatchingRow {
    row: usize,
    order: Vec<ExprValue>,
    group: Vec<key::GroupKey>,
    having: bool,
}

pub(in crate::session) fn capture(
    session: &mut Simulator,
    base: DataSelection,
    source: &str,
) -> SimulatorResult<Option<DataSelection>> {
    let Statement::Select(mut select) =
        taql::parse(&format!("SELECT * WHERE {source}")).map_err(error)?
    else {
        unreachable!()
    };
    if select.giving.is_some()
        || !select.joins.is_empty()
        || select.distinct
        || (select.having.is_some() && select.group_by.is_empty())
    {
        return Err(super::query::unsupported("this query clause"));
    }
    let (columns, nodes) = expression::prepare(&mut select)?;
    let computed = columns
        .iter()
        .any(|name| matches!(name.as_str(), "DATA" | "MODEL_DATA" | "CORRECTED_DATA"));
    session
        .with_selection_ms(computed, |ms| {
            capture_from_ms(session, ms, &base, &select, &columns, nodes)
        })
        .map(|selected| {
            selected.map(|rows| DataSelection::Captured {
                base: Arc::new(base),
                rows: Arc::new(rows),
            })
        })
}

fn capture_from_ms(
    session: &Simulator,
    ms: &MeasurementSet,
    base: &DataSelection,
    select: &SelectStatement,
    columns: &[String],
    nodes: usize,
) -> SimulatorResult<Option<SelectedRows>> {
    let resources = &session.options().resources;
    let _reading = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "reading simulator selection cells",
            memory: 2 << 20,
        },
    )
    .map_err(SimulatorError::admission)?;
    let row_bytes = expression::row_bytes(ms.main_table(), columns, nodes)?;
    let global = !select.order_by.is_empty() || !select.group_by.is_empty();
    let mut memory = admit(
        resources.host(),
        resources.policy(),
        &Demand {
            phase: "capturing simulator TaQL rows",
            memory: (ms.row_count().div_ceil(64) as u64)
                .saturating_mul(8)
                .saturating_add(row_bytes)
                .saturating_add(if global {
                    (ms.row_count() as u64).saturating_mul(
                        512 + row_bytes
                            .saturating_mul((select.order_by.len() + select.group_by.len()) as u64),
                    )
                } else {
                    0
                })
                .saturating_add(512 << 10),
        },
    )
    .map_err(SimulatorError::admission)?;
    let words = {
        let mut words = vec![0u64; ms.row_count().div_ceil(64)];
        let mut matches: Vec<MatchingRow> = Vec::new();
        let predicate = select.where_clause.as_ref().expect("WHERE prefix");
        let mut row_record = RecordValue::new(
            columns
                .iter()
                .map(|name| {
                    RecordField::new(name, Value::Scalar(casa_types::ScalarValue::Int64(0)))
                })
                .collect(),
        );
        let mut rank = 0usize;
        let mut first = 0;
        let mut kept = 0usize;
        let mut checked = false;
        let empty = RecordValue::default();
        let context = EvalContext {
            row: &empty,
            row_index: 0,
            style: select.style,
            measures: Some(&session.options().measures),
        };
        let limit = select
            .limit
            .as_ref()
            .map(|expr| {
                eval_expr(expr, &context)
                    .and_then(|v| v.to_int())
                    .map_err(error)
            })
            .transpose()?
            .unwrap_or(i64::MAX)
            .max(0) as usize;
        let offset = select
            .offset
            .as_ref()
            .map(|expr| {
                eval_expr(expr, &context)
                    .and_then(|v| v.to_int())
                    .map_err(error)
            })
            .transpose()?
            .unwrap_or(0)
            .max(0) as usize;
        for scan in session.scans() {
            let start = first;
            first += scan.row_count();
            if !base.contains_scan(scan) {
                continue;
            }
            for row in start..first {
                expression::read_row(ms.main_table(), row, columns, &mut row_record)?;
                let context = EvalContext {
                    row: &row_record,
                    row_index: rank,
                    style: select.style,
                    measures: Some(&session.options().measures),
                };
                if !checked {
                    expression::check(predicate, &context)?;
                    checked = true;
                }
                rank += 1;
                let ExprValue::Bool(selected) = eval_expr(predicate, &context).map_err(error)?
                else {
                    return Err(error("selection predicate must be Boolean"));
                };
                if !selected {
                    continue;
                }
                if global {
                    let order = select
                        .order_by
                        .iter()
                        .map(|spec| eval_expr(&spec.expr, &context).map_err(error))
                        .collect::<SimulatorResult<Vec<_>>>()?;
                    for key in &order {
                        key.compare(key).map_err(error)?;
                    }
                    if let Some(first) = matches.first() {
                        for (key, reference) in order.iter().zip(&first.order) {
                            key.compare(reference).map_err(error)?;
                        }
                    }
                    let group = select
                        .group_by
                        .iter()
                        .map(|expr| key::GroupKey::new(eval_expr(expr, &context).map_err(error)?))
                        .collect::<SimulatorResult<Vec<_>>>()?;
                    let having = select.having.as_ref().map_or(Ok(true), |expr| {
                        expression::check(expr, &context)?;
                        match eval_expr(expr, &context).map_err(error)? {
                            ExprValue::Bool(value) => Ok(value),
                            _ => Err(error("HAVING predicate must be Boolean")),
                        }
                    })?;
                    matches.push(MatchingRow {
                        row,
                        order,
                        group,
                        having,
                    });
                } else {
                    if kept >= offset && kept - offset < limit {
                        words[row / 64] |= 1 << (row % 64);
                    }
                    kept += 1;
                }
            }
        }
        if global {
            select_global(matches, select, &mut words, offset, limit);
        }
        words
    };
    if words.iter().all(|word| *word == 0) {
        return Ok(None);
    }
    memory.retain((words.capacity() * 8 + size_of::<SelectedRows>()) as u64);
    Ok(Some(SelectedRows {
        words,
        _memory: memory,
    }))
}

fn select_global(
    mut matches: Vec<MatchingRow>,
    select: &SelectStatement,
    words: &mut [u64],
    offset: usize,
    limit: usize,
) {
    if !select.group_by.is_empty() {
        // TableParseGroupby applies each row to its group's function
        // set, whose stored TableExprId is the last row in the group.
        // The groups themselves retain their first-encounter order.
        let mut groups = std::collections::HashMap::new();
        let mut representatives = Vec::new();
        for matched in matches {
            if let Some(&index) = groups.get(&matched.group) {
                representatives[index] = matched;
            } else {
                groups.insert(matched.group.clone(), representatives.len());
                representatives.push(matched);
            }
        }
        matches = representatives;
        matches.retain(|row| row.having);
    }
    matches.sort_by(|left, right| {
        for ((left, right), spec) in left.order.iter().zip(&right.order).zip(&select.order_by) {
            let comparison = left
                .compare(right)
                .expect("compatible prevalidated sort keys");
            if !comparison.is_eq() {
                return if spec.ascending {
                    comparison
                } else {
                    comparison.reverse()
                };
            }
        }
        std::cmp::Ordering::Equal
    });
    for matched in matches.iter().skip(offset).take(limit) {
        words[matched.row / 64] |= 1 << (matched.row % 64);
    }
}
