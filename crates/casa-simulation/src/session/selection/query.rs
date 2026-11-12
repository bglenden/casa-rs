// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded evaluation of row-header TaQL using the existing table evaluator.

use std::sync::Arc;

use casa_imaging_runtime::{Demand, admit, charge_process_caches};
use casa_ms::MeasurementSet;
use casa_tables::taql::{
    self,
    ast::{Expr, InSetElement, IndexStyle, Literal, Statement},
    eval::{EvalContext, ExprValue, eval_expr},
};
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};
use casa_types::measures::MeasuresProvider;
use casa_types::{RecordField, RecordValue, ScalarValue, Value};

use super::super::Simulator;
use super::super::error::{SimulatorError, SimulatorResult};
use super::super::observe::{Scan, ScanRows};
use super::DataSelection;

const HEADER_COLUMNS: [&str; 6] = [
    "ANTENNA1",
    "ANTENNA2",
    "TIME",
    "FIELD_ID",
    "DATA_DESC_ID",
    "SCAN_NUMBER",
];
const READ_ROWS: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RowHeader {
    pub(crate) antennas: [usize; 2],
    pub(crate) time_s: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowQuery {
    expression: Expr,
    style: IndexStyle,
    scratch_bytes: u64,
}

impl RowQuery {
    pub(crate) fn parse(source: &str) -> SimulatorResult<Self> {
        let statement = taql::parse(&format!("SELECT * WHERE {source}")).map_err(query_error)?;
        let Statement::Select(select) = statement else {
            unreachable!("SELECT prefix")
        };
        if select.distinct
            || !select.joins.is_empty()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || !select.order_by.is_empty()
            || select.limit.is_some()
            || select.offset.is_some()
            || select.giving.is_some()
        {
            return Err(unsupported("query clauses beyond a row predicate"));
        }
        let expression = select.where_clause.ok_or_else(|| {
            SimulatorError::refused("Caught exception: missing selection predicate")
        })?;
        let scratch_bytes = validate(&expression)?;
        Ok(Self {
            expression,
            style: select.style,
            scratch_bytes,
        })
    }

    pub(crate) fn scratch_bytes(&self) -> u64 {
        self.scratch_bytes
    }
}

fn unsupported(what: &str) -> SimulatorError {
    SimulatorError::not_yet_implemented(format!(
        "setdata {what} is not yet implemented in casa-rs (#1053)"
    ))
}

fn query_error(error: taql::TaqlError) -> SimulatorError {
    match error {
        taql::TaqlError::Unsupported { .. } | taql::TaqlError::UnknownFunction { .. } => {
            unsupported(&error.to_string())
        }
        _ => SimulatorError::refused(format!("Caught exception: {error}")),
    }
}

fn validate(expression: &Expr) -> SimulatorResult<u64> {
    let mut stack = vec![expression];
    let mut scratch_bytes = 0_u64;
    while let Some(expression) = stack.pop() {
        match expression {
            Expr::Literal(
                Literal::Int(_) | Literal::Float(_) | Literal::Bool(_) | Literal::Complex(_),
            ) => {}
            Expr::ColumnRef(column) => {
                if column.table.is_some() {
                    return Err(unsupported("qualified columns"));
                }
                if !HEADER_COLUMNS.contains(&column.column.as_str()) {
                    let known = casa_ms::schema::main_table::REQUIRED_COLUMNS
                        .iter()
                        .chain(casa_ms::schema::main_table::OPTIONAL_COLUMNS)
                        .any(|definition| definition.name == column.column);
                    return Err(if known {
                        unsupported(&format!("predicate column {}", column.column))
                    } else {
                        SimulatorError::refused(format!(
                            "Caught exception: column {} not found",
                            column.column
                        ))
                    });
                }
            }
            Expr::Unary { operand, .. } | Expr::IsNull { expr: operand, .. } => stack.push(operand),
            Expr::Binary { left, right, .. } => {
                stack.push(left);
                stack.push(right);
            }
            Expr::FunctionCall { name, args } => {
                let arity_ok = if name.eq_ignore_ascii_case("abs") {
                    args.len() == 1
                } else if name.eq_ignore_ascii_case("near") || name.eq_ignore_ascii_case("nearabs")
                {
                    (2..=3).contains(&args.len())
                } else {
                    return Err(unsupported(&format!("predicate function {name}")));
                };
                if !arity_ok {
                    return Err(SimulatorError::refused(format!(
                        "Caught exception: invalid {name} argument count"
                    )));
                }
                // The shared evaluator collects arguments in a short Vec.
                // These scalar functions return no dynamically sized result.
                scratch_bytes = scratch_bytes.saturating_add((4 * size_of::<ExprValue>()) as u64);
                stack.extend(args);
            }
            Expr::Between {
                expr, low, high, ..
            } => stack.extend([expr.as_ref(), low, high]),
            Expr::In { expr, values, .. } => {
                stack.push(expr);
                stack.extend(values);
            }
            Expr::Like { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
                stack.push(expr);
                stack.push(pattern);
            }
            Expr::InSet { expr, elements, .. } => {
                stack.push(expr);
                for element in elements {
                    match element {
                        InSetElement::Value(value) => stack.push(value),
                        InSetElement::Range { start, end, step } => {
                            stack.extend([start, end, step].into_iter().flatten())
                        }
                    }
                }
            }
            _ => return Err(unsupported("this predicate expression")),
        }
    }
    Ok(scratch_bytes)
}

pub(crate) struct QueryWorkspace {
    row: RecordValue,
}

impl QueryWorkspace {
    pub(crate) fn new() -> Self {
        Self {
            row: RecordValue::new(
                HEADER_COLUMNS
                    .into_iter()
                    .map(|name| RecordField::new(name, Value::Scalar(ScalarValue::Int64(0))))
                    .collect(),
            ),
        }
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.row.retained_heap_bytes().unwrap_or(usize::MAX) as u64
    }

    pub(crate) fn evaluate(
        &mut self,
        query: &RowQuery,
        scan: &Scan,
        header: RowHeader,
        measures: &Arc<dyn MeasuresProvider>,
    ) -> SimulatorResult<bool> {
        let values = [
            ScalarValue::Int64(header.antennas[0] as i64),
            ScalarValue::Int64(header.antennas[1] as i64),
            ScalarValue::Float64(header.time_s),
            ScalarValue::Int64(scan.field_id as i64),
            ScalarValue::Int64(scan.spectral_window_id as i64),
            ScalarValue::Int64(i64::from(scan.scan_number)),
        ];
        for (field, value) in self.row.fields_mut().iter_mut().zip(values) {
            field.value = Value::Scalar(value);
        }
        let value = eval_expr(
            &query.expression,
            &EvalContext {
                row: &self.row,
                row_index: 0,
                style: query.style,
                measures: Some(measures),
            },
        )
        .map_err(query_error)?;
        match value {
            ExprValue::Bool(value) => Ok(value),
            _ => Err(SimulatorError::refused(
                "Caught exception: selection predicate must be boolean",
            )),
        }
    }
}

impl Simulator {
    pub(in crate::session) fn query_keeps_rows(
        &self,
        selection: &DataSelection,
    ) -> SimulatorResult<bool> {
        let resources = &self.options.resources;
        let _reservation = admit(
            resources.host(),
            resources.policy(),
            &Demand {
                phase: "the simulator's row selection",
                memory: (512_u64 << 10).saturating_add(selection.evaluation_bytes()),
            },
        )
        .map_err(SimulatorError::admission)?;
        let mut workspace = QueryWorkspace::new();
        let mut found = false;
        if self.opened.is_some() {
            charge_process_caches(resources.host(), resources.policy())
                .map_err(SimulatorError::admission)?;
            let ms = MeasurementSet::open(self.path())
                .map_err(|error| SimulatorError::raised(format!("setdata: {error}")))?;
            let main = ms.main_table();
            let (mut indices, mut first, mut second, mut times) =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for scan in &self.scans {
                if !selection.contains_scan(scan) {
                    continue;
                }
                let ScanRows::OnDisk(rows) = &scan.rows else {
                    unreachable!("opened MS")
                };
                for start in (rows.start..rows.end).step_by(READ_ROWS) {
                    indices.clear();
                    indices.extend(start..(start + READ_ROWS).min(rows.end));
                    first.clear();
                    second.clear();
                    times.clear();
                    main.required_scalar_columns_for_rows_into(
                        &indices,
                        &mut [
                            RequiredScalarColumnDestination::new(
                                "ANTENNA1",
                                RequiredScalarColumnValuesMut::Int32(&mut first),
                            ),
                            RequiredScalarColumnDestination::new(
                                "ANTENNA2",
                                RequiredScalarColumnValuesMut::Int32(&mut second),
                            ),
                            RequiredScalarColumnDestination::new(
                                "TIME",
                                RequiredScalarColumnValuesMut::Float64(&mut times),
                            ),
                        ],
                    )
                    .map_err(|error| {
                        SimulatorError::raised(format!("setdata: row headers: {error}"))
                    })?;
                    for ((&a, &b), &time_s) in first.iter().zip(&second).zip(&times) {
                        let antennas = [usize::try_from(a), usize::try_from(b)].map(|value| {
                            value.map_err(|_| {
                                SimulatorError::raised("setdata: negative antenna index")
                            })
                        });
                        let [a, b] = antennas;
                        found |= selection.matches_row(
                            scan,
                            RowHeader {
                                antennas: [a?, b?],
                                time_s,
                            },
                            &mut workspace,
                            &self.options.measures,
                        )?;
                    }
                }
            }
        } else {
            for scan in &self.scans {
                if !selection.contains_scan(scan) {
                    continue;
                }
                let ScanRows::Planned(rows) = &scan.rows else {
                    unreachable!("fresh session")
                };
                for time_s in rows.integration_times_s() {
                    for first in 0..rows.antenna_count {
                        let start = first + usize::from(rows.autocorrelation_weight <= 0.0);
                        for second in start..rows.antenna_count {
                            found |= selection.matches_row(
                                scan,
                                RowHeader {
                                    antennas: [first, second],
                                    time_s,
                                },
                                &mut workspace,
                                &self.options.measures,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(found)
    }
}
