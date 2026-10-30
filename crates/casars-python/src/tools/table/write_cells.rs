// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tool's cell and column writes (`putcell`, `putcol`,
//! `putvarcol`), as CASA 6.7.6.14's `tb` does them
//! (`tools/casa-compat/table_tool_oracle.py`, `cells_write` and
//! `ms_cells_write`).
//!
//! A value is coerced to the column's declared type ([`super::cell_value`]),
//! checked against the column's shape, set in memory, and the cells written
//! in place in the column's storage manager with
//! `TableWritePlan::save_selected_rows`; no other cell and no other storage
//! manager is touched. The lock handling is [`State::write_with`]'s.

use casa_tables::{ArrayShapeContract, ColumnSchema, ColumnType, Table};
use casa_types::{PrimitiveType, Value};

use super::keyword_value::{Data, Incoming, Wide};
use super::read::rows;
use super::state::{Fail, State};
use super::write::{Persist, Target};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

fn runtime<T>(message: String) -> Result<T, Fail> {
    Err(Fail::Runtime(message))
}

fn put_column_error(what: &str) -> Fail {
    Fail::Runtime(format!("TableProxy::TableProxy::putColumn: {what}"))
}

/// What a column holds: its type (`None`: records) and whether its cells are
/// arrays.
fn describe(table: &Table, column: &str) -> Result<(Option<PrimitiveType>, bool), Fail> {
    let schema: &ColumnSchema = table
        .schema()
        .and_then(|schema| schema.column(column))
        .ok_or_else(|| put_column_error(&format!("column {column} does not exist")))?;
    Ok((
        schema.data_type(),
        matches!(schema.column_type(), ColumnType::Array(_)),
    ))
}

fn shape_text(shape: &[usize]) -> String {
    let parts: Vec<String> = shape.iter().map(ToString::to_string).collect();
    format!("[{}]", parts.join(", "))
}

/// The refusal of an array whose shape the column's cell cannot take.
fn conformance(row: usize, column: &str, shape: &[usize], expected: &[usize]) -> Fail {
    Fail::Runtime(format!(
        "ArrayColumn::put for row {row} in column {column}: Table array conformance error \
         (shape={}, expected {})",
        shape_text(shape),
        shape_text(expected)
    ))
}

/// Whether the column's storage manager holds one shape per cell once set
/// (the tiled managers do; the indirect ones let a cell be reshaped).
fn keeps_cell_shapes(table: &Table, column: &str) -> bool {
    table.data_manager_info().iter().any(|manager| {
        manager.dm_type.contains("Tiled") && manager.columns.iter().any(|c| c == column)
    })
}

/// Checks an array cell's shape against its column's.
fn check_shape(table: &Table, column: &str, row: usize, cell: &Value) -> Result<(), Fail> {
    let Value::Array(array) = cell else {
        return Ok(());
    };
    let shape = array.shape();
    let contract = table
        .schema()
        .and_then(|schema| schema.column(column))
        .map(ColumnSchema::column_type);
    if let Some(ColumnType::Array(ArrayShapeContract::Fixed { shape: fixed })) = contract
        && shape != fixed.as_slice()
    {
        return Err(conformance(row, column, shape, fixed));
    }
    if keeps_cell_shapes(table, column)
        && let Some(existing) = table.array_shape(row, column)?
        && existing != shape
    {
        return Err(conformance(row, column, shape, &existing));
    }
    Ok(())
}

/// Sets one cell in memory.
fn set_cell(table: &mut Table, column: &str, row: usize, cell: Value) -> Result<(), Fail> {
    check_shape(table, column, row, &cell)?;
    let mut accessor = table.column_accessor_mut(column)?;
    match cell {
        Value::Record(record) => accessor.set_record(row, record)?,
        other => accessor.set(row, other)?,
    }
    Ok(())
}

/// The elements of `data` at `indices`.
fn take(data: &Data, indices: &[usize]) -> Data {
    match data {
        Data::Bools(values) => Data::Bools(indices.iter().map(|i| values[*i]).collect()),
        Data::Ints(values) => Data::Ints(indices.iter().map(|i| values[*i]).collect()),
        Data::Floats(values, single) => {
            Data::Floats(indices.iter().map(|i| values[*i]).collect(), *single)
        }
        Data::Complexes(values, single) => {
            Data::Complexes(indices.iter().map(|i| values[*i]).collect(), *single)
        }
        Data::Texts(values) => Data::Texts(indices.iter().map(|i| values[*i].clone()).collect()),
    }
}

/// A column's values split into one value per row: the rows are the last
/// axis of an array column's values (NumPy's, in C order), the only axis of
/// a scalar column's.
fn split_rows(wide: Wide, array: bool, column: &str, wanted: usize) -> Result<Vec<Wide>, Fail> {
    let shape = wide.shape.clone().unwrap_or_else(|| vec![1]);
    let (cell_shape, nrows) = if array {
        match shape.split_last() {
            Some((rows, cell)) if !cell.is_empty() => (Some(cell.to_vec()), *rows),
            _ => (None, usize::MAX),
        }
    } else {
        (
            None,
            if shape.len() == 1 {
                shape[0]
            } else {
                usize::MAX
            },
        )
    };
    if nrows != wanted {
        return runtime(if array {
            format!(
                "ArrayColumn::putColumn - column {column} has {wanted}, array has {} rows: \
                 Table array conformance error",
                if nrows == usize::MAX { 0 } else { nrows }
            )
        } else {
            "ScalarColumn::putColumn(Vector&): Table conformance error (#rows mismatch)".into()
        });
    }
    let per_row: usize = cell_shape.as_ref().map_or(1, |cell| cell.iter().product());
    Ok((0..nrows)
        .map(|row| {
            let indices: Vec<usize> = (0..per_row).map(|element| element * nrows + row).collect();
            Wide {
                shape: cell_shape.clone(),
                data: take(&wide.data, &indices),
            }
        })
        .collect())
}

impl State {
    /// `putcell(columnname, rownr, thevalue)`.
    pub(super) fn putcell(
        &mut self,
        column: String,
        row: i64,
        value: Incoming,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        self.write_with(Target::Cells, log, move |table| {
            let (ty, array) = describe(table, &column)?;
            let row = usize::try_from(row)
                .ok()
                .filter(|row| *row < table.row_count())
                .ok_or_else(|| put_column_error("no such row"))?;
            let cell = value.into_cell(ty, array)?;
            set_cell(table, &column, row, cell)?;
            Ok(Persist::Rows {
                column,
                rows: vec![row],
            })
        })
    }

    /// `putcol(columnname, value, startrow, nrow, rowincr)`.
    pub(super) fn putcol(
        &mut self,
        column: String,
        value: Incoming,
        selection: (i64, i64, i64),
        log: &mut Vec<LogLine>,
    ) -> Reply {
        self.write_with(Target::Cells, log, move |table| {
            let (ty, array) = describe(table, &column)?;
            let selected = rows(
                table.row_count(),
                selection.0,
                selection.1,
                selection.2,
                "TableProxy::TableProxy::putColumn: no such row",
            )?;
            let cells = match (value, ty) {
                (Incoming::Wide(wide), Some(_)) => {
                    split_rows(wide, array, &column, selected.len())?
                }
                (value, _) => {
                    // A record column is written a cell at a time with
                    // `putcell`; `putcol` of one is not possible.
                    let _ = value;
                    return runtime(
                        "TableProxy::putColumn not possible for a column containing records".into(),
                    );
                }
            };
            for (row, cell) in selected.iter().zip(cells) {
                let cell = cell.into_cell(ty.expect("typed column"), array)?;
                set_cell(table, &column, *row, cell)?;
            }
            Ok(Persist::Rows {
                column,
                rows: selected,
            })
        })
    }

    /// `putvarcol(columnname, value, startrow, nrow, rowincr)`: a record of
    /// `r1`, `r2`, ... holding each row's value with a trailing axis of one,
    /// as `getvarcol` returns them.
    pub(super) fn putvarcol(
        &mut self,
        column: String,
        fields: Vec<(String, Incoming)>,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        self.write_with(Target::Cells, log, move |table| {
            let (ty, array) = describe(table, &column)?;
            let Some(ty) = ty else {
                return runtime(
                    "TableProxy::putVarColumn not possible for a column containing records".into(),
                );
            };
            let mut written = Vec::with_capacity(fields.len());
            for (key, value) in fields {
                let Incoming::Wide(wide) = value else {
                    return runtime(format!("a value of {key} is not an array"));
                };
                let row = key
                    .strip_prefix('r')
                    .and_then(|number| number.parse::<usize>().ok())
                    .and_then(|number| number.checked_sub(1))
                    .filter(|row| *row < table.row_count())
                    .ok_or_else(|| put_column_error("no such row"))?;
                let mut shape = wide.shape.clone().unwrap_or_else(|| vec![1]);
                if array && shape.len() < 2 || shape.pop() != Some(1) {
                    return runtime(format!(
                        "Invalid Table operation: ArrayColumn::setShape: mismatch in #dim of array of column {column}"
                    ));
                }
                let cell = Wide {
                    shape: array.then_some(shape),
                    data: wide.data,
                };
                set_cell(table, &column, row, cell.into_cell(ty, array)?)?;
                written.push(row);
            }
            Ok(Persist::Rows {
                column,
                rows: written,
            })
        })
    }
}
