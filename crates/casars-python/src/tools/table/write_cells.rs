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
use super::read::{rows, rows_stepping};
use super::state::{Fail, State};
use super::write::{Persist, Target};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

fn runtime<T>(message: String) -> Result<T, Fail> {
    Err(Fail::Runtime(message))
}

pub(super) fn put_column_error(what: &str) -> Fail {
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
pub(super) fn conformance(row: usize, column: &str, shape: &[usize], expected: &[usize]) -> Fail {
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

/// What a column asks of the shape of the arrays written to it, worked out
/// once for all the cells of a call.
struct ShapeRule {
    /// The one shape every cell has.
    fixed: Option<Vec<usize>>,
    /// Whether a cell keeps the shape it has once set (a tiled manager).
    kept: bool,
}

impl ShapeRule {
    fn of(table: &Table, column: &str) -> Self {
        let contract = table
            .schema()
            .and_then(|schema| schema.column(column))
            .map(ColumnSchema::column_type);
        Self {
            fixed: match contract {
                Some(ColumnType::Array(ArrayShapeContract::Fixed { shape })) => Some(shape.clone()),
                _ => None,
            },
            kept: keeps_cell_shapes(table, column),
        }
    }

    /// Checks an array cell's shape against its column's.
    fn check(&self, table: &Table, column: &str, row: usize, cell: &Value) -> Result<(), Fail> {
        let Value::Array(array) = cell else {
            return Ok(());
        };
        let shape = array.shape();
        if let Some(fixed) = &self.fixed
            && shape != fixed.as_slice()
        {
            return Err(conformance(row, column, shape, fixed));
        }
        if self.kept
            && let Some(existing) = table.array_shape(row, column)?
            && existing != shape
        {
            return Err(conformance(row, column, shape, &existing));
        }
        Ok(())
    }
}

/// Sets cells of one column in memory: each cell is checked, and none is
/// set before every one has been (a refused call changes nothing). A cell
/// of a scalar or array column is set where it is, without loading the
/// rest of the table (a table is read lazily, and a MeasurementSet's visibilities
/// would not fit in memory); a record cell goes through the row store.
fn set_cells(
    table: &mut Table,
    column: &str,
    cells: Vec<(usize, Value)>,
) -> Result<(), Fail> {
    let record_column = matches!(
        table
            .schema()
            .and_then(|schema| schema.column(column))
            .map(ColumnSchema::column_type),
        Some(ColumnType::Record)
    );
    let rule = ShapeRule::of(table, column);
    for (row, cell) in &cells {
        rule.check(table, column, *row, cell)?;
    }
    if record_column || cells.iter().any(|(_, cell)| matches!(cell, Value::Record(_))) {
        let mut accessor = table.column_accessor_mut(column)?;
        for (row, cell) in cells {
            match cell {
                Value::Record(record) => accessor.set_record(row, record)?,
                other => accessor.set(row, other)?,
            }
        }
        return Ok(());
    }
    let mut writer = table.row_accessor_mut().prepare(&[column])?;
    for (row, cell) in cells {
        writer.seek(row)?;
        writer.set_value_at(0, cell)?;
    }
    writer.flush()?;
    Ok(())
}

/// The distinct rows of `rows`, in order, as a save of selected rows wants
/// them.
fn distinct(rows: &[usize]) -> Vec<usize> {
    let mut seen = std::collections::HashSet::with_capacity(rows.len());
    rows.iter().copied().filter(|row| seen.insert(*row)).collect()
}

/// The elements of `data` at `indices`.
fn take(data: &Data, indices: &[usize]) -> Data {
    match data {
        Data::Bools(values) => Data::Bools(indices.iter().map(|i| values[*i]).collect()),
        Data::Ints(values) => Data::Ints(indices.iter().map(|i| values[*i]).collect()),
        Data::Floats(values) => Data::Floats(indices.iter().map(|i| values[*i]).collect()),
        Data::Complexes(values) => Data::Complexes(indices.iter().map(|i| values[*i]).collect()),
        Data::Texts(values) => Data::Texts(indices.iter().map(|i| values[*i].clone()).collect()),
    }
}

/// A column's values split into one value per row: the rows are the last
/// axis of an array column's values (NumPy's, in C order), the only axis of
/// a scalar column's.
pub(super) fn split_rows(
    wide: Wide,
    array: bool,
    column: &str,
    wanted: usize,
) -> Result<Vec<Wide>, Fail> {
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
    /// `putcell(columnname, rownr, thevalue)`: one row number, or several,
    /// each given the value. A row that does not exist refuses the call
    /// whole (CASA writes the rows before it, then fails).
    pub(super) fn putcell(
        &mut self,
        column: String,
        rows: Vec<i64>,
        value: Incoming,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        self.write_with(Target::Cells, log, move |table| {
            let (ty, array) = describe(table, &column)?;
            let nrows = table.row_count();
            let rows = rows
                .into_iter()
                .map(|row| {
                    usize::try_from(row)
                        .ok()
                        .filter(|row| *row < nrows)
                        .ok_or_else(|| put_column_error("no such row"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if rows.is_empty() {
                return Ok(Persist::Nothing);
            }
            let cell = value.into_cell(ty, array)?;
            let rows = distinct(&rows);
            set_cells(
                table,
                &column,
                rows.iter().map(|row| (*row, cell.clone())).collect(),
            )?;
            Ok(Persist::Rows { column, rows })
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
            let cells = selected
                .iter()
                .zip(cells)
                .map(|(row, cell)| Ok((*row, cell.into_cell(ty.expect("typed column"), array)?)))
                .collect::<Result<Vec<_>, Fail>>()?;
            if cells.is_empty() {
                return Ok(Persist::Nothing);
            }
            set_cells(table, &column, cells)?;
            Ok(Persist::Rows {
                column,
                rows: selected,
            })
        })
    }

    /// `putvarcol(columnname, value, startrow, nrow, rowincr)`: a record
    /// whose fields, in the order they have, are the values of the selected
    /// rows, each with a trailing axis of one, as `getvarcol` returns them.
    /// The field names are labels (`getvarcol` names them `r1`, `r2`, ...),
    /// not row numbers.
    pub(super) fn putvarcol(
        &mut self,
        column: String,
        fields: Vec<(String, Incoming)>,
        selection: (i64, i64, i64),
        log: &mut Vec<LogLine>,
    ) -> Reply {
        self.write_with(Target::Cells, log, move |table| {
            let (ty, array) = describe(table, &column)?;
            let selected = rows_stepping(
                table.row_count(),
                selection.0,
                selection.1,
                selection.2,
                "TableProxy::TableProxy::putVarColumn: no such row",
                "TableProxy::putVarColumn: rowincr<=0",
            )?;
            if fields.len() != selected.len() {
                return runtime("TableProxy::putVarColumn: #rows mismatches #elem in value".into());
            }
            let Some(ty) = ty else {
                return runtime(
                    "TableProxy::putVarColumn not possible for a column containing records".into(),
                );
            };
            let mut cells = Vec::with_capacity(fields.len());
            for ((key, value), row) in fields.into_iter().zip(&selected) {
                let Incoming::Wide(wide) = value else {
                    return runtime(format!("a value of {key} is not an array"));
                };
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
                cells.push((*row, cell.into_cell(ty, array)?));
            }
            if cells.is_empty() {
                return Ok(Persist::Nothing);
            }
            set_cells(table, &column, cells)?;
            Ok(Persist::Rows {
                column,
                rows: selected,
            })
        })
    }
}
