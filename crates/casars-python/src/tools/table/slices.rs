// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` tool's slices of array cells and columns (`getcellslice`,
//! `getcolslice`, `putcellslice`, `putcolslice`), as CASA 6.7.6.14's `tb`
//! does them (`tools/casa-compat/table_tool_oracle.py`, `slices` and
//! `ms_slices`).
//!
//! A slice is given by its bottom-left corner `blc`, top-right corner `trc`
//! (both inclusive) and step `incr`, one element of each per axis. CASA
//! aborts its whole process when a corner has the wrong rank or a slice
//! starts outside the array; casa-rs raises a `RuntimeError` for those
//! (`docs/CASA (C++) bugs.md`).

use casa_tables::{ColumnType, Slicer, Table};
use casa_types::{ArrayValue, Value};

use super::keyword_value::Incoming;
use super::read::rows;
use super::state::{Fail, Open, State};
use super::values;
use super::write::{Persist, Target};
use super::write_cells::{conformance, put_column_error, split_rows};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

type Reply = Result<CasaValue, Fail>;

fn runtime<T>(message: String) -> Result<T, Fail> {
    Err(Fail::Runtime(message))
}

/// A slice's corners and step, as the caller gave them.
#[derive(Debug, Clone)]
pub(crate) struct Corners {
    pub(crate) blc: Vec<i64>,
    pub(crate) trc: Vec<i64>,
    pub(crate) incr: Vec<i64>,
}

impl Corners {
    /// The slicer over an array of `shape` and the shape the slice has.
    /// `trc` may be `-1` (the last element) where `end_ok`, as `getcolslice`
    /// accepts it and `getcellslice` does not.
    fn over(&self, shape: &[usize], end_ok: bool) -> Result<(Slicer, Vec<usize>), Fail> {
        let rank = shape.len();
        // The default `incr=[1]` steps every axis by one (CASA aborts on it
        // for an array of more than one axis).
        let mut incr = self.incr.clone();
        if incr.len() == 1 && rank > 1 {
            incr = vec![incr[0]; rank];
        }
        if self.blc.len() != rank || self.trc.len() != rank || incr.len() != rank {
            return runtime(format!(
                "blc, trc and incr need {rank} elements each, one per axis of the array"
            ));
        }
        let mut start = Vec::with_capacity(rank);
        let mut end = Vec::with_capacity(rank);
        let mut stride = Vec::with_capacity(rank);
        let mut result = Vec::with_capacity(rank);
        for axis in 0..rank {
            let dim = shape[axis];
            let (blc, trc, incr) = (self.blc[axis], self.trc[axis], incr[axis]);
            if blc < 0 {
                return runtime(
                    "Exception: All elements of blc must be greater than or equal to 0.".into(),
                );
            }
            let trc = if trc == -1 && end_ok {
                i64::try_from(dim).unwrap_or(i64::MAX) - 1
            } else if trc < 0 {
                return runtime(
                    "Exception: All elements of trc must be greater than or equal to 0.".into(),
                );
            } else {
                trc
            };
            if incr < 1 {
                return runtime("Exception: All elements of incr must be greater than 0.".into());
            }
            let (blc, trc, incr) = (blc as usize, trc as usize, incr as usize);
            if blc > trc || trc >= dim {
                return runtime(format!(
                    "the slice {blc}..={trc} lies outside an axis of {dim} elements"
                ));
            }
            start.push(blc);
            end.push(trc + 1);
            stride.push(incr);
            result.push((trc - blc) / incr + 1);
        }
        Ok((Slicer::new(start, end, stride)?, result))
    }
}

impl Open {
    /// The slice of one array cell.
    fn cell_slice(
        &self,
        column: &str,
        row: usize,
        corners: &Corners,
        end_ok: bool,
    ) -> Result<(CasaValue, Slicer), Fail> {
        let shape = self
            .table
            .array_shape(row, column)?
            .ok_or_else(|| Fail::Runtime(format!(
                "Table DataManager error: Invalid operation: no array in row {row} of column {column}"
            )))?;
        let (slicer, _) = corners.over(&shape, end_ok)?;
        let Value::Array(array) = self.table.get_cell_slice(column, row, &slicer)? else {
            return runtime(format!("row {row} of column {column} is not an array"));
        };
        Ok((values::array(&array), slicer))
    }

    fn require_array_column(&self, column: &str, what: &str) -> Result<(), Fail> {
        let schema = self
            .table
            .schema()
            .and_then(|schema| schema.column(column))
            .ok_or_else(|| Fail::Runtime(format!("Table column {column} is unknown")))?;
        if matches!(schema.column_type(), ColumnType::Array(_)) {
            Ok(())
        } else {
            runtime(format!(
                "TableProxy::{what}: column {column} is not an array column"
            ))
        }
    }

    fn getcellslice(&self, column: &str, row: i64, corners: &Corners) -> Reply {
        self.require_array_column(column, "getCellSlice")?;
        let nrows = self.table.row_count();
        let row = usize::try_from(row)
            .ok()
            .filter(|row| *row < nrows)
            .ok_or_else(|| {
                Fail::Runtime(format!(
                    "TableColumn: row number {} exceeds #rows {nrows} in table {}",
                    row as u64,
                    self.name.display()
                ))
            })?;
        Ok(self.cell_slice(column, row, corners, false)?.0)
    }

    fn getcolslice(&self, column: &str, corners: &Corners, selection: (i64, i64, i64)) -> Reply {
        self.require_array_column(column, "getColumnSlice")?;
        let selected = rows(
            self.table.row_count(),
            selection.0,
            selection.1,
            selection.2,
            "TableProxy::getColumnSlice: no such row",
        )?;
        if selected.is_empty() {
            let data_type = self
                .table
                .schema()
                .and_then(|schema| schema.column(column))
                .and_then(|schema| schema.data_type());
            return Ok(values::empty_column(data_type));
        }
        let cells = selected
            .iter()
            .map(|row| Ok(self.cell_slice(column, *row, corners, true)?.0))
            .collect::<Result<Vec<_>, Fail>>()?;
        values::stack_rows(&cells)
    }
}

/// The array a slice write holds, from the value coerced to the column.
fn slice_data(
    table: &Table,
    column: &str,
    wide: super::keyword_value::Wide,
) -> Result<ArrayValue, Fail> {
    let ty = table
        .schema()
        .and_then(|schema| schema.column(column))
        .and_then(|schema| schema.data_type())
        .ok_or_else(|| Fail::Runtime(format!("Table column {column} is unknown")))?;
    match wide.into_cell(ty, true)? {
        Value::Array(array) => Ok(array),
        _ => runtime(format!("a slice of column {column} needs an array")),
    }
}

fn wide_of(value: Incoming) -> Result<super::keyword_value::Wide, Fail> {
    match value {
        Incoming::Wide(wide) => Ok(wide),
        Incoming::Record(_) => runtime("a record cannot be written into a slice".into()),
    }
}

impl State {
    /// `getcellslice(columnname, rownr, blc, trc, incr)`.
    pub(super) fn getcellslice(
        &self,
        column: &str,
        row: i64,
        corners: &Corners,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        match self.table(log) {
            Some(open) => open.getcellslice(column, row, corners),
            None => Ok(CasaValue::Bools {
                shape: vec![0],
                values: Vec::new(),
            }),
        }
    }

    /// `getcolslice(columnname, blc, trc, incr, startrow, nrow, rowincr)`.
    pub(super) fn getcolslice(
        &self,
        column: &str,
        corners: &Corners,
        selection: (i64, i64, i64),
        log: &mut Vec<LogLine>,
    ) -> Reply {
        match self.table(log) {
            Some(open) => open.getcolslice(column, corners, selection),
            None => Ok(CasaValue::Bools {
                shape: vec![0],
                values: Vec::new(),
            }),
        }
    }

    /// `putcellslice(columnname, rownr, value, blc, trc, incr)`.
    pub(super) fn putcellslice(
        &mut self,
        column: String,
        row: i64,
        value: Incoming,
        corners: Corners,
        log: &mut Vec<LogLine>,
    ) -> Reply {
        self.write_with(Target::Cells, log, move |table| {
            let wide = wide_of(value)?;
            let array_column = table
                .schema()
                .and_then(|schema| schema.column(&column))
                .map(|schema| matches!(schema.column_type(), ColumnType::Array(_)));
            match array_column {
                None => return Err(put_column_error(&format!("column {column} does not exist"))),
                Some(false) => {
                    return runtime(format!("TableProxy::putCellSlice: column {column} is not an array column"));
                }
                Some(true) => {}
            }
            let row = usize::try_from(row)
                .ok()
                .filter(|row| *row < table.row_count())
                .ok_or_else(|| put_column_error("no such row"))?;
            let shape = table
                .array_shape(row, &column)?
                .ok_or_else(|| Fail::Runtime(format!(
                    "Table DataManager error: Invalid operation: no array in row {row} of column {column}"
                )))?;
            let (slicer, expected) = corners.over(&shape, false)?;
            let data = slice_data(table, &column, wide)?;
            if data.shape() != expected.as_slice() {
                return Err(conformance_slice(row, &column, data.shape(), &expected));
            }
            table.put_cell_slice(&column, row, &slicer, &data)?;
            Ok(Persist::Rows {
                column,
                rows: vec![row],
            })
        })
    }

    /// `putcolslice(columnname, value, blc, trc, incr, startrow, nrow, rowincr)`.
    pub(super) fn putcolslice(
        &mut self,
        column: String,
        value: Incoming,
        corners: Corners,
        selection: (i64, i64, i64),
        log: &mut Vec<LogLine>,
    ) -> Reply {
        self.write_with(Target::Cells, log, move |table| {
            let wide = wide_of(value)?;
            let array_column = table
                .schema()
                .and_then(|schema| schema.column(&column))
                .map(|schema| matches!(schema.column_type(), ColumnType::Array(_)));
            match array_column {
                None => return Err(put_column_error(&format!("column {column} does not exist"))),
                Some(false) => {
                    return runtime(format!("TableProxy::putColumnSlice: column {column} is not an array column"));
                }
                Some(true) => {}
            }
            let selected = rows(
                table.row_count(),
                selection.0,
                selection.1,
                selection.2,
                "TableProxy::TableProxy::putColumn: no such row",
            )?;
            let given = wide.shape.as_ref().and_then(|shape| shape.last()).copied().unwrap_or(1);
            let per_row = split_rows(wide, true, &column, selected.len()).map_err(|_| {
                Fail::Runtime(format!(
                    "ArrayColumn::putColumn(slicer) - column {column} has {}, but array has {given} rows: \
                     Table array conformance error",
                    selected.len()
                ))
            })?;
            for (row, cell) in selected.iter().zip(per_row) {
                let shape = table
                    .array_shape(*row, &column)?
                    .ok_or_else(|| Fail::Runtime(format!(
                        "Table DataManager error: Invalid operation: no array in row {row} of column {column}"
                    )))?;
                let (slicer, expected) = corners.over(&shape, false)?;
                let data = slice_data(table, &column, cell)?;
                if data.shape() != expected.as_slice() {
                    return Err(conformance_slice(*row, &column, data.shape(), &expected));
                }
                table.put_cell_slice(&column, *row, &slicer, &data)?;
            }
            if selected.is_empty() {
                return Ok(Persist::Nothing);
            }
            Ok(Persist::Rows {
                column,
                rows: selected,
            })
        })
    }
}

/// The refusal of a slice value of the wrong shape.
fn conformance_slice(row: usize, column: &str, shape: &[usize], expected: &[usize]) -> Fail {
    let text = conformance(row, column, shape, expected);
    match text {
        Fail::Runtime(message) => {
            Fail::Runtime(message.replace("ArrayColumn::put ", "ArrayColumn::putSlice "))
        }
        other => other,
    }
}
