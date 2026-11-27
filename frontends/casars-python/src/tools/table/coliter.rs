// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `table.getcoliter` column-value iterator (`PyTableColIter`), matching
//! CASA 6.7.6.14 semantics (`tools/casa-compat/table_tool_oracle.py`).

use std::sync::{Arc, Mutex, PoisonError};

use casa_tables::ColumnType;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;

use super::read::scalar_cells;
use super::state::{Fail, RootOpen, State, synchronize_root};
use super::values;
use crate::convert::value::CasaValue;

#[derive(Debug, Clone, Copy)]
struct IterState {
    next_row: usize,
    remaining: usize,
    step: usize,
}

enum IterYield {
    Stop,
    None,
    Value(CasaValue),
}

/// Iterator returned by `table.getcoliter`.
#[pyclass(name = "_tablecoliter", module = "casars._core", frozen)]
pub(crate) struct PyTableColIter {
    root: Arc<Mutex<RootOpen>>,
    row_map: Option<Vec<usize>>,
    columns: Vec<String>,
    torecord: bool,
    state: Mutex<IterState>,
}

fn view_nrows(root: &RootOpen, row_map: Option<&[usize]>) -> usize {
    row_map.map_or_else(|| root.table.row_count(), <[usize]>::len)
}

fn read_iter_row(
    root: &RootOpen,
    row_map: Option<&[usize]>,
    columns: &[String],
    torecord: bool,
    view_row: usize,
) -> Result<IterYield, Fail> {
    if view_row >= view_nrows(root, row_map) {
        return Err(Fail::Runtime("loading values from table failed".into()));
    }
    if columns.is_empty() {
        return Ok(IterYield::None);
    }
    let root_row = row_map.map_or(view_row, |map| map[view_row]);
    let mut cells = Vec::with_capacity(columns.len());
    for column in columns {
        let schema = root
            .table
            .schema()
            .and_then(|s| s.column(column))
            .ok_or_else(|| Fail::Runtime(format!("column \"{column}\" does not exist")))?;
        let cell = match schema.column_type() {
            ColumnType::Record => {
                return Err(Fail::Runtime(
                    "TableProxy::getColumn not possible for a column containing records".into(),
                ));
            }
            ColumnType::Scalar => {
                let scalar = scalar_cells(&root.table, column, &[root_row])?.remove(0);
                values::scalar_column(vec![scalar])
            }
            ColumnType::Array(_) => match root
                .table
                .column_accessor(column)?
                .array_cells_owned_uncached(&[root_row])?
                .into_iter()
                .next()
                .flatten()
            {
                Some(array) => values::stack_rows(&[values::array(&array)])?,
                None => CasaValue::Bools {
                    shape: Vec::new(),
                    values: vec![false],
                },
            },
        };
        cells.push(cell);
    }
    let value = if torecord {
        CasaValue::record(columns.iter().cloned().zip(cells))
    } else if cells.len() == 1 {
        cells.remove(0)
    } else {
        CasaValue::Tuple(cells)
    };
    Ok(IterYield::Value(value))
}

#[pymethods]
impl PyTableColIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let outcome = py.detach(|| -> Result<IterYield, Fail> {
            let view_row = {
                let mut iter = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                if iter.remaining == 0 {
                    return Ok(IterYield::Stop);
                }
                let current = iter.next_row;
                iter.next_row = iter.next_row.saturating_add(iter.step);
                iter.remaining -= 1;
                current
            };
            let mut root = self.root.lock().unwrap_or_else(PoisonError::into_inner);
            synchronize_root(&mut root)?;
            read_iter_row(
                &root,
                self.row_map.as_deref(),
                &self.columns,
                self.torecord,
                view_row,
            )
        })?;
        match outcome {
            IterYield::Stop => Ok(None),
            IterYield::None => Ok(Some(py.None())),
            IterYield::Value(value) => Ok(Some(value.into_python(py)?)),
        }
    }

    fn __richcmp__(
        &self,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
    ) -> PyResult<Py<PyAny>> {
        // CASA compares each operand to the next yielded row, including when
        // NumPy dispatches an array comparison one element at a time.
        let next = self
            .__next__(py)?
            .ok_or_else(|| PyStopIteration::new_err(()))?;
        Ok(next.bind(py).rich_compare(other, op)?.unbind())
    }
}

/// Validates arguments on `state` and builds a lazy [`PyTableColIter`].
pub(super) fn create(
    state: &mut State,
    columns: Vec<String>,
    selection: (i64, i64, i64),
    torecord: bool,
) -> Result<PyTableColIter, Fail> {
    let Some(open) = state.open.as_ref() else {
        return Err(Fail::Runtime("no opened table available".into()));
    };
    let (startrow, nrow, rowincr) = selection;
    let row_map = open.row_map.clone();
    let root_arc = Arc::clone(&open.root);
    let (next_row, remaining, step) = {
        let mut root = open.root();
        synchronize_root(&mut root)?;
        for column in &columns {
            if !open.column_allowed(column) {
                return Err(Fail::Runtime(format!("column \"{column}\" does not exist")));
            }
            let schema = root
                .table
                .schema()
                .and_then(|s| s.column(column))
                .ok_or_else(|| Fail::Runtime(format!("column \"{column}\" does not exist")))?;
            if matches!(schema.column_type(), ColumnType::Record) {
                return Err(Fail::Runtime(
                    "TableProxy::getColumn not possible for a column containing records".into(),
                ));
            }
        }
        let nrows = view_nrows(&root, row_map.as_deref());
        let start = usize::try_from(startrow.max(0)).unwrap_or(usize::MAX);
        if nrow == 0 {
            if start > nrows {
                return Err(Fail::Runtime(
                    "TableProxy::fillColumnValues: no such row".into(),
                ));
            }
            (start, 0, 1)
        } else if nrow < 0 && nrows == 0 && start == 0 {
            (0, 0, 1)
        } else if start >= nrows {
            return Err(Fail::Runtime(
                "TableProxy::fillColumnValues: no such row".into(),
            ));
        } else if rowincr == 0 {
            return Err(Fail::Runtime("fillColumnValues: rowincr<=0".into()));
        } else {
            let step = usize::try_from(if rowincr < 0 { 1 } else { rowincr }).unwrap_or(usize::MAX);
            let remaining = if nrow < 0 {
                (nrows - start).div_ceil(step)
            } else {
                usize::try_from(nrow).unwrap_or(usize::MAX)
            };
            (start, remaining, step)
        }
    };
    Ok(PyTableColIter {
        root: root_arc,
        row_map,
        columns,
        torecord,
        state: Mutex::new(IterState {
            next_row,
            remaining,
            step,
        }),
    })
}
