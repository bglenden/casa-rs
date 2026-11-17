// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `tablerow` accessor tool (`table.row` and `casars.tools.tablerow`),
//! matching CASA 6.7.6.14 semantics (`tools/casa-compat/table_tool_oracle.py`).

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use casa_tables::{ArrayShapeContract, ColumnSchema, ColumnType, Table};
use casa_types::{PrimitiveType, Value};
use pyo3::exceptions::{PyIndexError, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyList, PySlice};

use super::keyword_value::{Data, Incoming};
use super::read::scalar_cells;
use super::state::{Fail, RootOpen, State, synchronize_root};
use super::write::{Persist, Target, write_root};
use super::write_cells::{ShapeRule, set_cells};
use super::{LOGGER, PyTableTool, flag, integer, record_fields, values};
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

#[derive(Clone)]
struct RowHandle {
    root: Arc<Mutex<RootOpen>>,
    alive: Arc<AtomicBool>,
    row_map: Option<Vec<usize>>,
    name: PathBuf,
    columns: Vec<String>,
}

/// Accessor over a subset of a table's columns, row by row (`tb.row`).
#[pyclass(name = "tablerow", module = "casars.tools", frozen)]
pub(crate) struct PyTableRowTool {
    handle: Mutex<Option<RowHandle>>,
}

impl RowHandle {
    fn ensure_alive(&self) -> Result<(), Fail> {
        if self.alive.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Fail::Runtime("use of uninitialized table row".into()))
        }
    }

    fn view_nrows(&self, root: &RootOpen) -> usize {
        self.row_map
            .as_ref()
            .map_or_else(|| root.table.row_count(), Vec::len)
    }
}

fn runtime_err(message: impl Into<String>) -> Fail {
    Fail::Runtime(message.into())
}

fn undefined_row_array(schema: &ColumnSchema, contract: &ArrayShapeContract) -> CasaValue {
    let shape = match contract {
        ArrayShapeContract::Fixed { shape } => vec![0; shape.len().max(1)],
        ArrayShapeContract::Variable { ndim: Some(ndim) } => vec![0; (*ndim).max(1)],
        ArrayShapeContract::Variable { ndim: None } => vec![0],
    };
    values::empty_array_of(schema.data_type(), shape)
}

fn read_row_dict(root: &RootOpen, handle: &RowHandle, view_row: usize) -> Result<CasaValue, Fail> {
    let root_row = handle
        .row_map
        .as_ref()
        .map_or(view_row, |map| map[view_row]);
    let mut fields = Vec::with_capacity(handle.columns.len());
    for col in &handle.columns {
        let schema = root
            .table
            .schema()
            .and_then(|s| s.column(col))
            .ok_or_else(|| runtime_err(format!("Table column {col} is unknown")))?;
        let val = match schema.column_type() {
            ColumnType::Scalar => {
                let scalar = scalar_cells(&root.table, col, &[root_row])?.remove(0);
                CasaValue::from_scalar(&scalar)
            }
            ColumnType::Record => {
                let record = root.table.column_accessor(col)?.record_cell(root_row)?;
                values::record(&record, &root.name)
            }
            ColumnType::Array(contract) => match root
                .table
                .column_accessor(col)?
                .array_cells_owned_uncached(&[root_row])?
                .into_iter()
                .next()
                .flatten()
            {
                Some(array) => values::array(&array),
                None => undefined_row_array(schema, contract),
            },
        };
        fields.push((col.clone(), val));
    }
    Ok(CasaValue::record(fields))
}

fn strict_scalar_error(ty: PrimitiveType, field_name: &str) -> Fail {
    let method = match ty {
        PrimitiveType::Bool => "asBool",
        PrimitiveType::UInt8
        | PrimitiveType::UInt16
        | PrimitiveType::UInt32
        | PrimitiveType::Int16
        | PrimitiveType::Int32 => "asInt",
        PrimitiveType::Int64 => "asInt64",
        PrimitiveType::Float32 => "asFloat",
        PrimitiveType::Float64 => "asDouble",
        PrimitiveType::Complex32 => "asComplex",
        PrimitiveType::Complex64 => "asDComplex",
        PrimitiveType::String => {
            return runtime_err(format!(
                "TableRecordRep::get_pointer - incorrect data type used for field {field_name}"
            ));
        }
    };
    runtime_err(format!("RecordInterface::{method} - invalid data type"))
}

fn data_compatible(ty: PrimitiveType, data: &Data) -> bool {
    match ty {
        PrimitiveType::Bool => matches!(data, Data::Bools(_) | Data::Ints(_) | Data::Floats(_)),
        PrimitiveType::UInt8
        | PrimitiveType::UInt16
        | PrimitiveType::UInt32
        | PrimitiveType::Int16
        | PrimitiveType::Int32
        | PrimitiveType::Int64 => matches!(data, Data::Ints(_)),
        PrimitiveType::Float32 | PrimitiveType::Float64 => {
            matches!(data, Data::Ints(_) | Data::Floats(_))
        }
        PrimitiveType::Complex32 | PrimitiveType::Complex64 => {
            matches!(data, Data::Ints(_) | Data::Floats(_) | Data::Complexes(_))
        }
        PrimitiveType::String => matches!(data, Data::Texts(_)),
    }
}

fn strict_into_cell(
    field_name: &str,
    incoming: Incoming,
    schema: &ColumnSchema,
) -> Result<Value, Fail> {
    match (schema.column_type(), schema.data_type()) {
        (ColumnType::Record, _) => match incoming {
            Incoming::Record(_) => incoming.natural(),
            Incoming::Wide(_) => Err(runtime_err(format!(
                "TableRecordRep::get_pointer - incorrect data type used for field {field_name}"
            ))),
        },
        (ColumnType::Scalar, Some(ty)) => match incoming {
            Incoming::Record(_) => Err(strict_scalar_error(ty, field_name)),
            Incoming::Wide(wide) => {
                if !data_compatible(ty, &wide.data) {
                    return Err(strict_scalar_error(ty, field_name));
                }
                wide.into_cell(ty, false)
            }
        },
        (ColumnType::Array(_), Some(ty)) => match incoming {
            Incoming::Record(_) => Err(runtime_err(format!(
                "TableRecordRep::get_pointer - incorrect data type used for field {field_name}"
            ))),
            Incoming::Wide(wide) => {
                if !data_compatible(ty, &wide.data) {
                    return Err(runtime_err(format!(
                        "TableRecordRep::get_pointer - incorrect data type used for field {field_name}"
                    )));
                }
                wide.into_cell(ty, true)
            }
        },
        (_, None) => Err(runtime_err(format!(
            "TableRecordRep::get_pointer - incorrect data type used for field {field_name}"
        ))),
    }
}

fn check_matching_fields(fields: &[(String, Incoming)], columns: &[String]) -> Result<(), Fail> {
    if fields.len() != columns.len() {
        return Err(runtime_err("TableRow::put; names not conforming"));
    }
    for column in columns {
        if !fields.iter().any(|(name, _)| name == column) {
            return Err(runtime_err("TableRow::put; names not conforming"));
        }
    }
    Ok(())
}

fn apply_row_put(
    table: &mut Table,
    row_map: Option<&[usize]>,
    row: i64,
    table_name: &str,
    columns: &[String],
    fields: Vec<(String, Incoming)>,
    matching: bool,
) -> Result<Persist, Fail> {
    let nrows = row_map.map_or_else(|| table.row_count(), <[usize]>::len);
    let view_row = usize::try_from(row)
        .ok()
        .filter(|&r| r < nrows)
        .ok_or_else(|| {
            runtime_err(format!(
                "TableRow::put: row {} does not exist in table {table_name}",
                row as u64
            ))
        })?;
    if !matching {
        check_matching_fields(&fields, columns)?;
    }
    let root_row = row_map.map_or(view_row, |map| map[view_row]);
    let wanted: HashSet<&str> = columns.iter().map(String::as_str).collect();
    let mut updates = Vec::new();
    let mut updated_cols = Vec::new();
    for (field_name, incoming) in fields {
        if !wanted.contains(field_name.as_str()) {
            continue;
        }
        let schema = table
            .schema()
            .and_then(|s| s.column(&field_name))
            .ok_or_else(|| runtime_err(format!("Table column {field_name} is unknown")))?
            .clone();
        let cell = strict_into_cell(&field_name, incoming, &schema)?;
        ShapeRule::of(table, &field_name).check(table, &field_name, root_row, &cell)?;
        updated_cols.push(field_name.clone());
        updates.push((field_name, cell));
    }
    if updates.is_empty() {
        return Ok(Persist::Nothing);
    }
    for (col, cell) in updates {
        set_cells(table, &col, vec![(root_row, cell)])?;
    }
    Ok(Persist::Rows {
        columns: updated_cols,
        rows: vec![root_row],
    })
}

impl PyTableRowTool {
    fn clone_handle(&self) -> Result<RowHandle, Fail> {
        let guard = self.handle.lock().unwrap_or_else(PoisonError::into_inner);
        let handle = guard
            .as_ref()
            .ok_or_else(|| runtime_err("use of uninitialized table row"))?;
        handle.ensure_alive()?;
        Ok(handle.clone())
    }

    fn read_normalized_index(&self, py: Python<'_>, idx: i64) -> PyResult<Py<PyAny>> {
        let nrows = self.__len__(py)?;
        let n_i64 = i64::try_from(nrows).unwrap_or(i64::MAX);
        let normalized = if idx < 0 { idx + n_i64 } else { idx };
        if normalized < 0 || normalized >= n_i64 {
            return Err(PyIndexError::new_err(format!(
                "row index {idx} out of range for table row with {nrows} rows"
            )));
        }
        self.read_single_row(py, normalized)
    }

    fn read_single_row(&self, py: Python<'_>, row: i64) -> PyResult<Py<PyAny>> {
        let mut lines = Vec::new();
        let result = py.detach(|| -> Result<CasaValue, Fail> {
            let handle = self.clone_handle()?;
            let mut root = handle.root.lock().unwrap_or_else(PoisonError::into_inner);
            synchronize_root(&mut root)?;
            handle.ensure_alive()?;
            let nrows = handle.view_nrows(&root);
            let view_row = usize::try_from(row)
                .ok()
                .filter(|&r| r < nrows)
                .ok_or_else(|| {
                    let message = format!(
                        "TableRow::get: row {} does not exist in table {}",
                        row as u64,
                        handle.name.display()
                    );
                    lines.push(LogLine::error(message.clone()));
                    runtime_err(message)
                })?;
            read_row_dict(&root, &handle, view_row)
        });
        emit(py, LOGGER, &lines)?;
        result?.into_python(py)
    }

    fn read_indexed_rows(&self, py: Python<'_>, indices: Vec<i64>) -> PyResult<Py<PyAny>> {
        let dicts = py.detach(|| -> Result<Vec<CasaValue>, PyErr> {
            let handle = self.clone_handle().map_err(PyErr::from)?;
            let mut root = handle.root.lock().unwrap_or_else(PoisonError::into_inner);
            synchronize_root(&mut root).map_err(PyErr::from)?;
            handle.ensure_alive().map_err(PyErr::from)?;
            let nrows = handle.view_nrows(&root);
            let n_i64 = i64::try_from(nrows).unwrap_or(i64::MAX);
            let mut out = Vec::with_capacity(indices.len());
            for idx in indices {
                let normalized = if idx < 0 { idx + n_i64 } else { idx };
                let Some(view_row) = usize::try_from(normalized).ok().filter(|&r| r < nrows) else {
                    return Err(PyIndexError::new_err(format!(
                        "row index {idx} out of range for table row with {nrows} rows"
                    )));
                };
                out.push(read_row_dict(&root, &handle, view_row).map_err(PyErr::from)?);
            }
            Ok(out)
        })?;
        let items = dicts
            .into_iter()
            .map(|d| d.into_python(py))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(PyList::new(py, items)?.into_any().unbind())
    }
}

pub(super) fn parse_columnnames(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(name) = value.extract::<String>() {
        return Ok(vec![name]);
    }
    if let Ok(names) = value.extract::<Vec<String>>() {
        return Ok(names);
    }
    if let Ok(list) = value.call_method0("tolist")
        && let Ok(names) = list.extract::<Vec<String>>()
    {
        return Ok(names);
    }
    Err(PyTypeError::new_err(
        "argument _columnnames must be a string or list of strings",
    ))
}

/// Opens a [`PyTableRowTool`] from `state`, selecting `columnnames` (or
/// excluding them when `exclude` is true).
pub(super) fn create(
    state: &mut State,
    columnnames: Vec<String>,
    exclude: bool,
    log: &mut Vec<LogLine>,
) -> Result<PyTableRowTool, Fail> {
    let Some(open) = state.open.as_ref() else {
        log.push(LogLine::error("Row access from unitialized table"));
        return Err(runtime_err("row access from unitialized table"));
    };
    let root = open.root();
    let schema = root
        .table
        .schema()
        .ok_or_else(|| runtime_err("table has no schema"))?;
    let schema_cols: Vec<String> = open
        .visible_columns(&root.table)
        .into_iter()
        .map(|col| col.name().to_string())
        .collect();
    let selected = if exclude {
        let excluded: HashSet<&str> = columnnames.iter().map(String::as_str).collect();
        schema_cols
            .into_iter()
            .filter(|name| !excluded.contains(name.as_str()))
            .collect()
    } else if columnnames.is_empty() {
        schema_cols
    } else {
        let mut seen = HashSet::with_capacity(columnnames.len());
        for name in &columnnames {
            if !open.column_allowed(name) || schema.column(name).is_none() {
                return Err(runtime_err(format!("Table column {name} is unknown")));
            }
            if !seen.insert(name.as_str()) {
                return Err(runtime_err(format!(
                    "RecordDesc::addField() - field {name} already has been defined"
                )));
            }
        }
        columnnames
    };
    Ok(PyTableRowTool {
        handle: Mutex::new(Some(RowHandle {
            root: Arc::clone(&open.root),
            alive: Arc::clone(&open.alive),
            row_map: open.row_map.clone(),
            name: open.name.clone(),
            columns: selected,
        })),
    })
}

#[pymethods]
#[allow(non_snake_case)]
impl PyTableRowTool {
    #[new]
    #[pyo3(signature = (table = None, columnnames = None, exclude = None), text_signature = "(table=None, columnnames=[], exclude=False)")]
    fn new(
        py: Python<'_>,
        table: Option<&Bound<'_, PyAny>>,
        columnnames: Option<&Bound<'_, PyAny>>,
        exclude: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let Some(table_arg) = table else {
            return Err(PyRuntimeError::new_err(
                "invalid table passed for parameter one",
            ));
        };
        let Ok(py_table) = table_arg.extract::<PyRef<'_, PyTableTool>>() else {
            return Err(PyRuntimeError::new_err(
                "invalid table passed for parameter one",
            ));
        };
        let cols = parse_columnnames(columnnames)?;
        let excl = flag(exclude, false)?;
        let state_arc = py_table.shared_state();
        drop(py_table);
        let mut lines = Vec::new();
        let row = py.detach(|| {
            let mut guard = state_arc.lock().unwrap_or_else(PoisonError::into_inner);
            if guard.open.is_none() {
                return Err(runtime_err("invalid table passed for parameter one"));
            }
            create(&mut guard, cols, excl, &mut lines)
        });
        emit(py, LOGGER, &lines)?;
        Ok(row?)
    }

    /// Return `True` if rows can be written through this accessor.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn iswritable(&self, py: Python<'_>) -> PyResult<bool> {
        py.detach(|| -> Result<bool, Fail> {
            let handle = self.clone_handle()?;
            let root = handle.root.lock().unwrap_or_else(PoisonError::into_inner);
            handle.ensure_alive()?;
            Ok(root.writable)
        })
        .map_err(PyErr::from)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn _iswritable(&self, py: Python<'_>) -> PyResult<bool> {
        self.iswritable(py)
    }

    /// Return the selected column values for row `rownr` as a dictionary.
    #[pyo3(signature = (rownr), text_signature = "($self, rownr)")]
    fn get(&self, py: Python<'_>, rownr: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let row = integer(Some(rownr), "rownr", 0)?;
        self.read_single_row(py, row)
    }

    #[pyo3(signature = (rownr), text_signature = "($self, rownr)")]
    fn _get(&self, py: Python<'_>, rownr: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.get(py, rownr)
    }

    /// Write column values from `value` to row `rownr`.
    #[pyo3(signature = (rownr, value, matchingfields = None), text_signature = "($self, rownr, value, matchingfields=True)")]
    fn put(
        &self,
        py: Python<'_>,
        rownr: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        matchingfields: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let row = integer(Some(rownr), "rownr", 0)?;
        let fields = record_fields(value)?;
        let matching = flag(matchingfields, true)?;
        let mut lines = Vec::new();
        let result = py.detach(|| -> Result<(), Fail> {
            let handle = self.clone_handle()?;
            let table_name = handle.name.display().to_string();
            let name_for_closure = table_name.clone();
            let row_map = handle.row_map.clone();
            let columns = handle.columns.clone();
            let mut root = handle.root.lock().unwrap_or_else(PoisonError::into_inner);
            handle.ensure_alive()?;
            match write_root(
                &mut root,
                &table_name,
                Target::RowProxy,
                row_map.as_deref(),
                move |table, row_map| {
                    apply_row_put(
                        table,
                        row_map,
                        row,
                        &name_for_closure,
                        &columns,
                        fields,
                        matching,
                    )
                },
            ) {
                Ok(_) => Ok(()),
                Err(Fail::Runtime(message)) => {
                    lines.push(LogLine::error(message.clone()));
                    Err(Fail::Runtime(message))
                }
                Err(other) => Err(other),
            }
        });
        emit(py, LOGGER, &lines)?;
        result?;
        Ok(true)
    }

    #[pyo3(signature = (rownr, value, matchingfields = None), text_signature = "($self, rownr, value, matchingfields=True)")]
    fn _put(
        &self,
        py: Python<'_>,
        rownr: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        matchingfields: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.put(py, rownr, value, matchingfields)
    }

    /// Release resources associated with this row accessor.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn done(&self) {
        let mut guard = self.handle.lock().unwrap_or_else(PoisonError::into_inner);
        *guard = None;
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| -> Result<usize, Fail> {
            let handle = self.clone_handle()?;
            let root = handle.root.lock().unwrap_or_else(PoisonError::into_inner);
            handle.ensure_alive()?;
            Ok(handle.view_nrows(&root))
        })
        .map_err(PyErr::from)
    }

    fn __getitem__(&self, py: Python<'_>, rownr: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if !rownr.is_instance_of::<PyBool>()
            && let Ok(idx) = rownr.extract::<i64>()
        {
            return self.read_normalized_index(py, idx);
        }
        if let Ok(slice) = rownr.cast::<PySlice>() {
            let nrows = self.__len__(py)?;
            let indices = slice.indices(isize::try_from(nrows).unwrap_or(isize::MAX))?;
            let selected: Vec<i64> = (0..indices.slicelength)
                .map(|step| (indices.start + (step as isize) * indices.step) as i64)
                .collect();
            return self.read_indexed_rows(py, selected);
        }
        if let Ok(indices) = rownr.extract::<Vec<i64>>() {
            return self.read_indexed_rows(py, indices);
        }
        if let Ok(native) = rownr.call_method0("tolist") {
            if !native.is_instance_of::<PyBool>()
                && let Ok(idx) = native.extract::<i64>()
            {
                return self.read_normalized_index(py, idx);
            }
            if let Ok(indices) = native.extract::<Vec<i64>>() {
                return self.read_indexed_rows(py, indices);
            }
        }
        Err(PyTypeError::new_err(
            "row indices must be integers, slices, or sequences of integers",
        ))
    }
}
