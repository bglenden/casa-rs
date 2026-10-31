// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `table` tool (tb) over [`casa_tables::Table`]: every method of CASA
//! 6.7.6.14's binding, with its signature (ADR-0019, #1025). A method
//! casa-rs does not implement yet raises `NotImplementedError` naming #1028
//! (`stubs.rs`).
//!
//! Implemented so far: `open`, `close`, `done`, `ok`, `name`, `iswritable`,
//! the lock calls, and the read side (`state`, `read`): sizes and names,
//! column descriptions, cells and columns, keywords and column keywords.
//! Values are casatools' (`values`): integers come back as `int64`, reals
//! as `float64`, complex numbers as `complex128`, a column's rows last.
//!
//! The class is `frozen` and keeps the open table behind a `Mutex`, and
//! every call runs with the GIL released (ADR-0019 item 4). What CASA logs
//! goes to the Python logger `casars.tools.table`, its INFO at INFO, WARN at
//! WARNING and SEVERE at ERROR.

mod cell_value;
mod keyword_value;
mod read;
mod slices;
mod state;
mod stubs;
pub(crate) mod surface;
mod values;
mod write;
mod write_cells;

use std::sync::{Mutex, MutexGuard, PoisonError};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit, not_yet_implemented_in};
use keyword_value::Incoming;
use read::KeywordRef;
use slices::Corners;
use state::{Fail, LockRequest, State};
use write::InfoUpdate;

/// The issue that records the methods casa-rs refuses.
const ISSUE: u32 = 1028;

/// The Python logger CASA's `tb` messages go to.
const LOGGER: &str = "casars.tools.table";

/// A method casa-rs does not implement yet.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("table", method, ISSUE)
}

/// The tool keeps its table behind a `Mutex` and releases the GIL around
/// Rust work (ADR-0019 item 4), which needs the table to be `Send`.
const _: fn() = || {
    fn is_send<T: Send>() {}
    is_send::<State>();
};

/// CASA's `table` tool.
#[pyclass(name = "table", module = "casars.tools", frozen)]
pub(crate) struct PyTableTool {
    state: Mutex<State>,
}

impl PyTableTool {
    /// Runs `work` on the state with the GIL released, logs what it reports
    /// and returns its value as Python.
    fn call(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut State, &mut Vec<LogLine>) -> Result<CasaValue, Fail> + Send,
    ) -> PyResult<Py<PyAny>> {
        let mut lines = Vec::new();
        let result = py.detach(|| work(&mut self.guard(), &mut lines));
        emit(py, LOGGER, &lines)?;
        result?.into_python(py)
    }

    /// As [`call`](Self::call), for a call that reads the table: the table
    /// first takes the read lock its lock options call for and synchronizes
    /// with what another process has written since (casacore's
    /// `TableProxy::syncTable`).
    fn read(
        &self,
        py: Python<'_>,
        work: impl FnOnce(&mut State, &mut Vec<LogLine>) -> Result<CasaValue, Fail> + Send,
    ) -> PyResult<Py<PyAny>> {
        self.call(py, move |state, log| {
            state.synchronize()?;
            work(state, log)
        })
    }

    /// The state lock, taken without the GIL (inside `Python::detach`).
    fn guard(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A required text argument (a column name); CASA names the argument with
/// a leading underscore.
fn text(value: &Bound<'_, PyAny>, argument: &str) -> PyResult<String> {
    value
        .extract()
        .map_err(|_| PyTypeError::new_err(format!("argument _{argument} must be a string")))
}

/// An integer argument, or `default`.
fn integer(value: Option<&Bound<'_, PyAny>>, argument: &str, default: i64) -> PyResult<i64> {
    value.map_or(Ok(default), |value| {
        value
            .extract()
            .map_err(|_| PyTypeError::new_err(format!("argument _{argument} must be an integer")))
    })
}

/// A row number argument that may also be a list of them (`putcell`).
fn row_numbers(value: &Bound<'_, PyAny>, argument: &str) -> PyResult<Vec<i64>> {
    if let Ok(row) = value.extract::<i64>() {
        return Ok(vec![row]);
    }
    value.extract::<Vec<i64>>().map_err(|_| {
        PyTypeError::new_err(format!(
            "argument _{argument} must be an integer or a list of integers"
        ))
    })
}

/// A flag argument, or `default`.
fn flag(value: Option<&Bound<'_, PyAny>>, default: bool) -> PyResult<bool> {
    value.map_or(Ok(default), |value| value.extract())
}

/// The `startrow`, `nrow` and `rowincr` arguments of a column call.
fn row_selection(
    start: Option<&Bound<'_, PyAny>>,
    count: Option<&Bound<'_, PyAny>>,
    step: Option<&Bound<'_, PyAny>>,
) -> PyResult<(i64, i64, i64)> {
    Ok((
        integer(start, "startrow", 0)?,
        integer(count, "nrow", -1)?,
        integer(step, "rowincr", 1)?,
    ))
}

/// A keyword argument: a name or a position; absent and `[]` name no
/// keyword at all.
fn keyword_ref(value: Option<&Bound<'_, PyAny>>) -> PyResult<KeywordRef> {
    let Some(value) = value else {
        return Ok(KeywordRef::Whole);
    };
    if let Ok(name) = value.extract::<String>() {
        return Ok(KeywordRef::Name(name));
    }
    if let Ok(list) = value.cast::<PyList>()
        && list.is_empty()
    {
        return Ok(KeywordRef::Whole);
    }
    Ok(KeywordRef::Index(value.extract()?))
}

/// A list of integers argument (`blc`, `trc`, `incr`); absent is `[]`, and
/// `incr` defaults to `[1]`.
fn integers(value: Option<&Bound<'_, PyAny>>, argument: &str) -> PyResult<Option<Vec<i64>>> {
    value
        .map(|value| {
            value.extract::<Vec<i64>>().map_err(|_| {
                PyTypeError::new_err(format!("argument _{argument} must be a list of integers"))
            })
        })
        .transpose()
}

/// The corners and step of a slice.
fn corners(
    blc: Option<&Bound<'_, PyAny>>,
    trc: Option<&Bound<'_, PyAny>>,
    incr: Option<&Bound<'_, PyAny>>,
) -> PyResult<Corners> {
    Ok(Corners {
        blc: integers(blc, "blc")?.unwrap_or_default(),
        trc: integers(trc, "trc")?.unwrap_or_default(),
        incr: integers(incr, "incr")?.unwrap_or_else(|| vec![1]),
    })
}

/// A keyword's value; absent is `[]`, an empty array.
fn keyword_value(py: Python<'_>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Incoming> {
    match value {
        Some(value) => Incoming::from_python(value),
        None => Incoming::from_python(&PyList::empty(py)),
    }
}

/// The fields of a `dict` argument, as keyword values.
fn record_fields(value: &Bound<'_, PyAny>) -> PyResult<Vec<(String, Incoming)>> {
    let Ok(record) = value.cast::<PyDict>() else {
        return Err(PyTypeError::new_err("_value is not a dictionary"));
    };
    let mut fields = Vec::with_capacity(record.len());
    for (key, item) in record.iter() {
        fields.push((key.extract::<String>()?, Incoming::from_python(&item)?));
    }
    Ok(fields)
}

/// `putinfo`'s record: the `type`, `subType` and `readme` it has.
fn info_update(value: &Bound<'_, PyAny>) -> PyResult<InfoUpdate> {
    let Ok(record) = value.cast::<PyDict>() else {
        return Err(PyTypeError::new_err("_value is not a dictionary"));
    };
    let field = |name: &str| -> PyResult<Option<String>> {
        record
            .get_item(name)?
            .map(|item| item.extract())
            .transpose()
    };
    Ok(InfoUpdate {
        table_type: field("type")?,
        sub_type: field("subType")?,
        readme: field("readme")?,
    })
}

/// `open`'s `lockoptions`: a `dict` with `option`, `interval` and `maxwait`.
fn lock_request(value: Option<&Bound<'_, PyAny>>) -> PyResult<LockRequest> {
    let Some(value) = value else {
        return Ok(LockRequest::default());
    };
    let Ok(options) = value.cast::<PyDict>() else {
        return Err(Fail::Type("_lockoptions is not a dictionary".into()).into());
    };
    Ok(LockRequest {
        option: options
            .get_item("option")?
            .map(|v| v.extract())
            .transpose()?,
        interval: options
            .get_item("interval")?
            .map(|v| v.extract())
            .transpose()?,
        maxwait: options
            .get_item("maxwait")?
            .map(|v| v.extract())
            .transpose()?,
    })
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyTableTool {
    #[new]
    fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }

    /// Open a table, closing the one already open.
    #[pyo3(signature = (tablename = None, lockoptions = None, nomodify = None), text_signature = "($self, tablename='', lockoptions={}, nomodify=True)")]
    fn open(
        &self,
        py: Python<'_>,
        tablename: Option<&Bound<'_, PyAny>>,
        lockoptions: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let name = tablename.map_or(Ok(String::new()), |name| text(name, "tablename"))?;
        let request = lock_request(lockoptions)?;
        let writable = !flag(nomodify, true)?;
        self.call(py, move |state, _| {
            state.open(&name, &request, writable)?;
            Ok(CasaValue::Bool(true))
        })
    }

    /// Close the table.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn close(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| {
            state.close();
            Ok(CasaValue::Bool(true))
        })
    }

    /// Close the table.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn done(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| {
            state.close();
            Ok(CasaValue::Bool(true))
        })
    }

    /// Whether a table is open.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn ok(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, _| Ok(state.ok()))
    }

    /// The open table's absolute path.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn name(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, log| Ok(state.name(log)))
    }

    /// The number of rows.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn nrows(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.read(py, |state, log| Ok(state.nrows(log)))
    }

    /// The number of columns.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn ncols(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.read(py, |state, log| Ok(state.ncols(log)))
    }

    /// The column names, in table order.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn colnames(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.read(py, |state, log| Ok(state.colnames(log)))
    }

    /// Whether the table was opened for update.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn iswritable(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, log| Ok(state.iswritable(log)))
    }

    /// The table's type, subtype and readme.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.read(py, |state, log| Ok(state.info(log)))
    }

    /// The lock options the table was opened with.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn lockoptions(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, log| Ok(state.lockoptions(log)))
    }

    /// Whether the table holds a write (or, with `write=False`, a read) lock.
    #[pyo3(signature = (write = None), text_signature = "($self, write=True)")]
    fn haslock(&self, py: Python<'_>, write: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let write = flag(write, true)?;
        self.call(py, move |state, log| Ok(state.haslock(write, log)))
    }

    /// Take a lock.
    #[pyo3(signature = (write = None, nattempts = None), text_signature = "($self, write=True, nattempts=0)")]
    fn lock(
        &self,
        py: Python<'_>,
        write: Option<&Bound<'_, PyAny>>,
        nattempts: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let write = flag(write, true)?;
        let attempts =
            u32::try_from(integer(nattempts, "nattempts", 0)?.max(0)).unwrap_or(u32::MAX);
        self.call(py, move |state, log| {
            if state.open.is_none() {
                state::no_table_specified(log);
            }
            Ok(CasaValue::Bool(state.lock(write, attempts)?))
        })
    }

    /// Release the lock.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn unlock(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.call(py, |state, log| {
            if state.open.is_none() {
                state::no_table_specified(log);
            }
            Ok(CasaValue::Bool(state.unlock()?))
        })
    }

    /// Whether a column holds one value per row (a record column does).
    #[pyo3(signature = (columnname), text_signature = "($self, columnname)")]
    fn isscalarcol(&self, py: Python<'_>, columnname: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        self.read(py, move |state, log| state.isscalarcol(&column, log))
    }

    /// Whether a column's arrays vary in shape.
    #[pyo3(signature = (columnname), text_signature = "($self, columnname)")]
    fn isvarcol(&self, py: Python<'_>, columnname: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        self.read(py, move |state, log| state.isvarcol(&column, log))
    }

    /// A column's data type, in casacore's names.
    #[pyo3(signature = (columnname), text_signature = "($self, columnname)")]
    fn coldatatype(&self, py: Python<'_>, columnname: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        self.read(py, move |state, log| state.coldatatype(&column, log))
    }

    /// How an array column stores its cells.
    #[pyo3(signature = (columnname), text_signature = "($self, columnname)")]
    fn colarraytype(&self, py: Python<'_>, columnname: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        self.read(py, move |state, log| state.colarraytype(&column, log))
    }

    /// One cell.
    #[pyo3(signature = (columnname, rownr = None), text_signature = "($self, columnname, rownr=0)")]
    fn getcell(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        rownr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let row = integer(rownr, "rownr", 0)?;
        self.read(py, move |state, log| state.getcell(&column, row, log))
    }

    /// A column, its rows last.
    #[pyo3(signature = (columnname, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, startrow=0, nrow=-1, rowincr=1)")]
    fn getcol(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let rows = row_selection(startrow, nrow, rowincr)?;
        self.read(py, move |state, log| state.getcol(&column, rows, log))
    }

    /// A column of arrays of different shapes, as a `dict` of rows.
    #[pyo3(signature = (columnname, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, startrow=0, nrow=-1, rowincr=1)")]
    fn getvarcol(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let rows = row_selection(startrow, nrow, rowincr)?;
        self.read(py, move |state, log| state.getvarcol(&column, rows, log))
    }

    /// The shapes of a column's arrays, as text.
    #[pyo3(signature = (columnname, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, startrow=0, nrow=-1, rowincr=1)")]
    fn getcolshapestring(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let rows = row_selection(startrow, nrow, rowincr)?;
        self.read(py, move |state, log| {
            state.getcolshapestring(&column, rows, log)
        })
    }

    /// Whether a cell holds a value.
    #[pyo3(signature = (columnname, rownr = None), text_signature = "($self, columnname, rownr=0)")]
    fn iscelldefined(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        rownr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let row = integer(rownr, "rownr", 0)?;
        self.read(py, move |state, log| state.iscelldefined(&column, row, log))
    }

    /// The table keywords' names, in table order.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn keywordnames(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.read(py, |state, log| Ok(state.keywordnames(log)))
    }

    /// All table keywords.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn getkeywords(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.read(py, |state, log| Ok(state.getkeywords(log)))
    }

    /// One table keyword, by name or position.
    #[pyo3(signature = (keyword = None), text_signature = "($self, keyword=[])")]
    fn getkeyword(
        &self,
        py: Python<'_>,
        keyword: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let key = keyword_ref(keyword)?;
        self.read(py, move |state, log| state.getkeyword(&key, log))
    }

    /// The field names of a table keyword that is a record (all keywords for
    /// `''`).
    #[pyo3(signature = (keyword = None), text_signature = "($self, keyword='')")]
    fn fieldnames(
        &self,
        py: Python<'_>,
        keyword: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let key = keyword_ref(keyword)?;
        self.read(py, move |state, log| state.fieldnames(&key, log))
    }

    /// A column's keyword names.
    #[pyo3(signature = (columnname), text_signature = "($self, columnname)")]
    fn colkeywordnames(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        self.read(py, move |state, log| state.colkeywordnames(&column, log))
    }

    /// All of a column's keywords.
    #[pyo3(signature = (columnname), text_signature = "($self, columnname)")]
    fn getcolkeywords(&self, py: Python<'_>, columnname: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        self.read(py, move |state, log| state.getcolkeywords(&column, log))
    }

    /// One keyword of a column, by name or position.
    #[pyo3(signature = (columnname, keyword = None), text_signature = "($self, columnname, keyword=[])")]
    fn getcolkeyword(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        keyword: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let key = keyword_ref(keyword)?;
        self.read(py, move |state, log| {
            state.getcolkeyword(&column, &key, log)
        })
    }

    /// The field names of a column keyword that is a record (all keywords
    /// for `''`).
    #[pyo3(signature = (columnname, keyword = None), text_signature = "($self, columnname, keyword='')")]
    fn colfieldnames(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        keyword: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let key = keyword_ref(keyword)?;
        self.read(py, move |state, log| {
            state.colfieldnames(&column, &key, log)
        })
    }

    /// Define a table keyword (by 1-based position, or by name; a dotted
    /// name reaches into sub-records, made when `makesubrecord` is set).
    #[pyo3(signature = (keyword = None, value = None, makesubrecord = None), text_signature = "($self, keyword=[], value=[], makesubrecord=False)")]
    fn putkeyword(
        &self,
        py: Python<'_>,
        keyword: Option<&Bound<'_, PyAny>>,
        value: Option<&Bound<'_, PyAny>>,
        makesubrecord: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let key = keyword_ref(keyword)?;
        let value = keyword_value(py, value)?;
        let make = flag(makesubrecord, false)?;
        self.call(py, move |state, log| {
            state.putkeyword(key, value, make, log)
        })
    }

    /// Define several table keywords; the ones already there stay.
    #[pyo3(signature = (value), text_signature = "($self, value)")]
    fn putkeywords(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let fields = record_fields(value)?;
        self.call(py, move |state, log| state.putkeywords(fields, log))
    }

    /// Remove a table keyword.
    #[pyo3(signature = (keyword = None), text_signature = "($self, keyword=[])")]
    fn removekeyword(
        &self,
        py: Python<'_>,
        keyword: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let key = keyword_ref(keyword)?;
        self.call(py, move |state, log| state.removekeyword(key, log))
    }

    /// Define a keyword of a column.
    #[pyo3(signature = (columnname, keyword = None, value = None), text_signature = "($self, columnname, keyword=[], value=[])")]
    fn putcolkeyword(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        keyword: Option<&Bound<'_, PyAny>>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let key = keyword_ref(keyword)?;
        let value = keyword_value(py, value)?;
        self.call(py, move |state, log| {
            state.putcolkeyword(column, key, value, log)
        })
    }

    /// Define several keywords of a column; the ones already there stay.
    #[pyo3(signature = (columnname, value), text_signature = "($self, columnname, value)")]
    fn putcolkeywords(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let fields = record_fields(value)?;
        self.call(py, move |state, log| {
            state.putcolkeywords(column, fields, log)
        })
    }

    /// Remove a keyword of a column.
    #[pyo3(signature = (columnname, keyword = None), text_signature = "($self, columnname, keyword=[])")]
    fn removecolkeyword(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        keyword: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let key = keyword_ref(keyword)?;
        self.call(py, move |state, log| {
            state.removecolkeyword(column, key, log)
        })
    }

    /// Set the table's type, subtype and readme, from the fields of a record
    /// (`type`, `subType`, `readme`).
    #[pyo3(signature = (value), text_signature = "($self, value)")]
    fn putinfo(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let update = info_update(value)?;
        self.call(py, move |state, log| state.putinfo(update, log))
    }

    /// Append a line to the table's readme.
    #[pyo3(signature = (value), text_signature = "($self, value)")]
    fn addreadmeline(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let line = text(value, "value")?;
        self.call(py, move |state, log| state.addreadmeline(line, log))
    }

    /// Write one cell, coerced to the column's type.
    #[pyo3(signature = (columnname, rownr, thevalue = None), text_signature = "($self, columnname, rownr, thevalue=[])")]
    fn putcell(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        rownr: &Bound<'_, PyAny>,
        thevalue: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let rows = row_numbers(rownr, "rownr")?;
        let value = keyword_value(py, thevalue)?;
        self.call(py, move |state, log| {
            state.putcell(column, rows, value, log)
        })
    }

    /// Write rows of a column, the rows last in an array column's values.
    #[pyo3(signature = (columnname, value = None, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, value=[], startrow=0, nrow=-1, rowincr=1)")]
    fn putcol(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let value = keyword_value(py, value)?;
        let rows = row_selection(startrow, nrow, rowincr)?;
        self.call(py, move |state, log| state.putcol(column, value, rows, log))
    }

    /// Read a slice of an array cell: corners `blc` and `trc` are inclusive.
    #[pyo3(signature = (columnname, rownr, blc, trc, incr = None), text_signature = "($self, columnname, rownr, blc, trc, incr=[1])")]
    fn getcellslice(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        rownr: &Bound<'_, PyAny>,
        blc: &Bound<'_, PyAny>,
        trc: &Bound<'_, PyAny>,
        incr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let row = integer(Some(rownr), "rownr", 0)?;
        let corners = corners(Some(blc), Some(trc), incr)?;
        self.read(py, move |state, log| {
            state.getcellslice(&column, row, &corners, log)
        })
    }

    /// Read a slice of an array column, the rows last; `trc` may be `-1`.
    #[pyo3(signature = (columnname, blc, trc, incr, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, blc, trc, incr, startrow=0, nrow=-1, rowincr=1)")]
    fn getcolslice(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        blc: &Bound<'_, PyAny>,
        trc: &Bound<'_, PyAny>,
        incr: &Bound<'_, PyAny>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let corners = corners(Some(blc), Some(trc), Some(incr))?;
        let rows = row_selection(startrow, nrow, rowincr)?;
        self.read(py, move |state, log| {
            state.getcolslice(&column, &corners, rows, log)
        })
    }

    /// Write a slice of an array cell.
    #[pyo3(signature = (columnname, rownr, value = None, blc = None, trc = None, incr = None), text_signature = "($self, columnname, rownr, value=[], blc=[], trc=[], incr=[1])")]
    fn putcellslice(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        rownr: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        blc: Option<&Bound<'_, PyAny>>,
        trc: Option<&Bound<'_, PyAny>>,
        incr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let row = integer(Some(rownr), "rownr", 0)?;
        let value = keyword_value(py, value)?;
        let corners = corners(blc, trc, incr)?;
        self.call(py, move |state, log| {
            state.putcellslice(column, row, value, corners, log)
        })
    }

    /// Write a slice of an array column, the rows last.
    #[pyo3(signature = (columnname, value = None, blc = None, trc = None, incr = None, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, value=[], blc=[], trc=[], incr=[1], startrow=0, nrow=-1, rowincr=1)")]
    fn putcolslice(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        blc: Option<&Bound<'_, PyAny>>,
        trc: Option<&Bound<'_, PyAny>>,
        incr: Option<&Bound<'_, PyAny>>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let value = keyword_value(py, value)?;
        let corners = corners(blc, trc, incr)?;
        let rows = row_selection(startrow, nrow, rowincr)?;
        self.call(py, move |state, log| {
            state.putcolslice(column, value, corners, rows, log)
        })
    }

    /// Write the selected rows of a column from the fields of a record, in
    /// the order they have.
    #[pyo3(signature = (columnname, value, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, value, startrow=0, nrow=-1, rowincr=1)")]
    fn putvarcol(
        &self,
        py: Python<'_>,
        columnname: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let column = text(columnname, "columnname")?;
        let fields = record_fields(value)?;
        let rows = row_selection(startrow, nrow, rowincr)?;
        self.call(py, move |state, log| {
            state.putvarcol(column, fields, rows, log)
        })
    }
}
