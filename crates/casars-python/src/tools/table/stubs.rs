// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `table` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1028.

use pyo3::prelude::*;

use super::{PyTableTool, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyTableTool {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn browse(&self) -> PyResult<Py<PyAny>> {
        stub("browse")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (expr, prefix = None, showtaql = None), text_signature = "($self, expr, prefix='using style base0, endincl, fortranorder', showtaql=False)")]
    fn calc(
        &self,
        expr: &Bound<'_, PyAny>,
        prefix: Option<&Bound<'_, PyAny>>,
        showtaql: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("calc")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (tablename, xmlfile), text_signature = "($self, tablename, xmlfile)")]
    fn fromASDM(
        &self,
        tablename: &Bound<'_, PyAny>,
        xmlfile: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromASDM")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (tablename, asciifile, headerfile = None, autoheader = None, autoshape = None, sep = None, commentmarker = None, firstline = None, lastline = None, nomodify = None, columnnames = None, datatypes = None), text_signature = "($self, tablename, asciifile, headerfile='', autoheader=False, autoshape=[-1], sep='', commentmarker='', firstline=0, lastline=-1, nomodify=True, columnnames=[], datatypes=[])")]
    fn fromascii(
        &self,
        tablename: &Bound<'_, PyAny>,
        asciifile: &Bound<'_, PyAny>,
        headerfile: Option<&Bound<'_, PyAny>>,
        autoheader: Option<&Bound<'_, PyAny>>,
        autoshape: Option<&Bound<'_, PyAny>>,
        sep: Option<&Bound<'_, PyAny>>,
        commentmarker: Option<&Bound<'_, PyAny>>,
        firstline: Option<&Bound<'_, PyAny>>,
        lastline: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
        columnnames: Option<&Bound<'_, PyAny>>,
        datatypes: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromascii")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (tablename, fitsfile, whichhdu = None, storage = None, convention = None, nomodify = None, ack = None), text_signature = "($self, tablename, fitsfile, whichhdu=1, storage='standard', convention='none', nomodify=True, ack=True)")]
    fn fromfits(
        &self,
        tablename: &Bound<'_, PyAny>,
        fitsfile: &Bound<'_, PyAny>,
        whichhdu: Option<&Bound<'_, PyAny>>,
        storage: Option<&Bound<'_, PyAny>>,
        convention: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
        ack: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromfits")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname, startrow = None, nrow = None, rowincr = None, torecord = None), text_signature = "($self, columnname, startrow=0, nrow=-1, rowincr=1, torecord=False)")]
    fn getcoliter(
        &self,
        columnname: &Bound<'_, PyAny>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
        torecord: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getcoliter")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (query = None, name = None, sortlist = None, columns = None, style = None), text_signature = "($self, query='String', name='', sortlist='', columns='', style='')")]
    fn query(
        &self,
        query: Option<&Bound<'_, PyAny>>,
        name: Option<&Bound<'_, PyAny>>,
        sortlist: Option<&Bound<'_, PyAny>>,
        columns: Option<&Bound<'_, PyAny>>,
        style: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("query")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnnames = None, exclude = None), text_signature = "($self, columnnames=[], exclude=False)")]
    fn row(
        &self,
        columnnames: Option<&Bound<'_, PyAny>>,
        exclude: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("row")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (rownrs, name = None), text_signature = "($self, rownrs, name='')")]
    fn selectrows(
        &self,
        rownrs: &Bound<'_, PyAny>,
        name: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("selectrows")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (recurse = None), text_signature = "($self, recurse=False)")]
    fn summary(&self, recurse: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("summary")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (taqlcommand = None), text_signature = "($self, taqlcommand='TaQL expression')")]
    fn taql(&self, taqlcommand: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("taql")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (asciifile, headerfile = None, columns = None, sep = None), text_signature = "($self, asciifile, headerfile='', columns=[], sep='')")]
    fn toasciifmt(
        &self,
        asciifile: &Bound<'_, PyAny>,
        headerfile: Option<&Bound<'_, PyAny>>,
        columns: Option<&Bound<'_, PyAny>>,
        sep: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("toasciifmt")
    }
}
