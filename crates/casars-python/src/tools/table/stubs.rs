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
    #[pyo3(signature = (desc, dminfo = None), text_signature = "($self, desc, dminfo={})")]
    fn addcols(
        &self,
        desc: &Bound<'_, PyAny>,
        dminfo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("addcols")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value), text_signature = "($self, value)")]
    fn addreadmeline(&self, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("addreadmeline")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (nrow = None), text_signature = "($self, nrow=1)")]
    fn addrows(&self, nrow: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("addrows")
    }

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
    #[pyo3(signature = (), text_signature = "($self)")]
    fn clearlocks(&self) -> PyResult<Py<PyAny>> {
        stub("clearlocks")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (newtablename, deep = None, valuecopy = None, dminfo = None, endian = None, memorytable = None, returnobject = None, norows = None), text_signature = "($self, newtablename, deep=False, valuecopy=False, dminfo={}, endian='aipsrc', memorytable=False, returnobject=False, norows=False)")]
    fn copy(
        &self,
        newtablename: &Bound<'_, PyAny>,
        deep: Option<&Bound<'_, PyAny>>,
        valuecopy: Option<&Bound<'_, PyAny>>,
        dminfo: Option<&Bound<'_, PyAny>>,
        endian: Option<&Bound<'_, PyAny>>,
        memorytable: Option<&Bound<'_, PyAny>>,
        returnobject: Option<&Bound<'_, PyAny>>,
        norows: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("copy")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outtable, startrowin = None, startrowout = None, nrow = None), text_signature = "($self, outtable, startrowin=0, startrowout=-1, nrow=-1)")]
    fn copyrows(
        &self,
        outtable: &Bound<'_, PyAny>,
        startrowin: Option<&Bound<'_, PyAny>>,
        startrowout: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("copyrows")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (tablename = None, tabledesc = None, lockoptions = None, endianformat = None, memtype = None, nrow = None, dminfo = None), text_signature = "($self, tablename='', tabledesc={}, lockoptions={}, endianformat='', memtype='', nrow=0, dminfo={})")]
    fn create(
        &self,
        tablename: Option<&Bound<'_, PyAny>>,
        tabledesc: Option<&Bound<'_, PyAny>>,
        lockoptions: Option<&Bound<'_, PyAny>>,
        endianformat: Option<&Bound<'_, PyAny>>,
        memtype: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        dminfo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("create")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outputTableName, tables, subdirname), text_signature = "($self, outputTableName, tables, subdirname)")]
    fn createmultitable(
        &self,
        outputTableName: &Bound<'_, PyAny>,
        tables: &Bound<'_, PyAny>,
        subdirname: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("createmultitable")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn datachanged(&self) -> PyResult<Py<PyAny>> {
        stub("datachanged")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn endianformat(&self) -> PyResult<Py<PyAny>> {
        stub("endianformat")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn flush(&self) -> PyResult<Py<PyAny>> {
        stub("flush")
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
    #[pyo3(signature = (columnname, rownr, blc, trc, incr = None), text_signature = "($self, columnname, rownr, blc, trc, incr=[1])")]
    fn getcellslice(
        &self,
        columnname: &Bound<'_, PyAny>,
        rownr: &Bound<'_, PyAny>,
        blc: &Bound<'_, PyAny>,
        trc: &Bound<'_, PyAny>,
        incr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getcellslice")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname), text_signature = "($self, columnname)")]
    fn getcoldesc(&self, columnname: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("getcoldesc")
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
    #[pyo3(signature = (columnname, blc, trc, incr, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, blc, trc, incr, startrow=0, nrow=-1, rowincr=1)")]
    fn getcolslice(
        &self,
        columnname: &Bound<'_, PyAny>,
        blc: &Bound<'_, PyAny>,
        trc: &Bound<'_, PyAny>,
        incr: &Bound<'_, PyAny>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("getcolslice")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (actual = None), text_signature = "($self, actual=True)")]
    fn getdesc(&self, actual: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("getdesc")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn getdminfo(&self) -> PyResult<Py<PyAny>> {
        stub("getdminfo")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (checksubtables = None), text_signature = "($self, checksubtables=False)")]
    fn ismultiused(&self, checksubtables: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("ismultiused")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (tablename), text_signature = "($self, tablename)")]
    fn isopened(&self, tablename: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("isopened")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn listlocks(&self) -> PyResult<Py<PyAny>> {
        stub("listlocks")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname, rownr, thevalue = None), text_signature = "($self, columnname, rownr, thevalue=[])")]
    fn putcell(
        &self,
        columnname: &Bound<'_, PyAny>,
        rownr: &Bound<'_, PyAny>,
        thevalue: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putcell")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname, rownr, value = None, blc = None, trc = None, incr = None), text_signature = "($self, columnname, rownr, value=[], blc=[], trc=[], incr=[1])")]
    fn putcellslice(
        &self,
        columnname: &Bound<'_, PyAny>,
        rownr: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        blc: Option<&Bound<'_, PyAny>>,
        trc: Option<&Bound<'_, PyAny>>,
        incr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putcellslice")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname, value = None, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, value=[], startrow=0, nrow=-1, rowincr=1)")]
    fn putcol(
        &self,
        columnname: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putcol")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname, keyword = None, value = None), text_signature = "($self, columnname, keyword=[], value=[])")]
    fn putcolkeyword(
        &self,
        columnname: &Bound<'_, PyAny>,
        keyword: Option<&Bound<'_, PyAny>>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putcolkeyword")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname, value), text_signature = "($self, columnname, value)")]
    fn putcolkeywords(
        &self,
        columnname: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("putcolkeywords")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname, value = None, blc = None, trc = None, incr = None, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, value=[], blc=[], trc=[], incr=[1], startrow=0, nrow=-1, rowincr=1)")]
    fn putcolslice(
        &self,
        columnname: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        blc: Option<&Bound<'_, PyAny>>,
        trc: Option<&Bound<'_, PyAny>>,
        incr: Option<&Bound<'_, PyAny>>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putcolslice")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value), text_signature = "($self, value)")]
    fn putinfo(&self, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("putinfo")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (keyword = None, value = None, makesubrecord = None), text_signature = "($self, keyword=[], value=[], makesubrecord=False)")]
    fn putkeyword(
        &self,
        keyword: Option<&Bound<'_, PyAny>>,
        value: Option<&Bound<'_, PyAny>>,
        makesubrecord: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putkeyword")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (value), text_signature = "($self, value)")]
    fn putkeywords(&self, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("putkeywords")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnname, value, startrow = None, nrow = None, rowincr = None), text_signature = "($self, columnname, value, startrow=0, nrow=-1, rowincr=1)")]
    fn putvarcol(
        &self,
        columnname: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        startrow: Option<&Bound<'_, PyAny>>,
        nrow: Option<&Bound<'_, PyAny>>,
        rowincr: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("putvarcol")
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
    #[pyo3(signature = (columnname, keyword = None), text_signature = "($self, columnname, keyword=[])")]
    fn removecolkeyword(
        &self,
        columnname: &Bound<'_, PyAny>,
        keyword: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("removecolkeyword")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (columnames), text_signature = "($self, columnames)")]
    fn removecols(&self, columnames: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("removecols")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (keyword = None), text_signature = "($self, keyword=[])")]
    fn removekeyword(&self, keyword: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("removekeyword")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (rownrs), text_signature = "($self, rownrs)")]
    fn removerows(&self, rownrs: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("removerows")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (oldname, newname), text_signature = "($self, oldname, newname)")]
    fn renamecol(
        &self,
        oldname: &Bound<'_, PyAny>,
        newname: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("renamecol")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn resync(&self) -> PyResult<Py<PyAny>> {
        stub("resync")
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
    #[pyo3(signature = (tab = None, nbytes = None), text_signature = "($self, tab={}, nbytes=0)")]
    fn rownumbers(
        &self,
        tab: Option<&Bound<'_, PyAny>>,
        nbytes: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("rownumbers")
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
    #[pyo3(signature = (columnname, nbytes), text_signature = "($self, columnname, nbytes)")]
    fn setmaxcachesize(
        &self,
        columnname: &Bound<'_, PyAny>,
        nbytes: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        stub("setmaxcachesize")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (verbose = None), text_signature = "($self, verbose=True)")]
    fn showcache(&self, verbose: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("showcache")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (column, complex_value = None), text_signature = "($self, column, complex_value='')")]
    fn statistics(
        &self,
        column: &Bound<'_, PyAny>,
        complex_value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("statistics")
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
    #[pyo3(signature = (column), text_signature = "($self, column)")]
    fn testincrstman(&self, column: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("testincrstman")
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
