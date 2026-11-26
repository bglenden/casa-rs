// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `ms` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1030.

use pyo3::prelude::*;

use super::{PyMsTool, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyMsTool {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (abspath = None), text_signature = "($self, abspath='')")]
    fn asdmref(&self, abspath: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("asdmref")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msfile = None, fitsfile = None, nomodify = None, lock = None, obstype = None, host = None, forcenewserver = None, antnamescheme = None), text_signature = "($self, msfile='', fitsfile='', nomodify=True, lock=False, obstype=0, host='', forcenewserver=False, antnamescheme='old')")]
    fn fromfits(
        &self,
        msfile: Option<&Bound<'_, PyAny>>,
        fitsfile: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
        lock: Option<&Bound<'_, PyAny>>,
        obstype: Option<&Bound<'_, PyAny>>,
        host: Option<&Bound<'_, PyAny>>,
        forcenewserver: Option<&Bound<'_, PyAny>>,
        antnamescheme: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromfits")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (msfile = None, fitsfile = None, nomodify = None, lock = None, obstype = None), text_signature = "($self, msfile='', fitsfile='', nomodify=True, lock=False, obstype=0)")]
    fn fromfitsidi(
        &self,
        msfile: Option<&Bound<'_, PyAny>>,
        fitsfile: Option<&Bound<'_, PyAny>>,
        nomodify: Option<&Bound<'_, PyAny>>,
        lock: Option<&Bound<'_, PyAny>>,
        obstype: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromfitsidi")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (fitsfile = None, column = None, field = None, spw = None, baseline = None, time = None, scan = None, uvrange = None, taql = None, writesyscal = None, multisource = None, combinespw = None, writestation = None, padwithflags = None, overwrite = None), text_signature = "($self, fitsfile='', column='corrected', field=[], spw=[], baseline=[], time='', scan=[], uvrange=[], taql='', writesyscal=False, multisource=False, combinespw=False, writestation=False, padwithflags=False, overwrite=False)")]
    fn tofits(
        &self,
        fitsfile: Option<&Bound<'_, PyAny>>,
        column: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        baseline: Option<&Bound<'_, PyAny>>,
        time: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        uvrange: Option<&Bound<'_, PyAny>>,
        taql: Option<&Bound<'_, PyAny>>,
        writesyscal: Option<&Bound<'_, PyAny>>,
        multisource: Option<&Bound<'_, PyAny>>,
        combinespw: Option<&Bound<'_, PyAny>>,
        writestation: Option<&Bound<'_, PyAny>>,
        padwithflags: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("tofits")
    }
}
