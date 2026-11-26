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
    #[pyo3(signature = (field = None, fitspw = None, spw = None, solint = None, fitorder = None, mode = None), text_signature = "($self, field=[], fitspw=[], spw=[], solint=[], fitorder=0, mode='subtract')")]
    fn continuumsub(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        solint: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("continuumsub")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None, fitspw = None, spw = None, solint = None, fitorder = None, mode = None), text_signature = "($self, field=[], fitspw=[], spw=[], solint=[], fitorder=0, mode='subtract')")]
    fn continuumsubold(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        solint: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        mode: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("continuumsubold")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outputms = None, fitspw = None, fitorder = None, combine = None, spw = None, unionspw = None, field = None, scan = None, intent = None, correlation = None, obs = None, whichcol = None), text_signature = "($self, outputms='', fitspw=[], fitorder=1, combine='', spw='*', unionspw='*', field='', scan='', intent='', correlation='', obs='', whichcol='CORRECTED_DATA')")]
    fn contsub(
        &self,
        outputms: Option<&Bound<'_, PyAny>>,
        fitspw: Option<&Bound<'_, PyAny>>,
        fitorder: Option<&Bound<'_, PyAny>>,
        combine: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
        unionspw: Option<&Bound<'_, PyAny>>,
        field: Option<&Bound<'_, PyAny>>,
        scan: Option<&Bound<'_, PyAny>>,
        intent: Option<&Bound<'_, PyAny>>,
        correlation: Option<&Bound<'_, PyAny>>,
        obs: Option<&Bound<'_, PyAny>>,
        whichcol: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("contsub")
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
    #[pyo3(signature = (fitsfile), text_signature = "($self, fitsfile)")]
    fn listfits(&self, fitsfile: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        stub("listfits")
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
