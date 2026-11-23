// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `msmetadata` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1029.

use pyo3::prelude::*;

use super::{PyMsMetadata, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyMsMetadata {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (antenna = None), text_signature = "($self, antenna=-1)")]
    fn antennadiameter(&self, antenna: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("antennadiameter")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (name = None, mindiameter = None, maxdiameter = None, obsid = None), text_signature = "($self, name='', mindiameter='0m', maxdiameter='1pc', obsid=-1)")]
    fn antennaids(
        &self,
        name: Option<&Bound<'_, PyAny>>,
        mindiameter: Option<&Bound<'_, PyAny>>,
        maxdiameter: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("antennaids")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None), text_signature = "($self, which=0)")]
    fn antennaoffset(&self, which: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("antennaoffset")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None), text_signature = "($self, which=0)")]
    fn antennaposition(&self, which: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("antennaposition")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None, obsid = None), text_signature = "($self, which=-1, obsid=-1)")]
    fn antennastations(
        &self,
        which: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("antennastations")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn effexposuretime(&self) -> PyResult<Py<PyAny>> {
        stub("effexposuretime")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (scan = None, spwid = None, polid = None, obsid = None, arrayid = None), text_signature = "($self, scan=0, spwid=0, polid=-1, obsid=0, arrayid=0)")]
    fn exposuretime(
        &self,
        scan: Option<&Bound<'_, PyAny>>,
        spwid: Option<&Bound<'_, PyAny>>,
        polid: Option<&Bound<'_, PyAny>>,
        obsid: Option<&Bound<'_, PyAny>>,
        arrayid: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("exposuretime")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (source = None, asnames = None), text_signature = "($self, source=-1, asnames=False)")]
    fn fieldsforsource(
        &self,
        source: Option<&Bound<'_, PyAny>>,
        asnames: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fieldsforsource")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (asnames = None), text_signature = "($self, asnames=False)")]
    fn fieldsforsources(&self, asnames: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("fieldsforsources")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (autoc = None, flagged = None), text_signature = "($self, autoc=True, flagged=True)")]
    fn nrows(
        &self,
        autoc: Option<&Bound<'_, PyAny>>,
        flagged: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("nrows")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (which = None), text_signature = "($self, which=0)")]
    fn observatoryposition(&self, which: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("observatoryposition")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (fieldid = None, epoch = None), text_signature = "($self, fieldid=0, epoch={})")]
    fn phasecenter(
        &self,
        fieldid: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("phasecenter")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (rownum = None, interpolate = None, initialrow = None), text_signature = "($self, rownum=0, interpolate=False, initialrow=0)")]
    fn pointingdirection(
        &self,
        rownum: Option<&Bound<'_, PyAny>>,
        interpolate: Option<&Bound<'_, PyAny>>,
        initialrow: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("pointingdirection")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn propermotions(&self) -> PyResult<Py<PyAny>> {
        stub("propermotions")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (field = None, epoch = None), text_signature = "($self, field=0, epoch={})")]
    fn refdir(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("refdir")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (sourceid = None, spw = None), text_signature = "($self, sourceid=0, spw=0)")]
    fn restfreqs(
        &self,
        sourceid: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("restfreqs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None), text_signature = "($self, obsid=-1)")]
    fn schedule(&self, obsid: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("schedule")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourcedirs(&self) -> PyResult<Py<PyAny>> {
        stub("sourcedirs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn sourcetimes(&self) -> PyResult<Py<PyAny>> {
        stub("sourcetimes")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn summary(&self) -> PyResult<Py<PyAny>> {
        stub("summary")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (obsid = None), text_signature = "($self, obsid=-1)")]
    fn timerangeforobs(&self, obsid: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        stub("timerangeforobs")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (sourceid = None, spw = None), text_signature = "($self, sourceid=0, spw=0)")]
    fn transitions(
        &self,
        sourceid: Option<&Bound<'_, PyAny>>,
        spw: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("transitions")
    }
}
