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
    #[pyo3(signature = (autoc = None, flagged = None), text_signature = "($self, autoc=True, flagged=True)")]
    fn nrows(
        &self,
        autoc: Option<&Bound<'_, PyAny>>,
        flagged: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("nrows")
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
    #[pyo3(signature = (field = None, epoch = None), text_signature = "($self, field=0, epoch={})")]
    fn refdir(
        &self,
        field: Option<&Bound<'_, PyAny>>,
        epoch: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("refdir")
    }

    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (), text_signature = "($self)")]
    fn summary(&self) -> PyResult<Py<PyAny>> {
        stub("summary")
    }
}
