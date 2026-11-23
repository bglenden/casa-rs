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
    #[pyo3(signature = (), text_signature = "($self)")]
    fn summary(&self) -> PyResult<Py<PyAny>> {
        stub("summary")
    }
}
