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
}
