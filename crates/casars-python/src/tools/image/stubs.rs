// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `image` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1027.

use pyo3::prelude::*;

use super::{PyImageTool, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyImageTool {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (outfile = None, shape = None, cl = None, csys = None, overwrite = None, log = None, cache = None), text_signature = "($self, outfile='', shape=[], cl='', csys={}, overwrite=False, log=True, cache=True)")]
    fn fromcomplist(
        &self,
        outfile: Option<&Bound<'_, PyAny>>,
        shape: Option<&Bound<'_, PyAny>>,
        cl: Option<&Bound<'_, PyAny>>,
        csys: Option<&Bound<'_, PyAny>>,
        overwrite: Option<&Bound<'_, PyAny>>,
        log: Option<&Bound<'_, PyAny>>,
        cache: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("fromcomplist")
    }
}
