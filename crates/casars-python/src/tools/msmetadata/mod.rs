// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `msmetadata` tool (msmd): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0019, #1025). A method casa-rs does not implement
//! yet raises `NotImplementedError` naming #1029 (`stubs.rs`).

mod stubs;
pub(crate) mod surface;

use pyo3::prelude::*;

use crate::tools::not_yet_implemented_in;

/// The issue that records the methods casa-rs refuses.
const ISSUE: u32 = 1029;

/// A method casa-rs does not implement yet.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("msmetadata", method, ISSUE)
}

/// CASA's `msmetadata` tool.
#[pyclass(name = "msmetadata", module = "casars.tools", frozen)]
pub(crate) struct PyMsMetadata;

#[pymethods]
impl PyMsMetadata {
    #[new]
    fn new() -> Self {
        Self
    }
}
