// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's `ms` tool (ms): every method of CASA 6.7.6.14's binding,
//! with its signature (ADR-0019, #1025). A method casa-rs does not implement
//! yet raises `NotImplementedError` naming #1030 (`stubs.rs`).

mod stubs;
pub(crate) mod surface;

use pyo3::prelude::*;

use crate::tools::not_yet_implemented_in;

/// The issue that records the methods casa-rs refuses.
const ISSUE: u32 = 1030;

/// A method casa-rs does not implement yet.
fn stub(method: &str) -> PyResult<Py<PyAny>> {
    not_yet_implemented_in("ms", method, ISSUE)
}

/// CASA's `ms` tool.
#[pyclass(name = "ms", module = "casars.tools", frozen)]
pub(crate) struct PyMsTool;

#[pymethods]
impl PyMsTool {
    #[new]
    fn new() -> Self {
        Self
    }
}
