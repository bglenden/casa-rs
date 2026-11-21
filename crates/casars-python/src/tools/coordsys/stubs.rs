// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `coordsys` methods casa-rs does not implement yet: each has CASA's
//! signature and refuses, naming #1027.

use pyo3::prelude::*;

use super::{PyCoordsys, stub};

#[pymethods]
#[allow(
    non_snake_case,
    unused_variables,
    clippy::too_many_arguments,
    clippy::unused_self
)]
impl PyCoordsys {
    /// Not yet implemented in casa-rs.
    #[pyo3(signature = (doppler = None, list = None), text_signature = "($self, doppler='RADIO', list=True)")]
    fn summary(
        &self,
        doppler: Option<&Bound<'_, PyAny>>,
        list: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        stub("summary")
    }
}
