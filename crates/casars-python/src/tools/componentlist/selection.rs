// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's component-index argument validation.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::convert::integer_vector;

pub(super) fn indices(value: &Bound<'_, PyAny>) -> PyResult<(Vec<i64>, bool)> {
    if value.is_instance_of::<PyString>() {
        return Err(PyTypeError::new_err("argument _which must not be a string"));
    }
    integer_vector::flatten(
        value,
        "cyclic component selection (CASA crash: #1176)",
        "non-finite component selection (CASA crash: #1176)",
    )
}
