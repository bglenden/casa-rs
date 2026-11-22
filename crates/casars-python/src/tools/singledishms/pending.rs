// SPDX-License-Identifier: LGPL-3.0-or-later
//! Public signatures for the remaining operations in CAL-20.

use pyo3::prelude::*;

use super::PySingleDishMs;
use crate::tools::not_yet_implemented_in;

#[pymethods]
#[allow(unused_variables, clippy::too_many_arguments, clippy::unused_self)]
impl PySingleDishMs {
    #[pyo3(signature = (infile = None, outfile = None, parallel = None), text_signature = "($self, infile='', outfile='', parallel=False)")]
    fn importasap(
        &self,
        infile: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        parallel: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "importasap", 1532)
    }

    #[pyo3(signature = (infile = None, outfile = None, parallel = None), text_signature = "($self, infile='', outfile='', parallel=False)")]
    fn importnro(
        &self,
        infile: Option<&Bound<'_, PyAny>>,
        outfile: Option<&Bound<'_, PyAny>>,
        parallel: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        not_yet_implemented_in("singledishms", "importnro", 1532)
    }
}
