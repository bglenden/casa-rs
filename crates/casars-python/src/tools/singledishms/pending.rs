// SPDX-License-Identifier: LGPL-3.0-or-later
//! Detached import methods for single-dish instrument formats.

use std::path::Path;

use pyo3::prelude::*;

use super::PySingleDishMs;
use crate::tools::not_yet_implemented_in;

#[pymethods]
#[allow(unused_variables, clippy::too_many_arguments, clippy::unused_self)]
impl PySingleDishMs {
    #[pyo3(signature = (infile = "", outfile = "", parallel = false), text_signature = "($self, infile='', outfile='', parallel=False)")]
    fn importasap(&self, py: Python<'_>, infile: &str, outfile: &str, parallel: bool) -> bool {
        py.detach(|| {
            casa_calibration::import_asap(Path::new(infile), Path::new(outfile), parallel).is_ok()
        })
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
