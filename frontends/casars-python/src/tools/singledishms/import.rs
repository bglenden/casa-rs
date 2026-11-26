// SPDX-License-Identifier: LGPL-3.0-or-later
//! Detached import methods for single-dish instrument formats.

use std::path::Path;

use pyo3::prelude::*;

use super::PySingleDishMs;

#[pymethods]
#[allow(clippy::unused_self)]
impl PySingleDishMs {
    #[pyo3(signature = (infile = "", outfile = "", parallel = false), text_signature = "($self, infile='', outfile='', parallel=False)")]
    fn importasap(&self, py: Python<'_>, infile: &str, outfile: &str, parallel: bool) -> bool {
        py.detach(|| {
            casa_calibration::import_asap(Path::new(infile), Path::new(outfile), parallel).is_ok()
        })
    }

    #[pyo3(signature = (infile = "", outfile = "", parallel = false), text_signature = "($self, infile='', outfile='', parallel=False)")]
    fn importnro(&self, py: Python<'_>, infile: &str, outfile: &str, parallel: bool) -> bool {
        py.detach(|| {
            casa_calibration::import_nro(Path::new(infile), Path::new(outfile), parallel).is_ok()
        })
    }
}
