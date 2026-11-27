// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;
use std::sync::PoisonError;

use casa_singledish::SingleDishAtmosphere;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::PySingleDishMs;
use crate::tools::table::casa_record;

#[pymethods]
impl PySingleDishMs {
    #[pyo3(signature = (config = None, datacolumn = "data", outfile = ""), text_signature = "($self, config={}, datacolumn='data', outfile='')")]
    fn atmcor(
        &self,
        py: Python<'_>,
        config: Option<&Bound<'_, PyDict>>,
        datacolumn: &str,
        outfile: &str,
    ) -> PyResult<bool> {
        let config = casa_record(py, config)?;
        py.detach(|| {
            self.session
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .atmcor(SingleDishAtmosphere {
                    config: &config,
                    datacolumn,
                    outfile: Path::new(outfile),
                    overwrite: true,
                })
                .map(|_| true)
        })
        .map_err(PyRuntimeError::new_err)
    }
}
