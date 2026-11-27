// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.imagecalc` wrapper delegating to [`casa_imageanalysis::tool::imagecalc`].

use casa_imageanalysis::tool::imagecalc::evaluate_image_calc_casa;
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{external_open_check, raised};

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (outfile="", pixels="", overwrite=false, imagemd="", prec="float"), text_signature = "($self, outfile='', pixels='', overwrite=False, imagemd='', prec='float')")]
    fn imagecalc(
        &self,
        py: Python<'_>,
        outfile: &str,
        pixels: &str,
        overwrite: bool,
        imagemd: &str,
        prec: &str,
    ) -> PyResult<Py<PyImageTool>> {
        let attached = py.detach(|| {
            let history = self.state().history;
            evaluate_image_calc_casa(
                outfile,
                pixels,
                overwrite,
                imagemd,
                prec,
                history,
                external_open_check(),
            )
            .map_err(raised)
        })?;
        Py::new(py, Self::attached(attached))
    }
}
