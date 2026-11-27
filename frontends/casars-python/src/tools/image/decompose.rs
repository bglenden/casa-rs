// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.decompose` wrapper delegating to [`casa_imageanalysis::tool::ops_fit::decompose`].

use casa_imageanalysis::analysis::DecompositionOptions;
use casa_imageanalysis::tool::ops_fit::decompose;
use numpy::PyArrayDyn;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::PyImageTool;
use super::handle::raised;
use super::subimage::{RegionArgument, mask_argument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Identify and fit separated emission components within a selected image.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (region=RegionArgument::default(), mask=None, simple=false, threshold=-1.0, ncontour=11, minrange=1, naxis=2, fit=true, maxrms=-1.0, maxretry=-1, maxiter=256, convcriteria=0.0001, stretch=false), text_signature = "($self, region={}, mask='', simple=False, threshold=-1.0, ncontour=11, minrange=1, naxis=2, fit=True, maxrms=-1.0, maxretry=-1, maxiter=256, convcriteria=0.0001, stretch=False)")]
    fn decompose<'py>(
        &self,
        py: Python<'py>,
        region: RegionArgument,
        mask: Option<&Bound<'py, PyAny>>,
        #[pyo3(from_py_with = require_bool)] simple: bool,
        threshold: f64,
        ncontour: i64,
        minrange: i64,
        naxis: i64,
        #[pyo3(from_py_with = require_bool)] fit: bool,
        maxrms: f64,
        maxretry: i64,
        maxiter: i64,
        convcriteria: f64,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        let mask = mask_argument(mask)?;
        let output = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(source) = &state.image else {
                return Ok(None);
            };
            decompose(
                source,
                DecompositionOptions {
                    region: region.selection(),
                    mask: Some(&mask),
                    stretch,
                    simple,
                    threshold,
                    contours: ncontour,
                    min_range: minrange,
                    neighbor_axes: naxis,
                    fit,
                    maximum_rms: maxrms,
                    max_retry: maxretry,
                    max_iterations: maxiter,
                    criterion: convcriteria,
                },
            )
            .map(Some)
            .map_err(raised)
        })?;
        let result = PyDict::new(py);
        if let Some(output) = output {
            result.set_item("blc", PyArrayDyn::from_owned_array(py, output.blc))?;
            result.set_item(
                "components",
                PyArrayDyn::from_owned_array(py, output.components),
            )?;
            result.set_item("trc", PyArrayDyn::from_owned_array(py, output.trc))?;
        }
        Ok(result)
    }
}
