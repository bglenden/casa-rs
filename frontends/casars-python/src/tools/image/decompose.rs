// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::ImageInterface;
use casa_images::analysis::{DecompositionOptions, decompose_image};
use numpy::PyArrayDyn;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::PyImageTool;
use super::attachment::read_locked;
use super::handle::{ImageHandle, Storage, raised, with_storage};
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
        let output = py.detach(|| {
            let state = self.state();
            let Some(source) = &state.image else {
                return Ok(None);
            };
            read_locked(source, |image| {
                let ImageHandle::Float(storage) = image else {
                    return Err(raised("This application supports only real-valued images"));
                };
                let record = image.coordinate_record();
                let decompose = |source: &dyn ImageInterface<f32>| {
                    decompose_image(
                        source,
                        record,
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
                    .map_err(raised)
                };
                match storage {
                    Storage::Temp {
                        image,
                        fits: Some(fits),
                        ..
                    } => decompose(&fits.image_view(image)),
                    _ => with_storage!(storage, image => decompose(image.as_ref())),
                }
            })
            .map(Some)
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
