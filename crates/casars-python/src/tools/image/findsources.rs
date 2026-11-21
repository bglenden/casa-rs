// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::ImageInterface;
use casa_images::analysis::{FindSourcesOptions, find_image_sources};
use pyo3::prelude::*;

use super::PyImageTool;
use super::attachment::read_locked;
use super::data_compat::record_to_py;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::{RegionArgument, mask_argument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Find compact emission sources in the selected sky plane.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (nmax=20, cutoff=0.1, region=None, mask=None, point=true, width=5, negfind=false), text_signature = "($self, nmax=20, cutoff=0.1, region={}, mask='', point=True, width=5, negfind=False)")]
    fn findsources<'py>(
        &self,
        py: Python<'py>,
        nmax: i64,
        cutoff: f64,
        region: Option<&Bound<'py, PyAny>>,
        mask: Option<&Bound<'py, PyAny>>,
        #[pyo3(from_py_with = require_bool)] point: bool,
        width: i64,
        #[pyo3(from_py_with = require_bool)] negfind: bool,
    ) -> PyResult<Py<PyAny>> {
        let attached_float = py.detach(|| {
            self.state()
                .image
                .as_ref()
                .map(|source| matches!(*source.lock(), ImageHandle::Float(_)))
        });
        match attached_float {
            None => return record_to_py(py, Default::default()),
            Some(false) => {
                return Err(raised("This application supports only float-valued images"));
            }
            Some(true) => {}
        }
        let region: RegionArgument =
            region.map_or_else(|| Ok(Default::default()), |argument| argument.extract())?;
        let mask = mask_argument(mask)?;
        let record = py.detach(|| {
            let state = self.state();
            let Some(source) = &state.image else {
                return Ok(Default::default());
            };
            read_locked(source, |image| {
                let ImageHandle::Float(storage) = image else {
                    return Err(raised("This application supports only float-valued images"));
                };
                let maximum =
                    usize::try_from(nmax).map_err(|_| raised("nmax must be nonnegative"))?;
                let coordinates = image.coordinate_record();
                let search = |source: &dyn ImageInterface<f32>| {
                    find_image_sources(
                        source,
                        coordinates,
                        FindSourcesOptions {
                            region: region.selection(),
                            mask: Some(&mask),
                            maximum,
                            cutoff,
                            point,
                            fit_width: width,
                            find_negative: negfind,
                        },
                    )
                    .map_err(raised)
                };
                match storage {
                    Storage::Temp {
                        image,
                        fits: Some(fits),
                        ..
                    } => search(&fits.image_view(image)),
                    _ => with_storage!(storage, image => search(image.as_ref())),
                }
            })
        })?;
        record_to_py(py, record)
    }
}
