// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imageanalysis::analysis::{
    FindSourcesOptions, MaxFitOptions, find_image_sources, fit_image_maximum,
};
use casa_images::ImageInterface;
use pyo3::prelude::*;

use crate::tools::{LogLine, emit};

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

    /// Fit one point or Gaussian source in the region's brightest sky plane.
    #[pyo3(signature = (region=None, point=true, width=5, negfind=false, list=true), text_signature = "($self, region={}, point=True, width=5, negfind=False, list=True)")]
    fn maxfit<'py>(
        &self,
        py: Python<'py>,
        region: Option<&Bound<'py, PyAny>>,
        #[pyo3(from_py_with = require_bool)] point: bool,
        width: i64,
        #[pyo3(from_py_with = require_bool)] negfind: bool,
        #[pyo3(from_py_with = require_bool)] list: bool,
    ) -> PyResult<Py<PyAny>> {
        let attached = py.detach(|| self.state().image.is_some());
        if !attached {
            return record_to_py(py, Default::default());
        }
        let region: RegionArgument =
            region.map_or_else(|| Ok(Default::default()), |argument| argument.extract())?;
        let result = py.detach(|| {
            let state = self.state();
            let Some(source) = &state.image else {
                return Ok(None);
            };
            read_locked(source, |image| {
                let ImageHandle::Float(storage) = image else {
                    return Err(raised("This method only supports float-valued images"));
                };
                let coordinates = image.coordinate_record();
                let fit = |source: &dyn ImageInterface<f32>| {
                    fit_image_maximum(
                        source,
                        coordinates,
                        MaxFitOptions {
                            region: region.selection(),
                            point,
                            width,
                            negative: negfind,
                            list,
                        },
                    )
                    .map(Some)
                    .map_err(raised)
                };
                match storage {
                    Storage::Temp {
                        image,
                        fits: Some(fits),
                        ..
                    } => fit(&fits.image_view(image)),
                    _ => with_storage!(storage, image => fit(image.as_ref())),
                }
            })
        })?;
        let Some(result) = result else {
            return record_to_py(py, Default::default());
        };
        let messages: Vec<_> = result.messages.into_iter().map(LogLine::info).collect();
        emit(py, "casars.tools.image", &messages)?;
        record_to_py(py, result.record)
    }
}
