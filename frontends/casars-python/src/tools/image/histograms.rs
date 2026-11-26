// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::RecordValue;
use casa_imageanalysis::analysis::{HistogramOptions, image_histogram};
use pyo3::prelude::*;

use crate::convert::{DoubleList, IntList};

use super::PyImageTool;
use super::attachment::read_locked;
use super::data_compat::record_to_py;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::subimage::{RegionArgument, mask_argument, require_bool};

#[pymethods]
impl PyImageTool {
    /// Bin the accepted pixels together or along selected image axes.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (axes=IntList::of(&[-1]), region=None, mask=None, nbins=25, includepix=DoubleList::of(&[-1.]), cumu=false, log=false, stretch=false), text_signature = "($self, axes=[-1], region={}, mask='', nbins=25, includepix=[-1.0], cumu=False, log=False, stretch=False)")]
    fn histograms<'py>(
        &self,
        py: Python<'py>,
        axes: IntList,
        region: Option<&Bound<'py, PyAny>>,
        mask: Option<&Bound<'py, PyAny>>,
        nbins: i64,
        includepix: DoubleList,
        #[pyo3(from_py_with = require_bool)] cumu: bool,
        #[pyo3(from_py_with = require_bool)] log: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<PyAny>> {
        let real = py.detach(|| {
            self.state().image.as_ref().map(|image| {
                matches!(
                    *image.lock(),
                    ImageHandle::Float(_) | ImageHandle::Double(_)
                )
            })
        });
        match real {
            None => return record_to_py(py, RecordValue::default()),
            Some(false) => return Err(raised("This method only supports real-valued images")),
            Some(true) => {}
        }
        let region =
            region.map_or_else(|| Ok(RegionArgument::default()), |value| value.extract())?;
        let mask = mask_argument(mask)?;
        let values = py.detach(|| {
            let state = self.state();
            let Some(source) = &state.image else {
                return Ok(RecordValue::default());
            };
            let options = HistogramOptions {
                axes: &axes.0,
                region: region.selection(),
                mask: Some(&mask),
                stretch,
                bins: nbins,
                include: &includepix.0,
                cumulative: cumu,
                logarithmic: log,
            };
            read_locked(source, |handle| match handle {
                ImageHandle::Float(storage) => {
                    storage.with_float_view(|image| image_histogram(image, options).map_err(raised))
                }
                ImageHandle::Double(storage) => with_storage!(storage, image =>
                    image_histogram(image.as_ref(), options).map_err(raised)
                ),
                _ => Err(raised("This method only supports real-valued images")),
            })
        })?;
        record_to_py(py, values)
    }
}
