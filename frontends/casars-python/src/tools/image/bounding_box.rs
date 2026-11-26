// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imageanalysis::analysis::ImageRegionBounds;
use pyo3::prelude::*;

use crate::convert::value::CasaValue;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::Region;

fn into_record(bounds: ImageRegionBounds) -> CasaValue {
    let ints = |axes: Vec<usize>| CasaValue::Ints {
        shape: vec![axes.len()],
        values: axes.into_iter().map(|axis| axis as i64).collect(),
    };
    let enclosing_shape = bounds
        .lower
        .iter()
        .zip(&bounds.upper)
        .map(|(&first, &last)| last - first + 1)
        .collect();
    CasaValue::record([
        ("bbShape", ints(enclosing_shape)),
        ("blc", ints(bounds.lower)),
        ("blcf", CasaValue::Text(bounds.lower_world)),
        ("imageShape", ints(bounds.image_shape)),
        ("inc", ints(bounds.stride)),
        ("regionShape", ints(bounds.sampled_shape)),
        ("trc", ints(bounds.upper)),
        ("trcf", CasaValue::Text(bounds.upper_world)),
    ])
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (region=None), text_signature = "($self, region={})")]
    fn boundingbox(
        &self,
        py: Python<'_>,
        region: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let selection = Region::capture(py, region);
        let result = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let image = attachment.lock();
            let unsupported_precision = match &*image {
                ImageHandle::Double(_) => Some("double"),
                ImageHandle::DComplex(_) => Some("complex<double>"),
                _ => None,
            };
            if let Some(precision) = unsupported_precision {
                return Err(raised(format!(
                    "boundingbox does not support images with {precision} precision pixel values"
                )));
            }
            let selection = selection?;
            let region = selection.as_ref().map(Region::as_region);
            let bounds = match &*image {
                ImageHandle::Float(Storage::Temp {
                    image,
                    fits: Some(fits),
                    ..
                }) => ImageRegionBounds::resolve(&fits.image_view(image), region),
                handle => with_pixels!(handle, storage => with_storage!(
                    storage, image => ImageRegionBounds::resolve(image.as_ref(), region)
                )),
            };
            bounds.map(into_record).map_err(raised)
        })?;
        result.into_python(py)
    }
}
