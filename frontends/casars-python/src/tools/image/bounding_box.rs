// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.boundingbox` wrapper delegating to [`casa_imageanalysis::tool::ops_fit::bounding_box`].

use casa_imageanalysis::analysis::ImageRegionBounds;
use casa_imageanalysis::tool::ops_fit::bounding_box;
use pyo3::prelude::*;

use crate::convert::value::CasaValue;

use super::PyImageTool;
use super::handle::raised;
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
        let result = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else {
                return Ok(CasaValue::empty_record());
            };
            let selection = selection?;
            let bounds = bounding_box(attachment, selection.as_ref().map(Region::as_region))
                .map_err(raised)?;
            Ok(into_record(bounds))
        })?;
        result.into_python(py)
    }
}
