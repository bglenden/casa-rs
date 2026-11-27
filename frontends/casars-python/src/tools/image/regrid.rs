// SPDX-License-Identifier: LGPL-3.0-or-later

//! Thin PyO3 `ia.regrid` wrapper delegating to [`casa_imageanalysis::tool::ops_transform::regrid_attached`].

use std::path::Path;

use casa_core::RecordValue;
use casa_imageanalysis::analysis::{ImageRegridOptions, ImageRegridTarget};
use casa_imageanalysis::tool::ops_transform::regrid_attached;
use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{DoubleList, IntList};
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::handle::{external_open_check, raised};
use super::subimage::{Region, mask_argument, require_bool};

#[derive(Default)]
struct GridCoordinates(Option<RecordValue>);

impl<'a, 'py> FromPyObject<'a, 'py> for GridCoordinates {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        let record = value
            .cast::<PyDict>()
            .map_err(|_| PyTypeError::new_err("csys is not a dictionary"))?;
        if record.is_empty() {
            Ok(Self::default())
        } else {
            Ok(Self(Some(casa_record(value.py(), Some(&record))?)))
        }
    }
}

pub(super) fn regrid_indices(value: &Bound<'_, PyAny>) -> PyResult<IntList> {
    if let Ok(integers) = value.extract::<IntList>() {
        return Ok(integers);
    }
    let DoubleList(numbers) = value.extract()?;
    numbers
        .into_iter()
        .map(|number| {
            if number.is_finite() && number >= i64::MIN as f64 && number < -(i64::MIN as f64) {
                Ok(number as i64)
            } else {
                Err(PyOverflowError::new_err(
                    "image shape and axes need finite integer indices",
                ))
            }
        })
        .collect::<PyResult<Vec<_>>>()
        .map(IntList)
}

#[pymethods]
impl PyImageTool {
    /// Resample the selected image onto the requested coordinate grid.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (outfile="", shape=IntList(vec![-1]), csys=GridCoordinates::default(), axes=IntList(vec![-1]), region=None, mask=None, method="linear", decimate=10, replicate=false, doref=true, dropdeg=false, overwrite=false, force=false, asvelocity=false, stretch=false), text_signature = "($self, outfile='', shape=[-1], csys={}, axes=[-1], region={}, mask='', method='linear', decimate=10, replicate=False, doref=True, dropdeg=False, overwrite=False, force=False, asvelocity=False, stretch=False)")]
    fn regrid(
        &self,
        py: Python<'_>,
        outfile: &str,
        #[pyo3(from_py_with = regrid_indices)] shape: IntList,
        csys: GridCoordinates,
        #[pyo3(from_py_with = regrid_indices)] axes: IntList,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        method: &str,
        decimate: i64,
        #[pyo3(from_py_with = require_bool)] replicate: bool,
        #[pyo3(from_py_with = require_bool)] doref: bool,
        #[pyo3(from_py_with = require_bool)] dropdeg: bool,
        #[pyo3(from_py_with = require_bool)] overwrite: bool,
        #[pyo3(from_py_with = require_bool)] force: bool,
        #[pyo3(from_py_with = require_bool)] asvelocity: bool,
        #[pyo3(from_py_with = require_bool)] stretch: bool,
    ) -> PyResult<Py<Self>> {
        let GridCoordinates(target) = csys;
        let region = Region::capture(py, region);
        let mask = mask_argument(mask);
        let attachment = py.detach(|| -> PyResult<_> {
            let state = self.state();
            let input = state
                .image
                .as_ref()
                .ok_or_else(|| raised("Unable to create image"))?;
            let region = region?;
            let mask = mask?;
            let destination = (!outfile.is_empty()).then(|| Path::new(outfile));
            let options = ImageRegridOptions {
                shape: &shape.0,
                target: ImageRegridTarget::Coordinates(target.as_ref()),
                axes: &axes.0,
                region: region.as_ref().map(Region::as_region),
                mask: (!mask.is_empty()).then_some(mask.as_str()),
                method,
                decimate,
                replicate,
                convert_reference: doref,
                dropdeg,
                force,
                as_velocity: asvelocity,
                stretch,
                outfile: destination,
            };
            let history = state.history.then(|| {
                let region = region.as_ref().map_or_else(|| "{}".into(), Region::history);
                let csys = if target.is_some() { "{...}" } else { "{}" };
                (
                    "regrid",
                    format!(
                        "ia.regrid(outfile={outfile:?}, shape={:?}, csys={csys}, axes={:?}, region={region}, mask={mask:?}, method={method:?}, decimate={decimate}, replicate={replicate}, doref={doref}, dropdegen={dropdeg}, overwrite={overwrite}, force={force}, asvelocity={asvelocity}, stretch={stretch})",
                        shape.0, axes.0,
                    ),
                )
            });
            regrid_attached(input, outfile, overwrite, options, history, external_open_check())
                .map_err(raised)
        })?;
        Py::new(py, Self::attached(attachment))
    }
}
