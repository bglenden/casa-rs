// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateSystem;
use casa_types::quanta::{Unit, unit::normalize_casa_unit_name};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::IntList;
use crate::tools::coordsys::PyCoordsys;
use crate::tools::table::casa_record;

use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::{PyImageTool, stub};

#[pymethods]
impl PyImageTool {
    fn brightnessunit(&self, py: Python<'_>) -> String {
        py.detach(|| {
            self.state().image.as_ref().map_or_else(String::new, |image| {
                with_pixels!(&*image.lock(), storage => with_storage!(storage, image => image.units().into()))
            })
        })
    }

    fn setbrightnessunit(&self, py: Python<'_>, unit: String) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let history = state.history;
            let Some(image) = state.image.as_ref() else {
                return Ok(false);
            };
            let mut image = image.lock();
            image.check_single_precision("setbrightnessunit")?;
            let normalized = normalize_casa_unit_name(unit.trim());
            Unit::new(&normalized)
                .map_err(|_| raised(format!("Unit::check Illegal unit string '{unit}'")))?;
            with_pixels!(&mut *image, storage => with_storage!(storage, image => image.set_units(normalized)))
                .map_err(raised)?;
            if history {
                image.record_history(
                    "setbrightnessunit",
                    format!("ia.setbrightnessunit(unit={unit:?})"),
                )?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        })
    }

    #[pyo3(signature = (axes=None), text_signature = "($self, axes=[-1])")]
    fn coordsys(&self, py: Python<'_>, axes: Option<IntList>) -> PyResult<Py<PyAny>> {
        let tool = py.detach(|| {
            let state = self.state();
            let Some(image) = state.image.as_ref() else {
                return Ok(PyCoordsys::empty());
            };
            let image = image.lock();
            let axes = axes.as_ref().map_or(&[][..], |arg| arg.0.as_slice());
            let all_axes = axes.is_empty()
                || axes == [-1]
                || axes.iter().copied().eq(0..image.shape().len() as i64);
            let record = image.coordinate_record();
            if all_axes {
                return PyCoordsys::from_record(record);
            }
            let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
            let mut result = CoordinateSystem::new().with_obs_info(coordinates.obs_info().clone());
            let mut first = 0;
            let mut selected = Vec::new();
            for index in 0..coordinates.n_coordinates() {
                let coordinate = coordinates.coordinate(index);
                let end = first + coordinate.n_pixel_axes() as i64;
                if (first..end).all(|axis| axes.contains(&axis)) {
                    result.add_coordinate(coordinate.clone());
                    selected.extend(first..end);
                }
                first = end;
            }
            if selected != axes {
                return Err(stub("coordsys").unwrap_err());
            }
            PyCoordsys::from_record(result.to_record())
        })?;
        Ok(Py::new(py, tool)?.into_any())
    }

    fn setcoordsys(&self, py: Python<'_>, csys: &Bound<'_, PyDict>) -> PyResult<bool> {
        let record = casa_record(py, Some(csys))?;
        py.detach(|| {
            let state = self.state();
            let history = state.history;
            let Some(image) = state.image.as_ref() else {
                return Ok(false);
            };
            let mut image = image.lock();
            let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
            if coordinates.n_pixel_axes() != image.shape().len() {
                return Err(raised(
                    "Exception: Supplied CoordinateSystem and image shape are inconsistent",
                ));
            }
            image.set_coordinate_record(record)?;
            if history {
                image.record_history("setcoordsys", "ia.setcoordsys(csys={...})".into())?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        })
    }

    #[pyo3(signature = (list=true), text_signature = "($self, list=True)")]
    fn history(&self, py: Python<'_>, list: bool) -> PyResult<Vec<String>> {
        let entries = py.detach(|| {
            let state = self.state();
            let Some(image) = state.image.as_ref() else {
                return Ok(Vec::new());
            };
            with_pixels!(&*image.lock(), storage => with_storage!(storage, image => image.history()))
                .map_err(raised)
        })?;
        if list {
            crate::tools::emit(
                py,
                "casars.tools.image",
                &entries
                    .iter()
                    .cloned()
                    .map(crate::tools::LogLine::info)
                    .collect::<Vec<_>>(),
            )?;
        }
        Ok(entries)
    }
}
