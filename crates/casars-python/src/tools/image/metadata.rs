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
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            let mut image = attachment.lock();
            image.check_single_precision("setbrightnessunit")?;
            let normalized = normalize_casa_unit_name(unit.trim());
            Unit::new(&normalized)
                .map_err(|_| raised(format!("Unit::check Illegal unit string '{unit}'")))?;
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
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
            let Some(attachment) = state.image.as_ref() else {
                return Ok(PyCoordsys::empty());
            };
            let image = attachment.lock();
            let axes = axes.as_ref().map_or(&[][..], |arg| arg.0.as_slice());
            let all_axes = axes.is_empty()
                || axes == [-1]
                || axes.iter().copied().eq(0..image.shape().len() as i64);
            if all_axes {
                return attachment.coordsys(&image);
            }
            let (coordinates, _) = attachment.coordinate_view(&image);
            let mut result = CoordinateSystem::new().with_obs_info(coordinates.obs_info().clone());
            let mut selected = Vec::new();
            for index in 0..coordinates.n_coordinates() {
                let pixel = coordinates
                    .coordinate_pixel_axes(index)
                    .expect("existing coordinate");
                if pixel
                    == coordinates
                        .coordinate_world_axes(index)
                        .expect("existing coordinate")
                    && pixel
                        .iter()
                        .all(|axis| axis.is_some_and(|axis| axes.contains(&(axis as i64))))
                {
                    result.add_coordinate(coordinates.coordinate(index).clone());
                    selected.extend(pixel.into_iter().flatten().map(|axis| axis as i64));
                }
            }
            if selected.len() != axes.len() {
                return Err(stub("coordsys").unwrap_err());
            }
            let order: Option<Vec<_>> = axes
                .iter()
                .map(|axis| selected.iter().position(|candidate| candidate == axis))
                .collect();
            let order = order.ok_or_else(|| stub("coordsys").unwrap_err())?;
            result.transpose_axes(&order, &order).map_err(raised)?;
            PyCoordsys::from_image_coordinates(&result, result.to_record())
        })?;
        Ok(Py::new(py, tool)?.into_any())
    }

    fn setcoordsys(&self, py: Python<'_>, csys: &Bound<'_, PyDict>) -> PyResult<bool> {
        let record = casa_record(py, Some(csys))?;
        py.detach(|| {
            let state = self.state();
            let history = state.history;
            let Some(attachment) = state.image.as_ref() else {
                return Ok(false);
            };
            let mut image = attachment.lock();
            if record.is_empty() {
                return Err(raised("Exception: Record is empty."));
            }
            let coordinates = CoordinateSystem::from_record(&record).map_err(raised)?;
            if coordinates.n_pixel_axes() != image.shape().len() {
                return Err(raised(
                    "Exception: Supplied CoordinateSystem and image shape are inconsistent",
                ));
            }
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            image.set_coordinate_record(record)?;
            attachment.coordinates_changed();
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
