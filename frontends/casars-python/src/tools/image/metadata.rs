// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_core::quanta::{Unit, unit::normalize_casa_unit_name};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::IntList;
use crate::tools::coordsys::{PyCoordsys, validate_restored_linear_coordinates};
use crate::tools::table::casa_record;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};

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
            if image.image_type_name() == "ImageConcat" {
                attachment.check_bound_name(&image)?;
            } else {
                attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            }
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
            if axes.is_empty() || axes == [-1] {
                return attachment.coordsys(&image);
            }
            let (coordinates, record) = attachment.coordinate_view(&image);
            let placements = coordinates.coordinates_in_record(&record).map_err(raised)?;
            let mut result = CoordinateSystem::new().with_obs_info(coordinates.obs_info().clone());
            let mut offsets = vec![None; placements.len()];
            let mut keep = Vec::new();
            for &requested in axes {
                let axis = usize::try_from(requested)
                    .ok()
                    .filter(|&axis| axis < coordinates.n_pixel_axes())
                    .ok_or_else(|| raised(format!(
                        "Specified zero-based pixel axis {requested} is not a valid pixel axis"
                    )))?;
                let (index, _) = coordinates.pixel_axis_owner(axis).ok_or_else(|| {
                    raised(format!("Zero-based pixel axis {requested} has been removed"))
                })?;
                let placement = &placements[index];
                let world = coordinates.pixel_axis_to_world_axis(axis)
                    .and_then(|world| placement.world_axes.iter().position(|axis| *axis == Some(world)))
                    .ok_or_else(|| raised(format!(
                        "World axis corresponding to zero-based pixel axis {requested} has been removed"
                    )))?;
                let base = match offsets[index] {
                    Some(base) => base,
                    None => {
                        let base = result.n_world_axes();
                        // Sampled one-axis models retain their live reference
                        // until the image is reopened. Restoring their saved
                        // records here would rebase the selected coordinate.
                        let model = coordinates.coordinate(index);
                        let model = match model {
                            CoordinateModel::Spectral(spectral) if !spectral.has_linear_mapping() => model.clone(),
                            CoordinateModel::Tabular(_) => model.clone(),
                            _ => placement.model(&record).map_err(raised)?,
                        };
                        result.add_coordinate(model);
                        offsets[index] = Some(base);
                        base
                    }
                };
                keep.push(base + world);
            }
            for (axis, replacement) in result.reference_value().into_iter().enumerate().rev() {
                if !keep.contains(&axis) {
                    result.remove_world_axis(axis, replacement).map_err(raised)?;
                }
            }
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
            validate_restored_linear_coordinates(&coordinates, &record)?;
            if coordinates.n_pixel_axes() != image.shape().len() {
                return Err(raised(
                    "Exception: Supplied CoordinateSystem and image shape are inconsistent",
                ));
            }
            if image.image_type_name() == "ImageConcat" {
                attachment.check_bound_name(&image)?;
            } else {
                attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            }
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
