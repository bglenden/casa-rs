// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateSystem, CoordinateType, RecordCoordinate};
use casa_types::{ArrayValue, Value};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::construction::{stokes_values, string_arguments};
use super::inspect::{coordinate_type, numbered_coordinates};
use super::{CoordinateState, PyCoordsys};

fn names(mut names: Vec<String>) -> Vec<String> {
    if matches!(names.as_slice(), [name] if !name.is_empty()) {
        let name = names.pop().expect("a singleton vector");
        return name
            .split(if name.contains(',') { ',' } else { ' ' })
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
    }
    names
}

fn coordinate(state: &CoordinateState, kind: CoordinateType) -> PyResult<RecordCoordinate> {
    numbered_coordinates(state)?
        .into_iter()
        .find(|(_, coordinate, _)| coordinate.coordinate_type() == kind)
        .map(|(_, _, placement)| placement)
        .ok_or_else(|| {
            PyRuntimeError::new_err(format!(
                "No coordinate of type {kind} in this CoordinateSystem"
            ))
        })
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature=(value, r#type=""), text_signature="($self, value, type='')")]
    fn setnames(&self, py: Python<'_>, value: &Bound<'_, PyAny>, r#type: &str) -> PyResult<bool> {
        let names = names(string_arguments(Some(value), "value")?);
        py.detach(|| {
            let mut state = self.state();
            let axes = if r#type.is_empty() {
                if names.len() != state.coordinates.n_world_axes() {
                    return Err(PyRuntimeError::new_err(
                        "names vector must be of length nWorldAxes()",
                    ));
                }
                // The native reader requires matching visible world and
                // pixel maps, even when whole coordinates are transposed.
                (0..names.len()).map(Some).collect()
            } else {
                let target = coordinate(&state, coordinate_type(r#type))?;
                if names.len() != target.pixel_axes.len() {
                    return Err(PyRuntimeError::new_err(format!(
                        "Supplied axis names vector must be of length {}",
                        target.pixel_axes.len()
                    )));
                }
                target.pixel_axes
            };
            let mut record = state.record.clone();
            let mut updated = state.coordinates.clone();
            for (axis, name) in axes.into_iter().zip(names) {
                let axis = axis.ok_or_else(|| {
                    PyRuntimeError::new_err("Cannot set the name of a removed coordinate axis")
                })?;
                (updated, _) = CoordinateSystem::set_axis_name_in_record(&mut record, axis, &name)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            }
            state.record = record;
            state.coordinates = updated;
            Ok(true)
        })
    }

    fn setstokes(&self, py: Python<'_>, stokes: &Bound<'_, PyAny>) -> PyResult<bool> {
        let names = names(string_arguments(Some(stokes), "stokes")?);
        py.detach(|| {
            let mut state = self.state();
            let target = coordinate(&state, CoordinateType::Stokes)?;
            let stokes = stokes_values(names.iter().map(String::as_str))?;
            let first = stokes.first().ok_or_else(|| {
                PyRuntimeError::new_err("You did not specify any new Stokes values")
            })?;
            let mut record = state.record.clone();
            let Some(Value::Record(target)) = record.get_mut(&target.field) else {
                return Err(PyRuntimeError::new_err(
                    "Stokes coordinate is missing from the record",
                ));
            };
            target.upsert(
                "crval",
                ArrayValue::from_f64_vec(vec![f64::from(first.code())]).into(),
            );
            target.upsert(
                "stokes",
                ArrayValue::from_string_vec(stokes.iter().map(|code| code.name().into()).collect())
                    .into(),
            );
            state.coordinates = CoordinateSystem::from_record(&record)
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            state.record = record;
            Ok(true)
        })
    }
}
