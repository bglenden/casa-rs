// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateSystem, CoordinateType, RecordCoordinate};
use casa_types::{ArrayValue, ScalarValue, Value};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::construction::{stokes_values, string_arguments};
use super::inspect::{
    axis_values, coordinate_type, numbered_coordinates, units as displayed_units,
};
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
                (0..names.len()).map(Some).collect()
            } else {
                let target = coordinate(&state, coordinate_type(r#type))?;
                if names.len() != target.world_axes.len() {
                    return Err(PyRuntimeError::new_err(format!(
                        "Supplied axis names vector must be of length {}",
                        target.world_axes.len()
                    )));
                }
                target.world_axes
            };
            let mut record = state.editable_record()?;
            for (axis, name) in axes.into_iter().zip(names) {
                let axis = axis.ok_or_else(|| {
                    PyRuntimeError::new_err("Cannot set the name of a removed coordinate axis")
                })?;
                CoordinateSystem::set_world_axis_name_in_record(&mut record, axis, &name)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            }
            state.apply_edited_record(record)?;
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
            let mut record = state.editable_record()?;
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
            state.apply_edited_record(record)?;
            Ok(true)
        })
    }

    #[pyo3(signature = (value, r#type="", overwrite=false, which=-10), text_signature = "($self, value, type='', overwrite=False, which=-10)")]
    fn setunits(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        r#type: &str,
        overwrite: bool,
        which: i64,
    ) -> PyResult<bool> {
        let units = names(string_arguments(Some(value), "value")?);
        py.detach(|| {
            let mut state = self.state();
            let mut record = state.editable_record()?;
            let axes = if r#type.is_empty() {
                if units.len() != state.coordinates.n_world_axes() {
                    return Err(PyRuntimeError::new_err(
                        "units vector must be of length nWorldAxes()",
                    ));
                }
                (0..units.len()).map(Some).collect()
            } else {
                let kind = coordinate_type(r#type);
                let (_, model, coordinate) = numbered_coordinates(&state)?
                    .into_iter()
                    .find(|(index, model, _)| {
                        if which < 0 {
                            model.coordinate_type() == kind
                        } else {
                            *index as i64 == which
                        }
                    })
                    .ok_or_else(|| {
                        PyRuntimeError::new_err(format!(
                            "No requested {kind} coordinate in this CoordinateSystem"
                        ))
                    })?;
                if units.len() != coordinate.world_axes.len() {
                    return Err(PyRuntimeError::new_err(format!(
                        "Supplied axis units vector must be of length {}",
                        coordinate.world_axes.len()
                    )));
                }
                if overwrite && matches!(kind, CoordinateType::Linear | CoordinateType::Tabular) {
                    if model.coordinate_type() != kind {
                        return Err(PyRuntimeError::new_err(format!(
                            "Coordinate {which} is not of type {kind}"
                        )));
                    }
                    let Some(Value::Record(target)) = record.get_mut(&coordinate.field) else {
                        return Err(PyRuntimeError::new_err(
                            "Coordinate is missing from the record",
                        ));
                    };
                    if kind == CoordinateType::Tabular && target.get("unit").is_some() {
                        target.upsert("unit", Value::Scalar(ScalarValue::String(units[0].clone())));
                    }
                    target.upsert(
                        "units",
                        Value::Array(ArrayValue::from_string_vec(units.clone())),
                    );
                    Vec::new()
                } else {
                    coordinate.world_axes
                }
            };
            let existing = axis_values(&state.coordinates, true, displayed_units);
            for (axis, unit) in axes.into_iter().zip(units) {
                let axis = axis.ok_or_else(|| {
                    PyRuntimeError::new_err("Cannot set the unit of a removed coordinate axis")
                })?;
                if existing.get(axis) == Some(&unit) {
                    continue;
                }
                CoordinateSystem::set_world_axis_unit_in_record(&mut record, axis, &unit)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            }
            state.apply_edited_record(record)?;
            Ok(true)
        })
    }
}
