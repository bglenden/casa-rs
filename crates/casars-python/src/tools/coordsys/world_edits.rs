// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_types::quanta::{Quantity, Unit};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::inspect::coordinate_type;
use super::transform_single::WorldArgument;
use super::{CoordinateState, PyCoordsys};

#[derive(Clone, Copy)]
enum WorldEdit {
    Reference,
    Increment,
}

impl WorldEdit {
    fn native(self, coordinate: &CoordinateModel) -> Vec<f64> {
        match self {
            Self::Reference => coordinate.reference_value(),
            Self::Increment => coordinate.increment(),
        }
    }

    fn declared(self, coordinate: &CoordinateModel) -> Vec<f64> {
        if let CoordinateModel::Direction(direction) = coordinate {
            let (reference, increment) = direction.reference_and_increment_in_world_units();
            match self {
                Self::Reference => reference,
                Self::Increment => increment,
            }
        } else {
            self.native(coordinate)
        }
    }
}

fn raised(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn coordinates(system: &CoordinateSystem) -> impl Iterator<Item = &CoordinateModel> {
    (0..system.n_coordinates()).map(|index| system.coordinate(index))
}

fn declared_value(value: f64, unit: Option<&str>) -> PyResult<f64> {
    if let Some(unit) = unit {
        let angle = Quantity::new(value, "rad").map_err(raised)?;
        let unit = Unit::new(unit).map_err(raised)?;
        angle.get_value_in(&unit).map_err(raised)
    } else {
        Ok(value)
    }
}

fn apply_edit(
    state: &mut CoordinateState,
    value: WorldArgument,
    kind: &str,
    edit: WorldEdit,
) -> PyResult<bool> {
    let selected = if kind.is_empty() {
        None
    } else {
        Some(
            state
                .coordinates
                .find_coordinate(coordinate_type(kind))
                .ok_or_else(|| raised(format!("A coordinate of type {kind} does not exist")))?,
        )
    };
    let one_coordinate = selected.map(|index| {
        let mut system =
            CoordinateSystem::new().with_obs_info(state.coordinates.obs_info().clone());
        system.add_coordinate(state.coordinates.coordinate(index).clone());
        CoordinateState::new(system)
    });
    let input_coordinates = one_coordinate.as_ref().unwrap_or(state);
    // An explicit coordinate type fills missing values from that coordinate's reference.
    let fallback = if selected.is_some() {
        WorldEdit::Reference
    } else {
        edit
    };
    let defaults = coordinates(&input_coordinates.coordinates)
        .flat_map(|coordinate| fallback.native(coordinate))
        .collect();
    let mut values = value
        .values_with_defaults(state, selected.zip(one_coordinate.as_ref()), defaults)?
        .into_iter();
    let mut updated = state.record.clone();
    for (index, placement) in CoordinateSystem::record_coordinates(&state.record)
        .map_err(raised)?
        .into_iter()
        .enumerate()
        .filter(|(index, _)| selected.is_none_or(|selected| selected == *index))
    {
        let model = state.coordinates.coordinate(index);
        let angle_units = match model {
            CoordinateModel::Direction(direction) => direction.visible_world_units(),
            _ => Vec::new(),
        };
        let old_values = edit.declared(model);
        for (local, axis) in placement.pixel_axes.into_iter().flatten().enumerate() {
            let value = values
                .next()
                .ok_or_else(|| raised("World and pixel axes do not align"))?;
            let value = declared_value(value, angle_units.get(local).map(String::as_str))?;
            if old_values.get(local) == Some(&value) {
                continue;
            }
            match edit {
                WorldEdit::Reference => {
                    CoordinateSystem::set_reference_value_in_record(&mut updated, axis, value)
                }
                WorldEdit::Increment => {
                    CoordinateSystem::set_increment_in_record(&mut updated, axis, value)
                }
            }
            .map_err(raised)?;
        }
    }
    if values.next().is_some() {
        return Err(raised("World and pixel axes do not align"));
    }
    let mut edited = CoordinateState::from_record(updated)?;
    edited.parent_name = std::mem::take(&mut state.parent_name);
    *state = edited;
    Ok(true)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (value=None, r#type=""), text_signature = "($self, value=[], type='')")]
    fn setincrement(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        r#type: &str,
    ) -> PyResult<bool> {
        let value = WorldArgument::capture(py, value)?;
        py.detach(|| apply_edit(&mut self.state(), value, r#type, WorldEdit::Increment))
    }

    #[pyo3(signature = (value=None, r#type=""), text_signature = "($self, value=[], type='')")]
    fn setreferencevalue(
        &self,
        py: Python<'_>,
        value: Option<&Bound<'_, PyAny>>,
        r#type: &str,
    ) -> PyResult<bool> {
        let value = WorldArgument::capture(py, value)?;
        py.detach(|| apply_edit(&mut self.state(), value, r#type, WorldEdit::Reference))
    }
}
