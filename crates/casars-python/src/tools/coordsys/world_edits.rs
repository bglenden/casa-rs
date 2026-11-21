// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::tabular::TabularEdit;
use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_types::quanta::{Quantity, Unit};
use pyo3::exceptions::{PyOverflowError, PyRuntimeError, PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};

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

fn declared_value(value: f64, unit: Option<&str>) -> PyResult<f64> {
    if let Some(unit) = unit {
        let angle = Quantity::new(value, "rad").map_err(raised)?;
        let unit = Unit::new(unit).map_err(raised)?;
        angle.get_value_in(&unit).map_err(raised)
    } else {
        Ok(value)
    }
}

fn location_pixels(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<f64>> {
    let Some(value) = value else {
        return Ok(vec![-1.0]);
    };
    if value.is_instance_of::<PyTuple>() {
        return Err(PySystemError::new_err(
            "<built-in function coordsys_setreferencelocation> returned a result with an exception set",
        ));
    }
    let pixel = |item: &Bound<'_, PyAny>| -> PyResult<f64> {
        if let Ok(integer) = item.extract::<i64>() {
            return Ok(integer as f64);
        }
        let value = if let Ok(text) = item.cast::<PyString>() {
            text.to_str()?
                .parse::<f64>()
                .map_err(|_| PyTypeError::new_err("pixel must contain integer-valued inputs"))?
        } else {
            item.extract::<f64>()?
        };
        if !value.is_finite() || value < i64::MIN as f64 || value >= -(i64::MIN as f64) {
            return Err(PyOverflowError::new_err(
                "pixel is outside the integer range",
            ));
        }
        Ok(value.trunc())
    };
    if !value.is_instance_of::<PyString>()
        && let Ok(items) = value.try_iter()
    {
        items.map(|item| pixel(&item?)).collect()
    } else {
        Ok(vec![pixel(value)?])
    }
}

fn location_mask(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<bool>> {
    let Some(value) = value else {
        return Ok(vec![false]);
    };
    if value.is_instance_of::<PyString>() {
        return Err(PyTypeError::new_err("mask must contain boolean values"));
    }
    if let Ok(items) = value.try_iter() {
        items.map(|item| item?.is_truthy()).collect()
    } else {
        Ok(vec![value.is_truthy()?])
    }
}

fn apply_location(
    state: &mut CoordinateState,
    pixels: Vec<f64>,
    world: PyResult<WorldArgument>,
    mask: Vec<bool>,
) -> PyResult<bool> {
    let coordinates = &state.coordinates;
    let axes = coordinates.n_pixel_axes();
    let use_all = mask == [false];
    if axes != pixels.len() || (!use_all && axes != mask.len()) {
        return Ok(false);
    }
    let world = world?.values(state)?;
    let canonical = coordinates
        .to_pixel(&world)
        .and_then(|pixels| coordinates.to_world(&pixels))
        .map_err(raised)?;
    let mut updated = state.editable_record()?;
    for (axis, pixel) in pixels.into_iter().enumerate() {
        if !(use_all || mask[axis])
            || coordinates.pixel_axis_type(axis) == Some(CoordinateType::Stokes)
        {
            continue;
        }
        state.edit_record_axis(&mut updated, axis, TabularEdit::ReferencePixel(pixel))?;
        if let Some(world_axis) = coordinates.pixel_axis_to_world(axis) {
            let (index, local) = coordinates
                .world_axis(world_axis)
                .expect("mapped world axis");
            let unit = match coordinates.coordinate(index) {
                CoordinateModel::Direction(direction) => {
                    Some(direction.world_units()[local].as_str())
                }
                _ => None,
            };
            let value = declared_value(canonical[world_axis], unit)?;
            state.edit_record_world_axis(
                &mut updated,
                world_axis,
                TabularEdit::ReferenceValue(value),
            )?;
        }
    }
    state.apply_edited_record(updated)?;
    Ok(true)
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
    let defaults = match fallback {
        WorldEdit::Reference => input_coordinates.coordinates.reference_value(),
        WorldEdit::Increment => input_coordinates.coordinates.increment(),
    };
    let values =
        value.values_with_defaults(state, selected.zip(one_coordinate.as_ref()), defaults)?;
    let mut updated = state.editable_record()?;
    let coordinates = &state.coordinates;
    for index in (0..coordinates.n_coordinates())
        .filter(|&index| selected.is_none_or(|selected| selected == index))
    {
        let model = coordinates.coordinate(index);
        let old = edit.declared(model);
        for (local, mapped) in coordinates.world_axes(index).into_iter().enumerate() {
            let Some(axis) = mapped else {
                continue;
            };
            let value = values[if selected.is_some() { local } else { axis }];
            let unit = match model {
                CoordinateModel::Direction(direction) => {
                    Some(direction.world_units()[local].as_str())
                }
                _ => None,
            };
            let value = declared_value(value, unit)?;
            if old[local] == value {
                continue;
            }
            let parameter = match edit {
                WorldEdit::Reference => TabularEdit::ReferenceValue(value),
                WorldEdit::Increment => TabularEdit::Increment(value),
            };
            state.edit_record_world_axis(&mut updated, axis, parameter)?;
        }
    }
    state.apply_edited_record(updated)?;
    Ok(true)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (pixel=None, world=None, mask=None), text_signature = "($self, pixel=[-1], world=[], mask=[False])")]
    fn setreferencelocation(
        &self,
        py: Python<'_>,
        pixel: Option<&Bound<'_, PyAny>>,
        world: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let pixels = location_pixels(pixel)?;
        let mask = location_mask(mask)?;
        let world = WorldArgument::capture(py, world);
        py.detach(|| apply_location(&mut self.state(), pixels, world, mask))
    }

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
