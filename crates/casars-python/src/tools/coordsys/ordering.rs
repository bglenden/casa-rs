// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateSystem;
use numpy::PyUntypedArray;
use pyo3::exceptions::{PyRuntimeError, PySystemError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyString, PyTuple};

use crate::convert::{double_vector_element, numeric_text};

use super::inspect::numbered_coordinates;
use super::{CoordinateState, PyCoordsys};

fn raised(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn is_array(value: &Bound<'_, PyAny>) -> bool {
    value.is_instance_of::<PyList>() || value.cast::<PyUntypedArray>().is_ok()
}

fn integer(value: &Bound<'_, PyAny>) -> PyResult<i32> {
    if let Ok(integer) = value.extract::<i64>() {
        return Ok(integer as i32);
    }
    if let Some(text) = numeric_text(value)? {
        let text = text.to_string_lossy();
        let trimmed = text.trim_start();
        let number = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
        let count = number.bytes().take_while(u8::is_ascii_digit).count();
        let end = trimmed.len() - number.len() + count;
        return Ok(trimmed[..end].parse::<i64>().unwrap_or(0) as i32);
    }
    let number = double_vector_element(value).unwrap_or(0.0);
    Ok(if (i64::MIN as f64..-(i64::MIN as f64)).contains(&number) {
        number as i64 as i32
    } else {
        0
    })
}

pub(super) fn integer_vector(
    value: &Bound<'_, PyAny>,
    method: &str,
    argument: &str,
) -> PyResult<Vec<i32>> {
    if value.is_none() {
        return Ok(Vec::new());
    }
    if value.is_instance_of::<PyTuple>() {
        return Err(PySystemError::new_err(format!(
            "<built-in function coordsys_{method}> returned a result with an exception set"
        )));
    }
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Err(PyTypeError::new_err(format!(
            "argument _{argument} must not be a string"
        )));
    }
    if let Ok(list) = value.cast::<PyList>()
        && list
            .iter()
            .all(|item| !is_array(&item) && !item.is_instance_of::<PyTuple>())
    {
        return list.iter().map(|item| integer(&item)).collect();
    }
    if is_array(value) {
        let array = value
            .py()
            .import("numpy")?
            .call_method1("asarray", (value,))?;
        let flat = array
            .call_method1("ravel", ("F",))?
            .call_method0("tolist")?;
        return flat.try_iter()?.map(|item| integer(&item?)).collect();
    }
    Ok(vec![integer(value)?])
}

fn transpose(state: &mut CoordinateState, order: Vec<i32>) -> PyResult<bool> {
    if order.len() != state.coordinates.n_world_axes() {
        return Err(raised(
            "Failed AlwaysAssert newWorldOrder.nelements() == nWorldAxes()",
        ));
    }
    if order.len() != state.coordinates.n_pixel_axes() {
        return Err(raised(
            "Failed AlwaysAssert newPixelOrder.nelements() == nPixelAxes()",
        ));
    }

    let mut seen = vec![false; order.len()];
    let order = order
        .into_iter()
        .map(|value| {
            let bad = || raised("Failed AlwaysAssert which>=0 && uInt(which)<nw && !found(which)");
            let index = usize::try_from(value).map_err(|_| bad())?;
            let visited = seen.get_mut(index).ok_or_else(bad)?;
            if std::mem::replace(visited, true) {
                return Err(bad());
            }
            Ok(index)
        })
        .collect::<PyResult<Vec<_>>>()?;

    let mut record = state.editable_record()?;
    let mut coordinates = CoordinateSystem::from_mapped_record(&record).map_err(raised)?;
    coordinates.transpose_axes(&order, &order).map_err(raised)?;
    let mapped = coordinates.to_record();
    // CASA transposes the placements and keeps every coordinate in creation order.
    for index in 0..coordinates.n_coordinates() {
        for domain in ["pixelmap", "worldmap"] {
            let field = format!("{domain}{index}");
            let axes = mapped.get(&field).expect("transposed coordinate map");
            record.upsert(field, axes.clone());
        }
    }
    state.apply_edited_coordinates(record, coordinates)?;
    Ok(true)
}

fn reorder(state: &mut CoordinateState, order: Vec<i32>) -> PyResult<bool> {
    let placements = numbered_coordinates(state)?;
    if order.len() != placements.len() {
        return Err(raised(format!(
            "order vector must be of length {}",
            placements.len()
        )));
    }

    let mut coordinates =
        CoordinateSystem::new().with_obs_info(state.coordinates.obs_info().clone());
    let mut stored = Vec::with_capacity(order.len());
    for (output, requested) in order.into_iter().enumerate() {
        let axis = usize::try_from(requested)
            .ok()
            .and_then(|axis| placements.get(axis))
            .ok_or_else(|| raised("Failed AlwaysAssert which < nCoordinates()"))?;
        let (_, model, placement) = axis;
        // CASA adds each full local coordinate to a fresh system, discarding
        // its previous placement. A compact model can omit physical axes.
        let model = if placement.pixel_axes.len() == model.n_pixel_axes()
            && placement.world_axes.len() == model.n_world_axes()
        {
            (*model).clone()
        } else {
            placement.model(&state.record).map_err(raised)?
        };
        let prefix = model.coordinate_type().to_string().to_ascii_lowercase();
        let data = state
            .record
            .get(&placement.field)
            .expect("coordinate record");
        stored.push((format!("{prefix}{output}"), data.clone()));
        coordinates.add_coordinate(model);
    }
    let mut record = coordinates.to_record();
    for (field, data) in stored {
        record.upsert(field, data);
    }
    state.coordinates = coordinates;
    state.record = record;
    Ok(true)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (order), text_signature = "($self, order)")]
    fn transpose(&self, py: Python<'_>, order: &Bound<'_, PyAny>) -> PyResult<bool> {
        let order = integer_vector(order, "transpose", "order")?;
        py.detach(|| transpose(&mut self.state(), order))
    }

    #[pyo3(signature = (order), text_signature = "($self, order)")]
    fn reorder(&self, py: Python<'_>, order: &Bound<'_, PyAny>) -> PyResult<bool> {
        let order = integer_vector(order, "reorder", "order")?;
        py.detach(|| reorder(&mut self.state(), order))
    }
}
