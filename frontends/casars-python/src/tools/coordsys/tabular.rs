// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, TabularCoordinate};
use casa_types::Value;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;

use super::inspect::numbered_coordinates;
use super::{CoordinateState, PyCoordsys, doppler, index_argument};

struct TabularIndex(i32);

impl<'a, 'py> FromPyObject<'a, 'py> for TabularIndex {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        index_argument(&value, "which").map(Self)
    }
}

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn samples(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<f64>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    match doppler::values(value, "settabular") {
        Ok(values) if values == [-1.0] => Ok(Vec::new()),
        Err(error) if error.is_instance_of::<PyTypeError>(value.py()) => Ok(Vec::new()),
        result => result,
    }
}

fn set_tabular(
    state: &mut CoordinateState,
    mut pixel: Vec<f64>,
    mut world: Vec<f64>,
    which: i32,
) -> PyResult<bool> {
    if pixel.is_empty() && world.is_empty() {
        return Ok(false);
    }
    let which = usize::try_from(which)
        .map_err(|_| raised("The specified TabularCoordinate number must be >= 0"))?;
    let (table, field) = numbered_coordinates(state)?
        .into_iter()
        .filter_map(|(_, model, placement)| match model {
            CoordinateModel::Tabular(table) => Some((table, placement.field)),
            _ => None,
        })
        .nth(which)
        .ok_or_else(|| raised("Specified TabularCoordinate could not be found"))?;
    if pixel.is_empty() {
        pixel = table.pixel_values().to_vec();
    } else if world.is_empty() {
        world = table
            .pixel_values()
            .iter()
            .map(|&sample| table.to_world(&[sample]).map(|value| value[0]))
            .collect::<Result<_, _>>()
            .map_err(raised)?;
    }
    if pixel.len() != world.len() {
        return Err(raised("Pixel and world vectors must be the same length"));
    }
    let replacement = TabularCoordinate::from_samples(
        pixel,
        world,
        table.axis_names().remove(0),
        table.axis_units().remove(0),
    )
    .map(CoordinateModel::from)
    .map_err(raised)?;
    let index = state
        .coordinates
        .coordinates_in_record(&state.record)
        .map_err(raised)?
        .iter()
        .position(|placement| placement.field == field)
        .ok_or_else(|| raised("Missing coordinate placement"))?;
    state
        .record
        .upsert(field, Value::Record(replacement.to_record()));
    state.coordinates.replace_coordinate(index, replacement);
    Ok(true)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (pixel=None, world=None, which=TabularIndex(0)), text_signature = "($self, pixel=[-1.0], world=[-1.0], which=0)")]
    fn settabular(
        &self,
        py: Python<'_>,
        pixel: Option<&Bound<'_, PyAny>>,
        world: Option<&Bound<'_, PyAny>>,
        which: TabularIndex,
    ) -> PyResult<bool> {
        let pixel = samples(pixel)?;
        let world = samples(world)?;
        py.detach(|| set_tabular(&mut self.state(), pixel, world, which.0))
    }
}
