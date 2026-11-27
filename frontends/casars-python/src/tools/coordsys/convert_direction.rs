// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateModel;
use casa_core::Value;
use casa_core::measures::direction::DirectionRef;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::holder::DIRECTION_CODES;
use crate::convert::value::CasaValue;

use super::conversion_frames::FrameName;
use super::inspect::numbered_coordinates;
use super::replace::replace_at;
use super::{CoordinateState, PyCoordsys};

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn convert_direction(state: &mut CoordinateState, frame: &str) -> PyResult<f64> {
    let (index, _, placement) = numbered_coordinates(state)?
        .into_iter()
        .find(|(_, model, _)| matches!(model, CoordinateModel::Direction(_)))
        .ok_or_else(|| raised("The coordinate system does not have a direction coordinate."))?;
    let reference = DIRECTION_CODES
        .resolve(frame)
        .and_then(|name| name.parse::<DirectionRef>().ok())
        .ok_or_else(|| raised(format!("Unknown frame specifier {frame}")))?;
    let CoordinateModel::Direction(direction) = placement.model(&state.record).map_err(raised)?
    else {
        return Err(raised("Invalid direction coordinate"));
    };
    let (converted, angle) = direction
        .convert_reference_frame(reference)
        .map_err(raised)?;
    let model = CoordinateModel::from(converted);
    let record = Value::Record(model.to_record());
    replace_at(state, index as i32, model, record)?;
    Ok(angle)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (frame), text_signature = "($self, frame)")]
    fn convertdirection(&self, py: Python<'_>, frame: FrameName) -> PyResult<Py<PyAny>> {
        let frame = frame.into_name("frame")?;
        let angle = py.detach(|| convert_direction(&mut self.state(), &frame))?;
        CasaValue::record([
            ("unit", CasaValue::Text("rad".into())),
            ("value", CasaValue::Float(angle)),
        ])
        .into_python(py)
    }
}
