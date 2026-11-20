// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateSystem;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{PyCoordsys, inspect::coordinate_type};
use crate::convert::{DoubleList, value::CasaValue};

fn axes(coordinates: &CoordinateSystem, kind: &str) -> Option<std::ops::Range<usize>> {
    if kind.is_empty() {
        return Some(0..coordinates.n_pixel_axes());
    }
    let kind = coordinate_type(kind);
    let mut first = 0;
    for index in 0..coordinates.n_coordinates() {
        let coordinate = coordinates.coordinate(index);
        let end = first + coordinate.n_pixel_axes();
        if coordinate.coordinate_type() == kind {
            return Some(first..end);
        }
        first = end;
    }
    None
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (r#type = ""), text_signature = "($self, type='')")]
    fn referencepixel(&self, py: Python<'_>, r#type: &str) -> PyResult<Py<PyAny>> {
        let value = py.detach(|| {
            let state = self.state();
            let Some(range) = axes(&state.coordinates, r#type) else {
                return CasaValue::empty_record();
            };
            let pixels = (0..state.coordinates.n_coordinates())
                .flat_map(|index| state.coordinates.coordinate(index).reference_pixel())
                .skip(range.start)
                .take(range.len())
                .collect();
            CasaValue::record([
                ("numeric", CasaValue::floats(pixels)),
                ("pw_type", CasaValue::Text("pixel".into())),
                ("ar_type", CasaValue::Text("absolute".into())),
            ])
        });
        value.into_python(py)
    }

    #[pyo3(signature = (value, r#type = ""), text_signature = "($self, value, type='')")]
    fn setreferencepixel(&self, py: Python<'_>, value: DoubleList, r#type: &str) -> PyResult<bool> {
        py.detach(|| {
            let mut state = self.state();
            let range = axes(&state.coordinates, r#type).ok_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "A coordinate of type {type_name} does not exist",
                    type_name = r#type
                ))
            })?;
            let mut record = state.editable_record()?;
            for (axis, value) in range.zip(value.0) {
                CoordinateSystem::set_reference_pixel_in_record(&mut record, axis, value)
                    .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            }
            state.apply_edited_record(record)?;
            Ok(true)
        })
    }

    fn torecord(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            crate::tools::table::values::record(&self.state().record, std::path::Path::new("."))
        })
        .into_python(py)
    }

    fn done(&self, py: Python<'_>) -> bool {
        py.detach(|| {
            let mut state = self.lock_state();
            *state = super::CoordinateState::empty();
            state.parent_name.clear();
        });
        true
    }

    fn r#type(&self) -> &'static str {
        "coordsys"
    }
}
