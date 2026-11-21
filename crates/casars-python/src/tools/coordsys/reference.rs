// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::tabular::TabularEdit;
use casa_coordinates::{CoordinateModel, CoordinateSystem};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{
    PyCoordsys,
    inspect::{axis_values, coordinate_type},
};
use crate::convert::{DoubleList, value::CasaValue};

fn axes(coordinates: &CoordinateSystem, kind: &str) -> Option<Vec<usize>> {
    if kind.is_empty() {
        return Some((0..coordinates.n_pixel_axes()).collect());
    }
    let index = coordinates.find_coordinate(coordinate_type(kind))?;
    Some(
        coordinates
            .coordinate_pixel_axes(index)?
            .into_iter()
            .flatten()
            .collect(),
    )
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
            let all = axis_values(&state.coordinates, false, CoordinateModel::reference_pixel);
            let pixels = range.into_iter().map(|axis| all[axis]).collect();
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
            for (axis, value) in range.into_iter().zip(value.0) {
                state.edit_record_axis(&mut record, axis, TabularEdit::ReferencePixel(value))?;
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
