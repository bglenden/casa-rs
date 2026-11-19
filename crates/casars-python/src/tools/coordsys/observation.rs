// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_types::measures::{MPosition, PositionRef, position_to_record};
use casa_types::{ScalarValue, Value};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;

use crate::tools::{LogLine, emit};

use super::PyCoordsys;

fn text(value: &Bound<'_, PyAny>) -> PyResult<String> {
    value
        .extract()
        .map_err(|_| PyTypeError::new_err("argument _value must be a string"))
}

#[pymethods]
impl PyCoordsys {
    fn setobserver(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let observer = text(value)?;
        py.detach(|| {
            let mut state = self.state();
            state.coordinates.obs_info_mut().observer = observer.clone();
            state
                .record
                .upsert("observer", Value::Scalar(ScalarValue::String(observer)));
        });
        Ok(true)
    }

    fn settelescope(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let telescope = text(value)?;
        let found = py.detach(|| {
            let measures = casa_ms::open_measures_runtime()
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let position = MPosition::from_observatory_name(&telescope, measures.as_ref())
                .and_then(|position| {
                    position
                        .map(|position| position.convert_to(PositionRef::ITRF))
                        .transpose()
                })
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            let mut state = self.state();
            state.coordinates.obs_info_mut().telescope = telescope.clone();
            state
                .record
                .upsert("telescope", Value::Scalar(ScalarValue::String(telescope)));
            if let Some(position) = position {
                state
                    .record
                    .upsert("telescopeposition", position_to_record(&position).into());
                state.coordinates.obs_info_mut().telescope_position = Some(position);
                Ok::<_, PyErr>(true)
            } else {
                Ok(false)
            }
        })?;
        if !found {
            emit(
                py,
                "casars.tools.coordsys",
                &[LogLine::warning(
                    "This telescope and its position is not known to the casapy system.",
                )],
            )?;
        }
        Ok(found)
    }
}
