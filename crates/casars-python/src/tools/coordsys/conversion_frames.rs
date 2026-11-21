// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{MPosition, MeasFrame};
use casa_types::{ScalarValue, Value};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::convert::holder::{DIRECTION_CODES, FREQUENCY_CODES};

use super::frame_metadata::coordinate;
use super::{CoordinateState, PyCoordsys};

struct FrameName(Option<String>);

impl Default for FrameName {
    fn default() -> Self {
        Self(Some(String::new()))
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for FrameName {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(bytes) = value.cast::<PyBytes>() {
            return Ok(Self(Some(
                String::from_utf8_lossy(bytes.as_bytes()).into_owned(),
            )));
        }
        Ok(Self(value.extract::<String>().ok()))
    }
}

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn apply_conversion(
    state: &mut CoordinateState,
    field: &str,
    model: CoordinateModel,
    key: &str,
    value: Value,
) -> PyResult<()> {
    let index = state
        .coordinates
        .coordinates_in_record(&state.record)
        .map_err(raised)?
        .iter()
        .position(|placement| placement.field == field)
        .ok_or_else(|| raised("Missing coordinate placement"))?;
    let Some(Value::Record(stored)) = state.record.get_mut(field) else {
        return Err(raised("Missing coordinate record"));
    };
    stored.upsert(key, value);
    state.coordinates.replace_coordinate(index, model);
    Ok(())
}

fn set_direction(state: &mut CoordinateState, name: &str) -> PyResult<()> {
    let Some((CoordinateModel::Direction(model), placement)) =
        coordinate(state, CoordinateType::Direction)?
    else {
        return Ok(());
    };
    let frame = DIRECTION_CODES.resolve(name)
        .and_then(|canonical| canonical.parse::<DirectionRef>().ok())
        .ok_or_else(|| raised("Failed to set the new DirectionCoordinate reference frame because Invalid direction reference system"))?;
    let changed = model.clone().with_conversion_direction_ref(frame);
    apply_conversion(
        state,
        &placement.field,
        changed.into(),
        "conversionSystem",
        Value::Scalar(ScalarValue::String(frame.as_str().into())),
    )
}

fn spectral_error(message: impl std::fmt::Display) -> PyErr {
    raised(format!(
        "Failed to set the new SpectralCoordinate reference frame because {message}"
    ))
}

fn set_spectral(state: &mut CoordinateState, name: &str) -> PyResult<()> {
    let Some((CoordinateModel::Spectral(model), placement)) =
        coordinate(state, CoordinateType::Spectral)?
    else {
        return Ok(());
    };
    let Some((CoordinateModel::Direction(direction), _)) =
        coordinate(state, CoordinateType::Direction)?
    else {
        return Err(spectral_error(
            "No DirectionCoordinate; cannot set Spectral conversion layer",
        ));
    };
    let frame = FREQUENCY_CODES
        .resolve(name)
        .and_then(|canonical| canonical.parse::<FrequencyRef>().ok())
        .ok_or_else(|| spectral_error(format!("invalid frequency system {name}")))?;
    let native = if model.has_undefined_frame() {
        FrequencyRef::Undefined
    } else {
        model.frequency_ref()
    };
    if model.conversion_frequency_ref().unwrap_or(native) == frame {
        return Ok(());
    }

    let reference = direction.reference_pixel();
    let pixel = [reference[0], reference[1]];
    let [longitude, latitude] = direction.pixel_to_world(pixel).map_err(raised)?;
    let direction =
        MDirection::from_angles(longitude, latitude, direction.conversion_direction_ref());
    let info = state.coordinates.obs_info();
    let measures = casa_ms::open_measures_runtime().map_err(raised)?;
    let position =
        MPosition::casa_observatory(&info.telescope, measures.as_ref()).ok_or_else(|| {
            spectral_error("Cannot find observatory; cannot set Spectral conversion layer")
        })?;
    let epoch = info
        .date
        .as_ref()
        .filter(|epoch| epoch.value().as_mjd() > 0.0)
        .ok_or_else(|| spectral_error("Epoch not valid; cannot set Spectral conversion layer"))?;
    if matches!(native, FrequencyRef::Undefined) || matches!(frame, FrequencyRef::Undefined) {
        return Err(raised(
            "Transformations to/from frame \"Undefined\" are not possible.",
        ));
    }
    // CASA accepts this request without changing the conversion when no
    // source radial velocity can be supplied by the coordinate tool.
    if matches!(native, FrequencyRef::REST) || matches!(frame, FrequencyRef::REST) {
        return Ok(());
    }
    let context = MeasFrame::new()
        .with_direction(direction)
        .with_position(position)
        .with_epoch(epoch.clone())
        .with_measures(measures);
    let changed = CoordinateModel::from(model.clone().with_conversion(frame, context));
    let record = changed.to_record();
    let conversion = record
        .get("conversion")
        .ok_or_else(|| raised("Missing spectral conversion record"))?;
    apply_conversion(
        state,
        &placement.field,
        changed,
        "conversion",
        conversion.clone(),
    )
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (direction=FrameName::default(), spectral=FrameName::default()), text_signature = "($self, direction='', spectral='')")]
    fn setconversiontype(
        &self,
        py: Python<'_>,
        direction: FrameName,
        spectral: FrameName,
    ) -> PyResult<bool> {
        let direction = direction
            .0
            .ok_or_else(|| PyTypeError::new_err("argument _direction must be a string"))?;
        let spectral = spectral
            .0
            .ok_or_else(|| PyTypeError::new_err("argument _spectral must be a string"))?;
        py.detach(|| {
            let mut state = self.state();
            let mut updated = state.clone();
            if !direction.is_empty() {
                set_direction(&mut updated, &direction)?;
            }
            if !spectral.is_empty() {
                set_spectral(&mut updated, &spectral)?;
            }
            *state = updated;
            Ok(true)
        })
    }
}
