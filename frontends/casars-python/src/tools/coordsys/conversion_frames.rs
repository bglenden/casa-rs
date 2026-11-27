// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType, SpectralCoordinate};
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::frequency::FrequencyRef;
use casa_core::measures::{MPosition, MeasFrame};
use casa_core::{RecordValue, ScalarValue, Value};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::convert::holder::{DIRECTION_CODES, FREQUENCY_CODES};
use crate::tools::{LogLine, emit};

use super::frame_metadata::coordinate;
use super::{CoordinateState, PyCoordsys, default_spectral_conversion};

pub(super) struct FrameName(Option<String>);

impl FrameName {
    pub(super) fn radio_default() -> Self {
        Self(Some("radio".into()))
    }

    pub(super) fn direction_default() -> Self {
        Self(Some("direction".into()))
    }

    pub(super) fn into_name(self, argument: &str) -> PyResult<String> {
        self.0
            .ok_or_else(|| PyTypeError::new_err(format!("argument _{argument} must be a string")))
    }
}

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

pub(super) fn declared_frequency_ref(model: &SpectralCoordinate) -> FrequencyRef {
    if model.has_undefined_frame() {
        FrequencyRef::Undefined
    } else {
        model.frequency_ref()
    }
}

pub(super) fn relabel_spectral_record(record: &mut RecordValue, frame: FrequencyRef) {
    let system = Value::Scalar(ScalarValue::String(frame.as_str().into()));
    record.upsert("system", system.clone());
    if record.get("frequency_ref").is_some() {
        record.upsert("frequency_ref", system.clone());
    }
    let mut conversion = match record.remove("conversion") {
        Some(Value::Record(record)) => record,
        _ => default_spectral_conversion(),
    };
    conversion.upsert("system", system);
    record.upsert("conversion", Value::Record(conversion));
}

fn set_spectral(state: &mut CoordinateState, name: &str) -> PyResult<Option<LogLine>> {
    let Some((CoordinateModel::Spectral(model), placement)) =
        coordinate(state, CoordinateType::Spectral)?
    else {
        return Ok(None);
    };
    let Some((_, direction_placement)) = coordinate(state, CoordinateType::Direction)? else {
        return Err(spectral_error(
            "No DirectionCoordinate; cannot set Spectral conversion layer",
        ));
    };
    let frame = FREQUENCY_CODES
        .resolve(name)
        .and_then(|canonical| canonical.parse::<FrequencyRef>().ok())
        .ok_or_else(|| spectral_error(format!("invalid frequency system {name}")))?;
    let native = declared_frequency_ref(model);
    if model.conversion_frequency_ref().unwrap_or(native) == frame {
        return Ok(None);
    }

    let CoordinateModel::Direction(direction) =
        direction_placement.model(&state.record).map_err(raised)?
    else {
        return Err(spectral_error("Invalid DirectionCoordinate"));
    };
    let context = spectral_observation_frame(&state.coordinates, &direction)?;
    if matches!(native, FrequencyRef::Undefined) || matches!(frame, FrequencyRef::Undefined) {
        return Err(raised(
            "Transformations to/from frame \"Undefined\" are not possible.",
        ));
    }
    // CASA accepts this request without changing the conversion when no
    // source radial velocity can be supplied by the coordinate tool.
    if matches!(native, FrequencyRef::REST) || matches!(frame, FrequencyRef::REST) {
        return Ok(Some(LogLine::warning(
            "Unable to convert between the input and output SpectralCoordinates. Conversion involving the REST frame requires a source radial velocity.",
        )));
    }
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
    )?;
    Ok(None)
}

fn spectral_observation_frame(
    coordinates: &casa_coordinates::CoordinateSystem,
    direction: &casa_coordinates::DirectionCoordinate,
) -> PyResult<MeasFrame> {
    let pixel = direction
        .reference_pixel()
        .try_into()
        .map_err(|_| spectral_error("Invalid direction reference pixel"))?;
    let [longitude, latitude] = direction.pixel_to_world(pixel).map_err(raised)?;
    let direction =
        MDirection::from_angles(longitude, latitude, direction.conversion_direction_ref());
    let info = coordinates.obs_info();
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
    Ok(MeasFrame::new()
        .with_direction(direction)
        .with_position(position)
        .with_epoch(epoch.clone())
        .with_measures(measures))
}

/// Prepare the same observing context used by the coordinate tool's conversion layer.
pub(in crate::tools) fn profile_frequency_frame(
    coordinates: &casa_coordinates::CoordinateSystem,
    name: &str,
) -> PyResult<Option<(FrequencyRef, MeasFrame)>> {
    let Some(owner) = coordinates.find_coordinate(CoordinateType::Spectral) else {
        return Ok(None);
    };
    let CoordinateModel::Spectral(model) = coordinates.coordinate(owner) else {
        unreachable!("spectral model matched by type");
    };
    let frame = FREQUENCY_CODES
        .resolve(name)
        .and_then(|canonical| canonical.parse::<FrequencyRef>().ok())
        .ok_or_else(|| spectral_error(format!("invalid frequency system {name}")))?;
    let native = declared_frequency_ref(model);
    if model.conversion_frequency_ref().unwrap_or(native) == frame {
        return Ok(None);
    }
    if matches!(native, FrequencyRef::REST) || matches!(frame, FrequencyRef::REST) {
        return Ok(None);
    }
    if matches!(native, FrequencyRef::Undefined) || matches!(frame, FrequencyRef::Undefined) {
        return Err(spectral_error(
            "Transformations to/from frame \"Undefined\" are not possible",
        ));
    }
    let Some(owner) = coordinates.find_coordinate(CoordinateType::Direction) else {
        return Err(spectral_error(
            "No DirectionCoordinate; cannot set Spectral conversion layer",
        ));
    };
    let CoordinateModel::Direction(direction) = coordinates.coordinate(owner) else {
        unreachable!("direction model matched by type");
    };
    let context = spectral_observation_frame(coordinates, direction)?;
    Ok(Some((frame, context)))
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
        let direction = direction.into_name("direction")?;
        let spectral = spectral.into_name("spectral")?;
        if direction.is_empty() && spectral.is_empty() {
            return Ok(true);
        }
        let warning = py.detach(|| {
            let mut state = self.state();
            let mut updated = state.clone();
            if !direction.is_empty() {
                set_direction(&mut updated, &direction)?;
            }
            let warning = if spectral.is_empty() {
                None
            } else {
                set_spectral(&mut updated, &spectral)?
            };
            *state = updated;
            Ok::<_, PyErr>(warning)
        })?;
        if let Some(warning) = warning {
            emit(py, "casars.tools.coordsys", &[warning])?;
        }
        Ok(true)
    }
}
