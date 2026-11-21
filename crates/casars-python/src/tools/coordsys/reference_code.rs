// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType, DirectionCoordinate, ObsInfo};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::{MPosition, MeasFrame};
use casa_types::{ArrayValue, ScalarValue, Value};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyBool;

use crate::convert::holder::{DIRECTION_CODES, FREQUENCY_CODES};
use crate::tools::{LogLine, emit};

use super::conversion_frames::{FrameName, declared_frequency_ref, relabel_spectral_record};
use super::frame_metadata::{coordinate, missing};
use super::inspect::{coordinate_type, numbered_coordinates};
use super::replace::replace_at;
use super::{CoordinateState, PyCoordsys};

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

struct Adjust(bool);

impl<'a, 'py> FromPyObject<'a, 'py> for Adjust {
    type Error = PyErr;

    fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        value
            .cast::<PyBool>()
            .map(|value| Self(value.is_true()))
            .map_err(|_| {
                PyTypeError::new_err(
                    "in method 'coordsys_setreferencecode', argument 4 of type 'bool'",
                )
            })
    }
}

fn native_direction(model: &DirectionCoordinate) -> PyResult<MDirection> {
    let [longitude, latitude] = model
        .reference_value()
        .try_into()
        .map_err(|_| raised("Invalid direction reference value"))?;
    Ok(MDirection::from_angles(
        longitude,
        latitude,
        model.direction_ref(),
    ))
}

fn epoch_frame(frame: MeasFrame, obs: &ObsInfo) -> PyResult<MeasFrame> {
    let epoch = obs
        .date
        .as_ref()
        .filter(|epoch| epoch.value().as_mjd() >= 0.0)
        .ok_or_else(|| raised("The output CoordinateSystem has no valid epoch"))?;
    Ok(frame.with_epoch(epoch.clone()))
}

fn position_frame(frame: MeasFrame, obs: &ObsInfo) -> PyResult<MeasFrame> {
    let measures = casa_ms::open_measures_runtime().map_err(raised)?;
    let position =
        MPosition::casa_observatory(&obs.telescope, measures.as_ref()).ok_or_else(|| {
            raised(format!(
                "Cannot find the observatory name {} in the CASA database",
                obs.telescope
            ))
        })?;
    Ok(frame.with_position(position).with_measures(measures))
}

fn adjust_direction(
    model: &DirectionCoordinate,
    frame: DirectionRef,
    obs: &ObsInfo,
) -> PyResult<[f64; 2]> {
    let direction = native_direction(model)?;
    let empty = MeasFrame::new();
    let converted = match direction.convert_to(frame, &empty) {
        Ok(converted) => converted,
        Err(_) => {
            let context = epoch_frame(empty, obs)?;
            match direction.convert_to(frame, &context) {
                Ok(converted) => converted,
                Err(_) => direction
                    .convert_to(frame, &position_frame(context, obs)?)
                    .map_err(raised)?,
            }
        }
    };
    let (mut longitude, latitude) = converted.as_angles();
    if longitude > std::f64::consts::PI {
        longitude -= std::f64::consts::TAU;
    }
    Ok([longitude, latitude])
}

fn direction_code(state: &mut CoordinateState, name: &str, adjust: bool) -> PyResult<()> {
    let (index, _, placement) = numbered_coordinates(state)?
        .into_iter()
        .find(|(_, model, _)| matches!(model, CoordinateModel::Direction(_)))
        .ok_or_else(|| missing(CoordinateType::Direction))?;
    let frame = DIRECTION_CODES
        .resolve(name)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| raised(format!("Invalid direction code '{name}' given")))?;
    let CoordinateModel::Direction(model) = placement.model(&state.record).map_err(raised)? else {
        return Err(missing(CoordinateType::Direction));
    };
    if model.direction_ref() == frame {
        return Ok(());
    }
    let reference = adjust
        .then(|| adjust_direction(&model, frame, state.coordinates.obs_info()))
        .transpose()?;
    let changed = CoordinateModel::from(
        model
            .with_reassigned_reference_frame(frame, reference)
            .map_err(raised)?,
    );
    let record = Value::Record(changed.to_record());
    replace_at(state, index as i32, changed, record)
}

fn frequency_context(state: &CoordinateState) -> PyResult<MeasFrame> {
    let Some((_, placement)) = coordinate(state, CoordinateType::Direction)? else {
        return Err(raised("No Direction coordinate in 'to' CoordinateSystem"));
    };
    let CoordinateModel::Direction(direction) = placement.model(&state.record).map_err(raised)?
    else {
        return Err(missing(CoordinateType::Direction));
    };
    let obs = state.coordinates.obs_info();
    let frame = position_frame(
        MeasFrame::new().with_direction(native_direction(&direction)?),
        obs,
    )?;
    epoch_frame(frame, obs)
}

fn spectral_code(
    state: &mut CoordinateState,
    name: &str,
    adjust: bool,
    log: &mut Vec<LogLine>,
) -> PyResult<()> {
    let (index, CoordinateModel::Spectral(model), placement) = numbered_coordinates(state)?
        .into_iter()
        .find(|(_, model, _)| matches!(model, CoordinateModel::Spectral(_)))
        .ok_or_else(|| missing(CoordinateType::Spectral))?
    else {
        return Err(missing(CoordinateType::Spectral));
    };
    let frame = FREQUENCY_CODES
        .resolve(name)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| raised(format!("Invalid frequency code '{name}' given")))?;
    let native = declared_frequency_ref(model);
    if native == frame {
        return Ok(());
    }
    if let Some(conversion) = model
        .conversion_frequency_ref()
        .filter(|value| *value != native)
    {
        log.push(LogLine::warning(format!(
            "Resetting the conversion frequency system {conversion} to the new native frequency system {frame}. You must explicitly reset the conversion frequency system if desired"
        )));
    }

    let mut changed = model.clone().with_frequency_reference(frame);
    if adjust {
        let context = frequency_context(state)?;
        if matches!(native, FrequencyRef::REST) || matches!(frame, FrequencyRef::REST) {
            log.push(LogLine::warning("Unable to convert between the input and output SpectralCoordinates. Conversion involving the REST frame requires a source radial velocity."));
            return Err(raised("No MeasFrame specified for conversion of Frequency"));
        }
        if matches!(native, FrequencyRef::Undefined) || matches!(frame, FrequencyRef::Undefined) {
            return Err(raised(
                "Transformations to/from frame \"Undefined\" are not possible.",
            ));
        }
        let hz = changed
            .world_frequency_hz(model.reference_pixel()[0])
            .map_err(raised)?;
        let frequency = MFrequency::new(hz, native)
            .convert_to(frame, &context)
            .map_err(raised)?;
        changed = changed
            .with_reference_frequency_hz(frequency.hz())
            .map_err(raised)?;
    }

    let Some(Value::Record(mut record)) = state.record.get(&placement.field).cloned() else {
        return Err(raised("Missing spectral coordinate record"));
    };
    relabel_spectral_record(&mut record, frame);
    let changed = CoordinateModel::from(changed);
    if adjust {
        let rebuilt = changed.to_record();
        if let Some(table) = rebuilt.get("tabular") {
            record.upsert("tabular", table.clone());
        } else if let Some(Value::Record(wcs)) = record.get_mut("wcs") {
            wcs.upsert(
                "crval",
                Value::Scalar(ScalarValue::Float64(changed.reference_value()[0])),
            );
        } else {
            let value = ArrayValue::from_f64_vec(changed.reference_value()).into();
            // Version 1 stored even a linear spectral WCS in `tabular`.
            if let Some(Value::Record(table)) = record.get_mut("tabular") {
                table.upsert("crval", value);
            } else {
                record.upsert("crval", value);
            }
        }
    }
    replace_at(state, index as i32, changed, Value::Record(record))
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (value, r#type=FrameName::direction_default(), adjust=Adjust(true)), text_signature = "($self, value, type='direction', adjust=True)")]
    fn setreferencecode(
        &self,
        py: Python<'_>,
        value: FrameName,
        r#type: FrameName,
        adjust: Adjust,
    ) -> PyResult<bool> {
        let value = value.into_name("value")?;
        let kind = coordinate_type(&r#type.into_name("type")?);
        let mut log = Vec::new();
        let result = py.detach(|| {
            let mut state = self.state();
            match kind {
                CoordinateType::Direction => direction_code(&mut state, &value, adjust.0),
                CoordinateType::Spectral => spectral_code(&mut state, &value, adjust.0, &mut log),
                _ => Err(raised("Coordinate type must be 'Direction' or 'Spectral'")),
            }
        });
        emit(py, "casars.tools.coordsys", &log)?;
        result.map(|()| true)
    }
}
