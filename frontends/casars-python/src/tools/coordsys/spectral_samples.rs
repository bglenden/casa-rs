// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, SpectralCoordinate, SpectralNativeType};
use casa_types::measures::doppler::DopplerRef;
use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim, casacore};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::holder::{DOPPLER_CODES, FREQUENCY_CODES, doppler_ref};
use crate::convert::{Variant, quantity};
use crate::tools::table::casa_record;
use crate::tools::{LogLine, emit};

use super::conversion_frames::{FrameName, declared_frequency_ref, relabel_spectral_record};
use super::doppler::sampled_frequencies_from_velocities;
use super::inspect::numbered_coordinates;
use super::replace::replace_at;
use super::rest_frequency::{quantity_tokens, quantity_vector};
use super::transform_single::bool_vector;
use super::{CoordinateState, PyCoordsys};

fn raised(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn frequency_dimension() -> UnitDim {
    UnitDim::NODIM / UnitDim::basis(Dimension::Time)
}

struct Samples {
    values: Vec<f64>,
    unit: Unit,
}

fn record_samples(record: &RecordValue) -> Result<(Vec<f64>, &str), &'static str> {
    let missing = || "Illegal Quantum record in QuantumHolder::fromRecord";
    let Some(Value::Scalar(ScalarValue::String(unit))) = record.get("unit") else {
        return Err(missing());
    };
    let values = match record.get("value").ok_or_else(missing)? {
        Value::Scalar(ScalarValue::Float64(value)) => vec![*value],
        Value::Scalar(ScalarValue::Float32(value)) => vec![f64::from(*value)],
        Value::Array(ArrayValue::Float64(values)) if values.ndim() == 1 => {
            values.iter().copied().collect()
        }
        Value::Array(ArrayValue::Float32(values)) if values.ndim() == 1 => {
            values.iter().copied().map(f64::from).collect()
        }
        _ => return Err("Cannot convert QuantumHolder to Quantum<Vector<Double> >"),
    };
    Ok((values, unit))
}

fn capture_samples(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
    log: &mut Vec<LogLine>,
) -> PyResult<Option<Samples>> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(None);
    };
    if bool_vector(value)?.is_some() {
        return Ok(None);
    }
    let mut warn = |error: &str| {
        log.push(LogLine::warning(format!(
            "Error in {name} parameter.  {error}"
        )));
        Ok(None)
    };
    let (values, unit) = if let Ok(record) = value.cast::<PyDict>() {
        let record = casa_record(py, Some(record))?;
        match record_samples(&record) {
            Ok((values, unit)) => (values, casacore::casacore_unit(unit).map_err(raised)?),
            Err(error) => return warn(error),
        }
    } else {
        let texts = match value.extract::<Variant>()? {
            Variant::Text(value) if value.is_empty() => vec![value],
            Variant::Text(value) => quantity_tokens(&value),
            Variant::Texts(values) => values,
            _ => return warn("Unrecognized input parameter"),
        };
        let Some(samples) = quantity_vector(&texts)? else {
            return warn("Bad input parameter");
        };
        samples
    };
    Ok((!values.is_empty()).then_some(Samples { values, unit }))
}

fn update_rest_frequency(
    model: SpectralCoordinate,
    rest: Option<Quantity>,
) -> PyResult<SpectralCoordinate> {
    let Some(rest) = rest.filter(|rest| rest.value() > 0.0) else {
        return Ok(model);
    };
    let unit = rest.unit().val();
    if unit.dim != frequency_dimension() && unit.dim != UnitDim::NODIM {
        return Err(raised("Rest frequency units are not compatible with Hz"));
    }
    let hz = rest.value() * unit.factor;
    let native = model.value_from_hz(hz).map_err(raised)?;
    model.with_replaced_rest_frequency(native).map_err(raised)
}

fn sampled_coordinate(
    original: &SpectralCoordinate,
    frequencies: Option<Samples>,
    doppler: &str,
    velocities: Option<Samples>,
) -> PyResult<Option<(CoordinateModel, RecordValue)>> {
    if frequencies.is_none() && velocities.is_none() {
        return Ok(None);
    }
    let rest = original.rest_frequency_hz().map_err(raised)?;
    let (samples, native_type, convention) = match (frequencies, velocities) {
        (Some(mut frequencies), None) => {
            if frequencies.unit.val().dim != frequency_dimension() {
                return Err(raised("Illegal unit for frequencies"));
            }
            frequencies
                .values
                .iter_mut()
                .for_each(|value| *value *= frequencies.unit.val().factor);
            (
                frequencies.values,
                SpectralNativeType::Frequency,
                DopplerRef::RADIO,
            )
        }
        (None, Some(velocities)) => {
            if velocities.unit.val().dim
                != UnitDim::basis(Dimension::Length) / UnitDim::basis(Dimension::Time)
            {
                return Err(raised("Illegal unit for velocities"));
            }
            if doppler.is_empty() {
                return Err(raised("You must specify the doppler type"));
            }
            let convention = DOPPLER_CODES
                .resolve(doppler)
                .and_then(|code| doppler_ref(code).ok())
                .ok_or_else(|| raised(format!("Invalid doppler '{doppler}'")))?;
            let native = if convention == DopplerRef::Z {
                SpectralNativeType::OpticalVelocity
            } else {
                SpectralNativeType::RadioVelocity
            };
            (
                sampled_frequencies_from_velocities(
                    velocities.values,
                    &velocities.unit,
                    convention,
                    rest,
                )?,
                native,
                convention,
            )
        }
        _ => return Ok(None),
    };
    let native_frame = declared_frequency_ref(original);
    let model = SpectralCoordinate::from_channel_frequencies(native_frame, samples, rest)
        .map_err(raised)?
        .with_name(original.axis_names().remove(0))
        .with_velocity_convention(convention)
        .with_native_type(native_type);
    let model = CoordinateModel::from(model);
    let mut record = model.to_record();
    relabel_spectral_record(&mut record, native_frame);
    Ok(Some((model, record)))
}

struct SpectralEdit {
    frame: String,
    rest: Option<Quantity>,
    frequencies: Option<Samples>,
    doppler: String,
    velocities: Option<Samples>,
}

fn apply_edit(
    state: &mut CoordinateState,
    edit: SpectralEdit,
    log: &mut Vec<LogLine>,
) -> PyResult<bool> {
    if edit.frequencies.is_some() && edit.velocities.is_some() {
        log.push(LogLine::error(
            "You cannot give both frequencies and velocities",
        ));
        return Ok(false);
    }
    if !edit.frame.is_empty()
        && !edit.doppler.is_empty()
        && edit.rest.as_ref().is_some_and(|rest| rest.value() != -1.0)
        && edit.frequencies.is_none()
        && edit.velocities.is_none()
    {
        log.push(LogLine::warning("Nothing to do"));
        return Ok(true);
    }
    let (index, original, placement) = numbered_coordinates(state)?
        .into_iter()
        .find_map(|(index, model, placement)| {
            if let CoordinateModel::Spectral(spectral) = model {
                Some((index, spectral, placement))
            } else {
                None
            }
        })
        .ok_or_else(|| raised("No coordinate of type Spectral in this CoordinateSystem"))?;
    let old_frame = declared_frequency_ref(original);
    let frame = if edit.frame.is_empty() {
        old_frame
    } else {
        FREQUENCY_CODES
            .resolve(&edit.frame)
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| {
                raised(format!(
                    "Invalid frequency reference '{}'",
                    edit.frame.to_uppercase()
                ))
            })?
    };
    let changed =
        update_rest_frequency(original.clone().with_frequency_reference(frame), edit.rest)?;
    if let Some((model, record)) =
        sampled_coordinate(&changed, edit.frequencies, &edit.doppler, edit.velocities)?
    {
        replace_at(state, index as i32, model, Value::Record(record))?;
        return Ok(true);
    }
    let Some(Value::Record(stored)) = state.record.get(&placement.field) else {
        return Err(raised("Missing spectral coordinate record"));
    };
    let mut record = stored.clone();
    let model = CoordinateModel::from(changed);
    let native = model.to_record();
    for field in ["restfreq", "restfreqs"] {
        if let Some(value) = native.get(field) {
            record.upsert(field, value.clone());
        }
    }
    if old_frame != frame {
        relabel_spectral_record(&mut record, frame);
    }
    replace_at(state, index as i32, model, Value::Record(record))?;
    Ok(true)
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (refcode=FrameName::default(), restfreq=None, frequencies=None, doppler=FrameName::default(), velocities=None), text_signature = "($self, refcode='', restfreq=[], frequencies=[], doppler='', velocities=[])")]
    fn setspectral(
        &self,
        py: Python<'_>,
        refcode: FrameName,
        restfreq: Option<&Bound<'_, PyAny>>,
        frequencies: Option<&Bound<'_, PyAny>>,
        doppler: FrameName,
        velocities: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let frame = refcode.into_name("refcode")?;
        let doppler = doppler.into_name("doppler")?;
        let rest = restfreq
            .filter(|value| !value.is_none())
            .map(|value| {
                let variant = value.extract::<Variant>()?;
                if variant.is_empty_sequence() {
                    Ok(None)
                } else {
                    quantity(&variant)
                        .map(Some)
                        .map_err(|error| raised(error.to_string()))
                }
            })
            .transpose()?
            .flatten();
        let mut log = Vec::new();
        let result = (|| {
            let edit = SpectralEdit {
                frame,
                rest,
                frequencies: capture_samples(py, frequencies, "frequencies", &mut log)?,
                doppler,
                velocities: capture_samples(py, velocities, "velocities", &mut log)?,
            };
            py.detach(|| apply_edit(&mut self.state(), edit, &mut log))
        })();
        emit(py, "casars.tools.coordsys", &log)?;
        result
    }
}
