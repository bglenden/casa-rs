// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, SpectralCoordinate, SpectralNativeType};
use casa_types::measures::doppler::DopplerRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim, casacore};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::holder::{DOPPLER_CODES, FREQUENCY_CODES, doppler_ref};
use crate::convert::{Variant, quantity};
use crate::tools::table::casa_record;
use crate::tools::{LogLine, emit};

use super::conversion_frames::FrameName;
use super::doppler::sampled_frequencies_from_velocities;
use super::inspect::numbered_coordinates;
use super::replace::replace_at;
use super::transform_single::bool_vector;
use super::{CoordinateState, PyCoordsys, default_spectral_conversion};

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

fn record_samples(record: &RecordValue) -> Result<Samples, String> {
    let missing = || "Illegal Quantum record in QuantumHolder::fromRecord".to_owned();
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
        _ => return Err("Cannot convert QuantumHolder to Quantum<Vector<Double> >".into()),
    };
    Ok(Samples {
        values,
        unit: casacore::casacore_unit(unit).map_err(|error| error.to_string())?,
    })
}

fn text_samples(values: Vec<String>) -> Result<Samples, String> {
    let quantities = values
        .into_iter()
        .map(|value| quantity(&Variant::Text(value)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "Bad input parameter".to_owned())?;
    let unit = quantities
        .first()
        .ok_or_else(|| "Bad input parameter".to_owned())?
        .unit()
        .clone();
    let values = quantities
        .iter()
        .map(|quantity| quantity.get_value_in(&unit))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    Ok(Samples { values, unit })
}

fn sample_argument(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
) -> Result<Option<Samples>, String> {
    let Some(value) = value.filter(|value| !value.is_none()) else {
        return Ok(None);
    };
    if bool_vector(value)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(None);
    }
    let samples = if let Ok(record) = value.cast::<PyDict>() {
        record_samples(&casa_record(py, Some(record)).map_err(|error| error.to_string())?)?
    } else {
        match value
            .extract::<Variant>()
            .map_err(|error| error.to_string())?
        {
            Variant::Text(value) => text_samples(vec![value])?,
            Variant::Texts(values) => text_samples(values)?,
            _ => return Err("Unrecognized input parameter".into()),
        }
    };
    Ok((!samples.values.is_empty()).then_some(samples))
}

fn capture_samples(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
    log: &mut Vec<LogLine>,
) -> Option<Samples> {
    match sample_argument(py, value) {
        Ok(samples) => samples,
        Err(error) => {
            log.push(LogLine::warning(format!(
                "Error in {name} parameter.  {error}"
            )));
            None
        }
    }
}

fn declared_frame(model: &SpectralCoordinate) -> FrequencyRef {
    if model.has_undefined_frame() {
        FrequencyRef::Undefined
    } else {
        model.frequency_ref()
    }
}

fn native_conversion_record(record: &mut RecordValue, frame: FrequencyRef) {
    let mut conversion = match record.remove("conversion") {
        Some(Value::Record(record)) => record,
        _ => default_spectral_conversion(),
    };
    conversion.upsert(
        "system",
        Value::Scalar(ScalarValue::String(frame.as_str().into())),
    );
    record.upsert("conversion", Value::Record(conversion));
}

fn update_rest_frequency(
    model: SpectralCoordinate,
    rest: Option<Quantity>,
) -> PyResult<SpectralCoordinate> {
    let Some(rest) = rest.filter(|rest| rest.value() > 0.0) else {
        return Ok(model);
    };
    let mut hz = rest.value();
    if rest.unit().val().dim == frequency_dimension() {
        hz *= rest.unit().val().factor;
    } else if rest.unit().val().dim != UnitDim::NODIM {
        return Err(raised("Rest frequency units are not compatible with Hz"));
    }
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
    let native_frame = declared_frame(original);
    let model = SpectralCoordinate::from_channel_frequencies(native_frame, samples, rest)
        .map_err(raised)?
        .with_name(original.axis_names().remove(0))
        .with_velocity_convention(convention)
        .with_native_type(native_type);
    let model = CoordinateModel::from(model);
    let mut record = model.to_record();
    native_conversion_record(&mut record, native_frame);
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
    let old_frame = declared_frame(original);
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
        let name = Value::Scalar(ScalarValue::String(frame.as_str().into()));
        record.upsert("system", name.clone());
        if record.get("frequency_ref").is_some() {
            record.upsert("frequency_ref", name);
        }
        native_conversion_record(&mut record, frame);
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
        let frequencies = capture_samples(py, frequencies, "frequencies", &mut log);
        let velocities = capture_samples(py, velocities, "velocities", &mut log);
        let edit = SpectralEdit {
            frame,
            rest,
            frequencies,
            doppler,
            velocities,
        };
        let (result, log) = py.detach(|| {
            let result = apply_edit(&mut self.state(), edit, &mut log);
            (result, log)
        });
        emit(py, "casars.tools.coordsys", &log)?;
        result
    }
}
