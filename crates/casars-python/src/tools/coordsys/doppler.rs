// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::{Coordinate, CoordinateModel, CoordinateType, SpectralCoordinate};
use casa_types::measures::MeasFrame;
use casa_types::measures::doppler::{DopplerRef, MDoppler};
use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim, casacore};
use casa_types::{RecordValue, ScalarValue, Value};
use numpy::{PyArrayDescrMethods, PyReadonlyArrayDyn, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::exceptions::{PyRuntimeError, PySystemError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use crate::convert::holder::{C_MPS, DOPPLER_CODES, doppler_ref};
use crate::convert::{Variant, double_vector_element, quantity, value::CasaValue};

use super::frame_metadata::coordinate;
use super::{CoordinateState, PyCoordsys};

fn raised(message: impl ToString) -> PyErr {
    PyRuntimeError::new_err(message.to_string())
}

pub(super) fn values(value: &Bound<'_, PyAny>, method: &str) -> PyResult<Vec<f64>> {
    if value.is_none() {
        return Ok(Vec::new());
    }
    if value.is_instance_of::<PyTuple>() {
        return Err(PySystemError::new_err(format!(
            "<built-in function coordsys_{method}> returned a result with an exception set"
        )));
    }
    // Scalar text takes the binding's -1 sentinel. Vector text is parsed.
    if value.is_instance_of::<PyString>() || value.is_instance_of::<PyBytes>() {
        return Ok(vec![-1.0]);
    }
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyUntypedArray>() {
        let py = value.py();
        let options = PyDict::new(py);
        if value.is_instance_of::<PyList>() {
            // Preserve mixed element types while determining the nested shape.
            options.set_item("dtype", "object")?;
        }
        let array = py
            .import("numpy")?
            .call_method("asarray", (value,), Some(&options))?;
        let kind = array.cast::<PyUntypedArray>()?.dtype().kind();
        if matches!(kind, b'b' | b'i' | b'u' | b'f') {
            let doubles = array.call_method1("astype", ("float64",))?;
            let values = doubles.extract::<PyReadonlyArrayDyn<'_, f64>>()?;
            return Ok(values.as_array().t().iter().copied().collect());
        }
        let flat = array.call_method1("ravel", ("F",))?;
        return flat
            .try_iter()?
            .map(|item| item.map(|item| double_vector_element(&item).unwrap_or(0.0)))
            .collect();
    }
    Ok(vec![value.extract::<f64>()?])
}

fn beta(velocity: &Quantity) -> PyResult<f64> {
    let unit = velocity.unit();
    let base = *unit.val();
    let speed = UnitDim::basis(Dimension::Length) / UnitDim::basis(Dimension::Time);
    if base.dim == speed {
        Ok(velocity.value() * base.factor / C_MPS)
    } else if base.dim == UnitDim::NODIM {
        Ok(velocity.value() * base.factor)
    } else {
        Err(nonconformant(unit.name()))
    }
}

fn nonconformant(unit: &str) -> PyErr {
    raised(format!("Quantum::assure non-conforming unit type '{unit}'"))
}

fn spectral_shift(values: &mut [f64], freq_unit: &str, velocity: &Quantity) -> PyResult<()> {
    let ratio = MDoppler::new(beta(velocity)?, DopplerRef::BETA).as_frequency_ratio();
    let unit = casacore::casacore_unit(freq_unit).map_err(raised)?;
    if values.is_empty() {
        return Ok(());
    }
    let dim = unit.val().dim;
    if casacore::frequency_hz(&Quantity::with_unit(1.0, unit)).is_none() {
        return Err(nonconformant(freq_unit));
    }
    let ratio = if [Dimension::Time, Dimension::Length]
        .into_iter()
        .any(|dimension| dim == UnitDim::basis(dimension))
    {
        ratio.recip()
    } else {
        ratio
    };
    for value in values {
        *value *= ratio;
    }
    Ok(())
}

pub(super) struct VelocityConversion {
    rest_hz: f64,
    hz_per_unit: f64,
    velocity_scale: f64,
    convention: DopplerRef,
}

pub(super) fn sampled_frequencies_from_velocities(
    velocities: Vec<f64>,
    unit: &Unit,
    convention: DopplerRef,
    rest_hz: f64,
) -> PyResult<Vec<f64>> {
    let conversion = VelocityConversion {
        rest_hz,
        hz_per_unit: 1.0,
        velocity_scale: unit.val().factor / C_MPS,
        convention,
    };
    velocities
        .into_iter()
        .map(|value| conversion.velocity_to_frequency(value))
        .collect()
}

fn velocity_settings(
    state: &CoordinateState,
    field: &str,
    spectral: &SpectralCoordinate,
    vel_unit: &str,
    doppler: &str,
) -> PyResult<(f64, DopplerRef)> {
    let speed = UnitDim::basis(Dimension::Length) / UnitDim::basis(Dimension::Time);
    let unit = casacore::casacore_unit(vel_unit).map_err(raised)?;
    let requested = doppler_ref(DOPPLER_CODES.resolve_or_default(doppler)).map_err(raised)?;
    if unit.val().dim == speed {
        return Ok((unit.val().factor / C_MPS, requested));
    }
    let record = match state.record.get(field) {
        Some(Value::Record(record)) => Some(record),
        _ => None,
    };
    let (stored, stored_convention) = stored_velocity_settings(record, spectral)?;
    let convention = if vel_unit.is_empty() {
        requested
    } else {
        stored_convention
    };
    Ok((stored.val().factor / C_MPS, convention))
}

pub(in crate::tools) fn stored_velocity_settings(
    record: Option<&RecordValue>,
    spectral: &SpectralCoordinate,
) -> PyResult<(Unit, DopplerRef)> {
    let mut convention = spectral.velocity_convention();
    let stored_unit = if let Some(record) = record {
        if record.get("velType").is_none() {
            let legacy_code = match record.get("prefVelType") {
                Some(Value::Scalar(ScalarValue::Int32(code))) => Some(*code),
                Some(Value::Scalar(ScalarValue::Int64(code))) => i32::try_from(*code).ok(),
                _ => None,
            };
            if let Some(legacy) = legacy_code.and_then(DopplerRef::from_casacore_code) {
                convention = legacy;
            }
        }
        match record.get("velUnit").or_else(|| record.get("prefVelUnit")) {
            Some(Value::Scalar(ScalarValue::String(unit))) if !unit.is_empty() => unit.as_str(),
            _ => "km/s",
        }
    } else {
        "km/s"
    };
    let stored = casacore::casacore_unit(stored_unit).map_err(raised)?;
    let speed = UnitDim::basis(Dimension::Length) / UnitDim::basis(Dimension::Time);
    if stored.val().dim == speed {
        Ok((stored, convention))
    } else {
        // Restoring an incompatible unit leaves both defaults.
        Ok((
            Unit::new("km/s").expect("standard speed unit"),
            DopplerRef::RADIO,
        ))
    }
}

impl VelocityConversion {
    pub(super) fn for_spectral_axis(
        spectral: &SpectralCoordinate,
        unit: &Unit,
        convention: DopplerRef,
    ) -> PyResult<Self> {
        Ok(Self {
            rest_hz: spectral.rest_frequency_hz().map_err(raised)?,
            hz_per_unit: spectral.value_in_hz(1.0).map_err(raised)?,
            velocity_scale: unit.val().factor / C_MPS,
            convention,
        })
    }

    pub(super) fn mixed_velocity(&self, frequency: f64) -> PyResult<f64> {
        if self.rest_hz == 0.0 {
            return Err(raised(
                "Exception: Rest frequency is 0 so cannot convert to velocity.",
            ));
        }
        let velocity = self.frequency_to_velocity(frequency)?;
        if velocity.is_nan() {
            return Err(raised("velocity is NaN"));
        }
        Ok(velocity)
    }

    pub(super) fn mixed_frequency(&self, velocity: f64) -> PyResult<f64> {
        let frequency = self.velocity_to_frequency(velocity)?;
        if frequency <= 0.0 {
            return Err(raised("frequency <= 0"));
        }
        Ok(frequency)
    }

    fn new(
        state: &CoordinateState,
        spectral: &SpectralCoordinate,
        field: &str,
        freq_unit: &str,
        doppler: &str,
        vel_unit: &str,
    ) -> PyResult<Self> {
        let axis_units = spectral.axis_units();
        let freq_unit = if freq_unit.is_empty() {
            &axis_units[0]
        } else {
            freq_unit
        };
        let frequency_error = |cause| {
            raised(format!(
                "Failed to set frequency units of {freq_unit} because {cause}"
            ))
        };
        let frequency = casacore::casacore_unit(freq_unit)
            .map_err(|_| frequency_error("Unknown unit - cannot calculate scaling"))?;
        if frequency.val().dim != UnitDim::NODIM / UnitDim::basis(Dimension::Time) {
            return Err(frequency_error("Units are not compatible dimensionally"));
        }
        let (velocity_scale, convention) =
            velocity_settings(state, field, spectral, vel_unit, doppler)?;
        Ok(Self {
            rest_hz: spectral.rest_frequency_hz().map_err(raised)?,
            hz_per_unit: frequency.val().factor,
            velocity_scale,
            convention,
        })
    }

    fn frequency_to_velocity(&self, frequency: f64) -> PyResult<f64> {
        if !frequency.is_finite() || self.rest_hz <= 0.0 {
            return Ok(f64::NAN);
        }
        let ratio = frequency.abs() * self.hz_per_unit / self.rest_hz;
        // CASA's VelocityMachine expresses physical shifts in beta before
        // converting to the requested convention.
        let frame = MeasFrame::new();
        let velocity = MDoppler::new(ratio, DopplerRef::RATIO)
            .convert_to(DopplerRef::BETA, &frame)
            .and_then(|beta| beta.convert_to(self.convention, &frame))
            .map_err(raised)?;
        Ok(velocity.value() / self.velocity_scale)
    }

    fn velocity_to_frequency(&self, velocity: f64) -> PyResult<f64> {
        let beta = MDoppler::new(velocity * self.velocity_scale, self.convention)
            .convert_to(DopplerRef::BETA, &MeasFrame::new())
            .map_err(raised)?;
        Ok(beta.shift_frequency_hz(self.rest_hz) / self.hz_per_unit)
    }
}

#[derive(Clone, Copy)]
enum Output {
    Velocity,
    Frequency,
}

impl Output {
    fn convert(self, conversion: &VelocityConversion, value: f64) -> PyResult<f64> {
        match self {
            Self::Velocity => conversion.frequency_to_velocity(value),
            Self::Frequency => conversion.velocity_to_frequency(value),
        }
    }
}

impl PyCoordsys {
    fn convert_doppler(
        &self,
        py: Python<'_>,
        values: Vec<f64>,
        frequnit: &str,
        doppler: &str,
        velunit: &str,
        output: Output,
    ) -> PyResult<Py<PyAny>> {
        let result = py.detach(|| {
            let state = self.state();
            let Some((CoordinateModel::Spectral(spectral), placement)) =
                coordinate(&state, CoordinateType::Spectral)?
            else {
                return Err(raised(
                    "There is no spectral coordinate in this CoordinateSystem",
                ));
            };
            let conversion = VelocityConversion::new(
                &state,
                spectral,
                &placement.field,
                frequnit,
                doppler,
                velunit,
            )?;
            values
                .into_iter()
                .map(|value| output.convert(&conversion, value))
                .collect::<PyResult<Vec<_>>>()
        })?;
        CasaValue::floats(result).into_python(py)
    }
}

#[pymethods]
impl PyCoordsys {
    #[pyo3(signature = (value, frequnit="", velocity=None), text_signature = "($self, value, frequnit='', velocity=[])")]
    fn frequencytofrequency(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        frequnit: &str,
        velocity: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let mut values = values(value, "frequencytofrequency")?;
        let velocity = velocity
            .map(|value| value.extract::<Variant>())
            .transpose()?
            .unwrap_or(Variant::Empty);
        py.detach(|| {
            let state = self.state();
            let default_unit;
            let unit = if frequnit.is_empty() {
                default_unit = state
                    .coordinates
                    .find_coordinate(CoordinateType::Spectral)
                    .map_or_else(String::new, |index| {
                        state.coordinates.coordinate(index).axis_units().remove(0)
                    });
                default_unit.as_str()
            } else {
                frequnit
            };
            spectral_shift(&mut values, unit, &quantity(&velocity)?)
        })?;
        CasaValue::floats(values).into_python(py)
    }

    #[pyo3(signature = (value, frequnit="", doppler="radio", velunit="km/s"))]
    fn frequencytovelocity(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        frequnit: &str,
        doppler: &str,
        velunit: &str,
    ) -> PyResult<Py<PyAny>> {
        let values = values(value, "frequencytovelocity")?;
        self.convert_doppler(py, values, frequnit, doppler, velunit, Output::Velocity)
    }

    #[pyo3(signature = (value, frequnit="", doppler="radio", velunit="km/s"))]
    fn velocitytofrequency(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        frequnit: &str,
        doppler: &str,
        velunit: &str,
    ) -> PyResult<Py<PyAny>> {
        let values = values(value, "velocitytofrequency")?;
        self.convert_doppler(py, values, frequnit, doppler, velunit, Output::Frequency)
    }
}
