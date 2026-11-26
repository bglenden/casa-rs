// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA record imports: validate/default tool records before shared decoding.

mod measures;
mod spectrum;

use casa_components::{ComponentShape, Flux, ShapeType, SkyComponent, SpectralKind, SpectralModel};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use num_complex::Complex64;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{ConvertError, ConvertResult};
use crate::tools::LogLine;
use crate::tools::table::{component_keyword_value, define_component_keyword};

pub(super) struct Component {
    pub(super) value: SkyComponent,
    pub(super) units: Option<[Unit; 6]>,
    pub(super) offsets: super::ops::offsets::Offsets,
}

/// Reuse the CASA record typemap, including scalar/array and numeric kinds.
pub(super) fn argument(
    py: Python<'_>,
    value: Option<&Bound<'_, PyAny>>,
    name: &str,
) -> PyResult<RecordValue> {
    let Some(value) = value else {
        return Ok(RecordValue::default());
    };
    if !value.is_instance_of::<PyDict>() {
        return Err(PyTypeError::new_err(format!(
            "argument _{name} must be a dictionary"
        )));
    }
    let mut holder = RecordValue::default();
    define_component_keyword(
        &mut holder,
        "record".into(),
        component_keyword_value(py, Some(value))?,
    )?;
    match holder.get("record") {
        Some(Value::Record(record)) => Ok(record.clone()),
        _ => unreachable!("dictionary converts to a record"),
    }
}

pub(super) fn count(record: &RecordValue) -> ConvertResult<usize> {
    match record.get("nelements") {
        None => Ok(0),
        Some(Value::Scalar(ScalarValue::Int32(count))) => Ok(*count as u32 as usize),
        Some(Value::Scalar(ScalarValue::Int64(count))) => Ok(*count as u32 as usize),
        Some(Value::Scalar(ScalarValue::UInt32(count))) => Ok(*count as usize),
        _ => Err(ConvertError::runtime(
            "RecordInterface::asuInt - invalid data type",
        )),
    }
}

pub(super) fn item(record: &RecordValue, index: usize) -> ConvertResult<&RecordValue> {
    let name = format!("component{index}");
    match record.get(&name) {
        None => Err(ConvertError::runtime(format!(
            "RecordInterface: field {name} is unknown"
        ))),
        Some(Value::Record(record)) => Ok(record),
        Some(value) => Err(ConvertError::runtime(format!(
            "RecordRep::get_pointer - incorrect data type Record   used for field {name} with type {}",
            type_name(value)
        ))),
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Scalar(ScalarValue::Int32(_) | ScalarValue::Int64(_)) => "Int     ",
        Value::Scalar(ScalarValue::Float64(_) | ScalarValue::Float32(_)) => "double  ",
        Value::Scalar(ScalarValue::String(_)) => "String  ",
        Value::Scalar(ScalarValue::Bool(_)) => "Bool    ",
        _ => "Array   ",
    }
}

pub(super) fn decode(input: &RecordValue, lines: &mut Vec<LogLine>) -> Result<Component, String> {
    let defaults = SkyComponent::new(
        Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
        ComponentShape::point(MDirection::from_angles(
            0.0,
            std::f64::consts::FRAC_PI_2,
            DirectionRef::J2000,
        )),
        SpectralModel::new(
            MFrequency::new(1e9, FrequencyRef::LSRK),
            SpectralKind::Constant,
        ),
    )
    .to_record()
    .map_err(|e| e.to_string())?;
    let flux = match input.get("flux") {
        None => {
            lines.push(LogLine::warning("The component does not have a 'flux' field.\nThe default is 1.0 Jy in I and 0.0 in Q, U & V"));
            defaults.get("flux").expect("default flux").clone()
        }
        Some(Value::Record(record)) => Value::Record(flux(record, lines).map_err(|e| {
            if e.starts_with("Unit::check ") {
                e
            } else {
                format!("{e}Problem parsing the 'flux' field\n")
            }
        })?),
        Some(_) => return Err("The 'flux' field must be a record\n".into()),
    };
    let (shape, units) = match input.get("shape") {
        None => {
            lines.push(LogLine::warning("The component does not have a 'shape' field.\nThe default is a point component at the J2000 north pole"));
            (defaults.get("shape").expect("default shape").clone(), None)
        }
        Some(Value::Record(record)) => {
            let (record, units) = shape(record, lines)?;
            (Value::Record(record), units)
        }
        Some(_) => return Err("\nThe 'shape' field must be a record".into()),
    };
    let spectrum = match input.get("spectrum") {
        None => {
            lines.push(LogLine::warning("The component does not have a 'spectrum' field.\nThe default is a constant spectrum"));
            defaults.get("spectrum").expect("default spectrum").clone()
        }
        Some(Value::Record(record)) => Value::Record(spectrum(record, lines)?),
        Some(_) => return Err("\nThe 'spectrum' field must be a record".into()),
    };
    let mut record = RecordValue::new(vec![
        RecordField::new("flux", flux),
        RecordField::new("shape", shape),
        RecordField::new("spectrum", spectrum),
    ]);
    if let Some(label) = input.get("label") {
        if !matches!(label, Value::Scalar(ScalarValue::String(_))) {
            return Err("\nThe 'label' field must be a string".into());
        }
        record.push(RecordField::new("label", label.clone()));
    }
    if let Some(parameters) = input.get("optionalParameters") {
        if !matches!(parameters, Value::Array(ArrayValue::Float64(_))) {
            return Err("\nThe 'optionalParameters' field must be a Double Array".into());
        }
        record.push(RecordField::new("optionalParameters", parameters.clone()));
    }
    let offsets = super::ops::offsets::Offsets::extract(&mut record);
    let mut value = SkyComponent::from_record(&record).map_err(|e| e.to_string())?;
    value
        .spectrum_mut()
        .set_frequency_unit(Unit::new("GHz").expect("GHz"))
        .map_err(|e| e.to_string())?;
    Ok(Component {
        value,
        units,
        offsets,
    })
}

fn text<'a>(record: &'a RecordValue, key: &str) -> Option<&'a str> {
    match record.get(key) {
        Some(Value::Scalar(ScalarValue::String(text))) => Some(text),
        _ => None,
    }
}
fn scalar(value: &Value) -> Option<f64> {
    match value {
        Value::Scalar(ScalarValue::Float64(v)) => Some(*v),
        Value::Scalar(ScalarValue::Float32(v)) => Some(f64::from(*v)),
        Value::Scalar(ScalarValue::Int32(v)) => Some(f64::from(*v)),
        _ => None,
    }
}
fn numbers(value: &Value) -> Option<Vec<Complex64>> {
    let real = |v: f64| Complex64::new(v, 0.0);
    if let Some(number) = scalar(value) {
        return Some(vec![real(number)]);
    }
    match value {
        Value::Scalar(ScalarValue::Complex64(v)) => Some(vec![*v]),
        Value::Array(ArrayValue::Float64(a)) => Some(a.iter().map(|&v| real(v)).collect()),
        Value::Array(ArrayValue::Int32(a)) => Some(a.iter().map(|&v| real(f64::from(v))).collect()),
        Value::Array(ArrayValue::Complex64(a)) => Some(a.iter().copied().collect()),
        _ => None,
    }
}
fn replace(record: &mut RecordValue, key: &str, value: Value) {
    if let Some(old) = record.get_mut(key) {
        *old = value;
    } else {
        record.push(RecordField::new(key, value));
    }
}
fn flux(input: &RecordValue, lines: &mut Vec<LogLine>) -> Result<RecordValue, String> {
    let basis=match input.get("polarisation") {
        None=>{
            lines.push(LogLine::warning("The flux does not have a 'polarisation' field.\nUsing the default of Stokes"));
            "Stokes"
        }
        Some(Value::Scalar(ScalarValue::String(name)))=>match casa_components::Polarisation::parse_casa(name) {
            Ok(basis)=>basis.casa_name(),
            Err(_)=>return Err("The polarisation type is not known.\nAllowed values are 'Stokes', 'Linear' & 'Circular'\n".into()),
        },
        _=>return Err("The 'polarisation' field must be a record\n".into()),
    };
    let illegal =
        "Illegal Quantum record in QuantumHolder::fromRecord\nCould not parse the flux record\n";
    let unit = text(input, "unit").ok_or(illegal)?;
    let values = input
        .get("value")
        .and_then(numbers)
        .filter(|v| !v.is_empty())
        .ok_or(illegal)?;
    let scalar_value = matches!(input.get("value"), Some(Value::Scalar(_))) || values.len() == 1;
    let values: [Complex64; 4] = if scalar_value && basis == "Stokes" {
        if values[0].im.abs() > f64::MIN_POSITIVE {
            return Err("I value cannot be complex\n".into());
        }
        lines.push(LogLine::warning(
            "Only the I flux specified. Assuming Q, U, & V are zero\n",
        ));
        [
            Complex64::new(values[0].re, 0.0),
            Complex64::default(),
            Complex64::default(),
            Complex64::default(),
        ]
    } else {
        values.try_into().map_err(|_|"Must specify all 4 flux values\nif the polarisation representation is not Stokes\n")?
    };
    let errors = match input.get("error") {
        None => [Complex64::default(); 4],
        Some(value) => {
            let values = numbers(value)
                .filter(|v| v.len() == 4)
                .ok_or("The 'error' field must be a vector with 4 elements\n")?;
            let real = matches!(value, Value::Array(ArrayValue::Float64(_)));
            let complex = matches!(value, Value::Array(ArrayValue::Complex64(_)));
            if !(complex || real && basis == "Stokes" && input.get("polarisation").is_some()) {
                return Err("The 'error' field must be a complex vector with 4 elements\n".into());
            }
            values.try_into().expect("four errors")
        }
    };
    let parsed = casa_types::quanta::casacore::casacore_unit(unit).map_err(|e| e.to_string())?;
    if !parsed.conformant(&Unit::new("Jy").expect("Jy"))
        && !parsed.conformant(&Unit::new("K.rad.rad").expect("K.rad.rad"))
    {
        return Err("The dimensions of the units must be same as the Jy\n".into());
    }
    let flux = Flux::new(
        values,
        unit,
        casa_components::Polarisation::parse_casa(basis).expect("validated basis"),
    )
    .map_err(|e| e.to_string())?
    .with_errors(errors);
    Ok(flux.to_record())
}

pub(super) fn angular(record: &RecordValue, key: &str) -> Result<Quantity, String> {
    let value = record
        .get(key)
        .ok_or_else(|| format!("The '{key}' field does not exist\n"))?;
    let quantity=match value {
        Value::Scalar(ScalarValue::String(text))=>Quantity::read(text).map_err(|_|format!("in QuantumHolder::fromString with input string \"{text}\": Illegal input units or format\nProblem parsing the '{key}' string\n"))?,
        Value::Record(record)=>{
            let unit = text(record, "unit");
            let value = record.get("value").and_then(|value| match value {
                Value::Array(ArrayValue::Float64(values)) if values.len() == 1 => values.iter().copied().next(),
                Value::Array(ArrayValue::Float32(values)) if values.len() == 1 => values.iter().map(|&n| f64::from(n)).next(),
                Value::Array(ArrayValue::Int32(values)) if values.len() == 1 => values.iter().map(|&n| f64::from(n)).next(),
                scalar_value => scalar(scalar_value),
            });
            match (unit,value) {
                (Some(unit),Some(value))=>Quantity::new(value,unit).map_err(|e|e.to_string())?,
                _=>return Err(format!("Illegal Quantum record in QuantumHolder::fromRecord\nProblem parsing the '{key}' record\n")),
            }
        }
        _=>return Err(format!("The '{key}' field must be a record\nor a string (but not a vector of strings)\n")),
    };
    if !quantity.unit().conformant(&Unit::new("rad").expect("rad")) {
        return Err(format!("The '{key}' field must have angular units\n"));
    }
    Ok(quantity)
}
fn quantum(q: &Quantity) -> Value {
    Value::Record(RecordValue::new(vec![
        RecordField::new(
            "unit",
            Value::Scalar(ScalarValue::String(q.unit().name().into())),
        ),
        RecordField::new("value", Value::Scalar(ScalarValue::Float64(q.value()))),
    ]))
}
fn shape(
    input: &RecordValue,
    lines: &mut Vec<LogLine>,
) -> Result<(RecordValue, Option<[Unit; 6]>), String> {
    let unknown = "Cannot create a component with a 'Unknown' shape\n";
    let name = match input.get("type") {
        None => {
            return Err(format!(
                "The 'shape' record does not have a 'type' field.\n{unknown}"
            ));
        }
        Some(Value::Scalar(ScalarValue::String(name))) => name,
        _ => {
            return Err(format!(
                "The 'type' field, in the shape record, must be a String\n{unknown}"
            ));
        }
    };
    let kind = ShapeType::parse_casa(name).map_err(|_| unknown)?;
    let mut record = input.clone();
    let direction = match input.get("direction") {
        None => casa_types::measures::direction_to_record(&MDirection::from_angles(
            0.0,
            std::f64::consts::FRAC_PI_2,
            DirectionRef::J2000,
        )),
        Some(Value::Record(direction)) => {
            let mut direction = measures::normalize(direction, crate::convert::holder::MeasureKind::Direction, lines)
                .map_err(|e| format!("{e}Could not parse the reference direction\nProblem parsing the 'shape' field\n"))?;
            if let Some(error) = direction.get("error") {
                let Value::Record(error) = error else {
                    return Err(
                        "The 'error' field must be a record\nProblem parsing the 'shape' field\n"
                            .into(),
                    );
                };
                let mut errors = error.clone();
                for name in ["latitude", "longitude"] {
                    let quantity = angular(error, name)?;
                    if quantity.value() < 0.0 {
                        return Err(
                            "Exception: The errors must be non-negative angular quantities.".into(),
                        );
                    }
                    replace(&mut errors, name, quantum(&quantity));
                }
                replace(&mut direction, "error", Value::Record(errors));
            }
            direction
        }
        _ => {
            return Err(
                "The 'direction' field must be a record\nProblem parsing the 'shape' field\n"
                    .into(),
            );
        }
    };
    replace(&mut record, "direction", Value::Record(direction));
    if kind == ShapeType::Point {
        return Ok((record, None));
    }
    let names = [
        "majoraxis",
        "minoraxis",
        "positionangle",
        "majoraxiserror",
        "minoraxiserror",
        "positionangleerror",
    ];
    let mut units = Vec::with_capacity(6);
    for (index, name) in names.iter().enumerate() {
        let q = angular(input, name).map_err(|e| {
            format!(
                "{e}{}\nProblem parsing the 'shape' field\n",
                if index < 3 {
                    "Shape not changed"
                } else {
                    "Shape errors not changed"
                }
            )
        })?;
        if index >= 3 && q.value() < 0.0 {
            return Err("The errors must be non-negative angular quantities.".into());
        }
        units.push(q.unit().clone());
        replace(&mut record, name, quantum(&q));
    }
    Ok((record, Some(units.try_into().expect("six units"))))
}
fn spectrum(input: &RecordValue, lines: &mut Vec<LogLine>) -> Result<RecordValue, String> {
    let unknown = "Cannot create a component with a 'Unknown' spectrum\n";
    let name = match input.get("type") {
        None => {
            return Err(format!(
                "\nThe record does not have a 'type' field.{unknown}"
            ));
        }
        Some(Value::Scalar(ScalarValue::String(name))) => name,
        _ => {
            return Err(format!(
                "\nThe 'type' field in the spectrum record,must be a String{unknown}"
            ));
        }
    };
    let kind = casa_components::SpectralType::parse_casa(name).map_err(|_| unknown)?;
    let mut record = input.clone();
    match input.get("frequency") {
        None=>return Err("The 'frequency' field does not exist\n".into()),
        Some(Value::Scalar(ScalarValue::String(value))) if value=="current"=>{
            replace(&mut record,"frequency",Value::Record(casa_types::measures::frequency_to_record(&MFrequency::new(1e9,FrequencyRef::LSRK))));
        }
        Some(Value::Record(frequency))=>{
            let frequency = measures::normalize(frequency, crate::convert::holder::MeasureKind::Frequency, lines)
                .map_err(|e| format!("{e}Could not parse the reference frequency\n"))?;
            replace(&mut record,"frequency",Value::Record(frequency));
        },
        _=>return Err("The 'frequency' field must be a record\nor the string 'current' (to use the current value)\n".into()),
    }
    spectrum::validate(&record, kind)?;
    Ok(record)
}

/// C++ throws these errors directly. Each caller adds its own context to
/// failures returned by `fromRecord`.
pub(super) fn is_direct_import_error(message: &str) -> bool {
    message.starts_with("RecordRep::")
        || message.starts_with("RecordInterface::")
        || message.starts_with("Unit::check ")
        || message.ends_with("The errors must be non-negative angular quantities.")
}

pub(super) fn import_error(message: String, adding: bool) -> ConvertError {
    if is_direct_import_error(&message) {
        ConvertError::runtime(message)
    } else if adding {
        ConvertError::runtime(format!(
            "Converting Component from record failed with error {message}"
        ))
    } else {
        ConvertError::runtime(format!("Error {message} in converting from record"))
    }
}
