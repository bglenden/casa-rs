// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the embedded component records used by CASA's virtual sky model.

use casa_types::measures::{direction_from_record, frequency_from_record};
use casa_types::quanta::{Quantity, Unit};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use num_complex::Complex64;

use crate::component::SkyComponent;
use crate::component_type::{Polarisation, ShapeType, SpectralType};
use crate::error::ComponentError;
use crate::flux::Flux;
use crate::list::ComponentList;
use crate::shape::{ComponentShape, DirectionError};
use crate::spectrum::{SpectralKind, SpectralModel};

impl ComponentList {
    /// Decode the `nelements` and `componentN` records written by CASA's
    /// `ComponentList::toRecord` and stored in virtual models.
    ///
    /// # Errors
    /// Missing fields or component values that CASA's model cannot represent.
    pub fn from_record(record: &RecordValue) -> Result<Self, ComponentError> {
        let count = match record.get("nelements") {
            Some(Value::Scalar(ScalarValue::Int32(count))) => usize::try_from(*count).ok(),
            Some(Value::Scalar(ScalarValue::Int64(count))) => usize::try_from(*count).ok(),
            Some(Value::Scalar(ScalarValue::UInt32(count))) => usize::try_from(*count).ok(),
            _ => None,
        }
        .ok_or_else(|| invalid("nelements"))?;
        let mut list = Self::new();
        for index in 0..count {
            list.push(component(subrecord(record, &format!("component{index}"))?)?);
        }
        Ok(list)
    }
}

fn component(record: &RecordValue) -> Result<SkyComponent, ComponentError> {
    let flux = subrecord(record, "flux")?;
    let values = complex_values(flux.get("value")).ok_or_else(|| invalid("flux.value"))?;
    let basis = Polarisation::parse_casa(string(flux, "polarisation")?)?;
    let mut flux_model = Flux::new(values, string(flux, "unit")?, basis)?;
    if let Some(errors) = complex_values(flux.get("error")) {
        flux_model.set_errors(errors);
    }
    let mut component = SkyComponent::new(
        flux_model,
        shape(subrecord(record, "shape")?)?,
        spectrum(subrecord(record, "spectrum")?)?,
    );
    if let Ok(label) = string(record, "label") {
        component = component.with_label(label);
    }
    if let Some(Value::Array(ArrayValue::Float64(parameters))) = record.get("optionalParameters") {
        component = component.with_optional_parameters(parameters.iter().copied().collect());
    }
    Ok(component)
}

fn shape(record: &RecordValue) -> Result<ComponentShape, ComponentError> {
    let shape_type = ShapeType::parse_casa(string(record, "type")?)?;
    let direction_record = subrecord(record, "direction")?;
    let direction =
        direction_from_record(direction_record).map_err(|error| invalid(error.to_string()))?;
    let radians = Unit::new("rad").expect("rad is a unit");
    let angle = |name| {
        quantity(subrecord(record, name)?)?
            .get_value_in(&radians)
            .map_err(|error| invalid(error.to_string()))
    };
    let mut shape = if shape_type == ShapeType::Point {
        ComponentShape::point(direction)
    } else {
        let axes = [
            angle("majoraxis")?,
            angle("minoraxis")?,
            angle("positionangle")?,
        ];
        let names = ["majoraxiserror", "minoraxiserror", "positionangleerror"];
        let errors = names.map(|name| {
            if record.get(name).is_some() {
                angle(name)
            } else {
                Ok(0.0)
            }
        });
        let [major, minor, position] = errors;
        ComponentShape::from_parameters(
            shape_type,
            direction,
            &axes,
            Some(&[major?, minor?, position?]),
        )?
    };
    if let Ok(error) = subrecord(direction_record, "error") {
        shape.set_direction_error(DirectionError::new(
            quantity(subrecord(error, "latitude")?)?,
            quantity(subrecord(error, "longitude")?)?,
        )?);
    }
    Ok(shape)
}

fn spectrum(record: &RecordValue) -> Result<SpectralModel, ComponentError> {
    let frequency_record = subrecord(record, "frequency")?;
    let frequency =
        frequency_from_record(frequency_record).map_err(|error| invalid(error.to_string()))?;
    let mut kind = SpectralKind::casa_default(SpectralType::parse_casa(string(record, "type")?)?);
    crate::table::apply_spectral_record(&mut kind, record)?;
    let mut model = SpectralModel::new(frequency, kind);
    let unit = string(subrecord(frequency_record, "m0")?, "unit")?;
    model.set_frequency_unit(Unit::new(unit).map_err(|error| invalid(error.to_string()))?)?;
    Ok(model)
}

fn subrecord<'a>(record: &'a RecordValue, name: &str) -> Result<&'a RecordValue, ComponentError> {
    match record.get(name) {
        Some(Value::Record(value)) => Ok(value),
        _ => Err(invalid(name)),
    }
}

fn string<'a>(record: &'a RecordValue, name: &str) -> Result<&'a str, ComponentError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::String(value))) => Ok(value),
        _ => Err(invalid(name)),
    }
}

fn quantity(record: &RecordValue) -> Result<Quantity, ComponentError> {
    let value = match record.get("value") {
        Some(Value::Scalar(ScalarValue::Float64(value))) => *value,
        Some(Value::Scalar(ScalarValue::Float32(value))) => f64::from(*value),
        _ => return Err(invalid("quantity.value")),
    };
    Quantity::new(value, string(record, "unit")?).map_err(|error| invalid(error.to_string()))
}

fn complex_values(value: Option<&Value>) -> Option<[Complex64; 4]> {
    let data = match value? {
        Value::Array(ArrayValue::Float64(values)) => values
            .iter()
            .map(|&value| Complex64::new(value, 0.0))
            .collect::<Vec<_>>(),
        Value::Array(ArrayValue::Float32(values)) => values
            .iter()
            .map(|&value| Complex64::new(f64::from(value), 0.0))
            .collect(),
        Value::Array(ArrayValue::Complex64(values)) => values.iter().copied().collect(),
        Value::Array(ArrayValue::Complex32(values)) => values
            .iter()
            .map(|value| Complex64::new(f64::from(value.re), f64::from(value.im)))
            .collect(),
        _ => return None,
    };
    data.try_into().ok()
}

fn invalid(field: impl Into<String>) -> ComponentError {
    ComponentError::Record(field.into())
}
