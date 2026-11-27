// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read the embedded component records used by CASA's virtual sky model.

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::measures::{direction_from_record, frequency_from_record};
use casa_core::quanta::{Quantity, Unit};
use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
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
        Self::decode_record(record, component)
    }

    /// Decode a supplied component list using the defaults of CASA `cl.fromrecord`.
    ///
    /// # Errors
    /// A supplied component field is malformed or cannot be represented.
    pub fn from_import_record(record: &RecordValue) -> Result<Self, ComponentError> {
        let reference =
            MDirection::from_angles(0., std::f64::consts::FRAC_PI_2, DirectionRef::J2000);
        let default = SkyComponent::new(
            Flux::stokes_jy(1., 0., 0., 0.),
            ComponentShape::point(reference),
            SpectralModel::new(
                MFrequency::new(1e9, FrequencyRef::LSRK),
                SpectralKind::Constant,
            ),
        )
        .to_record()?;
        Self::decode_record(record, |input| import_component(input, &default))
    }

    fn decode_record(
        record: &RecordValue,
        decode: impl Fn(&RecordValue) -> Result<SkyComponent, ComponentError>,
    ) -> Result<Self, ComponentError> {
        let count = match record.get("nelements") {
            Some(Value::Scalar(ScalarValue::Int32(count))) => usize::try_from(*count).ok(),
            Some(Value::Scalar(ScalarValue::Int64(count))) => usize::try_from(*count).ok(),
            Some(Value::Scalar(ScalarValue::UInt32(count))) => usize::try_from(*count).ok(),
            _ => None,
        }
        .ok_or_else(|| invalid("nelements"))?;
        let mut list = Self::new();
        for index in 0..count {
            list.push(decode(subrecord(record, &format!("component{index}"))?)?);
        }
        Ok(list)
    }
}

fn import_component(
    input: &RecordValue,
    default: &RecordValue,
) -> Result<SkyComponent, ComponentError> {
    let mut record = input.clone();
    for name in ["flux", "shape", "spectrum"] {
        if record.get(name).is_none() {
            record.upsert(
                name,
                default.get(name).expect("default component field").clone(),
            );
        }
    }
    let Some(Value::Record(flux)) = record.get_mut("flux") else {
        return Err(invalid("flux"));
    };
    let basis = match flux.get("polarisation") {
        Some(_) => Polarisation::parse_casa(string(flux, "polarisation")?)?,
        None => {
            flux.upsert(
                "polarisation",
                Value::Scalar(ScalarValue::String("Stokes".into())),
            );
            Polarisation::Stokes
        }
    };
    let flux_value = flux.get("value").ok_or_else(|| invalid("flux.value"))?;
    let scalar = match flux_value {
        Value::Array(ArrayValue::Float64(values)) if values.len() == 1 => {
            values.iter().next().copied()
        }
        _ => number(flux, "value").ok(),
    };
    if basis == Polarisation::Stokes
        && let Some(intensity) = scalar
    {
        flux.upsert(
            "value",
            Value::Array(ArrayValue::from_f64_vec(vec![intensity, 0., 0., 0.])),
        );
    }
    if let Some(error) = flux.get("error") {
        let valid_type = matches!(error, Value::Array(ArrayValue::Complex64(_)))
            || (basis == Polarisation::Stokes
                && matches!(error, Value::Array(ArrayValue::Float64(_))));
        if !valid_type || complex_values(Some(error)).is_none() {
            return Err(invalid("flux.error"));
        }
    }
    let Some(Value::Record(shape)) = record.get_mut("shape") else {
        return Err(invalid("shape"));
    };
    if shape.get("direction").is_none() {
        shape.upsert(
            "direction",
            subrecord(default, "shape")?
                .get("direction")
                .expect("default direction")
                .clone(),
        );
    }
    let Some(Value::Record(spectrum)) = record.get_mut("spectrum") else {
        return Err(invalid("spectrum"));
    };
    if spectrum.get("frequency").is_none() {
        spectrum.upsert(
            "frequency",
            subrecord(default, "spectrum")?
                .get("frequency")
                .expect("default frequency")
                .clone(),
        );
    }
    if SpectralType::parse_casa(string(spectrum, "type")?)? == SpectralType::SpectralIndex {
        number(spectrum, "index")?;
    }
    if record.get("label").is_some() {
        string(&record, "label")?;
    }
    component(&record)
}

impl SkyComponent {
    /// Decode a complete CASA component record, including its flux basis,
    /// shape and direction errors, spectrum, label and optional parameters.
    ///
    /// This reads the records produced by [`Self::to_record`]. Tool-specific
    /// defaults and diagnostics belong to the calling tool.
    ///
    /// # Errors
    /// Missing fields or values the component model cannot represent.
    pub fn from_record(record: &RecordValue) -> Result<Self, ComponentError> {
        component(record)
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
    let mut frequency =
        frequency_from_record(frequency_record).map_err(|error| invalid(error.to_string()))?;
    let mut kind = SpectralKind::casa_default(SpectralType::parse_casa(string(record, "type")?)?);
    crate::table::apply_spectral_record(&mut kind, record)?;
    if matches!(kind, SpectralKind::Tabular(_)) {
        let frame = frequency_from_record(subrecord(record, "freqRef")?)
            .map_err(|error| invalid(error.to_string()))?
            .refer();
        let hz = number(record, "referenceFreq")?;
        number(record, "minFreq")?;
        number(record, "maxFreq")?;
        frequency = casa_core::measures::frequency::MFrequency::new(hz, frame);
    }
    let mut model = SpectralModel::new(frequency, kind);
    let unit = string(subrecord(frequency_record, "m0")?, "unit")?;
    let unit = Unit::new(unit).map_err(|error| invalid(error.to_string()))?;
    if unit.conformant(&Unit::new("Hz").expect("Hz")) {
        model.set_frequency_unit(unit)?;
    }
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

fn number(record: &RecordValue, name: &str) -> Result<f64, ComponentError> {
    match record.get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
        Some(Value::Scalar(ScalarValue::Float32(value))) => Ok(f64::from(*value)),
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(f64::from(*value)),
        _ => Err(invalid(name)),
    }
}

fn quantity(record: &RecordValue) -> Result<Quantity, ComponentError> {
    Quantity::new(number(record, "value")?, string(record, "unit")?)
        .map_err(|error| invalid(error.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use casa_core::measures::direction::{DirectionRef, MDirection};
    use casa_core::measures::frequency::{FrequencyRef, MFrequency};

    #[test]
    fn component_record_frequency_honors_its_unit() {
        let mut model = SpectralModel::new(
            MFrequency::new(1.5e9, FrequencyRef::LSRK),
            SpectralKind::Constant,
        );
        model.set_frequency_unit(Unit::new("GHz").unwrap()).unwrap();
        let original = SkyComponent::new(
            Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
            ComponentShape::point(MDirection::from_angles(0.0, 0.0, DirectionRef::J2000)),
            model,
        );
        let decoded = SkyComponent::from_record(&original.to_record().unwrap()).unwrap();
        assert_eq!(decoded.spectrum().reference_frequency().hz(), 1.5e9);
    }

    use casa_core::RecordField;

    #[test]
    fn resolves_component_record_offsets_like_casa_6_7_6_14() {
        let component = SkyComponent::new(
            Flux::stokes_jy(1.0, 0.0, 0.0, 0.0),
            ComponentShape::point(MDirection::from_angles(
                10_f64.to_radians(),
                20_f64.to_radians(),
                DirectionRef::J2000,
            )),
            SpectralModel::new(
                MFrequency::new(1.4e9, FrequencyRef::LSRK),
                SpectralKind::Constant,
            ),
        );
        let mut record = component.to_record().unwrap();
        let Value::Record(shape) = record.get_mut("shape").unwrap() else {
            unreachable!()
        };
        let Value::Record(direction) = shape.get_mut("direction").unwrap() else {
            unreachable!()
        };
        direction.push(RecordField::new(
            "offset",
            Value::Record(casa_core::measures::direction_to_record(
                &MDirection::from_angles(
                    1_f64.to_radians(),
                    2_f64.to_radians(),
                    DirectionRef::J2000,
                ),
            )),
        ));
        let Value::Record(spectrum) = record.get_mut("spectrum").unwrap() else {
            unreachable!()
        };
        let Value::Record(frequency) = spectrum.get_mut("frequency").unwrap() else {
            unreachable!()
        };
        frequency.push(RecordField::new(
            "offset",
            Value::Record(casa_core::measures::frequency_to_record(&MFrequency::new(
                1e9,
                FrequencyRef::LSRK,
            ))),
        ));
        let decoded = SkyComponent::from_record(&record).unwrap();
        // Independent CASA cl.add(record), rename/done/open values, preserved
        // in the Python componentlist offset fixture.
        let (longitude, latitude) = decoded.shape().reference_direction().as_angles();
        assert!((longitude - 0.093_570_138_597_165_76).abs() < 1e-15);
        assert!((latitude - 0.192_564_780_043_098_24).abs() < 1e-15);
        assert_eq!(decoded.spectrum().reference_frequency().hz(), 2.4e9);
    }
}
