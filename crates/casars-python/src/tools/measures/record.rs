// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scalar/vector measure records, retaining reference offsets separately.
use super::State;
use crate::convert::holder::{HolderError, Measure, MeasureValue};
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant};
use std::collections::BTreeMap;

pub(super) struct Measures {
    pub(super) values: Vec<Measure>,
    pub(super) vector: bool,
    pub(super) warning: Option<String>,
    pub(super) offset: Option<Box<Self>>,
}

impl Measures {
    pub(super) fn read(record: &RecordArg) -> Result<Self, HolderError> {
        Self::read_fields(&record.0, 0)
    }
    fn read_fields(fields: &BTreeMap<String, Variant>, depth: usize) -> Result<Self, HolderError> {
        if depth > 128 {
            return Err(HolderError::Invalid(
                "measure offsets are too deeply nested (#1252)".into(),
            ));
        }
        let mut fields = fields.clone();
        let offset = match fields.remove("offset") {
            Some(Variant::Record(offset)) => Some(Box::new(Self::read_fields(&offset, depth + 1)?)),
            Some(_) => {
                return Err(HolderError::Invalid(
                    "Illegal offset in MeasureHolder::fromRecord\n".into(),
                ));
            }
            None => None,
        };
        let mut length = None;
        let mut arrays = Vec::new();
        for (index, key) in ["m0", "m1", "m2"].into_iter().enumerate() {
            let Some(Variant::Record(quantity)) = fields.get_mut(key) else {
                break;
            };
            if let Some(Variant::Numbers(v) | Variant::Array { values: v, .. }) =
                quantity.get("value")
            {
                if length.is_some_and(|n| n != v.len()) {
                    return Err(HolderError::Invalid(
                        "Inconsistent measure array lengths in MeasureHolder::fromRecord\n".into(),
                    ));
                }
                if v.is_empty() {
                    return Err(HolderError::Invalid(
                        "empty measure array refused (#1252)".into(),
                    ));
                }
                length = Some(v.len());
                let data = v.clone();
                let mut scalar = (**quantity).clone();
                scalar.insert("value".into(), Variant::Number(data[0]));
                *quantity = scalar.into();
                arrays.push((index, data));
            }
        }
        let mut definition = Measure::record_definition(&fields)?;
        let mut values = Vec::with_capacity(length.unwrap_or(1));
        for i in 0..length.unwrap_or(1) {
            for (index, array) in &arrays {
                let q = &mut definition.quantities[*index];
                *q = casa_types::quanta::Quantity::with_unit(array[i], q.unit().clone());
            }
            values.push(Measure::from_values(
                definition.kind,
                definition.refer,
                &definition.quantities,
            )?);
        }
        let warning = definition.warning;
        if offset
            .as_ref()
            .is_some_and(|offset| offset.values[0].kind() != values[0].kind())
        {
            return Err(HolderError::Invalid(
                "Unmatched offset type in MeasureHolder::fromRecord\n".into(),
            ));
        }
        Ok(Self {
            values,
            vector: length.is_some_and(|n| n > 1),
            warning,
            offset,
        })
    }

    pub(super) fn scalar(value: Measure) -> Self {
        Self {
            values: vec![value],
            vector: false,
            warning: None,
            offset: None,
        }
    }
    pub(super) fn map(
        &self,
        mut f: impl FnMut(Measure) -> Result<Measure, String>,
    ) -> Result<Self, String> {
        Ok(Self {
            values: self
                .values
                .iter()
                .copied()
                .map(&mut f)
                .collect::<Result<_, _>>()?,
            vector: self.vector,
            warning: None,
            offset: None,
        })
    }

    pub(super) fn to_value(&self) -> CasaValue {
        let first = self.values[0];
        let mut out = if self.vector {
            let components = self
                .values
                .iter()
                .map(|v| v.quantity_values())
                .collect::<Vec<_>>();
            let mut fields = vec![
                (
                    "type".to_owned(),
                    CasaValue::Text(first.kind().record_type().to_owned()),
                ),
                ("refer".to_owned(), CasaValue::Text(first.refer.to_owned())),
            ];
            for (i, &unit) in first.quantity_units().iter().enumerate() {
                fields.push((
                    format!("m{i}"),
                    CasaValue::record([
                        ("unit", CasaValue::Text(unit.to_owned())),
                        (
                            "value",
                            CasaValue::Floats {
                                shape: vec![self.values.len()],
                                values: components.iter().map(|v| v[i]).collect(),
                            },
                        ),
                    ]),
                ));
            }
            CasaValue::record(fields)
        } else {
            first.to_value()
        };
        if let Some(offset) = &self.offset {
            let CasaValue::Record(fields) = &mut out else {
                unreachable!()
            };
            fields.push(("offset".into(), offset.to_value()));
            fields.sort_by(|a, b| a.0.cmp(&b.0));
        }
        out
    }

    pub(super) fn resolve(
        &self,
        state: &mut State,
        target: &'static str,
    ) -> pyo3::PyResult<Result<Self, String>> {
        let offset = if let Some(offset) = &self.offset {
            match offset.resolve(state, self.values[0].refer)? {
                Ok(offset) => Some(offset),
                Err(error) => return Ok(Err(error)),
            }
        } else {
            None
        };
        let mut converted = Vec::with_capacity(self.values.len());
        for (i, value) in self.values.iter().enumerate() {
            let value = if let Some(offset) = &offset {
                match add(*value, offset.values[i % offset.values.len()]) {
                    Ok(value) => value,
                    Err(error) => return Ok(Err(error)),
                }
            } else {
                *value
            };
            match super::frame::convert(state, &value, target)? {
                Ok(value) => converted.push(value),
                Err(error) => return Ok(Err(error)),
            }
        }
        Ok(Ok(Self {
            values: converted,
            vector: self.vector,
            warning: None,
            offset: None,
        }))
    }
}

fn add(mut base: Measure, offset: Measure) -> Result<Measure, String> {
    base.value = match (base.value, offset.value) {
        (MeasureValue::Frequency(a), MeasureValue::Frequency(b)) => MeasureValue::Frequency(a + b),
        (MeasureValue::RadialVelocity(a), MeasureValue::RadialVelocity(b)) => {
            MeasureValue::RadialVelocity(a + b)
        }
        (MeasureValue::Doppler(a), MeasureValue::Doppler(b)) => MeasureValue::Doppler(a + b),
        (MeasureValue::Epoch(a), MeasureValue::Epoch(b)) => MeasureValue::Epoch(
            casa_types::measures::MjdHighPrec::new(a.day() + b.day(), a.frac() + b.frac()),
        ),
        (MeasureValue::Position(a), MeasureValue::Position(b)) => {
            MeasureValue::Position(std::array::from_fn(|i| a[i] + b[i]))
        }
        (MeasureValue::Direction(a), MeasureValue::Direction(b)) => {
            let v: [f64; 3] = std::array::from_fn(|i| a[i] + b[i]);
            let norm = v.iter().map(|x| x * x).sum::<f64>().sqrt();
            MeasureValue::Direction(v.map(|x| x / norm))
        }
        _ => return Err("Unmatched offset type in MeasureHolder::fromRecord".into()),
    };
    Ok(base)
}
