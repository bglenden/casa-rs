// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scalar/vector measure records, retaining reference offsets separately.
use super::State;
use crate::convert::holder::{HolderError, Measure, MeasureValue};
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
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

    pub(super) fn read_variant(
        record: &crate::convert::VariantRecord,
    ) -> Result<Self, HolderError> {
        Self::read_fields(record, 0)
    }
    fn read_fields(fields: &BTreeMap<String, Variant>, depth: usize) -> Result<Self, HolderError> {
        if depth > 128 {
            return Err(HolderError::Invalid(
                "measure offsets are too deeply nested (#1252)".into(),
            ));
        }
        let mut fields = fields.clone();
        let offset = match fields.remove("offset") {
            Some(Variant::Record(offset)) => Some(Box::new(
                Self::read_fields(&offset, depth + 1)?.into_reference_origin(),
            )),
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
            let n = match quantity.get("value") {
                Some(Variant::Numbers(v) | Variant::Array { values: v, .. }) => v.len(),
                _ => 1,
            };
            if length.is_some_and(|nvalues| nvalues != n) {
                return Err(HolderError::Invalid(format!(
                    "Illegal number of values in MeasureHolder m{index}\n"
                )));
            }
            length = Some(n);
            if let Some(Variant::Numbers(v) | Variant::Array { values: v, .. }) =
                quantity.get("value")
            {
                if v.is_empty() || !quantity.is_double_vector() {
                    return Err(HolderError::Invalid(
                        "Illegal quantity in MeasureHolder::fromRecord\n".into(),
                    ));
                }
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

    /// A reference offset and a frame element use the first MeasureHolder value.
    pub(super) fn into_reference_origin(mut self) -> Self {
        self.values.truncate(1);
        self.vector = false;
        self.offset = self.offset.map(|v| Box::new(v.into_reference_origin()));
        self
    }

    /// Materialize a radial-velocity frame origin against the real frame.
    /// Keep its offset until this point so doframe can precede other elements.
    pub(super) fn radial_velocity(
        &self,
        frame: &casa_types::measures::MeasFrame,
    ) -> Result<casa_types::measures::radial_velocity::MRadialVelocity, String> {
        use casa_types::measures::radial_velocity::MRadialVelocity;
        let first = self.values[0];
        let MeasureValue::RadialVelocity(mut ms) = first.value else {
            return Err("Non-radialvelocity frame element".into());
        };
        let refer = first
            .refer
            .parse()
            .map_err(|e: casa_types::measures::MeasureError| e.to_string())?;
        if let Some(offset) = &self.offset {
            ms += offset
                .radial_velocity(frame)?
                .convert_to(refer, frame)
                .map_err(|e| e.to_string())?
                .ms();
        }
        Ok(MRadialVelocity::new(ms, refer))
    }

    /// Express absolute values relative to one output reference origin.
    pub(super) fn relative_to(
        mut self,
        origin: Self,
        state: &mut State,
    ) -> pyo3::PyResult<Result<Self, String>> {
        let origin = origin.into_reference_origin();
        let resolved = match origin.resolve(state, self.values[0].refer)? {
            Ok(values) => values.values[0],
            Err(error) => return Ok(Err(error)),
        };
        let negative = match resolved.value {
            MeasureValue::Baseline(v) => MeasureValue::Baseline(v.map(|x| -x)),
            MeasureValue::Uvw(v) => MeasureValue::Uvw(v.map(|x| -x)),
            _ => return Ok(Err("Non-conforming offset measure type".into())),
        };
        for value in &mut self.values {
            *value = match add(
                *value,
                Measure {
                    value: negative,
                    ..resolved
                },
            ) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
        }
        self.offset = Some(Box::new(origin));
        Ok(Ok(self))
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
        let plan = if self.values[0].refer != target
            && matches!(
                self.values[0].value,
                MeasureValue::Baseline(_) | MeasureValue::Uvw(_) | MeasureValue::EarthMagnetic(_)
            ) {
            match super::geometry::Conversion::new(state, self.values[0], target)? {
                Ok(plan) => Some(plan),
                Err(error) => return Ok(Err(error)),
            }
        } else {
            None
        };
        let mut converted = Vec::with_capacity(self.values.len());
        for value in &self.values {
            let value = if let Some(offset) = &offset {
                match add(*value, offset.values[0]) {
                    Ok(value) => value,
                    Err(error) => return Ok(Err(error)),
                }
            } else {
                *value
            };
            let result = if let Some(plan) = &plan {
                plan.apply(value)
            } else {
                super::frame::convert(state, &value, target)?
            };
            match result {
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
        (MeasureValue::Baseline(a), MeasureValue::Baseline(b)) => {
            MeasureValue::Baseline(std::array::from_fn(|i| a[i] + b[i]))
        }
        (MeasureValue::Uvw(a), MeasureValue::Uvw(b)) => {
            MeasureValue::Uvw(std::array::from_fn(|i| a[i] + b[i]))
        }
        (MeasureValue::EarthMagnetic(a), MeasureValue::EarthMagnetic(b)) => {
            MeasureValue::EarthMagnetic(std::array::from_fn(|i| a[i] + b[i]))
        }
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
            MeasureValue::Direction(if norm == 0.0 {
                [0.0, 0.0, 1.0]
            } else {
                v.map(|x| x / norm)
            })
        }
        _ => return Err("Unmatched offset type in MeasureHolder::fromRecord".into()),
    };
    Ok(base)
}
