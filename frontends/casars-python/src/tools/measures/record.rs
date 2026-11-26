// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scalar/vector measure records, retaining reference offsets separately.
use super::State;
use crate::convert::holder::{CasaMeasure, HolderError, Measure, MeasureValue};
use crate::convert::value::CasaValue;
use crate::convert::{RecordArg, Variant};
use crate::tools::LogLine;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::{MEpoch, MeasFrame, MeasureError};
use std::collections::BTreeMap;

/// Decodes the scalar epoch and its offsets stored by `ObsInfo`.
pub(in crate::tools) fn observation_epoch(
    record: &RecordArg,
    log: &mut Vec<LogLine>,
) -> Result<Option<MEpoch>, &'static str> {
    for key in ["m0", "m1", "m2"] {
        let Some(Variant::Record(quantity)) = record.0.get(key) else {
            break;
        };
        if let Some(Variant::Numbers(values) | Variant::Array { values, .. }) =
            quantity.get("value")
            && !values.is_empty()
            && !quantity.is_double_vector()
        {
            return Err("Cannot convert to QuantumVectorDouble");
        }
    }
    let measures = match Measures::read(record) {
        Ok(measures) => measures,
        Err(error) => {
            log.push(LogLine::error(format!(
                "{}Non-measure type value in measure conversion",
                error.message(),
            )));
            return Ok(None);
        }
    };
    if let Some(warning) = &measures.warning {
        log.push(LogLine::warning(warning.clone()));
    }
    match measures.as_epoch() {
        Some(epoch) => Ok(Some(epoch)),
        None => {
            log.push(LogLine::error(
                "Record value does not contain an Epoch measure.",
            ));
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Measures {
    pub(super) values: Vec<Measure>,
    pub(super) vector: bool,
    pub(super) warning: Option<String>,
    pub(super) offset: Option<Box<Self>>,
}

impl Measures {
    fn as_epoch(&self) -> Option<MEpoch> {
        let CasaMeasure::Epoch(epoch) = self.values.first()?.to_casa().ok()? else {
            return None;
        };
        match &self.offset {
            Some(offset) => Some(epoch.with_offset(offset.as_epoch()?)),
            None => Some(epoch),
        }
    }

    pub(super) fn read(record: &RecordArg) -> Result<Self, HolderError> {
        Self::read_fields(&record.0, 0)
    }

    pub(super) fn read_variant(
        record: &crate::convert::VariantRecord,
    ) -> Result<Self, HolderError> {
        Self::read_fields(record, 0)
    }

    /// Decode the scalar direction and its reference offsets before the
    /// observing frame is attached to CASA's rise/set conversion.
    pub(super) fn direction_without_frame(
        &self,
        target: DirectionRef,
    ) -> Result<MDirection, String> {
        let mut value = self.values[0];
        if let Some(offset) = &self.offset {
            let refer = value
                .refer
                .parse()
                .map_err(|error: MeasureError| error.to_string())?;
            let origin = offset.direction_without_frame(refer)?;
            value = add(value, Measure::from_casa(&CasaMeasure::Direction(origin)))?;
        }
        let CasaMeasure::Direction(direction) = value.to_casa()? else {
            return Err("Non-direction source".into());
        };
        direction
            .convert_to(target, &MeasFrame::new())
            .map_err(|error| error.to_string())
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

    /// Express absolute values relative to one output reference origin.
    pub(super) fn relative_to(
        mut self,
        origin: Self,
        state: &mut State,
    ) -> pyo3::PyResult<Result<Self, String>> {
        if origin.values[0].kind() != self.values[0].kind() {
            return Ok(Err("Non-conforming offset measure type".into()));
        }
        let origin = origin.into_reference_origin();
        let resolved = match origin.resolve(state, self.values[0].refer)? {
            Ok(values) => values.values[0],
            Err(error) => return Ok(Err(error)),
        };
        for value in &mut self.values {
            *value = match offset_value(*value, resolved, -1.0) {
                Ok(value) => value,
                Err(error) => return Ok(Err(error)),
            };
        }
        self.offset = Some(Box::new(origin));
        Ok(Ok(self))
    }

    /// Convert absolute coordinates and, when supplied, subtract the output
    /// reference origin after the rotation or scale conversion.
    pub(super) fn resolve_relative(
        &self,
        state: &mut State,
        target: &'static str,
        origin: Option<Self>,
    ) -> pyo3::PyResult<Result<Self, String>> {
        if origin
            .as_ref()
            .is_some_and(|offset| offset.values[0].kind() != self.values[0].kind())
        {
            return Ok(Err("Non-conforming offset measure type".into()));
        }
        match (self.resolve(state, target)?, origin) {
            (Ok(converted), Some(origin)) => converted.relative_to(origin, state),
            (result, _) => Ok(result),
        }
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

fn add(base: Measure, offset: Measure) -> Result<Measure, String> {
    offset_value(base, offset, 1.0)
}

/// Materialize the first value of a measure and its nested origins in a
/// supplied frame without rebuilding that frame from the tool state.
pub(super) fn absolute_value(
    base: Measure,
    origin: Option<&Measures>,
    convert: &mut impl FnMut(Measure, &'static str) -> Result<Measure, String>,
) -> Result<Measure, String> {
    let Some(origin) = origin else {
        return Ok(base);
    };
    let absolute = absolute_value(origin.values[0], origin.offset.as_deref(), convert)?;
    add(base, convert(absolute, base.refer)?)
}

fn offset_value(mut base: Measure, offset: Measure, sign: f64) -> Result<Measure, String> {
    let vector = |a: [f64; 3], b: [f64; 3]| std::array::from_fn(|i| a[i] + sign * b[i]);
    base.value = match (base.value, offset.value) {
        (MeasureValue::Baseline(a), MeasureValue::Baseline(b)) => {
            MeasureValue::Baseline(vector(a, b))
        }
        (MeasureValue::Uvw(a), MeasureValue::Uvw(b)) => MeasureValue::Uvw(vector(a, b)),
        (MeasureValue::EarthMagnetic(a), MeasureValue::EarthMagnetic(b)) => {
            MeasureValue::EarthMagnetic(vector(a, b))
        }
        (MeasureValue::Frequency(a), MeasureValue::Frequency(b)) => {
            MeasureValue::Frequency(a + sign * b)
        }
        (MeasureValue::RadialVelocity(a), MeasureValue::RadialVelocity(b)) => {
            MeasureValue::RadialVelocity(a + sign * b)
        }
        (MeasureValue::Doppler(a), MeasureValue::Doppler(b)) => MeasureValue::Doppler(a + sign * b),
        (MeasureValue::Epoch(a), MeasureValue::Epoch(b)) => {
            MeasureValue::Epoch(casa_types::measures::MjdHighPrec::new(
                a.day() + sign * b.day(),
                a.frac() + sign * b.frac(),
            ))
        }
        (MeasureValue::Position(a), MeasureValue::Position(b)) => {
            MeasureValue::Position(vector(a, b))
        }
        (MeasureValue::Direction(a), MeasureValue::Direction(b)) => {
            let v = vector(a, b);
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
