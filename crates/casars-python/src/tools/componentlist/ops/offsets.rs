// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA keeps reference offsets in memory and writes absolute table values.
use super::State;
use crate::convert::value::CasaValue;
use casa_components::{ComponentList, SkyComponent};
use casa_types::{RecordField, RecordValue, Value};
use std::borrow::Cow;

#[derive(Default, Clone)]
pub(crate) struct Offsets {
    direction: Option<RecordValue>,
    frequency: Option<RecordValue>,
}

impl Offsets {
    pub(crate) fn is_empty(&self) -> bool {
        self.direction.is_none() && self.frequency.is_none()
    }

    pub(super) fn clear_direction(&mut self) {
        self.direction = None;
    }
    pub(super) fn clear_frequency(&mut self) {
        self.frequency = None;
    }

    pub(crate) fn extract(record: &mut RecordValue) -> Self {
        fn take(record: &mut RecordValue, parent: &str, name: &str) -> Option<RecordValue> {
            let Value::Record(parent) = record.get_mut(parent)? else {
                return None;
            };
            let Value::Record(measure) = parent.get_mut(name)? else {
                return None;
            };
            let offset = match measure.get("offset") {
                Some(Value::Record(r)) => Some(r.clone()),
                _ => None,
            };
            *measure = RecordValue::new(
                measure
                    .fields()
                    .iter()
                    .filter(|f| f.name != "offset")
                    .cloned()
                    .collect(),
            );
            offset
        }
        Self {
            direction: take(record, "shape", "direction"),
            frequency: take(record, "spectrum", "frequency"),
        }
    }

    pub(crate) fn decorate(&self, record: &mut CasaValue) {
        for (parent, name, offset) in [
            ("shape", "direction", &self.direction),
            ("spectrum", "frequency", &self.frequency),
        ] {
            if let Some(offset) = offset {
                let CasaValue::Record(fields) = field_mut(field_mut(record, parent), name) else {
                    unreachable!("measure record");
                };
                fields.push(("offset".into(), CasaValue::from_record(offset)));
            }
        }
    }

    pub(crate) fn resolve_direction(
        &self,
        component: &SkyComponent,
    ) -> Result<casa_types::measures::direction::MDirection, String> {
        let mut record =
            casa_types::measures::direction_to_record(component.shape().reference_direction());
        if let Some(offset) = &self.direction {
            record.push(RecordField::new("offset", Value::Record(offset.clone())));
        }
        casa_types::measures::direction_from_record(&record).map_err(|e| e.to_string())
    }

    pub(super) fn resolve(&self, component: &mut SkyComponent) -> Result<(), String> {
        if self.direction.is_some() {
            let direction = self.resolve_direction(component)?;
            component.shape_mut().set_reference_direction(direction);
        }
        if let Some(offset) = &self.frequency {
            let mut record = casa_types::measures::frequency_to_record(
                component.spectrum().reference_frequency(),
            );
            record.push(RecordField::new("offset", Value::Record(offset.clone())));
            let frequency =
                casa_types::measures::frequency_from_record(&record).map_err(|e| e.to_string())?;
            component.spectrum_mut().set_reference_frequency(frequency);
        }
        Ok(())
    }
}

// Records emitted here have these fields by construction.
fn field_mut<'a>(record: &'a mut CasaValue, name: &str) -> &'a mut CasaValue {
    let CasaValue::Record(fields) = record else {
        unreachable!("component record");
    };
    &mut fields
        .iter_mut()
        .find(|(key, _)| key == name)
        .expect("component field")
        .1
}

impl State {
    pub(crate) fn getspectrum(&self, which: i64) -> crate::convert::ConvertResult<CasaValue> {
        let mut spectrum = super::record(self.unchecked(which)?.spectrum().to_record())?;
        if let Some(offset) = self
            .offsets
            .get(&(which as usize))
            .and_then(|offsets| offsets.frequency.as_ref())
        {
            let CasaValue::Record(fields) = field_mut(&mut spectrum, "frequency") else {
                unreachable!("frequency record")
            };
            fields.push(("offset".into(), CasaValue::from_record(offset)));
        }
        Ok(spectrum)
    }

    pub(in crate::tools::componentlist) fn spectral_records(
        &self,
    ) -> Result<std::collections::BTreeMap<usize, RecordValue>, String> {
        let mut records = std::collections::BTreeMap::new();
        for (&index, offsets) in &self.offsets {
            if let Some(offset) = &offsets.frequency {
                let mut record = self
                    .list
                    .get(index)
                    .expect("offset component")
                    .spectrum()
                    .to_record()
                    .map_err(|error| error.to_string())?;
                let Some(Value::Record(frequency)) = record.get_mut("frequency") else {
                    unreachable!("frequency record")
                };
                frequency.upsert("offset", Value::Record(offset.clone()));
                records.insert(index, record);
            }
        }
        Ok(records)
    }

    pub(super) fn materialized(&self) -> Result<Cow<'_, ComponentList>, String> {
        if self.offsets.is_empty() {
            return Ok(Cow::Borrowed(&self.list));
        }
        let mut list = self.list.clone();
        for (&index, offsets) in &self.offsets {
            offsets.resolve(list.get_mut(index).expect("offset component"))?;
        }
        Ok(Cow::Owned(list))
    }
}
