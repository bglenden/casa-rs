// SPDX-License-Identifier: LGPL-3.0-or-later
//! Mutations that import components, preserving CASA's partial-import lifecycle.

use super::super::records;
use super::State;
use crate::convert::{ConvertError, ConvertResult};
use crate::tools::LogLine;
use casa_components::ComponentList;
use casa_core::RecordValue;

impl State {
    pub(crate) fn add_record(
        &mut self,
        record: &RecordValue,
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<bool> {
        let component =
            records::decode(record, lines).map_err(|e| records::import_error(e, true))?;
        self.append_record_component(component);
        Ok(true)
    }
    fn append_record_component(&mut self, component: records::Component) {
        if let Some(units) = component.units {
            self.shape_units.insert(self.list.len(), units);
        }
        if !component.offsets.is_empty() {
            self.offsets.insert(self.list.len(), component.offsets);
        }
        self.push(component.value);
    }
    pub(crate) fn fromrecord(
        &mut self,
        record: &RecordValue,
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<bool> {
        if !self.list.is_empty() {
            lines.push(LogLine::warning(
                "componentlist is not empty; overwriting it",
            ));
            self.release().map_err(ConvertError::runtime)?;
        }
        self.import_record(record, lines, |e| records::import_error(e, false))?;
        Ok(true)
    }
    fn import_record(
        &mut self,
        record: &RecordValue,
        lines: &mut Vec<LogLine>,
        error_context: impl Fn(String) -> ConvertError,
    ) -> ConvertResult<()> {
        for index in 0..records::count(record)? {
            let component =
                records::decode(records::item(record, index)?, lines).map_err(&error_context)?;
            self.append_record_component(component);
        }
        Ok(())
    }

    /// Decode a casacore list with the image operation's import diagnostics.
    pub(crate) fn image_components(
        record: &RecordValue,
        lines: &mut Vec<LogLine>,
        invalid_record: impl Fn(String) -> ConvertError,
    ) -> ConvertResult<Self> {
        let mut source = Self::default();
        source.import_record(record, lines, |e| {
            if records::is_direct_import_error(&e) {
                ConvertError::runtime(e)
            } else {
                invalid_record(e)
            }
        })?;
        Ok(source)
    }

    pub(crate) fn into_image_model(self) -> ConvertResult<ComponentList> {
        if self.offsets.is_empty() {
            return Ok(self.list);
        }
        self.materialized()
            .map(std::borrow::Cow::into_owned)
            .map_err(ConvertError::runtime)
    }
    pub(crate) fn concatenate_record(
        &mut self,
        record: &RecordValue,
        indices: &[i64],
        pending: bool,
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<bool> {
        let mut source = Self::default();
        source.import_record(record, lines, |e| records::import_error(e, false))?;
        if source.list.is_empty() {
            lines.push(LogLine::warning("Empty componentlist"));
            return Ok(false);
        }
        if pending {
            return Ok(false);
        }
        let indices = if indices.is_empty() || indices[0] < 0 {
            (0..source.list.len()).map(|i| i as i64).collect::<Vec<_>>()
        } else {
            indices.to_vec()
        };
        for (position, index) in indices.into_iter().enumerate() {
            if let Ok(index) = usize::try_from(index)
                && let Some(component) = source.list.get(index)
            {
                if let Some(units) = source.shape_units.get(&index) {
                    self.shape_units.insert(self.list.len(), units.clone());
                }
                if let Some(offsets) = source.offsets.get(&index) {
                    self.offsets.insert(self.list.len(), offsets.clone());
                }
                self.push(component.clone());
            } else {
                lines.push(LogLine::error(format!(
                    "component {position}does not exist in this list "
                )));
            }
        }
        Ok(true)
    }
}
