// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column overrides for the subtables a MeasurementSet save writes.

use std::collections::HashMap;

use casa_tables::ColumnOverrides;

use crate::schema::SubtableId;

/// The column overrides of some subtables of a MeasurementSet save, by
/// subtable.
///
/// A subtable with overrides is saved through
/// [`TableWritePlan::save_with_column_overrides`](casa_tables::TableWritePlan::save_with_column_overrides)
/// instead of from its rows alone: its row count can come from the
/// overrides ([`ColumnOverrides::for_row_count`]) and its columns from
/// generated cells ([`GeneratedScalarColumn`](casa_tables::GeneratedScalarColumn),
/// [`GeneratedArrayColumn`](casa_tables::GeneratedArrayColumn)), so a
/// creator writes a large subtable, such as a simulator's POINTING, without
/// holding its rows. Used by
/// [`MeasurementSet::save_with_column_overrides`](crate::MeasurementSet::save_with_column_overrides).
#[derive(Clone, Debug, Default)]
pub struct SubtableColumnOverrides {
    tables: HashMap<SubtableId, ColumnOverrides>,
}

impl SubtableColumnOverrides {
    /// No subtable overridden.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override columns of `subtable`, replacing and returning its earlier
    /// overrides.
    pub fn insert(
        &mut self,
        subtable: SubtableId,
        overrides: ColumnOverrides,
    ) -> Option<ColumnOverrides> {
        self.tables.insert(subtable, overrides)
    }

    /// The overrides of `subtable`, if any.
    pub fn get(&self, subtable: SubtableId) -> Option<&ColumnOverrides> {
        self.tables.get(&subtable)
    }

    /// Whether no subtable is overridden.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// The overridden subtables and their overrides, in no set order.
    pub fn iter(&self) -> impl Iterator<Item = (SubtableId, &ColumnOverrides)> {
        self.tables
            .iter()
            .map(|(subtable, overrides)| (*subtable, overrides))
    }
}
