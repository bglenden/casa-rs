// SPDX-License-Identifier: LGPL-3.0-or-later
//! Adding an optional subtable to a MeasurementSet already made, as C++
//! code adds one (`MSSource::requiredTableDesc`, then the MS keyword that
//! names it).

use casa_tables::Table;
use casa_types::{RecordField, Value};

use super::{MeasurementSet, apply_column_metadata};
use crate::column_def::build_table_schema;
use crate::error::{MsError, MsResult};
use crate::schema::{self, SubtableId};

impl MeasurementSet {
    /// Add optional subtable `id`, empty, with its required columns in
    /// casacore's order and their measure and unit keywords, named by
    /// MAIN's keyword as [`MeasurementSet::create`] names a subtable. It is
    /// written with the MeasurementSet on the next [`MeasurementSet::save`];
    /// optional columns are the caller's to add.
    ///
    /// # Errors
    ///
    /// [`MsError::InvalidInput`] when `id` is a required subtable or the
    /// MeasurementSet has it already.
    pub fn add_optional_subtable(&mut self, id: SubtableId) -> MsResult<&mut Table> {
        if id.is_required() || self.subtables.contains_key(&id) {
            return Err(MsError::InvalidInput(format!(
                "{} is required or present already; only an absent optional subtable is added",
                id.name()
            )));
        }
        let mut table = Table::with_schema(build_table_schema(
            &schema::required_columns_in_casacore_order(id),
        )?);
        apply_column_metadata(&mut table, schema::required_columns(id));
        apply_column_metadata(&mut table, schema::optional_columns(id));
        self.main.keywords_mut().push(RecordField::new(
            id.name(),
            Value::table_ref(id.name().to_string()),
        ));
        if let Some(path) = &self.path {
            self.subtable_paths.insert(id, path.join(id.name()));
        }
        Ok(self.subtables.entry(id).or_insert(table))
    }
}
