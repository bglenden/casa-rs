// SPDX-License-Identifier: LGPL-3.0-or-later
//! Builder for constructing MeasurementSet schemas.
//!
//! The [`MeasurementSetBuilder`] assembles the schemas for the main table
//! and all required (plus selected optional) subtables, ready for use
//! with [`MeasurementSet::create`](crate::ms::MeasurementSet::create).

use std::collections::BTreeSet;

use casacore_tables::{SchemaError, TableSchema};

use crate::column_def::{ColumnDef, build_table_schema};
use crate::schema::{self, SubtableId, main_table::OptionalMainColumn};

/// Builder for constructing MeasurementSet table schemas.
///
/// By default, the builder includes all 21 required main-table columns
/// and all 12 required subtables. Optional subtables and columns can be
/// added incrementally.
///
/// # Example
///
/// ```rust
/// use casacore_ms::builder::MeasurementSetBuilder;
/// use casacore_ms::OptionalMainColumn;
/// use casacore_ms::schema::SubtableId;
///
/// let builder = MeasurementSetBuilder::new()
///     .with_optional_subtable(SubtableId::Source)
///     .with_main_column(OptionalMainColumn::Data);
/// let schemas = builder.build_schemas().expect("valid schemas");
/// assert!(schemas.main.contains_column("DATA"));
/// ```
pub struct MeasurementSetBuilder {
    optional_subtables: BTreeSet<SubtableId>,
    extra_main_columns: BTreeSet<OptionalMainColumn>,
}

/// The output of [`MeasurementSetBuilder::build_schemas`]: a main table schema
/// plus a schema for each subtable.
pub struct MsSchemas {
    /// Schema for the main table.
    pub main: TableSchema,
    /// Schemas for each subtable, keyed by subtable ID.
    pub subtables: Vec<(SubtableId, TableSchema)>,
}

impl MeasurementSetBuilder {
    /// Create a new builder with default settings (all required tables, no optional).
    pub fn new() -> Self {
        Self {
            optional_subtables: BTreeSet::new(),
            extra_main_columns: BTreeSet::new(),
        }
    }

    /// Include an optional subtable.
    pub fn with_optional_subtable(mut self, id: SubtableId) -> Self {
        self.optional_subtables.insert(id);
        self
    }

    /// Add an optional column to the main table.
    pub fn with_main_column(mut self, column: OptionalMainColumn) -> Self {
        self.extra_main_columns.insert(column);
        self
    }

    /// Build all table schemas.
    pub fn build_schemas(&self) -> Result<MsSchemas, SchemaError> {
        // Main table: required + selected optional columns
        let mut main_cols: Vec<ColumnDef> = schema::main_table::REQUIRED_COLUMNS.to_vec();
        for column in OptionalMainColumn::ALL {
            if self.extra_main_columns.contains(column) {
                main_cols.push(*column.column_def());
            }
        }
        let main = build_table_schema(&main_cols)?;

        // Subtables: all required + selected optional
        let mut subtables = Vec::new();
        for id in SubtableId::ALL_REQUIRED {
            let schema = build_table_schema(schema::required_columns(*id))?;
            subtables.push((*id, schema));
        }
        for id in &self.optional_subtables {
            if !id.is_required() {
                let schema = build_table_schema(schema::required_columns(*id))?;
                subtables.push((*id, schema));
            }
        }

        Ok(MsSchemas { main, subtables })
    }
}

impl Default for MeasurementSetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_builder_produces_valid_schemas() {
        let schemas = MeasurementSetBuilder::new()
            .build_schemas()
            .expect("valid schemas");
        assert_eq!(schemas.main.columns().len(), 21);
        assert_eq!(schemas.subtables.len(), 12);
    }

    #[test]
    fn builder_with_data_column() {
        let schemas = MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .build_schemas()
            .expect("valid schemas");
        assert!(schemas.main.contains_column("DATA"));
        assert_eq!(schemas.main.columns().len(), 22);
    }

    #[test]
    fn builder_orders_and_deduplicates_optional_selections() {
        let schemas = MeasurementSetBuilder::new()
            .with_optional_subtable(SubtableId::Weather)
            .with_optional_subtable(SubtableId::Doppler)
            .with_optional_subtable(SubtableId::Weather)
            .with_main_column(OptionalMainColumn::ModelData)
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::ModelData)
            .build_schemas()
            .expect("valid schemas");

        let column_names: Vec<_> = schemas.main.columns().iter().map(|c| c.name()).collect();
        assert!(column_names.contains(&"DATA"));
        assert!(column_names.contains(&"MODEL_DATA"));
        let data_idx = column_names
            .iter()
            .position(|name| *name == "DATA")
            .unwrap();
        let model_idx = column_names
            .iter()
            .position(|name| *name == "MODEL_DATA")
            .unwrap();
        assert!(data_idx < model_idx);

        let optional_ids: Vec<_> = schemas
            .subtables
            .iter()
            .skip(12)
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(optional_ids, vec![SubtableId::Doppler, SubtableId::Weather]);
    }

    #[test]
    fn builder_with_optional_subtable() {
        let schemas = MeasurementSetBuilder::new()
            .with_optional_subtable(SubtableId::Source)
            .build_schemas()
            .expect("valid schemas");
        assert_eq!(schemas.subtables.len(), 13);
        assert!(
            schemas
                .subtables
                .iter()
                .any(|(id, _)| *id == SubtableId::Source)
        );
    }

    #[test]
    fn builder_with_all_optional_subtables() {
        let mut builder = MeasurementSetBuilder::new();
        for id in SubtableId::ALL_OPTIONAL {
            builder = builder.with_optional_subtable(*id);
        }
        let schemas = builder.build_schemas().expect("valid schemas");
        assert_eq!(schemas.subtables.len(), 17);
    }
}
