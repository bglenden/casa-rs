// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrapper for the DATA_DESCRIPTION subtable.
//!
//! Provides read access to the spectral window / polarization pairing.
//!
//! Cf. C++ `MSDataDescColumns`.

use casacore_tables::Table;
use casacore_types::ScalarValue;

use crate::column_def::ColumnDef;
use crate::error::MsResult;
use crate::schema::{self, SubtableId};
use crate::subtables::{SubTable, get_bool, get_i32, set_scalar};

/// Read-only typed wrapper for the DATA_DESCRIPTION subtable.
pub struct MsDataDescription<'a> {
    table: &'a Table,
}

impl<'a> MsDataDescription<'a> {
    /// Wrap an existing table as a DATA_DESCRIPTION subtable.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Number of data description rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Spectral window ID (index into SPECTRAL_WINDOW subtable).
    pub fn spectral_window_id(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "SPECTRAL_WINDOW_ID")
    }

    /// Polarization ID (index into POLARIZATION subtable).
    pub fn polarization_id(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "POLARIZATION_ID")
    }

    /// Row flag.
    pub fn flag_row(&self, row: usize) -> MsResult<bool> {
        get_bool(self.table, row, "FLAG_ROW")
    }
}

impl SubTable for MsDataDescription<'_> {
    fn id() -> SubtableId {
        SubtableId::DataDescription
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::data_description::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::data_description::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

/// Mutable typed wrapper for the DATA_DESCRIPTION subtable.
pub struct MsDataDescriptionMut<'a> {
    table: &'a mut Table,
}

impl<'a> MsDataDescriptionMut<'a> {
    /// Wrap an existing mutable table as a DATA_DESCRIPTION subtable.
    pub fn new(table: &'a mut Table) -> Self {
        Self { table }
    }

    /// Number of data description rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// View the table through the read-only wrapper.
    pub fn as_ref(&self) -> MsDataDescription<'_> {
        MsDataDescription { table: self.table }
    }

    /// Access the underlying mutable table.
    pub fn table_mut(&mut self) -> &mut Table {
        self.table
    }

    /// Set a scalar `i32` column by name.
    pub fn set_i32(&mut self, row: usize, col: &str, value: i32) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::Int32(value))
    }

    /// Set a scalar `bool` column by name.
    pub fn set_bool(&mut self, row: usize, col: &str, value: bool) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::Bool(value))
    }
}

impl SubTable for MsDataDescriptionMut<'_> {
    fn id() -> SubtableId {
        SubtableId::DataDescription
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::data_description::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::data_description::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

    #[test]
    fn read_data_description() {
        let schema =
            build_table_schema(schema::data_description::REQUIRED_COLUMNS).expect("valid schema");
        let mut table = Table::with_schema(schema);

        let row = RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
        ]);
        table.add_row(row).unwrap();

        let dd = MsDataDescription::new(&table);
        assert_eq!(dd.row_count(), 1);
        assert_eq!(dd.spectral_window_id(0).unwrap(), 0);
        assert_eq!(dd.polarization_id(0).unwrap(), 0);
        assert!(!dd.flag_row(0).unwrap());
    }
}
