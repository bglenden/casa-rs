// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrapper for the FIELD subtable.
//!
//! Provides read access to field names, positions (phase/delay/reference
//! direction polynomials), and source IDs.
//!
//! Cf. C++ `MSFieldColumns`.

use casacore_tables::Table;
use casacore_types::{ArrayValue, ScalarValue};

use crate::column_def::ColumnDef;
use crate::error::MsResult;
use crate::schema::{self, SubtableId};
use crate::subtables::{
    SubTable, get_array, get_bool, get_f64, get_i32, get_string, set_array, set_scalar,
};

/// Read-only typed wrapper for the FIELD subtable.
pub struct MsField<'a> {
    table: &'a Table,
}

impl<'a> MsField<'a> {
    /// Wrap an existing table as a FIELD subtable.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Number of field rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Field name.
    pub fn name(&self, row: usize) -> MsResult<String> {
        get_string(self.table, row, "NAME")
    }

    /// Special characteristics code (e.g. `"C"` for calibrator).
    pub fn code(&self, row: usize) -> MsResult<String> {
        get_string(self.table, row, "CODE")
    }

    /// Polynomial order of the direction columns.
    pub fn num_poly(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "NUM_POLY")
    }

    /// Delay direction as polynomial coefficients, shape `[2, NUM_POLY+1]`.
    pub fn delay_dir(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "DELAY_DIR")
    }

    /// Phase direction as polynomial coefficients, shape `[2, NUM_POLY+1]`.
    pub fn phase_dir(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "PHASE_DIR")
    }

    /// Reference direction as polynomial coefficients, shape `[2, NUM_POLY+1]`.
    pub fn reference_dir(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "REFERENCE_DIR")
    }

    /// Source ID (index into SOURCE subtable, or -1 if not specified).
    pub fn source_id(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "SOURCE_ID")
    }

    /// Time origin for the directions and rates (MJD seconds, UTC).
    pub fn time(&self, row: usize) -> MsResult<f64> {
        get_f64(self.table, row, "TIME")
    }

    /// Row flag.
    pub fn flag_row(&self, row: usize) -> MsResult<bool> {
        get_bool(self.table, row, "FLAG_ROW")
    }
}

impl SubTable for MsField<'_> {
    fn id() -> SubtableId {
        SubtableId::Field
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::field::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::field::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

/// Mutable typed wrapper for the FIELD subtable.
pub struct MsFieldMut<'a> {
    table: &'a mut Table,
}

impl<'a> MsFieldMut<'a> {
    /// Wrap an existing mutable table as a FIELD subtable.
    pub fn new(table: &'a mut Table) -> Self {
        Self { table }
    }

    /// Number of field rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// View the table through the read-only wrapper.
    pub fn as_ref(&self) -> MsField<'_> {
        MsField { table: self.table }
    }

    /// Access the underlying mutable table.
    pub fn table_mut(&mut self) -> &mut Table {
        self.table
    }

    /// Set a scalar `String` column by name.
    pub fn set_string(&mut self, row: usize, col: &str, value: impl Into<String>) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::String(value.into()))
    }

    /// Set a scalar `i32` column by name.
    pub fn set_i32(&mut self, row: usize, col: &str, value: i32) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::Int32(value))
    }

    /// Set a scalar `f64` column by name.
    pub fn set_f64(&mut self, row: usize, col: &str, value: f64) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::Float64(value))
    }

    /// Set a scalar `bool` column by name.
    pub fn set_bool(&mut self, row: usize, col: &str, value: bool) -> MsResult<()> {
        set_scalar(self.table, row, col, ScalarValue::Bool(value))
    }

    /// Set an array column by name.
    pub fn set_array(&mut self, row: usize, col: &str, value: ArrayValue) -> MsResult<()> {
        set_array(self.table, row, col, value)
    }
}

impl SubTable for MsFieldMut<'_> {
    fn id() -> SubtableId {
        SubtableId::Field
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::field::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::field::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use casacore_types::{RecordField, RecordValue, Value};
    use ndarray::ArrayD;

    fn make_field_table() -> Table {
        let schema = build_table_schema(schema::field::REQUIRED_COLUMNS).expect("valid schema");
        Table::with_schema(schema)
    }

    fn make_dir_array(ra: f64, dec: f64) -> ArrayValue {
        ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![ra, dec]).unwrap())
    }

    #[test]
    fn read_field_row() {
        let mut table = make_field_table();
        let dir = make_dir_array(1.0, 0.5);

        let row = RecordValue::new(vec![
            RecordField::new(
                "CODE",
                Value::Scalar(casacore_types::ScalarValue::String("T".to_string())),
            ),
            RecordField::new("DELAY_DIR", Value::Array(dir.clone())),
            RecordField::new(
                "FLAG_ROW",
                Value::Scalar(casacore_types::ScalarValue::Bool(false)),
            ),
            RecordField::new(
                "NAME",
                Value::Scalar(casacore_types::ScalarValue::String("3C286".to_string())),
            ),
            RecordField::new(
                "NUM_POLY",
                Value::Scalar(casacore_types::ScalarValue::Int32(0)),
            ),
            RecordField::new("PHASE_DIR", Value::Array(dir.clone())),
            RecordField::new("REFERENCE_DIR", Value::Array(dir)),
            RecordField::new(
                "SOURCE_ID",
                Value::Scalar(casacore_types::ScalarValue::Int32(0)),
            ),
            RecordField::new(
                "TIME",
                Value::Scalar(casacore_types::ScalarValue::Float64(4.8e9)),
            ),
        ]);
        table.add_row(row).unwrap();

        let field = MsField::new(&table);
        assert_eq!(field.row_count(), 1);
        assert_eq!(field.name(0).unwrap(), "3C286");
        assert_eq!(field.code(0).unwrap(), "T");
        assert_eq!(field.num_poly(0).unwrap(), 0);
        assert_eq!(field.source_id(0).unwrap(), 0);
        assert!(!field.flag_row(0).unwrap());
    }
}
