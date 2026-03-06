// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrapper for the POLARIZATION subtable.
//!
//! Provides read access to correlation types and products.
//!
//! Cf. C++ `MSPolarizationColumns`.

use casacore_tables::Table;
use casacore_types::{ArrayValue, ScalarValue};

use crate::column_def::ColumnDef;
use crate::error::{MsError, MsResult};
use crate::schema::{self, SubtableId};
use crate::subtables::{SubTable, get_array, get_bool, get_i32, set_array, set_scalar};

/// Read-only typed wrapper for the POLARIZATION subtable.
pub struct MsPolarization<'a> {
    table: &'a Table,
}

impl<'a> MsPolarization<'a> {
    /// Wrap an existing table as a POLARIZATION subtable.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Number of polarization setup rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Number of correlation products.
    pub fn num_corr(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, "NUM_CORR")
    }

    /// Correlation type codes (Stokes enum values), one per product.
    pub fn corr_type(&self, row: usize) -> MsResult<Vec<i32>> {
        extract_i32_vec(get_array(self.table, row, "CORR_TYPE")?)
    }

    /// Correlation product receptor pairs, shape `[2, NUM_CORR]`.
    pub fn corr_product(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "CORR_PRODUCT")
    }

    /// Row flag.
    pub fn flag_row(&self, row: usize) -> MsResult<bool> {
        get_bool(self.table, row, "FLAG_ROW")
    }
}

impl SubTable for MsPolarization<'_> {
    fn id() -> SubtableId {
        SubtableId::Polarization
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::polarization::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::polarization::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

/// Mutable typed wrapper for the POLARIZATION subtable.
pub struct MsPolarizationMut<'a> {
    table: &'a mut Table,
}

impl<'a> MsPolarizationMut<'a> {
    /// Wrap an existing mutable table as a POLARIZATION subtable.
    pub fn new(table: &'a mut Table) -> Self {
        Self { table }
    }

    /// Number of polarization setup rows.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// View the table through the read-only wrapper.
    pub fn as_ref(&self) -> MsPolarization<'_> {
        MsPolarization { table: self.table }
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

    /// Set an array column by name.
    pub fn set_array(&mut self, row: usize, col: &str, value: ArrayValue) -> MsResult<()> {
        set_array(self.table, row, col, value)
    }
}

impl SubTable for MsPolarizationMut<'_> {
    fn id() -> SubtableId {
        SubtableId::Polarization
    }
    fn required_columns() -> &'static [ColumnDef] {
        schema::polarization::REQUIRED_COLUMNS
    }
    fn optional_columns() -> &'static [ColumnDef] {
        schema::polarization::OPTIONAL_COLUMNS
    }
    fn table(&self) -> &Table {
        self.table
    }
}

fn extract_i32_vec(arr: &ArrayValue) -> MsResult<Vec<i32>> {
    match arr {
        ArrayValue::Int32(a) => Ok(a.iter().copied().collect()),
        other => Err(MsError::ColumnTypeMismatch {
            column: "array".to_string(),
            table: "POLARIZATION".to_string(),
            expected: "Int32 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    #[test]
    fn read_polarization() {
        let schema =
            build_table_schema(schema::polarization::REQUIRED_COLUMNS).expect("valid schema");
        let mut table = Table::with_schema(schema);

        // Stokes: XX=9, XY=10, YX=11, YY=12
        let corr_type =
            ArrayValue::Int32(ArrayD::from_shape_vec(vec![4], vec![9, 10, 11, 12]).unwrap());
        let corr_product = ArrayValue::Int32(
            ArrayD::from_shape_vec(vec![2, 4], vec![0, 0, 1, 1, 0, 1, 0, 1]).unwrap(),
        );

        let row = RecordValue::new(vec![
            RecordField::new("CORR_PRODUCT", Value::Array(corr_product)),
            RecordField::new("CORR_TYPE", Value::Array(corr_type)),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("NUM_CORR", Value::Scalar(ScalarValue::Int32(4))),
        ]);
        table.add_row(row).unwrap();

        let pol = MsPolarization::new(&table);
        assert_eq!(pol.row_count(), 1);
        assert_eq!(pol.num_corr(0).unwrap(), 4);
        assert_eq!(pol.corr_type(0).unwrap(), vec![9, 10, 11, 12]);
        assert!(!pol.flag_row(0).unwrap());
    }
}
