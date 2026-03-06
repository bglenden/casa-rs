// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessors for WEIGHT, SIGMA, WEIGHT_SPECTRUM, and SIGMA_SPECTRUM columns.
//!
//! Cf. C++ `MSMainColumns::weight()`, `sigma()`, etc.

use casacore_tables::Table;
use casacore_types::ArrayValue;

use crate::error::{MsError, MsResult};
use crate::subtables::{get_array, has_column};

/// Typed accessor for the WEIGHT column (Float32 array, shape `[num_corr]`).
pub struct WeightColumn<'a> {
    table: &'a Table,
}

impl<'a> WeightColumn<'a> {
    /// Create a WEIGHT column accessor.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read the weights for the given row.
    pub fn get(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "WEIGHT")
    }
}

/// Typed accessor for the SIGMA column (Float32 array, shape `[num_corr]`).
pub struct SigmaColumn<'a> {
    table: &'a Table,
}

impl<'a> SigmaColumn<'a> {
    /// Create a SIGMA column accessor.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read the sigma values for the given row.
    pub fn get(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "SIGMA")
    }
}

/// Typed accessor for the WEIGHT_SPECTRUM column (optional).
pub struct WeightSpectrumColumn<'a> {
    table: &'a Table,
}

impl<'a> WeightSpectrumColumn<'a> {
    /// Create a WEIGHT_SPECTRUM column accessor.
    ///
    /// Returns `MsError::ColumnNotPresent` if the column is absent.
    pub fn new(table: &'a Table) -> MsResult<Self> {
        if !has_column(table, "WEIGHT_SPECTRUM") {
            return Err(MsError::ColumnNotPresent("WEIGHT_SPECTRUM".to_string()));
        }
        Ok(Self { table })
    }

    /// Read the per-channel weights for the given row.
    pub fn get(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "WEIGHT_SPECTRUM")
    }
}

/// Typed accessor for the SIGMA_SPECTRUM column (optional).
pub struct SigmaSpectrumColumn<'a> {
    table: &'a Table,
}

impl<'a> SigmaSpectrumColumn<'a> {
    /// Create a SIGMA_SPECTRUM column accessor.
    ///
    /// Returns `MsError::ColumnNotPresent` if the column is absent.
    pub fn new(table: &'a Table) -> MsResult<Self> {
        if !has_column(table, "SIGMA_SPECTRUM") {
            return Err(MsError::ColumnNotPresent("SIGMA_SPECTRUM".to_string()));
        }
        Ok(Self { table })
    }

    /// Read the per-channel sigma values for the given row.
    pub fn get(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "SIGMA_SPECTRUM")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use crate::schema;

    #[test]
    fn weight_spectrum_not_present() {
        let schema =
            build_table_schema(schema::main_table::REQUIRED_COLUMNS).expect("valid schema");
        let table = Table::with_schema(schema);
        let result = WeightSpectrumColumn::new(&table);
        assert!(matches!(result, Err(MsError::ColumnNotPresent(_))));
    }

    #[test]
    fn sigma_spectrum_not_present() {
        let schema =
            build_table_schema(schema::main_table::REQUIRED_COLUMNS).expect("valid schema");
        let table = Table::with_schema(schema);
        let result = SigmaSpectrumColumn::new(&table);
        assert!(matches!(result, Err(MsError::ColumnNotPresent(_))));
    }
}
