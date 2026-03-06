// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessors for FLAG, FLAG_ROW, and FLAG_CATEGORY columns.
//!
//! Cf. C++ `MSMainColumns::flag()`, `flagRow()`, `flagCategory()`.

use casacore_tables::Table;
use casacore_types::ArrayValue;

use crate::error::MsResult;
use crate::subtables::{get_array, get_bool};

/// Typed accessor for the FLAG column (Bool array, same shape as DATA).
pub struct FlagColumn<'a> {
    table: &'a Table,
}

impl<'a> FlagColumn<'a> {
    /// Create a FLAG column accessor.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read the flags for the given row.
    pub fn get(&self, row: usize) -> MsResult<&ArrayValue> {
        get_array(self.table, row, "FLAG")
    }

    /// Return the shape of the flags in the given row.
    pub fn shape(&self, row: usize) -> MsResult<Vec<usize>> {
        let arr = get_array(self.table, row, "FLAG")?;
        Ok(arr.shape().to_vec())
    }
}

/// Typed accessor for the FLAG_ROW column (scalar Bool).
pub struct FlagRowColumn<'a> {
    table: &'a Table,
}

impl<'a> FlagRowColumn<'a> {
    /// Create a FLAG_ROW column accessor.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read the row flag for the given row.
    pub fn get(&self, row: usize) -> MsResult<bool> {
        get_bool(self.table, row, "FLAG_ROW")
    }
}
