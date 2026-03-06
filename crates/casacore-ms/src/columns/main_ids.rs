// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessors for scalar integer ID columns in the main table.
//!
//! Cf. C++ `MSMainColumns::antenna1()`, `antenna2()`, etc.

use casacore_tables::Table;

use crate::error::MsResult;
use crate::subtables::get_i32;

/// Typed accessor for a scalar Int32 column.
pub struct ScalarIdColumn<'a> {
    table: &'a Table,
    column: &'static str,
}

impl<'a> ScalarIdColumn<'a> {
    /// Read the integer value for the given row.
    pub fn get(&self, row: usize) -> MsResult<i32> {
        get_i32(self.table, row, self.column)
    }

    /// The column name.
    pub fn column_name(&self) -> &str {
        self.column
    }
}

/// Create an ANTENNA1 column accessor.
pub fn antenna1(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "ANTENNA1",
    }
}

/// Create an ANTENNA2 column accessor.
pub fn antenna2(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "ANTENNA2",
    }
}

/// Create a FIELD_ID column accessor.
pub fn field_id(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "FIELD_ID",
    }
}

/// Create a DATA_DESC_ID column accessor.
pub fn data_desc_id(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "DATA_DESC_ID",
    }
}

/// Create a SCAN_NUMBER column accessor.
pub fn scan_number(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "SCAN_NUMBER",
    }
}

/// Create an ARRAY_ID column accessor.
pub fn array_id(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "ARRAY_ID",
    }
}

/// Create an OBSERVATION_ID column accessor.
pub fn observation_id(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "OBSERVATION_ID",
    }
}

/// Create a PROCESSOR_ID column accessor.
pub fn processor_id(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "PROCESSOR_ID",
    }
}

/// Create a STATE_ID column accessor.
pub fn state_id(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "STATE_ID",
    }
}

/// Create a FEED1 column accessor.
pub fn feed1(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "FEED1",
    }
}

/// Create a FEED2 column accessor.
pub fn feed2(table: &Table) -> ScalarIdColumn<'_> {
    ScalarIdColumn {
        table,
        column: "FEED2",
    }
}
