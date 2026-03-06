// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessors for EXPOSURE and INTERVAL scalar double columns.
//!
//! Cf. C++ `MSMainColumns::exposure()`, `interval()`.

use casacore_tables::Table;

use crate::error::MsResult;
use crate::subtables::get_f64;

/// Typed accessor for the EXPOSURE column (seconds).
pub struct ExposureColumn<'a> {
    table: &'a Table,
}

impl<'a> ExposureColumn<'a> {
    /// Create an EXPOSURE column accessor.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read the effective integration time in seconds.
    pub fn get(&self, row: usize) -> MsResult<f64> {
        get_f64(self.table, row, "EXPOSURE")
    }
}

/// Typed accessor for the INTERVAL column (seconds).
pub struct IntervalColumn<'a> {
    table: &'a Table,
}

impl<'a> IntervalColumn<'a> {
    /// Create an INTERVAL column accessor.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read the sampling interval in seconds.
    pub fn get(&self, row: usize) -> MsResult<f64> {
        get_f64(self.table, row, "INTERVAL")
    }
}
