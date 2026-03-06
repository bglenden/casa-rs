// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessor for the UVW column.
//!
//! Returns UVW coordinates as `[f64; 3]` in meters.
//!
//! Cf. C++ `MSMainColumns::uvw()`.

use casacore_tables::Table;
use casacore_types::ArrayValue;

use crate::error::{MsError, MsResult};
use crate::subtables::get_array;

/// Typed accessor for the UVW column.
pub struct UvwColumn<'a> {
    table: &'a Table,
}

impl<'a> UvwColumn<'a> {
    /// Create a UVW column accessor.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read the UVW coordinates for the given row as `[u, v, w]` in meters.
    pub fn get(&self, row: usize) -> MsResult<[f64; 3]> {
        let arr = get_array(self.table, row, "UVW")?;
        match arr {
            ArrayValue::Float64(a) => {
                let slice = a.as_slice().ok_or_else(|| MsError::ColumnTypeMismatch {
                    column: "UVW".to_string(),
                    table: "MAIN".to_string(),
                    expected: "contiguous f64[3]".to_string(),
                    found: "non-contiguous".to_string(),
                })?;
                if slice.len() != 3 {
                    return Err(MsError::ColumnTypeMismatch {
                        column: "UVW".to_string(),
                        table: "MAIN".to_string(),
                        expected: "f64[3]".to_string(),
                        found: format!("f64[{}]", slice.len()),
                    });
                }
                Ok([slice[0], slice[1], slice[2]])
            }
            other => Err(MsError::ColumnTypeMismatch {
                column: "UVW".to_string(),
                table: "MAIN".to_string(),
                expected: "Float64 array".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
        }
    }
}
