// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessor for the POSITION column in the ANTENNA subtable.
//!
//! Returns antenna positions as [`MPosition`] (ITRF).
//!
//! Cf. C++ `MSAntennaColumns::positionMeas()`.

use casacore_tables::Table;
use casacore_types::ArrayValue;
use casacore_types::measures::MPosition;

use crate::error::{MsError, MsResult};
use crate::subtables::get_array;

/// Typed accessor for the POSITION column.
pub struct AntennaPositionColumn<'a> {
    table: &'a Table,
}

impl<'a> AntennaPositionColumn<'a> {
    /// Create a POSITION column accessor over the ANTENNA subtable.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Read the antenna position as an [`MPosition`] (ITRF) for the given row.
    pub fn get_position(&self, row: usize) -> MsResult<MPosition> {
        let arr = get_array(self.table, row, "POSITION")?;
        match arr {
            ArrayValue::Float64(a) => {
                let slice = a.as_slice().ok_or_else(|| MsError::ColumnTypeMismatch {
                    column: "POSITION".to_string(),
                    table: "ANTENNA".to_string(),
                    expected: "contiguous f64[3]".to_string(),
                    found: "non-contiguous".to_string(),
                })?;
                if slice.len() != 3 {
                    return Err(MsError::ColumnTypeMismatch {
                        column: "POSITION".to_string(),
                        table: "ANTENNA".to_string(),
                        expected: "f64[3]".to_string(),
                        found: format!("f64[{}]", slice.len()),
                    });
                }
                Ok(MPosition::new_itrf(slice[0], slice[1], slice[2]))
            }
            other => Err(MsError::ColumnTypeMismatch {
                column: "POSITION".to_string(),
                table: "ANTENNA".to_string(),
                expected: "Float64 array".to_string(),
                found: format!("{:?}", other.primitive_type()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use crate::schema;
    use crate::subtables::antenna::MsAntennaMut;
    use casacore_types::measures::PositionRef;

    #[test]
    fn read_position_as_mposition() {
        let schema = build_table_schema(schema::antenna::REQUIRED_COLUMNS).expect("valid schema");
        let mut table = Table::with_schema(schema);

        let mut ant = MsAntennaMut::new(&mut table);
        ant.add_antenna(
            "ANT1",
            "PAD1",
            "GROUND-BASED",
            "ALT-AZ",
            [2225142.0, -5440307.0, -2481029.0],
            [0.0, 0.0, 0.0],
            25.0,
        )
        .unwrap();

        let col = AntennaPositionColumn::new(&table);
        let pos = col.get_position(0).unwrap();
        assert_eq!(pos.refer(), PositionRef::ITRF);
        let vals = pos.values();
        assert!((vals[0] - 2225142.0).abs() < 1e-6);
        assert!((vals[1] - (-5440307.0)).abs() < 1e-6);
        assert!((vals[2] - (-2481029.0)).abs() < 1e-6);
    }
}
