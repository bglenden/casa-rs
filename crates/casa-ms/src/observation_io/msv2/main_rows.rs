// SPDX-License-Identifier: LGPL-3.0-or-later

//! MAIN rows in physical order, for the selections that CASA defines by a
//! row's place in the table: `MSSelection`'s time defaults come from the
//! first row the other expressions select (`MSTimeParse::getDefaults`), which
//! a partition traversal cannot name.

use casa_observation::ObservationError;
use casa_tables::{RequiredScalarColumnDestination, RequiredScalarColumnValuesMut};
use casa_types::ArrayValue;

use super::{Msv2Observation, table_error};

/// Rows read per block while searching MAIN in order.
const ROWS_PER_BLOCK: usize = 4096;

/// The key columns of one MAIN row a row-level selection tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MainRowKeys {
    /// The physical row.
    pub row: usize,
    /// `FIELD_ID`.
    pub field: i32,
    /// `DATA_DESC_ID`.
    pub data_description: i32,
    /// `ANTENNA1`.
    pub antenna1: i32,
    /// `ANTENNA2`.
    pub antenna2: i32,
    /// `SCAN_NUMBER`.
    pub scan: i32,
    /// `OBSERVATION_ID`.
    pub observation: i32,
    /// `STATE_ID`.
    pub state: i32,
}

impl Msv2Observation {
    /// The first MAIN row, in physical order, that `accept` accepts, or
    /// `None` when it accepts none. Key columns are read a block of rows at a
    /// time and the search stops at the first accepted row, so a selection
    /// that holds MAIN's first rows reads one block.
    ///
    /// # Errors
    ///
    /// A key column that cannot be read, or `accept`'s own error.
    pub fn first_main_row(
        &self,
        mut accept: impl FnMut(&MainRowKeys) -> Result<bool, ObservationError>,
    ) -> Result<Option<usize>, ObservationError> {
        let rows = self.ms.row_count();
        let mut columns: [Vec<i32>; 7] = Default::default();
        let mut block = Vec::with_capacity(ROWS_PER_BLOCK.min(rows));
        for start in (0..rows).step_by(ROWS_PER_BLOCK) {
            block.clear();
            block.extend(start..(start + ROWS_PER_BLOCK).min(rows));
            let [field, ddid, antenna1, antenna2, scan, observation, state] = &mut columns;
            let mut destinations = [
                ("FIELD_ID", field),
                ("DATA_DESC_ID", ddid),
                ("ANTENNA1", antenna1),
                ("ANTENNA2", antenna2),
                ("SCAN_NUMBER", scan),
                ("OBSERVATION_ID", observation),
                ("STATE_ID", state),
            ]
            .map(|(name, values)| {
                values.clear();
                RequiredScalarColumnDestination::new(
                    name,
                    RequiredScalarColumnValuesMut::Int32(values),
                )
            });
            self.ms
                .main_table()
                .required_scalar_columns_for_rows_into(&block, &mut destinations)
                .map_err(table_error)?;
            for (offset, &row) in block.iter().enumerate() {
                let keys = MainRowKeys {
                    row,
                    field: columns[0][offset],
                    data_description: columns[1][offset],
                    antenna1: columns[2][offset],
                    antenna2: columns[3][offset],
                    scan: columns[4][offset],
                    observation: columns[5][offset],
                    state: columns[6][offset],
                };
                if accept(&keys)? {
                    return Ok(Some(row));
                }
            }
        }
        Ok(None)
    }

    /// MAIN row `row`'s `TIME` and `EXPOSURE`, in seconds as stored.
    ///
    /// # Errors
    ///
    /// When either cannot be read.
    pub fn main_row_time_exposure(&self, row: usize) -> Result<(f64, f64), ObservationError> {
        let (mut time, mut exposure) = (Vec::with_capacity(1), Vec::with_capacity(1));
        self.ms
            .main_table()
            .required_scalar_columns_for_rows_into(
                &[row],
                &mut [
                    RequiredScalarColumnDestination::new(
                        "TIME",
                        RequiredScalarColumnValuesMut::Float64(&mut time),
                    ),
                    RequiredScalarColumnDestination::new(
                        "EXPOSURE",
                        RequiredScalarColumnValuesMut::Float64(&mut exposure),
                    ),
                ],
            )
            .map_err(table_error)?;
        Ok((time[0], exposure[0]))
    }

    /// MAIN row `row`'s `UVW` in metres.
    ///
    /// # Errors
    ///
    /// When the cell cannot be read or is not three `Float64` values.
    pub fn main_row_uvw(&self, row: usize) -> Result<[f64; 3], ObservationError> {
        let cells = self
            .ms
            .main_table()
            .column_accessor("UVW")
            .map_err(table_error)?
            .array_cells_owned(&[row])
            .map_err(table_error)?;
        match cells.into_iter().next().flatten() {
            Some(ArrayValue::Float64(values)) => match values.as_slice() {
                Some([u, v, w]) => Ok([*u, *v, *w]),
                _ => Err(super::invalid(format!("UVW row {row} is not three values"))),
            },
            _ => Err(super::invalid(format!("UVW row {row} is not Float64"))),
        }
    }
}
