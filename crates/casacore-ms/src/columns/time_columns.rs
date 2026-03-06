// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed accessors for TIME and TIME_CENTROID columns.
//!
//! Returns values as [`MEpoch`] with the correct reference frame.
//! The MS stores times as MJD seconds in UTC by default.
//!
//! Cf. C++ `MSMainColumns::timeMeas()`.

use casacore_tables::Table;
use casacore_types::measures::{EpochRef, MEpoch};

use crate::error::MsResult;
use crate::subtables::get_f64;

/// Seconds per day, for MJD conversion.
const SECONDS_PER_DAY: f64 = 86400.0;

/// Typed accessor for the TIME column of the MS main table.
///
/// Values are stored as MJD seconds (UTC) and returned as [`MEpoch`].
pub struct TimeColumn<'a> {
    table: &'a Table,
    column: &'static str,
    refer: EpochRef,
}

impl<'a> TimeColumn<'a> {
    /// Create a TIME column accessor.
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            column: "TIME",
            refer: EpochRef::UTC,
        }
    }

    /// Create a TIME_CENTROID column accessor.
    pub fn centroid(table: &'a Table) -> Self {
        Self {
            table,
            column: "TIME_CENTROID",
            refer: EpochRef::UTC,
        }
    }

    /// Read the time as an [`MEpoch`] for the given row.
    pub fn get_epoch(&self, row: usize) -> MsResult<MEpoch> {
        let mjd_seconds = get_f64(self.table, row, self.column)?;
        let mjd_days = mjd_seconds / SECONDS_PER_DAY;
        Ok(MEpoch::from_mjd(mjd_days, self.refer))
    }

    /// Read the raw MJD seconds value for the given row.
    pub fn get_mjd_seconds(&self, row: usize) -> MsResult<f64> {
        get_f64(self.table, row, self.column)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use crate::schema;
    use crate::test_helpers::{default_value, default_value_for_def};
    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};

    #[test]
    fn read_time_as_epoch() {
        let schema =
            build_table_schema(schema::main_table::REQUIRED_COLUMNS).expect("valid schema");
        let mut table = Table::with_schema(schema);

        let mjd_sec = 59000.0 * SECONDS_PER_DAY;

        let fields: Vec<RecordField> = schema::main_table::REQUIRED_COLUMNS
            .iter()
            .map(|c| {
                if c.name == "TIME" || c.name == "TIME_CENTROID" {
                    RecordField::new(c.name, Value::Scalar(ScalarValue::Float64(mjd_sec)))
                } else {
                    RecordField::new(c.name, default_value_for_def(c))
                }
            })
            .collect();
        table.add_row(RecordValue::new(fields)).unwrap();

        let time_col = TimeColumn::new(&table);
        let epoch = time_col.get_epoch(0).unwrap();
        assert_eq!(epoch.refer(), EpochRef::UTC);
        assert!((epoch.value().as_mjd() - 59000.0).abs() < 1e-10);

        let mjd_s = time_col.get_mjd_seconds(0).unwrap();
        assert!((mjd_s - mjd_sec).abs() < 1e-6);

        // Verify default_value helper returns the right thing
        let dv = default_value("TIME");
        assert!(matches!(dv, Value::Scalar(ScalarValue::Float64(_))));
    }
}
