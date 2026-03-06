// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the FREQ_OFFSET subtable.
//!
//! Cf. C++ `MSFreqOffsetEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the FREQ_OFFSET subtable (7 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "ANTENNA1",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Antenna 1",
    },
    ColumnDef {
        name: "ANTENNA2",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Antenna 2",
    },
    ColumnDef {
        name: "FEED_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Feed id",
    },
    ColumnDef {
        name: "INTERVAL",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: None,
        measure_ref: "",
        comment: "Time interval",
    },
    ColumnDef {
        name: "OFFSET",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "Hz",
        measure_type: None,
        measure_ref: "",
        comment: "Frequency offset",
    },
    ColumnDef {
        name: "SPECTRAL_WINDOW_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Spectral window id",
    },
    ColumnDef {
        name: "TIME",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Midpoint of time for which this set of parameters is accurate",
    },
];

/// Optional columns of the FREQ_OFFSET subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 7);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
