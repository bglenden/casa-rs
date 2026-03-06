// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the OBSERVATION subtable.
//!
//! Cf. C++ `MSObservationEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the OBSERVATION subtable (9 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "FLAG_ROW",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Row flag",
    },
    ColumnDef {
        name: "LOG",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Observing log",
    },
    ColumnDef {
        name: "OBSERVER",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Name of observer(s)",
    },
    ColumnDef {
        name: "PROJECT",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Project identification string",
    },
    ColumnDef {
        name: "RELEASE_DATE",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Release date when data becomes public",
    },
    ColumnDef {
        name: "SCHEDULE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Observing schedule",
    },
    ColumnDef {
        name: "SCHEDULE_TYPE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Observing schedule type",
    },
    ColumnDef {
        name: "TELESCOPE_NAME",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Telescope Name (e.g. WSRT, VLBA)",
    },
    ColumnDef {
        name: "TIME_RANGE",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[2] },
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Start and end of observation",
    },
];

/// Optional columns of the OBSERVATION subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 9);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
