// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the FLAG_CMD subtable.
//!
//! Cf. C++ `MSFlagCmdEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the FLAG_CMD subtable (8 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "APPLIED",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "True if flag has been applied to main table",
    },
    ColumnDef {
        name: "COMMAND",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flagging command string",
    },
    ColumnDef {
        name: "INTERVAL",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: None,
        measure_ref: "",
        comment: "Time interval for which this flag command applies",
    },
    ColumnDef {
        name: "LEVEL",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag level - revision level",
    },
    ColumnDef {
        name: "REASON",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag reason, user specified",
    },
    ColumnDef {
        name: "SEVERITY",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Severity code (0-10)",
    },
    ColumnDef {
        name: "TIME",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Midpoint of interval for which this flag command applies",
    },
    ColumnDef {
        name: "TYPE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Type of flag command (FLAG or UNFLAG)",
    },
];

/// Optional columns of the FLAG_CMD subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 8);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
