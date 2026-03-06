// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the STATE subtable.
//!
//! Cf. C++ `MSStateEnums.h`.

use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the STATE subtable (7 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "CAL",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Noise calibration temperature",
    },
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
        name: "LOAD",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Load temperature",
    },
    ColumnDef {
        name: "OBS_MODE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Observing mode, e.g. OFF_SPECTRUM",
    },
    ColumnDef {
        name: "REF",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "True for a reference observation",
    },
    ColumnDef {
        name: "SIG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "True if the source signal is being observed",
    },
    ColumnDef {
        name: "SUB_SCAN",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Sub scan number, relative to scan number",
    },
];

/// Optional columns of the STATE subtable.
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
