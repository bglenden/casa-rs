// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the POINTING subtable.
//!
//! Cf. C++ `MSPointingEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the POINTING subtable (9 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "ANTENNA_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Antenna Id",
    },
    ColumnDef {
        name: "DIRECTION",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Antenna pointing direction as polynomial in time",
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
        name: "NAME",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Pointing position name",
    },
    ColumnDef {
        name: "NUM_POLY",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Series order",
    },
    ColumnDef {
        name: "TARGET",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Target direction as polynomial in time",
    },
    ColumnDef {
        name: "TIME",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Time interval midpoint",
    },
    ColumnDef {
        name: "TIME_ORIGIN",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Time origin for the directions and rates",
    },
    ColumnDef {
        name: "TRACKING",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Tracking flag - True if on position",
    },
];

/// Optional columns of the POINTING subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "ENCODER",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[2] },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Encoder values",
    },
    ColumnDef {
        name: "ON_SOURCE",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "On source flag",
    },
    ColumnDef {
        name: "OVER_THE_TOP",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The antenna is driven past zenith",
    },
    ColumnDef {
        name: "POINTING_MODEL_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Pointing model id",
    },
    ColumnDef {
        name: "POINTING_OFFSET",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "A priori pointing corrections as polynomial in time",
    },
    ColumnDef {
        name: "SOURCE_OFFSET",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Offset from source as polynomial in time",
    },
];

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
