// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the FIELD subtable.
//!
//! Cf. C++ `MSFieldEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the FIELD subtable (9 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "CODE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Special characteristics of field, e.g. Bandpass calibrator",
    },
    ColumnDef {
        name: "DELAY_DIR",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Direction of delay center (e.g. RA, DEC) as polynomial in time",
    },
    ColumnDef {
        name: "FLAG_ROW",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Row Flag",
    },
    ColumnDef {
        name: "NAME",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Name of this field",
    },
    ColumnDef {
        name: "NUM_POLY",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Polynomial order of *_DIR columns",
    },
    ColumnDef {
        name: "PHASE_DIR",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Direction of phase center (e.g. RA, DEC) as polynomial in time",
    },
    ColumnDef {
        name: "REFERENCE_DIR",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Direction of REFERENCE Center (e.g. RA, DEC) as polynomial in time",
    },
    ColumnDef {
        name: "SOURCE_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Source id",
    },
    ColumnDef {
        name: "TIME",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "Time origin for direction and rate",
    },
];

/// Optional columns of the FIELD subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[ColumnDef {
    name: "EPHEMERIS_ID",
    data_type: PrimitiveType::Int32,
    column_kind: ColumnKind::Scalar,
    unit: "",
    measure_type: None,
    measure_ref: "",
    comment: "Ephemeris id, index into EPHEMERIS table",
}];

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
