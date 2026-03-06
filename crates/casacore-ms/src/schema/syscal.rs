// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the SYSCAL subtable.
//!
//! Cf. C++ `MSSysCalEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the SYSCAL subtable (5 columns).
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
        comment: "Interval for which data is relevant",
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
        comment: "Midpoint of time for which data is relevant",
    },
];

/// Optional columns of the SYSCAL subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "PHASE_DIFF",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "rad",
        measure_type: None,
        measure_ref: "",
        comment: "Phase difference between receptor 2 and receptor 1",
    },
    ColumnDef {
        name: "PHASE_DIFF_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for PHASE_DIFF",
    },
    ColumnDef {
        name: "TANT",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Antenna temperature",
    },
    ColumnDef {
        name: "TANT_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for TANT",
    },
    ColumnDef {
        name: "TANT_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Antenna temperature spectrum",
    },
    ColumnDef {
        name: "TANT_TSYS",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Ratio of antenna temperature to system temperature",
    },
    ColumnDef {
        name: "TANT_TSYS_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for TANT_TSYS",
    },
    ColumnDef {
        name: "TANT_TSYS_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Spectrum of Tant/Tsys ratio",
    },
    ColumnDef {
        name: "TCAL",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Calibration temperature",
    },
    ColumnDef {
        name: "TCAL_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for TCAL",
    },
    ColumnDef {
        name: "TCAL_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Calibration temperature spectrum",
    },
    ColumnDef {
        name: "TRX",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Receiver temperature",
    },
    ColumnDef {
        name: "TRX_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for TRX",
    },
    ColumnDef {
        name: "TRX_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Receiver temperature spectrum",
    },
    ColumnDef {
        name: "TSKY",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Sky temperature",
    },
    ColumnDef {
        name: "TSKY_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for TSKY",
    },
    ColumnDef {
        name: "TSKY_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Sky temperature spectrum",
    },
    ColumnDef {
        name: "TSYS",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "System temperature",
    },
    ColumnDef {
        name: "TSYS_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for TSYS",
    },
    ColumnDef {
        name: "TSYS_SPECTRUM",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "System temperature spectrum",
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 5);
    }

    #[test]
    fn optional_column_count() {
        assert_eq!(OPTIONAL_COLUMNS.len(), 20);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
