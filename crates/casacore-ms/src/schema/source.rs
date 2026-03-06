// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the SOURCE subtable.
//!
//! Cf. C++ `MSSourceEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the SOURCE subtable (10 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "CALIBRATION_GROUP",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Number of grouping for calibration purpose",
    },
    ColumnDef {
        name: "CODE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Special characteristics of source, e.g. Bandpass calibrator",
    },
    ColumnDef {
        name: "DIRECTION",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[2] },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Direction (e.g. RA, DEC)",
    },
    ColumnDef {
        name: "INTERVAL",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: None,
        measure_ref: "",
        comment: "Interval of time for which this set of parameters is accurate",
    },
    ColumnDef {
        name: "NAME",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Name of source as given during observations",
    },
    ColumnDef {
        name: "NUM_LINES",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Number of spectral lines",
    },
    ColumnDef {
        name: "PROPER_MOTION",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[2] },
        unit: "rad/s",
        measure_type: None,
        measure_ref: "",
        comment: "Proper motion",
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
        name: "SPECTRAL_WINDOW_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID for this spectral window setup",
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

/// Optional columns of the SOURCE subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "POSITION",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[3] },
        unit: "m",
        measure_type: Some(MeasureType::Position),
        measure_ref: "ITRF",
        comment: "Position (e.g. for solar system objects)",
    },
    ColumnDef {
        name: "PULSAR_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Pulsar Id",
    },
    ColumnDef {
        name: "REST_FREQUENCY",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "Hz",
        measure_type: Some(MeasureType::Frequency),
        measure_ref: "REST",
        comment: "Line rest frequency",
    },
    ColumnDef {
        name: "SOURCE_MODEL",
        data_type: PrimitiveType::String, // Record in C++, but we use String placeholder
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Component Source Model",
    },
    ColumnDef {
        name: "SYSVEL",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "m/s",
        measure_type: Some(MeasureType::RadialVelocity),
        measure_ref: "LSRK",
        comment: "Systemic velocity at reference",
    },
    ColumnDef {
        name: "TRANSITION",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Line Transition name",
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 10);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
