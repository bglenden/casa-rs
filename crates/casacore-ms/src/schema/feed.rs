// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the FEED subtable.
//!
//! Cf. C++ `MSFeedEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the FEED subtable (12 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "ANTENNA_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "ID of antenna in this array",
    },
    ColumnDef {
        name: "BEAM_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Id for BEAM model",
    },
    ColumnDef {
        name: "BEAM_OFFSET",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "rad",
        measure_type: Some(MeasureType::Direction),
        measure_ref: "J2000",
        comment: "Beam position offset (on sky but in antenna reference frame)",
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
        comment: "Interval for which this set of parameters is accurate",
    },
    ColumnDef {
        name: "NUM_RECEPTORS",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Number of receptors on this feed (probably 1 or 2)",
    },
    ColumnDef {
        name: "POL_RESPONSE",
        data_type: PrimitiveType::Complex32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "D-matrix i.e. leakage between two receptors",
    },
    ColumnDef {
        name: "POLARIZATION_TYPE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Type of polarization to which a given RECEPTOR responds",
    },
    ColumnDef {
        name: "POSITION",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[3] },
        unit: "m",
        measure_type: Some(MeasureType::Position),
        measure_ref: "ITRF",
        comment: "Position of feed relative to feed reference position",
    },
    ColumnDef {
        name: "RECEPTOR_ANGLE",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "rad",
        measure_type: None,
        measure_ref: "",
        comment: "The reference angle for polarization",
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

/// Optional columns of the FEED subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "FOCUS_LENGTH",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "m",
        measure_type: None,
        measure_ref: "",
        comment: "Focus length",
    },
    ColumnDef {
        name: "PHASED_FEED_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Index into optional PHASED_FEED table",
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 12);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
