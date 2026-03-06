// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the SPECTRAL_WINDOW subtable.
//!
//! Cf. C++ `MSSpWindowEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the SPECTRAL_WINDOW subtable (14 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "CHAN_FREQ",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "Hz",
        measure_type: Some(MeasureType::Frequency),
        measure_ref: "",
        comment: "Center frequencies for each channel in the data matrix",
    },
    ColumnDef {
        name: "CHAN_WIDTH",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "Hz",
        measure_type: None,
        measure_ref: "",
        comment: "Channel width for each channel",
    },
    ColumnDef {
        name: "EFFECTIVE_BW",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "Hz",
        measure_type: None,
        measure_ref: "",
        comment: "Effective noise bandwidth of each channel",
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
        name: "FREQ_GROUP",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Frequency group",
    },
    ColumnDef {
        name: "FREQ_GROUP_NAME",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Frequency group name",
    },
    ColumnDef {
        name: "IF_CONV_CHAIN",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The IF conversion chain number",
    },
    ColumnDef {
        name: "MEAS_FREQ_REF",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Frequency Measure reference",
    },
    ColumnDef {
        name: "NAME",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Spectral window name",
    },
    ColumnDef {
        name: "NET_SIDEBAND",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Net sideband",
    },
    ColumnDef {
        name: "NUM_CHAN",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Number of spectral channels",
    },
    ColumnDef {
        name: "REF_FREQUENCY",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "Hz",
        measure_type: Some(MeasureType::Frequency),
        measure_ref: "",
        comment: "The reference frequency",
    },
    ColumnDef {
        name: "RESOLUTION",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "Hz",
        measure_type: None,
        measure_ref: "",
        comment: "The effective noise bandwidth for each channel",
    },
    ColumnDef {
        name: "TOTAL_BANDWIDTH",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "Hz",
        measure_type: None,
        measure_ref: "",
        comment: "The total bandwidth for this window",
    },
];

/// Optional columns of the SPECTRAL_WINDOW subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "ASSOC_NATURE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Nature of association with other spectral window",
    },
    ColumnDef {
        name: "ASSOC_SPW_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Associated spectral window id",
    },
    ColumnDef {
        name: "BBC_NO",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Baseband converter number",
    },
    ColumnDef {
        name: "BBC_SIDEBAND",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "BBC sideband",
    },
    ColumnDef {
        name: "DOPPLER_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Doppler id, pointer to DOPPLER table",
    },
    ColumnDef {
        name: "RECEIVER_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Receiver id for this spectral window",
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 14);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
