// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the DOPPLER subtable.
//!
//! Cf. C++ `MSDopplerEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the DOPPLER subtable (4 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "DOPPLER_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Doppler tracking id",
    },
    ColumnDef {
        name: "SOURCE_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Source id, pointer to SOURCE table",
    },
    ColumnDef {
        name: "TRANSITION_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Transition id, index into list of transitions in SOURCE",
    },
    ColumnDef {
        name: "VELDEF",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "m/s",
        measure_type: Some(MeasureType::Doppler),
        measure_ref: "RADIO",
        comment: "Velocity definition of Doppler shift",
    },
];

/// Optional columns of the DOPPLER subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 4);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
