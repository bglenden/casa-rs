// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the POLARIZATION subtable.
//!
//! Cf. C++ `MSPolarizationEnums.h`.

use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the POLARIZATION subtable (4 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "CORR_PRODUCT",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::VariableArray { ndim: 2 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Indices describing receptors of feed going into correlation",
    },
    ColumnDef {
        name: "CORR_TYPE",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::VariableArray { ndim: 1 },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "The polarization type for each correlation product, as a Stokes enum",
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
        name: "NUM_CORR",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Number of correlation products",
    },
];

/// Optional columns of the POLARIZATION subtable.
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
