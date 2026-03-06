// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the DATA_DESCRIPTION subtable.
//!
//! Cf. C++ `MSDataDescEnums.h`.

use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the DATA_DESCRIPTION subtable (3 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "FLAG_ROW",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag this row",
    },
    ColumnDef {
        name: "POLARIZATION_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Pointer to polarization table",
    },
    ColumnDef {
        name: "SPECTRAL_WINDOW_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Pointer to spectral window table",
    },
];

/// Optional columns of the DATA_DESCRIPTION subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[ColumnDef {
    name: "LAG_ID",
    data_type: PrimitiveType::Int32,
    column_kind: ColumnKind::Scalar,
    unit: "",
    measure_type: None,
    measure_ref: "",
    comment: "Pointer to lag table",
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 3);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
