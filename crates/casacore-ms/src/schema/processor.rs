// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the PROCESSOR subtable.
//!
//! Cf. C++ `MSProcessorEnums.h`.

use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the PROCESSOR subtable (5 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
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
        name: "MODE_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Processor mode id",
    },
    ColumnDef {
        name: "SUB_TYPE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Processor sub type",
    },
    ColumnDef {
        name: "TYPE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Processor type",
    },
    ColumnDef {
        name: "TYPE_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Processor type id",
    },
];

/// Optional columns of the PROCESSOR subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[ColumnDef {
    name: "PASS_ID",
    data_type: PrimitiveType::Int32,
    column_kind: ColumnKind::Scalar,
    unit: "",
    measure_type: None,
    measure_ref: "",
    comment: "Processor pass number",
}];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 5);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
