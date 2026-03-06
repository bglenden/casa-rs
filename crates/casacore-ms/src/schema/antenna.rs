// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the ANTENNA subtable.
//!
//! Cf. C++ `MSAntennaEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the ANTENNA subtable (8 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "DISH_DIAMETER",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "m",
        measure_type: None,
        measure_ref: "",
        comment: "Physical diameter of dish",
    },
    ColumnDef {
        name: "FLAG_ROW",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for this row",
    },
    ColumnDef {
        name: "MOUNT",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Mount type e.g. alt-az, equatorial, etc.",
    },
    ColumnDef {
        name: "NAME",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Antenna name, e.g. VLA22, CA03",
    },
    ColumnDef {
        name: "OFFSET",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[3] },
        unit: "m",
        measure_type: Some(MeasureType::Position),
        measure_ref: "ITRF",
        comment: "Axes offset of mount to FEED REFERENCE point",
    },
    ColumnDef {
        name: "POSITION",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[3] },
        unit: "m",
        measure_type: Some(MeasureType::Position),
        measure_ref: "ITRF",
        comment: "Antenna X,Y,Z phase reference position",
    },
    ColumnDef {
        name: "STATION",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Station (antenna pad) name",
    },
    ColumnDef {
        name: "TYPE",
        data_type: PrimitiveType::String,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Antenna type (e.g. GROUND-BASED)",
    },
];

/// Optional columns of the ANTENNA subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "MEAN_ORBIT",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::FixedArray { shape: &[6] },
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Mean Keplerian orbital elements",
    },
    ColumnDef {
        name: "ORBIT_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Index into optional ORBIT table",
    },
    ColumnDef {
        name: "PHASED_ARRAY_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Index into optional PHASED_ARRAY table",
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;
    use std::collections::HashSet;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 8);
    }

    #[test]
    fn no_duplicate_names() {
        let names: HashSet<&str> = REQUIRED_COLUMNS.iter().map(|c| c.name).collect();
        assert_eq!(names.len(), REQUIRED_COLUMNS.len());
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
