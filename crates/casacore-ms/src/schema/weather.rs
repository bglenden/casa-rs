// SPDX-License-Identifier: LGPL-3.0-or-later
//! Column definitions for the WEATHER subtable.
//!
//! Cf. C++ `MSWeatherEnums.h`.

use casacore_tables::table_measures::MeasureType;
use casacore_types::PrimitiveType;

use crate::column_def::{ColumnDef, ColumnKind};

/// Required columns of the WEATHER subtable (3 columns).
pub const REQUIRED_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "ANTENNA_ID",
        data_type: PrimitiveType::Int32,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Antenna number",
    },
    ColumnDef {
        name: "INTERVAL",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: None,
        measure_ref: "",
        comment: "Interval over which data is relevant",
    },
    ColumnDef {
        name: "TIME",
        data_type: PrimitiveType::Float64,
        column_kind: ColumnKind::Scalar,
        unit: "s",
        measure_type: Some(MeasureType::Epoch),
        measure_ref: "UTC",
        comment: "An MEpoch specifying the midpoint of the time for which data is relevant",
    },
];

/// Optional columns of the WEATHER subtable.
pub const OPTIONAL_COLUMNS: &[ColumnDef] = &[
    ColumnDef {
        name: "DEW_POINT",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Dew point",
    },
    ColumnDef {
        name: "DEW_POINT_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for dew point",
    },
    ColumnDef {
        name: "H2O",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "m-2",
        measure_type: None,
        measure_ref: "",
        comment: "Average column density of water-vapor",
    },
    ColumnDef {
        name: "H2O_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for H2O",
    },
    ColumnDef {
        name: "IONOS_ELECTRON",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "m-2",
        measure_type: None,
        measure_ref: "",
        comment: "Average column density of electrons",
    },
    ColumnDef {
        name: "IONOS_ELECTRON_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for IONOS_ELECTRON",
    },
    ColumnDef {
        name: "PRESSURE",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "hPa",
        measure_type: None,
        measure_ref: "",
        comment: "Ambient atmospheric pressure",
    },
    ColumnDef {
        name: "PRESSURE_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for pressure",
    },
    ColumnDef {
        name: "REL_HUMIDITY",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "%",
        measure_type: None,
        measure_ref: "",
        comment: "Ambient relative humidity",
    },
    ColumnDef {
        name: "REL_HUMIDITY_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for rel humidity",
    },
    ColumnDef {
        name: "TEMPERATURE",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "K",
        measure_type: None,
        measure_ref: "",
        comment: "Ambient air temperature for antenna",
    },
    ColumnDef {
        name: "TEMPERATURE_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for temperature",
    },
    ColumnDef {
        name: "WIND_DIRECTION",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "rad",
        measure_type: None,
        measure_ref: "",
        comment: "Average wind direction",
    },
    ColumnDef {
        name: "WIND_DIRECTION_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for wind direction",
    },
    ColumnDef {
        name: "WIND_SPEED",
        data_type: PrimitiveType::Float32,
        column_kind: ColumnKind::Scalar,
        unit: "m/s",
        measure_type: None,
        measure_ref: "",
        comment: "Average wind speed",
    },
    ColumnDef {
        name: "WIND_SPEED_FLAG",
        data_type: PrimitiveType::Bool,
        column_kind: ColumnKind::Scalar,
        unit: "",
        measure_type: None,
        measure_ref: "",
        comment: "Flag for wind speed",
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_def::build_table_schema;

    #[test]
    fn required_column_count() {
        assert_eq!(REQUIRED_COLUMNS.len(), 3);
    }

    #[test]
    fn optional_column_count() {
        assert_eq!(OPTIONAL_COLUMNS.len(), 16);
    }

    #[test]
    fn build_schema_succeeds() {
        build_table_schema(REQUIRED_COLUMNS).expect("schema should build");
    }
}
