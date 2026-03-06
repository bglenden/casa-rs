// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compile-time column and keyword metadata for MeasurementSet tables.
//!
//! Each MS table (main and subtables) is defined by arrays of [`ColumnDef`]
//! constants that carry the column name, data type, shape contract, units,
//! measure metadata, and documentation comment — everything needed to construct
//! a [`casacore_tables::TableSchema`] and attach MEASINFO / QuantumUnits
//! keywords for C++ interoperability.
//!
//! The column names and types match the C++ casacore `MS*Enums.h` headers
//! exactly, ensuring binary compatibility with C++-created MeasurementSets.

use casacore_tables::table_measures::MeasureType;
use casacore_tables::{ColumnSchema, SchemaError, TableSchema};
use casacore_types::PrimitiveType;

/// Describes the shape contract for a column in the MS schema.
///
/// This is the MS-level analog of [`casacore_tables::ColumnType`]. It carries
/// shape information as `&'static` data so that column definitions can be
/// `const`.
///
/// Cf. C++ `MSTableImpl::addColumnToDesc` which switches on `TpArray*` vs
/// `Tp*` types and optionally attaches an `IPosition` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnKind {
    /// A single scalar value per row.
    Scalar,
    /// A fixed-shape array (all rows share this shape).
    FixedArray {
        /// The fixed shape shared by all rows.
        shape: &'static [usize],
    },
    /// A variable-shape array with a known number of dimensions.
    VariableArray {
        /// Number of dimensions (rank) of the array.
        ndim: usize,
    },
}

/// Compile-time metadata for a single column in an MS table.
///
/// Each MS table module exports `REQUIRED_COLUMNS` and `OPTIONAL_COLUMNS`
/// arrays of these. The metadata is sufficient to:
/// 1. Build a [`TableSchema`] via [`build_table_schema`].
/// 2. Attach MEASINFO keywords via [`casacore_tables::table_measures::TableMeasDesc`].
/// 3. Attach QuantumUnits keywords via [`casacore_tables::table_quantum::TableQuantumDesc`].
///
/// Cf. C++ `MSTableImpl::colMapDef` which populates `SimpleOrderedMap`
/// entries with the same information.
#[derive(Debug, Clone, Copy)]
pub struct ColumnDef {
    /// Column name, matching C++ `MS*Enums::columnName()` exactly.
    pub name: &'static str,
    /// Element data type (e.g. `Float64` for `TpDouble`).
    pub data_type: PrimitiveType,
    /// Scalar vs. fixed-shape array vs. variable-shape array.
    pub column_kind: ColumnKind,
    /// Physical unit string (e.g. `"s"`, `"m"`, `"Hz"`, `"rad"`), or empty if dimensionless.
    pub unit: &'static str,
    /// Measure type for MEASINFO attachment, if any.
    pub measure_type: Option<MeasureType>,
    /// Default measure reference frame string (e.g. `"UTC"`, `"J2000"`, `"ITRF"`).
    pub measure_ref: &'static str,
    /// Documentation comment, matching the C++ column description.
    pub comment: &'static str,
}

/// Compile-time metadata for a table-level keyword.
///
/// Used for the MS main table keywords (subtable references, MS_VERSION, etc.)
/// and any subtable-level keywords.
#[derive(Debug, Clone, Copy)]
pub struct KeywordDef {
    /// Keyword name.
    pub name: &'static str,
    /// The expected value type.
    pub value_type: KeywordValueType,
    /// Whether this keyword must be present.
    pub required: bool,
    /// Documentation comment.
    pub comment: &'static str,
}

/// The type of value a keyword holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeywordValueType {
    /// A table reference (path to subtable directory).
    Table,
    /// A floating-point version number.
    Float,
    /// A string value.
    String,
}

/// Convert an array of [`ColumnDef`] constants into a [`TableSchema`].
///
/// Each `ColumnDef` is mapped to a [`ColumnSchema`] with the appropriate
/// scalar/array type. This does not attach MEASINFO or QuantumUnits keywords;
/// those must be added separately after the table is created.
///
/// # Errors
///
/// Returns [`SchemaError::DuplicateColumn`] if any two columns share the
/// same name.
pub fn build_table_schema(columns: &[ColumnDef]) -> Result<TableSchema, SchemaError> {
    let col_schemas: Vec<ColumnSchema> = columns
        .iter()
        .map(|def| match def.column_kind {
            ColumnKind::Scalar => ColumnSchema::scalar(def.name, def.data_type),
            ColumnKind::FixedArray { shape } => {
                ColumnSchema::array_fixed(def.name, def.data_type, shape.to_vec())
            }
            ColumnKind::VariableArray { ndim } => {
                ColumnSchema::array_variable(def.name, def.data_type, Some(ndim))
            }
        })
        .collect();
    TableSchema::new(col_schemas)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_COLS: &[ColumnDef] = &[
        ColumnDef {
            name: "COL_A",
            data_type: PrimitiveType::Int32,
            column_kind: ColumnKind::Scalar,
            unit: "",
            measure_type: None,
            measure_ref: "",
            comment: "test scalar",
        },
        ColumnDef {
            name: "COL_B",
            data_type: PrimitiveType::Float64,
            column_kind: ColumnKind::FixedArray { shape: &[3] },
            unit: "m",
            measure_type: Some(MeasureType::Position),
            measure_ref: "ITRF",
            comment: "test fixed array",
        },
        ColumnDef {
            name: "COL_C",
            data_type: PrimitiveType::Complex32,
            column_kind: ColumnKind::VariableArray { ndim: 2 },
            unit: "",
            measure_type: None,
            measure_ref: "",
            comment: "test variable array",
        },
    ];

    #[test]
    fn build_schema_from_column_defs() {
        let schema = build_table_schema(TEST_COLS).expect("valid schema");
        assert_eq!(schema.columns().len(), 3);
        assert!(schema.contains_column("COL_A"));
        assert!(schema.contains_column("COL_B"));
        assert!(schema.contains_column("COL_C"));
    }

    #[test]
    fn build_schema_rejects_duplicates() {
        let dups = &[
            ColumnDef {
                name: "DUP",
                data_type: PrimitiveType::Int32,
                column_kind: ColumnKind::Scalar,
                unit: "",
                measure_type: None,
                measure_ref: "",
                comment: "",
            },
            ColumnDef {
                name: "DUP",
                data_type: PrimitiveType::Float64,
                column_kind: ColumnKind::Scalar,
                unit: "",
                measure_type: None,
                measure_ref: "",
                comment: "",
            },
        ];
        assert!(build_table_schema(dups).is_err());
    }
}
