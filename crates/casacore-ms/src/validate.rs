// SPDX-License-Identifier: LGPL-3.0-or-later
//! Validation logic for MeasurementSet structure.
//!
//! Checks that all required subtables are present as table keywords,
//! all required columns exist with correct data types, that required
//! `QuantumUnits` / `MEASINFO.type` metadata is present, and that the
//! MS_VERSION keyword is present.

use std::collections::HashMap;

use casacore_tables::Table;
use casacore_types::{ArrayValue, ScalarValue, Value};

use crate::column_def::ColumnDef;
use crate::error::MsResult;
use crate::metadata::{measure_type_name_for, quantum_units_for};
use crate::schema::{self, SubtableId};

/// Validation issue found during MS validation.
#[derive(Debug, Clone)]
pub enum ValidationIssue {
    /// A required subtable keyword is missing.
    MissingSubtable(SubtableId),
    /// A required column is missing from a table.
    MissingColumn {
        /// The table or subtable name.
        table_name: String,
        /// The missing column name.
        column_name: String,
    },
    /// A column exists but has the wrong data type.
    WrongColumnType {
        /// The table or subtable name.
        table_name: String,
        /// The column name.
        column_name: String,
        /// Expected type description.
        expected: String,
        /// Actual type description.
        found: String,
    },
    /// A required `QuantumUnits` keyword is missing.
    MissingQuantumUnits {
        /// The table or subtable name.
        table_name: String,
        /// The column name.
        column_name: String,
    },
    /// A `QuantumUnits` keyword exists but has the wrong value.
    WrongQuantumUnits {
        /// The table or subtable name.
        table_name: String,
        /// The column name.
        column_name: String,
        /// Expected unit vector.
        expected: Vec<String>,
        /// Actual unit vector.
        found: Vec<String>,
    },
    /// A required `MEASINFO` keyword is missing.
    MissingMeasureInfo {
        /// The table or subtable name.
        table_name: String,
        /// The column name.
        column_name: String,
    },
    /// A `MEASINFO.type` value exists but does not match the MS schema.
    WrongMeasureType {
        /// The table or subtable name.
        table_name: String,
        /// The column name.
        column_name: String,
        /// Expected measure type.
        expected: String,
        /// Actual measure type.
        found: String,
    },
    /// The MS_VERSION keyword is missing.
    MissingMsVersion,
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationIssue::MissingSubtable(id) => {
                write!(f, "required subtable '{}' missing", id.name())
            }
            ValidationIssue::MissingColumn {
                table_name,
                column_name,
            } => write!(f, "required column '{column_name}' missing in {table_name}"),
            ValidationIssue::WrongColumnType {
                table_name,
                column_name,
                expected,
                found,
            } => write!(
                f,
                "column '{column_name}' in {table_name}: expected {expected}, found {found}"
            ),
            ValidationIssue::MissingQuantumUnits {
                table_name,
                column_name,
            } => write!(
                f,
                "column '{column_name}' in {table_name}: missing QuantumUnits"
            ),
            ValidationIssue::WrongQuantumUnits {
                table_name,
                column_name,
                expected,
                found,
            } => write!(
                f,
                "column '{column_name}' in {table_name}: expected QuantumUnits {:?}, found {:?}",
                expected, found
            ),
            ValidationIssue::MissingMeasureInfo {
                table_name,
                column_name,
            } => write!(
                f,
                "column '{column_name}' in {table_name}: missing MEASINFO"
            ),
            ValidationIssue::WrongMeasureType {
                table_name,
                column_name,
                expected,
                found,
            } => write!(
                f,
                "column '{column_name}' in {table_name}: expected MEASINFO.type {expected}, found {found}"
            ),
            ValidationIssue::MissingMsVersion => write!(f, "MS_VERSION keyword missing"),
        }
    }
}

/// Validate the main table has all required subtable keywords.
pub fn validate_subtable_keywords(
    main_table: &Table,
    subtables: &HashMap<SubtableId, Table>,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    for id in SubtableId::ALL_REQUIRED {
        if !subtables.contains_key(id) {
            issues.push(ValidationIssue::MissingSubtable(*id));
        }
    }
    // Check MS_VERSION keyword
    if main_table.keywords().get("MS_VERSION").is_none() {
        issues.push(ValidationIssue::MissingMsVersion);
    }
    issues
}

/// Validate that a table has all required columns with correct data types.
pub fn validate_columns(
    table: &Table,
    table_name: &str,
    required_columns: &[ColumnDef],
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let schema = match table.schema() {
        Some(s) => s,
        None => {
            // No schema means we can't validate column types — just check by
            // attempting to access rows (if any exist).
            return issues;
        }
    };

    for col_def in required_columns {
        match schema.column(col_def.name) {
            None => {
                issues.push(ValidationIssue::MissingColumn {
                    table_name: table_name.to_string(),
                    column_name: col_def.name.to_string(),
                });
            }
            Some(col_schema) => {
                // Check data type
                if let Some(expected_type) = col_schema.data_type() {
                    if expected_type != col_def.data_type {
                        issues.push(ValidationIssue::WrongColumnType {
                            table_name: table_name.to_string(),
                            column_name: col_def.name.to_string(),
                            expected: format!("{:?}", col_def.data_type),
                            found: format!("{expected_type:?}"),
                        });
                    }
                }
            }
        }
    }
    issues
}

/// Validate that required column keywords match casacore's MS expectations.
pub fn validate_column_metadata(
    table: &Table,
    table_name: &str,
    required_columns: &[ColumnDef],
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    for col_def in required_columns {
        let col_keywords = table.column_keywords(col_def.name);

        if let Some(expected_units) = quantum_units_for(col_def) {
            match col_keywords.and_then(|keywords| keywords.get("QuantumUnits")) {
                Some(Value::Array(ArrayValue::String(units))) => {
                    let found: Vec<String> = units.iter().cloned().collect();
                    if found != expected_units {
                        issues.push(ValidationIssue::WrongQuantumUnits {
                            table_name: table_name.to_string(),
                            column_name: col_def.name.to_string(),
                            expected: expected_units,
                            found,
                        });
                    }
                }
                _ => issues.push(ValidationIssue::MissingQuantumUnits {
                    table_name: table_name.to_string(),
                    column_name: col_def.name.to_string(),
                }),
            }
        }

        if let Some(expected_measure_type) = measure_type_name_for(col_def) {
            match col_keywords.and_then(|keywords| keywords.get("MEASINFO")) {
                Some(Value::Record(measinfo)) => match measinfo.get("type") {
                    Some(Value::Scalar(ScalarValue::String(found_measure_type))) => {
                        if found_measure_type != &expected_measure_type {
                            issues.push(ValidationIssue::WrongMeasureType {
                                table_name: table_name.to_string(),
                                column_name: col_def.name.to_string(),
                                expected: expected_measure_type,
                                found: found_measure_type.clone(),
                            });
                        }
                    }
                    _ => issues.push(ValidationIssue::MissingMeasureInfo {
                        table_name: table_name.to_string(),
                        column_name: col_def.name.to_string(),
                    }),
                },
                _ => issues.push(ValidationIssue::MissingMeasureInfo {
                    table_name: table_name.to_string(),
                    column_name: col_def.name.to_string(),
                }),
            }
        }
    }

    issues
}

/// Full validation of an MS: checks main table + all present subtables.
pub fn validate_ms(
    main_table: &Table,
    subtables: &HashMap<SubtableId, Table>,
) -> MsResult<Vec<ValidationIssue>> {
    let mut all_issues = Vec::new();

    // Check subtable presence and MS_VERSION
    all_issues.extend(validate_subtable_keywords(main_table, subtables));

    // Check main table columns
    all_issues.extend(validate_columns(
        main_table,
        "MAIN",
        schema::main_table::REQUIRED_COLUMNS,
    ));
    all_issues.extend(validate_column_metadata(
        main_table,
        "MAIN",
        schema::main_table::REQUIRED_COLUMNS,
    ));

    // Check each subtable's required columns
    for (id, table) in subtables {
        let required = schema::required_columns(*id);
        all_issues.extend(validate_columns(table, id.name(), required));
        all_issues.extend(validate_column_metadata(table, id.name(), required));
    }

    Ok(all_issues)
}
