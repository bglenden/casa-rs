// SPDX-License-Identifier: LGPL-3.0-or-later
//! ForwardColumnEngine — virtual columns that delegate to another table.
//!
//! A forwarding column reads its values from a column with the same name in
//! a referenced table. This avoids data duplication when multiple tables
//! share reference data (e.g. antenna positions, spectral windows).
//!
//! # On-disk convention
//!
//! Each forwarding column stores a keyword `_ForwardColumn_TableName` whose
//! value is the relative path (using the C++ `Path::addDirectory` convention)
//! to the referenced table. The DM type string is `"ForwardColumnEngine"`.
//!
//! # C++ equivalent
//!
//! `ForwardColumnEngine` / `ForwardColumn` in
//! `casacore/tables/DataMan/ForwardCol.h`.

use casa_types::{RecordField, RecordValue, Value};

use super::StorageError;
use super::table_control::PlainColumnEntry;
use super::virtual_engine::{VirtualColumnEngine, VirtualContext};
use super::{CompositeStorage, StorageManager, add_directory};

/// Keyword name storing the referenced table path on each forwarding column.
const FORWARD_TABLE_KEYWORD: &str = "_ForwardColumn_TableName";

/// Virtual column engine that reads values from a column in another table.
///
/// During materialization, for each bound column:
/// 1. Read the `_ForwardColumn_TableName` keyword to find the referenced table.
/// 2. Open the referenced table and load its rows.
/// 3. Copy the matching column values into this table's rows.
///
/// # C++ equivalent
///
/// `ForwardColumnEngine` in `casacore/tables/DataMan/ForwardCol.h`.
#[derive(Debug)]
pub(crate) struct ForwardColumnEngine;

impl VirtualColumnEngine for ForwardColumnEngine {
    fn type_name(&self) -> &str {
        "ForwardColumnEngine"
    }

    fn materialize(
        &self,
        ctx: &VirtualContext,
        bound_cols: &[(usize, &PlainColumnEntry)],
        rows: &mut [RecordValue],
    ) -> Result<(), StorageError> {
        for &(desc_idx, _pc) in bound_cols {
            let col_desc = &ctx.col_descs[desc_idx];
            let col_name = &col_desc.col_name;

            // Read the referenced table path from the column keyword.
            let ref_table_rel = get_forward_table_name(&col_desc.keywords, col_name)?;

            // Resolve relative path using C++ addDirectory convention.
            let ref_table_path = add_directory(&ref_table_rel, ctx.table_path)?;

            // Open the referenced table.
            let storage = CompositeStorage;
            let ref_snapshot = storage.load(&ref_table_path)?;

            // Copy matching column values from referenced table into our rows.
            for (row_idx, row) in rows.iter_mut().enumerate() {
                let value = if row_idx < ref_snapshot.rows.len() {
                    ref_snapshot.rows[row_idx]
                        .get(col_name)
                        .cloned()
                        .unwrap_or(Value::Scalar(casa_types::ScalarValue::Int32(0)))
                } else {
                    Value::Scalar(casa_types::ScalarValue::Int32(0))
                };
                row.push(RecordField::new(col_name.clone(), value));
            }
        }
        Ok(())
    }
}

/// Keyword name storing the referenced table path (with `_Row` suffix) for
/// ForwardColumnIndexedRowEngine.
const FORWARD_TABLE_ROW_KEYWORD: &str = "_ForwardColumn_TableName_Row";

/// Virtual column engine that reads values from another table with row remapping.
///
/// Like `ForwardColumnEngine`, but for each row `r` it reads an index column
/// to determine which row in the referenced table to read from. This is used
/// by MeasurementSet subtables.
///
/// # On-disk keywords
///
/// - `_ForwardColumn_TableName_Row` (String) — path to referenced table
/// - Table keyword `<dmname>_ForwardColumn_RowName` — name of the row-mapping column
///
/// # C++ equivalent
///
/// `ForwardColumnIndexedRowEngine` in `casacore/tables/DataMan/ForwardColRow.h`.
#[derive(Debug)]
pub(crate) struct ForwardColumnIndexedRowEngine;

impl VirtualColumnEngine for ForwardColumnIndexedRowEngine {
    fn type_name(&self) -> &str {
        "ForwardColumnIndexedRowEngine"
    }

    fn materialize(
        &self,
        ctx: &VirtualContext,
        bound_cols: &[(usize, &PlainColumnEntry)],
        rows: &mut [RecordValue],
    ) -> Result<(), StorageError> {
        if bound_cols.is_empty() {
            return Ok(());
        }

        // All columns bound to this engine share the same referenced table and
        // row mapping. Use the first bound column to find the table path.
        let (first_desc_idx, _) = bound_cols[0];
        let first_col_desc = &ctx.col_descs[first_desc_idx];

        // Read the referenced table path.
        let ref_table_rel = get_string_keyword(
            &first_col_desc.keywords,
            FORWARD_TABLE_ROW_KEYWORD,
            &first_col_desc.col_name,
        )
        .or_else(|_| get_forward_table_name(&first_col_desc.keywords, &first_col_desc.col_name))?;
        let ref_table_path = add_directory(&ref_table_rel, ctx.table_path)?;

        // Read the row-mapping column name from the DM group keyword.
        let dm_group = &first_col_desc.data_manager_group;
        let row_col_keyword = format!("{dm_group}_ForwardColumn_RowName");
        let row_col_name = get_row_column_name(ctx, &row_col_keyword, dm_group)?;

        // Open the referenced table.
        let storage = CompositeStorage;
        let ref_snapshot = storage.load(&ref_table_path)?;

        for &(desc_idx, _pc) in bound_cols {
            let col_name = &ctx.col_descs[desc_idx].col_name;

            for (row_idx, row) in rows.iter_mut().enumerate() {
                // Read the row index from the mapping column.
                let mapped_row =
                    get_row_index(&ctx.rows[row_idx], &row_col_name).unwrap_or(row_idx);

                let value = if mapped_row < ref_snapshot.rows.len() {
                    ref_snapshot.rows[mapped_row]
                        .get(col_name)
                        .cloned()
                        .unwrap_or(Value::Scalar(casa_types::ScalarValue::Int32(0)))
                } else {
                    Value::Scalar(casa_types::ScalarValue::Int32(0))
                };
                row.push(RecordField::new(col_name.clone(), value));
            }
        }
        Ok(())
    }
}

/// Read a row index (UInt32) from a row record.
fn get_row_index(row: &RecordValue, col_name: &str) -> Option<usize> {
    match row.get(col_name)? {
        Value::Scalar(casa_types::ScalarValue::UInt32(v)) => Some(*v as usize),
        Value::Scalar(casa_types::ScalarValue::Int32(v)) => Some(*v as usize),
        Value::Scalar(casa_types::ScalarValue::Int64(v)) => Some(*v as usize),
        Value::Scalar(casa_types::ScalarValue::UInt16(v)) => Some(*v as usize),
        _ => None,
    }
}

/// Try to find the row column name from column keywords or table keywords.
fn get_row_column_name(
    ctx: &VirtualContext,
    keyword: &str,
    dm_group: &str,
) -> Result<String, StorageError> {
    // First try column keywords on any bound column.
    for col_desc in ctx.col_descs {
        if let Some(Value::Scalar(casa_types::ScalarValue::String(s))) =
            col_desc.keywords.get(keyword)
        {
            return Ok(s.clone());
        }
    }
    // Fallback: the row column name is conventionally stored as a table keyword.
    // If we can't find it, use a reasonable default.
    Err(StorageError::FormatMismatch(format!(
        "ForwardColumnIndexedRowEngine: cannot find row column name keyword '{keyword}' \
         for DM group '{dm_group}'"
    )))
}

/// Extract a string keyword value from a RecordValue.
fn get_string_keyword(kw: &RecordValue, key: &str, col_name: &str) -> Result<String, StorageError> {
    match kw.get(key) {
        Some(Value::Scalar(casa_types::ScalarValue::String(s))) => Ok(s.clone()),
        Some(_) => Err(StorageError::FormatMismatch(format!(
            "column '{col_name}': keyword '{key}' is not a string"
        ))),
        None => Err(StorageError::FormatMismatch(format!(
            "column '{col_name}': missing keyword '{key}'"
        ))),
    }
}

/// Extract the `_ForwardColumn_TableName` keyword value from a column's keywords.
fn get_forward_table_name(keywords: &RecordValue, col_name: &str) -> Result<String, StorageError> {
    match keywords.get(FORWARD_TABLE_KEYWORD) {
        Some(Value::Scalar(casa_types::ScalarValue::String(s))) => Ok(s.clone()),
        Some(_) => Err(StorageError::FormatMismatch(format!(
            "column '{col_name}': keyword '{FORWARD_TABLE_KEYWORD}' is not a string"
        ))),
        None => Err(StorageError::FormatMismatch(format!(
            "column '{col_name}': missing keyword '{FORWARD_TABLE_KEYWORD}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::data_type::CasacoreDataType;
    use crate::storage::table_control::ColumnDescContents;
    use casa_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
    use std::path::Path;
    use tempfile::tempdir;

    fn keyword_record(key: &str, value: Value) -> RecordValue {
        let mut record = RecordValue::default();
        record.upsert(key, value);
        record
    }

    fn column_desc(
        col_name: &str,
        data_manager_group: &str,
        keywords: RecordValue,
    ) -> ColumnDescContents {
        ColumnDescContents {
            class_name: String::new(),
            col_name: col_name.to_string(),
            comment: String::new(),
            data_manager_type: "ForwardColumnEngine".to_string(),
            data_manager_group: data_manager_group.to_string(),
            data_type: CasacoreDataType::TpInt,
            option: 0,
            nrdim: 0,
            shape: Vec::new(),
            max_length: 0,
            keywords,
            is_array: false,
            primitive_type: Some(PrimitiveType::Int32),
        }
    }

    #[test]
    fn helper_functions_cover_keyword_and_row_index_variants() {
        let string_kw = keyword_record(
            "name",
            Value::Scalar(ScalarValue::String("table".to_string())),
        );
        assert_eq!(
            get_string_keyword(&string_kw, "name", "col").unwrap(),
            "table".to_string()
        );
        assert!(matches!(
            get_string_keyword(
                &keyword_record("name", Value::Scalar(ScalarValue::Int32(1))),
                "name",
                "col"
            ),
            Err(StorageError::FormatMismatch(msg)) if msg.contains("not a string")
        ));
        assert!(matches!(
            get_string_keyword(&RecordValue::default(), "name", "col"),
            Err(StorageError::FormatMismatch(msg)) if msg.contains("missing keyword")
        ));

        let forward_kw = keyword_record(
            FORWARD_TABLE_KEYWORD,
            Value::Scalar(ScalarValue::String("ref".to_string())),
        );
        assert_eq!(
            get_forward_table_name(&forward_kw, "col").unwrap(),
            "ref".to_string()
        );
        assert!(matches!(
            get_forward_table_name(
                &keyword_record(FORWARD_TABLE_KEYWORD, Value::Scalar(ScalarValue::Int32(1))),
                "col"
            ),
            Err(StorageError::FormatMismatch(msg)) if msg.contains("is not a string")
        ));
        assert!(matches!(
            get_forward_table_name(&RecordValue::default(), "col"),
            Err(StorageError::FormatMismatch(msg)) if msg.contains("missing keyword")
        ));

        let row = RecordValue::new(vec![
            RecordField::new("u32", Value::Scalar(ScalarValue::UInt32(3))),
            RecordField::new("i32", Value::Scalar(ScalarValue::Int32(4))),
            RecordField::new("i64", Value::Scalar(ScalarValue::Int64(5))),
            RecordField::new("u16", Value::Scalar(ScalarValue::UInt16(6))),
            RecordField::new(
                "bad",
                Value::Scalar(ScalarValue::String("not-an-index".to_string())),
            ),
        ]);
        assert_eq!(get_row_index(&row, "u32"), Some(3));
        assert_eq!(get_row_index(&row, "i32"), Some(4));
        assert_eq!(get_row_index(&row, "i64"), Some(5));
        assert_eq!(get_row_index(&row, "u16"), Some(6));
        assert_eq!(get_row_index(&row, "bad"), None);
        assert_eq!(get_row_index(&row, "missing"), None);

        let ctx_descs = [column_desc(
            "value",
            "dm",
            keyword_record(
                "dm_ForwardColumn_RowName",
                Value::Scalar(ScalarValue::String("row_index".to_string())),
            ),
        )];
        let ctx = VirtualContext {
            col_descs: &ctx_descs,
            rows: &[],
            table_path: Path::new("/tmp"),
            nrrow: 0,
        };
        assert_eq!(
            get_row_column_name(&ctx, "dm_ForwardColumn_RowName", "dm").unwrap(),
            "row_index".to_string()
        );

        let ctx_missing_descs = [column_desc("value", "dm", RecordValue::default())];
        let ctx_missing = VirtualContext {
            col_descs: &ctx_missing_descs,
            rows: &[],
            table_path: Path::new("/tmp"),
            nrrow: 0,
        };
        assert!(matches!(
            get_row_column_name(&ctx_missing, "dm_ForwardColumn_RowName", "dm"),
            Err(StorageError::FormatMismatch(msg)) if msg.contains("cannot find row column name keyword")
        ));
    }

    #[test]
    fn forward_engine_reports_missing_reference_table() {
        let dir = tempdir().unwrap();

        let col_descs = [column_desc(
            "value",
            "dm",
            keyword_record(
                FORWARD_TABLE_KEYWORD,
                Value::Scalar(ScalarValue::String("ref".to_string())),
            ),
        )];
        let ctx = VirtualContext {
            col_descs: &col_descs,
            rows: &[],
            table_path: dir.path(),
            nrrow: 2,
        };
        let entry = PlainColumnEntry {
            original_name: "value".to_string(),
            dm_seq_nr: 0,
            is_array: false,
        };
        let mut rows = vec![RecordValue::default(), RecordValue::default()];

        assert!(matches!(
            ForwardColumnEngine.materialize(&ctx, &[(0, &entry)], &mut rows),
            Err(StorageError::MissingPath(_))
        ));
    }

    #[test]
    fn indexed_forward_engine_reports_missing_reference_table() {
        let dir = tempdir().unwrap();

        let col_descs = [column_desc(
            "value",
            "dm",
            keyword_record(
                FORWARD_TABLE_KEYWORD,
                Value::Scalar(ScalarValue::String("ref".to_string())),
            ),
        )];
        let mut first_keywords = col_descs[0].keywords.clone();
        first_keywords.upsert(
            "dm_ForwardColumn_RowName",
            Value::Scalar(ScalarValue::String("row_index".to_string())),
        );
        let col_descs = [ColumnDescContents {
            keywords: first_keywords,
            ..col_descs[0].clone()
        }];
        let ctx = VirtualContext {
            col_descs: &col_descs,
            rows: &[
                RecordValue::new(vec![RecordField::new(
                    "row_index",
                    Value::Scalar(ScalarValue::UInt32(0)),
                )]),
                RecordValue::default(),
            ],
            table_path: dir.path(),
            nrrow: 2,
        };
        let entry = PlainColumnEntry {
            original_name: "value".to_string(),
            dm_seq_nr: 0,
            is_array: false,
        };
        let mut rows = vec![RecordValue::default(), RecordValue::default()];

        assert!(matches!(
            ForwardColumnIndexedRowEngine.materialize(&ctx, &[(0, &entry)], &mut rows),
            Err(StorageError::MissingPath(_))
        ));
    }

    #[test]
    fn indexed_forward_engine_requires_row_column_name_keyword() {
        let dir = tempdir().unwrap();
        let col_descs = [column_desc(
            "value",
            "dm",
            keyword_record(
                FORWARD_TABLE_KEYWORD,
                Value::Scalar(ScalarValue::String("ref".to_string())),
            ),
        )];
        let ctx = VirtualContext {
            col_descs: &col_descs,
            rows: &[],
            table_path: dir.path(),
            nrrow: 0,
        };
        let entry = PlainColumnEntry {
            original_name: "value".to_string(),
            dm_seq_nr: 0,
            is_array: false,
        };
        let mut rows = Vec::new();

        let error = ForwardColumnIndexedRowEngine
            .materialize(&ctx, &[(0, &entry)], &mut rows)
            .unwrap_err();
        assert!(matches!(
            error,
            StorageError::FormatMismatch(msg) if msg.contains("cannot find row column name keyword")
        ));
    }
}
