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

use casacore_types::{RecordField, RecordValue, Value};

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
                        .unwrap_or(Value::Scalar(casacore_types::ScalarValue::Int32(0)))
                } else {
                    Value::Scalar(casacore_types::ScalarValue::Int32(0))
                };
                row.push(RecordField::new(col_name.clone(), value));
            }
        }
        Ok(())
    }
}

/// Extract the `_ForwardColumn_TableName` keyword value from a column's keywords.
fn get_forward_table_name(keywords: &RecordValue, col_name: &str) -> Result<String, StorageError> {
    match keywords.get(FORWARD_TABLE_KEYWORD) {
        Some(Value::Scalar(casacore_types::ScalarValue::String(s))) => Ok(s.clone()),
        Some(_) => Err(StorageError::FormatMismatch(format!(
            "column '{col_name}': keyword '{FORWARD_TABLE_KEYWORD}' is not a string"
        ))),
        None => Err(StorageError::FormatMismatch(format!(
            "column '{col_name}': missing keyword '{FORWARD_TABLE_KEYWORD}'"
        ))),
    }
}
