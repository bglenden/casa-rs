// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reference table — a view over a parent table's rows and columns.
//!
//! A [`RefTable`] holds a mutable borrow of a parent [`Table`] together with
//! a row index map and an optional column projection. Cell reads and writes
//! pass through to the parent, translating row indices on the fly.
//!
//! # C++ reference
//!
//! `RefTable`, `RefColumn`, `RefRows`.

use std::path::Path;

use casacore_types::{RecordValue, Value};

use crate::schema::{ColumnSchema, TableSchema};
use crate::storage::{CompositeStorage, RefTableDatContents, strip_directory};
use crate::table::{Table, TableError};

/// A view over a parent [`Table`]'s rows and/or columns.
///
/// `RefTable` borrows the parent mutably, so the parent cannot be accessed
/// while the view exists. Drop the `RefTable` to regain access to the parent.
///
/// # Construction
///
/// Use one of the selection methods on [`Table`]:
/// - [`Table::select_rows`] — pick specific rows by index
/// - [`Table::select_columns`] — pick specific columns by name
/// - [`Table::select`] — filter rows with a predicate closure
///
/// # Persistence
///
/// [`RefTable::save`] writes a `table.dat` with a `"RefTable"` type marker
/// that is binary-compatible with C++ casacore. The file stores the relative
/// path to the parent table and the row/column mapping — no column data is
/// duplicated.
///
/// # C++ equivalent
///
/// `casacore::RefTable`.
pub struct RefTable<'a> {
    parent: &'a mut Table,
    row_map: Vec<usize>,
    column_names: Vec<String>,
    /// (view_name, parent_name) pairs for column name translation.
    column_name_map: Vec<(String, String)>,
    row_order: bool,
}

impl<'a> RefTable<'a> {
    /// Creates a new RefTable from a parent table and row indices.
    pub(crate) fn from_rows(
        parent: &'a mut Table,
        row_map: Vec<usize>,
    ) -> Result<Self, TableError> {
        let count = parent.row_count();
        for &idx in &row_map {
            if idx >= count {
                return Err(TableError::RowIndexOutOfRange { index: idx, count });
            }
        }

        // All columns, same names.
        let column_names: Vec<String> = parent
            .schema()
            .map(|s| s.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default();
        let column_name_map: Vec<(String, String)> = column_names
            .iter()
            .map(|n| (n.clone(), n.clone()))
            .collect();

        let row_order = row_map.windows(2).all(|w| w[0] < w[1]);

        Ok(Self {
            parent,
            row_map,
            column_names,
            column_name_map,
            row_order,
        })
    }

    /// Creates a new RefTable from a parent table and column names.
    pub(crate) fn from_columns(
        parent: &'a mut Table,
        columns: &[&str],
    ) -> Result<Self, TableError> {
        if let Some(schema) = parent.schema() {
            for &name in columns {
                if !schema.columns().iter().any(|c| c.name() == name) {
                    return Err(TableError::UnknownColumn {
                        name: name.to_string(),
                    });
                }
            }
        }

        let row_count = parent.row_count();
        let row_map: Vec<usize> = (0..row_count).collect();
        let column_names: Vec<String> = columns.iter().map(|&s| s.to_string()).collect();
        let column_name_map: Vec<(String, String)> = column_names
            .iter()
            .map(|n| (n.clone(), n.clone()))
            .collect();

        Ok(Self {
            parent,
            row_map,
            column_names,
            column_name_map,
            row_order: true,
        })
    }

    /// Creates a new RefTable from a parent table and a predicate.
    pub(crate) fn from_predicate<F>(parent: &'a mut Table, predicate: F) -> Self
    where
        F: Fn(&RecordValue) -> bool,
    {
        let row_map: Vec<usize> = (0..parent.row_count())
            .filter(|&i| parent.row(i).map(&predicate).unwrap_or(false))
            .collect();

        let column_names: Vec<String> = parent
            .schema()
            .map(|s| s.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default();
        let column_name_map: Vec<(String, String)> = column_names
            .iter()
            .map(|n| (n.clone(), n.clone()))
            .collect();

        Self {
            parent,
            row_map,
            column_names,
            column_name_map,
            row_order: true, // predicate preserves order
        }
    }

    /// Returns the number of rows in this view.
    pub fn row_count(&self) -> usize {
        self.row_map.len()
    }

    /// Returns a reference to the full parent row at view index `index`.
    ///
    /// The returned `RecordValue` contains all parent columns, not just the
    /// projected subset. Use [`cell`](RefTable::cell) for column-aware access.
    pub fn row(&self, index: usize) -> Option<&RecordValue> {
        let parent_idx = *self.row_map.get(index)?;
        self.parent.row(parent_idx)
    }

    /// Returns a reference to the cell value at `(row, column)`.
    ///
    /// Both the row index and column name are translated through the view's
    /// mappings. Returns an error if the column name is not in this view's
    /// projection, or if the row index is out of range.
    pub fn cell(&self, row: usize, column: &str) -> Result<&Value, TableError> {
        let parent_idx = self.translate_row(row)?;
        let parent_col = self.translate_column(column)?;
        self.parent
            .cell(parent_idx, parent_col)
            .ok_or_else(|| TableError::ColumnNotFound {
                row_index: parent_idx,
                column: parent_col.to_string(),
            })
    }

    /// Sets a cell value, writing through to the parent table.
    pub fn set_cell(&mut self, row: usize, column: &str, value: Value) -> Result<(), TableError> {
        let parent_idx = self.translate_row(row)?;
        let parent_col = self.translate_column(column)?.to_string();
        self.parent.set_cell(parent_idx, &parent_col, value)
    }

    /// Returns the row number mapping (view row → parent row).
    pub fn row_numbers(&self) -> &[usize] {
        &self.row_map
    }

    /// Returns the column names visible in this view.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Returns a projected schema containing only the columns in this view.
    pub fn schema(&self) -> Option<TableSchema> {
        let parent_schema = self.parent.schema()?;
        let cols: Vec<ColumnSchema> = self
            .column_names
            .iter()
            .filter_map(|name| {
                let parent_name = self
                    .column_name_map
                    .iter()
                    .find(|(v, _)| v == name)
                    .map(|(_, p)| p.as_str())
                    .unwrap_or(name.as_str());
                parent_schema
                    .columns()
                    .iter()
                    .find(|c| c.name() == parent_name)
                    .cloned()
            })
            .collect();
        TableSchema::new(cols).ok()
    }

    /// Returns the parent table's disk path, if it was loaded from disk.
    pub fn parent_path(&self) -> Option<&Path> {
        self.parent.path()
    }

    /// Saves this reference table to disk in C++-compatible format.
    ///
    /// The parent table must have been saved to disk (i.e. have a
    /// [`path()`](Table::path)) so that the relative path can be computed.
    /// Only metadata is written — no column data files.
    pub fn save(&self, opts: crate::table::TableOptions) -> Result<(), TableError> {
        let parent_path = self.parent.path().ok_or(TableError::ParentNotSaved)?;

        // Compute relative path from ref table directory to parent.
        let ref_path = opts.path();
        let parent_relative = strip_directory(parent_path, ref_path);

        let ref_dat = RefTableDatContents {
            nrrow: self.row_map.len() as u64,
            big_endian: opts.endian_format().is_big_endian(),
            parent_relative_path: parent_relative,
            column_name_map: self.column_name_map.clone(),
            column_names: self.column_names.clone(),
            parent_nrrow: self.parent.row_count() as u64,
            row_order: self.row_order,
            row_map: self.row_map.iter().map(|&r| r as u64).collect(),
        };

        let storage = CompositeStorage;
        storage
            .save_ref_table(ref_path, &ref_dat)
            .map_err(|e| TableError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Translate a view row index to the parent row index.
    fn translate_row(&self, index: usize) -> Result<usize, TableError> {
        self.row_map
            .get(index)
            .copied()
            .ok_or(TableError::RowIndexOutOfRange {
                index,
                count: self.row_map.len(),
            })
    }

    /// Translate a view column name to the parent column name.
    fn translate_column<'b>(&'b self, name: &'b str) -> Result<&'b str, TableError> {
        // Look up in the column name map.
        for (view_name, parent_name) in &self.column_name_map {
            if view_name == name {
                return Ok(parent_name.as_str());
            }
        }
        Err(TableError::UnknownColumn {
            name: name.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casacore_types::{PrimitiveType, RecordField, ScalarValue};

    fn test_table() -> Table {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .unwrap();

        let mut table = Table::with_schema(schema);
        for i in 0..5 {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
                    RecordField::new(
                        "name",
                        Value::Scalar(ScalarValue::String(format!("row_{i}"))),
                    ),
                ]))
                .unwrap();
        }
        table
    }

    #[test]
    fn select_rows_basic() {
        let mut table = test_table();
        let view = table.select_rows(&[1, 3]).unwrap();
        assert_eq!(view.row_count(), 2);

        let id = view.cell(0, "id").unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(1)));

        let id = view.cell(1, "id").unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(3)));
    }

    #[test]
    fn select_rows_out_of_range() {
        let mut table = test_table();
        let result = table.select_rows(&[0, 99]);
        assert!(matches!(
            result,
            Err(TableError::RowIndexOutOfRange {
                index: 99,
                count: 5
            })
        ));
    }

    #[test]
    fn select_columns_basic() {
        let mut table = test_table();
        let view = table.select_columns(&["name"]).unwrap();
        assert_eq!(view.row_count(), 5);
        assert_eq!(view.column_names(), &["name"]);

        // Can access projected column.
        let name = view.cell(0, "name").unwrap();
        assert_eq!(
            name,
            &Value::Scalar(ScalarValue::String("row_0".to_string()))
        );

        // Cannot access non-projected column through cell().
        let result = view.cell(0, "id");
        assert!(matches!(result, Err(TableError::UnknownColumn { .. })));
    }

    #[test]
    fn select_columns_unknown() {
        let mut table = test_table();
        let result = table.select_columns(&["nonexistent"]);
        assert!(matches!(result, Err(TableError::UnknownColumn { .. })));
    }

    #[test]
    fn select_predicate() {
        let mut table = test_table();
        let view = table.select(|row| {
            row.get("id")
                .map(|v| matches!(v, Value::Scalar(ScalarValue::Int32(i)) if *i >= 3))
                .unwrap_or(false)
        });
        assert_eq!(view.row_count(), 2);
        assert_eq!(view.row_numbers(), &[3, 4]);
    }

    #[test]
    fn write_through_view() {
        let mut table = test_table();
        {
            let mut view = table.select_rows(&[2]).unwrap();
            view.set_cell(
                0,
                "name",
                Value::Scalar(ScalarValue::String("modified".into())),
            )
            .unwrap();
        }
        // Parent is updated.
        let name = table.cell(2, "name").unwrap();
        assert_eq!(
            name,
            &Value::Scalar(ScalarValue::String("modified".to_string()))
        );
    }

    #[test]
    fn ref_table_schema_projection() {
        let mut table = test_table();
        let view = table.select_columns(&["name"]).unwrap();
        let schema = view.schema().unwrap();
        assert_eq!(schema.columns().len(), 1);
        assert_eq!(schema.columns()[0].name(), "name");
    }

    #[test]
    fn save_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let parent_path = dir.path().join("parent.tbl");
        let ref_path = dir.path().join("ref.tbl");

        let table = test_table();
        table
            .save(crate::table::TableOptions::new(&parent_path))
            .unwrap();

        // Re-open so source_path is set.
        let mut table = Table::open(crate::table::TableOptions::new(&parent_path)).unwrap();
        let view = table.select_rows(&[0, 2, 4]).unwrap();
        view.save(crate::table::TableOptions::new(&ref_path))
            .unwrap();

        // Reopen the ref table — materializes the view.
        let reopened = Table::open(crate::table::TableOptions::new(&ref_path)).unwrap();
        assert_eq!(reopened.row_count(), 3);

        let id = reopened.cell(0, "id").unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(0)));
        let id = reopened.cell(1, "id").unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(2)));
        let id = reopened.cell(2, "id").unwrap();
        assert_eq!(id, &Value::Scalar(ScalarValue::Int32(4)));
    }

    #[test]
    fn row_numbers_accessor() {
        let mut table = test_table();
        let view = table.select_rows(&[4, 1, 3]).unwrap();
        assert_eq!(view.row_numbers(), &[4, 1, 3]);
        assert!(!view.row_order); // not ascending
    }

    #[test]
    fn strip_directory_siblings() {
        // Siblings in the same parent directory → "./" prefix.
        let rel = strip_directory(Path::new("/a/b/parent.tbl"), Path::new("/a/b/ref.tbl"));
        assert_eq!(rel, "./parent.tbl");
    }

    #[test]
    fn strip_directory_nested() {
        // Parent is inside the ref table directory → "././" prefix.
        let rel = strip_directory(Path::new("/a/b/ref.tbl/sub"), Path::new("/a/b/ref.tbl"));
        assert_eq!(rel, "././sub");
    }
}
