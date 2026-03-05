// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sorting and grouped iteration over tables.
//!
//! This module provides [`TableIterator`] for grouping table rows by equal
//! values in one or more key columns, and [`TableGroup`] for representing
//! each group. Sorting itself is exposed through [`Table::sort`] and
//! [`Table::sort_by`].
//!
//! # C++ reference
//!
//! `Sort`, `TableIterator`, `BaseTableIterator`.

use std::cmp::Ordering;

use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

use crate::schema::ColumnType;
use crate::table::{SortOrder, Table, TableError};

/// A group of rows sharing equal key values, produced by [`TableIterator`].
///
/// Each group contains the key values that define the group and the indices
/// of all matching rows in the parent table.
///
/// # C++ equivalent
///
/// The `Table` returned by `TableIterator::table()`. The Rust version yields
/// owned data instead of a sub-`RefTable` to avoid mutable-borrow conflicts.
#[derive(Debug, Clone, PartialEq)]
pub struct TableGroup {
    /// The key column values shared by all rows in this group.
    ///
    /// Contains one field per key column, in the same order as the key columns
    /// were specified when creating the iterator.
    pub keys: RecordValue,
    /// Parent-table row indices for the rows in this group.
    pub row_indices: Vec<usize>,
}

/// An iterator that groups table rows by equal values in key columns.
///
/// Created via [`Table::iter_groups`]. The table is first sorted internally
/// by the key columns, then consecutive rows with equal key values are
/// collected into [`TableGroup`] values.
///
/// Implements the standard [`Iterator`] trait, yielding one [`TableGroup`]
/// per group.
///
/// # C++ equivalent
///
/// `casacore::TableIterator`. The C++ version yields sub-`Table` (RefTable)
/// objects per group; the Rust version yields owned [`TableGroup`] values to
/// avoid borrow conflicts.
pub struct TableIterator<'a> {
    table: &'a Table,
    sorted_indices: Vec<usize>,
    key_columns: Vec<String>,
    cursor: usize,
}

impl<'a> TableIterator<'a> {
    /// Creates a new iterator that groups rows by the given key columns.
    ///
    /// The table is sorted by the key columns first, then the iterator
    /// walks the sorted order, collecting consecutive rows with equal
    /// key values into groups.
    pub(crate) fn new(table: &'a Table, keys: &[(&str, SortOrder)]) -> Result<Self, TableError> {
        let sorted_indices = argsort(table, keys)?;
        let key_columns: Vec<String> = keys.iter().map(|&(k, _)| k.to_string()).collect();
        Ok(Self {
            table,
            sorted_indices,
            key_columns,
            cursor: 0,
        })
    }

    /// Creates a new iterator that groups rows in natural (insertion) order.
    ///
    /// No sorting is performed — consecutive rows with equal key values are
    /// grouped together, but non-adjacent duplicates remain in separate groups.
    ///
    /// # C++ equivalent
    ///
    /// `TableIterator` constructed with `TableIterator::NoSort`.
    pub(crate) fn new_nosort(table: &'a Table, key_columns: &[&str]) -> Result<Self, TableError> {
        for &col in key_columns {
            validate_sort_column(table, col)?;
        }
        let sorted_indices: Vec<usize> = (0..table.row_count()).collect();
        let key_columns: Vec<String> = key_columns.iter().map(|s| s.to_string()).collect();
        Ok(Self {
            table,
            sorted_indices,
            key_columns,
            cursor: 0,
        })
    }

    /// Extracts key column values from the given parent row index.
    fn extract_keys(&self, row_index: usize) -> RecordValue {
        let mut fields = Vec::with_capacity(self.key_columns.len());
        for col_name in &self.key_columns {
            let value = self
                .table
                .cell(row_index, col_name)
                .cloned()
                .unwrap_or(Value::Scalar(ScalarValue::Bool(false)));
            fields.push(RecordField::new(col_name.as_str(), value));
        }
        RecordValue::new(fields)
    }
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = TableGroup;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.sorted_indices.len() {
            return None;
        }

        let first = self.sorted_indices[self.cursor];
        let keys = self.extract_keys(first);

        let mut row_indices = vec![first];
        self.cursor += 1;

        while self.cursor < self.sorted_indices.len() {
            let idx = self.sorted_indices[self.cursor];
            let candidate_keys = self.extract_keys(idx);
            if candidate_keys != keys {
                break;
            }
            row_indices.push(idx);
            self.cursor += 1;
        }

        Some(TableGroup { keys, row_indices })
    }
}

/// Compute an indirect sort permutation (argsort) over the given key columns.
///
/// Returns a `Vec<usize>` of row indices sorted according to the key columns
/// and their respective [`SortOrder`]s.
pub(crate) fn argsort(table: &Table, keys: &[(&str, SortOrder)]) -> Result<Vec<usize>, TableError> {
    if keys.is_empty() {
        return Err(TableError::SortNoKeys);
    }

    // Validate all key columns.
    for &(col_name, _) in keys {
        validate_sort_column(table, col_name)?;
    }

    let n = table.row_count();
    let mut indices: Vec<usize> = (0..n).collect();

    indices.sort_by(|&a, &b| {
        for &(col_name, order) in keys {
            let val_a = table.cell(a, col_name).and_then(|v| match v {
                Value::Scalar(s) => Some(s),
                _ => None,
            });
            let val_b = table.cell(b, col_name).and_then(|v| match v {
                Value::Scalar(s) => Some(s),
                _ => None,
            });

            let cmp = match (val_a, val_b) {
                (Some(a), Some(b)) => a.sort_cmp(b).unwrap_or(Ordering::Equal),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };

            let cmp = match order {
                SortOrder::Ascending => cmp,
                SortOrder::Descending => cmp.reverse(),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });

    Ok(indices)
}

/// Validates that a column can be used as a sort key.
///
/// A sort key column must be scalar and must not be a Complex type.
pub(crate) fn validate_sort_column(table: &Table, col_name: &str) -> Result<(), TableError> {
    // Use schema if available for authoritative type info.
    if let Some(schema) = table.schema() {
        let col = schema
            .column(col_name)
            .ok_or_else(|| TableError::UnknownColumn {
                name: col_name.to_string(),
            })?;

        if !matches!(col.column_type(), ColumnType::Scalar) {
            return Err(TableError::SortKeyNotScalar {
                column: col_name.to_string(),
            });
        }

        if let Some(dt) = col.data_type() {
            if matches!(dt, PrimitiveType::Complex32 | PrimitiveType::Complex64) {
                return Err(TableError::SortKeyUnsortable {
                    column: col_name.to_string(),
                });
            }
        }

        return Ok(());
    }

    // Without schema, check the first row dynamically.
    if table.row_count() > 0 {
        match table.cell(0, col_name) {
            Some(Value::Scalar(sv)) => {
                if sv.sort_cmp(sv).is_none() {
                    return Err(TableError::SortKeyUnsortable {
                        column: col_name.to_string(),
                    });
                }
            }
            Some(_) => {
                return Err(TableError::SortKeyNotScalar {
                    column: col_name.to_string(),
                });
            }
            None => {} // Column not in this row; allow (may exist in other rows).
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, TableSchema};
    use casacore_types::PrimitiveType;

    /// Build a 5-row test table with (id: Int32, name: String).
    fn test_table() -> Table {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .unwrap();

        let mut table = Table::with_schema(schema);
        let data = [
            (4, "delta"),
            (1, "alpha"),
            (3, "charlie"),
            (0, "zero"),
            (2, "bravo"),
        ];
        for (id, name) in data {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
                    RecordField::new("name", Value::Scalar(ScalarValue::String(name.into()))),
                ]))
                .unwrap();
        }
        table
    }

    #[test]
    fn sort_single_column_ascending() {
        let mut table = test_table();
        let view = table.sort(&[("id", SortOrder::Ascending)]).unwrap();
        assert_eq!(view.row_count(), 5);
        // Should be 0,1,2,3,4.
        for i in 0..5 {
            let val = view.cell(i, "id").unwrap();
            assert_eq!(val, &Value::Scalar(ScalarValue::Int32(i as i32)));
        }
    }

    #[test]
    fn sort_single_column_descending() {
        let mut table = test_table();
        let view = table.sort(&[("id", SortOrder::Descending)]).unwrap();
        assert_eq!(view.row_count(), 5);
        // Should be 4,3,2,1,0.
        for i in 0..5 {
            let val = view.cell(i, "id").unwrap();
            assert_eq!(val, &Value::Scalar(ScalarValue::Int32((4 - i) as i32)));
        }
    }

    #[test]
    fn sort_multiple_columns_mixed_order() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("group", PrimitiveType::String),
            ColumnSchema::scalar("value", PrimitiveType::Int32),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let data = [("b", 2), ("a", 3), ("b", 1), ("a", 1), ("a", 2)];
        for (g, v) in data {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("group", Value::Scalar(ScalarValue::String(g.into()))),
                    RecordField::new("value", Value::Scalar(ScalarValue::Int32(v))),
                ]))
                .unwrap();
        }

        // Sort by group ascending, then value descending.
        let view = table
            .sort(&[
                ("group", SortOrder::Ascending),
                ("value", SortOrder::Descending),
            ])
            .unwrap();

        // Expected: a3, a2, a1, b2, b1
        let expected = [("a", 3), ("a", 2), ("a", 1), ("b", 2), ("b", 1)];
        for (i, (eg, ev)) in expected.iter().enumerate() {
            let g = view.cell(i, "group").unwrap();
            let v = view.cell(i, "value").unwrap();
            assert_eq!(g, &Value::Scalar(ScalarValue::String(eg.to_string())));
            assert_eq!(v, &Value::Scalar(ScalarValue::Int32(*ev)));
        }
    }

    #[test]
    fn sort_floats_with_nan() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Float64)]).unwrap();
        let mut table = Table::with_schema(schema);
        for v in [f64::NAN, 1.0, -0.0, f64::INFINITY, 0.0] {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Float64(v)),
                )]))
                .unwrap();
        }

        let view = table.sort(&[("x", SortOrder::Ascending)]).unwrap();
        // total_cmp order: -0.0 < 0.0 < 1.0 < inf < NaN
        let vals: Vec<f64> = (0..5)
            .map(|i| match view.cell(i, "x").unwrap() {
                Value::Scalar(ScalarValue::Float64(f)) => *f,
                _ => panic!("expected Float64"),
            })
            .collect();
        assert!(vals[0].is_sign_negative() && vals[0] == 0.0); // -0.0
        assert!(vals[1] == 0.0 && vals[1].is_sign_positive()); // +0.0
        assert_eq!(vals[2], 1.0);
        assert!(vals[3].is_infinite());
        assert!(vals[4].is_nan());
    }

    #[test]
    fn sort_complex_rejected() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("z", PrimitiveType::Complex64)]).unwrap();
        let mut table = Table::with_schema(schema);
        let result = table.sort(&[("z", SortOrder::Ascending)]);
        assert!(matches!(result, Err(TableError::SortKeyUnsortable { .. })));
    }

    #[test]
    fn sort_non_scalar_rejected() {
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "arr",
            PrimitiveType::Float64,
            Some(1),
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let result = table.sort(&[("arr", SortOrder::Ascending)]);
        assert!(matches!(result, Err(TableError::SortKeyNotScalar { .. })));
    }

    #[test]
    fn sort_no_keys() {
        let mut table = test_table();
        let result = table.sort(&[]);
        assert!(matches!(result, Err(TableError::SortNoKeys)));
    }

    #[test]
    fn sort_empty_table() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        let view = table.sort(&[("id", SortOrder::Ascending)]).unwrap();
        assert_eq!(view.row_count(), 0);
    }

    #[test]
    fn sort_by_custom_comparator() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("name", PrimitiveType::String)]).unwrap();
        let mut table = Table::with_schema(schema);
        for s in ["Charlie", "alpha", "BRAVO"] {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "name",
                    Value::Scalar(ScalarValue::String(s.into())),
                )]))
                .unwrap();
        }

        // Case-insensitive sort.
        let view = table
            .sort_by("name", |a, b| {
                let a = match a {
                    Value::Scalar(ScalarValue::String(s)) => s.to_lowercase(),
                    _ => String::new(),
                };
                let b = match b {
                    Value::Scalar(ScalarValue::String(s)) => s.to_lowercase(),
                    _ => String::new(),
                };
                a.cmp(&b)
            })
            .unwrap();

        let names: Vec<&str> = (0..3)
            .map(|i| match view.cell(i, "name").unwrap() {
                Value::Scalar(ScalarValue::String(s)) => s.as_str(),
                _ => panic!("expected String"),
            })
            .collect();
        assert_eq!(names, ["alpha", "BRAVO", "Charlie"]);
    }

    #[test]
    fn iter_groups_single_key() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("group", PrimitiveType::String),
            ColumnSchema::scalar("value", PrimitiveType::Int32),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let data = [("b", 1), ("a", 2), ("b", 3), ("a", 4), ("c", 5)];
        for (g, v) in data {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("group", Value::Scalar(ScalarValue::String(g.into()))),
                    RecordField::new("value", Value::Scalar(ScalarValue::Int32(v))),
                ]))
                .unwrap();
        }

        let groups: Vec<TableGroup> = table
            .iter_groups(&[("group", SortOrder::Ascending)])
            .unwrap()
            .collect();

        assert_eq!(groups.len(), 3);
        // Group "a": rows 1,3
        assert_eq!(
            groups[0].keys.get("group").unwrap(),
            &Value::Scalar(ScalarValue::String("a".into()))
        );
        assert_eq!(groups[0].row_indices.len(), 2);
        // Group "b": rows 0,2
        assert_eq!(
            groups[1].keys.get("group").unwrap(),
            &Value::Scalar(ScalarValue::String("b".into()))
        );
        assert_eq!(groups[1].row_indices.len(), 2);
        // Group "c": row 4
        assert_eq!(
            groups[2].keys.get("group").unwrap(),
            &Value::Scalar(ScalarValue::String("c".into()))
        );
        assert_eq!(groups[2].row_indices.len(), 1);
    }

    #[test]
    fn iter_groups_composite_key() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("k1", PrimitiveType::Int32),
            ColumnSchema::scalar("k2", PrimitiveType::String),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        let data = [(1, "a"), (2, "a"), (1, "b"), (1, "a"), (2, "b")];
        for (k1, k2) in data {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("k1", Value::Scalar(ScalarValue::Int32(k1))),
                    RecordField::new("k2", Value::Scalar(ScalarValue::String(k2.into()))),
                ]))
                .unwrap();
        }

        let groups: Vec<TableGroup> = table
            .iter_groups(&[("k1", SortOrder::Ascending), ("k2", SortOrder::Ascending)])
            .unwrap()
            .collect();

        // Groups: (1,"a"), (1,"b"), (2,"a"), (2,"b")
        assert_eq!(groups.len(), 4);
        assert_eq!(groups[0].row_indices.len(), 2); // rows 0,3
        assert_eq!(groups[1].row_indices.len(), 1); // row 2
        assert_eq!(groups[2].row_indices.len(), 1); // row 1
        assert_eq!(groups[3].row_indices.len(), 1); // row 4
    }

    #[test]
    fn iter_groups_all_equal() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("k", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        for _ in 0..4 {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "k",
                    Value::Scalar(ScalarValue::Int32(42)),
                )]))
                .unwrap();
        }

        let groups: Vec<TableGroup> = table
            .iter_groups(&[("k", SortOrder::Ascending)])
            .unwrap()
            .collect();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].row_indices.len(), 4);
    }

    #[test]
    fn iter_groups_all_distinct() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("k", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        for i in 0..5 {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "k",
                    Value::Scalar(ScalarValue::Int32(i)),
                )]))
                .unwrap();
        }

        let groups: Vec<TableGroup> = table
            .iter_groups(&[("k", SortOrder::Ascending)])
            .unwrap()
            .collect();
        assert_eq!(groups.len(), 5);
        for group in &groups {
            assert_eq!(group.row_indices.len(), 1);
        }
    }

    #[test]
    fn sorted_ref_table_save_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let parent_path = dir.path().join("parent.tbl");
        let sorted_path = dir.path().join("sorted.tbl");

        // Build and save parent.
        let mut table = test_table();
        table
            .save(crate::table::TableOptions::new(&parent_path))
            .unwrap();
        table.set_path(&parent_path);

        // Sort descending by id and save as RefTable.
        let sorted = table.sort(&[("id", SortOrder::Descending)]).unwrap();
        sorted
            .save(crate::table::TableOptions::new(&sorted_path))
            .unwrap();
        drop(sorted);

        // Reopen — materializes the sorted RefTable.
        let reopened = Table::open(crate::table::TableOptions::new(&sorted_path)).unwrap();
        assert_eq!(reopened.row_count(), 5);
        // Should be 4,3,2,1,0.
        for i in 0..5 {
            let val = reopened.cell(i, "id").unwrap();
            assert_eq!(val, &Value::Scalar(ScalarValue::Int32((4 - i) as i32)));
        }
    }
}
