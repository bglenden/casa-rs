// SPDX-License-Identifier: LGPL-3.0-or-later
//! Concatenated table — a virtual union of tables with the same schema.
//!
//! A [`ConcatTable`] owns two or more tables and presents them as a single
//! logical table. Row reads dispatch to the correct underlying table via
//! binary search on cumulative row offsets. No data is copied.
//!
//! # C++ reference
//!
//! `ConcatTable`, `ConcatColumn`, `ConcatRows`.

use std::collections::HashMap;

use casacore_types::{RecordValue, Value};

use crate::schema::TableSchema;
use crate::storage::{
    CompositeStorage, StorageManager, StorageSnapshot, strip_directory,
    table_control::ConcatTableDatContents,
};
use crate::table::{Table, TableError, TableOptions};

/// Row mapping for concatenated tables.
///
/// Stores cumulative row counts as a prefix-sum array. Given N tables with
/// row counts \[r₀, r₁, …, rₙ₋₁\], the offsets array holds
/// \[0, r₀, r₀+r₁, …\]. A global row number is mapped to
/// `(table_index, local_row)` via binary search on the prefix sums.
///
/// # C++ equivalent
///
/// `casacore::ConcatRows`.
struct ConcatRows {
    /// Prefix sums: `offsets[0] = 0`, `offsets[i+1] = offsets[i] + tables[i].row_count()`.
    offsets: Vec<usize>,
}

impl ConcatRows {
    /// Creates a new `ConcatRows` from a slice of tables.
    fn new(tables: &[Table]) -> Self {
        let mut offsets = Vec::with_capacity(tables.len() + 1);
        offsets.push(0);
        for table in tables {
            let prev = *offsets.last().unwrap();
            offsets.push(prev + table.row_count());
        }
        Self { offsets }
    }

    /// Returns the total number of rows across all tables.
    fn total_rows(&self) -> usize {
        *self.offsets.last().unwrap_or(&0)
    }

    /// Maps a global row index to `(table_index, local_row)`.
    ///
    /// Returns `None` if `global_row` is out of range.
    fn map_row(&self, global_row: usize) -> Option<(usize, usize)> {
        if global_row >= self.total_rows() {
            return None;
        }
        // Binary search: find the last offset ≤ global_row.
        let pos = self.offsets.partition_point(|&o| o <= global_row);
        // partition_point returns the first index where offset > global_row,
        // so the table index is pos - 1.
        let table_idx = pos - 1;
        let local_row = global_row - self.offsets[table_idx];
        Some((table_idx, local_row))
    }
}

/// A virtual table formed by concatenating two or more tables with the same
/// schema.
///
/// `ConcatTable` owns the constituent tables and presents them as a single
/// logical table whose row count is the sum of all parts. Cell reads dispatch
/// to the correct underlying table using binary search on cumulative row
/// counts.
///
/// # Construction
///
/// Use [`Table::concat`] to create a `ConcatTable`:
///
/// ```rust,no_run
/// # use casacore_tables::{Table, TableSchema, ColumnSchema};
/// # use casacore_types::PrimitiveType;
/// # let schema = TableSchema::new(vec![
/// #     ColumnSchema::scalar("id", PrimitiveType::Int32),
/// # ]).unwrap();
/// # let table_a = Table::with_schema(schema.clone());
/// # let table_b = Table::with_schema(schema);
/// let concat = Table::concat(vec![table_a, table_b]).unwrap();
/// ```
///
/// # Persistence
///
/// [`ConcatTable::save`] writes a `table.dat` with a `"ConcatTable"` type
/// marker that is binary-compatible with C++ casacore. The file stores
/// relative paths to each constituent table (which must themselves have been
/// saved to disk).
///
/// When reopened via [`Table::open`], the concatenation is materialized: all
/// constituent tables are loaded and their rows collected into a single table.
///
/// # C++ equivalent
///
/// `casacore::ConcatTable`.
pub struct ConcatTable {
    tables: Vec<Table>,
    rows: ConcatRows,
    schema: TableSchema,
    keywords: RecordValue,
    column_keywords: HashMap<String, RecordValue>,
}

impl ConcatTable {
    /// Creates a new `ConcatTable` from a vector of tables.
    ///
    /// All tables must have identical schemas (same column names, types, and
    /// shapes). Returns [`TableError::SchemaMismatch`] if schemas differ, or
    /// [`TableError::ConcatEmpty`] if the vector is empty.
    pub(crate) fn new(tables: Vec<Table>) -> Result<Self, TableError> {
        if tables.is_empty() {
            return Err(TableError::ConcatEmpty);
        }

        let schema = tables[0]
            .schema()
            .cloned()
            .ok_or_else(|| TableError::SchemaMismatch {
                message: "first table has no schema".to_string(),
            })?;

        for (i, table) in tables.iter().enumerate().skip(1) {
            let other = table.schema().ok_or_else(|| TableError::SchemaMismatch {
                message: format!("table {i} has no schema"),
            })?;
            if *other != schema {
                return Err(TableError::SchemaMismatch {
                    message: format!(
                        "table {i} schema differs from table 0 ({} columns vs {})",
                        other.columns().len(),
                        schema.columns().len()
                    ),
                });
            }
        }

        let keywords = tables[0].keywords().clone();
        let column_keywords: HashMap<String, RecordValue> = schema
            .columns()
            .iter()
            .filter_map(|col| {
                tables[0]
                    .column_keywords(col.name())
                    .cloned()
                    .map(|kw| (col.name().to_string(), kw))
            })
            .collect();

        let rows = ConcatRows::new(&tables);

        Ok(Self {
            tables,
            rows,
            schema,
            keywords,
            column_keywords,
        })
    }

    /// Returns the total number of rows across all constituent tables.
    pub fn row_count(&self) -> usize {
        self.rows.total_rows()
    }

    /// Returns the number of constituent tables.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Returns the shared schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Returns a reference to the constituent tables.
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    /// Returns table-level keywords (from the first constituent table).
    pub fn keywords(&self) -> &RecordValue {
        &self.keywords
    }

    /// Returns column keywords for the named column (from the first
    /// constituent table).
    pub fn column_keywords(&self, column: &str) -> Option<&RecordValue> {
        self.column_keywords.get(column)
    }

    /// Returns a reference to the row at the given global index.
    ///
    /// The global index spans all constituent tables: rows `0..n₀` come from
    /// the first table, `n₀..n₀+n₁` from the second, and so on.
    pub fn row(&self, index: usize) -> Option<&RecordValue> {
        let (table_idx, local_row) = self.rows.map_row(index)?;
        self.tables[table_idx].row(local_row)
    }

    /// Returns a reference to the cell value at `(row, column)`.
    ///
    /// Returns an error if the column name does not exist in the schema or
    /// if the row index is out of range.
    pub fn cell(&self, row: usize, column: &str) -> Result<&Value, TableError> {
        let (table_idx, local_row) = self.rows.map_row(row).ok_or(TableError::RowOutOfBounds {
            row_index: row,
            row_count: self.row_count(),
        })?;
        self.tables[table_idx]
            .cell(local_row, column)
            .ok_or_else(|| TableError::ColumnNotFound {
                row_index: local_row,
                column: column.to_string(),
            })
    }

    /// Saves this concatenated table to disk in C++-compatible format.
    ///
    /// Each constituent table must have a disk path (i.e. must have been
    /// saved or opened from disk). The `table.dat` file stores relative
    /// paths to each constituent table, plus an empty list of subtable
    /// names to concatenate.
    ///
    /// # C++ equivalent
    ///
    /// `ConcatTable::writeConcatTable`.
    pub fn save(&self, opts: TableOptions) -> Result<(), TableError> {
        let concat_path = opts.path();

        // Compute relative paths for each constituent table.
        let mut table_paths = Vec::with_capacity(self.tables.len());
        for (i, table) in self.tables.iter().enumerate() {
            let src_path = table
                .path()
                .ok_or(TableError::ConstituentNotSaved { index: i })?;
            table_paths.push(strip_directory(src_path, concat_path));
        }

        let contents = ConcatTableDatContents {
            nrrow: self.row_count() as u64,
            big_endian: opts.endian_format().is_big_endian(),
            table_paths,
            sub_table_names: Vec::new(),
        };

        let storage = CompositeStorage;
        storage
            .save_concat_table(concat_path, &contents)
            .map_err(|e| TableError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Creates a deep copy of this concatenated table as a standalone table.
    ///
    /// All rows from all constituent tables are collected and written to a
    /// new physical table at the specified path with the specified storage
    /// manager. This "materializes" the virtual concatenation.
    ///
    /// # C++ equivalent
    ///
    /// `TableCopy::makeEmptyTable` + `TableCopy::copyRows`.
    pub fn deep_copy(&self, opts: TableOptions) -> Result<(), TableError> {
        let mut all_rows = Vec::with_capacity(self.row_count());
        for table in &self.tables {
            all_rows.extend(table.rows().iter().cloned());
        }

        let snapshot = StorageSnapshot {
            rows: all_rows,
            keywords: self.keywords.clone(),
            column_keywords: self.column_keywords.clone(),
            schema: Some(self.schema.clone()),
        };

        let storage = CompositeStorage;
        storage.save(
            opts.path(),
            &snapshot,
            opts.data_manager(),
            opts.endian_format().is_big_endian(),
            opts.tile_shape(),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use casacore_types::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

    use crate::schema::{ColumnSchema, TableSchema};
    use crate::table::{DataManagerKind, Table, TableError, TableOptions};

    /// Build a table with the given id range: schema (id: Int32, name: String).
    fn build_table(ids: std::ops::Range<i32>) -> Table {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .unwrap();

        let mut table = Table::with_schema(schema);
        for i in ids {
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
    fn concat_two_tables_basic() {
        let t0 = build_table(0..5);
        let t1 = build_table(5..10);
        let concat = Table::concat(vec![t0, t1]).unwrap();
        assert_eq!(concat.row_count(), 10);
        assert_eq!(concat.table_count(), 2);
    }

    #[test]
    fn concat_row_access_spans_boundary() {
        let t0 = build_table(0..5);
        let t1 = build_table(5..10);
        let concat = Table::concat(vec![t0, t1]).unwrap();

        // Last row of first table.
        let row4 = concat.row(4).expect("row 4 exists");
        assert_eq!(
            row4.get("id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(4))
        );

        // First row of second table.
        let row5 = concat.row(5).expect("row 5 exists");
        assert_eq!(
            row5.get("id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(5))
        );
    }

    #[test]
    fn concat_cell_access() {
        let t0 = build_table(0..3);
        let t1 = build_table(10..13);
        let concat = Table::concat(vec![t0, t1]).unwrap();

        assert_eq!(
            concat.cell(0, "id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(0))
        );
        assert_eq!(
            concat.cell(3, "id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(10))
        );
        assert_eq!(
            concat.cell(5, "name").unwrap(),
            &Value::Scalar(ScalarValue::String("row_12".to_string()))
        );
    }

    #[test]
    fn concat_schema_mismatch_rejected() {
        let t0 = build_table(0..3);
        let schema2 =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Float64)]).unwrap();
        let t1 = Table::with_schema(schema2);

        let result = Table::concat(vec![t0, t1]);
        assert!(matches!(result, Err(TableError::SchemaMismatch { .. })));
    }

    #[test]
    fn concat_empty_vec_rejected() {
        let result = Table::concat(vec![]);
        assert!(matches!(result, Err(TableError::ConcatEmpty)));
    }

    #[test]
    fn concat_single_table() {
        let t = build_table(0..5);
        let concat = Table::concat(vec![t]).unwrap();
        assert_eq!(concat.row_count(), 5);
        assert_eq!(concat.table_count(), 1);

        for i in 0..5 {
            assert_eq!(
                concat.cell(i, "id").unwrap(),
                &Value::Scalar(ScalarValue::Int32(i as i32))
            );
        }
    }

    #[test]
    fn concat_three_tables() {
        let t0 = build_table(0..3);
        let t1 = build_table(10..12);
        let t2 = build_table(20..24);
        let concat = Table::concat(vec![t0, t1, t2]).unwrap();
        assert_eq!(concat.row_count(), 9);
        assert_eq!(concat.table_count(), 3);

        // Verify dispatch to each table.
        assert_eq!(
            concat.cell(2, "id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(2))
        );
        assert_eq!(
            concat.cell(3, "id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(10))
        );
        assert_eq!(
            concat.cell(5, "id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(20))
        );
        assert_eq!(
            concat.cell(8, "id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(23))
        );
    }

    #[test]
    fn concat_save_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let p0 = dir.path().join("part0.tbl");
        let p1 = dir.path().join("part1.tbl");
        let concat_path = dir.path().join("concat.tbl");

        let t0 = build_table(0..3);
        t0.save(TableOptions::new(&p0)).unwrap();
        let mut t0 = Table::open(TableOptions::new(&p0)).unwrap();
        t0.set_path(&p0);

        let t1 = build_table(10..13);
        t1.save(TableOptions::new(&p1)).unwrap();
        let mut t1 = Table::open(TableOptions::new(&p1)).unwrap();
        t1.set_path(&p1);

        let concat = Table::concat(vec![t0, t1]).unwrap();
        concat.save(TableOptions::new(&concat_path)).unwrap();

        // Reopen — materializes as plain Table.
        let reopened = Table::open(TableOptions::new(&concat_path)).unwrap();
        assert_eq!(reopened.row_count(), 6);

        let expected_ids = [0, 1, 2, 10, 11, 12];
        for (i, &expected) in expected_ids.iter().enumerate() {
            assert_eq!(
                reopened.cell(i, "id").unwrap(),
                &Value::Scalar(ScalarValue::Int32(expected)),
                "row {i} id mismatch"
            );
        }
    }

    #[test]
    fn concat_keywords_from_first() {
        let mut t0 = build_table(0..3);
        t0.keywords_mut().push(RecordField::new(
            "origin",
            Value::Scalar(ScalarValue::String("first_table".to_string())),
        ));

        let t1 = build_table(10..13);
        let concat = Table::concat(vec![t0, t1]).unwrap();

        assert_eq!(
            concat.keywords().get("origin").unwrap(),
            &Value::Scalar(ScalarValue::String("first_table".to_string()))
        );
    }

    #[test]
    fn deep_copy_with_dm_conversion() {
        let dir = tempfile::tempdir().unwrap();
        let original_path = dir.path().join("original.tbl");
        let copy_path = dir.path().join("copy.tbl");

        let table = build_table(0..5);
        table.save(TableOptions::new(&original_path)).unwrap();

        let table = Table::open(TableOptions::new(&original_path)).unwrap();
        table
            .deep_copy(
                TableOptions::new(&copy_path).with_data_manager(DataManagerKind::StandardStMan),
            )
            .unwrap();

        let reopened = Table::open(TableOptions::new(&copy_path)).unwrap();
        assert_eq!(reopened.row_count(), 5);
        for i in 0..5 {
            assert_eq!(
                reopened.cell(i, "id").unwrap(),
                &Value::Scalar(ScalarValue::Int32(i as i32)),
                "row {i} id mismatch after deep copy"
            );
        }
    }

    #[test]
    fn deep_copy_preserves_keywords() {
        let dir = tempfile::tempdir().unwrap();
        let original_path = dir.path().join("original.tbl");
        let copy_path = dir.path().join("copy.tbl");

        let mut table = build_table(0..3);
        table.keywords_mut().push(RecordField::new(
            "author",
            Value::Scalar(ScalarValue::String("test".to_string())),
        ));
        table.set_column_keywords(
            "id",
            RecordValue::new(vec![RecordField::new(
                "unit",
                Value::Scalar(ScalarValue::String("count".to_string())),
            )]),
        );
        table.save(TableOptions::new(&original_path)).unwrap();

        let table = Table::open(TableOptions::new(&original_path)).unwrap();
        table.deep_copy(TableOptions::new(&copy_path)).unwrap();

        let reopened = Table::open(TableOptions::new(&copy_path)).unwrap();
        assert_eq!(
            reopened.keywords().get("author").unwrap(),
            &Value::Scalar(ScalarValue::String("test".to_string()))
        );
        let col_kw = reopened.column_keywords("id").unwrap();
        assert_eq!(
            col_kw.get("unit").unwrap(),
            &Value::Scalar(ScalarValue::String("count".to_string()))
        );
    }

    #[test]
    fn shallow_copy_zero_rows() {
        let dir = tempfile::tempdir().unwrap();
        let original_path = dir.path().join("original.tbl");
        let copy_path = dir.path().join("shallow.tbl");

        let mut table = build_table(0..10);
        table.keywords_mut().push(RecordField::new(
            "key",
            Value::Scalar(ScalarValue::Int32(42)),
        ));
        table.save(TableOptions::new(&original_path)).unwrap();

        let table = Table::open(TableOptions::new(&original_path)).unwrap();
        table.shallow_copy(TableOptions::new(&copy_path)).unwrap();

        let reopened = Table::open(TableOptions::new(&copy_path)).unwrap();
        assert_eq!(reopened.row_count(), 0);
        assert!(reopened.schema().is_some());
        assert_eq!(
            reopened.schema().unwrap().columns().len(),
            2,
            "schema preserved"
        );
        assert_eq!(
            reopened.keywords().get("key").unwrap(),
            &Value::Scalar(ScalarValue::Int32(42)),
            "keywords preserved"
        );
    }
}
