// SPDX-License-Identifier: LGPL-3.0-or-later
//! In-memory sorted index for fast scalar-column lookups.
//!
//! This module provides [`ColumnsIndex`], an in-memory sorted index over one
//! or more scalar columns of a [`Table`]. At construction the index builds a
//! sorted permutation of row indices; subsequent lookups use binary search for
//! O(log n) exact and range queries.
//!
//! The index is **transient** — it is not persisted to disk and carries no
//! on-disk format. It must be rebuilt after any structural change to the table.
//!
//! # C++ reference
//!
//! `casacore::ColumnsIndex` — `tables/Tables/ColumnsIndex.h`.

use std::cmp::Ordering;

use casacore_types::{ScalarValue, Value};

use crate::sorting::{argsort, validate_sort_column};
use crate::table::{SortOrder, Table, TableError};

/// An in-memory sorted index on one or more scalar columns of a [`Table`].
///
/// Building the index sorts row indices by the key columns (ascending). All
/// lookup methods use binary search on the sorted list, giving O(log n)
/// exact-match and O(log n + k) range queries where k is the result count.
///
/// The index holds a shared reference to the table for the duration of its
/// lifetime; no rows can be added or removed while an index is live.
///
/// # C++ equivalent
///
/// `casacore::ColumnsIndex`. The C++ version supports both unique and
/// non-unique indices; this Rust version is always non-unique at construction
/// and exposes `is_unique()` as a pre-computed convenience flag.
///
/// # Example
///
/// ```rust
/// # use casacore_tables::{Table, TableSchema, ColumnSchema, ColumnsIndex};
/// # use casacore_types::{PrimitiveType, RecordValue, RecordField, Value, ScalarValue};
/// let schema = TableSchema::new(vec![
///     ColumnSchema::scalar("id", PrimitiveType::Int32),
/// ]).unwrap();
/// let mut table = Table::with_schema(schema);
/// for i in [3i32, 1, 2] {
///     table.add_row(RecordValue::new(vec![
///         RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
///     ])).unwrap();
/// }
/// let idx = ColumnsIndex::new(&table, &["id"]).unwrap();
/// let rows = idx.lookup(&[("id", &ScalarValue::Int32(2))]);
/// assert_eq!(rows, vec![2]); // row 2 contains id=2
/// ```
pub struct ColumnsIndex<'a> {
    // Retained to enforce the borrow: the table cannot be mutated while the
    // index is live.  Key values are cached in `sorted_keys` so lookups never
    // read through this reference after construction.
    _table: &'a Table,
    column_names: Vec<String>,
    /// Row indices in sorted key order.
    sorted_rows: Vec<usize>,
    /// Flat cache of sorted key values.
    ///
    /// Laid out as `[key_for_pos_0..., key_for_pos_1..., ...]` with stride
    /// `column_names.len()`. Concretely, position `i`'s key occupies
    /// `sorted_keys[i * ncols .. (i + 1) * ncols]`. Contiguous layout avoids
    /// the extra pointer dereference that a `Vec<Vec<ScalarValue>>` would
    /// require, and keeps binary-search comparisons cache-friendly.
    sorted_keys: Vec<ScalarValue>,
    is_unique: bool,
}

impl<'a> ColumnsIndex<'a> {
    /// Builds a sorted index on the given scalar columns.
    ///
    /// Columns must be scalar and must not be Complex32/Complex64 (which have
    /// no total order). The index is sorted ascending by each column in the
    /// order listed (lexicographic).
    ///
    /// # Errors
    ///
    /// - [`TableError::IndexNoColumns`] — `columns` is empty.
    /// - [`TableError::IndexColumnNotScalar`] — a column contains array or record values.
    /// - [`TableError::IndexColumnUnsortable`] — a column contains Complex values.
    /// - [`TableError::UnknownColumn`] — a column name is not in the table schema.
    ///
    /// # C++ equivalent
    ///
    /// `ColumnsIndex(const Table&, const Block<String>& columnNames)`.
    pub fn new(table: &'a Table, columns: &[&str]) -> Result<Self, TableError> {
        if columns.is_empty() {
            return Err(TableError::IndexNoColumns);
        }

        // Validate each column, mapping sort errors → index errors.
        for &col in columns {
            validate_sort_column(table, col).map_err(|e| match e {
                TableError::SortKeyNotScalar { column } => {
                    TableError::IndexColumnNotScalar { column }
                }
                TableError::SortKeyUnsortable { column } => {
                    TableError::IndexColumnUnsortable { column }
                }
                other => other,
            })?;
        }

        // Build ascending sort permutation via the existing argsort helper.
        let sort_keys: Vec<(&str, SortOrder)> =
            columns.iter().map(|&c| (c, SortOrder::Ascending)).collect();
        let sorted_rows = argsort(table, &sort_keys)?;

        let col_names: Vec<String> = columns.iter().map(|&c| c.to_string()).collect();
        let ncols = col_names.len();

        // Materialise key values in a flat contiguous array: one ScalarValue
        // per (position × column) pair.  Contiguous layout avoids the extra
        // heap indirection that a Vec<Vec<ScalarValue>> would impose on the
        // binary-search hot path.
        let mut sorted_keys = Vec::with_capacity(sorted_rows.len() * ncols);
        for &row in &sorted_rows {
            for col in &col_names {
                match table.cell(row, col)? {
                    Some(Value::Scalar(sv)) => sorted_keys.push(sv.clone()),
                    // Validated above — should not occur.
                    _ => unreachable!("index column must be scalar"),
                }
            }
        }

        let is_unique = is_all_unique(&sorted_keys, ncols);

        Ok(Self {
            _table: table,
            column_names: col_names,
            sorted_rows,
            sorted_keys,
            is_unique,
        })
    }

    /// Returns the sorted row indices for all rows matching `key` exactly.
    ///
    /// `key` is a slice of `(column_name, value)` pairs. The number and order
    /// of pairs must match the columns given to [`ColumnsIndex::new`]; extra
    /// pairs are silently ignored, and missing columns are treated as wildcards.
    ///
    /// Returns an empty `Vec` if no rows match.
    ///
    /// # C++ equivalent
    ///
    /// `ColumnsIndex::getRowNumbers(const Record& key)`.
    pub fn lookup(&self, key: &[(&str, &ScalarValue)]) -> Vec<usize> {
        let lo = self.lower_bound(key, true);
        let hi = self.upper_bound(key, true);
        if lo >= hi {
            return Vec::new();
        }
        // Single allocation + memcpy — no iterative push or reallocation.
        self.sorted_rows[lo..hi].to_vec()
    }

    /// Returns the unique row matching `key`, or `None` if there is no match.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::IndexNotUnique`] if more than one row matches.
    ///
    /// # C++ equivalent
    ///
    /// `ColumnsIndex::getRowNumber(uInt& rownr, const Record& key)` — the C++
    /// version signals uniqueness failure via a thrown exception.
    pub fn lookup_unique(&self, key: &[(&str, &ScalarValue)]) -> Result<Option<usize>, TableError> {
        let rows = self.lookup(key);
        match rows.len() {
            0 => Ok(None),
            1 => Ok(Some(rows[0])),
            n => Err(TableError::IndexNotUnique { count: n }),
        }
    }

    /// Returns all rows whose key falls within `[lower, upper]` (or open
    /// variants controlled by `lower_incl` / `upper_incl`).
    ///
    /// Both `lower` and `upper` follow the same `(column_name, value)` slice
    /// convention as [`lookup`](ColumnsIndex::lookup). Passing an empty slice
    /// for either bound means "unbounded on that side".
    ///
    /// # C++ equivalent
    ///
    /// `ColumnsIndex::getRowNumbers(const Record& lower, const Record& upper,
    ///  Bool lowerInclusive, Bool upperInclusive)`.
    pub fn lookup_range(
        &self,
        lower: &[(&str, &ScalarValue)],
        upper: &[(&str, &ScalarValue)],
        lower_incl: bool,
        upper_incl: bool,
    ) -> Vec<usize> {
        let n = self.sorted_rows.len();
        if n == 0 {
            return Vec::new();
        }

        let lo = if lower.is_empty() {
            0
        } else {
            self.lower_bound(lower, lower_incl)
        };

        let hi = if upper.is_empty() {
            n
        } else {
            self.upper_bound(upper, upper_incl)
        };

        if lo >= hi {
            return Vec::new();
        }

        self.sorted_rows[lo..hi].to_vec()
    }

    /// Batch exact-match lookup: returns matching row indices for each key.
    ///
    /// For each element in `keys`, performs the same operation as
    /// [`lookup`](ColumnsIndex::lookup). Returns a `Vec` of equal length,
    /// where each element is the result for the corresponding query key.
    ///
    /// # C++ equivalent
    ///
    /// `ColumnsIndexArray::getRowNumbers()` (batch overload).
    pub fn lookup_many(&self, keys: &[Vec<(&str, &ScalarValue)>]) -> Vec<Vec<usize>> {
        keys.iter().map(|k| self.lookup(k)).collect()
    }

    /// Returns `true` if every key in the index is unique (no duplicate rows
    /// for any key combination).
    ///
    /// This is pre-computed at construction time.
    pub fn is_unique(&self) -> bool {
        self.is_unique
    }

    /// Returns the names of the columns this index was built on, in order.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    // ── Private helpers ────────────────────────────────────────────────

    /// Returns the sorted position of the first entry that is ≥ `key`
    /// (if `inclusive`) or > `key` (if `!inclusive`).
    fn lower_bound(&self, key: &[(&str, &ScalarValue)], inclusive: bool) -> usize {
        let n = self.sorted_rows.len();
        let ncols = self.column_names.len();
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let ord = cmp_cached(&self.sorted_keys[mid * ncols..(mid + 1) * ncols], key);
            // inclusive: go right while ord == Less (i.e. cached < key)
            // exclusive: go right while ord != Greater (cached <= key)
            let go_right = if inclusive {
                ord == Ordering::Less
            } else {
                ord != Ordering::Greater
            };
            if go_right {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Returns the exclusive upper sorted position for entries ≤ `key`
    /// (if `inclusive`) or < `key` (if `!inclusive`).
    fn upper_bound(&self, key: &[(&str, &ScalarValue)], inclusive: bool) -> usize {
        let n = self.sorted_rows.len();
        let ncols = self.column_names.len();
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let ord = cmp_cached(&self.sorted_keys[mid * ncols..(mid + 1) * ncols], key);
            // inclusive: go right while ord != Greater (cached <= key)
            // exclusive: go right while ord == Less (cached < key)
            let go_right = if inclusive {
                ord != Ordering::Greater
            } else {
                ord == Ordering::Less
            };
            if go_right {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

/// Compares a cached key slice against a query key lexicographically.
///
/// `cached` holds values in the same column order as the index was built.
/// Column names in `key` are ignored — the position in `cached` encodes the
/// column identity.
fn cmp_cached(cached: &[ScalarValue], key: &[(&str, &ScalarValue)]) -> Ordering {
    for (cached_val, &(_, expected)) in cached.iter().zip(key.iter()) {
        let ord = cached_val.sort_cmp(expected).unwrap_or(Ordering::Equal);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Returns `true` when every adjacent pair of keys in the sorted flat array
/// is distinct (i.e., no duplicate entries).
fn is_all_unique(sorted_keys: &[ScalarValue], ncols: usize) -> bool {
    let n = sorted_keys.len() / ncols;
    if n < 2 {
        return true;
    }
    for i in 0..n - 1 {
        let a = &sorted_keys[i * ncols..(i + 1) * ncols];
        let b = &sorted_keys[(i + 1) * ncols..(i + 2) * ncols];
        let all_equal = a
            .iter()
            .zip(b.iter())
            .all(|(av, bv)| av.sort_cmp(bv) == Some(Ordering::Equal));
        if all_equal {
            return false;
        }
    }
    true
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, TableSchema};
    use casacore_types::{PrimitiveType, RecordField, RecordValue};

    // ── Helpers ────────────────────────────────────────────────────────

    /// Build a table with a single Int32 column "id".
    fn int_table(values: &[i32]) -> Table {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        for &v in values {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "id",
                    Value::Scalar(ScalarValue::Int32(v)),
                )]))
                .unwrap();
        }
        table
    }

    /// Build a two-column table (name: String, value: Int32).
    fn composite_table(rows: &[(&str, i32)]) -> Table {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("name", PrimitiveType::String),
            ColumnSchema::scalar("value", PrimitiveType::Int32),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        for &(name, val) in rows {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("name", Value::Scalar(ScalarValue::String(name.into()))),
                    RecordField::new("value", Value::Scalar(ScalarValue::Int32(val))),
                ]))
                .unwrap();
        }
        table
    }

    // ── Basic exact lookup ──────────────────────────────────────────────

    #[test]
    fn index_single_column_exact_match() {
        // rows: 3, 1, 2  (insertion order)
        let table = int_table(&[3, 1, 2]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();

        let rows = idx.lookup(&[("id", &ScalarValue::Int32(1))]);
        assert_eq!(rows, vec![1]); // parent-table row 1 contains id=1
    }

    #[test]
    fn index_non_unique_lookup() {
        // Multiple rows with the same key value.
        let table = int_table(&[5, 3, 5, 1, 5]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();

        let mut rows = idx.lookup(&[("id", &ScalarValue::Int32(5))]);
        rows.sort_unstable();
        assert_eq!(rows, vec![0, 2, 4]); // rows 0, 2, 4 all have id=5
        assert!(!idx.is_unique());
    }

    // ── lookup_unique ───────────────────────────────────────────────────

    #[test]
    fn index_lookup_unique_returns_single() {
        let table = int_table(&[10, 20, 30]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();
        let result = idx.lookup_unique(&[("id", &ScalarValue::Int32(20))]);
        assert_eq!(result.unwrap(), Some(1));
    }

    #[test]
    fn index_lookup_unique_not_found() {
        let table = int_table(&[10, 20, 30]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();
        let result = idx.lookup_unique(&[("id", &ScalarValue::Int32(99))]);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn index_lookup_unique_not_unique_error() {
        let table = int_table(&[7, 7, 8]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();
        let result = idx.lookup_unique(&[("id", &ScalarValue::Int32(7))]);
        assert!(matches!(
            result,
            Err(TableError::IndexNotUnique { count: 2 })
        ));
    }

    // ── Range queries ───────────────────────────────────────────────────

    #[test]
    fn index_range_query_inclusive() {
        // rows 0..=9
        let table = int_table(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();

        let mut rows = idx.lookup_range(
            &[("id", &ScalarValue::Int32(3))],
            &[("id", &ScalarValue::Int32(6))],
            true,
            true,
        );
        rows.sort_unstable();
        assert_eq!(rows, vec![3, 4, 5, 6]); // rows 3,4,5,6 contain id=3,4,5,6
    }

    #[test]
    fn index_range_query_exclusive() {
        let table = int_table(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();

        let mut rows = idx.lookup_range(
            &[("id", &ScalarValue::Int32(3))],
            &[("id", &ScalarValue::Int32(6))],
            false,
            false,
        );
        rows.sort_unstable();
        assert_eq!(rows, vec![4, 5]); // exclusive: id=4,5 only
    }

    #[test]
    fn index_range_query_open_ended() {
        let table = int_table(&[10, 20, 30, 40, 50]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();

        // Lower-open: all rows with id <= 30.
        let mut rows = idx.lookup_range(&[], &[("id", &ScalarValue::Int32(30))], true, true);
        rows.sort_unstable();
        assert_eq!(rows, vec![0, 1, 2]);

        // Upper-open: all rows with id >= 30.
        let mut rows = idx.lookup_range(&[("id", &ScalarValue::Int32(30))], &[], true, true);
        rows.sort_unstable();
        assert_eq!(rows, vec![2, 3, 4]);
    }

    // ── Multi-column index ──────────────────────────────────────────────

    #[test]
    fn index_multi_column_exact_match() {
        let table = composite_table(&[("bob", 2), ("alice", 1), ("alice", 2), ("bob", 1)]);
        let idx = ColumnsIndex::new(&table, &["name", "value"]).unwrap();

        let rows = idx.lookup(&[
            ("name", &ScalarValue::String("alice".into())),
            ("value", &ScalarValue::Int32(2)),
        ]);
        assert_eq!(rows, vec![2]); // row 2: (alice, 2)
    }

    #[test]
    fn index_multi_column_range() {
        // Range on leading column only.
        let table = composite_table(&[("c", 1), ("a", 2), ("b", 3), ("a", 1), ("c", 2)]);
        let idx = ColumnsIndex::new(&table, &["name", "value"]).unwrap();

        // All rows where name is in [a, b] inclusive.
        let mut rows = idx.lookup_range(
            &[("name", &ScalarValue::String("a".into()))],
            &[("name", &ScalarValue::String("b".into()))],
            true,
            true,
        );
        rows.sort_unstable();
        // rows with name=a: indices 1,3; name=b: index 2
        assert_eq!(rows, vec![1, 2, 3]);
    }

    // ── Error paths ─────────────────────────────────────────────────────

    #[test]
    fn index_no_columns_rejected() {
        let table = int_table(&[1, 2, 3]);
        let result = ColumnsIndex::new(&table, &[]);
        assert!(matches!(result, Err(TableError::IndexNoColumns)));
    }

    #[test]
    fn index_complex_column_rejected() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("z", PrimitiveType::Complex64)]).unwrap();
        let table = Table::with_schema(schema);
        let result = ColumnsIndex::new(&table, &["z"]);
        assert!(matches!(
            result,
            Err(TableError::IndexColumnUnsortable { .. })
        ));
    }

    #[test]
    fn index_non_scalar_rejected() {
        use crate::schema::ColumnSchema;
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "arr",
            PrimitiveType::Float64,
            Some(1),
        )])
        .unwrap();
        let table = Table::with_schema(schema);
        let result = ColumnsIndex::new(&table, &["arr"]);
        assert!(matches!(
            result,
            Err(TableError::IndexColumnNotScalar { .. })
        ));
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[test]
    fn index_empty_table() {
        let table = int_table(&[]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();
        let rows = idx.lookup(&[("id", &ScalarValue::Int32(42))]);
        assert!(rows.is_empty());
        assert!(idx.is_unique());
    }

    #[test]
    fn index_unique_flag_true_when_all_distinct() {
        let table = int_table(&[1, 2, 3, 4, 5]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();
        assert!(idx.is_unique());
    }

    // ── Performance smoke test ──────────────────────────────────────────

    #[test]
    fn index_performance_100k_rows() {
        use std::time::Instant;

        let n = 100_000usize;
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        for i in 0..n {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "id",
                    Value::Scalar(ScalarValue::Int32((i % 1000) as i32)),
                )]))
                .unwrap();
        }

        // Build index.
        let t0 = Instant::now();
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();
        let build_ms = t0.elapsed().as_millis();

        // Index lookup for a key that appears 100 times.
        let t1 = Instant::now();
        let rows = idx.lookup(&[("id", &ScalarValue::Int32(42))]);
        let lookup_ms = t1.elapsed().as_millis();
        assert_eq!(rows.len(), 100);

        // Linear scan for comparison.
        let t2 = Instant::now();
        let linear: Vec<usize> = (0..n)
            .filter(|&r| table.cell(r, "id") == Ok(Some(&Value::Scalar(ScalarValue::Int32(42)))))
            .collect();
        let linear_ms = t2.elapsed().as_millis();
        assert_eq!(linear.len(), 100);

        eprintln!(
            "100k rows: build={}ms  index_lookup={}ms  linear_scan={}ms",
            build_ms, lookup_ms, linear_ms
        );
        assert!(build_ms < 5_000, "build took too long: {build_ms}ms");
        assert!(lookup_ms < 5_000, "lookup took too long: {lookup_ms}ms");
    }

    #[test]
    fn lookup_many_returns_per_key_results() {
        let table = int_table(&[10, 20, 10, 30, 20]);
        let idx = ColumnsIndex::new(&table, &["id"]).unwrap();

        let results = idx.lookup_many(&[
            vec![("id", &ScalarValue::Int32(10))],
            vec![("id", &ScalarValue::Int32(20))],
            vec![("id", &ScalarValue::Int32(99))],
        ]);

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].len(), 2); // rows 0, 2
        assert_eq!(results[1].len(), 2); // rows 1, 4
        assert!(results[2].is_empty()); // no match
    }
}
