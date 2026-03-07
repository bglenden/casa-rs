// SPDX-License-Identifier: LGPL-3.0-or-later
use super::*;

impl Table {
    // -----------------------------------------------------------------------
    // Selection (RefTable creation)
    // -----------------------------------------------------------------------

    /// Creates a reference table containing only the specified rows.
    ///
    /// Row indices are validated against `row_count()`. The returned
    /// [`RefTable`](crate::RefTable) borrows `self` mutably; drop it to
    /// regain access to the parent.
    ///
    /// C++ equivalent: constructing a `RefTable` from a `Vector<rownr_t>`.
    pub fn select_rows(&mut self, indices: &[usize]) -> Result<crate::RefTable<'_>, TableError> {
        crate::RefTable::from_rows(self, indices.to_vec())
    }

    /// Creates a reference table containing only the named columns.
    ///
    /// All rows are included. Column names are validated against the schema.
    ///
    /// C++ equivalent: constructing a `RefTable` from a `Vector<String>`.
    pub fn select_columns(&mut self, names: &[&str]) -> Result<crate::RefTable<'_>, TableError> {
        crate::RefTable::from_columns(self, names)
    }

    /// Creates a reference table containing rows that satisfy `predicate`.
    ///
    /// Iterates all rows, calling `predicate` on each. Rows for which the
    /// closure returns `true` are included in the view.
    pub fn select<F>(&mut self, predicate: F) -> crate::RefTable<'_>
    where
        F: Fn(&RecordValue) -> bool,
    {
        crate::RefTable::from_predicate(self, predicate)
    }

    // -----------------------------------------------------------------------
    // Row-set algebra
    // -----------------------------------------------------------------------

    /// Returns row indices present in **either** `a` or `b` (union).
    ///
    /// The result is sorted and deduplicated. If both inputs are already
    /// sorted, this runs in O(n) via a merge; otherwise it falls back to
    /// sort + dedup.
    ///
    /// # C++ equivalent
    ///
    /// `TableExprNode::operator|` (set union on row numbers).
    pub fn row_union(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_sorted() && b.is_sorted() {
            let mut result = Vec::with_capacity(a.len() + b.len());
            let (mut i, mut j) = (0, 0);
            while i < a.len() && j < b.len() {
                match a[i].cmp(&b[j]) {
                    std::cmp::Ordering::Less => {
                        result.push(a[i]);
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        result.push(b[j]);
                        j += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        result.push(a[i]);
                        i += 1;
                        j += 1;
                    }
                }
            }
            result.extend_from_slice(&a[i..]);
            result.extend_from_slice(&b[j..]);
            result
        } else {
            let mut set: Vec<usize> = a.iter().chain(b.iter()).copied().collect();
            set.sort_unstable();
            set.dedup();
            set
        }
    }

    /// Returns row indices present in **both** `a` and `b` (intersection).
    ///
    /// The result is sorted. If both inputs are already sorted, this runs
    /// in O(n) via a merge; otherwise it falls back to hash + sort.
    ///
    /// # C++ equivalent
    ///
    /// `TableExprNode::operator&` (set intersection on row numbers).
    pub fn row_intersection(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_sorted() && b.is_sorted() {
            let mut result = Vec::new();
            let (mut i, mut j) = (0, 0);
            while i < a.len() && j < b.len() {
                match a[i].cmp(&b[j]) {
                    std::cmp::Ordering::Less => i += 1,
                    std::cmp::Ordering::Greater => j += 1,
                    std::cmp::Ordering::Equal => {
                        result.push(a[i]);
                        i += 1;
                        j += 1;
                    }
                }
            }
            result
        } else {
            let set_b: std::collections::HashSet<usize> = b.iter().copied().collect();
            let mut result: Vec<usize> = a.iter().copied().filter(|x| set_b.contains(x)).collect();
            result.sort_unstable();
            result.dedup();
            result
        }
    }

    /// Returns row indices present in `a` but not in `b` (difference).
    ///
    /// The result is sorted. If both inputs are already sorted, this runs
    /// in O(n) via a merge; otherwise it falls back to hash + sort.
    ///
    /// # C++ equivalent
    ///
    /// `TableExprNode::operator-` (set difference on row numbers).
    pub fn row_difference(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_sorted() && b.is_sorted() {
            let mut result = Vec::new();
            let (mut i, mut j) = (0, 0);
            while i < a.len() && j < b.len() {
                match a[i].cmp(&b[j]) {
                    std::cmp::Ordering::Less => {
                        result.push(a[i]);
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => j += 1,
                    std::cmp::Ordering::Equal => {
                        i += 1;
                        j += 1;
                    }
                }
            }
            result.extend_from_slice(&a[i..]);
            result
        } else {
            let set_b: std::collections::HashSet<usize> = b.iter().copied().collect();
            let mut result: Vec<usize> = a.iter().copied().filter(|x| !set_b.contains(x)).collect();
            result.sort_unstable();
            result.dedup();
            result
        }
    }

    // -----------------------------------------------------------------------
    // Array cell slicing
    // -----------------------------------------------------------------------

    /// Reads a sub-region of an array cell.
    ///
    /// Returns a new `Value::Array` containing only the elements selected by
    /// the [`Slicer`]. The cell must be an array-valued cell; returns
    /// [`TableError::CellNotArray`] otherwise.
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::getSlice(rownr, slicer)`.
    pub fn get_cell_slice(
        &self,
        column: &str,
        row: usize,
        slicer: &Slicer,
    ) -> Result<Value, TableError> {
        let cell = self
            .cell(row, column)
            .ok_or_else(|| TableError::ColumnNotFound {
                row_index: row,
                column: column.to_string(),
            })?;
        match cell {
            Value::Array(av) => {
                let shape = av.shape();
                validate_slicer_bounds(slicer, shape, row, column)?;
                Ok(Value::Array(slice_array_value(av, slicer)))
            }
            _ => Err(TableError::CellNotArray {
                row,
                column: column.to_string(),
            }),
        }
    }

    /// Writes a sub-region of an array cell.
    ///
    /// Loads the full cell, replaces the slice region with `data`, and writes
    /// the updated array back. Both the existing cell and `data` must be
    /// arrays.
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::putSlice(rownr, slicer, array)`.
    pub fn put_cell_slice(
        &mut self,
        column: &str,
        row: usize,
        slicer: &Slicer,
        data: &ArrayValue,
    ) -> Result<(), TableError> {
        let cell = self
            .inner
            .row_mut(row)
            .and_then(|r| r.get_mut(column))
            .ok_or_else(|| TableError::ColumnNotFound {
                row_index: row,
                column: column.to_string(),
            })?;
        match cell {
            Value::Array(av) => {
                let shape = av.shape();
                validate_slicer_bounds(slicer, shape, row, column)?;
                put_slice_array_value(av, slicer, data);
                Ok(())
            }
            _ => Err(TableError::CellNotArray {
                row,
                column: column.to_string(),
            }),
        }
    }

    /// Reads a sub-region of an array cell for each row in `row_range`,
    /// returning one sliced value per row.
    ///
    /// Combines row selection ([`RowRange`]) with array slicing ([`Slicer`]).
    /// Each returned element is the slice of the array cell for that row.
    ///
    /// # Errors
    ///
    /// - [`TableError::CellNotArray`] if a cell in the column is scalar
    /// - [`TableError::SlicerDimensionMismatch`] if slicer ndim != array ndim
    /// - [`TableError::SlicerOutOfBounds`] if slicer exceeds array shape
    /// - [`TableError::ColumnNotFound`] if `column` does not exist
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::getColumnRange(slicer, rowRange)`.
    pub fn get_column_slice(
        &self,
        column: &str,
        row_range: RowRange,
        slicer: &Slicer,
    ) -> Result<Vec<Value>, TableError> {
        let mut results = Vec::new();
        for row in row_range.iter() {
            if row >= self.row_count() {
                break;
            }
            results.push(self.get_cell_slice(column, row, slicer)?);
        }
        Ok(results)
    }

    /// Writes a sub-region of an array cell for each row in `row_range`.
    ///
    /// `data` must have one element per selected row. Each element replaces
    /// the corresponding slice region in that row's array cell.
    ///
    /// # Errors
    ///
    /// - [`TableError::CellNotArray`] if a cell in the column is scalar
    /// - [`TableError::SlicerDimensionMismatch`] if slicer ndim != array ndim
    /// - [`TableError::SlicerOutOfBounds`] if slicer exceeds array shape
    /// - [`TableError::ColumnNotFound`] if `column` does not exist
    /// - [`TableError::ColumnSliceLengthMismatch`] if `data` does not contain
    ///   one slice per selected in-bounds row
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::putColumnRange(slicer, rowRange, data)`.
    pub fn put_column_slice(
        &mut self,
        column: &str,
        row_range: RowRange,
        slicer: &Slicer,
        data: &[ArrayValue],
    ) -> Result<(), TableError> {
        let rows: Vec<usize> = row_range
            .iter()
            .take_while(|&r| r < self.row_count())
            .collect();
        if rows.len() != data.len() {
            return Err(TableError::ColumnSliceLengthMismatch {
                rows: rows.len(),
                data_len: data.len(),
            });
        }
        for (row, patch) in rows.into_iter().zip(data.iter()) {
            self.put_cell_slice(column, row, slicer, patch)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Sorting
    // -----------------------------------------------------------------------

    /// Sorts the table by the given key columns, returning a [`RefTable`]
    /// with the rows in the new order.
    ///
    /// The result is an indirect sort: no data is moved, only the row
    /// index permutation changes. The returned [`RefTable`] has
    /// `row_order = false` (not in original ascending order).
    ///
    /// Only scalar columns with a total ordering can be sort keys.
    /// Complex columns are rejected. This matches C++ `Table::sort`.
    ///
    /// # Errors
    ///
    /// - [`TableError::SortNoKeys`] if `keys` is empty
    /// - [`TableError::SortKeyNotScalar`] if a key column is non-scalar
    /// - [`TableError::SortKeyUnsortable`] if a key column is Complex
    /// - [`TableError::UnknownColumn`] if a key column is not in schema
    ///
    /// # C++ equivalent
    ///
    /// `Table::sort(columnNames, sortOrders)`.
    ///
    /// [`RefTable`]: crate::RefTable
    pub fn sort(&mut self, keys: &[(&str, SortOrder)]) -> Result<crate::RefTable<'_>, TableError> {
        let permutation = crate::sorting::argsort(self, keys)?;
        crate::RefTable::from_rows(self, permutation)
    }

    /// Sorts the table by a single column using a custom comparison function.
    ///
    /// The closure receives two [`Value`] references from the specified column
    /// and must return an [`Ordering`]. This is the Rust analogue of passing
    /// a `BaseCompare` object to C++ `Table::sort`.
    ///
    /// [`Ordering`]: std::cmp::Ordering
    pub fn sort_by<F>(
        &mut self,
        column: &str,
        compare: F,
    ) -> Result<crate::RefTable<'_>, TableError>
    where
        F: Fn(&Value, &Value) -> std::cmp::Ordering,
    {
        let n = self.row_count();
        let mut indices: Vec<usize> = (0..n).collect();

        indices.sort_by(|&a, &b| {
            let va = self.cell(a, column);
            let vb = self.cell(b, column);
            match (va, vb) {
                (Some(a), Some(b)) => compare(a, b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });

        crate::RefTable::from_rows(self, indices)
    }

    /// Returns an iterator that groups rows by equal values in the key columns.
    ///
    /// The table is first sorted by the key columns, then consecutive rows
    /// with equal key values are collected into [`TableGroup`] values.
    /// Each group contains the shared key values and the parent-table row
    /// indices for that group.
    ///
    /// Unlike [`sort`](Table::sort), this borrows the table immutably because
    /// it yields owned data rather than a mutable view.
    ///
    /// # C++ equivalent
    ///
    /// `casacore::TableIterator`.
    ///
    /// [`TableGroup`]: crate::TableGroup
    pub fn iter_groups(
        &self,
        keys: &[(&str, SortOrder)],
    ) -> Result<crate::sorting::TableIterator<'_>, TableError> {
        crate::sorting::TableIterator::new(self, keys)
    }

    /// Groups rows by key columns in natural (insertion) order, without sorting.
    ///
    /// Consecutive rows with equal key values are grouped together, but
    /// non-adjacent duplicates appear as separate groups. This is useful when
    /// the table is already sorted or when group ordering must match the
    /// on-disk row order.
    ///
    /// # C++ equivalent
    ///
    /// `TableIterator` constructed with `TableIterator::NoSort`.
    pub fn iter_groups_nosort(
        &self,
        key_columns: &[&str],
    ) -> Result<crate::sorting::TableIterator<'_>, TableError> {
        crate::sorting::TableIterator::new_nosort(self, key_columns)
    }

    /// Creates a [`crate::ConcatTable`] from two or more tables with the same schema.
    ///
    /// The resulting virtual table has a row count equal to the sum of all
    /// constituent tables. Row reads dispatch to the correct underlying table
    /// via binary search on cumulative row offsets. No data is copied.
    ///
    /// All tables must have identical schemas. Returns
    /// [`TableError::SchemaMismatch`] if they differ, or
    /// [`TableError::ConcatEmpty`] if the vector is empty.
    ///
    /// # C++ equivalent
    ///
    /// `ConcatTable(Block<Table>(...), Block<String>(), "")`.
    pub fn concat(tables: Vec<Table>) -> Result<crate::ConcatTable, TableError> {
        crate::ConcatTable::new(tables)
    }

    // ── TaQL query methods ──────────────────────────────────────────

    /// Executes a TaQL SELECT query and returns a [`QueryResult`].
    ///
    /// For simple SELECTs (only column references, no computed expressions),
    /// returns `QueryResult::View` with a zero-copy [`RefTable`](crate::RefTable). For SELECTs
    /// with computed columns, GROUP BY, or aggregate functions, returns
    /// `QueryResult::Materialized` with an owned in-memory [`Table`] containing
    /// the evaluated result rows.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::Taql`] if the query is invalid or execution fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use casacore_tables::{Table, TableSchema, ColumnSchema, QueryResult};
    /// # use casacore_types::*;
    /// # let schema = TableSchema::new(vec![
    /// #     ColumnSchema::scalar("id", PrimitiveType::Int32),
    /// #     ColumnSchema::scalar("flux", PrimitiveType::Float64),
    /// # ]).unwrap();
    /// # let mut table = Table::with_schema(schema);
    /// # for i in 0..5 {
    /// #     table.add_row(RecordValue::new(vec![
    /// #         RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
    /// #         RecordField::new("flux", Value::Scalar(ScalarValue::Float64(i as f64))),
    /// #     ])).unwrap();
    /// # }
    /// match table.query_result("SELECT * WHERE flux > 2.0").unwrap() {
    ///     QueryResult::View(view) => assert_eq!(view.row_count(), 2),
    ///     QueryResult::Materialized(mat) => {
    ///         // Computed columns produce a materialized table
    ///         let _ = mat.row_count();
    ///     }
    /// }
    /// ```
    ///
    /// C++ equivalent: `tableCommand()` with a SELECT query.
    pub fn query_result(&mut self, taql: &str) -> Result<QueryResult<'_>, TableError> {
        let stmt = crate::taql::parse(taql).map_err(|e| TableError::Taql(e.to_string()))?;
        let result = crate::taql::exec::execute_materializing(&stmt, self)
            .map_err(|e| TableError::Taql(e.to_string()))?;
        match result {
            crate::taql::TaqlResult::Select {
                row_indices,
                columns,
            } => {
                let view = if columns.is_empty() {
                    crate::RefTable::from_rows(self, row_indices)?
                } else {
                    crate::RefTable::from_rows_and_columns(self, row_indices, &columns)?
                };
                Ok(QueryResult::View(view))
            }
            crate::taql::TaqlResult::Materialized { table } => {
                Ok(QueryResult::Materialized(table))
            }
            _ => Err(TableError::Taql(
                "Table::query_result() only supports SELECT statements; use execute_taql() for mutations"
                    .to_string(),
            )),
        }
    }

    /// Executes a TaQL SELECT query and returns a [`RefTable`](crate::RefTable) view.
    ///
    /// This is a convenience method that parses the query, executes it, and
    /// wraps the result in a [`RefTable`](crate::RefTable). Only SELECT statements are accepted;
    /// for UPDATE/INSERT/DELETE use [`execute_taql`](Table::execute_taql).
    ///
    /// # Errors
    ///
    /// Returns [`TableError::Taql`] if the query is invalid or execution fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use casacore_tables::{Table, TableSchema, ColumnSchema};
    /// # use casacore_types::*;
    /// # let schema = TableSchema::new(vec![
    /// #     ColumnSchema::scalar("id", PrimitiveType::Int32),
    /// #     ColumnSchema::scalar("flux", PrimitiveType::Float64),
    /// # ]).unwrap();
    /// # let mut table = Table::with_schema(schema);
    /// # for i in 0..5 {
    /// #     table.add_row(RecordValue::new(vec![
    /// #         RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
    /// #         RecordField::new("flux", Value::Scalar(ScalarValue::Float64(i as f64))),
    /// #     ])).unwrap();
    /// # }
    /// let view = table.query("SELECT * WHERE flux > 2.0").unwrap();
    /// assert_eq!(view.row_count(), 2);
    /// ```
    ///
    /// C++ equivalent: `tableCommand()` with a SELECT query.
    pub fn query(&mut self, taql: &str) -> Result<crate::RefTable<'_>, TableError> {
        let stmt = crate::taql::parse(taql).map_err(|e| TableError::Taql(e.to_string()))?;
        let result =
            crate::taql::execute(&stmt, self).map_err(|e| TableError::Taql(e.to_string()))?;
        match result {
            crate::taql::TaqlResult::Select {
                row_indices,
                columns,
            } => {
                if columns.is_empty() {
                    crate::RefTable::from_rows(self, row_indices)
                } else {
                    crate::RefTable::from_rows_and_columns(self, row_indices, &columns)
                }
            }
            _ => Err(TableError::Taql(
                "Table::query() only supports SELECT statements; use execute_taql() for mutations"
                    .to_string(),
            )),
        }
    }

    /// Executes any TaQL statement (SELECT, UPDATE, INSERT, DELETE).
    ///
    /// Returns a [`TaqlResult`](crate::taql::TaqlResult) describing the outcome.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::Taql`] if the query is invalid or execution fails.
    ///
    /// C++ equivalent: `tableCommand()`.
    pub fn execute_taql(&mut self, taql: &str) -> Result<crate::taql::TaqlResult, TableError> {
        let stmt = crate::taql::parse(taql).map_err(|e| TableError::Taql(e.to_string()))?;
        crate::taql::execute(&stmt, self).map_err(|e| TableError::Taql(e.to_string()))
    }
}

// ── Slicer helpers ────────────────────────────────────────────────────

fn validate_slicer_bounds(
    slicer: &Slicer,
    shape: &[usize],
    row: usize,
    column: &str,
) -> Result<(), TableError> {
    if slicer.ndim() != shape.len() {
        return Err(TableError::SlicerDimensionMismatch {
            start_ndim: slicer.ndim(),
            end_ndim: shape.len(),
            stride_ndim: slicer.ndim(),
        });
    }
    for (axis, ((&s, &e), &ext)) in slicer
        .start()
        .iter()
        .zip(slicer.end().iter())
        .zip(shape.iter())
        .enumerate()
    {
        if e > ext {
            return Err(TableError::SlicerOutOfBounds {
                axis,
                index: e,
                extent: ext,
            });
        }
        let _ = (s, row, column); // suppress unused warnings
    }
    Ok(())
}

/// Build ndarray `SliceInfoElem` vector from a `Slicer`.
fn slicer_to_slice_elems(slicer: &Slicer) -> Vec<ndarray::SliceInfoElem> {
    slicer
        .start()
        .iter()
        .zip(slicer.end().iter())
        .zip(slicer.stride().iter())
        .map(|((&s, &e), &st)| ndarray::SliceInfoElem::Slice {
            start: s as isize,
            end: Some(e as isize),
            step: st as isize,
        })
        .collect()
}

/// Extract a sub-array from `av` using `slicer`.
fn slice_array_value(av: &ArrayValue, slicer: &Slicer) -> ArrayValue {
    use ndarray::SliceInfoElem;
    let elems = slicer_to_slice_elems(slicer);
    let si: Vec<SliceInfoElem> = elems;

    macro_rules! do_slice {
        ($arr:expr) => {{
            let view = $arr.slice_each_axis(|ax| match si[ax.axis.index()] {
                SliceInfoElem::Slice { start, end, step } => ndarray::Slice { start, end, step },
                _ => unreachable!(),
            });
            view.to_owned()
        }};
    }

    match av {
        ArrayValue::Bool(a) => ArrayValue::Bool(do_slice!(a)),
        ArrayValue::UInt8(a) => ArrayValue::UInt8(do_slice!(a)),
        ArrayValue::UInt16(a) => ArrayValue::UInt16(do_slice!(a)),
        ArrayValue::UInt32(a) => ArrayValue::UInt32(do_slice!(a)),
        ArrayValue::Int16(a) => ArrayValue::Int16(do_slice!(a)),
        ArrayValue::Int32(a) => ArrayValue::Int32(do_slice!(a)),
        ArrayValue::Int64(a) => ArrayValue::Int64(do_slice!(a)),
        ArrayValue::Float32(a) => ArrayValue::Float32(do_slice!(a)),
        ArrayValue::Float64(a) => ArrayValue::Float64(do_slice!(a)),
        ArrayValue::Complex32(a) => ArrayValue::Complex32(do_slice!(a)),
        ArrayValue::Complex64(a) => ArrayValue::Complex64(do_slice!(a)),
        ArrayValue::String(a) => ArrayValue::String(do_slice!(a)),
    }
}

/// Write `data` into a sub-region of `target` specified by `slicer`.
fn put_slice_array_value(target: &mut ArrayValue, slicer: &Slicer, data: &ArrayValue) {
    use ndarray::SliceInfoElem;
    let elems = slicer_to_slice_elems(slicer);
    let si: Vec<SliceInfoElem> = elems;

    macro_rules! do_put {
        ($dst:expr, $src:expr) => {{
            let mut view = $dst.slice_each_axis_mut(|ax| match si[ax.axis.index()] {
                SliceInfoElem::Slice { start, end, step } => ndarray::Slice { start, end, step },
                _ => unreachable!(),
            });
            view.assign($src);
        }};
    }

    match (target, data) {
        (ArrayValue::Bool(t), ArrayValue::Bool(s)) => do_put!(t, s),
        (ArrayValue::UInt8(t), ArrayValue::UInt8(s)) => do_put!(t, s),
        (ArrayValue::UInt16(t), ArrayValue::UInt16(s)) => do_put!(t, s),
        (ArrayValue::UInt32(t), ArrayValue::UInt32(s)) => do_put!(t, s),
        (ArrayValue::Int16(t), ArrayValue::Int16(s)) => do_put!(t, s),
        (ArrayValue::Int32(t), ArrayValue::Int32(s)) => do_put!(t, s),
        (ArrayValue::Int64(t), ArrayValue::Int64(s)) => do_put!(t, s),
        (ArrayValue::Float32(t), ArrayValue::Float32(s)) => do_put!(t, s),
        (ArrayValue::Float64(t), ArrayValue::Float64(s)) => do_put!(t, s),
        (ArrayValue::Complex32(t), ArrayValue::Complex32(s)) => do_put!(t, s),
        (ArrayValue::Complex64(t), ArrayValue::Complex64(s)) => do_put!(t, s),
        (ArrayValue::String(t), ArrayValue::String(s)) => do_put!(t, s),
        _ => {} // type mismatch silently ignored (validated at higher level)
    }
}
