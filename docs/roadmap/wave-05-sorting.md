# Wave 5: Sorting & Table Iteration

**Why:** Sorting is fundamental for data analysis. Table iteration (grouping
by key columns) is the casacore pattern for processing data in chunks.

**C++ reference:** `Table::sort`, `TableIter`.

## Scope

1. **Sort** — `Table::sort(columns, orders)` → `RefTable` with rows reordered
   (no data copy, just index permutation). Support ascending/descending per column.

2. **Table iteration** — `TableIter::new(table, key_columns)` iterates
   sub-tables where key columns have equal values. Returns `(key_values, sub_table)`.

3. **Custom comparison** — allow user-supplied comparison functions for sort.

## Tests

- Sort by single column, verify order.
- Sort by multiple columns with mixed asc/desc.
- Iterate by single key, verify group boundaries.
- Iterate by composite key.
- 2×2: Rust sorts → C++ reads sorted RefTable; C++ sorts → Rust reads.

## Lessons learned

- **Sorted RefTables reuse the existing RefTable format.** A sorted view is
  just a `RefTable` with a permuted `row_map` and `row_order = false`. No new
  on-disk format was needed — the Wave 4 persistence format handles both
  selections and sorts.

- **`ScalarValue` cannot implement `Ord` or `PartialOrd`.** Floats lack `Ord`
  and complex types have no total order. A dedicated `sort_cmp()` method using
  `f32::total_cmp()` / `f64::total_cmp()` is the cleanest approach, returning
  `Option<Ordering>` to signal unsortable types.

- **`TableIterator` borrows `&Table` (immutably) and yields owned groups.**
  C++ `TableIterator::table()` returns a sub-RefTable, but in Rust this
  would require multiple simultaneous mutable borrows of the parent. Yielding
  `TableGroup { keys, row_indices }` avoids borrow conflicts and implements
  the standard `Iterator` trait naturally.
