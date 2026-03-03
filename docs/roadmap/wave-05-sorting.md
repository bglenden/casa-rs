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
