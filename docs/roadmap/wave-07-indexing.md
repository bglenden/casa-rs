# Wave 7: Column Indexing

**Why:** Fast lookups on scalar columns (e.g. find row by antenna ID) without
scanning all rows.

**C++ reference:** `ColumnsIndex`.

## Scope

1. **Single-column index** — build a sorted index on a scalar column for
   O(log n) lookup by value.

2. **Multi-column index** — composite key index on two or more columns.

3. **Range queries** — find all rows where the indexed column falls in a range.

## Tests

- Build index, lookup exact match, verify row.
- Range query on indexed column.
- Multi-column index lookup.
- Performance: index lookup vs. linear scan on 100K rows.
