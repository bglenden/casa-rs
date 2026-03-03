# Wave 6: Table Concatenation & Copy

**Why:** Concatenation virtually combines partitioned datasets (e.g. a
MeasurementSet split by time). Copy with DM conversion is needed for
format migration.

**C++ reference:** `ConcatTable`, `ConcatColumn`, `TableCopy`.

## Scope

1. **ConcatTable** — virtual union of tables with the same schema. Row count
   is the sum; row access dispatches to the correct underlying table.

2. **Deep copy** — `Table::deep_copy(path, options)` with optional storage
   manager conversion (e.g. StManAipsIO → StandardStMan).

3. **Shallow copy** — `Table::copy(path)` for structure + metadata only
   (no row data).

## Tests

- Concatenate two tables, verify combined row count and data.
- Deep copy with DM conversion, verify round-trip.
- 2×2: Rust concatenates → C++ reads; C++ concatenates → Rust reads.
