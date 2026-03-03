# Wave 2: Schema Mutation & Row Operations

**Why:** Real workflows need to add columns to existing tables (e.g. adding
a FLAG column to a MeasurementSet) and delete rows (e.g. flagged data).

**C++ reference:** `TableDesc::addColumn`, `removeColumn`, `renameColumn`;
`Table::removeRow`, `Table::addRow`.

## Scope

1. **Add column** — `Table::add_column(ColumnSchema)` + optional default value.
   Existing rows get the default (or undefined if allowed). Persists correctly
   with both storage managers.

2. **Remove column** — `Table::remove_column(name)`. Removes from schema and
   all rows. Storage reclaimed on next save.

3. **Rename column** — `Table::rename_column(old, new)`. Schema + row keys.

4. **Delete rows** — `Table::remove_rows(indices)`. Compact remaining rows.
   Must update any row-indexed structures.

5. **Insert rows** — `Table::insert_row(index, RecordValue)` at arbitrary
   position (not just append).

## Tests

- Add column to populated table, verify old rows get default, save/reopen.
- Remove column, verify absent in reopened table.
- Rename column, verify accessible by new name after reopen.
- Delete rows, verify remaining data correct after reopen.
- 2×2: Rust modifies schema → C++ reads; C++ modifies schema → Rust reads.

## Lessons learned

1. **Full-rewrite `save()` simplifies mutations — for now.** Because `save()`
   serializes the entire in-memory snapshot, schema and row mutations are
   purely in-memory operations with zero storage layer changes. This was the
   right trade-off for Wave 2, but it assumes the entire table fits in memory.
   Future waves or real-world large tables will likely need incremental or
   streaming persistence, at which point mutations will need storage-layer
   support.

2. **RC tests matter even when the format hasn't changed.** RC tests validate
   that in-memory structures are correct before serialization. RR round-trips
   can silently agree on a wrong encoding; RC forces correctness against C++.

3. **The `CppTableFixture` pattern extends to verify-only fixtures.** Mutation
   fixtures only need a C++ verify function (no write side), since the
   mutations originate in Rust.

4. **Foundational type changes propagate upward — do them first.**
   `RecordValue::remove()` and `rename_field()` had to land before
   `TableSchema` or `Table` could implement their mutation methods. Bottom-up
   ordering avoided rework.
