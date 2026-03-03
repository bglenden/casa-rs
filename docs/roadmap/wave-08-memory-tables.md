# Wave 8: Memory Tables

**Why:** Temporary tables for intermediate results (e.g. TaQL query results)
that don't need disk persistence.

**C++ reference:** `MemoryTable`, `MemoryStMan`.

## Scope

1. **MemoryStMan** — in-memory storage manager. All data lost on drop.

2. **Memory table creation** — `Table::new_memory()` or equivalent.

3. **Full API parity** — all Table methods work on memory tables.

4. **Convert to disk** — `memory_table.save(options)` materializes to disk.

## Tests

- Create memory table, populate, query, verify.
- Convert memory table to disk, reopen, verify.
- Memory table as sort/select result target.
