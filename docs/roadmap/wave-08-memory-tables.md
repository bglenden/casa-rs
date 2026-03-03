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

## Implementation summary

| Component | Location |
|-----------|----------|
| `TableKind` enum | `casacore-tables/src/table.rs` |
| Memory constructors (`new_memory`, etc.) | `casacore-tables/src/table.rs` |
| `to_memory()`, `is_memory()`, `table_kind()` | `casacore-tables/src/table.rs` |
| Locking no-ops for memory tables | `casacore-tables/src/table.rs` |
| Demo | `demo_memory_tables()` in `demo.rs` |

**Test counts:** 14 unit tests + 1 demo test.

**No interop tests needed:** saved memory tables produce regular PlainTables
on disk, byte-identical to tables created directly. Existing cross-matrix
tests already cover this.

## Lessons learned

- **In Rust, all Table data is already in-memory.** The `TableImpl` struct
  stores rows as `Vec<RecordValue>` regardless of `DataManagerKind`. Memory
  tables are therefore a thin API layer (constructors, `TableKind` enum,
  locking no-ops) rather than a new storage mechanism.

- **Memory-ness is orthogonal to `DataManagerKind`.** Adding a `MemoryStMan`
  variant to the DM enum would be misleading — the DM kind only matters at
  save time, and saved memory tables use a real DM (StManAipsIO or
  StandardStMan). A separate `TableKind` enum is the right abstraction.

- **Locking no-ops need early returns, not separate implementations.**
  Rather than forking the locking code path, adding `if self.kind ==
  TableKind::Memory { return Ok(...); }` at the top of each locking method
  is minimal and matches how C++ `MemoryTable` overrides its BaseTable
  lock methods.

- **No new files needed for a lightweight feature.** `TableKind`, the memory
  constructors, `to_memory()`, and all unit tests fit cleanly into the
  existing `table.rs` module. Creating a separate `memory_table.rs` would
  have added indirection for little benefit.
