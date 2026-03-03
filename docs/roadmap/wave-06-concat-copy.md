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

## Implementation summary

| Component | Location |
|-----------|----------|
| `ConcatTable`, `ConcatRows` | `casacore-tables/src/concat_table.rs` |
| `Table::concat`, `deep_copy`, `shallow_copy` | `casacore-tables/src/table.rs` |
| On-disk read/write | `storage/table_control.rs` + `storage/mod.rs` |
| C++ interop shims | `casacore-test-support/src/cpp/casacore_cpp_table_shim.cpp` |
| Interop tests | `tests/tables_concat_table_interop.rs`, `tests/tables_deep_copy_interop.rs` |
| Demo | `demo_concat_and_copy()` in `demo.rs` |

**Test counts:** 12 unit tests + 8 interop tests (4 concat + 4 deep copy).

## Lessons learned

- **C++ `ConcatTable` constructor has many overloads.** The `Block<String>`
  constructor takes 6 arguments (tableNames, subTableNames, subDirName,
  option, lockOptions, tsmOption), not just the names. Always check the
  header file for the exact signature.

- **`Block<String>` serialization is wrapped in an AipsIO "Block" envelope.**
  It uses `putstart("Block", 1)` with a `u32` count, not flat strings. This
  matters for reading/writing the ConcatTable `table.dat` subtable names.

- **Shared path helpers prevent duplication.** Both `RefTable::save` and
  `ConcatTable::save` need `strip_directory` / `add_directory`. Moving
  these to `storage/mod.rs` as `pub(crate)` was the right call.

- **ConcatTable materialization on open matches RefTable pattern.** When
  `Table::open()` encounters a ConcatTable on disk, it recursively loads
  all constituent tables and collects rows into a single `StorageSnapshot`
  — same strategy that already worked for RefTable.

- **`deep_copy` for a plain `Table` is just `save` with different options.**
  No special logic needed — the storage layer handles DM conversion
  transparently.
