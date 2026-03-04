# Wave 11: Incremental Storage Manager

**Why:** Excellent compression for columns that change slowly across rows
(e.g. antenna positions, flags that are mostly the same).

**C++ reference:** `IncrementalStMan`, `ISMBase`, `ISMColumn`.

## Scope

1. **Read support** — open and read C++-written ISM tables.
2. **Write support** — create ISM tables readable by C++.
3. **Mixed DM** — ISM columns alongside SSM/AipsIO columns in one table.

## Tests

- Write slowly-changing column, verify compression ratio.
- Mixed storage managers in one table.
- 2×2: Full interop both directions.

## Notes from earlier waves

- Each storage manager has its own byte-order rules. C++ `ISMBase` does check
  `asBigEndian()` (unlike StManAipsIO which ignores it). Survey `ISMBase` and
  `ISMColumn` to determine exactly which fields are always-BE vs table-endian.
- The C++ shim FFI doesn't currently support endian parameters; consider adding
  that if endian-aware CR tests are needed.
