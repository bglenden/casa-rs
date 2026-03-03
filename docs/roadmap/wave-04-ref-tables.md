# Wave 4: Reference Tables & Selections

**Why:** Selections create views over existing tables without copying data.
This is the foundation for TaQL results and efficient data subsetting.

**C++ reference:** `RefTable`, `RefRows`, `RefColumn`, `RowNumbers`.

## Scope

1. **RefTable** — a table that references rows/columns of a parent table.
   Reads and writes pass through to the parent.

2. **Row selection** — `Table::select_rows(indices)` → `RefTable` containing
   only the specified rows.

3. **Column projection** — `Table::select_columns(names)` → `RefTable` with
   only the named columns.

4. **Predicate selection** — `Table::select(predicate)` with a closure
   `Fn(&RecordValue) -> bool` → `RefTable`.

5. **Persistence** — reference tables can be saved to disk (store parent path
   + row/column mapping). C++ casacore writes `table.dat` with `RefTable` type.

## Tests

- Select rows, verify view reflects parent data.
- Modify through view, verify parent updated.
- Save/reopen reference table.
- 2×2: Rust creates RefTable → C++ reads; C++ creates RefTable → Rust reads.

## Lessons learned

- **C++ AipsIO serialization wraps containers in object headers.** `std::map`
  serializes as `SimpleOrderedMap` with extra legacy fields (default value,
  incr). `Vector<T>` serializes as `"Array"` with ndim/shape/count, not the
  simpler `"Block"` format. Always check the actual `operator<<` / `operator>>`
  implementations in the C++ source rather than assuming bare data.

- **C++ path conventions are non-trivial.** `Path::stripDirectory` and
  `Path::addDirectory` use a `"./"` / `"././"` prefix convention, not standard
  POSIX relative paths. The `"./"` prefix signals "same parent directory as the
  ref table", while `"././"` means "inside the ref table directory". Both sides
  must use the same convention for interop to work.

- **`Table::save()` takes `&self`, so it cannot set `source_path`.**
  After saving an in-memory table, call `set_path()` explicitly before
  creating a `RefTable` that will be saved to disk. Alternatively, re-open
  the table (which sets `source_path` automatically).
