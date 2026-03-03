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
