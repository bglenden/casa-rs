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
