# casa-tables

Read and write casacore tables and evaluate TaQL queries.

## Main types

- `Table` — Row and column access, metadata, and table persistence.
- `TableSchema` — Typed columns, shapes, and table descriptors.
- `TableOptions` — Filesystem path and opening or storage configuration.
- `QueryResult` — Rows or values produced by a table query.

Create an empty in-memory table.

```rust
use casa_tables::Table;

let table = Table::new();
assert_eq!(table.row_count(), 0);
```
