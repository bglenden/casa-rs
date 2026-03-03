# Wave 12: Virtual Column Engines

**Why:** Computed columns avoid storing redundant data (e.g. a column that
is always 2× another column, or unit conversions).

**C++ reference:** `VirtColEng`, `VirtScaCol`, `VirtArrCol`,
`ScaledArrayEngine`, `MappedArrayEngine`, `ForwardCol`.

## Scope

1. **Virtual column trait** — `VirtualColumnEngine` trait with `get_scalar`,
   `get_array`, `shape` methods.

2. **Scaled array engine** — virtual column = stored_column * scale + offset.

3. **Forward column engine** — column that delegates to another table's column.

4. **Registration** — `Table::register_virtual_engine(name, engine)`.

## Tests

- Scaled array: write base column, read virtual, verify values.
- Forward column: access column in referenced table.
- 2×2: Rust virtual column → C++ reads (may be limited; C++ must know engine).
