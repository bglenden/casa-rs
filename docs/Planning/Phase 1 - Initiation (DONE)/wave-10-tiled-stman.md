# Wave 10: Tiled Storage Managers

**Why:** Efficient access to large N-dimensional arrays (e.g. visibility
cubes in MeasurementSets). Tiling aligns I/O with access patterns.

**C++ reference:** `TiledStMan`, `TiledShapeStMan`, `TiledColumnStMan`,
`TiledCellStMan`, `TSMCube`.

## Scope

1. **TiledShapeStMan** — the modern default for array columns. Tiles stored
   in hypercubes with configurable tile shape.

2. **TiledColumnStMan** — simpler variant where all cells in a column share
   the same shape.

3. **Read support** — open and read C++-written tiled tables.

4. **Write support** — create tiled tables readable by C++.

5. **Tile cache** — configurable in-memory cache for accessed tiles.

## Tests

- Write tiled table, reopen, verify array data.
- Various tile shapes and access patterns.
- 2×2: Rust writes tiled → C++ reads; C++ writes tiled → Rust reads.
- Performance comparison: tiled vs. StandardStMan for large arrays.

## Notes from earlier waves

- Each storage manager has its own byte-order rules. StManAipsIO always uses
  canonical (BE); StandardStMan has a split model (metadata BE, column data
  respects table endian). Survey C++ `TSMCube`/`TiledStMan` to determine which
  fields are always-BE vs table-endian before implementing.
- The C++ shim FFI doesn't currently support endian parameters; consider adding
  that if endian-aware CR tests are needed.
