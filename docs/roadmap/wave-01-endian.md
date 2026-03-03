# Wave 1: Endian Support

**Why now:** Old casacore files written on big-endian machines (Solaris, SPARC,
older Macs) must be readable. Current code assumes the host endianness for
StandardStMan and uses canonical (big-endian) for AipsIO. Full endian support
means reading any table regardless of the endianness it was written with.

**C++ reference:** `Table::EndianFormat` enum (`BigEndian`, `LittleEndian`,
`LocalEndian`, `AipsrcEndian`), `LECanonicalIO`, `CanonicalIO`.

## Scope

1. **Detect endianness on open** — read the endian marker from `table.dat` and
   propagate to all storage managers and column readers.

2. **Endian-aware StandardStMan** — bucket headers and column data must be read
   in the file's endianness, not assumed host order. The SSM already stores an
   endian flag; honor it.

3. **Endian-aware AipsIO framing** — `AipsIo` already supports `ByteOrder`;
   ensure the table-level open path threads the detected order through.

4. **Write endianness option** — extend `TableOptions` with an `EndianFormat`
   choice (default: `LocalEndian`, i.e. host order). Store the chosen format in
   `table.dat` on save.

5. **LECanonical codec** — if not already present, implement little-endian
   canonical read/write in `casacore-aipsio` alongside the existing big-endian
   canonical path.

## Tests

- Write a table on the current (little-endian) host, verify round-trip.
- Write a table forced to big-endian, reopen and verify.
- Create a reference big-endian table with C++ casacore, read in Rust.
- Create a reference little-endian table with C++ casacore, read in Rust.
- Rust writes big-endian → C++ reads successfully.
- Rust writes little-endian → C++ reads successfully.

## Key files

- `crates/casacore-tables/src/storage/table_control.rs` — endian marker parse
- `crates/casacore-tables/src/storage/standard_stman.rs` — endian-aware buckets
- `crates/casacore-tables/src/storage/stman_aipsio.rs` — thread byte order
- `crates/casacore-tables/src/table.rs` — `TableOptions` endian field
- `crates/casacore-aipsio/src/lib.rs` — LE canonical codec if needed
- `crates/casacore-test-support/` — new interop tests
