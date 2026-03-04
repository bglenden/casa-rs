# Wave 1 — Variable-Shape Arrays in StManAipsIO and StandardStMan

## Origin

Backlog item 1.1.

## Goal

Read and write variable-shape array columns via indirect storage in
`StManAipsIO` and `StandardStMan`, including undefined (absent) cells.
Achieve full 2x2 C++ interop parity with tiled storage managers.

## Scope

### Read path
- `stman_aipsio.rs`: indirect-array read — per-row shape header + flat
  Fortran-order data.
- `standard_stman.rs`: indirect-array bucket handling — variable-length
  records in SSM string/indirect buckets.
- Both: detect and handle undefined (absent) cells for variable-shape columns.

### Write path
- `stman_aipsio.rs`: matching indirect-array write path.
- `standard_stman.rs`: matching indirect-array write path.
- Both: write undefined cells correctly.

## Files to Modify

- `crates/casacore-tables/src/storage/stman_aipsio.rs`
- `crates/casacore-tables/src/storage/standard_stman.rs`

## Tests

- New `CppTableFixture` variants: `VariableArray` (AipsIO),
  `SsmVariableArray` (SSM).
- C++ shim write/verify functions for each.
- Full 2x2 cross-matrix (Rust-write/C++-read, C++-write/Rust-read) for both
  storage managers.
- Endian cross-matrix.
- All scalar and array types exercised.
- Edge cases: empty array cell, zero-length axis, 1-D and N-D shapes,
  undefined cells interspersed with defined ones.

## Performance

- Release-mode benchmark: read/write 10k variable-shape array cells.
- Compare Rust vs C++ times; flag if Rust exceeds 2x C++.

## Demo

- If C++ has a relevant demo, provide Rust equivalent with C++ code in
  comments above corresponding Rust lines.

## Closeout Criteria

1. `cargo test --workspace` passes with all new tests green.
2. 2x2 interop matrix fully covered for both storage managers.
3. Endian cross-matrix passes.
4. Undefined-cell round-trip verified against C++.
5. Performance within 2x of C++ on the benchmark.
6. `cargo clippy --workspace --all-targets -- -D warnings` clean.
7. `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`.
8. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` clean.
9. SPDX headers on all new `.rs` files.
10. Lessons learned appended below.

## Lessons Learned

_(to be filled at wave completion)_
