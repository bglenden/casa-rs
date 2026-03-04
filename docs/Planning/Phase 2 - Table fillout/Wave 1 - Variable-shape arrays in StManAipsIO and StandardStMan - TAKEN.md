# Wave 1 - Variable-shape arrays in StManAipsIO and StandardStMan

## Origin

- Backlog item: 1.1.

## Goal

- Read/write variable-shape array columns via indirect storage in
  `StManAipsIO` and `StandardStMan`, including undefined cells.
- Reach full 2x2 C++ interop parity, including endian cross-matrix coverage.

## Non-goals

- Record-column serialization (`TpRecord`) in this wave.
- Table-level API redesign beyond what is required for variable arrays.

## Scope

### Read path

- `stman_aipsio.rs`: indirect-array read using shape header + flat Fortran-order
  payload.
- `standard_stman.rs`: indirect-array bucket read for variable-length records.
- Both managers: undefined-cell detection for variable-shape columns.

### Write path

- `stman_aipsio.rs`: matching indirect-array write support.
- `standard_stman.rs`: matching indirect-array write support.
- Both managers: write undefined cells in C++-compatible format.

### API/docs/demo

- Add interop fixtures/tests for this capability.
- No new public API surface expected.

## Dependencies

- C++ casacore headers/libs from Homebrew.
- C++ source reference in `../casacore`.

## Files likely touched

- `crates/casacore-tables/src/storage/stman_aipsio.rs`
- `crates/casacore-tables/src/storage/standard_stman.rs`
- `crates/casacore-tables/src/storage/stman_array_file.rs`
- `crates/casacore-test-support/src/cpp/*vararray*.cpp`
- `crates/casacore-test-support/tests/tables_cross_matrix_variable_arrays.rs`

## Definition of Ready

- [x] C++ reference paths identified in `../casacore`.
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC) for AipsIO + SSM.
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Performance workload defined (10k variable-shape cells).
- [x] Non-goals documented.

## Implementation checklist

- [x] Add variable-array indirect storage support for AipsIO + SSM.
- [x] Add C++ shim functions for variable-array fixtures.
- [x] Add cross-matrix + endian tests for variable arrays.
- [ ] Run release-mode Rust vs C++ performance comparison.
- [ ] Run full workspace closeout gates.

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) for AipsIO and SSM.
- [x] Endian cross-matrix for AipsIO and SSM variable arrays.
- [x] Edge cases (empty arrays, zero-length axis, mixed defined/undefined).
- [x] Clean skip path when `pkg-config casacore` is unavailable.

## Performance plan

- Workload: read/write 10k variable-shape array cells.
- Rust command: `cargo test --release -p casacore-test-support --test tables_cross_matrix_variable_arrays`.
- C++ command: use matching C++ fixture path in test shim for comparison run.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- [ ] Performance comparison recorded and within threshold (or follow-up filed).
- [ ] Public docs/demos updated if user-visible workflow changed.

## Results

- Date: 2026-03-04
- Commit: `164e045`
- Commands:
  - `cargo test -p casacore-test-support --test tables_cross_matrix_variable_arrays -- --nocapture` -> PASS (4 passed, 0 failed)
- Interop matrix:
  - RR: PASS
  - RC: PASS
  - CR: PASS
  - CC: PASS
- Endian matrix:
  - AipsIO variable arrays: PASS
  - SSM variable arrays: PASS
- Performance:
  - Rust: not run yet
  - C++: not run yet
  - Ratio: pending
- Skips/blockers/follow-ups:
  - Full workspace closeout gate still pending.
  - Performance benchmark still pending.

## Lessons learned

- Results blocks with explicit command output make wave state clear while still
  in `-TAKEN`.
- Requiring a Definition of Ready reduced ambiguity in test scope before coding.
