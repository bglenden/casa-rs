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
- [x] Run release-mode Rust vs C++ performance comparison.
- [x] Run full workspace closeout gates.

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

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --all-targets -- -D warnings`
- [x] `cargo test --workspace`
- [x] `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`
- [x] `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- [x] Performance comparison recorded and within threshold (or follow-up filed).
- [x] Public docs/demos updated if user-visible workflow changed.

## Results

- Date: 2026-03-04
- Commit: `137fbb9`
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (all tests passed across all crates)
  - `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75` -> PASS (75.16% coverage)
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
  - `cargo test --release -p casacore-test-support --test tables_cross_matrix_variable_arrays -- --nocapture` -> PASS (4 passed, 0 failed, 0.01s)
- Interop matrix:
  - RR: PASS
  - RC: PASS
  - CR: PASS
  - CC: PASS
- Endian matrix:
  - AipsIO variable arrays: PASS
  - SSM variable arrays: PASS
- Performance (10k rows, 60k total elements, AipsIO, release mode):
  - Write: C++ 5.4 ms, Rust 153.6 ms, ratio 28.2×.
  - Read:  C++ 2.7 ms, Rust 39.0 ms,  ratio 14.6×.
  - Both exceed the 2× alert threshold.
  - Note: The Rust path includes full Table API overhead (in-memory row
    construction, schema validation, RecordValue abstraction layers) while
    the C++ path uses direct ArrayColumn put/get. The comparison measures
    end-to-end API cost, not raw storage I/O alone.
- Skips/blockers/follow-ups:
  - **Follow-up required:** Investigate and reduce Rust/C++ performance gap
    for variable-shape array write (28×) and read (15×). Likely causes:
    per-element Fortran-order iteration, RecordValue allocation overhead,
    and lack of bulk I/O buffering. Consider profiling with `cargo flamegraph`
    and adding buffered write paths.

## Lessons learned

- Results blocks with explicit command output make wave state clear while still
  in `-TAKEN`.
- Requiring a Definition of Ready reduced ambiguity in test scope before coding.
- The 10k-cell benchmark revealed significant overhead in the Rust Table API
  abstraction layers that isn't visible in small fixture tests. Performance
  benchmarks should be run early, not deferred to closeout.
