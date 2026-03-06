# Wave 6 - TableMeasures descriptors and column accessors

## Origin

- Backlog items: 5.1, 5.2, 5.3, 5.4, 5.5.

## Goal

- Implement `MEASINFO` descriptor persistence and typed scalar/array measure
  column access for fixed and variable refs/offsets.

## Non-goals

- Full coordinate projection stack.
- MeasurementSet typed wrappers.

## Scope

### Read path

- Reconstruct `MEASINFO` including `VarRefCol`, `RefOffCol`, and tab ref maps.

### Write path

- Persist `MEASINFO` and linked ref/offset metadata with C++-compatible keys.

### API/docs/demo

- Add typed table measure descriptor/accessor APIs and examples.

## Dependencies

- Wave 2 completed.
- Wave 4 completed.

## Ordering constraints

- Must run after Waves 2 and 4.
- Must run before Wave 7.

## Files likely touched

- `crates/casacore-tables/src/storage/table_control.rs`
- `crates/casacore-tables/src/table.rs`
- `crates/casacore-tables/tests/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [x] C++ reference paths identified in `../casacore` (class + function names).
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [x] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Data-table dependency reviewed (or marked N/A).
- [x] Performance workload defined or marked N/A.
- [x] Non-goals documented.

## Implementation checklist

- [x] Add read/write support for `MEASINFO` records and linked ref/offset cols.
- [x] Implement scalar and array measure accessors with conversion hooks.
- [x] Add fixture tests for fixed-ref, var-ref, fixed-offset, and var-offset.

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [x] Endian matrix (if applicable). N/A — keyword format is storage-manager–independent.
- [x] Edge cases (empty/zero-length/undefined/boundary/variable refs).
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [x] Clean skip when measures data tables are unavailable. N/A — no data tables needed.

## Performance plan

- Workload: bulk read/write of measure-tagged columns.
- Rust command: release benchmark for measure column put/get throughput.
- C++ command: `tTableMeasures`/`dVarRefMdirCol` style workload.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [x] All Phase 3 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed. N/A — existing `t_table` demo already covers measure patterns.

## Results

- Date: 2026-03-05
- Commit: (pending)
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (all tests, zero failures)
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
- Interop matrix:
  - RR: 8 tests (epoch fixed/var-int/var-str, direction fixed, measinfo reconstruct, quantum coexist, scalar meas mut, array meas mut)
  - RC: 3 tests (epoch fixed, epoch var-int, direction fixed)
  - CR: 4 tests (epoch fixed, epoch var-int, epoch var-str, direction fixed)
  - CC: 1 test (epoch roundtrip)
- Performance (release mode, 10K rows × 10 iterations):
  - Epoch read: Rust 0.09x C++ (Rust ~11x faster)
  - Direction read: Rust 0.06x C++ (Rust ~17x faster)
  - All below 2.0x threshold
- Skips/blockers/follow-ups:
  - `EpochRef::TT` stored as `"TDT"` in TabRefTypes for C++ casacore 3.7.1 compat (added `casacore_name()` method)
  - Fixed sub-record RecordDesc binary format bug (write empty RecordDesc for TpRecord fields)
  - `TableMeasDesc::write()` now also writes `QuantumUnits` (required by C++ ArrayMeasColumn)

## Lessons learned

- C++ casacore 3.7.1 uses "TDT" not "TT" as string name for Terrestrial Time — must use `casacore_name()` in TabRefTypes.
- C++ RecordDesc writes an empty sub-record schema (nfields=0) for `TpRecord` fields; actual content is in the RecordRep's Variable-type TableRecord. Getting this wrong causes "AipsIO: read beyond end of object" errors.
- C++ `TableMeasDesc::write()` writes both MEASINFO and QuantumUnits keywords; Rust must do the same or `ArrayMeasColumn` construction fails with assertion errors.
- Data columns are `ArrayColumnDesc<Double>` requiring `ArrayMeasColumn` (not `ScalarMeasColumn`) in C++, even for scalar measures stored as 1-element arrays.
