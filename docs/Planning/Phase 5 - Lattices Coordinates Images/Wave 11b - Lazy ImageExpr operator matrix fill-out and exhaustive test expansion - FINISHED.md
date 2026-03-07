# Wave 11b - Lazy ImageExpr operator matrix fill-out and exhaustive test expansion

## Origin

- Follow-up parity correction after Wave 10 review.
- Delegated middle pass in the split Wave 11 track.
- Constrained by the Wave 11a DAG, operator matrix, test patterns, and public
  API freeze.

## Goal

- Fill out the remaining lazy `ImageExpr<T>` operator and function matrix using
  the exact contracts frozen in Wave 11a, and expand unit and C++ interop tests
  to exhaustive coverage without redesigning the execution model.

## Non-goals

- DAG redesign, new public API design, or error-model changes.
- Parser or `.imgexpr` persistence compatibility.
- `TempImage` implementation.
- Final closeout/perf signoff.

## Scope

### Read path

- Add all remaining expression operators/functions from the Wave 11a matrix on
  top of the existing lazy execution engine.
- Reuse the established slice/chunk evaluation patterns and metadata behavior.

### Write path

- Keep `ImageExpr<T>` read-only with no mutation-surface changes.

### API/docs/demo

- Expand operator-level docs only where needed to reflect the fixed Wave 11a
  contracts.
- Do not introduce new public concepts in this wave.

## Dependencies

- Wave 11a completed.

## Ordering constraints

- Must run after Wave 11a.
- Must finish before Wave 11c closeout and before Wave 12a/12b.

## Files likely touched

- `crates/casacore-images/src/image_expr.rs`
- `crates/casacore-images/tests/`
- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] Wave 11a Results include a frozen operator/function matrix and delegated
      checklist.
- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Implement the remaining operators/functions exactly as enumerated in the
      Wave 11a matrix.
- [ ] Expand exhaustive Rust tests across the supported pixel-type matrix and
      edge cases established in Wave 11a.
- [ ] Expand exhaustive Rust/C++ interop coverage following the Wave 11a test
      patterns.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.
- [ ] Exhaustive operator/function interop matrix for the Wave 11a delegated
      checklist.

## Performance plan

- Workload: representative lazy-expression reads over the newly added operator
  families using the Wave 11a benchmark pattern.
- Rust command: release benchmark reusing the Wave 11a workloads.
- C++ command: matching expression-read benchmark reusing the Wave 11a
  workloads.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 5 closeout gates pass.
- [ ] No public API or execution-model redesign escaped this wave.
- [ ] Results clearly identify any checklist items left for Wave 11c review
      rather than silently deferring them.

## Results

- Date: 2026-03-06
- Commit: `1e71366` (closeout reviewed in Wave 11c on top of uncommitted worktree changes)
- Commands:
  - `cargo test -p casacore-images image_expr -- --nocapture` -> PASS
  - `cargo test -p casacore-test-support --test image_expr_interop -- --nocapture` -> PASS
  - `cargo clippy -p casacore-images -p casacore-test-support --all-targets -- -D warnings` -> PASS
- Interop matrix:
  - RR: Rust unit coverage for lazy operator families passed.
  - RC: Rust-authored persistent images and materialized expressions were read by the C++ shim.
  - CR: C++-authored persistent images were consumed by Rust lazy expressions across unary, binary, scalar, and mask cases.
  - CC: Not exercised directly in this wave; Wave 11c treats the shim as the C++ reference side rather than shipping a standalone C++ fixture round-trip.
- Iterator matrix:
  - full: `ImageExpr::get()` paths covered by Rust unit tests.
  - strided: Not expanded in 11b; deferred to Wave 11c audit and later image closeout waves.
  - tiled/chunked: Not expanded in 11b.
  - region/mask-aware: Mask composition covered; region/chunk traversal remained a closeout item.
- Performance:
  - Rust: N/A in 11b by design; consolidated lazy-expression perf moved to Wave 11c.
  - C++: N/A in 11b by design.
  - Ratio: N/A in 11b by design.
- Skips/blockers/follow-ups:
  - Adequacy review in Wave 11c found the initial 11b interop expansion was not actually exhaustive: `fmod` and `atan2` were implemented but omitted from the Rust/C++ parity matrix.
  - Wave 11c closed that gap and also filled the missing symmetric convenience helpers for the newer binary math families.
  - 11b is therefore acceptable only when read together with the Wave 11c corrective closeout.

## Lessons learned

- "Exhaustive" needs to be checked against the actual operator table, not inferred from the size of a diff.
