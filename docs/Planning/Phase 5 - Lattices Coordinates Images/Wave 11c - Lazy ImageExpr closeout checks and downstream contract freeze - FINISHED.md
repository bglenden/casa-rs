# Wave 11c - Lazy ImageExpr closeout checks and downstream contract freeze

## Origin

- Follow-up parity correction after Wave 10 review.
- Final strong-agent closeout pass for the split Wave 11 track.

## Goal

- Audit and close the combined output of Waves 11a and 11b, run the full
  quality/perf/interoperability checks, and freeze a clean downstream contract
  for Wave 12a/12b and later waves.

## Non-goals

- Major new operator design.
- Parser or `.imgexpr` persistence implementation.
- `TempImage` implementation.

## Scope

### Read path

- Re-verify lazy execution behavior, operator-family completeness, metadata
  propagation, and interop parity across the combined Wave 11a/11b surface.

### Write path

- Re-verify read-only behavior and mutation rejection across the finished lazy
  expression surface.

### API/docs/demo

- Finalize the Wave 11 downstream contract so later waves do not revisit DAG,
  operator-family, or error-model decisions.

## Dependencies

- Wave 11a completed.
- Wave 11b completed.

## Ordering constraints

- Must run after Waves 11a and 11b.
- Must finish before Wave 12a begins.

## Files likely touched

- `crates/casacore-images/src/image_expr.rs`
- `crates/casacore-images/src/lib.rs`
- `crates/casacore-images/tests/`
- `crates/casacore-test-support/tests/`
- `docs/Planning/Phase 5 - Lattices Coordinates Images/`

## Definition of Ready

- [ ] Waves 11a and 11b Results are filled in and list no unresolved design
      choices.
- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Audit the Wave 11a/11b operator matrix for completeness against the
      frozen checklist.
- [ ] Run and fix the full lazy-expression quality, interop, and perf checks.
- [ ] Publish the downstream contract that Wave 12a/12b must treat as fixed.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.
- [ ] Full consolidated Rust/C++ lazy-expression matrix across all operator
      families delivered by Waves 11a and 11b.

## Performance plan

- Workload: consolidated lazy-expression slice and chunk workloads over the
  full operator matrix.
- Rust command: release benchmark suite for lazy-expression workloads.
- C++ command: matching expression-read benchmark suite.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 5 closeout gates pass.
- [ ] Results explicitly freeze the downstream Wave 12a/12b contract.
- [ ] Remaining gaps, if any, are documented as blockers rather than silently
      carried into Wave 12a/12b.

## Downstream contract

Wave 12a/12b and later waves must treat the following `ImageExpr` behavior as
fixed:

- The execution model is lazy and slice-local: `get_at`, `get_slice`, and
  `get` evaluate only the requested region instead of materializing
  intermediates.
- `ImageExpr<T>` remains read-only. Mutation entry points return explicit
  `ImageError::ReadOnly("ImageExpr")` errors and later waves must not add
  write-through semantics.
- Metadata propagation is source-derived and persistence remains explicit via
  `save_as`; expression evaluation itself does not create on-disk `.imgexpr`
  artifacts in this wave.
- Supported operator families are frozen as:
  arithmetic (`+`, `-`, `*`, `/`), unary numeric transforms, transcendental
  unary functions, binary math functions (`pow`, `fmod`, `atan2`, `min`,
  `max`), scalar broadcasting, and scalar comparison/logical mask composition.
- The public convenience surface for binary math families is now symmetric
  across scalar, expression, and image operands where Wave 11 exposes helpers.
- The C++ shim-backed interop matrix is the reference parity check for the
  frozen Wave 11 contract.

## Results

- Date: 2026-03-06
- Commit: `1e71366` (validated on top of the current uncommitted worktree)
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS
  - `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps` -> PASS
  - `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75` -> PASS (`76.92%`)
  - `cargo test -p casacore-test-support --test images_perf_vs_cpp lazy_image_expr_closeout_slice_perf_vs_cpp -- --nocapture` -> PASS with performance alert
- Interop matrix:
  - RR: `cargo test -p casacore-images image_expr -- --nocapture` passed, including added helper coverage for `pow_image`, `fmod_image`, and `atan2_image`.
  - RC: Rust-created images and `save_as` outputs were read successfully by the C++ shim in `image_expr_interop`.
  - CR: C++-created images were consumed by Rust lazy expressions across unary, binary, scalar, and comparison/mask cases.
  - CC: Not exercised as a standalone matrix in this wave; the C++ side acts as the reference evaluator behind the shim.
- Iterator matrix:
  - full: Full-array lazy evaluation covered by Rust unit tests and workspace pass.
  - strided: No dedicated new 11c test; existing expression validation remained focused on point/full/slice access.
  - tiled/chunked: No dedicated `ImageExpr` chunk-iterator closeout landed in this wave.
  - region/mask-aware: Logical mask composition and range checks passed in Rust/C++ parity tests.
- Performance:
  - Rust: `243.9 ms` for 25 repeated closeout-expression slice reads over a `96x96` image (`48x48` slice).
  - C++: `28.2 ms` for the matching shim-backed closeout-expression slice workload.
  - Ratio: `8.66x` slower than C++, exceeding the `2x` alert threshold.
- Skips/blockers/follow-ups:
  - Wave 11b adequacy review found that `fmod` and `atan2` had shipped without corresponding exhaustive interop coverage; Wave 11c added that missing parity coverage and symmetric helper methods.
  - The consolidated lazy-expression correctness/interop gates are green, but the new performance smoke is well outside the target threshold. This is an explicit blocker/follow-up for later image-performance work and must not be silently ignored by Wave 12a/12b.
  - Coverage passed at `76.92%`, clearing the repo’s `75%` threshold.

## Lessons learned

- Closeout waves need to validate the claim of completeness, not just verify that code compiles.
- Freezing a downstream contract is only useful if the remaining performance debt is written down as debt, not treated as "good enough."
