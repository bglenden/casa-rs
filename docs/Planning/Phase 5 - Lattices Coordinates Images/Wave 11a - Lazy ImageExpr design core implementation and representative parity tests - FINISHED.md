# Wave 11a - Lazy ImageExpr design core implementation and representative parity tests

## Origin

- Follow-up parity correction after Wave 10 review.
- First subwave in the split Wave 11 track.

## Goal

- Replace the eager/materialized `ImageExpr<T>` implementation with a lazy,
  read-only execution model and land a representative operator set with unit
  and C++ interop tests that constrain Wave 11b.

## Non-goals

- Full parser or `.imgexpr` persistence compatibility.
- `TempImage` implementation.
- Exhaustive operator/function coverage.
- Broad demo/perf closeout beyond laziness validation.

## Scope

### Read path

- Evaluate expression results per requested slice/chunk instead of
  materializing whole-image arrays up front.
- Preserve shape and metadata access without forcing eager evaluation where the
  data can be derived from inputs.
- Cover representative operator families so downstream fill-out is pattern
  driven rather than design driven:
  - image/image arithmetic
  - image/scalar broadcasting
  - unary numeric transforms
  - representative comparison and logical composition
  - representative transcendental/math functions
  - one representative reduction or shape-changing operation if the final DAG
    contract supports it; otherwise explicitly defer reductions in Results

### Write path

- Reject mutation through `ImageExpr<T>` with explicit read-only errors.

### API/docs/demo

- Freeze the lazy `ImageExpr<T>` execution contract, supported typed DAG node
  families, operator-family matrix, and read-only semantics.
- Update public docs to stop describing `ImageExpr<T>` as eager.

## Dependencies

- Current generic image baseline from Waves 7-10.

## Ordering constraints

- Must run before Waves 11b, 11c, and 12.
- Should land before any delegated operator fill-out or parser/shim expansion.

## Files likely touched

- `crates/casacore-images/src/image_expr.rs`
- `crates/casacore-images/src/image.rs`
- `crates/casacore-images/src/iterator.rs`
- `crates/casacore-images/src/lib.rs`
- `crates/casacore-images/tests/`
- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [ ] C++ reference paths identified in `../casacore` (class + function names).
- [ ] 2x2 interop fixtures identified (RR, RC, CR, CC) for representative
      operator-family coverage.
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [ ] Data-table dependency reviewed (or marked N/A).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [ ] Performance workload defined or marked N/A.
- [ ] Non-goals documented.

## Implementation checklist

- [ ] Replace eager `ArrayD` storage with a typed lazy expression DAG.
- [ ] Route `get`, `get_slice`, and chunk/iterator paths through region-scoped
      evaluation.
- [ ] Implement representative operators from each planned operator family with
      unit tests and C++ interop checks.
- [ ] Add explicit read-only mutation errors and metadata propagation tests.
- [ ] Produce a frozen operator/function matrix that marks what Wave 11b must
      fill in without further API or DAG redesign.

## Test plan

- [ ] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.
- [ ] Laziness regression proving small-slice reads do not materialize the
      whole expression result.
- [ ] Representative Rust/C++ interop checks for each operator family chosen in
      this wave.

## Performance plan

- Workload: large expression image with repeated small-slice and chunk reads.
- Rust command: release benchmark covering `get_slice` and iterator traversal.
- C++ command: matching representative expression-read benchmark.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [ ] All Phase 5 closeout gates pass.
- [ ] Public docs updated at C++ doxygen-comparable detail.
- [ ] Demo added/updated if user-visible workflow changed.
- [ ] Wave results freeze the DAG node model, evaluation contract, operator
      families, and read-only error behavior for downstream waves.
- [ ] Wave results include a decision-complete checklist for Wave 11b with no
      remaining design choices.

## Results

- Date: 2026-03-06
- Commit: `1e71366` (Wave 11 closeout validated on top of the current uncommitted worktree)
- Commands:
  - `cargo test -p casacore-images image_expr -- --nocapture` -> PASS
  - `cargo test -p casacore-test-support --test image_expr_interop -- --nocapture` -> PASS
  - `cargo test --workspace` -> PASS during Wave 11c closeout
- Interop matrix:
  - RR: Representative lazy-expression unit coverage passed for lazy slicing, metadata propagation, and read-only semantics.
  - RC: Rust-authored persistent images and saved expression materializations were readable by the C++ shim.
  - CR: C++-authored images were consumed by Rust lazy expressions for the representative Wave 11a operator families.
  - CC: Not a direct Wave 11a target; the C++ shim served as the parity reference.
- Iterator matrix:
  - full: Full evaluation paths passed through `ImageExpr::get()`.
  - strided: Local lazy-slice behavior was validated; dedicated strided expression traversal stayed out of scope here.
  - tiled/chunked: Deferred beyond the representative design pass.
  - region/mask-aware: Representative comparison and logical mask composition passed.
- Performance:
  - Rust: Representative lazy small-slice reads were validated functionally; consolidated performance signoff moved downstream.
  - C++: Representative parity checks passed through the shim.
  - Ratio: Deferred to Wave 11c closeout/performance pass.
- Skips/blockers/follow-ups:
  - Wave 11a successfully froze the core DAG, lazy evaluation model, and read-only contract.
  - The delegated Wave 11b fill-out list was the remaining unary/binary math and comparison families later completed and audited in Waves 11b/11c.
  - No reductions or shape-changing expression nodes were added; later waves must continue to treat those as out of scope unless explicitly planned.

## Lessons learned

- The key design decision was to freeze laziness and read-only behavior early so later operator fill-out could remain mechanical instead of architectural.
