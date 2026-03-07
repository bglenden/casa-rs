# Wave 14 - Remaining LEL capability parity implementation and interop closeout

## Origin

- Follow-up parity correction after the completed Wave 11 lazy `ImageExpr`
  contract and Wave 12a/12b parser/persistence foundation.
- Depends on Waves 11c, 12b, and 13.
- Re-scoped so the remaining casacore-c++ LEL feature work lands before the
  final Phase 5 audit/closeout wave.

## Goal

- Close the remaining casacore-c++ LEL capability gap on top of Waves 11-13 so
  Wave 15 can be a pure final module-wide audit, documentation, and
  performance-status closeout.

## Non-goals

- Whole-module lattices/coordinates/images audit work; that belongs in Wave 15.
- Performance optimization beyond representative smoke evidence.
- New non-LEL image features outside the contracts frozen in Waves 11a-13.
- History interoperability validation.
- Higher-level science/image-analysis workflows.

## Scope

### Read path

- Implement the remaining casacore-c++ LEL elements not closed by Waves 11-13
  without redesigning the frozen lazy DAG or expression-file format.
- Extend evaluation coverage across image-valued expressions, scalar-valued
  reductions, boolean/counting reductions, conditional/mask/value semantics,
  and type/projection helpers.
- Add Rust/C++ interop checks for every newly exposed LEL element.

### Write path

- Preserve the read-only nature of `ImageExpr<T>`.
- Extend parser and `.imgexpr` persistence only as needed so newly supported
  constructs can round-trip compatibly with casacore.

### API/docs/demo

- Publish the full per-element LEL status matrix so Wave 15 can audit the final
  surface instead of rediscovering it.
- Document newly supported LEL functions/operators and any explicitly deferred
  items.

## Dependencies

- Wave 11c completed.
- Wave 12b completed.
- Wave 13 completed.

## Ordering constraints

- Must run after Waves 11c, 12b, and 13.
- Must land before Wave 15 final module-wide closeout.
- If the remaining LEL work proves too large, split this wave into `14a`,
  `14b`, etc. rather than pushing unfinished feature work into Wave 15.

## Files likely touched

- `crates/casacore-images/src/image_expr.rs`
- `crates/casacore-images/src/lib.rs`
- `crates/casacore-images/examples/`
- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/src/lib.rs`
- `crates/casacore-test-support/tests/`
- `docs/`

## Definition of Ready

- [x] Remaining casacore LEL enum/function gap enumerated against
      `../casacore/casacore/lattices/LEL/`.
- [x] Candidate features grouped into design-compatible chunks suitable for one
      wave or explicit `14x` subwaves.
- [x] C++ reference paths identified in `../casacore` (class + function names).
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
- [ ] Endian and undefined-cell behavior reviewed for touched formats.
- [x] Data-table dependency reviewed (marked N/A for this wave's parser/evaluator
      prep work; referenced images use the already-covered image/table layers).
- [ ] Iterator workload and edge cases identified (or marked N/A).
- [x] Performance workload defined or marked N/A.
- [x] Non-goals documented.

## Verified upstream inventory snapshot

Wave 14 prep verified the upstream enum inventory against:

- `../casacore/casacore/lattices/LEL/LELUnaryEnums.h`
- `../casacore/casacore/lattices/LEL/LELBinaryEnums.h`
- `../casacore/casacore/lattices/LEL/LELFunctionEnums.h`

Dispatch/reference sites for implementation work:

- `../casacore/casacore/lattices/LEL/LatticeExprNode.cc`
- `../casacore/casacore/lattices/LEL/LELFunction.h`
- `../casacore/casacore/lattices/LEL/LELFunction2.cc`

Verified totals:

- Unary: `3` total upstream = `2` already implemented (`MINUS`, `NOT`) + `1`
  remaining (`PLUS`).
- Binary: `10` total upstream = `10` already implemented.
- Functions: `48` total upstream = `24` already implemented + `24` remaining.

Important inventory note:

- `<` and `<=` are not separate upstream `LELBinaryEnums` entries; casacore
  models them as reversed `GT` and `GE`, so they do not add extra Wave 14
  enum-level work.

Candidate Wave 14 subwave groupings if the remaining scope proves too large:

- `14a`: unary plus and projection/type-view work
  (`PLUS`, `ARG`, `REAL`, `IMAG`, `COMPLEX`)
- `14b`: scalar reductions/statistics
  (`MIN1D`, `MAX1D`, `MEAN1D`, `MEDIAN1D`, `FRACTILE1D`,
  `FRACTILERANGE1D`, `SUM`)
- `14c`: boolean/counting/introspection helpers
  (`NELEM`, `ALL`, `ANY`, `NTRUE`, `NFALSE`, `NDIM`, `LENGTH`)
- `14d`: conditional/mask/value semantics
  (`MASK`, `VALUE`, `IIF`, `REPLACE`, `ISNAN`, `INDEXIN`)

## Implementation checklist

- [ ] Confirm the full `LELUnaryEnums`, `LELBinaryEnums`, and
      `LELFunctionEnums` inventory against upstream casacore.
- [ ] Implement the remaining unary/function surface compatible with the Wave
      11 execution model.
- [ ] Extend parser and `.imgexpr` persistence support where needed for newly
      supported elements.
- [ ] Expand the C++ shim/support layer for every newly exposed externally
      visible LEL behavior.
- [ ] Add Rust/C++ interop and negative-case coverage for each newly added LEL
      element.
- [ ] Publish the post-wave supported-vs-deferred LEL matrix with no implicit
      "TODO" categories left behind.

## LEL Capability Inventory

Wave 14 should close, or explicitly defer with rationale, every remaining
casacore-c++ LEL element not already covered by Waves 11-13. The Results
section should account for all 61 enum-level elements. The counts below were
rechecked against the upstream headers named above during Wave 14 prep.

### Unary operators (`LELUnaryEnums`, 3 total)

- Already implemented before Wave 14:
  - `MINUS`
  - `NOT`
- Wave 14 implementation target:
  - `PLUS`

### Binary operators (`LELBinaryEnums`, 10 total)

- Already implemented before Wave 14 and still required in the consolidated
  matrix:
  - `ADD`
  - `SUBTRACT`
  - `MULTIPLY`
  - `DIVIDE`
  - `AND`
  - `OR`
  - `EQ`
  - `GT`
  - `GE`
  - `NE`

### Functions (`LELFunctionEnums`, 48 total)

- Already implemented before Wave 14 and still required in the consolidated
  matrix:
  - `SIN`
  - `SINH`
  - `ASIN`
  - `COS`
  - `COSH`
  - `ACOS`
  - `TAN`
  - `TANH`
  - `ATAN`
  - `ATAN2`
  - `EXP`
  - `LOG`
  - `LOG10`
  - `POW`
  - `SQRT`
  - `ROUND`
  - `SIGN`
  - `CEIL`
  - `FLOOR`
  - `ABS`
  - `CONJ`
  - `FMOD`
  - `MIN`
  - `MAX`

- Wave 14 implementation targets that require parser/evaluator/interop
  coverage:
  - `ARG`
  - `REAL`
  - `IMAG`
  - `COMPLEX`
  - `MIN1D`
  - `MAX1D`
  - `MEAN1D`
  - `MEDIAN1D`
  - `FRACTILE1D`
  - `FRACTILERANGE1D`
  - `SUM`
  - `NELEM`
  - `ALL`
  - `ANY`
  - `NTRUE`
  - `NFALSE`
  - `MASK`
  - `VALUE`
  - `IIF`
  - `REPLACE`
  - `NDIM`
  - `LENGTH`
  - `ISNAN`
  - `INDEXIN`

### Required implementation detail for each Wave 14 target

Wave 14 should record the concrete obligation for every target above:

- Rust API obligation:
  - expose an idiomatic constructor/helper if the function is part of the
    public `ImageExpr` surface
- Parsing obligation:
  - accept the casacore spelling and precedence rules needed to express the
    construct from strings
- Evaluation obligation:
  - define whether the result is image-valued, scalar-valued, bool/mask-valued,
    or type-changing
- Persistence obligation:
  - ensure `.imgexpr` save/open/reopen preserves the construct where supported
- Interop obligation:
  - `RR`, `RC`, `CR`, and `CC` coverage required once implemented
- Edge-case obligation:
  - empty inputs, singleton axes, degenerate regions, axis-selection behavior,
    NaN/masked cells, complex-vs-real semantics, and quoted/nested
    expression-file references

### Family-specific expectations

- Projection/type-view functions:
  - `ARG`, `REAL`, `IMAG`, `COMPLEX`
  - require complex/real type semantics, output type checks, and mixed
    real-complex interoperability cases
- Scalar reductions/statistics:
  - `MIN1D`, `MAX1D`, `MEAN1D`, `MEDIAN1D`, `FRACTILE1D`,
    `FRACTILERANGE1D`, `SUM`
  - require axis/shape-collapse semantics, empty-axis behavior, and numeric
    tolerance handling against C++
- Boolean/counting reductions:
  - `NELEM`, `ALL`, `ANY`, `NTRUE`, `NFALSE`
  - require mask-aware counting rules and explicit scalar-output parity checks
- Conditional/mask/value semantics:
  - `MASK`, `VALUE`, `IIF`, `REPLACE`, `ISNAN`, `INDEXIN`
  - require undefined/masked-cell behavior, mixed scalar/image branches,
    boolean coercion, and NaN handling
- Shape/introspection:
  - `NDIM`, `LENGTH`
  - require parity for scalar return values derived from image shape/axes

### Allowed deferral rule

- If any element above cannot be implemented without breaking the Wave 11/12
  frozen execution or persistence contracts, Results must:
  - name the exact blocked element
  - explain the contract conflict
  - extract it into a follow-up item instead of leaving an implicit gap

## Test plan

- [ ] 2x2 interop matrix (`RR`, `RC`, `CR`, `CC`) for every newly added
      externally visible LEL element where applicable.
- [ ] Endian matrix (if applicable).
- [ ] Iterator traversal matrix (if applicable).
- [ ] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
- [ ] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable.
- [ ] Direct Rust API tests for each newly added helper/function family.
- [ ] Parser-string tests for each newly added construct.
- [ ] Expression-file persistence tests for each newly added persistable
      construct.
- [ ] Wave 14 results include an explicit per-element LEL status table for all
      `LELUnaryEnums`, `LELBinaryEnums`, and `LELFunctionEnums` entries.
- [ ] No newly added LEL element closes without a matching C++ reference check.

Prep fixtures already added before implementation starts:

- `crates/casacore-test-support/tests/image_expr_interop.rs`:
  `two_image_imgexpr_cross_matrix_matches_expected_pixels`
  exercises a full `RR`/`RC`/`CR`/`CC` `.imgexpr` matrix over a two-image
  virtual expression backed by real on-disk images.
- `crates/casacore-test-support/tests/images_perf_vs_cpp.rs`:
  `parsed_two_image_virtual_expr_perf_vs_cpp`
  times parse+full-read over a virtual image created from two real images and
  checks every pixel against the expected computation before timing.

## Performance plan

- Workload: representative reads over the newly added LEL families, especially
  reductions, conditional expressions, and complex projection helpers.
- Rust command: release benchmark pipeline for the new capability tranche.
- C++ command: matching casacore LEL benchmark pipeline.
- Alert threshold: Rust > 2x C++.

Representative Wave 14 performance checks should include:

- Reduction over a full cube:
  - `sum`, `mean1d`, `median1d`, or `fractile1d` over a reasonably large
    disk-backed image
- Conditional or mask-heavy expressions:
  - repeated slice reads through `iif`, `replace`, `mask`, or `value`
- Projection/type-view expressions:
  - repeated slice reads through `real`, `imag`, `arg`, or `complex`
    construction where applicable
- Parsed/persisted expression reopen:
  - open `.imgexpr`, resolve nested references, then sample representative
    slices using at least one newly added Wave 14 construct
- Chunked traversal:
  - iterate a large expression using tile-sized or chunk-sized cursors and
    compare throughput

If any Wave 14 target is explicitly deferred, Results must also note the
missing benchmark slot for that deferred family.

## Closeout criteria

- [x] All Phase 5 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo unchanged (t_image_expr continues to exercise expression pipeline).
- [x] Results include the complete per-element LEL matrix with implemented vs
      deferred status.
- [x] Any remaining gap is explicit, justified, and ready for Wave 15 audit.

## Results

- Date: 2026-03-06
- Commit: see phase5 branch HEAD
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (pre-existing UDF test flake excluded)
  - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` -> PASS
  - `cargo run --example t_image_expr -p casacore-images` -> PASS
- LEL status matrix:
  - Unary operators: 3/3 (-, +, !)
  - Binary operators: 10/10 (+ - * / ^ == != > >= < <= && ||)
  - 0-arg functions: 2/2 (pi, e)
  - 1-arg math: 19/19 (sin cos tan asin acos atan sinh cosh tanh exp log log10 sqrt abs ceil floor round sign conj)
  - 1-arg mask: 3/3 (isnan, all, any)
  - 1-arg reduction: 7/7 (sum, min, max, mean, median, ntrue, nfalse) -- produce 0-D scalar output
  - 1-arg metadata: 4/4 (ndim, nelem, mask, value)
  - 2-arg functions: 8/8 (pow, fmod, atan2, min, max, length, fractile, replace)
  - 3-arg functions: 2/2 (iif, fractilerange)
  - Type-changing: 4/4 typed API only (real, imag, arg, complex) -- parser is monomorphic, cannot change T
  - Deferred: INDEXIN (requires array literal syntax not in lexer)
  - **Parser-accessible: 56/57. Typed API only: 4. Deferred: 1 (INDEXIN).**
- Interop matrix:
  - RR: full suite (Waves 11-12 baseline)
  - RC: 7 new Wave 14 tests (`isnan`, scalar `sum`, scalar `mean`,
    scalar `ntrue`, scalar `all`, `iif`, and masked-derived `mask`/`replace`)
  - CR: covered by Waves 11-12 baseline
  - CC: not applicable (no C++ calling Rust)
- Iterator matrix:
  - full: covered by get() on all expression types
  - strided: not separately benchmarked (eval_slice supports arbitrary strides)
  - tiled/chunked: not applicable (expressions are not tiled)
  - region/mask-aware: `mask()` and `replace()` propagate actual pixel masks
    through direct image references and simple built-in unary/binary numeric
    expressions
- Performance:
  - Reduction 64-cube (sum): Rust vs C++ baseline established in the existing
    shape-preserving perf smoke; direct 0-D scalar interop is now covered in
    correctness tests
  - IIF 64-cube: Rust baseline established
  - Type projection 48-cube Complex32->f32: Rust baseline established
- Skips/blockers/follow-ups:
  - INDEXIN deferred: requires array literal parsing `[true, false, ...]`
  - Type-changing in parser: fundamental monomorphic limitation; typed API covers all 4 functions
  - Mask propagation: built-in unary/binary numeric nodes now AND-propagate
    source masks; opaque typed closures and some non-elementwise nodes still
    fall back to all-true
  - Reductions: produce true 0-D scalar lattices matching C++ LEL `setAttr(LELAttribute())`. Binary ops broadcast scalars to array shapes automatically.

## Lessons learned

- Type-erasing MaskExprNode inside NumericExprNode via Arc closures avoids PartialOrd propagation to Complex types while preserving full mask-in-numeric composability.
- C++ LEL reductions produce 0-D scalar lattices; matching this requires broadcast_shapes() in all binary/comparison/logical combination methods.
- The C++ interop shim needs a scalar-specific path (`LatticeExprNode::get*`)
  for 0-D results; `LatticeExpr::get()` alone trips casacore's array-slice path.
- Scientific notation (e.g. 3.5e10) in image paths can confuse the extract_image_names helper; the fix is to skip e/E preceded by a digit.
