# Wave 12a - ImageExpr parser grammar subset and entrypoints

## Origin

- Follow-up parity correction after Wave 10 review.
- Depends on the completed split Wave 11 track.

## Goal

- Add casacore-compatible `ImageExpr` parsing on top of the Wave 11c-frozen
  lazy execution engine, while freezing the supported grammar subset and
  parser entrypoints for downstream persistence/interoperability work.

## Non-goals

- `.imgexpr` persistence and reopen behavior; that belongs in Wave 12b.
- `TempImage` implementation.
- History interoperability checks.
- New image features outside expression parity.

## Scope

### Read path

- Parse casacore-style image expressions and lower them into the lazy DAG.
- Support the Wave 11 expression subset from strings, including operator
  precedence, quoted filenames, and escape handling needed for expression
  references.

### Write path

- No new persistence format work in this subwave.

### API/docs/demo

- Freeze parser entrypoints, supported grammar/features, path resolution rules,
  and parse-error behavior for downstream Wave 12b persistence/shim work.

## Dependencies

- Wave 11c completed.

## Ordering constraints

- Must run after Wave 11c.
- Must land before Wave 12b.
- Must land before Wave 14 remaining-LEL implementation and Wave 15 final
  module-wide closeout.

## Files likely touched

- `crates/casacore-images/src/image_expr.rs`
- `crates/casacore-images/src/lib.rs`
- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/src/lib.rs`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [x] C++ reference paths identified in `../casacore` (class + function names).
      `ImageExprParse::command()`, `LatticeExprNode`, `LELInterface`.
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
      RR: unit tests, CR: 5 parser interop tests, RC: `save_as` test,
      CC: C++ `ImageExprParse::command` reference.
- [x] Path quoting, escaping, and relative-path behavior identified from C++
      tests. Single/double quotes, bare identifiers with `./_/~/` chars.
- [x] Endian and undefined-cell behavior reviewed for touched formats.
      N/A — parser operates on in-memory DAG, no new on-disk format.
- [x] Data-table dependency reviewed (or marked N/A). N/A.
- [x] Iterator workload and edge cases identified (or marked N/A). N/A —
      expression evaluation delegates to existing ImageExpr lazy engine.
- [x] Performance workload defined or marked N/A.
      `parsed_lel_expr_perf_vs_cpp` benchmark: parse + full-read.
- [x] Non-goals documented.

## Implementation checklist

- [x] Implement parser support for the casacore expression forms needed for
      the Wave 11 frozen operator/function subset.
      `crates/casacore-images/src/expr_parser.rs` — complete recursive descent
      parser with tokenizer. 36 unit tests, 3 doc tests.
- [x] Implement precedence, associativity, quoted filenames, escaping, and
      path-token handling needed for supported expressions.
      8-level precedence table matching C++ LEL, right-associative `^`,
      single/double/bare path tokens.
- [x] Add Rust/C++ interoperability checks for parsed expression evaluation.
      5 cross-language tests in `image_expr_interop.rs`: arithmetic,
      transcendental, mask, composite, and quoted-path-with-special-chars.
- [x] Publish the exact supported grammar/features for Wave 12b to treat as
      fixed. EBNF grammar, precedence table, supported functions, deferred
      features all in `expr_parser.rs` module docs.

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
      RR: 36 unit tests. CR: 5 parser interop tests. RC: `save_as` test
      (Wave 11). CC: C++ LEL reference used for validation.
- [x] Endian matrix (if applicable). N/A — pure in-memory DAG construction.
- [x] Iterator traversal matrix (if applicable). N/A — delegates to lazy engine.
- [x] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
      Empty expression, whitespace-only, bare scalars (no shape context),
      type mismatches (mask vs numeric), unknown functions, unknown images.
- [x] Clean skip when `pkg-config casacore` is unavailable.
      All interop/perf tests guard on `cpp_backend_available()`.
- [x] Clean skip when measures data tables are unavailable. N/A.
- [x] Parser-string interoperability checks against C++ for representative
      arithmetic, transcendental, mask, and quoted-path expressions.
      5 tests, all pass with < 1e-6 tolerance.
- [x] Negative parse tests covering empty expressions, malformed quoting,
      unknown images, and precedence-sensitive cases.
      10 negative tests: empty, whitespace, unterminated quote, unknown
      image, unknown function, mismatched parens, trailing operator,
      lone `=`, mask-where-numeric, numeric-where-mask.

## Performance plan

- Workload: parse supported expressions, build the DAG, and sample small slices
  from the parsed expression.
- Rust command: release benchmark for parse plus small-slice reads.
- C++ command: matching parser/expression benchmark.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [x] All Phase 5 closeout gates pass.
      `cargo fmt`, `cargo clippy --workspace -D warnings`, `cargo test --workspace`
      all pass (interop tests require `--test-threads=1` due to pre-existing
      C++ concurrency issue).
- [x] Public docs updated at C++ doxygen-comparable detail.
      Module docs: EBNF grammar, precedence table, path quoting rules, deferred
      features, example. All public types/traits/functions have `///` docs.
- [x] Demo added/updated if user-visible workflow changed.
      `t_image_expr.rs` updated with parser API section.
- [x] Results document supported grammar/features, precedence rules, path
      resolution behavior, and any explicitly deferred parity cases.
      See Results section below.

## Results

- Date: 2026-03-06
- Commit: (pending)
- Commands:
  - `cargo fmt --all -- --check` -> PASS
  - `cargo clippy --workspace --all-targets -- -D warnings` -> PASS
  - `cargo test --workspace` -> PASS (interop: `--test-threads=1`)
  - `cargo run -p casacore-images --example t_image_expr` -> PASS
- Interop matrix:
  - RR: 36 unit tests + 3 doc tests (PASS)
  - RC: `saved_lazy_expr_is_cpp_readable` (PASS, Wave 11)
  - CR: 5 parser interop tests (PASS)
  - CC: C++ `ImageExprParse::command` used as reference
- Grammar/status:
  - supported: `+ - * / ^`, unary `- +`, comparisons `> < >= <= == !=`,
    logical `&& || !`, 1-arg functions (sin cos tan asin acos atan sinh
    cosh tanh exp log log10 sqrt abs ceil floor round sign conj), 2-arg
    functions (pow fmod atan2 min max), constants (pi() e()), numeric
    literals, single/double/bare-ident image references.
  - deferred: `$n` temps, region refs `::region`, array literals `[...]`,
    `indexin`/`indexnotin`, reduction functions, type conversions, `iif`,
    complex literal suffix `i`, `%` infix modulo.
- Iterator matrix: N/A (parser delegates to existing lazy engine)
  - full: N/A
  - strided: N/A
  - tiled/chunked: N/A
  - region/mask-aware: N/A
- Performance (64x64, 50 passes, parse + full-read):
  - Rust: 6.2 ms
  - C++: 106.8 ms
  - Ratio: 0.06× (Rust ~17× faster)
- Skips/blockers/follow-ups:
  - C++ interop tests SIGSEGV when run multi-threaded (pre-existing; use
    `--test-threads=1`).
  - Image-to-image comparisons not yet supported (deferred to Wave 14).
  - Complex-valued expression support deferred to Wave 14.

## Lessons learned

- Recursive descent parser with constant folding at parse time is
  straightforward to implement and produces competitive performance.
- The `ExprNode` enum (Numeric/Scalar/Mask) handles the three expression
  types cleanly and allows scalar sub-expressions to be folded without
  building DAG nodes.
- Scalar-on-left for non-commutative ops (e.g. `10.0 - 'a'`) was
  initially implemented via a multiply-by-zero + add-scalar broadcast
  pattern. This was later found to corrupt Inf/NaN values and was
  replaced by `ImageExpr::scalar_left_binary()` (see Post-closeout fixes).
- Boxing large enum variants (`ImageExpr` is 328 bytes) is necessary
  to satisfy clippy's `large_enum_variant` lint; the double-deref
  pattern `(**boxed_ref).clone()` is needed when matching on `&EnumVariant(Box<T>)`.

## Post-closeout fixes

Two bugs were identified and fixed after initial closeout:

1. **`promote_scalar` corrupts Inf/NaN** — The `promote_scalar()` method
   in `expr_parser.rs` used `reference.clone().multiply_scalar(0.0).add_scalar(s)`
   to broadcast a scalar to image shape. This produces `0 * Inf = NaN` and
   `0 * NaN = NaN`, breaking expressions like `1.0 / (0.0 * 'img')` where
   division by zero yields Inf. Fix: removed `promote_scalar()` entirely.

2. **Missing `scalar_left_binary` method** — The lazy engine only had
   `binary_scalar(lhs_image, scalar, op)` (scalar on the right). For
   non-commutative operators with scalar on the left (e.g. `10.0 - 'img'`),
   the parser now uses `ImageExpr::scalar_left_binary(scalar, rhs, op)`, which
   builds a `BinaryOp { lhs: Scalar, rhs: image_node }` DAG node directly.

   Regression tests added: `scalar_minus_inf_preserves_inf`,
   `scalar_divide_inf_preserves_zero`.

- Date: 2026-03-06
- Files changed: `expr_parser.rs`, `image_expr.rs`

