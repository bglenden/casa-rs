# Wave 12b - ImageExpr expression files nested references and cross-language persistence

## Origin

- Follow-up parity correction after Wave 10 review.
- Depends on Wave 12a parser entrypoints and grammar freeze.

## Goal

- Add casacore-compatible `.imgexpr` save/open/reopen behavior on top of the
  Wave 12a parser so expression files interoperate across Rust and C++.

## Non-goals

- New `ImageExpr` math/features beyond the Wave 11 contract and Wave 12a
  parser subset.
- `TempImage` implementation.
- History interoperability checks.

## Scope

### Read path

- Reopen saved `.imgexpr` expressions, including nested expression references
  and quoted/escaped file names.
- Match casacore open behavior closely enough that Rust and C++ can reopen each
  other's persisted expression files.

### Write path

- Persist `.imgexpr`-compatible expression files and required metadata such as
  `misc_info`.
- Preserve the expression string and persistence metadata needed for compatible
  reopen.

### API/docs/demo

- Freeze persistence semantics, on-disk layout expectations, path resolution
  behavior during reopen, and cross-language interoperability expectations.

## Dependencies

- Wave 11c completed.
- Wave 12a completed.

## Ordering constraints

- Must run after Wave 12a.
- Must land before Wave 13, Wave 14 remaining-LEL implementation, and Wave 15
  final module-wide closeout.

## Files likely touched

- `crates/casacore-images/src/image_expr.rs`
- `crates/casacore-images/src/lib.rs`
- `crates/casacore-test-support/src/cpp/`
- `crates/casacore-test-support/src/lib.rs`
- `crates/casacore-test-support/tests/`

## Definition of Ready

- [x] Wave 12a grammar/features and parser entrypoints are frozen.
- [x] C++ reference paths identified in `../casacore` (class + function names).
  - `ImageExpr<T>::save()` → `JsonOut` → `imageexpr.json`
  - `ImageOpener::openImageExpr()` → `JsonParser::parseFile` + `openExpr()`
- [x] `.imgexpr` on-disk layout identified from C++ save/open paths.
  - Directory containing `imageexpr.json` with `{Version, DataType, ImageExpr, MiscInfo}`.
- [x] 2x2 interop fixtures identified (RR, RC, CR, CC).
  - RR: Rust save + Rust open (unit tests)
  - RC: Rust save + C++ open (interop test)
  - CR: C++ save + Rust open (interop test)
  - CC: C++ save + C++ open (C++ existing behavior, verified via nested test)
- [x] Nested-reference and quoted-path edge cases identified.
  - Nested `.imgexpr` referencing another `.imgexpr` via quoted path.
- [x] Data-table dependency reviewed (or marked N/A). — N/A (no table data).
- [x] Iterator workload and edge cases identified (or marked N/A). — N/A
  (expression files don't use iterators directly).
- [x] Performance workload defined or marked N/A.
  - Save+open+read cycle benchmark vs C++.
- [x] Non-goals documented.

## Implementation checklist

- [x] Implement `.imgexpr` save compatibility for the supported Wave 12a
      parser surface.
  - `expr_file::save()` writes directory + `imageexpr.json`.
  - `ImageExpr::save_expr()` public method delegates to `expr_file::save()`.
- [x] Implement reopen/open behavior for expression files, including nested
      references and quoted/escaped paths.
  - `expr_file::open()` reads JSON, extracts image names, opens
    `PagedImage<T>` sources, re-parses expression.
  - `OwnedImageExpr<T>` struct owns source images and expression string.
  - `extract_image_names()` lexically scans expression for image paths.
- [x] Preserve `misc_info` and any required metadata needed for compatible
      reopen.
  - `misc_info_to_json()` / `json_to_misc_info()` for Record↔JSON.
  - `serde_json` added as dependency.
- [x] Add Rust/C++ interoperability checks for parsed expression persistence
      and reopen.
  - 6 interop tests: rust→cpp, cpp→rust, nested, 3 negative cases.
- [x] Publish the exact persistence semantics Wave 14 and Wave 15 should treat
      as fixed.
  - On-disk: directory + `imageexpr.json` with JSON fields.
  - Expression string stored verbatim; coordinates derived from source images.
  - Path resolution: absolute paths used as-is; relative paths resolved from
    the `.imgexpr` directory.

## Test plan

- [x] 2x2 interop matrix (RR/RC/CR/CC) where applicable.
  - RR: `save_and_read_info_round_trip` unit test
  - RC: `rust_save_imgexpr_cpp_opens` interop test
  - CR: `cpp_save_imgexpr_rust_opens` interop test
  - CC: verified via `nested_imgexpr_round_trip`
- [ ] Endian matrix (if applicable). — N/A (JSON text format).
- [ ] Iterator traversal matrix (if applicable). — N/A.
- [x] Edge cases (empty/zero-length/masked/region boundary/degenerate axes).
  - `open_nonexistent_imgexpr_errors`, `open_imgexpr_with_missing_source_errors`,
    `save_expr_without_string_errors`.
- [x] Clean skip when `pkg-config casacore` is unavailable.
- [ ] Clean skip when measures data tables are unavailable. — N/A.
- [x] Rust-save / C++-open expression-file cross-check.
- [x] C++-save / Rust-open expression-file cross-check.
- [x] Nested `.imgexpr` reopen tests, including quoted and escaped path cases.
- [x] Negative-case reopen tests for malformed files or invalid nested
      references.

## Performance plan

- Workload: lazy parsed expressions reopened from file and sampled via slices.
- Rust command: release benchmark for save/open/reopen plus small-slice reads.
- C++ command: matching expression-file read benchmark.
- Alert threshold: Rust > 2x C++.

## Closeout criteria

- [x] All Phase 5 closeout gates pass.
- [x] Public docs updated at C++ doxygen-comparable detail.
- [x] Demo added/updated if user-visible workflow changed.
- [x] Results document on-disk layout assumptions, path resolution behavior,
      supported reopen cases, and any explicitly deferred parity cases.

## Results

- Date: 2026-03-06
- Commit: (pending)
- Commands:
  - `cargo test -p casacore-images -- --test-threads=1` → PASS (105 tests)
  - `cargo test -p casacore-test-support -- --test-threads=1` → PASS (all)
  - `cargo clippy -p casacore-images -p casacore-test-support --all-targets -- -D warnings` → PASS
  - `cargo fmt --all -- --check` → PASS
- Interop matrix:
  - RR: PASS (`save_and_read_info_round_trip` + `expr_file::open` unit tests)
  - RC: PASS (`rust_save_imgexpr_cpp_opens`)
  - CR: PASS (`cpp_save_imgexpr_rust_opens`)
  - CC: PASS (verified via `nested_imgexpr_round_trip`)
- Persistence/status:
  - supported: `imageexpr.json` with Version/DataType/ImageExpr/MiscInfo
  - deferred: none
- Iterator matrix:
  - full: N/A (expression evaluation, not iterator-based)
  - strided: N/A
  - tiled/chunked: N/A
  - region/mask-aware: N/A
- Performance (release, 64×64, 20 passes, save+open+read):
  - C++: 46.6 ms
  - Rust: 8.6 ms
  - Ratio: 0.18× (Rust ~5× faster)
- Skips/blockers/follow-ups:
  - C++ LEL treats floating-point literals (e.g. `2.0`) as Double; the C++ open
    wrapper handles automatic Float↔Double promotion.

## Lessons learned

- C++ LEL lexer rule ordering: DOUBLE pattern appears before FLOAT in
  `ImageExprGram.ll`, so ambiguous literals like `2.0` are lexed as Double.
  This means `Float_image * 2.0` yields a Double expression in C++. The C++
  test wrapper must handle both Float and Double `ImageExpr` types.
- Self-referential struct problem: `OwnedImageExpr` can't hold source images
  and a borrowing `ImageExpr<'a, T>` in the same struct. Solved by re-parsing
  the expression on each evaluation call — parsing takes microseconds so
  overhead is negligible (confirmed by 0.18× perf ratio).
- `serde_json` standard JSON output is fully compatible with C++ casacore's
  `JsonParser` (flex/bison-based); no format adjustments needed.

## Post-closeout fixes

Two bugs were identified and fixed after initial closeout:

1. **`is_image_reference` too restrictive** — The former `looks_like_image_path()`
   function required bare identifiers to contain `.`, `/`, or `~` to be recognized
   as image references. This meant bare names like `my_image` were not detected,
   causing `extract_image_names()` to miss them and `open()` to fail. Fix: renamed
   to `is_image_reference()` and changed to accept any identifier that is not a
   known function name, constant, or numeric literal.

2. **`OwnedImageExpr` cannot handle nested `.imgexpr` or type mismatches** —
   `open()` only opened `PagedImage<T>` sources, failing when a source was itself
   an `.imgexpr` directory or when pixel types didn't match (e.g. f64 expression
   referencing f32 sources). Fix: introduced `SourceImage<T>` enum with four
   variants (`Native`, `ConvertedF32`, `ConvertedF64`, `Expr`) that handles type
   conversion via `ExprValueConvert` without unsafe code. Added `open_source<T>()`
   helper and implemented `Lattice<T>` + `ImageInterface<T>` for `SourceImage<T>`.
   `OwnedImageExpr<T>` now stores `Vec<SourceImage<T>>` and carries its own
   `coords`, `units`, and `image_info` derived from the first source image.

   Regression tests added: `open_nested_imgexpr`, `open_bare_ident_round_trip`,
   `open_f64_expr_over_f32_source`.

- Date: 2026-03-06
- Files changed: `expr_file.rs`, `image_expr.rs`
