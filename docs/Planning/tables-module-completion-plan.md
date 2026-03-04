# Tables Module Completion Plan (Executable Handoff for Another Agent)

This file is the implementation plan for finishing `casacore-tables` with casacore-compatible on-disk behavior and full 2x2 Rust/C++ interop verification.

Use this as a strict checklist. Execute in order. Do not skip verification gates.

## 1. Scope and non-negotiables

1. Public API remains Rust-first and table-centric:
   1. Row-wise cell access.
   2. Column-wise access with row range and stride.
   3. Table keywords and column keywords.
2. `table.dat` is internal implementation detail only.
3. Storage manager implementation must match real casacore storage manager semantics.
4. First compatibility target is `StManAipsIO`. Then add `StandardStMan`.
5. Array cells support both fixed-shape and variable-shape semantics.
6. Design for large columns:
   1. No requirement to materialize entire column in memory.
   2. Streaming/chunked read/write APIs are required.
7. Full 2x2 interop verification is required before calling tables "complete."

## 2. Environment and references

1. Repository root: `/Users/brianglendenning/SoftwareProjects/casa-rs`
2. Casacore source: `~/SoftwareProjects/casacore`
3. Must consult casacore code when implementing storage semantics:
   1. `tables/Tables/*` for table descriptor and storage manager behavior.
   2. `tables/DataMan/*` for storage manager formats/semantics.
   3. Existing record column code in `ScaRecordColDesc.*` and `ScaRecordColData.*`.
4. Existing Rust interop harness style lives in:
   1. `crates/casacore-test-support/src/lib.rs`
   2. `crates/casacore-test-support/src/cpp/casacore_cpp_aipsio_shim.cpp`
   3. `crates/casacore-test-support/tests/aipsio_cross_matrix.rs`
5. C++-dependent tests must skip cleanly when `pkg-config casacore` is missing.

## 3. Definition of done (tables module)

All must be true:

1. Rust can read existing casacore tables for:
   1. Scalars.
   2. Fixed-shape arrays.
   3. Variable-shape arrays.
   4. Scalar record columns.
   5. Table keywords.
   6. Column keywords.
2. Rust can write tables that casacore can read with matching semantics for the same feature set.
3. Public APIs provide row-wise and column-wise access, including range+stride, and keyword access.
4. Large-data-safe APIs exist for chunked/streaming column access (no forced whole-column `Vec` materialization).
5. Full 2x2 interop matrix passes for each supported storage manager.
6. Quality gates pass:
   1. `cargo fmt --all -- --check`
   2. `cargo clippy --workspace --all-targets -- -D warnings`
   3. `cargo test --workspace`
   4. `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`

## 4. 2x2 interop verification contract

For each fixture and each storage manager:

| Producer | Consumer | Label |
|---|---|---|
| Rust | Rust | RR |
| Rust | C++ | RC |
| C++ | Rust | CR |
| C++ | C++ | CC (control baseline) |

Required result:

1. `RR` validates Rust internal consistency.
2. `RC` validates Rust writer compatibility.
3. `CR` validates Rust reader compatibility.
4. `CC` ensures C++ fixture/harness itself is valid (control).

## 5. Interop fixture set (must all be covered)

Implement these fixture families. Keep each fixture small but semantically rich.

1. Scalar primitives:
   1. `Bool`, signed ints, unsigned ints, float, complex, string.
2. Fixed-shape array column:
   1. Same shape in all rows.
3. Variable-shape array column:
   1. Different shapes across rows.
4. Scalar record column:
   1. Non-empty record cells.
   2. Missing/empty behavior per schema semantics.
5. Undefined scalar cell behavior:
   1. Column with `undefined = true`.
6. Keywords:
   1. Table keywords.
   2. Column keywords.
7. Mixed schema fixture:
   1. Scalar + fixed array + variable array + record + keywords.
8. Row range + stride fixture:
   1. Verify ranged column reads/writes produce expected rows only.

## 6. Work plan by milestone

## Milestone 0: Baseline and branch hygiene

1. Create branch: `codex/tables-completion-plan`.
2. Capture baseline command output in a local note:
   1. `cargo test --workspace`
3. Do not revert unrelated existing changes in the repo.

Exit criteria:

1. Branch exists.
2. Baseline builds/tests known.

## Milestone 1: Build a reusable table interop harness in `casacore-test-support`

Files to create:

1. `crates/casacore-test-support/src/table_interop.rs`
2. `crates/casacore-test-support/tests/tables_cross_matrix_stman_aipsio.rs`
3. `crates/casacore-test-support/tests/tables_cross_matrix_standard_stman.rs`

Files to modify:

1. `crates/casacore-test-support/src/lib.rs` (export helper APIs)
2. `crates/casacore-test-support/build.rs` (compile new C++ table shim)

Implementation tasks:

1. Define Rust fixture model structs:
   1. Schema definition.
   2. Row values.
   3. Table keywords.
   4. Column keywords.
2. Define helper functions for matrix execution:
   1. `run_tables_cross_matrix_for_fixture(...)`.
   2. `cpp_backend_available()` reuse pattern.
3. Define shared assertion routine:
   1. Compare schema.
   2. Compare row count.
   3. Compare per-cell values.
   4. Compare shapes.
   5. Compare definedness.
   6. Compare table and column keywords.
4. Add skip logic identical in behavior to existing AipsIO cross-matrix tests.

Exit criteria:

1. Harness compiles.
2. New test file exists and can skip cleanly when C++ unavailable.

## Milestone 2: Add C++ table shim functions for fixture write/read/verify

Files to create:

1. `crates/casacore-test-support/src/cpp/casacore_cpp_table_shim.cpp`

Files to modify:

1. `crates/casacore-test-support/build.rs` (compile + link the new shim)
2. `crates/casacore-test-support/src/lib.rs` (FFI declarations + safe wrappers)

C++ shim design requirements:

1. Provide C ABI entrypoints for each fixture family:
   1. `cpp_write_fixture_<name>(path, manager_kind, out_error)`.
   2. `cpp_verify_fixture_<name>(path, out_error)`.
2. Keep fixture data deterministic and hardcoded in C++ and Rust to avoid parser complexity.
3. For verify functions, validate full semantic expectations and return rich error string.
4. Free error strings similarly to existing shim conventions.

Interop manager enum:

1. `StManAipsIO`
2. `StandardStMan`

Exit criteria:

1. C++ shim builds when `pkg-config casacore` is present.
2. Rust wrappers can call write/verify for at least one fixture.

## Milestone 3: Storage manager architecture cleanup in `casacore-tables`

Current note:

1. `crates/casacore-tables/src/storage.rs` currently uses a custom simplified format.
2. That format is not sufficient for reading arbitrary existing casacore tables.

Files to create or split (recommended):

1. `crates/casacore-tables/src/storage/mod.rs`
2. `crates/casacore-tables/src/storage/manager_registry.rs`
3. `crates/casacore-tables/src/storage/table_control.rs`
4. `crates/casacore-tables/src/storage/stman_aipsio.rs`
5. `crates/casacore-tables/src/storage/standard_stman.rs` (stub first)

Public API rule:

1. No new public API exposing `table.dat`.
2. Keep descriptor/manager parsing crate-internal.

Implementation tasks:

1. Parse table-level metadata and manager bindings internally.
2. Build manager registry: choose concrete manager implementation by on-disk metadata.
3. Route row/column read operations through the selected manager implementation.

Exit criteria:

1. `Table::open` no longer depends on custom ad hoc table format.
2. Manager dispatch is internal and tested.

## Milestone 4: `StManAipsIO` read-path parity first

Files to modify:

1. `crates/casacore-tables/src/table.rs`
2. `crates/casacore-tables/src/table_impl.rs`
3. `crates/casacore-tables/src/storage/stman_aipsio.rs` (or `storage.rs` if not split yet)

Implementation tasks:

1. Implement reader mapping for all milestone fixture families.
2. Ensure scalar record column semantics match casacore:
   1. Definedness semantics.
   2. Missing/empty behavior under schema.
3. Read table keywords and column keywords from casacore-compatible metadata.
4. Preserve row index ordering and shape fidelity.

Verification tasks:

1. Add CR tests (C++ write, Rust read) for each fixture under `StManAipsIO`.
2. Add CC control checks for the same fixtures.

Exit criteria:

1. `CR` and `CC` pass for `StManAipsIO` fixtures.

## Milestone 5: `StManAipsIO` write-path parity

Implementation tasks:

1. Write table metadata and manager bindings in casacore-compatible way.
2. Write cell payloads in casacore-compatible encoding.
3. Write table and column keywords compatibly.
4. Ensure re-open in Rust preserves semantics exactly.

Verification tasks:

1. Add RC tests (Rust write, C++ verify) for each fixture.
2. RR tests for each fixture (Rust write, Rust read).

Exit criteria:

1. `RR`, `RC`, `CR`, and `CC` all pass for `StManAipsIO`.

## Milestone 6: Large-data-safe column APIs

Files to modify:

1. `crates/casacore-tables/src/table.rs`
2. `crates/casacore-tables/src/lib.rs`

Required API additions:

1. Column range readers that fill caller-provided buffers (no forced allocation).
2. Chunk iterators for scalar columns:
   1. Example shape: `iter_column_chunks(column, row_range, chunk_rows)`.
3. Array cell read-into methods:
   1. Full cell into `ndarray` view.
   2. Slice read with start/stop/stride semantics.
4. Matching write-from-iterator/write-from-view forms.

Rules:

1. Keep convenience allocation methods optional and documented as materializing.
2. Keep streaming APIs first-class.

Verification tasks:

1. Add tests proving no whole-column allocation is required for multi-row updates.
2. Add stride/range behavior tests on scalar and array columns.

Exit criteria:

1. APIs exist and tests cover chunk/range/slice semantics.

## Milestone 7: Add `StandardStMan` support

Implementation tasks:

1. Implement manager selection for `StandardStMan` in registry.
2. Implement read path first for existing casacore tables.
3. Implement write path after read path is stable.

Verification tasks:

1. Repeat full fixture suite under `StandardStMan`.
2. Run full matrix `RR`, `RC`, `CR`, `CC` for `StandardStMan`.

Exit criteria:

1. Full matrix passes for both `StManAipsIO` and `StandardStMan`.

## Milestone 8: Documentation and demos

Files to modify:

1. `README.md`
2. `crates/casacore-tables/src/lib.rs` docs
3. `crates/casacore-tables/src/table.rs` docs

Tasks:

1. Document supported storage managers and guarantees.
2. Document large-data-safe APIs and expected memory behavior.
3. Add example(s) showing:
   1. Row-wise access.
   2. Column range/stride access.
   3. Keyword access.
   4. Chunked reads.

Exit criteria:

1. `cargo doc --workspace --no-deps` works and examples are accurate.

## 7. Interop test layout and exact naming (required)

Create these integration tests:

1. `crates/casacore-test-support/tests/tables_cross_matrix_stman_aipsio.rs`
2. `crates/casacore-test-support/tests/tables_cross_matrix_standard_stman.rs`

Each test file must:

1. Skip cleanly if C++ backend unavailable.
2. Run all fixtures.
3. Execute all four matrix cells.
4. Print clear failure messages identifying:
   1. Fixture.
   2. Manager.
   3. Matrix cell (`RR`/`RC`/`CR`/`CC`).

## 8. Minimal command checklist after each milestone

Run:

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets -- -D warnings`
3. `cargo test --workspace`

At final milestone only:

1. `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`

## 9. Agent execution rules (important for less capable agent)

1. Do not invent storage formats when casacore behavior exists.
2. Read casacore source before implementing each manager behavior.
3. Prefer small PRs by milestone.
4. Add failing tests first when feasible, then implementation, then pass.
5. If C++ interop is unavailable locally, ensure tests skip and continue implementing RR path.
6. Never expose internal metadata files as public API.
7. Keep API docs in source using Rust doc comments.

## 10. Suggested milestone-to-PR map

1. PR1: Milestones 1 and 2 harness/shim skeleton.
2. PR2: Milestones 3 and 4 (`StManAipsIO` read path + CR/CC).
3. PR3: Milestone 5 (`StManAipsIO` write path + RR/RC).
4. PR4: Milestone 6 large-data APIs.
5. PR5: Milestone 7 `StandardStMan` full matrix.
6. PR6: Milestone 8 docs and final quality gate pass.

## 11. Final signoff checklist

Check every box before merge:

1. [ ] Full fixture suite passes for `StManAipsIO` in RR/RC/CR/CC.
2. [ ] Full fixture suite passes for `StandardStMan` in RR/RC/CR/CC.
3. [ ] Rust reads existing casacore tables (not only Rust-written tables).
4. [ ] Rust writes tables that casacore reads without semantic drift.
5. [ ] Row, column-range/stride, and keyword APIs are documented.
6. [ ] Streaming/chunked APIs exist for large columns.
7. [ ] All workspace quality gates pass.
