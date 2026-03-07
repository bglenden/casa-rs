# Phase 5 Implementation Rules

**Required first step:** read `/AGENTS.md`. It is mandatory and overrides this
file when rules conflict.

## Purpose

Phase 5 now has two roles:

- Waves 1-10 are the closed historical delivery record for the original
  lattice/coordinate/image implementation track.
- Waves 11a-15 are the active parity-correction track for lazy `ImageExpr`,
  real `TempImage`, remaining LEL capability parity, and the final
  module-wide lattices/coordinates/images audit and closeout.

Phase 5 parity is not considered complete until Waves 11a-15 are closed.

## Closed historical record

Waves 1-10 are archived and must not be reopened for new implementation work.
Their filenames and top-of-file banners are the source of truth for that
status. Use them only for background context and lessons learned.

## Current wave track

Only these waves are active planning targets:

1. Wave 11a - Lazy `ImageExpr` design, core implementation, and representative
   parity tests
2. Wave 11b - Lazy `ImageExpr` operator matrix fill-out and exhaustive test
   expansion
3. Wave 11c - Lazy `ImageExpr` closeout checks and downstream contract freeze
4. Wave 12a - `ImageExpr` parser grammar subset and entrypoints
5. Wave 12b - `ImageExpr` expression files, nested references, and
   cross-language persistence
6. Wave 13 - Real `TempImage` and images overview/API cleanup
7. Wave 14 - Remaining LEL capability parity implementation and interop
   closeout
8. Wave 15 - Final lattices, coordinates, and images parity audit,
   documentation, and performance closeout

## Status source of truth

Wave file names are the canonical status marker:

- `Wave N - <name>.md` => planned
- `Wave N - <name> - TAKEN.md` => in progress
- `Wave N - <name> - FINISHED.md` => closed
- `Wave N<suffix> - <name>.md` => planned split subwave such as `11a`
- `Wave N<suffix> - <name> - TAKEN.md` => in progress split subwave
- `Wave N<suffix> - <name> - FINISHED.md` => closed split subwave

For Phase 5 specifically:

- Waves 1-10 are already closed and must remain `- FINISHED`.
- New work must be planned only in Waves 11a-15 or later.

## Wave lifecycle

1. Choose one active wave or create the next active wave from
   `WAVE_TEMPLATE.md`.
2. If the wave comes from backlog work, copy detail from `BACKLOG.md` and
   remove moved item(s) from backlog.
3. Pass the wave's Definition of Ready checklist before coding.
4. On implementation start, append ` - TAKEN` to the filename.
5. At closeout, fill `Results` and `Lessons learned`, then append
   ` - FINISHED`.

## Wave sizing rules

- Target work that touches 3-10 source files plus tests/support unless a
  parity-closeout wave requires a larger test/support footprint.
- Keep core execution-model decisions upstream of docs/shims/test expansion.
- Split a wave into lettered subwaves when an upstream design pass can safely
  constrain a downstream implementation fill-out pass.
- Keep explicit `Non-goals` in every wave file.
- Prefer idiomatic Rust APIs over direct C++ class mirroring.

## Required tests per wave

- 2x2 interop matrix where on-disk metadata/bytes are touched: RR, RC, CR, CC.
- Endian matrix when binary serialization is touched.
- Iterator traversal tests: full, strided, tiled/chunked, region/mask-aware.
- Edge cases: empty lattices/images, singleton axes, degenerate regions,
  undefined/masked cells.
- C++-dependent tests must skip cleanly when `pkg-config casacore` is missing.
- Data-dependent coordinate tests must skip cleanly when measures tables are
  unavailable.
- For Waves 11a-15, add C++ cross-checks for every externally visible behavior
  touched except history.

## Performance rule

- If iteration, caching, paging, chunk traversal, or lazy expression execution
  paths change, run release-mode smoke benchmarks.
- Report iterator throughput/latency deltas alongside correctness results.
- Flag and document follow-up when Rust is slower than 2x C++.

## Closeout gates (canonical)

Run all before `-FINISHED`:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`

## Required evidence in wave `Results`

- Date and commit SHA.
- Commands run with pass/fail summary.
- Interop matrix outcomes.
- Iterator correctness matrix outcomes (where applicable).
- Benchmark numbers + Rust/C++ ratio (if performance applies).
- Data-table availability/skips, blockers, and follow-up items.
