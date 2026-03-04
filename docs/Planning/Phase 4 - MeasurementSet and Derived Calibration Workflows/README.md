# Phase 4 Implementation Rules

**Required first step:** read `/AGENTS.md`. It is mandatory and overrides this
file when rules conflict.

## Purpose

Deliver practical MeasurementSet APIs and workflows on top of Phase 1-3 table,
quanta, measures, and coordinate foundations.

## Status source of truth

Wave file names are the only status source of truth:
- `Wave N - <name>.md` => planned
- `Wave N - <name> - TAKEN.md` => in progress
- `Wave N - <name> - FINISHED.md` => closed

## Wave lifecycle

1. Choose one backlog item (or one tightly coupled pair).
2. Create `Wave N - <name>.md` from `WAVE_TEMPLATE.md`.
3. Copy detail from `BACKLOG.md`, then remove moved item(s) from backlog.
4. Pass the wave's Definition of Ready checklist before coding.
5. On implementation start, append ` - TAKEN` to the filename.
6. At closeout, fill `Results` and `Lessons learned`, then append ` - FINISHED`.

## Wave sizing rules

- Target work that touches 3-8 source files plus tests/support.
- Split schema, storage, selection, and derived-calculation concerns.
- Keep explicit `Non-goals` in every wave file.
- Prefer idiomatic Rust wrappers over direct C++ class mirroring.

## Required tests per wave

- 2x2 interop matrix where on-disk metadata/bytes are touched: RR, RC, CR, CC.
- Endian matrix when binary serialization is touched.
- Edge cases: undefined cells, empty subtables, variable-shape arrays, missing
  required columns/keywords.
- C++-dependent tests must skip cleanly when `pkg-config casacore` is missing.
- Data-dependent tests must skip cleanly when measures tables are unavailable.

## Performance rule

- If row/column scans, selection, or array I/O paths change, run release-mode
  smoke benchmarks.
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
- Benchmark numbers + Rust/C++ ratio (if performance applies).
- Data/measures-table availability, skips, blockers, and follow-up items.
