# Phase 5 Implementation Rules

**Required first step:** read `/AGENTS.md`. It is mandatory and overrides this
file when rules conflict.

## Purpose

Deliver lattice foundations, full coordinate-system parity, and image APIs in
that order, with iteration as a first-class capability for both lattices and
images.

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

- Target work that touches 3-10 source files plus tests/support.
- Keep iteration and storage concerns split into separate waves.
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

## Performance rule

- If iteration, caching, paging, or chunk traversal paths change, run
  release-mode smoke benchmarks.
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
