# MSv2/MSv4 trial: review brief

Status: revision 3 reviewed (Astra, Fable; 2026-10-25, at `a6852cc`); revision 4 answers both reviews and is open for re-review
Truth class: review instructions
Date: 2026-10-25

This brief is self-contained: everything it refers to is on GitHub in the
public repository `bglenden/casa-rs`. The trial is draft pull request
[bglenden/casa-rs#898](https://github.com/bglenden/casa-rs/pull/898) from
branch `claude/ms-v4-contract-trial` into the design branch
`codex/ms-v4-architecture-plan` (trial base
`ae2060162a0eb58e527f057543447f370c25567c`). Review the branch head at the
time you start and cite the SHA you reviewed. Revision 3 (`a6852cc`) had a
design review by Astra and an independent code review by Fable, both on the
PR; revision 4 answers every finding (README, "Revision 4"), and a re-review
should check those answers first: each finding's thread on the PR names the
change and the law that tests it.

```text
git clone https://github.com/bglenden/casa-rs && cd casa-rs
git checkout claude/ms-v4-contract-trial
```

## The question

Is the shared observation boundary ready to freeze as the contract that M2
moves into production, and is the code clean, correct and free of gross
performance or memory problems? The owner's governing principle: **MSv4 is
the future; the interface reflects its native capabilities, and MSv2 is
assembled into the same structures.** The owner has also asked that the
design be clean rather than merely working, and that no performance be
lost.

## Read first, in this order

1. The design record, `docs/measurement-set/trial/revision-2-design.md`:
   the principle, decisions D1 to D25 (D1–D15 the owner's, D16–D25
   delegated judgements), "What the implementation did with each decision"
   and the "Revision 3" and "Revision 4 refinements".
2. The evidence index, `docs/measurement-set/trial/README.md`: the
   "Revision 4", "Revision 3", "Revision 2" and "Revision 1" tables, "What
   revision 2 taught", the scale check on real MeasurementSets, both
   examples, the interoperability results, and "Keep, change or discard".
3. The design: `docs/measurement-set/contracts.md` (revision 7: the current
   read contract in sections 1–7 and the provisional capabilities in
   section 8) and `docs/measurement-set/architecture.md` (its revision 7
   note and section 3).
4. The ADR-0016 amendment on `main` (`1eed6e9`, ported to the branch as
   `2fd4f06`): `docs/adr/0016-imaging-foundation.md` decision 10, which lets
   every imaging crate depend on the definitional crates `casa-values`,
   `casa-types` and `casa-coordinates`, with its checker in
   `scripts/check-imaging-dependencies.py`.

## Code map

- `crates/casa-observation/src/`: the boundary. `lib.rs` (overview and
  example), `catalogue.rs` (partitions by content, `bind_to`), `request.rs`
  (`AccessRequest`, `CorrelationRequest`, `Estimate`, the byte budget and
  decode scratch), `batch.rs` (`VisibilityBatch`, `Validity`, windows),
  `buffers.rs` (the one checked fill, packing), `pool.rs` (`BufferPool`,
  `FilledBatch`), `cursor.rs` (`Observation`, `Traversal`,
  `VisibilityCursor`), `grouping.rs` (`GroupKey`, fragments,
  `GroupCompletion` and `WorkToken`), `creation.rs` (`TimeBaselineGrid`).
- `crates/casa-types/src/correlation.rs` (the 32 casacore `Stokes` types
  under MSv4's labels) and `crates/casa-types/src/measures/uvw.rs` (`MUvw`).
- `crates/casa-ms/src/observation_io/`: `mod.rs` (opening and dispatch),
  `grouping.rs`, `msv2/` (`index.rs` the chunked opening index,
  `catalogue.rs`, `cursor.rs`, `mod.rs`), `msv4/` (`read.rs`, `cursor.rs`,
  `metadata.rs`, `write/mod.rs`, `write/payload.rs`).
- Example A (imaging): `crates/casa-imaging-application/tests/ms_trial_imaging.rs`
  and `tests/ms_trial/{source,work_units}.rs`.
- Example B (calibration): `crates/casa-calibration/src/solve/batch_trial.rs`
  and `batch_trial/feed.rs`.
- Interoperability: `crates/casa-ms/examples/ms_trial_interop.rs` and
  `tools/ms-trial/` (pinned XRADIO environment, `uv.lock`). Scale probe:
  `crates/casa-ms/examples/ms_trial_probe.rs`.
- Adapter laws: `crates/casa-ms/tests/observation_io.rs` and
  `tests/observation_io/{refusals,work,writer}.rs`. Fixtures (a simulated
  MSv2 and XRADIO's conversion of it) are committed under
  `crates/casa-ms/tests/fixtures/ms_trial/`.

## How to run

Rust 1.99 or newer, `CARGO_INCREMENTAL=0`; nothing below needs data outside
the repository except where noted.

| What | Command | Expected at revision 4 |
| --- | --- | --- |
| Boundary unit tests and doctest | `cargo test -p casa-observation` | 12 + 1 pass |
| Adapter laws, refusals, writer, work units | `cargo test -p casa-ms --features msv4 --test observation_io` | 28 pass (and the `casa-ms` library: 431) |
| Example A | `cargo test -p casa-imaging-application --test ms_trial_imaging -- --nocapture --test-threads 1` | 5 pass; planes bit-identical to the production imager |
| Example B | `cargo test -p casa-calibration --lib batch_trial -- --nocapture --test-threads 1` | 5 pass; gains identical to production `gaincal` |
| Interoperability, both directions | `tools/ms-trial/check_interop.sh` (needs `uv`) | 26/26 and 74/74; committed reports unchanged |
| Imaging T0 and T1 (the shared `Correlation` touched them) | `cargo nextest run -p casa-imaging-operator -p casa-imaging-runtime` and `cargo nextest run -p casars-imager --test t1` | pass |
| Gates | `just arch-check`, `just docs-check`, clippy `-D warnings` on the touched crates with `--all-targets` and `--features casa-ms/msv4` | clean |
| Scale probe (needs a real MeasurementSet) | `cargo run --release -p casa-ms --example ms_trial_probe -- STORE [--open-only]` under `/usr/bin/time -l` | README "Scale check" |

## What each reviewer is asked

### Astra: the design

You wrote the MS design and gave the trial's first review. Judge whether the
trial realizes the design the owner wants to freeze:

1. Does the boundary take MSv4's native shape everywhere, with MSv2 only
   assembled into it? Name any place where MSv2 concepts leak into the
   interface or an MSv4 capability is lost.
2. Is `contracts.md` a faithful single account of decisions D1–D25 and
   revisions 3 and 4, and is its provisional table complete, each with the
   right acceptance obligation? What else is stale, wrong, or should be
   removed?
3. Which contracts are ready to freeze for M2, which need changes first, and
   which should stay explicitly provisional (updates, creation from batches
   for MSv2, bitmask flags, revisits)?
4. Is the M2 path clear: imaging reading batches directly (dropping the
   runtime's `NativeBlock` copy), the buffer pool joining `HostResources`
   admission, `casa-observation`'s place in the ADR-0016 dependency policy,
   and the model's remaining private measure vocabulary (`DirectionFrame`,
   `FrequencyFrame`, `Epoch`)?

### Fable: independent code review

Review the code as if you had not seen any of the trial's reasoning; the
author's own assessment is deliberately not given here. `AGENTS.md` requires
this independent review before science, persistence and interoperability
changes are marked ready.

1. **Correctness.** Bugs, unsound invariants, panics reachable from data
   (`expect`, indexing), concurrency hazards in `BufferPool` and
   `GroupCompletion`, error paths that leak or lose a buffer, and refusals
   that should exist but do not.
2. **Clean design.** Anything janky: awkward protocols, duplicated
   vocabulary, two ways to do one thing, types that do not carry the
   invariants they rely on, functions or modules that should be split or
   merged. Compare with `AGENTS.md` ("Clarity follows the mathematics",
   prefer deleting and consolidating).
3. **Performance and memory.** Whether the evidence supports "no gross
   performance problem" and "memory bounded by the budget": per-element
   costs in hot paths (the validity rule, gathers, padding placement),
   copies, the opening index, decode scratch, `zarrs` concurrency and its
   internal codec scratch. For any doubt, propose the cheapest measurement
   that would settle it.
4. **Science and interoperability.** Measures and units at the boundary
   (time scales, UVW frames, frequencies), flag and weight semantics
   (padding, `FLAG_ROW`, broadcast weights, subset flag policy), and whether
   the MSv4 writer's output is what XRADIO and casacore expect.

## Known open items

These are recorded already and are not new findings, though a reviewer may
argue any of them should block the freeze:

- The MSv2 grid uses exact `TIME` values; XRADIO merges times within a
  quarter of the smallest step. Jittered times would give more padding.
- Ephemeris is not a content key of a partition.
- MSv2 tile-level read amplification is not modelled.
- A correlation subset is decoded whole and gathered (counted in the
  budget since revision 3); a strided in-place decode is not built.
- An MSv2 creator from batches does not exist; the writer is MSv4 only.
- CASA excludes `FLAG_ROW` rows in `gaincal` and `tclean` by its sources
  (as the adapter does); neither CASA nor production casa-rs `gaincal`,
  which does not, has been instrumented on such a row.
- The buffer pool is bounded in bytes but not yet connected to the
  runtime's admission, and `zarrs`' internal codec scratch is not counted.
- Unaligned MSv4 writes are counted (`partial_chunks`), not measured, and no
  caller aligns its batches yet.
- The batch read was not compared with the production reader on the same
  large data; the trial's only like-for-like timings are on a 120-sample
  fixture.

## How to report

- Post a GitHub review on
  [bglenden/casa-rs#898](https://github.com/bglenden/casa-rs/pull/898):
  inline comments on the lines concerned, and a summary comment.
- Label each finding **blocking**, **should fix** or **nit**, with the
  file and line, the concrete failure or cost (inputs or state and what goes
  wrong), and a suggested fix.
- State the SHA you reviewed.
- Do not push to the branch; the trial's author addresses each finding and
  answers on its thread.
- End the summary with a verdict: ready to freeze for M2; ready after the
  listed fixes; or not ready, and why.
