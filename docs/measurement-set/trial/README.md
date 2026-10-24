# MSv2/MSv4 trial evidence

Status: exploratory first pass in progress; interfaces not frozen; not M0/M1 acceptance
Truth class: trial evidence
Date: 2026-10-24
Handoff: [trial-handoff.md](../trial-handoff.md)

This is the evidence index for the trial's first assignment: two worked
examples (A, dirty-image channel planes; B, a gain solve) and the early
bidirectional XRADIO check. It records what ran, with what, and what it
showed, including what is missing. Nothing here claims M0 or M1 complete:
the full caller and Measures inventory, every Measures family's bulk route,
general updates and creation, failure injection and the benchmark matrix
remain later requirements.

## Provenance

| Item | Value |
| --- | --- |
| Trial branch | `claude/ms-v4-contract-trial` |
| Starting SHA | `ae2060162a0eb58e527f057543447f370c25567c` (design branch head) |
| Host | Claude Code cloud container: Linux 6.18 x86_64, 4 vCPU Intel Xeon @ 2.10 GHz, 15 GiB |
| Toolchain | rustc 1.99.1 (58cf8f9c3 2026-10-13), cargo 1.99.1 |
| FFTW | Ubuntu `libfftw3-dev` 3.3.10-1ubuntu3 through pkg-config (not the SIMD release build) |
| Rust Zarr stack | `zarrs =0.23.14` with features `filesystem`, `blosc`, `zstd` (no default features); `zarrs_filesystem 0.3.14`, `zarrs_codec 0.2.1`, `blosc-src 0.3.8` (c-blosc with lz4), `zstd-sys 2.1.2+zstd.1.5.7`, `unsafe_cell_slice =0.2.2`. Exact versions are in `Cargo.lock`. Optional, behind `casa-ms`'s `msv4` feature. |
| Oracle environment | XRADIO at commit `3ed765867515095d852eb68c073165c1f6c58761` (v1.3.0 plus 7 commits; package version 1.3.1, schema 4.0.1), zarr 3.4.1, numcodecs 0.17.1, xarray 2026.9.0, numpy 2.5.4, arcae 0.5.8, dask 2026.8.0, toolviper 0.1.12, CPython 3.13.17; the full lock is [`tools/ms-trial/uv.lock`](../../../tools/ms-trial/uv.lock). Python is used only to build fixtures and as an oracle. |
| Workers | One unless a row says otherwise; the work-unit probe uses one thread per channel |
| Cache state | Fixtures unpacked to a fresh temporary directory per test; the page cache is warm after the first read. Timings are diagnostics, not performance acceptance. |

## Fixtures

Both fixtures describe the same observation and live in
[`crates/casa-ms/tests/fixtures/ms_trial/`](../../../crates/casa-ms/tests/fixtures/ms_trial/)
(53 KB and 43 KB compressed), with
[`manifest.json`](../../../crates/casa-ms/tests/fixtures/ms_trial/manifest.json)
recording the oracle versions, the true gains and every array's shape,
chunks, codecs and fill value as XRADIO wrote them.

- **MSv2 `ms_trial.ms`.** Written by the casa-rs synthetic simulator
  (`crates/casa-ms/examples/ms_trial_fixture.rs`): five VLA-A antennas (ten
  baselines, no autocorrelations), one field, twelve 10 s integrations,
  eight 128 MHz Q-band channels in LSRK and four linear correlations. The sky
  is a 1 Jy point at the phase centre and a 0.1 Jy point 14 × 9 cells away,
  8 % Q and −5 % U so the cross hands carry signal. Then
  `tools/ms-trial/make_fixtures.py` (casacore through arcae) adds seeded
  complex noise (σ = 0.02 Jy per component) and known per-antenna,
  per-receptor gains that change every 60 s (two solution intervals); sets
  `WEIGHT` varying by row and correlation (SIGMA = 1/√WEIGHT) and 62 element
  flags of 3,840 (one channel, single cross hands, a whole correlation, a whole row,
  the last channel of four rows); and writes `UVW`'s MEASINFO reference ITRF.
  `FLAG_ROW` stays false, because XRADIO does not carry it.
- **MSv4 `ms_trial.ps.zarr`.** XRADIO's `convert_msv2_to_processing_set` of
  that MSv2 with `partition_scheme=[]`, `main_chunksize={"time": 5,
  "frequency": 4}` (three time chunks, the last a ragged edge, by two
  frequency chunks), default Blosc-LZ4 for the data variables and Zstandard
  for coordinates, and `with_pointing=False`. XRADIO's own
  `check_datatree` reports no schema issues.

Regenerate both with `tools/ms-trial/make_fixtures.sh` (needs `cargo` and
`uv`). The payload is identical on every run; XRADIO's creation dates in the
metadata are not. Origin and licence: generated for this repository, no
external data.

### Fixture findings

- casa-rs's synthetic writer leaves `UVW`'s MEASINFO without `Ref`; XRADIO
  then writes `frame: null` and fails its own schema. The fixture script
  writes `Ref: ITRF`, as CASA does.
- XRADIO's pointing dataset types `time_pointing` as `time`, which its own
  schema rejects; the fixture omits pointing.
- The synthetic writer's corruption panics for four correlations (it
  indexes antenna gains by correlation, for at most two), so noise and gains
  are applied by the fixture script.
- XRADIO relabels MSv2's `UVW` ITRF as the phase centre's frame (`fk5`),
  following CASA `fixvis`'s note that the label is usually wrong; names
  antennas `NAME_STATION` and fields `NAME_FIELDID`; and declares the
  visibility unit as `"unkown"`.

## Commands

| What | Command | Needs |
| --- | --- | --- |
| Adapter laws | `cargo test -p casa-ms --features msv4 --test observation_io` | Rust only |
| Example A | `cargo test -p casa-imaging-application --test ms_trial_imaging -- --nocapture --test-threads 1` | Rust only |
| Example A timings | `cargo test --release -p casa-imaging-application --test ms_trial_imaging narrow_timings -- --ignored --nocapture` | Rust only |
| Example B | `cargo test -p casa-calibration --lib batch_trial -- --nocapture --test-threads 1` | Rust only |
| Shared types | `cargo test -p casa-observation -p casa-types --lib` | Rust only |
| Fixtures | `tools/ms-trial/make_fixtures.sh` | cargo, uv, network for the first `uv sync` |

## Adapter laws

`crates/casa-ms/tests/observation_io.rs`, 6 tests, all passing:

- Both catalogues describe the same observation in each store's own names
  (antennas, field, phase centre, frequencies and widths, LSRK, correlation
  order, data-group roles and units).
- **Every sample XRADIO wrote equals its MSv2 row exactly**: visibilities,
  flags, weights (MSv2's per-correlation `WEIGHT` broadcast, MSv4's
  spectral `WEIGHT`), UVW and time. This holds for the whole band and for
  channel windows 2–5 and 7, at v4 batch sizes of 10, 20, 50 and 120
  samples and v2 sizes of 1, 7, 25 and 120. The v4 store holds exactly the
  120 v2 acquisitions.
- The MSv2 batch is listed (`Irregular`), with per-correlation weights,
  `FLAG_ROW` and known occupancy. The MSv4 batch is factored
  (`TimeBaseline`, 4 times × 10 baselines), with spectral weights and
  unknown occupancy, and its `[40, 8, 4]` view is the decode buffer itself
  (same pointer).
- MSv4 Unix-second times re-express the MSv2 MJD-second times exactly.
- 60 s groups: each bin's 60 samples arrive once, in order, whatever the batch
  size (10–120). Closure is announced exactly once, either as a `Last`/`Whole`
  fragment or, under deferred closure, as a later `GroupEnd`.
- The binding reports read amplification before reading: 15/12 for the whole
  band (Zarr stores the ragged last time chunk at full size), and twice that
  for a two-channel window of four-channel chunks. The decode count matches.

## Example A: dirty-image channel planes

`crates/casa-imaging-application/tests/ms_trial_imaging.rs`. A four-plane
LSRK cube (native channels 2–5) of 64 × 64 cells of 0.012″, natural
weighting, standard gridder, Stokes I. The production `MeasurementOperator`,
spectral resampler (linear) and major-cycle pass (`run_major_cycle`) read
`VisibilityBatch`es through a trial `BoundedSource`
([`tests/ms_trial/source.rs`](../../../crates/casa-imaging-application/tests/ms_trial/source.rs)).
Each wave binds its own `AccessRequest` for the native channels that reach its
planes. The baseline is the production imager (`casa_imaging_application::execute`,
`niter=0`) on the MSv2.

| Run | Image NRMS vs production | PSF NRMS | `sumwt` | Traversal windows | Decode units |
| --- | --- | --- | --- | --- | --- |
| v2 batches, every plane resident | 0 | 0 | equal | 2–5 | 120 rows |
| v4 batches, every plane resident | 0 | 0 | equal | 2–5 | 35 chunks |
| v2 batches, one-plane waves | 0 | 0 | equal | 2–3, 2–4, 3–5, 4–5 | 480 rows |
| v4 batches, one-plane waves | 0 | 0 | equal | same | 110 chunks |
| either, batch sizes 1–120, 2 workers | 0 vs 120-sample run | 0 | equal | 2–5 | — |

The planes are bit-identical to the production imager's: the same samples
reach the same operator in the same order. A cube in waves on full-height
time chunks decodes each frequency chunk once per wave that reaches it, so
the v4 waves decode 110 chunks against 35 resident. This is the design's
predicted amplification, measured.

### Work units with unequal completion

[`tests/ms_trial/work_units.rs`](../../../crates/casa-imaging-application/tests/ms_trial/work_units.rs):
each of the four channels is a work unit gridding its one-channel window of
every batch (40 samples, three batches) on its own thread. Unit 0 waits 60 ms
before each batch. Every unit's plane equals the pass's (NRMS < 1e-6).

| Strategy | Fast units done | Slow unit done | Producer stall | Batch buffers | Peak pinned | Copied |
| --- | --- | --- | --- | --- | --- | --- |
| Shared batch (`Arc<FilledBatch>`, pool of 2), v2 | 61.1 ms | 181.2 ms | 60.4 ms | 2 × 8.4 KB | 16.9 KB of batches | 0 |
| Packed pieces (`FilledBatch::pack`, 2 per unit), v2 | 0.6 ms | 181.2 ms | 60.2 ms | 1 × 8.4 KB | 8.4 KB batch + 19.6 KB pieces | 7,680 elements |
| Shared batch, v4 | 62.4 ms | 181.7 ms | 59.5 ms | 2 × 9.8 KB | 19.7 KB of batches | 0 |
| Packed pieces, v4 | 2.3 ms | 181.8 ms | 59.3 ms | 1 × 9.8 KB | 9.8 KB batch + 16.0 KB pieces | 7,200 elements |

Release build, one run. The slow unit's 3 × 60 ms delay dominates its time
by design.

Sharing costs no copy, but a slow unit pins every batch it has not finished,
and once the pool is used up the fast units wait for its buffers. Packing
lets the fast units finish while the slow one works. It costs one copy of
each window and leaves the slow unit holding only its own pieces, bounded by
its queue; the batch buffer is refilled at once. In both cases the producer
waits only for storage, never for a scheduler, and every queue is bounded.

### Narrow timings

`cargo test --release -p casa-imaging-application --test ms_trial_imaging narrow_timings -- --ignored --nocapture`,
25 runs each, one worker, warm page cache:

| Path | Median | Minimum |
| --- | --- | --- |
| Production imager, `major cycle 1` phase (its MSv2 source, admission and the pass) | 3.74 ms | 3.23 ms |
| Trial pass reading v2 batches (adapter read 0.10 ms, packing 0.014 ms) | 1.26 ms | 1.00 ms |
| Trial pass reading v4 batches (adapter read 0.62 ms, packing 0.017 ms) | 1.50 ms | 1.33 ms |

The production phase also opens and admits its source, so these are not
matched like for like. They show the batch path adds no visible cost at this
size. On 120 samples the v4 read is per-call overhead: zarrs opens each
subset read and decodes Blosc and Zstandard chunks. No noise band has been
measured; nothing here is a performance claim.

### Copies and allocation in example A

| Path | Store → buffer | Buffer → kernel | Notes |
| --- | --- | --- | --- |
| Production v2 (`MeasurementSetSource`) | typed `[row][channel][correlation]` fill | one copy into `NativeBlock` | — |
| Trial v2 batch | the same typed fills, straight into `BatchBuffers` | one copy into `NativeBlock` (5,760 elements resident) | antennas converted from i32 (240 elements) |
| Trial v4 batch | zarrs decodes each chunk's selection straight into `BatchBuffers` (`retrieve_array_subset_into`) | one copy into `NativeBlock` | flags decoded as bytes, checked and converted to `bool` (1,920 elements); target zeroed before decode |

Batch buffers grow once, on the first fill, and are reused after that; they
grow again only when a later wave asks for a wider window. Each Zarr chunk
decode still allocates inside zarrs.

## Example B: gain solve

[`crates/casa-calibration/src/solve/batch_trial.rs`](../../../crates/casa-calibration/src/solve/batch_trial.rs)
(crate-internal, test-only). The request is `gaincal(gaintype='G',
calmode='ap', solint='60s', refant=0, minsnr=3, minblperant=4)` against a
1 Jy point source. Production's `SolveAccumulator` and solver
(`solve_closed_group`, split out of `solve_gain` unchanged) consume batch
fragments. The `[sample, channel, correlation]` views are transposed to the
accumulator's `[correlation, channel]` without copying. For the comparison,
`solve_gain` was split into `solve_gain_rows` plus the caltable write; the
production run still writes its caltable (10 rows).

To feed both paths, `SolveAccumulator::observe` was split into its MAIN-cell
reads and `observe_samples`, which accumulates arrays from any source. The
arithmetic and its order are unchanged, and all 134 existing casa-calibration
tests pass. The split also brought `observe` under the function-length limit,
so its grandfathered `too_many_lines` exception is removed.

| Realization | Batch sizes | Closure | Gains vs production | SNR / weight vs production | Live groups | Accumulator entries |
| --- | --- | --- | --- | --- | --- | --- |
| Sequential, v2 | 1, 7, 25, 60, 120 rows | at delivery and deferred | identical | ≤ 1.5e-7 relative | ≤ 2 | ≤ 64 |
| Sequential, v4 | 10, 30, 40, 70, 120 samples | at delivery and deferred | identical | ≤ 1.5e-7 | ≤ 2 | ≤ 64 |
| Concurrent fragments, first held back 50 ms, v2 / v4 | 25 / 30 | both | ≤ 1.9e-7 absolute | ≤ 6.1e-6 | 2 | — |
| Production against itself | — | — | identical | ≤ 1.5e-7 | — | — |

- **Groups cross batches.** At 7 rows per batch, each 60-sample group spans
  up to ten batches (19 fragments over 18 batches). At one row per batch, a
  group spans 60 batches, with one 688-byte buffer and one group's
  sufficient statistics (32 entries) resident. Accumulator state depends on
  the group's distinct times, antennas and baseline-correlations, not its
  samples. No visibility is retained across batches.
- **Closure is exact.** Each group closes exactly once: two `Last`/`Whole`
  fragments at delivery, or two `GroupEnd` events under deferred closure.
  In the concurrent realization, both groups' closures arrived while
  fragment work was pending (`closed_with_pending = 2`), and each group was
  solved only after its partial accumulations had joined and merged.
- **Agreement.** The sequential solves match production as closely as
  production matches itself: its solver iterates hash maps in a per-run
  order. Merged partial sums change summation order, hence the 1e-7 gain
  differences in the concurrent run.
- **Science check.** The solutions recover the fixture's known gains,
  referred to antenna 0, to 0.092 rad in phase and 10.7 % in amplitude; the
  point-source model omits the 0.1 Jy offset source.

### Gaps example B exposed

- The batch carries neither `TIME_CENTROID`, `INTERVAL`/`EXPOSURE` nor scan.
  The solve needs all three: the centroid weights the solution time,
  `INTERVAL` gives the solution interval, and scan both keys the group and
  labels the solution row. The trial supplies time as the centroid (equal in
  this fixture), a 10 s interval, and no scan (solution scans are not
  compared). Proposed: the time auxiliaries the contract already lists
  (centroid, interval, exposure) become optional roles, and scan becomes a
  per-time or per-sample coordinate with a group-key rule.
- Antenna identities are the adapters' own; the trial relies on both
  catalogues listing the stations in the same order. A `CalibrationBinding`
  between observation and caltable namespaces (contracts §2) is still
  untested.
- The consumer owns closure bookkeeping: pending work per key, closed keys,
  and joins. `GroupEnd` and `Last` say only that input is complete. A small
  helper owned by calibration (not the source) could own this bookkeeping
  once the contract settles.

## MSv4 interoperability

- **XRADIO writes, Rust reads**: the adapter laws above compare Rust's read
  against the original MSv2. The independent comparison against XRADIO's own
  load has not been run yet.
- **Rust writes, XRADIO reads**: not yet run.

A missing run is not a pass.

## Findings and proposed contract changes (so far)

1. **Owned batches for handoff (added).** `VisibilityBatch` borrows its
   buffers, so it cannot move to another work unit. The trial added
   `FilledBatch` (buffers plus shape, `Send + Sync`), `next_owned`, and
   `FilledBatch::pack` for accounted packing. To make this possible, a cursor
   now implements `fill() -> Fill` and the trait provides `next` and
   `next_owned`. Proposed: keep this split.
2. **End of stream needs a buffer.** `next_owned` must be given buffers even
   to learn that a traversal has ended, so the producer stops by counting the
   realization's batches. Proposed: a cursor `exhausted()`, or
   `next_owned(Option<BatchBuffers>)`.
3. **Correlation routing is part of the consumer's access pattern.** The
   production imager routes every stored correlation, so a flagged cross hand
   drops its sample from Stokes I, as CASA does. Selecting only XX and YY
   changed `sumwt` by 0.86 %. The request should name the correlations the
   consumer reads, and the flag policy over unread ones, rather than leave
   them to glue code.
4. **The kernel-facing block is a second owned copy.** The runtime's
   `NativeBlock` owns its arrays, so every batch is copied into it once, as
   the production source's buffer is today. Proposed for M2: let the pass
   place directly from a batch view (`NativeRow` already borrows slices), and
   delete `NativeBlock`'s copy.
5. **Per-correlation weights broadcast by hand.** Packing a `NativeBlock`
   writes a v2 per-correlation weight once per channel. `WeightView::get`
   hides the broadcast, but a contiguous-slice consumer must still expand it.
6. **UVW frame labels differ by convention.** The v2 batch reports the
   stored ITRF; the v4 batch reports XRADIO's fk5 (J2000). The values agree
   exactly. Proposed: carry the stored label and the effective frame
   separately, or define the effective frame at binding.
7. **Duplicated vocabulary.** `casa-observation` has its own `Correlation`
   because it cannot depend on `casa-imaging-model`'s `CorrelationType`.
   Proposed: move the one definition down to `casa-observation` (or
   `casa-types`).
8. **No native UVW measure.** casa-types has no `MUvw`; `UvwView` uses
   `DirectionRef` for its frame, as casacore's `Muvw` shares the direction
   frames.
9. **Batch size.** A `VisibilityBatch` is about 480 bytes of views, returned
   by value once per batch; this is harmless at batch granularity.
10. **zarrs concurrency.** zarrs' default codec concurrency is the global
    rayon pool. The adapter sets the target to 1, so decoding runs on the
    caller's thread. Whether W4 can drive zarrs from the admitted team is
    untested.

## Unsupported in the trial (typed refusals)

The channel-major layout; `complex128` and narrowing; more than one
partition or data group per traversal; MSv4 time formats other than `unix`
and scales other than UTC/TAI; frames other than fk5/icrs for directions and
UVW; units other than the schema's; groups over traversals that are not in
time order; scan-based group keys; per-cell `WEIGHT_SPECTRUM` fallback;
`SIGMA`, `FLAG_CATEGORY`, row-flag writes; MSv4 sharding (not exercised);
remote stores.
