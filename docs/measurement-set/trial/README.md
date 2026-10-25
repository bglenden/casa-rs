# MSv2/MSv4 trial evidence

Status: revision 2 (the design walkthrough's decisions implemented), stopped for review again; interfaces not frozen; not M0/M1 acceptance
Truth class: trial evidence
Date: 2026-10-25
Handoff: [trial-handoff.md](../trial-handoff.md)

This is the evidence index for the trial's first assignment: two worked
examples (A, dirty-image channel planes; B, a gain solve) and the early
bidirectional XRADIO check. It records what ran, with what, and what it
showed, including what is missing. Nothing here claims M0 or M1 complete:
the full caller and Measures inventory, every Measures family's bulk route,
general updates and creation, failure injection and the benchmark matrix
remain later requirements.

## Revision 1: what the design review changed

Astra's review of the first pass (PR #898) asked for a smaller, cleaner
boundary and for refusals where the first pass accepted inputs it then
mishandled. Each change below has a test; the sections that follow give the
results.

| Review item | Change | Test |
| --- | --- | --- |
| One shared boundary | `casa_observation::Observation` (catalogue + `bind_read`) is all algorithm code sees; `casa_ms::observation_io::open` chooses the adapter once. The imaging source, its work-unit probe and the calibration feed take `&dyn Observation` and import no store type. | `consumer_modules_import_no_store_type`, `the_feed_imports_no_store_type` |
| Metadata the consumers need | Requestable roles for time centroid, interval, exposure and scan, each in its stored sharing (per sample, per time, or one per batch); a role the store lacks fails binding. The gain solve now takes its solution times, intervals and scans from the batch, and the comparison asserts them. | `time_auxiliaries_are_main_columns`, `missing_metadata_fails_binding` |
| One validity rule | `VisibilityBatch::validity()`: a value is excluded when its stored flag or row flag is set or its value or weight is not finite. Stored flags stay separate. Both consumers use it. | adapter and example tests |
| MSv2 `TIME` reference | Read from `MEASINFO`, kept when fixed (TAI is delivered as TAI), refused when variable or when `TIME_CENTROID` disagrees. | `the_msv2_time_reference_is_read_not_assumed` |
| MSv4 `unix` + `tai` | Refused: astropy's Unix epoch is UTC in every scale. The writer writes `unix`/`utc` only. The oracle compares represented instants. | `an_msv4_unix_time_in_tai_is_refused`; interop instant checks |
| UVW convention | Resolved once at opening into `UvwFrame { effective, stored_label }`; the batch carries the effective frame. MSv2 `ITRF`-labelled UVW is the phase centres' frame (CASA's convention), a different celestial label is refused; no consumer or writer relabels. | `catalogues_describe_the_same_observation` |
| Units | MSv2 `DATA` units read from `QuantumUnits`; time, UVW, position, diameter, angle and frequency units checked in both adapters. Catalogue positions are `MPosition`, diameters and angles `Quantity`. | catalogue test |
| Exact MSv4 dtypes and dimensions | `complex64`, `bool`, `float32`, `float64` and the dimension order checked at opening, and again at each decode. | `msv4_payload_types_and_dimensions_are_checked_at_opening` |
| Per-group context | Each data group keeps its own fields (from its `field_and_source` dataset) and UVW frame; fields merge only when name, phase centre and source agree. | `each_data_group_keeps_its_own_field_context` |
| Checked batch construction | `BatchBuffers` arrays are private; a cursor fills them through `begin` and `BatchFill::finish`, which checks lengths (overflow-checked), antenna/field/scan identities and fragment tiling once, and keeps the one `BatchHeader` the batch is built from. Packing goes through the same check. | `casa-observation` unit tests |
| End of stream | `next_owned` hands the buffers back at the end (`OwnedEvent::End`) and on error (`Reclaimed`): no batch count or extra state. | work-unit probe, calibration feed |
| Channel windows and closure | Windows carry no fragments, so a split batch announces a group's end once; `GroupCompletion` joins input completion with work completion. | `split_channel_windows_complete_each_group_once` |
| `Closure::Deferred` | Removed from `AccessRequest`; deferral is an opening option (`OpenOptions::group_closure`), a test mechanism. | grouping laws |
| Creation metadata vs storage | The writer takes the common `Catalogue`, a partition, a data group and a `TimeBaselineGrid`; `Msv4StorageOptions` holds name, provenance, chunks and codec. | writer tests |
| Writer validation | Every batch is checked against the declaration before any byte is written: partition, frequencies and frame, correlation products, units, UVW frame, time scale, and each sample's time, baseline, field, scan and interval. A row flag is refused. | `the_writer_refuses_batches_that_disagree_with_the_declaration` |
| Exclusive staging | Each writer creates its own uniquely named staging directory and publishes with a no-replace rename; a dropped or failed writer removes its staging. | `writers_stage_apart_and_never_replace_a_published_dataset` |
| Comparators | Shape, count and finiteness are checked before any reduction; a NaN is the worst difference; every claimed metric, calibration weights included, is asserted. | `comparators_never_pass_a_nan_or_a_mismatch`, `agreement_never_passes_a_nan_or_a_mismatch` |
| Catalogue rebinding | The calibration comparison maps antennas by station and position and fields by phase centre, refusing unmatched or ambiguous entries, instead of relying on list order. | example B tests |
| `decode_units` wording | Documented as chunks the selection touches, modelled from the chunk grid (the realization and the counter use the same model), not decodes observed inside zarrs. | adapter laws |

## Revision 2: the design walkthrough's decisions, implemented

After revision 1 the owner walked through the design chunk by chunk. The 25
decisions, the principle behind them (MSv4 is the future; MSv2 is assembled
into its structures), the vocabulary (batch shape, traversal order, payload
order; now in `GLOSSARY.md`), the facts verified against the pinned XRADIO
source and the implementation order are recorded in
[revision-2-design.md](revision-2-design.md), whose last section says what
the implementation did with each decision. Revision 2 implemented them all,
with the qualifications listed there (an MSv2 creator from batches was not
built). D18 was completed once ADR-0016 was amended to let imaging depend on
the definitional crates: both adapters, imaging and the simulator share one
`Correlation` of all 32 casacore `Stokes` types. Every open question listed at the end of this
document is resolved. Both examples run unchanged in their science: the
planes and the gains are bit-identical to production from both stores.

What changed at the boundary:

| Decision | Change | Test |
| --- | --- | --- |
| D1 | Per-partition constants (frequencies and frame, correlations, baseline table, time system, data groups with units and UVW frame) live once in `PartitionEntry`; a batch's views borrow them (`buffers.batch(&catalogue)`) and copy nothing; `BatchFill::finish` checks the batch against the catalogue. | `both_stores_deliver_one_batch_shape` (frequency and baseline pointers are the catalogue's) |
| D2, D4 | A partition is a store unit described by content (spectral setup, intents, observing session, feed pair). The MSv2 adapter partitions MAIN rows as XRADIO does (`DATA_DESC_ID` × `OBS_MODE` × `OBSERVATION_ID` × `EPHEMERIS_ID` × feed pair, plus `OpenOptions::partition_scheme`); duplicate (time, baseline) rows and several `PROCESSOR_ID`s are refused at opening. | `catalogues_describe_the_same_observation`, `duplicate_rows_are_refused_at_opening` |
| D3 | One topology: the MSv2 adapter places rows on the time × baseline grid at opening (`msv2/index.rs`); a cell no row fills is padding (flag true, weight 0, NaN values and auxiliaries). `Irregular`, `Occupancy` and the row-order refusal are gone. | `msv2_cells_no_row_fills_are_padding` |
| D5 | `FLAG_ROW` is folded into the flags at fill; `row_flags` is gone. One weight view `[sample, channel, correlation]`: MSv2 `WEIGHT` is seen through a zero channel stride. | `flag_row_is_folded_into_the_flags`, `both_stores_deliver_one_batch_shape` |
| D6, D11 | The request names a partition set; `bind_read` returns a `Traversal` of one `Send` cursor per partition with the estimate. | `the_estimate_is_the_bound_realization_and_models_the_store` |
| D7, D10 | A byte budget (`batch_bytes`) replaces `max_samples`; `Observation::estimate` costs a batch shape from the catalogue and the store's geometry (times, baselines, bytes per sample, whole times per batch, batches, decode units, amplification, natural blocks, padded fraction) without reading payload, and binding returns the same `Estimate`. | same, `the_budget_fits_whole_times_or_refuses_binding` |
| D8 | `NaturalBlocks`: the Zarr chunk shape (5 times × 4 channels) and the casacore tile shape, through a new `Table::array_column_tile_shape`. | same |
| D9, D22 | `Layout` and `Precision` are gone: payload order follows the store; visibilities are `Complex<f32>`. | compile-time |
| D12 | `next_owned(Option<BatchBuffers>)`: without buffers the cursor answers `End`, `GroupEnd` or `NeedBuffers`; nothing is lent at the end. | `the_budget_fits_whole_times_or_refuses_binding`, both examples' producers |
| D13–D15 | `GroupKey` carries the scan; `Grouping::{TimeBins {combine_fields, combine_scans}, PerScan, PerIntegration}` with the clock of each (field, scan) key starting at its first time, as production `gaincal`. | `group_fragments_do_not_depend_on_batch_size`, grouping unit tests |
| D16 | `Validity` stays inline; `mask_into` for multi-pass consumers. | consumers |
| D17 | `CorrelationRequest { deliver, flags_from }`: a subset is delivered in the partition's order with its own flags or the OR over every product. | `a_correlation_subset_carries_the_flags_it_is_asked_for` |
| D18, D21 | `Correlation` (the 32 casacore `Stokes` types under MSv4's labels) and `MUvw` in `casa-types`; the imaging model's `CorrelationType` is gone; `UvwView::get` is an `MUvw`. | casa-types unit tests, imaging T0 and T1 |
| D19, D20 | Measure views are built from the buffers and the catalogue entry; times stay offsets in the store's scale from the partition's origin. | `time_origins_re_express_each_other_exactly` |
| D23, D24 | One declaration (`Catalogue` + `TimeBaselineGrid::from_cursor`); the writer's fill values are the padding convention and its row-flag refusal is gone; a batch's grid is the dataset's, so the writer's scatter path is gone. | writer laws |
| D25 | `Catalogue::bind_to(&other, &BindingTolerance) -> Binding`; example B binds with it. | `catalogues_describe_the_same_observation`, example B |

### What revision 2 taught

- **The grid removes code everywhere it reaches.** Deleting `Irregular`
  removed every topology `match` from the consumers, the writer's scatter
  path and the "rows not in time order" refusal; the MSv2 cursor's only new
  code is the placement of present rows (and padding) when a time is
  incomplete or a correlation subset is delivered. The complete, all-product
  case still reads straight into the batch arrays.
- **Linking to the catalogue removed a class of disagreement.** Two writer
  refusal tests (reversed correlation labels, a shifted frequency in a batch)
  became impossible to write: a batch can no longer carry those values, so
  the writer checks only what a batch states (its times, intervals,
  baseline table) and what the declaration states (correlations, units).
- **A byte budget changes a consumer's bookkeeping.** The imaging source
  sized its maximum block from the full window and the pass's own assertion
  caught the narrower one-plane wave fitting more times; the block is now
  sized from the narrowest window's estimate. The estimate made that a
  one-line fix, which is what D10 is for.
- **Asking before lending** (`next_owned(None)`) removed the extra buffer
  wait at the end of a traversal from both producers; the work-unit probe
  and the concurrent solve ask first and lend only for a batch.
- **The estimate equals the realization by construction**, since binding
  computes it once and the cursors carry it; the test asserts equality
  rather than re-deriving it.
- **Subset gathering costs a copy** on both adapters (MSv4 decodes the full
  correlation axis into scratch; MSv2 reads full cells), counted in
  `copied_elements`. A strided in-place decode per product is the next
  measurement, not a design question.
- **The dependency policy bounded vocabulary unification, until it was
  amended.** `casa-types` owns `Correlation` and `MUvw`, but ADR-0016 let
  `casa-imaging-model` depend on no workspace crate, so the model kept
  `CorrelationType` and private copies of other measure vocabulary. The
  amendment of 2026-10-25 lets every imaging crate use the definitional
  crates; the model now uses `casa_types::Correlation`. Moving the model to
  the shared vocabulary exposed that revision 2's eight-product
  `Correlation` was narrower than MSv4, which allows all 32 `Stokes` types.
  The model's `DirectionFrame`, `FrequencyFrame` and `Epoch` remain its own
  and are the next candidates.

## Provenance

| Item | Value |
| --- | --- |
| Trial branch | `claude/ms-v4-contract-trial` |
| Starting SHA | `ae2060162a0eb58e527f057543447f370c25567c` (design branch head) |
| Host | Claude Code cloud container: Linux 6.18 x86_64, 4 vCPU Intel Xeon @ 2.10 GHz, 15 GiB |
| Toolchain | rustc 1.99.1 (58cf8f9c3 2026-10-13), cargo 1.99.1 |
| FFTW | Ubuntu `libfftw3-dev` 3.3.10-1ubuntu3 through pkg-config (not the SIMD release build) |
| Rust Zarr stack | `zarrs =0.23.14` with features `filesystem`, `blosc`, `zstd` (no default features); `zarrs_filesystem 0.3.14`, `zarrs_codec 0.2.1`, `blosc-src 0.3.8` (c-blosc with lz4), `zstd-sys 2.1.2+zstd.1.5.7`, `unsafe_cell_slice =0.2.2`. Exact versions are in `Cargo.lock`. Optional, behind `casa-ms`'s `msv4` feature. |
| Oracle environment | XRADIO at commit `3ed765867515095d852eb68c073165c1f6c58761` (v1.3.0 plus 7 commits; package version 1.3.1, schema 4.0.1), zarr 3.4.1, numcodecs 0.17.1, xarray 2026.9.0, numpy 2.5.4, arcae 0.5.8, dask 2026.8.0, toolviper 0.1.12, CPython 3.13.17; the full lock is `tools/ms-trial/uv.lock`. Python is used only to build fixtures and as an oracle. |
| Workers | One unless a row says otherwise; the work-unit probe uses one thread per channel |
| Cache state | Fixtures unpacked to a fresh temporary directory per test; the page cache is warm after the first read. Timings are diagnostics, not performance acceptance. |

## Fixtures

Both fixtures describe the same observation and live in
`crates/casa-ms/tests/fixtures/ms_trial/`
(53 KB and 43 KB compressed), with
`crates/casa-ms/tests/fixtures/ms_trial/manifest.json`
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
| Adapter laws, refusals, writer, work units | `cargo test -p casa-ms --features msv4 --test observation_io` | Rust only |
| Example A | `cargo test -p casa-imaging-application --test ms_trial_imaging -- --nocapture --test-threads 1` | Rust only |
| Example A timings | `cargo test --release -p casa-imaging-application --test ms_trial_imaging narrow_timings -- --ignored --nocapture` | Rust only |
| Example B | `cargo test -p casa-calibration --lib batch_trial -- --nocapture --test-threads 1` | Rust only |
| Shared types | `cargo test -p casa-observation -p casa-types` | Rust only |
| Interoperability, both directions | `tools/ms-trial/check_interop.sh` | cargo, uv (pinned XRADIO environment) |
| Fixtures | `tools/ms-trial/make_fixtures.sh` | cargo, uv, network for the first `uv sync` |
| Scale check on a real store | `/usr/bin/time -l cargo run --release -p casa-ms --example ms_trial_probe -- STORE [--open-only]` | Rust only; a MeasurementSet or MSv4 store |

## Adapter laws

`crates/casa-ms/tests/observation_io.rs` and its modules `refusals`,
`writer` and `work`: 22 tests, all passing (the writer laws are described
under interoperability, the refusals in revision 1's table). The shared
types have 6 unit tests of their own (`cargo test -p casa-observation`) and
the adapters' group index 3.

- Both catalogues describe the same observation in each store's own names
  (antennas with equal positions, field, phase centre, scan 1 with its
  intent, one observing session, one partition of the same content:
  frequencies and widths, LSRK, correlation order, intent; data-group roles,
  units and UVW frames: effective J2000 in both, stored `ITRF` in MSv2; the
  MSv2 partition keys its feed pair, the MSv4 one has none), and bind to
  each other by content.
- **Every sample XRADIO wrote equals its MSv2 row exactly**: visibilities,
  flags, weights (MSv2's per-correlation `WEIGHT` seen through every
  channel, MSv4's spectral `WEIGHT`), UVW, time, time centroid, interval,
  exposure and scan. This holds for the whole band and for channel windows
  2–5 and 7, at 1, 2, 5 and 12 whole times per v4 batch and 1, 3 and 5 per
  v2 batch. The v4 store holds exactly the 120 v2 acquisitions.
- The v2 batch's time auxiliaries equal MAIN's `TIME_CENTROID`,
  `INTERVAL`, `EXPOSURE` and `SCAN_NUMBER` as the production typed column
  accessors read them cell by cell, each sample finding its row by time and
  antennas (the grid, not MAIN's row order, orders the batch).
- Both stores deliver one batch shape: 4 times × 10 baselines under a
  four-time budget, field and scan per time, frequencies and baselines
  borrowed from the catalogue, the `[40, 8, 4]` value view the decode
  buffer itself. The stores differ only in sharing and strides: MSv2
  `WEIGHT` has strides `[4, 0, 1]` and its interval is per sample; MSv4
  weights are standard layout and its interval one constant.
- MSv4 Unix-second times re-express the MSv2 MJD-second times exactly.
- 60 s groups keyed by field and scan: each bin's 60 samples arrive once, in
  order, whatever the batch size (1–12 times). Closure is announced exactly
  once, either as a `Last`/`Whole` fragment or, from a store opened to defer
  it, as a later `GroupEnd`.
- A correlation subset (`These([YY, XX])`) is delivered as `[XX, YY]` with
  the values and weights of those products and flags either their own or
  the OR over all four, from both stores; the fixture flags some cross hands
  alone, so the two policies differ on it.
- One time that does not fit the byte budget fails binding; `next_owned`
  without buffers answers `NeedBuffers` for a batch and `End` without one
  at the end.
- An MSv2 row removed from the fixture leaves a padded cell: flagged, NaN
  values, UVW and auxiliaries, weight 0, excluded by the validity rule; the
  other 119 samples are unchanged, the batch count is the grid's and the
  estimate reports the padded fraction (1/120). A duplicated row is refused
  at opening, naming both rows. A row with `FLAG_ROW` set has every value
  flagged in the batch, its values untouched, and writes to MSv4 without
  refusal.
- A grouped batch split into two channel windows on two threads, one held
  back, completes each group's input once and finalizes each group once,
  after both windows' work; per-group sums equal a sequential traversal's,
  from both stores.
- The estimate models read amplification from the chunk grid before
  reading: 15/12 for the whole band (Zarr stores the ragged last time chunk
  at full size), and twice that for a two-channel window of four-channel
  chunks; it reports the natural blocks (5 times × 4 channels for MSv4, the
  tile shape for MSv2), the bytes per sample of the roles (352 for the MSv2
  batch with its per-correlation weight and three per-sample scalars, 456
  for the MSv4 batch) and the padded fraction. Binding returns the same
  estimate, and the cursor's count of touched chunks, by the same model,
  matches.

## Example A: dirty-image channel planes

`crates/casa-imaging-application/tests/ms_trial_imaging.rs`. A four-plane
LSRK cube (native channels 2–5) of 64 × 64 cells of 0.012″, natural
weighting, standard gridder, Stokes I. The production `MeasurementOperator`,
spectral resampler (linear) and major-cycle pass (`run_major_cycle`) read
`VisibilityBatch`es through a trial `BoundedSource`
(`crates/casa-imaging-application/tests/ms_trial/source.rs`), which takes
`&dyn Observation` and imports no store type; the test opens both fixtures
through `casa_ms::observation_io::open`. Each wave binds its own
`AccessRequest` for the native channels that reach its planes. Each cell's
flag is the batch's validity rule. The baseline is the production imager
(`casa_imaging_application::execute`, `niter=0`) on the MSv2. The
comparators check shapes, counts and finiteness first, so a NaN can never
read as agreement.

| Run | Image NRMS vs production | PSF NRMS | `sumwt` | Traversal windows | Decode units |
| --- | --- | --- | --- | --- | --- |
| v2 batches, every plane resident | 0 | 0 | equal | 2–5 | 120 rows |
| v4 batches, every plane resident | 0 | 0 | equal | 2–5 | 35 chunks |
| v2 batches, one-plane waves | 0 | 0 | equal | 2–3, 2–4, 3–5, 4–5 | 480 rows |
| v4 batches, one-plane waves | 0 | 0 | equal | same | 74 chunks |
| either, 1, 3 or 7 times per batch, 2 workers | 0 vs 12-time run | 0 | equal | 2–5 | — |

The planes are bit-identical to the production imager's: the same samples
reach the same operator in the same order. A cube in waves on full-height
time chunks decodes each frequency chunk once per wave that reaches it, so
the v4 waves decode 74 chunks against 35 resident (revision 1 decoded 110:
under a byte budget a narrower wave carries more times per batch, so fewer
batches straddle the five-time chunks). This is the design's predicted
amplification, measured, and the estimate reports it before the wave binds.

### Work units with unequal completion

`crates/casa-imaging-application/tests/ms_trial/work_units.rs`:
each of the four channels is a work unit gridding its one-channel window of
every batch (four times, three batches) on its own thread. Unit 0 waits 60 ms
before each batch. Every unit's plane equals the pass's (NRMS < 1e-6).

| Strategy | Fast units done | Slow unit done | Producer stall | Batch buffers | Peak pinned | Copied |
| --- | --- | --- | --- | --- | --- | --- |
| Shared batch (`Arc<FilledBatch>`, pool of 2), v2 | 61.3 ms | 181.3 ms | 60.5 ms | 2 × 7.4 KB | 14.8 KB of batches | 0 |
| Packed pieces (`FilledBatch::pack`, 2 per unit), v2 | 1.1 ms | 181.3 ms | 60.0 ms | 1 × 7.4 KB | 7.4 KB batch + 15.4 KB pieces | 7,200 elements |
| Shared batch, v4 | 63.4 ms | 182.6 ms | 59.3 ms | 2 × 9.7 KB | 19.4 KB of batches | 0 |
| Packed pieces, v4 | 3.6 ms | 182.4 ms | 58.6 ms | 1 × 9.7 KB | 9.7 KB batch + 12.4 KB pieces | 7,200 elements |

Revision 2, debug build, one run. The slow unit's 3 × 60 ms delay dominates
its time by design. The v2 batch is smaller than in revision 1: it no
longer carries frequencies, correlations, antennas, a group name or row
flags, and its weight is one per correlation (its packed pieces keep that
per-correlation weight, so a v2 piece is a different size from a v4 one
while holding the same values).

The producer asks what comes next before lending a buffer
(`next_owned(None)`), so the shared-batch stall is the one 60 ms wait for
the slow unit's buffer: revision 1's extra wait for a buffer that only
learned the traversal was over is gone, as the open question predicted.

Sharing costs no copy, but a slow unit pins every batch it has not finished,
and once the pool is used up the fast units wait for its buffers. Packing
lets the fast units finish while the slow one works. It costs one copy of
each window and leaves the slow unit holding only its own pieces, bounded by
its queue; the batch buffer is refilled at once. In both cases the producer
waits only for storage, never for a scheduler, and every queue is bounded.

### Narrow timings

`cargo test --release -p casa-imaging-application --test ms_trial_imaging narrow_timings -- --ignored --nocapture`,
25 runs each, one worker, warm page cache. **Measured on the first pass
(`21e0dde`) and not rerun for revision 1**, whose batch construction check
and per-element validity rule may change them slightly:

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

### Scale check on real MeasurementSets

A check for gross costs before the design is finalized, not a benchmark.
`crates/casa-ms/examples/ms_trial_probe.rs` opens a store through the
trial adapter, prints each partition's estimate and reads every partition
once (first data group, every role and product, all channels, 256 MiB
budget). Run on the Mac mini (`bg-mac-mini`: macOS 27.0.1, 8 cores, 24 GB),
release build, one thread, page cache warm, on 2026-10-25, before (`e3f42dc`)
and after (`4046d6c`) the opening index was built in row chunks:

| MeasurementSet | MAIN rows | Partitions | Open, before → after | Peak RSS open only, before → after | Traversal | Peak RSS traversal, before → after |
| --- | --- | --- | --- | --- | --- | --- |
| `pilot.ms` (4.1 GB; 351 baselines, 64 SPWs × 64 channels × 4 products) | 2,021,760 | 64 | 0.37 → 0.24 s | 164 → 30 MB | 4,566 MiB in 1.96 s (2,325 MiB/s) both | 278 → 146 MB |
| `wave3-vla-aw-widefield-medium.ms` (33 GB; 351 baselines, 512 channels × 2 products) | 4,212,000 | 1 | 0.81 → 0.78 s | 313 → 42 MB | not run | — |

- **No gross cost in the read.** One thread delivers 2.3 GB/s of batches
  (about 250 million visibilities a second with their flags, weights, UVW
  and time auxiliaries) against 0.35 s for `cat` of the same cached files:
  the adapter decodes the tiled columns, unpacks `FLAG` bits and places time
  auxiliaries, at about a sixth of a memory copy's speed. With the cache
  cold the same traversal took 16.2 s (282 MiB/s) for the same 1.9 s of
  user time: the external disk sets the pace, not the adapter. No grid was
  padded.
- **Batch memory is what the estimate says.** Each `pilot.ms` partition is
  one 71.3 MiB batch; the traversal's peak is the opening's plus about
  114 MB.
- **The opening index was the one cost that grew with the MeasurementSet:**
  75–80 bytes per MAIN row at its peak, so a 10⁸-row MeasurementSet would
  have needed about 7.5 GB to open, outside any admission. The index now
  reads the key columns 2¹⁸ rows at a time in two passes (`msv2/index.rs`)
  and holds only the grids (four bytes a cell) and one chunk: about 0.45 GB
  for 10⁸ rows. Opening still reads every row's keys, about 0.2 µs a row
  (about 20 s for 10⁸ rows); it became faster here because building a key
  no longer allocates.

The read was not compared with the production reader on the same data;
the narrow timings above are the only like-for-like evidence.

### Copies and allocation in example A

| Path | Store → buffer | Buffer → kernel | Notes |
| --- | --- | --- | --- |
| Production v2 (`MeasurementSetSource`) | typed `[row][channel][correlation]` fill | one copy into `NativeBlock` | — |
| Trial v2 batch | the same typed fills, straight into `BatchBuffers` (a complete time, every product); present rows placed and padded otherwise | one copy into `NativeBlock` (5,760 elements resident) | time auxiliaries placed from scratch (0 copied elements counted for the payload) |
| Trial v4 batch | zarrs decodes each chunk's selection straight into `BatchBuffers` (`retrieve_array_subset_into`) | one copy into `NativeBlock` | flags decoded as bytes, checked and converted to `bool` (1,920 elements); target zeroed before decode |

Batch buffers grow once, on the first fill, and are reused after that; they
grow again only when a later wave asks for a wider window. Each Zarr chunk
decode still allocates inside zarrs.

## Example B: gain solve

`crates/casa-calibration/src/solve/batch_trial.rs` (setup and tests) and
`batch_trial/feed.rs` (the consumer, which takes `&dyn Observation` and
imports no store type); crate-internal, test-only. The request is
`gaincal(gaintype='G', calmode='ap', solint='60s', refant=0, minsnr=3,
minblperant=4)` against a 1 Jy point source. Production's
`SolveAccumulator` and solver (`solve_closed_group`, split out of
`solve_gain` unchanged) consume batch fragments. The `[sample, channel,
correlation]` views are transposed to the accumulator's `[correlation,
channel]` without copying; the flags are the batch's validity rule,
written into one small array per fragment. Each sample's time, time
centroid, interval and scan come from the batch. For the comparison,
`solve_gain` was split into `solve_gain_rows` plus the caltable write; the
production run still writes its caltable (10 rows).

To feed both paths, `SolveAccumulator::observe` was split into its MAIN-cell
reads and `observe_samples`, which accumulates arrays from any source. The
arithmetic and its order are unchanged, and all 134 existing casa-calibration
tests pass. The split also brought `observe` under the function-length limit,
so its grandfathered `too_many_lines` exception is removed.

The comparison binds each solve's antennas and fields to the production
MeasurementSet's with `Catalogue::bind_to` (antenna station and position,
field phase centre, exact), which refuses unmatched or ambiguous entries,
and asserts every row's antenna, field, spectral window, reference antenna,
observation, scan and interval, then the gains, SNRs, weights and flags,
with NaN as the worst difference. The request names every partition of the
catalogue, 60 s bins per field and scan, and a byte budget of whole times.

| Realization | Whole times per batch | Closure | Gains vs production | SNR vs production | Weights vs production |
| --- | --- | --- | --- | --- | --- |
| Sequential, v2 | 1, 3, 4, 7, 12 | at delivery and deferred | identical | ≤ 1.2e-7 relative | ≤ 1.6e-7 relative |
| Sequential, v4 | 1, 3, 4, 7, 12 | at delivery and deferred | identical | ≤ 1.2e-7 | ≤ 1.6e-7 |
| Concurrent fragments, first held back 50 ms, v2 / v4 | 3 | both | ≤ 1.9e-7 absolute | ≤ 6.2e-6 | ≤ 1.6e-7 |
| Production against itself | — | — | identical | ≤ 1.2e-7 | ≤ 1.2e-7 |

Scans, intervals, identities and times are equal in every row of every
run. The two stores now give the same traces batch for batch (12, 4, 3, 2
and 1 batches; at most two groups live), differing only in buffer bytes
(3.8–42.7 KB for v2 with its per-correlation weight, 5.0–55.4 KB for v4).

- **Groups cross batches.** At one time per batch a 60 s group spans six
  batches with one group's sufficient statistics (32 entries) resident; at
  four times per batch a group's two fragments straddle a batch boundary.
  Accumulator state depends on the group's distinct times, antennas and
  baseline-correlations, not its samples. No visibility is retained across
  batches.
- **Closure is exact.** Each group closes exactly once: two `Last`/`Whole`
  fragments at delivery, or two `GroupEnd` events from a store opened to
  defer closure. In the concurrent realization `GroupCompletion` joins the
  two completions: both groups' input completed while fragment work was
  pending (`closed_with_pending = 2`), and each group was solved only after
  its partial accumulations had joined and merged.
- **Agreement.** The sequential solves match production as closely as
  production matches itself: its solver iterates hash maps in a per-run
  order. Merged partial sums change summation order, hence the 1e-7 gain
  differences in the concurrent run.
- **Science check.** The solutions recover the fixture's known gains,
  referred to antenna 0, to 0.092 rad in phase and 10.7 % in amplitude; the
  point-source model omits the 0.1 Jy offset source.

### Gaps example B exposed, and what revision 1 did

- *Time auxiliaries and scan* (first pass: missing, supplied by the trial):
  now requested roles, delivered from both stores and asserted against
  production.
- *Antenna identity* (first pass: list order; revision 1: rebound by
  context in test code): now `Catalogue::bind_to`, the contract's
  `CalibrationBinding` over two opened observations. Binding a caltable's
  own namespace is still untested.
- *Closure bookkeeping* (first pass: hand-written in the consumer): now
  `casa_observation::GroupCompletion`, shared with the work-unit law.
- **Finding:** production `gaincal` accumulates from `FLAG` and does not
  read `FLAG_ROW`; the MSv2 adapter folds `FLAG_ROW` into the flags (D5),
  so a row flagged only by it is excluded here and not in production. The
  fixture's `FLAG_ROW` is false everywhere, so this comparison does not
  exercise the case; whether CASA excludes such rows is to be checked
  separately.

## MSv4 interoperability

`tools/ms-trial/check_interop.sh` runs both directions on the committed
fixtures and writes the reports in [`interop/`](interop/). Rust does all the
reading and writing; Python is only the oracle
(`tools/ms-trial/check_interop.py`, in the pinned
environment).

| Direction | What runs | Checks | Result |
| --- | --- | --- | --- |
| XRADIO writes, Rust reads | `ms_trial_interop dump` reads XRADIO's store through the trial reader and dumps every array the batch delivers, with each time's instant as the epoch views represent it. XRADIO's `open_processing_set` loads the same store, and the oracle compares the two. | 26 | **all pass** ([report](interop/xradio_writes_rust_reads.json)) |
| Rust writes, XRADIO reads | `ms_trial_interop write` converts the MSv2 through four-time batches (which straddle the five-time chunks) into a fresh store with the trial writer. XRADIO opens it, `check_datatree` validates it and `.load()` forces every array. The oracle compares it with XRADIO's own conversion and with the MSv2 rows read through casacore. | 74 | **all pass** ([report](interop/rust_writes_xradio_reads.json)) |

Array comparisons are exact; nothing in these conversions is lossy. Time
comparisons are of the represented instants, as astropy (XRADIO's
reference for its time attributes) reads each side: they agree to
1e-11 s, the two-part MJD's rounding. Revision 2's conversion packs 4,080
elements where revision 1 packed 12,120
([counters](interop/rust_write_counters.json)): the MSv2 batch is already on
the dataset's grid, so only the per-correlation weight is written per
channel and the time centroids re-expressed from MJD 0.

- **XRADIO writes, Rust reads.** The oracle compares every value, flag and
  weight; `TIME_CENTROID` and `EFFECTIVE_INTEGRATION_TIME`; UVW, time and
  frequency coordinates; polarization labels, baseline antennas, the
  per-time field and scan; the instants of every time and time centroid;
  the interval against `time.integration_time`; and the time origin and
  scale, frequency frame, UVW frame, visibility units, data group, channel
  width, phase centre, and antenna names and positions.
- **Rust writes, XRADIO reads.** `VISIBILITY`, `FLAG`, `WEIGHT`, `UVW`,
  `TIME_CENTROID` and `EFFECTIVE_INTEGRATION_TIME` match in dims, dtype and
  every element. Every coordinate matches, as do the time, frequency, UVW
  and time-auxiliary measure attributes and the visibility units. The
  `antenna_xds` and `field_and_source_base_xds` variables match, with their
  measure attributes, as do the scan intents and the `base` data group's
  roles. The store now holds every main-dataset variable XRADIO's own
  conversion writes. Against the MSv2: every row has its own slot, with no
  padding; DATA, FLAG, the broadcast WEIGHT, UVW and EXPOSURE are equal,
  and the time and time-centroid instants equal the MSv2's (UTC).
- **Rust-only laws.** `observation_io`'s `writer` module, run in CI:
  stores written from MSv2 batches (their grid is the dataset's, so the
  only packing left is the per-correlation weight written per channel and
  the time centroids re-expressed from MJD 0, 4,080 elements) and from MSv4
  batches (encoded straight from their views, 0 elements packed) read back
  equal to the MSv2, time auxiliaries, scans and intents included; nothing
  is published until every time is written, a time is written once, a
  channel window is refused and a dropped writer removes its staging; a
  batch that disagrees with the declaration in value (a time the dataset
  lacks, another interval at a time) or a declaration that disagrees with
  the batches (reversed correlation products, other units, a field the
  catalogue lacks) is refused before anything of it is written, from either
  store; two live writers stage apart and the second finish is refused
  rather than replacing the first's dataset; and stale staging directories
  with another dataset inside are never adopted.

**Writer profile.** The writer takes the common `Catalogue`, a partition,
a data group and a `TimeBaselineGrid` (the scientific declaration) and
`Msv4StorageOptions` (dataset name, provenance, chunks, codec). It writes
the main dataset's coordinates, the data group (`complex64`, `bool`,
`float32`, `float64`), `TIME_CENTROID` and `EFFECTIVE_INTEGRATION_TIME`, an
`antenna_xds` and a `field_and_source_<group>_xds`. The payload variables'
fill values are the padding convention (NaN, flag true, weight 0). Chunks
are regular (here `[5, 10, 4, 4]`). Payload variables use Blosc-LZ4, none or
Zstandard; coordinates use Zstandard. Each writer creates its own staging
directory beside the destination (exclusively, under a name no other
writer or later retry uses) and publishes with a rename that refuses to
replace (`renameat2(RENAME_NOREPLACE)` on Linux, `renamex_np(RENAME_EXCL)`
on macOS, a checked rename elsewhere). It does not write pointing,
system-calibration or weather datasets, `EFFECTIVE_CHANNEL_WIDTH`, or
consolidated metadata, so XRADIO warns that it fell back to
non-consolidated reads. It writes no dataset larger than one partition,
only `unix`/`utc` times, and offers no update, append or resize.

**Conversion conventions applied explicitly.** XRADIO names antennas
`NAME_STATION`, fields and spectral windows `NAME_ID`, and an absent source
`Unknown`, and spells undeclared visibility units `"unkown"`. The trial's
MSv2 → MSv4 conversion renames antennas, fields and the spectral window in
its copy of the catalogue, as a stated policy, so the stores compare equal.
It does not relabel UVW: the MSv2 adapter already resolved MSv2's
`ITRF`-labelled UVW to the phase centres' J2000, which the writer checks
each batch against. Scans and intents come from the batch and the
catalogue.

### Interop findings

- **zarrs writes a configuration-less codec as a bare name.** zarr-python
  3.4.1 refuses to open it (`Expected dict, got str`), so the writer always
  gives the bytes codec its `endian`. zarrs also adds a `_zarrs` provenance
  attribute unless `include_zarrs_metadata(false)` is set per call; the
  writer sets it.
- **Zarr stores edge chunks at full size.** Reading the twelve times of
  five-time chunks decodes fifteen (amplification 1.25). Writing batches that
  straddle chunk boundaries makes zarrs read, modify and rewrite the partial
  chunks in place. That is fine for creation in staging, but it is not the
  contract's atomic object replacement for updates.
- **XRADIO's own outputs need care.** Without a UVW reference it writes
  `frame: null`; its pointing dataset fails its own schema; and it writes
  visibility units `"unkown"`.
- **`unix` times are UTC-based in every scale.** XRADIO defines its time
  attributes by astropy, whose `unix` format counts from 1970-01-01 UTC
  whatever the scale: `Time(1700000000., format="unix", scale="tai")` is
  22:13:57 TAI, while "Unix epoch in TAI plus the offset" gives 22:13:20.
  The first pass made exactly that error for `tai`; revision 1 reads and
  writes `unix`/`utc` only, and the oracle now compares instants.

## Keep, change or discard: the trial's summary for design review

Revision 1 kept what the review asked to keep and made the changes it
asked for (table at the top); revision 2 implemented the walkthrough's
decisions (second table). What remains open:

### Keep

- **One boundary, one interchange structure.** `Observation` (catalogue
  plus `bind_read`) is the only thing the two consumers see; behind it the
  same `VisibilityBatch` fed the production imaging pass and the production
  gain accumulator from MSv2 and MSv4 with bit-identical science.
  Dispatch is one virtual call per bind and per batch.
- **`ndarray` views under thin types.** Channel windows, time windows,
  transposes for the calibration layout, the `[T·B, F, P]` view and the
  zero-stride weight view all borrow without copying; the MSv4 view is the
  decode buffer itself.
- **Shared quantities in their stored form.** `Shared::{PerSample, PerTime,
  Constant}` carries an MSv4 interval once per batch and a scan once per
  time without expanding them, and `batch.interval_s(sample)` resolves any
  form, so consumers need no format rule.
- **One checked construction path.** Every batch, including packed
  windows, is built through `BatchFill::finish`, the one place lengths,
  identities and fragments are checked.
- **Input completion separate from work completion,** joined by
  `GroupCompletion` in the consumer.
- **Native Measures views** whose references are resolved, or refused, at
  opening.

### Open questions for the next iteration

Each of these was decided in [revision-2-design.md](revision-2-design.md)
(D12, D17, D16, D5, D25, D18 and D23 in order) and implemented in revision
2, with the qualifications recorded there; they are kept here as the record
of what revision 1 left open. What revision 2 leaves open is in that
record's last section: the time tolerance of the MSv2 grid, ephemeris as a
content key, MSv2 tile-level amplification, strided subset decodes, and an
MSv2 creator from batches.

1. **End of stream without a buffer.** `next_owned` returns the buffer at
   the end, as the review asked, but still needs one to ask; with an
   exhausted pool that costs a wait (measured above). `next_owned(Option<…>)`
   would answer `End` without one.
2. **Correlations and flag policy in the request.** Production imaging
   routes every stored correlation, so a flagged cross hand drops the Stokes
   I sample, as CASA does; gaincal flags a channel if any correlation is
   flagged. The request should name the correlations a consumer reads and
   the flag policy over unread ones.
3. **Validity cost.** `Validity::excluded` is evaluated per element, with
   up to four optional checks. That is fine at this size; a consumer's hot
   loop may want a per-batch mask or the rule folded into the fill.
4. **`FLAG_ROW` in production `gaincal`** (example B finding).
5. **Cross-store rebinding as a contract.** The calibration comparison
   rebinds by context in test code; the design's `CalibrationBinding` is
   still to be built and tested with a caltable.
6. **One vocabulary.** Move `CorrelationType` from the imaging model into
   `casa-observation` (or casa-types), and add a UVW measure to casa-types;
   `UvwView` still carries a `DirectionRef`.
7. **Creation catalogue for MSv2.** The writer's declaration is the common
   `Catalogue` plus a `TimeBaselineGrid`; an MSv2 creator needs a rows
   description in its place.

### Discard or defer

- The runtime's `NativeBlock` copy: the trial kept it so the production pass
  ran unchanged. M2 should let the pass place directly from batch views
  (`NativeRow` already borrows slices).
- The MSv2 opening index reads every row's key columns, in chunks of 2¹⁸
  rows, and keeps four bytes per grid cell (scale check above). Its time,
  about 0.2 µs a row, is what M2 should watch on the largest
  MeasurementSets; lazy construction per partition remains the option.
- Retained-byte figures (`capacity_bytes`, the work-unit ledger) count the
  batch buffers only, not allocations inside zarrs or casacore: they are
  probes, not admission bounds.

## M1 coverage of this pass

This pass touches parts of the M1 checklist and completes none of it.

| M1 packet | Touched here | Still required |
| --- | --- | --- |
| `layout-precision` | One sample-major layout from both stores, the grid's axes, Hz/m/s views, `complex64`, correlation subsets with a flag policy | Unit requests and conversions, Jy/weight scaling, `complex128` when a store needs it |
| `measures-bulk` | Native epoch, frequency and UVW array views only | Every family's prepared bulk conversion, ordered interpolation, scalar/CASA agreement, performance |
| `group-lifecycle` | Time-bin, per-scan and per-integration groups keyed by field and scan, closure, concurrent fragments, split channel windows, `GroupCompletion`, one cursor per partition, `Catalogue::bind_to` | Groups merged across partitions by a consumer, late samples, solve → caltable → reopen → apply |
| `work-unit-lifetimes` | Unequal channel units, shared pinning against packing, bounded pools, buffers returned at the end | Cooperating workers on the admitted team, cancellation in flight, cube minor-cycle separation |
| `native-role-updates` | — | All |
| `conversion-fidelity` | One loss-free MSv2 → MSv4 profile, both converters | Every audited loss row, rejection policies |
| `source-free-create` | Common catalogue + grid declaration for both stores, padding fill values, storage options, the narrow writer with per-batch validation, exclusive staging and no-replace publication | v2 creation from batches, producer seam, backpressure, publication failure injection |
| `store-retention-executor` | zarrs pinned; concurrency target 1 | Key/handle retention plateau, W1/W4 thread and buffer counts |
| `object-failure` | — | All |
| `production-access-cost` | Narrow diagnostics on a tiny fixture; the estimate (bytes, batches, amplification, natural blocks, padding) as the planner's inquiry | The named MFS/cube/bandpass rows on both chunk geometries, noise bands |

## Unsupported in the trial (typed refusals)

`complex128` stores; more than one data group per traversal; MSv2 times
merged by tolerance (distinct `TIME` values are distinct grid times); two
rows of one time, baseline and partition, or several `PROCESSOR_ID`s in one
partition; two fields or scans at one time of one partition; MSv4 time
formats other than `unix`,
`unix` times in a scale other than UTC, and MSv4 writes in other than
`unix`/`utc`; MSv2 measure columns with a variable reference, and `TIME`
and `TIME_CENTROID` in different scales; MSv2 UVW labelled with a celestial
frame other than the phase centres'; MSv4 groups whose UVW frame is not
their phase centres'; frames other than fk5/icrs for directions and UVW;
units other than the schema's (none are converted); MSv4 variables of other
data types or dimension orders (bitmask flags included); one antenna name
with two positions; a batch budget smaller than one time; per-cell
`WEIGHT_SPECTRUM` fallback; `SIGMA`, `FLAG_CATEGORY`; writing channel
windows, correlation subsets, or batches that disagree with the
declaration; MSv4 sharding (not exercised); remote stores.
