# MSv2/MSv4 trial evidence

Status: revision 4 (the walkthrough's decisions, the cleanups revision 2 exposed, and the answers to the reviews of revision 3); the read contract frozen narrowly for M2 by the owner, who waived a re-review (2026-10-25); the write-back and converter steps added (2026-10-26); not M0/M1 acceptance
Truth class: trial evidence
Date: 2026-10-26 (converter section)
Handoff: [trial-handoff.md](../trial-handoff.md)
Review brief: [review-brief.md](review-brief.md)

This is the evidence index for the trial's first assignment: two worked
examples (A, dirty-image channel planes; B, a gain solve) and the early
bidirectional XRADIO check. It records what ran, with what, and what it
showed, including what is missing. Nothing here claims M0 or M1 complete:
the full caller and Measures inventory, every Measures family's bulk route,
general updates and creation, failure injection and the benchmark matrix
remain later requirements.

## Revision 4: the reviews of revision 3

Astra's design review and Fable's independent code review of revision 3
(`a6852cc`) on [bglenden/casa-rs#898](https://github.com/bglenden/casa-rs/pull/898)
found three blocking and four should-fix design points (Astra: B1–B3,
S1–S4) and two blocking, seven should-fix and seven minor code points
(Fable). Revision 4 answers every one; the design documents now state the
current contract directly ([contracts](../contracts.md), revision 7).

| Finding | Change | Evidence |
| --- | --- | --- |
| Astra B1: partitions bound by spectral content alone | Sessions keep MSv4's archive UIDs (MSv2's from `OBSERVATION::SCHEDULE`) and bind first; partitions bind by content with channel widths, the bound session, the bound field and scan sets, and the feed pair where both stores have one; every binding is one to one, so a key one store lacks never collapses two entities the other keeps apart. | `partitions_bind_by_session_fields_scans_feeds_and_widths` (same spectrum in two sessions, field and scan splits, asymmetric subsets, feeds, widths), `catalogues_describe_the_same_observation` |
| Astra B2: a buffer count is not a byte bound | Every pool buffer has a byte **envelope**: `begin` checks the header against the catalogue and reserves exactly the batch's arrays, refuses a batch over the envelope before allocating, and frees what earlier fills kept when keeping it would exceed the envelope; a returned buffer over it is emptied. The estimate gains per-time bytes and `buffer_bytes` (exactly what a fill reserves); `peak_buffer_bytes` is a traversal's envelope. | `a_pool_buffer_never_holds_more_than_its_envelope` (values-heavy then weights-heavy fills, Astra's reproduction), `the_budget_fits_whole_times_or_refuses_binding` (a batch's buffer is exactly `buffer_bytes`; an envelope a byte short refuses before allocating) |
| Astra B3: centroid extra precision dropped | A dataset with `TIME_CENTROID_EXTRA_PRECISION` refuses a request for centroids (`Unsupported`); one without them reads. | `a_split_msv4_centroid_is_refused_not_truncated` |
| Astra S1: correlations copied per batch | `CorrelationAxis` (`All`, or a `Subset` of shared products and indices) is resolved once at binding; every batch and packed piece shares it, and all-products batches borrow the catalogue's list. The MSv2 cursor no longer builds an index list per fill. | `the_correlation_axis_is_shared_not_copied` |
| Astra S4: per-baseline polarization basis ignored | An MSv4 dataset with `polarization_mixed` is refused at opening. | `a_baseline_dependent_polarization_basis_is_refused` (two different baseline mappings) |
| Astra S2, S3: superseded contract text | `contracts.md` revision 7: one current read contract (sections 1–6 and creation) and a provisional table with each capability's acceptance obligation; `architecture.md` states the landed ADR-0016 amendment and only the remaining M2 consumer edges. | docs gates |
| Fable: validity differs by store on a flags-only read | The MSv4 adapter folds a non-finite stored weight into every delivered flag (XRADIO pads with NaN and `FLAG` false), so a delivered flag marks every absent acquisition in both stores. | `absent_acquisitions_are_excluded_whichever_roles_are_read` (XRADIO-style padding written into the fixture; an MSv2 with a row removed) |
| Fable: a padded partition can be neither declared nor written | `Observation::grid` gives the declaration from the store's index (no payload read; `from_cursor` is gone); padding has no interval, so neither the grid nor the writer compares one. | `a_padded_partition_is_declared_and_written` |
| Fable: grouped read without `scan_name` panics | A grouping keyed by scan on a store that names none is refused at binding; `field_name` and `scan_name` lengths are checked at opening. | `a_grouping_needs_the_scans_it_keys_by` |
| Fable: abandoned work hangs the join | A `WorkToken` dropped unfinished abandons its group; `finish`, `close` and `is_idle` report `WorkAbandoned`. The completion keeps each series' latest closed bin instead of every finalized group. | `abandoned_work_is_an_error_not_a_wait`, `a_group_is_ready_once_after_its_input_and_its_work` |
| Fable: the pool's wait is unbounded | `BufferPool::waiting_at_most(limit)` fails with `PoolExhausted`; a refused `next_owned` consumes nothing. | `a_limited_take_reports_an_exhausted_pool`, the budget law (retry delivers the next time) |
| Fable: chunk-straddling writes are unaligned | The writer holds a batch's times in a chunk it covers only partly until the chunk is complete, then stores it whole; whole chunks are stored straight from the batch (the owner's choice of three: count only, refuse unaligned batches, or hold). `Msv4Writer::buffer_bytes` is the most it holds, one chunk of times; `held_elements` counts the copies and `partial_chunks` any chunk still stored partly. | writer law (four-time batches on five-time chunks: no partial chunk, times 0..10 held; twelve-time batches: nothing held), `batches_out_of_time_order_are_written_and_counted`, the plan's unit laws; interop write counters (partial chunks 24 → 0) |
| Fable: padded and subset paths, parallel cursors unmeasured | The probe gains `--products` and `--parallel`, and `ms_trial_pad` pads a copy; the MSv2 cursor now spreads padded rows in place instead of decoding into scratch and copying. | scale check below; `spreading_in_place_is_placing` |
| Fable: clippy fails on macOS | One `rename_noreplace` per platform, each ending in a tail expression. | clippy on the Mac mini |
| Fable: request resolved twice; per-element validity in the examples; ignored `model_source`; `time_s` dropping the origin's fraction; `FlagsFrom` attribution; duplicated sentence | Resolved once per bind; `mask_into` once per batch or fragment; the parameter removed; the epoch view's own conversion; attributed to casa-rs's operator; contracts rewritten. | every law and example |

Gates for revision 4: clippy `-D warnings` on `casa-observation`, `casa-ms`
(`msv4`), `casa-imaging-application`, `casa-calibration` and `casa-types`,
every target, on Linux and on the Mac mini (macOS 27.0.1); `casa-observation`
12 unit tests and its doctest; `casa-ms` library, 431 pass, and adapter laws,
28 pass; example A, 5 pass; example B, 5 pass;
`tools/ms-trial/check_interop.sh` 26/26 and 74/74 with both oracle reports
unchanged (the write counters gain `partial_chunks`); imaging T0 and T1;
`just arch-check`, `just docs-check`, strict MkDocs, rustdoc with broken and
private links denied. The held chunks came after: `casa-ms` library and
adapter laws again (432 and 29 pass), the interoperability check
again (26/26 and 74/74, oracle reports unchanged; the write counters gain
`held_elements` and lose every partial chunk).

### The owner's decisions on revision 4

- **No re-review.** The owner waived an independent re-review of revision
  4: what remains open is implementation, to be found as the applications
  are ported, not design.
- **A narrow freeze.** The read contract, [contracts](../contracts.md)
  sections 1–6 and creation from batches, is frozen for M2; updates and the
  provisional capabilities (its section 8) are not. Porting the applications
  is expected to show where the design is not sufficient, and such a finding
  reopens the contract concerned under the implementation plan's change
  control.
- **Whole-chunk writes.** Of counting partial chunks, refusing unaligned
  batches, or holding partial chunks in the writer, the owner chose holding
  (the Fable row above).

## Revision 3: the weak spots revision 2 exposed

Revision 2's own review (this document's "What revision 2 taught", the scale
check below, and the owner's questions on performance and memory) found
places where the boundary was right in substance but awkward or unsafe in
form. Revision 3 changes them, and nothing else; the examples, the
interoperability checks and every adapter law are unchanged in what they
assert.

| Change | What it replaces | Evidence |
| --- | --- | --- |
| **A batch carries its catalogue.** `BatchFill::finish` keeps an `Arc<Catalogue>` in the buffers; `BatchBuffers::batch()`, `FilledBatch::batch()` and `BatchBuffers::pack(batch)` take no catalogue, and `VisibilityBatch::catalogue` names it. `Observation::catalogue()` returns the shared `Arc`. | Every consumer passing the observation's catalogue back in; another observation's catalogue gave a panic or silently wrong frequencies. | `buffers.rs` unit tests, every adapter law |
| **A bounded `BufferPool` for owned batches.** `next_owned(&pool)` takes a free buffer only for a batch and waits while none is free (the backpressure); a `FilledBatch` returns its buffer when dropped, or when its last `Arc` is. `BufferPool::pack` copies a window into a pooled piece. `OwnedEvent` is `Batch`, `GroupEnd` or `End`. | `next_owned(Option<BatchBuffers>)` with `NeedBuffers`, buffers handed back in every variant, and a boxed `Reclaimed` error: each consumer kept its own pool and handshake, and one development bug was an infinite loop in it. | `pool.rs` unit tests (a full pool waits; failures return the buffer), `the_budget_fits_whole_times_or_refuses_binding`, the work-unit probe and example B's concurrent solve (every buffer back at the end) |
| **Work tokens.** `GroupCompletion::begin` returns a `WorkToken`, which `finish` consumes. | `begin(key)` / `finish(key)` counting: a unit could finish work it never began, or twice. | grouping unit test, `split_channel_windows_complete_each_group_once`, example B |
| **The budget counts decode scratch.** `PartitionEstimate::scratch_bytes_per_sample` is what the cursor's own scratch holds per sample (MSv4: the flag bytes, and every stored product of values and weights when a subset is delivered; MSv2: each present row's number, sample, `FLAG_ROW` and time auxiliaries, and every stored product when a time has padding or a subset is delivered). `times_per_batch` budgets `held_bytes_per_sample()`; `Estimate::peak_held_bytes()` is what a pool buffer and its cursor hold. | A budget that counted only the delivered products: a two-of-four subset roughly doubled real use. | `the_estimate_is_the_bound_realization_and_models_the_store`, `a_correlation_subset_carries_the_flags_it_is_asked_for` |
| **The writer validates into a `Checked` batch** holding the views it proved present (payload, centroid, exposure, scan) and its placement. | Checks followed by `expect("checked")` where the views were used. | writer laws |
| **`Traversal` turns are an enum**, not an `Err` carrying a group end. | `Result<Option<usize>, Step>` with `unreachable!` arms. | every traversal |
| **The MSv2 opening index is built in row chunks** (`msv2/index.rs`, earlier on this branch). | 75–80 bytes per MAIN row at opening. | `the_index_does_not_depend_on_the_chunk_size`, scale check |

Gates for revision 3: clippy `-D warnings` on `casa-observation`, `casa-ms`
(`msv4`), `casa-imaging-application` and `casa-calibration`, every target;
`casa-observation` 8 unit tests and its doctest; `casa-ms` library and
adapter laws, 458 pass; example A, 5 pass, with the work-unit probe below
re-measured; example B, 5 pass; `tools/ms-trial/check_interop.sh` 26/26 and
74/74 with the committed reports unchanged; rustdoc with broken and private
links denied.

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
| D3 | One topology: the MSv2 adapter places rows on the time × baseline grid at opening (`msv2/index.rs`); a cell no row fills is padding (flag true, NaN values and auxiliaries, weight 0; NaN since the write-back trial). `Irregular`, `Occupancy` and the row-order refusal are gone. | `msv2_cells_no_row_fills_are_padding` |
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
| Convert either store | `cargo run --release -p casa-ms --features msv4 --example ms_trial_convert -- to-msv4\|to-msv2 SOURCE DEST [--workers N] [--chunk-times T] [--codec blosc\|zstd\|none] [--padding flagged\|omitted] [--accept sigma,tables,…]` (chunks of 10 times unless `T` is given) | Rust only |
| Read passes (retention, threads) | `cargo run --release -p casa-ms --features msv4 --example ms_trial_read_passes -- STORE [--passes N] [--workers W]` | Rust only |
| Update pass (replacement and synchronization cost) | `cargo run --release -p casa-ms --features msv4 --example ms_trial_update_pass -- COPY_OF_STORE` | Rust only; changes the store |
| Converter against XRADIO | `tools/ms-trial/check_convert.py xradio-convert --ms MS --out PS`; `… compare --ours PS --theirs PS` | the pinned XRADIO environment |
| Converted MSv2 against its original | `python tools/ms-trial/check_v2_readback.py ORIGINAL CONVERTED` | a CASA install's Python (casatools) |

## Adapter laws

`crates/casa-ms/tests/observation_io.rs` and its modules `refusals`,
`writer` and `work`: 28 tests, all passing (the writer laws are described
under interoperability, the refusals in revision 1's table and revision 4's).
The shared types have 12 unit tests of their own
(`cargo test -p casa-observation`), and the adapters' group index, padding
spread, `SCHEDULE` parser and partial-chunk count 7.

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
- One time that does not fit the byte budget fails binding; a traversal
  read through a pool of one buffer gets each batch in that buffer, has it
  back whenever the batch is dropped, and takes none at the end (revision 3;
  revision 2 asked without buffers, `NeedBuffers`).
- An MSv2 row removed from the fixture leaves a padded cell: flagged, NaN
  values, weights, UVW and auxiliaries (weight 0 until the write-back
  trial), excluded by the validity rule; the
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
| Shared batch (`Arc<FilledBatch>`, `BufferPool` of 2), v2 | 61.5 ms | 181.6 ms | 60.5 ms | 2 × 7.4 KB | 14.8 KB of batches | 0 |
| Packed pieces (`BufferPool::pack`, a pool of 2 per unit), v2 | 1.1 ms | 181.8 ms | 60.2 ms | 1 × 7.4 KB | 7.4 KB batch + 12.4 KB pieces | 7,200 elements |
| Shared batch, v4 | 64.3 ms | 183.5 ms | 60.6 ms | 2 × 9.7 KB | 19.4 KB of batches | 0 |
| Packed pieces, v4 | 4.7 ms | 183.1 ms | 58.0 ms | 1 × 9.7 KB | 9.7 KB batch + 15.4 KB pieces | 7,200 elements |

Revision 3, debug build, one run. The slow unit's 3 × 60 ms delay dominates
its time by design. The v2 batch is smaller than in revision 1: it no
longer carries frequencies, correlations, antennas, a group name or row
flags, and its weight is one per correlation (its packed pieces keep that
per-correlation weight, so a v2 piece is a different size from a v4 one
while holding the same values).

The producer draws on the pool, which hands out a buffer only for a batch,
so the shared-batch stall is the one 60 ms wait for the slow unit's buffer
and nothing is lent at the end; every buffer of every pool is back when the
run finishes, which the probe asserts.

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
| `pilot.ms` (4.1 GB; 351 baselines, 64 partitions over 32 spectral windows × 64 channels × 4 products) | 2,021,760 | 64 | 0.37 → 0.24 s | 164 → 30 MB | 4,566 MiB in 1.96 s (2,325 MiB/s) both | 278 → 146 MB |
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

Revision 4 measured what that run did not exercise: the padded and
correlation-subset paths, and parallel cursors. Same machine, release build,
page cache warm; each figure is one of two or three alternating runs, which
agreed within 2%. `padded` is a copy of `pilot.ms` from which
`ms_trial_pad` removed antenna 5's rows for the second half of its times
(37,440 rows; 32 of the 64 partitions padded, up to 3.7% of their cells):

| Read of `pilot.ms` (4,566 MiB delivered) | Revision 3 (`a6852cc`) | Revision 4 | Peak RSS, 3 → 4 |
| --- | --- | --- | --- |
| Every product, unpadded | 1.93–1.96 s | 1.92–1.96 s | 146 → 146 MB |
| Every product, `padded` copy | 2.85–2.90 s | 2.56–2.60 s | 241 → 170 MB |
| `RR,LL` (2,329 MiB delivered) | — | 2.31–2.34 s | 183 MB |
| Every product, 8 workers (`--parallel`) | — | 0.52 s (8.8 GB/s) | 760 MB |
| `padded`, 8 workers | — | 0.82 s (5.5 GB/s) | 815 MB |

- **No change where nothing was padded,** and the parallel reads deliver
  every partition's samples with the same checksum as the sequential read.
  Eight workers read 3.8 times as fast as one.
- **Padding now costs nothing measurable in the adapter.** Revision 3
  decoded a padded batch into scratch, pre-filled the batch with padding and
  copied each row; revision 4 decodes the present rows straight into the
  batch and spreads them to their samples in place, one move of runs, with no
  payload scratch (70 MB less at the peak). The copy's padded partitions now
  read in 1.28 s and its unpadded ones in 1.31 s, against 0.98 s for the same
  partitions of `pilot.ms`. The remaining 30% is the table `remove_rows` and
  `save` leave behind, which slows every partition alike; it is a property
  of that edit, not of the padding path.
- **A two-of-four subset** decodes every product and gathers two: 19% longer
  than the whole read for half the bytes.

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
| Sequential, v2 | 1, 3, 4, 7, 12 | at delivery and deferred | ≤ 1.3e-7 absolute | ≤ 2.5e-6 relative | ≤ 1.6e-7 relative |
| Sequential, v4 | 1, 3, 4, 7, 12 | at delivery and deferred | ≤ 1.3e-7 | ≤ 2.5e-6 | ≤ 1.6e-7 |
| Concurrent fragments, first held back 50 ms, v2 / v4 | 3 | both | ≤ 1.3e-7 | ≤ 2.1e-6 | ≤ 2.0e-7 |
| Production against itself | — | — | identical | ≤ 1.2e-7 | ≤ 1.6e-7 |

Scans, identities and times are equal in every row of every run, times to
a few ulps of MJD seconds (about 1 µs each): production's solution time is
a weighted mean over baselines summed in hash-map order, so it differs by
an ulp from itself too. These figures are after merging `main` into the
design branch (below); before it, the gains were identical. The two stores now give the same traces batch for batch (12, 4, 3, 2
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
  own namespace: section "Caltable round trip".
- *Closure bookkeeping* (first pass: hand-written in the consumer): now
  `casa_observation::GroupCompletion`, shared with the work-unit law.
- **Finding, resolved on `main`:** production `gaincal` accumulated from
  `FLAG` and did not read `FLAG_ROW`, while the MSv2 adapter folds
  `FLAG_ROW` into the flags (D5). `main`'s `0ddb970` made both solves treat
  such a row as fully flagged, confirmed against a CASA 6.7.5.18 oracle, so
  D5 and production now agree. The fixture's `FLAG_ROW` is still false
  everywhere; a case that sets it is what remains of #907.
- **Finding from merging `main`: the solve weights are not the batch's
  weights.** `main`'s `4eeaab2` makes production `gaincal` weight each
  sample by 1/σ² from `SIGMA` (`SIGMA_SPECTRUM` where the MeasurementSet
  has `WEIGHT_SPECTRUM`), as CASA's `CalSolvingVi2::calibrateCurrentVB`
  does through `resetWeightsUsingSigma`; it no longer reads `WEIGHT`. The
  batch carries `WEIGHT` and no `SIGMA` (contracts, section 8), and XRADIO's
  conversion maps no `SIGMA` into MSv4 (architecture, conversion
  fidelity). The fixture's `WEIGHT` is 1/σ² to 1.3e-7, so example B, which
  solves with the batch's weights, still agrees with production to
  rounding, but on data whose `WEIGHT` is not 1/σ² (after `statwt`, say)
  a batch-fed solve would differ from CASA. Before calibration moves onto
  batches, the contract needs the weight role a solve uses: `SIGMA` as a
  requestable role for MSv2, and a stated rule for MSv4, which has only
  `WEIGHT`. **Resolved in step 4** (section "Weights from sigmas" below):
  `sigma_weighted_batch_solves_equal_the_production_gaincal` gives the
  fixture `SIGMA` that is not 1/√`WEIGHT` by a factor varying over rows and
  correlations; the batch-fed solve reading 1/σ² equals production to
  1.3e-7 in solution weight, while one reading `WEIGHT` differs by a factor
  of 2,500.

## MSv4 interoperability

`tools/ms-trial/check_interop.sh` runs both directions on the committed
fixtures and writes the reports in [`interop/`](interop/). Rust does all the
reading and writing; Python is only the oracle
(`tools/ms-trial/check_interop.py`, in the pinned
environment).

| Direction | What runs | Checks | Result |
| --- | --- | --- | --- |
| XRADIO writes, Rust reads | `ms_trial_interop dump` reads XRADIO's store through the trial reader and dumps every array the batch delivers, with each time's instant as the epoch views represent it. XRADIO's `open_processing_set` loads the same store, and the oracle compares the two. | 26 | **all pass** ([report](interop/xradio_writes_rust_reads.json)) |
| Rust writes, XRADIO reads | `ms_trial_interop write` converts the MSv2 through four-time batches (which straddle the five-time chunks; the writer holds the partial ones) into a fresh store with the trial writer. XRADIO opens it, `check_datatree` validates it and `.load()` forces every array. The oracle compares it with XRADIO's own conversion and with the MSv2 rows read through casacore. | 74 | **all pass** ([report](interop/rust_writes_xradio_reads.json)) |

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
fill values are the padding convention (flag true, values and weights NaN,
as XRADIO pads). Chunks
are regular (here `[5, 10, 4, 4]`). Payload variables use Blosc-LZ4, none or
Zstandard; coordinates use Zstandard. Each writer creates its own staging
directory beside the destination (exclusively, under a name no other
writer or later retry uses) and publishes with a rename that refuses to
replace (`renameat2(RENAME_NOREPLACE)` on Linux, `renamex_np(RENAME_EXCL)`
on macOS, a checked rename elsewhere). It writes pointing only for the
converter ("Geometry inputs"), and no system-calibration or weather datasets, `EFFECTIVE_CHANNEL_WIDTH`, or
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
  chunks in place. Revision 4's writer holds a batch's times in such a chunk
  until the chunk is complete, so creation in time order encodes each chunk
  once; an update that covers a chunk partly still reads, modifies and
  rewrites it, which is not the contract's atomic object replacement.
- **XRADIO's own outputs need care.** Without a UVW reference it writes
  `frame: null`; its pointing dataset fails its own schema; and it writes
  visibility units `"unkown"`.
- **`unix` times are UTC-based in every scale.** XRADIO defines its time
  attributes by astropy, whose `unix` format counts from 1970-01-01 UTC
  whatever the scale: `Time(1700000000., format="unix", scale="tai")` is
  22:13:57 TAI, while "Unix epoch in TAI plus the offset" gives 22:13:20.
  The first pass made exactly that error for `tai`; revision 1 reads and
  writes `unix`/`utc` only, and the oracle now compares instants.

## Write-back: update sessions on both stores

Agreed with the owner on 2026-10-26 as the step before any consumer is
converted: the update side of the design is the part most likely to push
back on the frozen read contract, and nearly every consumer writes
something (`MODEL_DATA`, `CORRECTED_DATA`, `FLAG`). It realizes, narrowly,
the update obligations of `contracts.md` section 7 that the design reviews
raised: mutation scope over broadcast and data-group aliases (Pro R1),
the failure scope of an interrupted update and MSv4 atomic replacement
(Pro R3, Fable), the in-process lock rule of a mixed read/update session
(Fable), no read-back of a unit a patch supplies whole (Fable), and what a
flag change writes (Fable).

**The interface** (`casa_observation::update`, `casa_ms::observation_io::update`):

- `open_update(path, options, targets)` takes this process's write intent
  on the store, opens it once, and resolves the declared `UpdateTarget`s
  (data group, role, alias scope, create) before any payload. The session
  lends the observation for reading and `Updater`s for writing, each bound
  to a traversal and some targets; live updaters claim disjoint times of
  each variable (time selection, "Updaters of a split").
- `Updater::write(batch, values)` takes one `PatchValues` per bound target
  for a batch the session delivered: visibilities, a flag change (set and
  clear masks; elements neither names keep their stored flag), or weights
  for the delivered channels or all of them. Values are checked against the
  batch before anything is written.
- The batch's sample addresses are found from its own times and the
  partition's baselines (the MSv2 grid's rows, the MSv4 dataset's time
  index): **the frozen read contract needed no change to be written
  back.**

| Law | What it shows |
| --- | --- |
| `a_model_pass_reads_one_group_and_writes_another` | One owner reads `base` and writes `model`: MSv2 creates `MODEL_DATA` holding CASA's initial model (120 cells seeded), writes 120 cells over it and reads none, and opens afterwards with a `model` group carrying `DATA`'s keywords; `DATA` is unchanged. MSv4 refuses to add a group at opening and rewrites `base`: six chunks written whole from the held windows, none read, 14,416 bytes held (five times of every element and its mark, and two chunk counts). |
| `a_partial_model_pass_creates_model_data_over_casas_initial_model` | A two-channel model pass creates `MODEL_DATA`: its two channels hold the model and every other cell CASA's initial model, 1 in `XX` and `YY` and 0 in `XY` and `YX`, never `DATA`. |
| `partial_units_are_read_once_and_whole_ones_never` | A two-channel window: MSv2 reads each of the 120 cells it changes partly; MSv4 reads the three chunks it covers partly once each, across three batches. Every other channel reads back unchanged. |
| `a_weight_shared_by_channels_changes_only_as_a_whole` | Pro R1: an MSv2 `WEIGHT` serves every channel, so changing the delivered two channels is refused and nothing is written; `WeightChannels::All` is the explicit whole-scalar change. MSv4 changes just the delivered channels. |
| `a_shared_variable_is_written_only_with_its_aliases_accepted` | `base` and `model` share `FLAG`: a flag target of either is refused unless it accepts the other group as an alias, which the resolved target names; so is a `base` flag target in a session that creates `model`. |
| `flag_changes_keep_row_flags_and_padding` | Clearing `FLAG` leaves a `FLAG_ROW` row excluded; a padded cell (no MSv2 row) receives nothing (`unaddressed`) and clearing its flag is refused. |
| `msv4_padding_stays_flagged` | Clearing a flag or writing a finite weight where the MSv4 store holds padding (a non-finite weight) is refused, and so is clearing flags from a traversal that does not read weights. |
| `padding_the_writer_stores_stays_padding` | An MSv2 with a row removed, written by casa-rs's writer, holds its padding with a NaN weight, as XRADIO does: the updater refuses to clear its flag or weight it, and it reads back flagged. |
| `an_empty_flag_change_keeps_every_flag` | A flag change with neither mask keeps every flag: MSv2 rewrites the cells as stored, MSv4 writes nothing. |
| `clearing_every_msv4_flag_erases_chunks_and_finishes` | Clearing every flag makes each `FLAG` chunk equal to the fill value, which zarrs erases rather than stores, an absent chunk included; the update finishes (its directories synchronized) and reads back unflagged. |
| `an_msv4_batch_of_other_channels_is_refused_whole` | A batch whose channels are not the updater's traversal's is refused before any target takes it, so nothing of it is written. |
| `a_session_reads_what_its_updaters_wrote` | A second traversal of the session's observation delivers what its first updater wrote, in both stores. |
| `one_update_session_per_store_and_one_updater_per_time` | A second update session on a store, or a second live updater of a session over the same times, is a `Conflict`; one variable bound twice to an updater is refused (whichever wrote last would win); read-only observations open beside them. MSv2 states casacore's cross-process lock; MSv4 states atomic units and no cross-process lock. |
| `an_msv2_updater_takes_its_batches_in_order` | The MSv2 updater takes its traversal's batches in delivery order: a batch after a skipped one is refused, and finishing with selected times unwritten fails. |
| `an_msv2_session_holds_mains_lock_while_an_updater_is_live` | casacore's write lock on MAIN is held from the first live updater to the last, between batches too, and free when none is live. |
| `a_failed_replacement_leaves_each_chunk_old_or_new` | Pro R3: a replacement failing after its temporary file is written (as a killed process leaves it) publishes nothing of that chunk: times 0..5 read new, 5..12 old, the temporary file stays unread beside the old chunk. |
| `a_failed_first_write_leaves_an_absent_chunk_absent` | `FLAG`'s chunk `c/2/0/0/0` is absent (equal to the fill value); its failed first write leaves it absent, reading as the fill value, and the four chunks before it replaced. |
| `a_failure_before_writing_changes_nothing` | A failure before any byte is written changes nothing. |

**How each store writes.**

- **MSv2** reuses production's bounded selected-row mutation
  (`MeasurementSetWriteSession`), one per batch over its present rows,
  under casacore's write lock on MAIN, which the session holds from its
  first live updater to its last and takes with its handle reopened, so a
  merge never reads a table older than the lock. Each batch's cells are
  written in place, a cell read back only when a patch covers it partly. A
  failure leaves the rows already written, as casacore's writes do.
- **MSv4** holds one chunk-row of times per target variable (the window),
  over the channel chunks the traversal reaches (every channel for
  weights), and replaces each chunk whole through a store whose writes go
  to a synchronized temporary file renamed over the chunk. The window
  counts the elements assigned in each chunk: a chunk assigned whole is
  encoded without reading it, one assigned partly is read once per window,
  and an untouched one costs nothing. Every replacement is atomic; there is
  no transaction across chunks. Finishing synchronizes the directories the
  replacements changed (those whose objects it renamed or removed), so a
  finished update is on disk wherever the filesystem honours `fsync`.

**What it shows about the design.**

1. **Addressing is enough.** A batch's times and the partition's baselines
   locate every sample in both stores; no update map had to be added to the
   read side.
2. **Occupancy is implicit.** Padding is a missing MSv2 row or a NaN MSv4
   weight; the batch carries no presence mask, so the MSv4 updater needs
   weights read to keep padding flagged. An explicit presence role in the
   batch (`contracts.md` section 8, "Occupancy") would make this uniform.
3. **Production's MSv2 mutation plans its rows up front** and takes
   values as one `ArrayValue` per cell. The updater now plans per batch,
   one mutation per batch under the lock the session holds, so the
   mutation no longer needs the traversal planned whole; skipping
   unchanged batches would still need the updater to accept gaps (it takes
   every batch in order), and the M2 hot path still needs typed columns.
4. **MSv4 cannot gain a data group.** Writing a model to an MSv4 store needs
   a structural write (a new variable and group); MSv2 adds the column.
5. **Data groups are now the stores' own.** The MSv2 adapter lists
   `corrected` and `model` where `CORRECTED_DATA` and `MODEL_DATA` exist,
   as XRADIO's converter names them, all sharing `FLAG`, the weights and
   `UVW`.
6. **Padding needed one mark.** The MSv2 adapter padded with weight 0 and
   casa-rs's writer stored that, so an MSv4 written from a padded MSv2
   could not tell padding from a flagged acquisition, and a flag pass could
   clear it. Padding is now a NaN weight in both stores (as XRADIO pads),
   a contract revision to section 3; the validity rule already excluded a
   non-finite weight. Finding 2's presence mask would remove the
   inference.
7. **A new model starts as CASA's.** The first version left a created
   `MODEL_DATA` holding `DATA` wherever the pass did not reach, which gives
   a later solve identity gains there. CASA initializes a new `MODEL_DATA`
   to a unit point source, 1 and 0 in the cross hands `RL`, `LR`, `XY` and
   `YX` (`VisSetUtil::initScrCols`, called by tclean's `addScrCols` and by
   `VisSet`), and `CORRECTED_DATA` from `DATA`. The owner chose CASA's
   model (2026-10-26): the updater installs `MODEL_DATA` from `DATA`'s
   storage, the layout applycal's `CORRECTED_DATA` uses, and overwrites
   every row with that model before the pass writes. One helper,
   `casa_ms::initial_model_visibility`, holds the rule, and every casa-rs
   writer that adds `MODEL_DATA` uses it: this updater, imaging's in-place
   writer (which filled a new column with 0 until this change) and the
   VLA importer. The simulator's `predict` writes 1 in every product, cross
   hands included, because that is what CASA's `Simulator::predict` does
   after the prediction (`vb.setModelVisCube(Complex(1.0,0.0))`); it
   overwrites the model rather than initializing a new column. Every path
   that adds `MODEL_DATA` or `CORRECTED_DATA` to an existing
   MeasurementSet (this updater, imaging's writer, applycal) first clears
   the cached sort keywords, as CASA's `addScrCols` does, and every path
   that writes `MODEL_DATA` (this updater and imaging's writer), created
   or not, first clears every field's virtual model, as CASA's imagers do,
   since CASA would otherwise read it in the column's place. Both go
   through `casa_ms::scratch_columns` (#912).

**Not done:** per-batch MSv2 row planning; `WEIGHT_SPECTRUM` and
`FLAG_CATEGORY` (the fixture has neither); MSv4 structural writes; a
process actually killed during a replacement (the injected failure leaves
what a kill leaves); sweeping stray temporary files; the MSv2 read-back of
partly covered cells one cell at a time; a failure while `MODEL_DATA` is
being seeded, which leaves the rows not yet seeded holding `DATA`. The cost
of synchronizing each replaced chunk at scale is measured in the converter
section (finding 4).

**Independent review.** A separate agent reviewed the first version and
found three blocking defects, all fixed with laws: a flag change with
neither mask cleared every MSv2 flag of a whole cell; MSv4 padding written
by casa-rs's writer (weight 0) could be cleared or weighted into an
acquisition; and `MODEL_DATA` was seeded from `DATA` (finding 7). It also asked for
one binding per variable, aliases for groups the session creates, a
directory synchronization at finish and a window limited to the channels
reached, all done. Its check of the fixes found one more defect (finishing
failed after an absent chunk was erased, since its directory did not exist)
and a batch of other channels taken by one target before another refused
it; both are fixed with laws.

## Converter: MSv2 ↔ MSv4 through the boundary

Step (3) of the order agreed with the owner on 2026-10-26: a converter in
both directions, with MSv2 creation, the conversion-loss policy, and the
retention, codec-thread, pin and synchronization measurements on real
data. Both directions read the source as any consumer reads it (catalogue,
grids, batches) and create the destination from those alone.

**The interface** (`casa_ms::observation_io::convert`):

- `to_msv4(source, destination, options)` and `to_msv2(…)` take either
  store. `ConversionOptions` names the data groups (default every group,
  `base` first), the accepted losses, the batch budget, MSv4 workers,
  chunks, codec and provenance. A `ConversionReport` returns the losses
  taken, partitions, batches, samples, and the MSv2 rows or MSv4 elements
  written.
- **Losses are inventoried and refused before anything is written.** A
  `Loss` names what the destination cannot hold and its `LossKind`
  (`Sigma`, `RowFlags`, `FlagCategories`, `DataGroups`, `Tables`,
  `Columns`) is what a caller accepts:
  - from an MSv2: `SIGMA`/`SIGMA_SPECTRUM`, any `FLAG_ROW` set, held
    `FLAG_CATEGORY` cells (every row is read, 256 at a time, so at most
    about 100 MB of held cells at once; that costs about 60 ms per 2
    million rows over reading 4,096 at a time, as measured on a simulated
    MS of 2,016,000 rows), MAIN columns outside the boundary's roles, an
    `ARRAY_ID` or `PROCESSOR_ID` above 0 (a negative id names nothing:
    casacore's UVFITS filler writes `PROCESSOR_ID` −1), the `WEIGHT`
    summary beside `WEIGHT_SPECTRUM`, subtables the catalogue does not
    carry (system calibration, weather, flag commands, history,
    Doppler, frequency offsets, sources, and any further table a keyword
    names), the `POINTING` content the destination does not hold
    (planned before writing: "Geometry inputs"), `FEED` receptors that
    vary in time (the last row is kept), a `PROCESSOR` of more than
    one row or of one that is not a
    correlator, and `STATE` rows that say more than an intent (a sub-scan,
    calibration or load, a reference or non-signal row);
  - from an MSv4: sub-datasets other than antennas, fields and pointing,
    and variables the writer does not write;
  - in either, unselected data groups;
  - into an MSv4, antennas no partition's baselines name
    (`Loss::UnusedAntennas`, below);
  - into an MSv2, `SIGMA` itself (`Loss::SigmaDerived`, below).

  Columns of the subtables the catalogue does carry (an antenna's offset,
  a field's delay direction) are not inventoried. Fields whose direction
  moves are refused in either direction: those that follow an ephemeris
  (`EPHEMERIS_ID` ≥ 0) and those whose direction is a polynomial in time
  (`NUM_POLY` > 0, which XRADIO refuses too), since the catalogue would
  hold an offset or a constant term as the phase centre. Into an MSv4
  only the fields MAIN's rows name are checked, as XRADIO checks the
  fields it converts; into an MSv2, which writes every `FIELD` row, every
  row is.
- **Lockstep groups.** A partition's selected groups are read by one
  traversal each, sized to the same whole times per batch (the first with
  every role, the others their visibilities only), so a destination writes
  every variable of a time range together (`creation/lockstep.rs`).
  Batches align to the destination's chunk or, for an MSv2, to the
  source's natural time block.

**To MSv4.** One dataset per partition in one processing set
(`Msv4ProcessingSetWriter`: one staging directory, datasets written
concurrently, published by one rename that refuses to replace). Further
groups are `VISIBILITY_<GROUP>` variables beside one `FLAG`, `WEIGHT` and
`UVW`. Each dataset's `antenna_xds` lists the antennas its baselines
name, as XRADIO writes it, and an antenna no partition names is reported
(`Loss::UnusedAntennas`); `relocatable_antennas` is set only for the
telescopes whose antennas XRADIO renames, by the dataset's telescope as
XRADIO sets it (names follow the first session's telescope, below, so in
an MSv2 whose sessions name different telescopes the two can disagree). Both converters
list a dataset's antennas in the MSv2's `ANTENNA` order, so the MSv4
reader merges the datasets' lists keeping every one's order (a
topological merge, the antenna first seen first among those free to
come next): an MSv2 written back numbers its antennas as the original
did whenever the datasets determine the order, XRADIO's stores
included, and no baseline of either converter's store comes back
reversed (both of a baseline's antennas are in one list). Each
`antenna_xds` is read once, and checked there; opening 3,001 datasets
took as long as before the merge (24.0–24.9 s against 24.2–24.8 s, a
debug build; in a release build 4.89–5.42 s against 4.96–5.53 s, where
reading every `antenna_xds` twice had taken 6.37–6.61 s), and each
dataset's baselines resolve their antenna names by hash rather than by a
scan of its antennas. An MSv4's datasets keep their names. An MSv2's
datasets are named as XRADIO names them: the file name without `.ms`,
then the partition's number in the order the partitions' first MAIN rows
appear (XRADIO groups with `sort=False`), padded with zeros to the width
of the largest (`Msv2Observation::first_row`). Its names are XRADIO's
too: antennas `NAME_STATION` for the telescopes it renames (ALMA, VLA,
NOEMA and EVLA, judged by the first observation's telescope where XRADIO
judges each partition's; others keep their names), fields `NAME_FIELDID`,
sources `NAME_SOURCEID` by the field's `SOURCE_ID`, and spectral windows
`NAME_SPWID` by the partition's `SPECTRAL_WINDOW` row (`spw_ID` when
unnamed), which the MSv2 adapter now reports
(`Msv2Observation::spectral_window_id`). XRADIO names a source it finds
no `SOURCE` row for in the window `Unknown` (or `Unknown_SOURCEID` in a
partition of several fields); this converter names it from the field
when another window's `SOURCE` row names it, and writes `Unknown` when
none does. The MSv4 reader reads both placeholders as no source. Two
partitionings differ from XRADIO's: the MSv2 adapter splits partitions
by feed pair as well, and gives a row with `STATE_ID` −1 no intents
where XRADIO gives it the last `STATE` row's (NumPy's index −1). The
first makes more datasets here; the second can, or can give the same
datasets other intents. Either way the datasets numbered after the
first difference can hold other data than XRADIO's of the same name.
Partitions are converted `workers` at a time.

**To MSv2.** A new MeasurementSet through production's creation path:

- subtables from the catalogue: an antenna and a feed per antenna, a field
  per field, a source per source in each spectral window, an observation
  per session whose `TIME_RANGE` spans its integrations edge to edge,
  polarizations whose products name receptors by their place in the
  feeds (which must agree), and
  spectral windows, polarizations, data descriptions and states shared by
  partitions of equal content (`creation/msv2_tables.rs`). A data
  description is shared only by partitions whose state or observation
  differs, since an MSv2 keys partitions by data description, state and
  observation and would read two alike back as one.
- MAIN rows through the variable-shape creation writers
  (`MeasurementSetWritePlan::variable_array_creation`), one hypercube per
  visibility shape, partition after partition, in time and baseline order.
  `base`, `corrected` and `model` become `DATA`, `CORRECTED_DATA` and
  `MODEL_DATA`; another group name is refused.
- **Names are the MSv4's.** An MSv4 made from an MSv2 holds XRADIO's names
  (above), so the MSv2 converted back from it says them: a round trip
  renames field `trial` to `trial_0` and antenna `W08` of a VLA to
  `W08_W08`, and a second round trip suffixes again, as XRADIO's
  conversion of the round-tripped MSv2 would. The converter writes what
  the MSv4 says rather than guessing which suffixes XRADIO added; a CASA
  script that selects by the original names needs the MSv4's (finding 8).
- **Padding, as decided in D23** (`revision-2-design.md`). A sample whose
  every weight is non-finite is padding (section 3's one mark), and an
  acquisition is everything else. By default every grid cell becomes a
  row, and a padded cell a row flagged throughout with NaN data, weights
  and UVW (`FLAG_ROW` stays false: its flags already exclude it, and a
  reader folding `FLAG_ROW` in reads the same); its keys stay valid (the grid's interval, exposure 0, time
  centroid at its time), so the MSv2 reopens, and it reads back, and
  converts back, as padding. A conversion that must reproduce an original
  sparse row set declares it (`PaddingRows::Omitted`): padding then
  becomes no row. An MSv2's row count is fixed before its rows are written
  and the batch carries no presence mask, so that policy first reads the
  first group's weights to count each partition's acquisitions (finding
  5); the default takes the count from the grids.
- **`SIGMA` is derived** as CASA's `mstransform` derives it when corrected
  data become data (`weightToSigma`): `1/√w`, and −1 where `w` is not
  positive. This is reported as `Loss::SigmaDerived` and must be accepted,
  since no batch carries sigma; right for weights never recalibrated,
  wrong for the others (finding 6).
- Refused before anything is written: times in a scale other than UTC,
  feeds other than 0, units that differ between partitions of one column,
  fields in a frame other than J2000, a scan without a casacore number,
  feeds whose receptors differ, a correlation product the feeds cannot
  name (a Stokes product, or a receptor they lack), a data group selected
  twice, a source with nothing to write.
  Weights that vary over channels (an MSv2 `WEIGHT` holds one per
  correlation and no `WEIGHT_SPECTRUM` is written) are refused by the
  counting pass when there is one, and otherwise when reached. The MSv2 is
  written by one thread into a staging directory renamed into place, and
  removed on failure, so a refusal publishes nothing.

| Law | What it shows |
| --- | --- |
| `an_msv2_converts_to_the_msv4_xradio_makes_of_it` | Refused until `SIGMA` is accepted, with nothing written; then losses exactly `SIGMA`, `HISTORY` and `POINTING`'s `INTERVAL`, `NAME`, `TARGET` and `TRACKING`; every sample equal to the MSv2's and to XRADIO's own conversion; XRADIO's names. |
| `partitions_and_data_groups_convert_together` | Two windows of four and three channels and two groups become two datasets with `VISIBILITY_MODEL`; every group reads back equal; whole five-time chunks hold nothing; leaving `model` out is refused as a loss. |
| `an_msv4_converts_to_an_msv2_of_every_acquisition` | XRADIO's store to an MSv2: refused until `SIGMA` is accepted, 120 rows, every sample equal to XRADIO's and to the original MSv2's, `SIGMA` = `1/√WEIGHT` (−1 where not positive), and antennas, fields (source names included), scans, observations, windows, products and intents equal to XRADIO's catalogue. |
| `an_msv2_survives_the_round_trip_through_msv4` | The two-window MSv2 to MSv4 and back: the original's row count, `DATA` and `MODEL_DATA` of both partitions equal; fields and antennas named as the MSv4 names them (`NAME_FIELDID`, `NAME_STATION`). |
| `padding_becomes_a_flagged_row_or_none_as_declared` | An MSv2 missing a row is padding in the MSv4. By default the MSv2 made from it has 120 rows: the padded one flagged throughout with NaN data and weights, centred on its time, the grid's interval and exposure 0; it reads back as padding and is padding again in an MSv4 made from it. Declared `Omitted`, it has the original's 119. Every acquisition equal throughout. |
| `weights_varying_over_channels_are_refused` | An MSv4 whose weights vary over channels is refused naming `WEIGHT_SPECTRUM`; nothing is created. |
| `partitions_of_one_window_share_its_name_and_rows` | An MSv2 whose times fall in two sessions has two partitions of one window; both keep the window's one name (`_0`, its row) in the MSv4, and come back as one `SPECTRAL_WINDOW` row and one data description, the two `OBSERVATION` rows each spanning its own times; every sample equal. |
| `keys_and_subtable_rows_the_destination_drops_are_losses` | An `ARRAY_ID` of 1 is refused as a loss until columns are accepted; then the report names `ARRAY_ID` and a `PROCESSOR` of two rows. |
| `moving_fields_are_refused` | A field with an `EPHEMERIS_ID`, and one with `NUM_POLY` 1, are refused as unsupported; nothing is created. |
| `datasets_are_numbered_as_their_first_rows_appear` | A second session holding the earlier times is dataset `ms_trial_0`, as XRADIO numbers it, though its partition key sorts second. |
| `a_round_trip_keeps_the_antenna_order` | The first dataset lacks a middle antenna the second names; the MSv2 converted back has the original's `ANTENNA` order and no row with `ANTENNA1` > `ANTENNA2` (first-appearance order, before the merge, put that antenna last). |
| `an_unused_moving_field_is_refused_only_where_it_is_written` | A `FIELD` row with an `EPHEMERIS_ID` that no row names converts to an MSv4 and is refused into an MSv2, which would write it as a fixed direction. |
| `processors_are_lost_only_when_named_or_not_a_correlator` | `PROCESSOR_ID` −1 in every row is no loss; a lone `PROCESSOR` row of type `RADIOMETER` is one. |
| `antennas_no_baseline_names_are_a_loss` | An antenna (with its feed) that no row names is reported as `UnusedAntennas(1)` and is in no dataset's `antenna_xds`. |
| `a_flag_category_held_in_any_row_is_a_loss` | A `FLAG_CATEGORY` cell held in a middle row only is found (every row is read, in chunks, the first and last first) and refused until flag categories are accepted; nothing is created. |
| `only_the_telescopes_xradio_renames_get_station_names` | The fixture's antennas, renamed `NAME_STATION` as a VLA's, keep their names when the telescope is MeerKAT. |

**Measurements on `pilot.ms`** (Mac mini `bg-mac-mini`: Apple M2, 8
cores, 24 GiB, macOS 27.0.1, rustc 1.99.0; data on an external USB SSD,
APFS; release build, page cache warm unless noted; `378b472` unless
noted). `pilot.ms` has 2,021,760 rows in 64 partitions over 32 spectral
windows of 64 channels and 63 data descriptions, 4 products, 4.1 GB.
Commands: `ms_trial_convert`, `ms_trial_read_passes`,
`ms_trial_update_pass` (examples of `casa-ms`), and
`tools/ms-trial/check_convert.py` and `check_v2_readback.py`.

| Run | Wall | CPU (user + sys) | Peak RSS | Output |
| --- | --- | --- | --- | --- |
| MSv2 → MSv4, 1 worker, Blosc-LZ4, 10-time chunks | 22.3–22.5 s | 6.2 + 1.9 s | 203 MB | 1.6 GB |
| the same, 8 workers | 12.2–13.1 s | 10.1 + 7.0 s | 1.09 GB | 1.6 GB |
| the same, 1 worker, cold cache (`31c7456`) | 31.8 s | 7.9 + 3.1 s | 226 MB | 1.6 GB |
| 1 worker, Zstandard (`31c7456`) | 18.8 s | 10.5 + 2.4 s | 214 MB | 1.1 GB |
| 1 worker, uncompressed (`31c7456`) | 32.8 s | 2.9 + 3.3 s | 206 MB | 6.4 GB |
| XRADIO's `convert_msv2_to_processing_set` (`partition_scheme=[]`, no pointing) | 27.3 s (39.0 s with interpreter start) | 20.2 + 6.0 s | 1.33 GB | 1.6 GB |
| MSv4 → MSv2 (one writer thread), padding flagged (the default; D23 commit) | 15.8–19.6 s | 7.0 + 0.9–1.2 s | 148 MB | 4.0 GB |
| the same, padding omitted (counting pass first; D23 commit) | 17.0–17.2 s | 7.8 + 0.9 s | 146 MB | 4.0 GB |
| Update pass, MSv4: rewrite `base` visibilities | 17.6 s (23.5 s first) | 7.0 + 0.9 s | 149 MB | 576 chunks replaced, none read |
| Update pass, MSv2: the same | 24.6 s (37.6 s first) | 8.2 + 2.4 s | 386 MB | 2,021,760 cells |

- **Interoperability at scale.** XRADIO's schema check passes on the
  converted processing set, and every one of its 64 datasets equals
  XRADIO's own conversion of `pilot.ms`, every payload variable and
  coordinate exactly (`check_convert.py compare`). casacore (CASA 6.7.5.18's
  casatools) opens the MSv2 converted back and every one of its 2,021,760
  rows equals the original's, matched per spectral window on time and
  baseline: `TIME`, antennas, fields, scans, `DATA`, `FLAG` (row flags
  folded in), `WEIGHT`, `UVW`, `TIME_CENTROID`, `INTERVAL`, `EXPOSURE`;
  its derived `SIGMA` equals the original's exactly, since `pilot.ms`'s
  weights are `1/σ²` (`check_v2_readback.py`).
- **Rechecked after the review fixes (`d5d0bce`).** Every dataset now
  also matches XRADIO's in name (`pilot_00` … `pilot_63`, numbered by
  first row), antennas (each dataset's 27 of the 54) and fields; the 64
  source names differ only where XRADIO, finding no `SOURCE` row for the
  window, says `Unknown`. casacore reads back every row as before and the
  1,888 subtable rows the matched rows point at, equal to the original's:
  each spectral window's channels and reference, each polarization's
  `CORR_TYPE` and `CORR_PRODUCT`, each field's `PHASE_DIR`, each
  antenna's position and diameter, each session's telescope and a
  `TIME_RANGE` covering its rows; and each field has a `SOURCE` row in
  every window it is observed in. The losses are unchanged. The fixes cost
  nothing measurable: MSv2 → MSv4 with one worker took 31.5–32.6 s and
  7.5–8.0 s of user CPU both before the first round of fixes (`4a402ac`)
  and after it (`30fab54`), run alternately on the Mac mini while system
  indexing loaded it (load average 7–9, so slower than the 22 s above);
  at `d5d0bce` it took 7.6 s of user CPU, and MSv4 → MSv2 15.1 s.
  At `64668a3` (the reader's antenna merge) the same checks pass, and
  XRADIO's own processing set, converted to an MSv2 by `to_msv2`, reads
  back through casacore equal to `pilot.ms` too, row for row and in the
  same 1,888 subtable rows: its datasets hold 27 of the 54 antennas each,
  and the merge numbers them as `pilot.ms` does (the read-back pairs rows
  by antenna id). MSv4 → MSv2 from XRADIO's store took 20.7 s.
- **The converters are write-bound.** Writing to the external disk
  (about 200–250 MB/s) sets the pace: one worker spends about 8 s of CPU
  in a 22 s conversion, and the uncompressed run, with the least CPU, is
  the slowest because it writes four times the bytes. The MSv2 created is
  4.0 GB in 16 s.

**Findings.**

1. **XRADIO names a spectral window by its MSv2 row, not by partition.**
   The first `to_msv4` suffixed each window with its partition's index,
   which equals XRADIO's `SPECTRAL_WINDOW` id only when every window is one
   partition; `pilot.ms` has two partitions per window. Fixed (above); the
   scale check above, which had called `pilot.ms`'s 64 partitions 64
   spectral windows, now says 32.
2. **"Concurrency target 1" was not one thread (E1).** zarrs 0.23.14
   encodes and decodes `min(chunk_concurrent_minimum, chunks)` chunks of
   one access at once on rayon's global pool whatever the target, and the
   minimum defaults to 4; building its options also starts that pool (one
   thread per core, 8 here). Every MSv4 read, write and update now uses
   one `codec_options()` with both set to 1, so a codec runs on the
   calling thread and parallelism is the caller's workers. The pool still
   starts, idle: one worker's process has 10 threads, four workers' 13,
   eight workers' 17 (an MSv2 read, 2), and one worker's three read passes
   spend 7.8 s of CPU in 7.9 s. The cost of no longer borrowing those threads:
   a one-worker read pass of the converted `pilot.ms` went from 1.75 s to
   2.49–2.59 s and four workers from 0.72–0.77 s to 0.82–0.86 s; eight
   workers take 0.64 s; conversion with eight workers is unchanged and
   with one about 2 s slower (19.5–20.7 s before, with 10.4 s of CPU).
   In-partition parallelism comes back by giving workers disjoint parts
   of a partition: a time range in the request
   ([#910](https://github.com/bglenden/casa-rs/issues/910), section
   "Time selection" below), or an admitted pool zarrs runs inside.
3. **Retention plateaus (R4).** Thirty read passes of every partition in
   one process: peak RSS 138.87 MB after the first and 138.95 MB after
   the thirtieth with one worker, 518.85 and 519.34 MB with four.
   `zarrs_filesystem 0.3.14` keeps one lock per key it has touched, never
   removed: bounded by the store's keys (6,977 files here), so it grows
   with the distinct keys a process visits, not with passes. Before
   finding 2's change the one-worker peak crept from 163.2 to 165.4 MB
   over 20 passes, which the busy rayon threads explain. `zarrs_filesystem`
   is now pinned (`=0.3.14`) beside `zarrs`.
4. **Synchronizing each replaced chunk costs about a fifth of an update
   pass.** An MSv4 update replaces each chunk through a temporary file
   synchronized before its rename, and Rust's `sync_all` on macOS is
   `F_FULLFSYNC`. Replacing 576 files of 2.9 MB (the pass's chunk count
   and mean size) on the same disk took 6.9–7.9 s with a full
   synchronization each and 4.0 s with one at the end, so about 3–4 s of
   the 17.6 s pass. Its 64 finishes, which write each partition's last
   chunk-row and synchronize the directories, took 3.2 s. That is the price of each chunk being old or new after a power
   loss; workers updating disjoint partitions would overlap it.
5. **Omitting padding needs occupancy up front.** The creation plan fixes
   the row count, and padding is known only from the weights, so with
   padding omitted `to_msv2` reads every weight twice: about 0.9 s of CPU
   on `pilot.ms` (7.8 s against 7.0 s), lost in the disk's pace here. D23's
   default, every cell a row, needs no such pass. A presence role in the
   batch (`contracts.md` section 8, "Occupancy") would reduce the pass to
   a read of the mask alone.
6. **`SIGMA` into an MSv2 is a policy, not a mapping.** The architecture's
   loss table asks that `SIGMA` not be rebuilt as `1/√WEIGHT` without a
   justified relation. But `SIGMA` is a required MAIN column (casacore's
   `MeasurementSet::requiredTableDesc`), so every row needs a value, and an
   MSv4 holds no sigma: refusing would make every MSv4 unconvertible, and a
   sentinel such as −1 would make tools that weight by `1/σ²` (CASA's, and
   production `gaincal`) treat every sample as unusable. The converter
   applies CASA's own rule for corrected-to-data and makes the caller
   accept it. The owner kept this as the interim rule (2026-10-26), to be
   revisited with step 4's `SIGMA` weight role. **Revisited:** with the role
   in place the rule is consistent: 1/σ² of the derived `SIGMA` is the
   MSv4's weight to rounding (checked in
   `an_msv4_converts_to_an_msv2_of_every_acquisition`), and a weight that is
   not positive becomes σ = −1 and so weight 0, so a solve weighted as
   CASA's weighs the converted MSv2 as it weighs the MSv4 under
   `SigmaFallback::StoredWeights`. What stays a loss is information the MSv4
   never held: a sigma that differed from 1/√weight.
7. **Fast creation and per-element flags.** Production's fast creation
   stream (one shape, background writer) takes a row flag per row and no
   per-element `FLAG`, `CORRECTED_DATA` or `MODEL_DATA`, so `to_msv2` uses
   the variable-shape writers, which the simulator calls several times
   slower. Here the disk, not those writers, sets the pace (8 s of CPU in
   16 s).
8. **A round trip renames.** XRADIO's names are suffixes on the MSv2's
   (field `NAME_FIELDID`, antenna `NAME_STATION`, window `NAME_SPWID`,
   source `NAME_SOURCEID`), and `to_msv2` writes the MSv4's names, so
   MSv2 → MSv4 → MSv2 renames every field, window and source (and a VLA's
   antennas), and repeating it suffixes again. Undoing a suffix would
   guess at what XRADIO added (a native MSv4 field may be named `x_0`), so
   the names stay the MSv4's. **Owner decision (2026-10-27):** keep the
   MSv4's names for now; full round trips should be rare outside testing,
   and stripping the suffixes can be reconsidered if it proves necessary.

**Not done:** an MSv2 `WEIGHT_SPECTRUM` or `FLAG_CATEGORY` written; feeds
other than 0; the MSv4 `processor_info` (written empty, where XRADIO
writes the `PROCESSOR` row's type and sub-type, so a lone correlator's
sub-type is not carried; `to_msv2` writes one correlator); fields that follow an ephemeris or whose direction is a
polynomial in time, refused in either direction since the MSv2 adapter's
catalogue would hold an offset or a constant term as the phase centre; MSv2 subtable rows beyond the catalogue (an MSv2 → MSv2
conversion drops `SOURCE` detail, `HISTORY` and `POINTING`'s `NAME`,
`TARGET` and `TRACKING`, as reported);
a loss inventory at subtable-column level; a v4 → v2 reference converter
to compare with (XRADIO has none; casacore read-back stands in); MSv2
creation from parallel workers.

## Time selection: workers split a partition by time

Step (4)'s first item ([#910](https://github.com/bglenden/casa-rs/issues/910)):
a request can select times, so workers can share one partition without a
hidden pool (converter finding 2). `AccessRequest::times` is every grid time
or those within one half-open range of epochs, stated in the partitions' own
time origin and scale (contracts, section 4), so selecting a time rounds
nothing; `TimeRange::of_grid` states a slice a planner cut at grid indices.
The estimate gives the selection's times and its first (`first_time`); the
MSv2 adapter reads the selected rows from its opening index, the MSv4 adapter
the chunks the selection touches; the MSv2 updater plans and orders its
writes over the selection, and the MSv4 one already addressed times by value.

Groups keep the keys they have over the whole grid. A traversal delivers
and closes the part of each group its selection holds and marks a group the
selection cuts (`GroupFragment::clipped`). Workers that split a request plan
the slices together: `TimeSplit::plan` takes consecutive cuts, checks that
the slices tile the request in every partition, and counts from the store's
group keys (`Observation::group_keys`) the groups more than one slice holds.
The slices share one completion (`SplitCompletion`, a `SliceCompletion`
handle per slice), which makes a divided group ready once, after every slice
has closed its part and all the work begun on it has finished, whatever
order the slices run in. #910 first proposed a shared completion that
counted closings only, so slice 2's closing fragment could make the group
ready before slice 1's work on it had begun; the owner chose the shared
completion as the cleaner interface for application programmers, and it
counts parts as well as work. `clipped` stays for a traversal of one range
on its own.

| Law | What it shows |
| --- | --- |
| `time_slices_tile_a_partition_sample_for_sample` | On both stores, slices tiling the twelve times (one; two; three uneven; one per time) deliver exactly the whole traversal, each sample once; each slice's estimate gives its times, first time and batches. |
| `groups_keep_their_keys_and_close_within_each_slice` | Cut at every time, each slice's group parts have the whole traversal's keys, are positioned and closed within the slice, add up to the whole's groups, and are clipped exactly when the cut divides the group. |
| `aligned_slices_decode_no_unit_twice` | Slices aligned to the natural time blocks add up to the whole traversal's decode units, modelled and counted; an MSv4 slice that splits a chunk decodes it in both. |
| `empty_ranges_are_empty_and_malformed_ones_refused` | A range between two times, past the end or before the first is an empty traversal; one reversed, with a NaN end, or in another scale or origin is refused at binding. |
| `a_sliced_traversal_writes_back_its_times` | On both stores an updater writes a slice's flags; the slice's samples change and no other's. |
| `a_sliced_write_back_passes_partitions_the_range_leaves_empty` | The fixture relabelled as three scans and partitioned by them: a range over the first two leaves the third empty, listed between them or first; the MSv2 updater passes it and the two partitions write back (independent review B1). |
| `an_updater_of_a_slice_refuses_times_outside_it` | On both stores an updater bound to a slice refuses a batch from before it. |
| `an_updater_refuses_a_partition_before_the_last_is_written` | The MSv2 updater refuses a later partition's batch while an earlier partition's selected times are unwritten. |
| `a_time_split_finalizes_each_group_once_after_all_its_parts` | On both stores, grouped by time bins and by scan, cut several ways, the slices run on a thread each and in reverse order: each of the whole traversal's groups becomes ready exactly once, with all its samples, and the split names as divided the groups more than one slice holds. |
| `a_time_split_must_tile_the_request` | Cuts with a gap or an overlap, starting after the request's first time, ending before its last, or missing are refused before anything is read. |
| `a_split_writes_back_its_slices_at_once` | On both stores the slices of a split write back at once, an updater each on its own thread, and leave the store as one updater over the whole traversal does. |
| `live_updaters_claim_disjoint_times` | Live updaters of one session claim disjoint times of each variable: any cut on MSv2, whole chunk rows on MSv4; a slice sharing times or an MSv4 chunk row with a live updater is a `Conflict` until that updater is gone. |

Unit laws: `a_time_range_selects_the_grid_times_it_holds`,
`ranges_of_grid_indices_select_those_times` and the split's own
(casa-observation), and
`a_selection_closes_the_parts_it_holds_and_marks_cut_groups` (the shared
group index). For an MSv2 a decode unit is a row, so the decode law there
says each row is read once, not that a tile is; for an MSv4 it counts
chunks. Under deferred closure a slice's `GroupEnd` for a clipped group ends
the slice's part, not the group.

Planning cost (Astra's review): the group index of a whole traversal is the
same map it was before the time selection (built once, no clipped set), and
a slice builds one map of its own groups and the set of those it cuts. On
200,000 per-integration times, release build, medians of nine: the index
before the time selection 50.6–53.2 ms, now 51.7–54.2 ms for the whole grid
and 28.9 ms for a quarter slice (an earlier revision took 54–78 ms by
building two maps). The MSv2 estimate counts the selection's rows only for
a slice, using the opening's count for the whole grid.

**Measurement** (`ms_trial_read_passes --split partitions|times [--only I]`
on `pilot.ms` converted to MSv4: 64 partitions of 90 times in 10-time
chunks; Mac mini, release build, three passes; each slice a whole number of
chunks, spread as evenly as they go):

| Read | Workers | By partition | By time slice |
| --- | --- | --- | --- |
| Every partition | 1 | 2.48 s, 139 MB | 2.49 s, 138 MB |
| | 4 | 0.82 s, 519 MB | 0.81 s, 220 MB |
| | 8 | 0.60 s, 1.03 GB | 0.58 s, 269 MB |
| One partition (`--only 0`) | 1 | 0.039 s | 0.039 s |
| | 4 | 0.039 s | 0.016 s |
| | 8 | 0.039 s | 0.014–0.016 s |

Both ways decode the same 3,456 units (54 for one partition), so aligned
slices decode nothing twice. Over the whole store the two splits take the
same time, but workers sharing partitions hold two fifths of the memory
with four workers and a quarter with eight;
within one partition, which the partition split cannot share, slices of
nine chunks run 2.4–2.8 times faster with four or eight workers. The
one-worker pass with zarrs' hidden pool took 1.75 s (finding 2); four
workers on slices now take 0.81 s, every codec on a worker's own thread.

**Updaters of a split.** Each slice writes back through its own updater,
live at once. An updater claims the (partition, variable, times) it writes
when it is made and releases them when dropped; one that overlaps a live
claim is a `Conflict`; a slice of no times claims nothing. An MSv4 updater
claims whole chunk rows, since it replaces whole chunk objects, so its
slices are cut at chunk rows; each replaces through a store of its own, so
its report counts its own chunks and its finish synchronizes the
directories its own renames changed. The MSv2 updaters write one batch at
a time, each batch a mutation of its own, under casacore's write lock on
MAIN, which the session holds while any updater is live: another process
waits once for a pass, not at every batch (a casacore `AutoLocking` reader
gives a lock up only at its next inspection), and the session reopens its
handle whenever it takes the lock, so a merge never reads a table older
than the lock (independent review, finding 1). A slice that ends without
completing its parts, or whose view is dropped, abandons their groups:
the other slices and the split report `WorkAbandoned` rather than wait
(finding 2). On `pilot.ms` (Mac mini, release, update pass, fresh copies,
alternating) the MSv2 pass took 36.75 and 36.72 s against main's 36.38 and
38.86 s and per-batch locking's 36.67 and 37.09 s, RSS 386–389 MB in each;
reopening the MeasurementSet when the lock is taken costs about 0.27 s of
CPU per pass (8.5 s user against 8.2 s).

**Not done:** slices of a partition read concurrently by one cursor.

## Field selection: imaging's selections on the request

Step 2 of the imaging port (#932, `docs/measurement-set/imaging-port.md`
§2.3, option (a)): the imager selects fields, UV distances and intents per
MAIN row today. On batches, field and intent are per time, and UV distance
is per sample.
- `AccessRequest::fields` reads every field, or the whole times phased to
  some fields (`FieldSelection::Only`). With the time range it resolves at
  binding to runs of consecutive grid times (`TimeRuns`, the estimate's
  `time_runs`).
- A batch holds whole times of one run, so its times stay consecutive on
  the grid and an updater addresses them as before.
- The MSv2 adapter reads only the selected rows. The MSv4 adapter reads the
  chunks each batch touches, so a chunk row holding several runs is decoded
  once per batch, with its gap times (contracts, section 4; #935).
- Intents need no request field: the adapters partition by them (as
  XRADIO's converter does), so a partition's samples share their intents
  and a consumer selects intents by choosing partitions. Two differences
  from CASA:
  - CASA matches an intent pattern against the whole `OBS_MODE` string;
    the catalogue keeps a partition's intents split, sorted and
    deduplicated, so a pattern tied to an intent's place in the string
    can select otherwise when matched intent by intent.
  - Selection by `STATE_ID` has no counterpart: the states of one
    `OBS_MODE` share a partition.
- UV distance stays with imaging, tested per sample as it packs (step 4).

Group keys are taken over the selected fields' times, as production
`gaincal` bins only the rows it selects. A group keyed by field is held
whole or not at all. A group combining fields holds the selected fields'
times alone, with bins counted from the earliest of them (production
`gaincal` counts from the lowest-numbered field's first time: #936), so a
field selection cuts no group and marks nothing `clipped` (a time range
still does). Time splits tile the selected
runs, and updaters claim and write them. An update session merges each
updater's runs per variable and checks claims in time order, so a selection
of many short runs claims cheaply.

| Law | What it shows |
| --- | --- |
| `a_field_selection_delivers_whole_times_of_its_fields` | On both stores (a simulated three-field mosaic, one integration per field in turn, and its MSv4), each field selection, alone or with a time range, delivers exactly the whole traversal's samples at its fields' times; no batch holds an unselected field; the estimate counts the selected times and batches. A field the partition never visits selects nothing. |
| `a_field_selection_decodes_no_unselected_time` | A one-field selection decodes a third of the units, as modelled: MSv2 rows, MSv4 one-time chunks. In six-time chunks a selected time decodes its whole chunk: as many units, six times the elements. |
| `a_field_selection_keys_groups_over_its_fields_and_splits_tile_it` | Under a field selection, 65 s bins combining fields count from the first selected time (five times and three, where bins from the grid's first time would hold four and four) and none is clipped; a time split of the selection tiles it and names the one bin it divides. |
| `a_field_selected_traversal_writes_back_its_times` | On both stores an updater bound to a field selection flags that field's times and no other. |

Unit laws: `a_field_selection_leaves_runs_of_whole_times` and
`blocks_stay_within_runs` (casa-observation),
`a_field_selection_keys_and_closes_groups_within_its_fields` (the shared
group index) and `claims_merge_runs_and_meet_only_held_times` (an update
session's claims).

## Geometry inputs: what imaging geometry reads beside the batches

Step 3a of the imaging port (#932, `docs/measurement-set/imaging-port.md`
§2.2.1): today's per-row imaging geometry reads inputs the boundary did not
carry. Now both adapters carry them and the converter carries them both
ways:
- **Field reference epochs and per-row frames.** A field read from an MSv2
  carries `FIELD.TIME` as its reference epoch (`FieldEntry::reference_epoch`).
  `PHASE_DIR` with a variable reference column (`PhaseDir_Ref`, as ALMA
  writes it) resolves per row; fields in different frames are refused, since
  a data group has one UVW frame and MSv4 one phase-centre frame. An MSv4
  field has no reference epoch.
- **Pointing**, as a query, not catalogue content: `Observation::pointing`
  returns a partition's pointing series over a span of times within a byte
  budget, each antenna's samples within it and its nearest either side, and
  `PointingSeries::bracket` evaluates the samples around one (antenna, time)
  there (GLOSSARY, "Pointing Bracket"). MSv2 reads `POINTING` through today's
  imaging reader (`PreparedSelectedPointingCatalog`), so its brackets are
  that reader's, bit for bit but for one tie (below). That reader's covering
  rule is casa-rs's, not casacore's `pointingIndex` (#948). MSv4 reads
  `POINTING_BEAM[time_pointing, antenna_name, local_sky_dir_label]`
  (XRADIO's layout, `time` accepted for `time_pointing`, an `n_polynomial`
  of one accepted and more refused, since XRADIO defines no origin for
  them), in `fk5`, `icrs` or `altaz`. Directions stay in the store's frame;
  converting them is the geometry owner's.
- **Receptors per partition** (`PartitionEntry::receptors`), no longer per
  antenna: an MSv2's `FEED` rows as casacore's `MSIter` and XRADIO's
  converter select them (the window's own rows once any row names a window,
  else the rows for every window), an MSv4 dataset's `antenna_xds`. Of rows
  of one antenna, feed and window that differ, the last in table order is
  read, as casacore's `MSIter` reads it, and a conversion reports them as a
  `FEED` column loss (owner decision on #946, 2026-10-28). An MSv2
  destination writes one `FEED` row per antenna where every
  antenna states the same receptors in every window it appears in, and
  otherwise a row per window it states receptors in, so that casacore reads
  each back; a window stating none (ALMA's WVR window) then has no row, as
  ALMA writes `FEED`, and rows of no receptors are written only when no
  antenna states any. An MSv4 destination omits the receptor
  variables of a dataset whose source has no `FEED` row for it, as XRADIO's
  converter does for ALMA's WVR window, although XRADIO's schema marks
  `polarization_type` and `receptor_label` required (the simulator writes
  window 0's rows only: #943).

What a conversion carries of the pointing is planned before anything is
written (`creation/pointing_plan.rs`):
- **Into an MSv4**, each partition's rows within its times (as XRADIO takes
  rows within each dataset's times) and each antenna's nearest either side,
  each as its direction at its own time. MSv4 states no interval or target
  and keeps one polynomial term, so `INTERVAL`, `NAME`, `TARGET`,
  `TRACKING`, further polynomial terms, rows at the antenna and time of an
  earlier one, and rows beyond every partition's times and each antenna's
  nearest are reported losses, counted by one scan of `POINTING`. Pointing
  in several frames or a frame MSv4 does not name is lost whole, as a
  reported loss, rather than refused part way through writing.
- **Into an MSv2**, every row of the partitions' antennas, in (time,
  antenna) order: an MSv2 source's antennas each read once, grouped so that
  each read fits the budget, an MSv4 source's datasets whole (a sample two
  datasets hold written once). An MSv4's samples become rows of `INTERVAL`
  0 with `TARGET` repeating the direction. Rows of antennas no partition
  names are a counted loss.
- Each read is checked against the batch budget with the catalog's own
  bound (`catalog_peak_bytes`, the sum of every check the reader makes), and
  an MSv2 destination's gathered rows with the writer's; beyond it the
  conversion is refused before anything is written. Rows a read would
  refuse (more than four polynomial terms, times that are not finite) lose
  the pointing whole.
- The MSv4 reader finds the antennas' nearest samples beyond the requested
  times together, reading every antenna's directions outward in blocks of
  4,096 times that double up to 16,384, so each chunk is decoded once.

**Cost, measured** (Mac mini, `ms_trial_convert`, batch budget 256 MB,
casatestdata's ALMA `CAS-7841.ms`: 12 partitions, 193,050 samples,
`POINTING` of 1,830,984 rows, a per-row `PHASE_DIR` frame, which the
converter refused before step 3a):

| Direction | With `POINTING` | `POINTING` emptied | Pointing carried |
| --- | --- | --- | --- |
| to MSv4 | 21–26 s, 7.4 s user, 250 MB | 13.3 s, 0.7 s user, 121 MB | 250,330 rows into 12 datasets; 1,580,654 beyond every partition's times and each antenna's nearest reported lost |
| to MSv2 | 7.2 s, 1.49 GB | 1.2 s, 122 MB | 590,640 rows, each antenna read once; 1,240,344 of antennas no partition names reported lost |

The plan's scan of `POINTING` takes under 0.25 s; the reads go through
today's reader. In blocks of 1,024 rows those reads took 141 s into an
MSv4, nearly all re-parsing StandardStMan's index per block (#949), so the
adapter scans in blocks as large as a sixteenth of its budget allows, up to
65,536 rows. The MSv2 destination's memory is its `POINTING` table, which
MSv2 creation holds in memory until it is saved, about 2.3 KB a row
whatever the budget (#950); the reads themselves fit the budget.

A converted store therefore has no covering sample, and imaging's law
chooses between the samples either side; step 3b measures what that
changes. Field ephemerides are not carried yet; the converter still refuses
fields that follow one (#942).

| Law | What it shows |
| --- | --- |
| `field_directions_resolve_variable_frames_and_carry_reference_epochs` | A mosaic whose `PHASE_DIR` has a variable reference resolves each field's frame and keeps its angles; `FIELD.TIME` is each field's reference epoch; fields in two frames are refused; the MSv4 conversion's fields have no epoch. |
| `brackets_are_the_selected_observation_reading` (casa-ms unit) | The MSv2 adapter's brackets equal today's imaging reader's (`direction_brackets`) bit for bit, for rows with a rate term, rows of no interval and overlapping rows. Of two rows equally near a time, the boundary takes the earlier and today's reader the lower row: the same wherever an antenna's rows are in time order. |
| `pointing_brackets_hold_each_stores_samples_and_survive_conversion` | On the MSv2, every (antenna, time) bracket holds the rows today's reader names (the nearest row whose positive `INTERVAL` covers the time, the last before and the first after); the MSv4 conversion holds the same samples at the same instants with nothing covering; converting back writes the same rows with `INTERVAL` 0. |
| `msv4_pointing_finds_each_antennas_own_nearest_samples` | With one antenna sampled a second after the others (NaN between on MSv4's grid) and one with no pointing, a request between samples gives each antenna its own nearest sample either side, as the MSv2 does, and the antenna without pointing none. |
| `pointing_the_destination_cannot_hold_is_reported_before_writing` | `AZELGEO` directions keep their frame both ways; `GALACTIC` ones are lost whole into an MSv4 (no `pointing_xds`) and kept into an MSv2; a repeated row and rows beyond the observation are counted losses into an MSv4 and kept into an MSv2; two partitions sharing rows write them once. |
| `pointing_polynomials_keep_each_rows_direction_at_its_time_in_msv4` | Rows with a rate term (`NUM_POLY` 1) evaluate at the query time on the MSv2; the MSv4 conversion reports the terms as a loss and holds each row's direction at its own time, which converting back keeps. |
| `receptors_are_each_partitions_own` | Window rows where one antenna's angles differ and the others agree survive MSv2 → MSv4 → MSv2, written back as a row per window for every antenna; rows for every window serve both windows, and beside a window row are ignored, as casacore ignores them; of a row and one appended that changes an antenna's angles, the appended one is read whether it is 10 s later or earlier (table order, as casacore's `MSIter`); a conversion refuses the change until columns are accepted, then carries that row and reports the loss; rows for every window that change beside window rows are never read and are no loss. |

Unit laws: (casa-observation) `brackets_name_the_samples_around_a_time` and
`samples_must_be_whole_and_in_time_order`; (casa-ms)
`feed_rows_are_per_window_once_any_antenna_differs`,
`antenna_reads_group_antennas_within_the_budget` and
`padding_does_not_make_a_different_sample`.

## Weights from sigmas: the solve weight role

Step (4)'s second item (example B's finding): a solve weights by 1/σ² of
the stored sigmas, as CASA's do, so the request names what the weight role
holds (contracts, section 4). `WeightRequest::Stored` reads the stored
weights as before; `WeightRequest::InverseSigmaSquared` reads
`SIGMA_SPECTRUM` from an MSv2 whose MAIN has both it and `WEIGHT_SPECTRUM`,
else `SIGMA` broadcast over channels (`VisBufferImpl2`'s rule), and makes each
value a weight by `casa_observation::inverse_sigma_squared`, 0 where σ is not
positive (`resetWeightsUsingSigma`), before padding is spread. An MSv4 has no
sigmas, so the request names a fallback: `SigmaFallback::Refuse` refuses a
traversal that reads weights, `SigmaFallback::StoredWeights` reads the
stored weights. The estimate records each partition's rule and column
(`PartitionEstimate::weights`), so a fallback taken is visible to the
planner, and an updater refuses to write weights beside a traversal of
sigma-derived ones. Production `gaincal` keeps its own statement of the
rule until it moves onto batches (casa-calibration depends on
casa-observation only in its tests), and the parity law below holds the two
equal.

| Law | What it shows |
| --- | --- |
| `msv2_weights_from_sigmas_are_inverse_sigma_squared` | With `SIGMA` and then with `SIGMA_SPECTRUM` beside `WEIGHT_SPECTRUM`, made to differ from 1/√weight by a factor varying over products and channels and to be zero, negative or NaN for some cells, every delivered weight is 1/σ² of its own cell bit for bit, 0 where σ is not positive; the padding cell stays NaN; the estimate records rule and column for both readings. |
| `a_product_subset_takes_its_own_sigmas` | A subset of products delivers the subset's 1/σ², in the partition's order: the transform precedes the gather. |
| `msv4_states_its_sigma_fallback` | An MSv4 refuses a sigma request that reads weights, at estimate and binding, and not one that does not; with `StoredWeights` it delivers its stored weights and records the stored rule. |
| `a_weight_write_refuses_sigma_derived_weights` | An update session refuses a weight updater bound to a sigma-weighted traversal and binds a flag updater to it; a weight updater bound to the stored weights still refuses a patch from a sigma-weighted batch, which states its rule. |
| `a_weight_write_binds_beside_an_msv4_fallback` | Beside an MSv4's `StoredWeights` fallback a weight updater binds; its batches state the stored rule and write back. |
| `spectral_columns_without_content_are_read_past` | `WEIGHT_SPECTRUM` and `SIGMA_SPECTRUM` added and never filled hold no content (casacore's `hasContent`): the stored weights are `WEIGHT` and the sigmas `SIGMA`, and with only `SIGMA_SPECTRUM` filled still `SIGMA`. |
| `sigma_weighted_batch_solves_equal_the_production_gaincal` | Example B with `SIGMA` not 1/√`WEIGHT` (casa-calibration): the batch-fed solve reading 1/σ² equals production to 1.3e-7 in weight; the one reading `WEIGHT` differs by 2,500×. |
| `an_msv4_converts_to_an_msv2_of_every_acquisition` | 1/σ² of the converter's derived `SIGMA` gives the MSv4's weights back to 1e-6. |

Unit law: `sigma_weights_follow_casas_column_rule` (the MSv2 adapter's column
choice for the four combinations of spectral columns).

**Measurement** (`ms_trial_read_passes --weights stored|sigma` on `pilot.ms`,
MSv2: 64 partitions, 2.02 million samples, every role; Mac mini, release
build, passes 2 and 3 of three, two rounds each):

| Read | 1 worker | 4 workers |
| --- | --- | --- |
| Stored weights, before this change | 1.89–1.90 s | 0.65–0.66 s |
| Stored weights | 1.89–1.90 s | 0.65–0.66 s |
| 1/σ² of `SIGMA` | 1.89–1.90 s | 0.65 s |

The stored path costs what it did, and reading sigmas instead of weights
costs nothing measurable: the same number of cells is read, and the
transform is one pass over the values already in cache.

**Not done:** a per-cell fallback where a sigma cell is undefined (an
undefined cell is a read error, as for every role); `SIGMA` written by an
updater.

## Caltable round trip

Step (4)'s third item (contracts, section 2, "Binding a calibration table"):
a solve from batches is persisted as a CASA caltable, the store is closed,
and the table is bound to a store that numbers its entities otherwise and
applied from batches. `casa_ms::observation_io::persisted_namespace` gives a
store's namespace as it opens with given options: an MSv2's own subtables,
or for an MSv4 the subtables its MSv2 conversion holds (the converter's
builders, now shared: `catalogue_subtables`). The calibration trial's `binding.rs` holds the
`CalibrationBinding` and the writer, which translates the solve's catalogue
identities to namespace rows and hands them to production's gain-table
writer; it moves into production with the calibration port.

| Law | What it shows |
| --- | --- |
| `a_batch_solve_persists_the_msv2s_own_namespace` | A batch solve on the MSv2 fixture, written through its namespace, has production `gaincal`'s `ANTENNA` and `SPECTRAL_WINDOW` rows (the window collapsed to one channel at the channels' mean, as CASA's `gaincal` collapses it: #917) and its solutions to 1e-5; binding it back is CASA's own association. |
| `a_caltable_rebinds_to_a_store_numbered_otherwise` | Solved on an MSv4 converted from the fixture without antenna 0 (four antennas, numbered from 0), written through that store's namespace, closed; bound to the full MSv2 (five antennas, those four numbered from 1) by station and position, its field and window asserted; applied from batches: every sample equals production `gaincal` on the MSv2 without antenna 0 then `applycal` on the full MSv2, values to 1e-5 relative and flags exactly, antenna 0's 48 samples flagged both ways for want of a solution. Swapping two antennas' rows fails it on values. |
| `antennas_windows_and_fields_rebind_after_reordering` | Tables written through a simulated MSv2 of antennas N01–N03, windows (low, high) and two fields, and through its MSv4 conversion's namespace, each bind to a store of N03, N04, N01, windows (high, low) and the fields reversed, as an MSv2 and as its MSv4 conversion (XRADIO's names): every bound antenna, partition and field looks up the gain written for its own station, window and field; only N04 binds to nothing. Fields bind within 1e-12 rad, since a conversion moves a direction by a few ulps (#918). |
| `collapsed_windows_bind_and_overlaps_are_refused` | A one-channel window at the channels' arithmetic mean frequency (CASA's `gaincal`, checked against CASA 6.7.5.18 on the fixture, 44.448 GHz not channel 4's 44.512, and on ngc5921 with its widths rewritten to differ, where CASA's frequency stays the unweighted mean; casa-rs's writer since #917) binds; a window of decreasing frequency binds to its collapse whether the summed width is signed (CASA's unsolved rows) or not (its solved ones, measured on alma/multiobs.ms), and a one-channel one to its channel whatever the width's sign; one overlapping the window but not collapsing it, a width-weighted mean or the centre channel casa-rs wrote before #917 included, is refused, and one elsewhere binds to nothing. |
| `an_antenna_on_another_antennas_pad_is_refused` | Names agreeing up to XRADIO's `_STATION` suffixes (once or twice) bind; another name on the same pad is refused. |
| `the_writer_refuses_a_namespace_of_another_opening` | A solve's catalogue opened by scan against the namespace of the whole store is refused, and nothing is written. |
| `equal_windows_need_their_own_namespace_or_a_stated_association` | Two windows of equal channels are refused by content even in the store the table was written through, and bind to their own rows there when the caller states the namespaces are one; in a store listing them the other way round stating the windows' rows, as `spwmap` does, resolves it. |
| `collapsed_windows_alike_are_refused_unless_the_namespace_is_stated` | Two windows of different channels that collapse to one gain-table row alike (Astra's counterexample) are refused by content, names notwithstanding, and bind by index only when the namespaces are stated one. |
| `a_stated_same_namespace_is_checked` | A stated same namespace with another row count, a row contradicting its counterpart, or another antenna's name on a pad, is refused. |
| `a_partial_solve_binds_in_its_own_namespace_to_an_ulp` | In a stated same namespace a window binds to a collapse of part of its channels whose band ends an ulp beyond the window's edge; by content that window only overlaps and is refused. |
| `overlaps_spare_windows_bound_elsewhere_but_not_other_frames` | A narrow window inside a wide one the table solves binds to nothing rather than being refused; an unused table window in another frame overlapping it is refused, since a table solved in another frame would otherwise flag every window. |
| `an_unbound_field_takes_no_solutions` | The target's field moved: it binds to nothing, its antennas still bind, and every sample applied through the binding is flagged for want of a solution. |
| `the_binding_refuses_an_applier_of_another_opening` | An applier that opened the store by scan is refused against a default namespace. |
| `antennas_bind_by_station_and_position_never_by_name` | An antenna renamed as a converter renames it binds; another antenna bearing a table antenna's name, elsewhere, binds to nothing. |
| `antennas_of_one_content_need_their_own_namespace_or_a_statement` | Two antennas at one station and position bind to their own rows in a namespace stated to be theirs, are refused by content elsewhere even when both names agree, and bind as stated; a statement is one to one, and one antenna stated twice is refused. |
| `windows_bind_by_content_and_statements_may_share_a_row` | Reordered and renamed windows bind by channels, a window the table lacks to nothing; two partitions may be stated to one row, including the row a third binds to by content, or by index in a stated same namespace (a partial `spwmap`). |

casa-ms laws: `an_msv2s_namespace_is_its_own_subtables`,
`an_msv4s_namespace_is_its_conversions` and
`the_namespace_follows_the_opening_options` (partitioned by scan, three
partitions, describing that opening and not the default one).

**CASA oracle** (`tools/ms-trial/check_caltable.py`, CASA 6.7.5.18 on the
Mac mini, from the outputs the rebind law keeps under
`CASA_RS_TRIAL_KEEP`): CASA's `applycal` of the table written through the
MSv4's namespace, applied by CASA's own association to that store's MSv2
conversion, equals CASA's `applycal` of production's table on the MSv2 it
was solved on, over all 72 samples, to 3.3e-7 relative. CASA reads the
namespace the writer persisted as casa-rs binds it.

**Not done:** the binding in production, with `gainfield` and nearest-field
selection through bound fields; bandpass and other per-channel tables (their
windows keep their channels, which the content rule already accepts);
tables solved over combined windows or over part of a window's channels
(refused as an overlap until stated); `FEED` and receptor association; a
`FIELD` whose `PHASE_DIR` has a variable reference (refused, as the MSv2
adapter refuses it); directions exact across stores (#918). The gain writer
now collapses windows as CASA does (#917), so the binding's rule for
casa-rs's former centre-channel collapse is gone.

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
   up to four optional checks; revision 4's examples evaluate it once per
   batch or fragment with `mask_into`. Folding the rule into the fill
   remains an option.
4. **Solve weights** (example B finding after merging `main`): production
   `gaincal` weights by 1/σ² as CASA does, and the batch had no `SIGMA`;
   see example B. (`FLAG_ROW` in production `gaincal`, the earlier
   finding here, is fixed on `main`.) Resolved in step 4: the request
   reads 1/σ² weights.
5. **Cross-store rebinding as a contract.** The calibration comparison
   rebinds by context in test code; the design's `CalibrationBinding` is
   still to be built and tested with a caltable. Resolved in step 4
   (section "Caltable round trip").
6. **One vocabulary.** Move `CorrelationType` from the imaging model into
   `casa-observation` (or casa-types), and add a UVW measure to casa-types;
   `UvwView` still carries a `DirectionRef`.
7. **Creation catalogue for MSv2.** The writer's declaration is the common
   `Catalogue` plus a `TimeBaselineGrid`; an MSv2 creator needs a rows
   description in its place. Answered by D23 and the converter: the
   catalogue and batches suffice, every cell a row (converter section).

### Discard or defer

- The runtime's `NativeBlock` copy: the trial kept it so the production pass
  ran unchanged. M2 should let the pass place directly from batch views
  (`NativeRow` already borrows slices).
- The MSv2 opening index reads every row's key columns, in chunks of 2¹⁸
  rows, and keeps four bytes per grid cell (scale check above). Its time,
  about 0.2 µs a row, is what M2 should watch on the largest
  MeasurementSets; lazy construction per partition remains the option.
- Pool buffers are bounded by their envelope (revision 4), but allocations
  inside zarrs or casacore are not counted: the envelope plus each cursor's
  counted scratch is not yet an admission bound for the whole read.

## M1 coverage of this pass

This pass touches parts of the M1 checklist and completes none of it.

| M1 packet | Touched here | Still required |
| --- | --- | --- |
| `layout-precision` | One sample-major layout from both stores, the grid's axes, Hz/m/s views, `complex64`, correlation subsets with a flag policy | Unit requests and conversions, Jy/weight scaling, `complex128` when a store needs it |
| `measures-bulk` | Native epoch, frequency and UVW array views only | Every family's prepared bulk conversion, ordered interpolation, scalar/CASA agreement, performance |
| `group-lifecycle` | Time-bin, per-scan and per-integration groups keyed by field and scan, closure, concurrent fragments, split channel windows, `GroupCompletion`, one cursor per partition, `Catalogue::bind_to`; time slices of a partition (#910); solve → caltable → reopen → apply through the calibration binding | Groups merged across partitions by a consumer, late samples, the binding in production |
| `work-unit-lifetimes` | Unequal channel units, shared pinning against packing, pools bounded in buffers and bytes, buffers returned at the end, abandoned work reported, a pool wait that can be bounded | Cooperating workers on the admitted team, cancellation in flight, cube minor-cycle separation |
| `native-role-updates` | Update sessions on both stores: visibilities, flag set/clear changes and weights of declared targets; broadcast-weight and data-group alias rules; `FLAG_ROW` and padding preserved; whole-unit writes without read-back (write-back section) | `WEIGHT_SPECTRUM` and `FLAG_CATEGORY`, bit flags, per-batch MSv2 planning, MSv4 structural writes, conflicting queued patches |
| `conversion-fidelity` | MSv2 ↔ MSv4 through the boundary: a loss inventory refused unless accepted, every group, padding as D23's flagged rows or omitted as declared, `pilot.ms` equal to XRADIO's conversion and back through casacore (converter section) | Subtable-column inventory, `WEIGHT_SPECTRUM` and `FLAG_CATEGORY` into an MSv2, the `SIGMA` policy, every audited loss row |
| `source-free-create` | Common catalogue + grid declaration for both stores (the grid from the store's index), padding fill values and padded partitions, storage options, the narrow writer with per-batch validation, whole-chunk writes (partial chunks held), exclusive staging and no-replace publication; a processing set of many datasets; MSv2 creation from the catalogue and batches (converter) | Producer seam, backpressure, publication failure injection, the held chunk admitted with the producer's batches, omitting padding without a counting pass |
| `store-retention-executor` | `zarrs` and `zarrs_filesystem` pinned; codecs one chunk at a time on the caller's thread; retention plateau over 30 passes and W1/W4/W8 thread counts on `pilot.ms` (converter findings 2, 3) | Codec buffer counts, rayon's idle global pool, an admitted pool for in-partition parallelism or a time range (#910) |
| `object-failure` | MSv4 chunk replacement through a temporary file and rename, with injected failures before writing and before the rename (including an absent chunk's first write); the changed directories synchronized at finish; its cost at scale (converter finding 4) | A process killed mid-replacement, ENOSPC |
| `production-access-cost` | Narrow diagnostics on a tiny fixture; the estimate (bytes, batches, amplification, natural blocks, padding) as the planner's inquiry | The named MFS/cube/bandpass rows on both chunk geometries, noise bands |

## Unsupported in the trial (typed refusals)

`complex128` stores; more than one data group per traversal; MSv2 times
merged by tolerance (distinct `TIME` values are distinct grid times); two
rows of one time, baseline and partition, or several `PROCESSOR_ID`s in one
partition; two fields or scans at one time of one partition; MSv4 time
formats other than `unix`,
`unix` times in a scale other than UTC, and MSv4 writes in other than
`unix`/`utc`; MSv2 measure columns with a variable reference other than
`FIELD` `PHASE_DIR`, phase centres in different frames, and `TIME`
and `TIME_CENTROID` in different scales; `POINTING` directions in several
frames, and an MSv4 pointing target; an antenna whose two feeds in one
partition differ; MSv2 UVW labelled with a celestial
frame other than the phase centres'; MSv4 groups whose UVW frame is not
their phase centres'; frames other than fk5/icrs for directions and UVW
(and altaz for pointing);
units other than the schema's (none are converted); MSv4 variables of other
data types or dimension orders (bitmask flags included); MSv4 datasets
with `polarization_mixed`; centroids from a dataset with
`TIME_CENTROID_EXTRA_PRECISION`; `field_name` or `scan_name` not one per
time; a grouping keyed by scan on a store without scans; one antenna name
with two positions; a batch budget smaller than one time; a batch over its
pool buffer's envelope; per-cell
`WEIGHT_SPECTRUM` fallback; converting into an MSv2 weights that vary over
channels, feeds other than 0 or data groups other than `base`,
`corrected` and `model`; `SIGMA`, `FLAG_CATEGORY`; writing channel
windows, correlation subsets, or batches that disagree with the
declaration; MSv4 sharding (not exercised); remote stores.
