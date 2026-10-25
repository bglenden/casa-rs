# MSv2/MSv4 trial: revision 2 design record

Status: design decisions for revision 2, agreed with the owner on 2026-10-24, implemented on the trial branch on 2026-10-25 (deviations listed under "What the implementation did with each decision"; D18 completed after the ADR-0016 amendment of the same day); PR #898 stays a draft and the interfaces are not frozen
Truth class: proposed design
Date: 2026-10-25
Evidence index: [README.md](README.md)
Design branch: [architecture.md](../architecture.md), [contracts.md](../contracts.md)

This records the design walkthrough of the trial's shared boundary, held with
the owner after revision 1 (head `2b07a37`). It is the durable record of the
decisions, the reasons for them and the facts they rest on, so that revision 2
can be implemented from it and reviewed against it. Decisions D1 to D15 were
taken by the owner; D16 to D25 were delegated to the agent's judgement after
the owner found the questions detailed enough, and are marked so.

## The principle

**MSv4 is the future.** The shared interface reflects MSv4's native
capabilities, and the MSv2 adapter assembles MSv2 data into those same
structures. Where the two stores differ, the interface takes MSv4's shape and
the MSv2 adapter does the work of fitting, once, at opening or binding. A
consumer must not be able to tell which store it is reading from the batch.

## Vocabulary

These terms go into `GLOSSARY.md` with the revision.

- **Sample.** One (time, baseline) point: an MSv4 `[time, baseline]` cell, or
  an MSv2 row placed on that grid. Payload arrays are
  `[sample, channel, correlation]`.
- **Batch shape.** What one batch covers: the partition set, the channel
  window, the roles read, and the number of whole times the byte budget
  allows. Applications differ here: cube imaging takes a narrow channel window
  and many times; `gaincal` takes every channel and time blocks; MFS takes
  every channel and every time the budget allows.
- **Traversal order.** The order batches arrive. Within one cursor it is
  fixed: time blocks of one channel window of one partition, in time order.
  Across the observation it is the consumer's, expressed by which cursors it
  binds and runs, and when: one cursor per channel window for a cube, one
  cursor for `gaincal`, one cursor per partition in parallel.
- **Payload order.** The element order inside one batch:
  `[sample, channel, correlation]`, correlation fastest, with the sample index
  `t * B + b`. Both stores deliver it without a transpose (MSv4's dimension
  order flattened; MSv2 cells `[correlation, channel]` concatenated by row). A
  channel-major view is free (`permuted_axes`), contiguity per plane is not.

## Decisions

Each entry gives the decision, why, and what it changes. "Accepted" means
the owner decided; "judgement" means delegated.

### D1. No redundant copies in the batch (accepted)

Per-partition constants live once in the `Catalogue`, linked from the batch by
`PartitionId`: channel frequencies, their frame and widths; the correlation
list; the data group's name, units and UVW frame; the MSv4 baseline table; and
the time system (scale and epoch origin, today in every `BatchHeader`). The
batch keeps `first_channel` and its channel count and drops `frequency`,
`correlations`, `group_name` and `units`; `BatchFill::finish` and
`BatchBuffers::pack` stop copying and checking them. "Same setup as the last
batch" is `PartitionId` equality. Application code caches if it needs to.

Why: the link already existed (`batch.partition` and
`Catalogue::partition(id)`); the copies at `msv4/cursor.rs:347-356` and
`msv2/mod.rs:379-384` were a second description that had to be rechecked.

### D2. A partition is a store unit described by content keys (accepted)

`PartitionEntry` states what defines the partition: spectral window,
correlation setup, intents (`OBS_MODE`), observation, ephemeris, and field or
scan when the partition scheme split by them. The MSv2 adapter partitions the
way XRADIO does by default (`DATA_DESC_ID` × `OBS_MODE` × `OBSERVATION_ID` ×
`EPHEMERIS_ID`, `partition_scheme=[]`), so the same observation opens as the
same partition set from either store. The opening options take the scheme, so
an MSv2 can be presented like a given conversion. Cross-store matching uses
content keys, never list positions.

Why: today an MSv2 partition is one `DATA_DESCRIPTION` row and an MSv4
partition is one dataset, which XRADIO splits further; the fixture had one of
everything, so the mismatch never showed. Consequences accepted by the owner:
more and smaller partitions (an MSv2 with 100 spectral windows, 3 intents and
2 observations opens as up to 600); imaging one spectral window reads several
partitions, which can run in parallel; MSv2 opening reads one more MAIN
integer column (`OBSERVATION_ID`); the calibration test stops renumbering
partitions by hand.

### D3. One topology: MSv2 rows are gridded at binding (accepted)

`SampleTopology::Irregular` is deleted. The MSv2 adapter builds the time ×
baseline grid at binding as XRADIO's converter does: unique times, unique
baselines, every row mapped to a cell (`ANTENNA1` and `ANTENNA2` join the
columns the binding index reads). A cell with no row is **padding**: flag
true, weight 0, values NaN. A time block without holes, which the index knows,
is filled directly as today. Field and scan become per time; binding asserts
that every row at one time shares a field. Row order stops mattering, which
removes the "rows not in time order" refusal.

No presence mask, no `Occupancy` enum, no refusal threshold: the owner asked
for the simplest form ("read whatever is there and flag it"). Binding may
report the padded-cell fraction in the estimate, since it has the row count
and T × B in hand; nothing acts on it.

Why "both" (flag and NaN): the MSv4 main-dataset schema is silent on missing
cells; where the schema does pad (the phased-array sub-dataset,
`schema.py:1993-2000`) it pads offsets with NaN and flags with True. XRADIO's
converter pads NaN with FLAG = false, which `validity()` still excludes
through the non-finite test; flag statistics would differ for padded cells
between our MSv2 read and an XRADIO conversion of the same MS. That is
XRADIO's divergence from its schema's own precedent and is recorded, not
copied. The cost is memory proportional to the padding fraction (an antenna
dropping out partway pads its baselines for the rest: 26 of 351 for the VLA);
padded cells are not read from disk. Baseline-dependent averaging would pad
heavily; so would XRADIO; no fixture exercises it.

### D4. Duplicate (time, baseline) rows (judgement, accepted by the owner)

The feed pair (`FEED1`, `FEED2`) becomes a partition content key, so a
multi-feed MSv2 opens as one partition per feed pair (MSv4 has no feed axis in
the main dataset). `PROCESSOR_ID` must be single per partition, as XRADIO
requires. Any collision left after that is a true duplicate and binding
refuses it, naming the rows. Never "last row wins".

Why: the MSv2 main key is wider than (TIME, ANTENNA1, ANTENNA2, DATA_DESC_ID);
XRADIO ignores the feed and keeps the last colliding row silently
(`_tables/read.py:1494`). The design table (`architecture.md`, "Duplicate
(time, ordered baseline) acquisitions") already asks for reject-or-split.

### D5. `FLAG_ROW` folded; one weight view (judgement, accepted by the owner)

The MSv2 adapter folds `FLAG_ROW` into the flag cube at fill (effective
`FLAG OR FLAG_ROW`, the rule the design table already gives for conversion);
`DataGroupView::row_flags` is deleted. Accepted consequences: our MSv2 read is
stricter than today's production `gaincal`, which ignores `FLAG_ROW`, on rows
flagged only by it (whether that is also CASA's behaviour is the check queued
in the README); an XRADIO-converted MSv4 of the same MS has fewer flags,
because XRADIO drops `FLAG_ROW` upstream, and the reader does not repair that.

Weights are one view, `[sample, channel, correlation]`, always. MSv2 `WEIGHT`
is presented with a zero stride on the channel axis (`insert_axis` then
`broadcast`): no copy, no per-element cost; `as_slice()` returns `None` on
it. The `WeightView` enum is deleted. This is CASA's meaning of `WEIGHT`
without `WEIGHT_SPECTRUM`, and the shape MSv4 stores physically.

### D6. The request selects a partition set; one cursor per partition (accepted)

`AccessRequest.partition` becomes a selection over content keys. Binding
resolves it to an ordered partition set and returns one `Send` cursor per
partition, each with its own estimate (D10, D11). A `chain()` helper serves
sequential consumers; a parallel consumer gives each cursor a thread.
`GroupKey` already carries the partition, so grouping is unaffected. A batch
never spans a partition.

### D7. A byte budget replaces `max_samples`, with inquiry functions (accepted)

Bytes per sample follow from the roles, the channel window, the correlations
and the element sizes (plus per-sample scalars); samples per batch is the
budget divided by that, rounded down to whole times; binding refuses when one
time exceeds the budget. The owner requires that a planner can ask what a
cursor shape costs without major I/O: see D10. In passing, the MSv4 payload
formula counts every role whether requested or not (`msv4/cursor.rs:203`);
the budget conversion fixes that.

### D8. The estimate reports the store's natural blocks (accepted)

Along time and frequency: the Zarr chunk shape for MSv4 (5 times × 4 channels
in the fixture), the tile shape for MSv2 (casa-tables reads honour it,
`crates/casa-tables/src/storage/tile_layout.rs`). A planner aligns channel
windows to it; the fixture showed why: 35 chunks decoded resident against 110
in one-plane waves.

### D9. `Layout` is deleted (accepted)

Payload order follows the store, and both stores deliver sample-major.
`ChannelMajor` would only have chosen where to pay a transpose; a channel-major
view over the same memory is free, and a plane consumer gets contiguity by
asking for a one-channel window.

### D10. `estimate`: the inquiry function (accepted)

`Observation::estimate(&request) -> Result<Estimate>`, pure over the
catalogue and the store geometry. Per partition of the set: times and
baselines; bytes per sample for the roles; samples per batch under the budget
and the batch count; bytes held per batch; decoded against selected elements
(the amplification); natural blocks (D8); padded fraction (D3). Totals over
the set. `Realization` is unified into `Estimate`: binding returns the
estimate for the request as bound, so plan and binding cannot drift.

Cost: arithmetic over the MSv4 chunk grid, which is in `zarr.json` and loaded
at opening; for MSv2 the row index moves from binding to opening, about 44
bytes per row read once, which D2 and D3 need anyway.

### D11. `bind_read` returns a traversal (accepted)

`Traversal { cursors: Vec<BoundCursor>, estimate: Estimate }`. `IoCounters`
stay per cursor; the traversal sums them. `Observation` is `Sync` and cursors
are `Send`, so threads share the observation by reference.

### D12. End of stream without a buffer (accepted)

`next_owned(Option<BatchBuffers>)`: given `None` the cursor answers `End`,
`GroupEnd(key)` or a new `NeedBuffers` event; given buffers, as now. No
wasted buffer, no `exhausted()` state machine, no reliance on a precomputed
batch count. Closes README open question 1.

### D13. Scan in the group key; origin per (field, scan) (accepted)

`GroupKey { partition, field: Option<FieldId>, scan: Option<ScanId>, bin }`.
The `solint` clock starts at the first time of each (field, scan) run, as
production `gaincal` does (`crates/casa-calibration/src/solve/grouping.rs`,
`SolveBaseKey` and `bucket_starts`). Observation and intent are partition
keys after D2. The trial key had no scan and a per-field origin, so a bin
could straddle a scan boundary where production splits; the fixture's two
scans happened to align with its 60 s intervals.

### D14. `Grouping` gains CASA's other intervals (accepted)

`TimeBins { width_s, combine_fields, combine_scans }`,
`PerScan { combine_fields }` (`solint='inf'`) and `PerIntegration`
(`solint='int'`).

### D15. Groups across partitions stay the consumer's (accepted)

CASA `combine='spw'` is a merge by (field, scan, bin) across cursors; the
per-(field, scan) origin makes those bins comparable whenever spectral windows
share scan times. No shared helper until a consumer needs one. The group
index, proven closure at delivery, `GroupEnd` as the fallback,
`GroupCompletion` and fragment-free windows all stay; with D3 the index is per
time for both stores and every fragment is whole times × all baselines.

### D16. Validity stays an inline rule; optional mask (judgement)

`validity().excluded(s, c, p)`: the stored flag is set, or the value or the
weight is not finite among the roles read (the row-flag test is gone with D5).
Stored flags are never altered by it. For multi-pass consumers,
`validity().mask_into(&mut Vec<bool>)` builds one byte per element in caller
storage. Folding the rule into the fill was rejected: for one-pass consumers,
which both examples are, it would add a pass to save the same work later, and
it would change stored flags. The doc comment at `batch.rs:345` saying XRADIO
flags absent slots is wrong (XRADIO pads flag false) and is corrected.

### D17. Correlations and flag policy in the request (judgement)

```rust
pub struct CorrelationRequest {
    pub deliver: Deliver,        // All | These(Vec<Correlation>), in stored order
    pub flags_from: FlagsFrom,   // Delivered | All
}
```

With `flags_from: All` the adapter reads the whole flag cube and ORs the
undelivered correlations' flags into each delivered one; the batch carries
`[sample, channel, delivered]`. Production Stokes I is
`These([XX, YY])` with `flags_from: All`: half the visibility memory, the
same `sumwt` (selecting XX and YY without the policy moved `sumwt` by 0.86 %
in the trial). Values are never altered. Closes README open question 2.

### D18. One correlation vocabulary and a UVW measure in casa-types (judgement)

`Correlation` (casacore `Stokes` codes and MSv4 labels as its persistent
forms) is defined once in `casa-types`; `casa-observation` and
`casa-imaging-model` use it. `MUvw` is added to casa-types. Closes README
open question 6.

### D19. Measure views are built from buffers plus the catalogue entry (judgement)

`EpochView`, `FrequencyView` and `UvwView` stay. After D1 they are built by
`buffers.batch(&entry)` from the partition's entry, so the time system and
frames are resolved once per batch from the one description and nothing is
copied into the header.

### D20. Times stay offsets in the store's scale (judgement)

From a per-partition origin; a consumer that needs another scale converts
through `MEpoch`. Adapters keep refusing scales they cannot label (MSv4
`unix` in UTC only; MSv2 `MEASINFO`, UTC when absent).

### D21. `UvwView` carries `MUvw` (judgement)

The effective frame is still resolved once at opening; the stored label stays
in the catalogue (`UvwFrame { effective, stored_label }`).

### D22. The payload is `Complex32` (judgement)

`Precision` and the unreachable `VisibilityValues::Complex64` are deleted. No
consumer asks for native precision, CASA `DATA` is single precision, and the
fixture's `complex64` is already `Complex32`. A `complex128` store is refused
at binding until a store needs it, when the variant returns with a real decode
path.

### D23. One creation declaration for both stores (judgement)

The common `Catalogue` plus a `TimeBaselineGrid` per partition declares a
dataset for MSv4 and MSv2 creators alike, now that the grid is the one sample
layout (D3): cells in time order become MSv2 rows, and padded cells become
flagged NaN rows. A reverse conversion that must reproduce an original sparse
row set is a declared policy or a refusal (`architecture.md`, "Absent slots
versus actual NaN-valued acquisitions"), since padding and flagged data are
indistinguishable by design. Closes README open question 7.

### D24. Writer fill values are the padding convention (judgement)

`Msv4Writer`'s Zarr fill values become NaN for values, true for flags and 0
for weights, so a cell never delivered reads back as padding. The writer's
row-flag refusal is moot after D5.

### D25. `CalibrationBinding` as contract (judgement)

`Catalogue::bind_to(&other, tolerance) -> Binding`: antennas matched by
station and position within a stated tolerance, fields by phase centre, scans
by number, partitions by content keys (D2); any ambiguity refused. It is what
applying a caltable solved on one store to the other needs; the calibration
test's `rebind` is its prototype. Closes README open question 5.

## Facts verified during the walkthrough

XRADIO at the pinned commit `3ed76586` (paths under
`src/xradio/measurement_set/_utils/_msv2/`):

- Rows are gridded onto (time, baseline): unique times merged within a
  quarter of the smallest time step (`_tables/read_main_table.py:12`), unique
  baselines, `tidxs`/`bidxs` per row (`conversion.py:616`).
- Cells with no row are padded by dtype (`xradio/_utils/list_and_array.py:15`):
  floats and complex NaN, integers `INT_MIN`, booleans **False**. So
  `VISIBILITY`, `WEIGHT` and `UVW` are NaN and `FLAG` is false.
- Duplicated (time, baseline) rows: the last row wins, silently
  (`_tables/read.py:1494`). `PROCESSOR_ID` must be single per partition
  (`msv4_info_dicts.py:61`). `FEED1`/`FEED2` are read only for the antenna
  dataset (`conversion.py:1947`).
- `FLAG_ROW` is dropped: only `ANTENNA::FLAG_ROW` is referenced.
- `WEIGHT` is expanded across frequency physically
  (`conversion.py:971`, `frequency_constant`); MSv4 `WEIGHT` is always
  `[time, baseline, frequency, polarization]`.
- The default partition scheme is `[]` (mandatory keys only), the fixture's.
- The MSv4 schema (`schema.py`) says nothing about missing main-dataset
  cells; `FlagArray` is "the cumulative flags applying to this data matrix";
  the phased-array sub-dataset is "nan-padded" and "True-padded".

The trial at `2b07a37`:

- The MSv4 reader runs zarrs with `concurrent_target(1)`
  (`msv4/read.rs:122`) and decodes a batch's variables one after another; one
  cursor reads one partition; a factored batch always carries the whole
  baseline table. These are adapter limits, not interface limits; D6, D7 and
  D8 address the interface side, the rest is revision-2 adapter work with
  measurements.
- Views cost nothing per element: an `ArrayView3` is a pointer, a shape and
  strides, built once per batch (`buffers.rs:338`); workers on an
  `Arc<FilledBatch>` dereference the `Arc` once.
- The MSv2 binding index reads `DATA_DESC_ID`, `TIME` and `FIELD_ID` per row
  and binds only time-ordered rows; group fragments are contiguous for that
  reason.
- Production `gaincal` keys solutions by field, spectral window, observation
  and scan with the `solint` origin per key; no imaging or calibration crate
  references `FLAG_ROW`.

## README open questions

| Question | Resolved by |
| --- | --- |
| 1. End of stream without a buffer | D12 |
| 2. Correlations and flag policy | D17 |
| 3. Validity cost | D16 |
| 4. `FLAG_ROW` in production `gaincal` | D5; the CASA behaviour check stays queued |
| 5. Cross-store rebinding | D25 |
| 6. One vocabulary | D18 |
| 7. Creation catalogue for MSv2 | D23 |

## Risks and checks to carry into revision 2

- **Padding growth** on sparse MSv2 (antenna dropouts, baseline-dependent
  averaging): measure the padded fraction on real MSs; the estimate reports
  it.
- **Flag statistics** differ between our MSv2 read and an XRADIO conversion of
  the same MS on padded cells and on `FLAG_ROW`-only rows. Record in the
  README; do not repair on read.
- **CASA's `FLAG_ROW` behaviour** in `gaincal` and `tclean`: instrument CASA
  before claiming parity on an MS with `FLAG_ROW`-only rows.
- **Opening cost**: the MSv2 index (about nine key columns per row) moves to
  opening; measure on a 10^8-row MS; consider lazy construction if it hurts.
- **Fold versus inline validity**: D16 is a judgement on one-pass consumers;
  measure if a multi-pass consumer appears.
- **Throughput**: raise zarrs concurrency and overlap variable decodes in the
  adapter; run cursors per partition in parallel; measure with matched
  timings before any performance claim.

## Suggested implementation order

1. Catalogue and partitions: D2, D4, D1, D18 (vocabulary), D25. Tests:
   partition sets from both fixtures agree by content keys; feed pairs;
   duplicate refusal; `bind_to`.
2. Grid and payload: D3, D5, D9, D22, D19 to D21, D16. Tests: gridded MSv2
   batches equal MSv4 batches cell for cell; padding convention; folded
   `FLAG_ROW`; broadcast weights; both examples bit-identical to production.
3. Request and binding: D6, D7, D8, D10 to D12, D17. Tests: estimate equals
   the bound realization; byte budget rounding; natural blocks; partition
   sets; `NeedBuffers`; Stokes I `sumwt` unchanged with `These([XX, YY])`.
4. Groups: D13 to D15. Tests: bins restart per (field, scan); `PerScan` and
   `PerIntegration` against production `gaincal`.
5. Creation: D23, D24. Tests: MSv2 and MSv4 written from one declaration
   read back identically; unwritten cells read as padding.

Each step keeps the two examples and the interop checks green before the
next; the PR stays a draft until the owner says otherwise.

## What the implementation did with each decision

Revision 2 implemented every decision as recorded, with these qualifications.
The evidence is in [README.md](README.md), "Revision 2".

- **D2, D4 grid.** Times on an MSv2 grid are the exact distinct `TIME`
  values; XRADIO merges times within a quarter of the smallest step. An MS
  whose times jitter opens with more times (and padding) than XRADIO's
  conversion of it would; no fixture exercises this, and a tolerance is a
  policy the record should take explicitly if it is wanted.
- **D2 partition keys.** `EPHEMERIS_ID` splits MSv2 partitions but is not
  exposed in `PartitionEntry` (the MSv4 reader does not read ephemerides);
  two partitions that differ only by ephemeris have the same content keys
  and `bind_to` refuses them as ambiguous until it does.
- **D8 MSv2 natural blocks.** The tile shape of the data group's
  visibility column, read through a new `casa-tables` accessor
  (`Table::array_column_tile_shape`); the time extent reported is the
  tile's rows divided by the baselines, which is exact only when the
  partition's rows are stored by whole times.
- **D10 MSv2 amplification.** Not modelled: decoded elements are the rows
  read, as before; the tile-level amplification of a channel window on a
  tiled column is a later refinement.
- **D17 subsets.** Both adapters read the whole correlation axis and
  gather the delivered products (MSv4 decodes the full cube into scratch);
  a strided in-place decode per product is a later optimization, to be
  measured.
- **D18 vocabulary.** `Correlation` and `MUvw` live in `casa-types`. The
  ADR-0016 dependency policy first allowed `casa-imaging-model` no workspace
  dependency, so the model kept its own `CorrelationType`; on owner direction
  (2026-10-25) ADR-0016 now lets every imaging crate depend on the
  definitional crates (`casa-values`, `casa-types`, `casa-coordinates`), and
  the model's private type is gone. Both adapters, `casa-observation`, the
  imaging crates and the simulator use the one `Correlation`, and the imaging
  example's batch and operator share one list of products. Unifying showed
  that revision 2's `Correlation`, the eight feed products, was narrower
  than MSv4: the schema allows all 32 casacore `Stokes` types in its
  `polarization` coordinate, as an MSv2 `CORR_TYPE` may store them, so
  `Correlation` now holds all 32 under MSv4's labels and a consumer refuses
  the types it cannot use (the operator refuses mixed and quasi-orthogonal
  feeds, as before).
- **D23 one declaration.** The common declaration (`Catalogue` plus
  `TimeBaselineGrid` per partition) and the rule for padded cells are in
  place and the MSv4 creator uses them; an MSv2 creator from batches was
  not built in revision 2.
- **D25.** `Catalogue::bind_to` with an explicit `BindingTolerance`;
  example B's comparison uses it in place of its test-code `rebind`.

## Provenance

Walkthrough held on 2026-10-24 against head `2b07a37` of
`claude/ms-v4-contract-trial`; every answer was re-verified against the code
and the pinned XRADIO source before it was given. Figures for human readers
are the two artifact pages from the walkthrough, linked from the PR #898
thread.
