# Imaging on the shared observation batches (M2 packet c)

Status: accepted by the owner on 2026-10-27 (section 3); steps 1–4 landed, step 5 in progress (#932)
Truth class: accepted design
Date: 2026-10-27
Repository baseline: `84824df`
Design inputs: [implementation plan](implementation-plan.md) §4 (M2) and D4, [contracts](contracts.md), [trial README](trial/README.md) example A

The plan's M2 packet (c) moves imaging's source preparation and operator
windows onto the shared batches, within the existing single
pass/worker/resource architecture. This document has three parts:
- how imaging reads a MeasurementSet today (section 1);
- the design that replaces it, and the owner's decisions on it (sections 2
  and 3);
- the order of work, and the baseline and blocking comparison that gate the
  cutover (sections 4 and 5).

## 1. Today

Imaging reaches a MeasurementSet only through `casa-ms`'s selected-observation
layer. That layer also computes imaging's geometry, which is why `casa-ms`
depends on `casa-imaging-model`.

- **Compile** (`casa-imaging-application/src/compile/mod.rs:94`) walks MAIN
  three times:
  1. the selection survey, with the MFS spectral envelope
     (`compile/selection.rs:157`);
  2. the cube density padding (`selected_observation/spectral_evaluation.rs:427`);
  3. the pointing query domain (`casa-ms/src/observation_owner.rs:374`).

  It then resolves a `SelectedObservationResolutionRequest` into an
  `ObservationSnapshotInput` and its access.
- **Admission** (`casa-imaging-runtime/src/source_access.rs:131`) plans a
  content budget that binary-searches `rows_per_block`
  (`selected_observation/content_plan/requirements.rs:87`).
- **Delivery** goes through `MeasurementSetSource`, the only production
  `BoundedSource` (`casa-imaging-application/src/imaging/source.rs:131`, trait
  at `casa-imaging-runtime/src/pass/mod.rs:175`). One producer thread with two
  block slots runs, per block:
  1. consecutive selected MAIN rows of one `DATA_DESC_ID`
     (`selected_observation/access.rs:522`);
  2. the channel window with its frame conversion
     (`access/channel_window.rs:14`);
  3. typed channel-range fills into a buffer
     (`selected_observation_buffer.rs:479`);
  4. **row geometry**: phase and pointing laws, per-domain UVW rotation and
     phase shift, parallactic angle, antenna class, output-frame frequency
     conversion cached per (field, time)
     (`access/geometry.rs:41`, `access.rs:887`);
  5. a stride gather into `NativeBlock` `[row][channel][correlation]`
     (`imaging/source.rs:296`).

  That is two copies: store to buffer, then buffer to block.
- **Consumers** place samples (`pass/wave.rs:238`). The imaging weight of
  every sample is cached across passes in `WeightCache`, positionally per
  (selected row, native channel) (`pass/weights.rs:17`).
- **`savemodel`** writes through `SelectedVisibilityWrite`, addressed by
  `RowAddress { physical_row, data_description }`, one cell at a time in
  batches of 10,000 rows (`observation_owner/visibility_write.rs:253`).

MAIN-row concepts run through every layer:
- `SelectedRows`, `SelectedObservationRunRow` and `SelectedNumericRow` (model);
- the survey's row manifest and `first_cross_row` (compile);
- one-DDID blocks and per-row predicates (casa-ms);
- `RowAddress`, `BlockShape.rows`, and `WeightCache`'s row order (runtime);
- non-finite errors that name a MAIN row (application).

Example A of the trial (`tests/ms_trial_imaging.rs`) already drives the
production operator and major cycle from batches through a `BoundedSource`
over `&dyn Observation`, on both stores. It skipped all of step 4: stored
UVW, native frequencies, one partition, one domain, placeholder row headers
and no write-back.

## 2. Target

Imaging reads `&dyn Observation` batches only. Its geometry compiles from the
catalogue and the batch. `casa-ms` keeps the storage adapters and nothing
imaging-specific, as D4 proposes. The same imaging run then works on MSv2 and
on MSv4.

### 2.1 The source

`ObservationSource` replaces `MeasurementSetSource` as the production
`BoundedSource`, in `casa-imaging-application`:

- **`begin`** binds one traversal per wave. Its `AccessRequest` names:
  - the selected partitions and time range;
  - the wave's channel window (`restrict`): the native channels whose
    samples reach the wave's planes, with both interpolation partners;
  - the content (`Roles::FLAGS | WEIGHTS` for density passes, as
    `FlagsAndWeights` asks today).

  A partition replaces a `DATA_DESC_ID`.
- **`fill`** packs one batch's valid samples into `NativeBlock` in one copy,
  as the trial's `pack` does. Padding cells (NaN weight) are dropped, and so
  are the samples a selection predicate excludes (2.3). A block holds whole
  times of one partition, already in baseline order, so
  `order_rows_by_baseline` has nothing to do.
- **`maximum_block`** comes from the estimate: the widest partition's batch
  at the admitted batch size.

The producer thread, two block slots, the worker team, the chunk planner and
admission stay as they are. One copy (store to batch buffers) plus the pack
replaces the two copies of today.

*As landed in step 4* (#932): `ObservationSource`
(`casa-imaging-application/src/imaging/observation_source/`) is the batch
route, chosen per run by `ImagingRequest::source_route`, a run option that
no imager parameter names. It sees only `&dyn Observation`.
- **Selection.** Compile still resolves the request against the
  MeasurementSet and walks MAIN (2.4 is not done). `compile::partitions`
  maps that selection to the partitions of the same MeasurementSet's MSv2
  adapter: a partition is read when its data description is selected and,
  under an intent selection, its `OBS_MODE` is a selected state's. The
  adapter keys partitions by the whole `OBS_MODE` string, so a partition
  holds the rows of every state with its mode (two modes listing the same
  intents in another order are two partitions), and rows without a state
  share the empty mode. An intent selection is therefore refused unless
  the partitions it reads hold the selected states' rows alone: no state
  that MAIN's rows hold and the selection leaves out has a selected
  state's mode (`intent='0'` is refused once rows hold a second state of
  state 0's mode, and images as today while none does). Fields go in the
  request. The UV distance is tested per sample, on stored UVW and in
  wavelengths of the window's `REF_FREQUENCY`, by the rule today's route
  now shares (`UvSelection::holds`). This module is the batch route's
  only store-specific code; it goes when compile resolves the selection
  against the catalogue.
- **Opening.** The MSv2 adapter refuses at opening what its grid cannot
  hold: two rows of one time and baseline in a partition, two fields or
  two scans at one time of a partition, and a partition's rows with
  several `PROCESSOR_ID`s (XRADIO's rule). Today's route images such
  MeasurementSets; the batch route does not, until the adapter does.
  Opening asks a grant for what its index will hold before allocating it
  (`Msv2Observation::open_granted`): its chunk of key columns, each
  partition's distinct times, baselines and processors as the first pass
  finds them (at most one row's keys beyond what was granted), then the
  scans and the grids. The batch route admits each increase into the
  opening's reservation, so an index beyond what the policy leaves free
  is refused, as an admission, before it is allocated, also when runs
  share a process. Times that differ by baseline (baseline-dependent
  averaging, jittered `TIME`) make a cell per time and baseline, which
  this refuses before allocating.
- **Delivery.** Each wave binds one request per partition in turn, and a
  batch holds whole times. Padding (every flag set and every weight not
  finite) and the samples the UV selection excludes are dropped. Samples
  the store flags, `FLAG_ROW` included, are kept and given geometry, and a
  sample whose every routed cell is flagged carries the row flag. A
  non-finite input fails as today, naming the spectral window, time,
  baseline, channel and correlation. One divergence: under
  `FiniteValuePolicy::RejectAll` today's route fails on a stored row whose
  every flag is set and no weight is finite, which the batch route reads
  as padding and drops.
- **Pointing.** When geometry reads pointing, each partition's samples are
  queried over the span of its selected times (time centroids included
  when geometry reads them). An antenna's pointing is a property of the
  antenna and the time, so partitions asking for the same antennas over
  the same span in the same time system (origin and scale) share an
  answer (the contract of `Observation::pointing`). The source keeps the
  answers across partitions and passes, so a store whose query reads a
  whole table (the MSv2 adapter scans `POINTING`) reads it once a distinct
  query. The charge is the most any query of the observation can hold
  (`Msv2Observation::pointing_bytes`: every `POINTING` row as a
  candidate, a scan block of at most every row, and the series), the
  bound today's route charges. A new query may hold what the answers kept
  leave of it; when the store refuses that (`OverBudget`), the answers go
  and it is asked again with all of it, under which no query is refused
  for its size and `POINTING` is read in one block of up to 65,536 rows.
- **Restricted waves.** A partition's channel window is bound before its
  batches are read, from the range of output-frame conversion factors its
  selected (field, time)s span. It holds every channel today's per-block
  windows hold; the extra channels reach no plane of the wave. Both the
  factors and the pointing spans come from one payload-free survey: at the
  first traversal when geometry reads pointing, and with the factors at
  the first restricted wave. The survey reads in buffers of its own, one
  channel and no payload a batch. Under linear interpolation the native
  channel width is taken from a row's first two delivered channels, so a
  gapped selection's fine grid can depend on the window delivered, on
  either route (#964).
- **Batches and admission.** Each partition's times per batch are sized as
  today's rows per block: at least one time of each partition and at most
  all its selected times (as today's blocks hold at most the selected
  rows), and growth up to the bootstrap budget or the source's share of a
  quarter of free memory. The charge holds the largest batch with its
  decode scratch, its geometry over the widest native span a batch
  delivers, the parallactic cache at every selected (time, field), the
  pointing charge when geometry reads pointing, the survey's batch and
  conversions, and the passes' blocks. Opening is admitted before the
  MeasurementSet opens, from MAIN's row count
  (`Msv2Observation::opening_bytes`: a cell and eight bytes of time tables
  a row, and a chunk of key columns), and raised by what it asks for
  beyond; once open the charge becomes the index
  (`Msv2Observation::index_bytes`).
  Counting laws:
  - the whole run's
    (`the_observation_batch_route_stays_within_its_admission`, with a
    101-channel window imaged whole and gapped, `0:0~1;100`), whose slack
    hides the source's charge;
  - the source's own (`tests/batch_source_memory.rs`, through
    `source_law`), which reads the source alone on an opened, admitted
    route and fails when the batches' charge is dropped;
  - each smaller term's owner's: geometry and the parallactic cache
    (`batch_geometry_memory`), a traversal's batch envelope (the adapters'
    laws), and the opening and pointing bounds (`casa-ms`
    `msv2_opening_memory`, which also refuses a jittered-time opening
    before its grids); the pointing answers kept by unit tests of the
    source's cache.

  Neither route charges the tiles the table layer stages while a tiled
  column is read (up to 8 a column, #965); the source's law allows
  exactly those.
- **Refused** until a later step: a field with `EPHEMERIS_ID` (#942); an
  ephemeris-table phase centre (batch geometry follows a body Measures
  names, and a table directory named like a body would be followed as the
  body); `FLOAT_DATA`; several MeasurementSets; and a `WEIGHT_SPECTRUM`
  that only some selected rows define (the MSv2 adapter takes the column
  from its first cell, today's route from every selected row).
- **Evidence.** The route law (`casa_imaging_application::route_law`)
  images every T1 run on both routes and compares every product: all 200
  are bit-identical, the image runs with `usepointing` (AW over two
  windows, the mosaic gridder) among them. On T1.5 (the refim and kernels
  harnesses, the AW pair with `usepointing`, and CAS-7841 under the mosaic
  gridder with `usepointing`), 53 runs and 442 products agree to a
  normalized RMS of 1.3e-10 at most, again after each review's fixes.
  Receipts: NAS `casa-rs/issue-932/step4/`, `step4/review-fixes/`,
  `step4/review-fixes-2/` and `step4/review-fixes-3/` (the last at the
  head).

### 2.2 Geometry

Row geometry moves from `casa-ms` into imaging (`casa-imaging-model`, compiled
once, applied per batch), keyed by what a batch carries:

| Today, per MAIN row | On a batch |
| --- | --- |
| UVW rotation and phase shift per domain or facet | per (time, baseline) from the batch's stored UVW, rotated per (partition, field, time) |
| Parallactic angle | per (antenna, time), from the catalogue's antenna positions and the batch's times |
| Output-frame frequency conversion, cached per (field, time) | per (partition, field, time), the same cache key without the DDID |
| AW pointing pixel per (DDID, field, time) | per (partition, field, time) |
| Antenna class (mosaic or heterogeneous primary beams) | per antenna, from the catalogue |

Measures come from the authoritative types in `casa-types` and the bulk
conversion API that M1 trialled. The MS-specific helpers (`MsCalEngine`'s
frame paths, `spectral_selection.rs`'s prepared frequency factors) fold into
Measures as the plan requires, with no translation layer left behind.

**Law:** for every T1 and T1.5 workload, the batch geometry must equal today's
per-row geometry to 1e-12 relative, sample for sample. Only summation order
may differ.

*As landed in step 3b* (#932): `casa_imaging_model::GeometryPlan` compiles
once from the problem and the catalogue and evaluates each batch; the shared
arithmetic (casacore's `UVWMachine`, the prepared frequency conversion and
the position angle in `casa-types`; `GridFtProjection`, the pointing law and
the AW pointing groups in `casa-imaging-model`) moved out of `casa-ms`, whose
route now calls it. `casa_imaging_application::geometry_law` runs both
routes on one MeasurementSet for one request and compares every row, matched
by MAIN row number; a row matched twice, a row not matched and a NaN on
either side fail it. The T1 fixture checks it after every T1 run and two T1
laws add `usepointing` (AW over two windows, the mosaic gridder). On all 27
T1 runs (1.05 M rows) and 55 T1.5 runs on the Mac mini (5.59 M rows: 18
refim and 34 kernels cases; the two `mawproject` cases with `usepointing`;
CAS-7841; the four native-AW variants, which need the EVLA surface from
`~/.casa/data`, did not run), every quantity of the two routes on the same MeasurementSet is
bit-identical, inside the law's 1e-12 bound: projections, pointing,
parallactic angles, AW pointing pixels, aperture classes and output-frame
frequencies. An MSv4 conversion of the
same data is not bit-identical; 2.2.1 records what it changes. Receipts:
NAS `casa-rs/issue-932/step3b/`.

`GeometryPlan::apply` computes geometry for the samples its row mask names.
Today's route computes it for every selected row, `FLAG_ROW` rows included,
so step 4's mask keeps flagged rows and drops only padding (a NaN weight on
both stores) and the samples the selection excludes.

A moving phase centre follows a body or source Measures names. The two
others today's route takes, a field's own ephemeris (`TRACKFIELD`) and an
external ephemeris table, are refused when the plan compiles. A batch whose
frequencies convert into or out of the rest frame is refused too: that
needs the radial velocity only an ephemeris gives. `TRACKFIELD` comes with
the field ephemerides of #942; the external table is a request input, which
`GeometryPlan` must take before step 4 images through it. The compiled law
(`PhaseCentreLaw::Ephemeris`) names a body and a table alike, a table being
a directory of that name, so it must also say which it is by then: a table
directory named like a body (`Mars`) would otherwise be followed as the
body.

### 2.2.1 What geometry needs from the boundary

Added 2026-10-27, when step 3 began. Section 2.2 assumed that the catalogue
holds everything today's per-row geometry reads, and 2.4 relies on "the
catalogue's pointing". A map of today's geometry (`casa-ms` `derived/engine.rs`,
`selected_observation/access/geometry.rs`, `selected_pointing.rs`,
`ephemeris.rs`) found six inputs that the boundary does not carry. The
converter refuses or drops the same inputs, so section 3's decision 5 (MSv4
T1 at cutover) needs them on both stores.

| Input | Today | On the boundary now | Proposed |
| --- | --- | --- | --- |
| Field direction's reference epoch | A non-J2000 `PHASE_DIR` is converted to J2000 once, at `FIELD.TIME` and the observatory position | Constant term and frame only | `FieldEntry` gains the reference epoch. MSv4 phase centres are in celestial frames, so theirs is `None`. |
| Per-row direction references | `PHASE_DIR` with a variable reference column (`PhaseDir_Ref`, common in ALMA data), integer or string codes | Refused at opening | The MSv2 adapter resolves each field's frame. Fields in different frames are refused, since a data group has one UVW frame and MSv4 one phase-centre frame. |
| Field ephemerides | A field with `EPHEMERIS_ID` takes its direction from the attached table, offset by `PHASE_DIR`; the radial velocity feeds the frequency conversion | Refused by the converter; the catalogue ignores `EPHEMERIS_ID` | `FieldEntry` gains the ephemeris as MSv4's `field_and_source_ephemeris` holds it: geocentric samples (time, direction, distance, radial velocity) and the phase centre's offset from them. The MSv2 adapter reads the attached table. Imaging interpolates as today. |
| Antenna pointing | `PreparedSelectedPointingCatalog` finds, per (antenna, time), the covering, preceding and following POINTING rows, each evaluated at that time (polynomials included); the imaging law (covering, nearest, slerp, hold, reject) chooses | Not carried; the converter drops POINTING | A boundary query, `Observation::pointing`, returns those brackets per (antenna, time) for a partition. Each store answers natively: MSv2 evaluates its polynomial rows, MSv4 its `POINTING_BEAM` samples. The imaging law is unchanged. Nothing large enters the catalogue. |
| Receptor angles | Native AW compile keys `FEED` `RECEPTOR_ANGLE` by time and window | One vector per antenna | Per partition, as MSv4's per-partition `antenna_xds` holds them. The MSv2 adapter takes the partition's window rows and, where `FEED` varies in time for an antenna and window, the last of them, as casacore's `MSIter` does (owner decision, 2026-10-28; XRADIO's converter refuses them). |
| Window identity | Per-row `DATA_DESC_ID` and spectral window number key frequency conversions and per-(field, window) rules | A window name per partition | Geometry keys by partition. Window numbers for task selection belong to step 4. |

The batch already carries the rest: stored UVW and time centroids per
(time, baseline), field per time, and antennas per baseline. The catalogue
carries the antenna positions, mounts, dish diameters, telescope names, and
the partitions' frequencies and frames.

As landed in step 3a (trial README, "Geometry inputs"): field reference
epochs, per-row phase-centre frames, pointing and per-partition receptors.
Field ephemerides take a PR of their own, which must merge before the
cutover (#942); until then the converter refuses them, as it did.

Two consequences:
- **Pointing in MSv4.** XRADIO keeps one polynomial term per sample and
  defines no origin for more, so an MSv2 row with `NUM_POLY` > 0 converts as
  its direction at its own time, the further terms a reported loss; the MSv2
  adapter keeps today's evaluation. MSv4 states no interval per sample, so
  a converted store's brackets have no covering sample, and imaging's law
  chooses between the samples either side (nearest, or interpolation). For
  pointing sampled at the integration, that can pick a different sample
  than the MSv2 did. Step 3b measured it (#958), and the owner decided
  (2026-10-28, #946) that the MSv4 route's choice stands, with no
  intervals recovered. The converter reports `INTERVAL`, `TARGET`, `NAME`
  and `TRACKING` as a table loss. Today's covering rule is casa-rs's, not
  casacore's (#948), and the batch route keeps it (#958).
  *Measured in step 3b* (the batch geometry of an MSv2 against its MSv4
  conversion, `geometry_law::compare_stores`): on ALMA CAS-7841, whose
  `POINTING` is sampled every 48 ms, imaged with the mosaic gridder and
  `usepointing`, the MSv4 route's pointing differs from the MSv2's by at
  most 1.4e-10 rad (0.03 mas) and its UVW and phase shift by under 1e-15
  relative; on the EVLA `mawproject` pair under AW projection with
  `usepointing`, not at all. The native AW workloads need the EVLA surface
  from `~/.casa/data`, which the Mac mini lacks. The owner kept the
  boundary's tie rule for the cutover (2026-10-28, #946).
- **AW pointing epochs.** Today's epochs follow MAIN order, which interleaves
  windows; batches are partition-major. Refresh decisions carry state across
  consecutive epochs, so the order can change which epochs refresh. CASA's
  imager iterates by field, then window, then time. Step 3b instruments both
  orders against CASA before choosing, and the equality law runs on the
  native AW workloads.
  *As landed in step 3b*: the batch route carries the refresh state in
  delivery order, partition by partition and each in time order
  (`GeometryTraversal`). On every workload measured (T1's two-window AW with
  `usepointing`, the EVLA `mawproject` pair with `usepointing`) its pixels
  equal today's MAIN-order plan bit for bit. That is not so in general:
  today's plan forms an epoch per (data description, field, time) in MAIN
  order and counts its rows before channel windows skip any block, where
  the batch route counts each (partition, time)'s row mask. Where those
  counts differ (windows at one time holding different numbers of rows, or
  a window a run's channels skip), refresh decisions can diverge. Both
  casa-rs orders refresh at
  every field change; CASA's field-major order does not, so a multi-field AW
  mosaic with `usepointing` can group differently from CASA on either route.
  No acceptance workload images AW with `usepointing` over several fields
  (native-aw-evla runs without it); instrumenting CASA's epochs is #957.

### 2.3 Selection

The imager's selection is resolved once (`RowSelection`,
`casa-imaging-model/src/observation.rs:188`): fields, a UV-distance
interval (in wavelengths per window) and scan intents. A compiled predicate
applies it per MAIN row (`selected_observation/row_selection.rs:20`).
Windows, channels, correlations and times already map onto `AccessRequest`.
On a batch:
- field and intent are per time;
- UV distance is per (time, baseline), scaled by the partition's wavelength.

There are two options:
- **(a)** `AccessRequest` states field and intent selections, which the
  adapters apply to whole times, and imaging tests the UV distance as it
  packs;
- **(b)** imaging tests all three as it packs.

Option (a) skips the decoding of unselected times, which matters for a
mosaic imaged one field at a time. It also serves calibration's field and
intent selections. It belongs to packet (a), catalogue/selection and read
binding.

As landed in step 2, `AccessRequest::fields` selects fields, and the
adapters deliver the whole selected times in runs (trial README, "Field
selection"). Group keys are taken over the selected fields' times, as
production `gaincal` bins only its selected rows (the bin origin with fields
combined is #936). On MSv4 a chunk row holding
several runs is decoded once per batch (#935). Intents need no request
field: the adapters partition by intent, as XRADIO's converter does, so the
imager selects intents by the partitions it requests. The catalogue keeps
a partition's intents split and sorted, so step 4 matches an intent pattern
intent by intent, where CASA matches the whole `OBS_MODE` string; selection
by `STATE_ID` is not supported (contracts, section 4).

### 2.4 Compile without MAIN walks

The opening's index (the MSv2 adapter grids MAIN's key columns once, in row
chunks) and the estimate answer what compile walks MAIN for today:
- selected sample counts per partition;
- the spectral envelope, from the partitions' frequencies and the fields'
  times;
- the cube density padding;
- the first cross-correlation time, for native AW;
- the pointing domain, from the catalogue's pointing and the selected times.

Three walks become one index built at opening. The index's cost is the
trial's measured opening time; the baseline (section 5) must show the sum
does not regress.

### 2.5 Weights, `WeightCache`, errors

- **Weight slots.** `WeightCache` keeps one slot per delivered sample, in
  traversal order. Traversals are deterministic for a given request and
  batch size, so passes still meet samples in one order. The slot count comes
  from the estimate's valid samples after selection.
- **Errors.** A non-finite value names its (partition, time, baseline,
  channel, correlation) and the store, not a MAIN row.
- **`RowContext`** keeps field, window (now partition) and original UVW.
  `RowAddress` stays the MAIN row of today's route: batch-route rows are
  unaddressed, and the write-back resolves each block's rows from the
  provenance its source queues (2.6), since a block is one batch.

### 2.6 Write-back

`savemodel` and the continuum `CORRECTED_DATA` write through an
`observation_io` update session (step 5). A run that writes opens the MSv2
adapter as every batch run does (the opening grant, §2.1), resolves its
selection against it, then hands it to the session
(`UpdateSession::over_msv2`): every pass reads the batches the session's
updaters write back, and the MeasurementSet is opened and indexed once.

- **Targets.** `model` (`MODEL_DATA`) is created when MAIN lacks it,
  seeded with CASA's initial model, after clearing what CASA's
  `addScrCols` clears (#912), as `SelectedVisibilityWrite` does.
  `corrected` (`CORRECTED_DATA`) is never created: the continuum transform
  writes an existing column, and today's route also refuses a missing one.
  An MSv4 store cannot gain a group, so it writes an existing model group
  or is refused at opening; production reaches MSv4 only after §2.4.
- **Addresses.** The passes hand each block to their sink on the consumer
  thread, after the producer has refilled the batch's buffers, so the sink
  cannot hold the batch. A delivered batch gives an owned `BatchAddress`
  (`VisibilityBatch::address`): its catalogue, partition, times, channels,
  products and weight rule, without its payload. An updater writes against
  an address (`Updater::write_at`) as it writes against the batch, except
  for a change whose check reads the payload: an MSv4 store knows its
  padding only from the batch's weights, so a flag cleared or a weight
  written by address is refused on every store.
- **Masks.** `PatchValues::Visibilities` carries `written`, a `[sample,
  channel, product]` mask: an element it leaves keeps its stored value, and
  a sample it leaves wholly is neither read nor written. Imaging leaves
  padding, samples outside the UV selection, the channels of a batch's span
  the selection leaves out (a gapped `spw`), the fit-only channels of a
  continuum transform and the products not routed: the cells today's route
  writes, flagged samples included. One difference: the source knows
  padding by its values (every flag set and every weight not finite, as
  both adapters pad), so a real row that is wholly flagged with no finite
  weight is left as padding is. Today's route writes its prediction; the
  row contributes nothing to any image either way.
- **Destinations.** A session opening an MSv2 refuses, before anything is
  cleared or written, a target column that cannot be written in place in
  bounded memory, as today's route refuses it: an existing column outside
  `TiledShapeStMan` and `TiledColumnStMan`, or a column to create whose
  clone of `DATA`'s manager would be one (`scratch_columns`). A column in
  another manager is saved by rewriting every row of its storage group,
  once a batch. The first updater applies the rule again under MAIN's lock,
  as today's writer checks when it begins, so a column another writer adds
  meanwhile is refused too.
- **Order.** An updater takes its traversal's batches in any order and any
  of them; the times it is not given keep what they hold. MSv4 updaters
  already did. The MSv2 updater's delivery-order check and its refusal, at
  finish, of unwritten times are gone: they guarded a write planned up
  front, which neither store makes. A batch with no imaged sample yields no
  block, and nothing is written for it.
- **Provenance.** A batch route block is one batch. While the writing
  traversal runs, the source queues, in delivery order, each partition it
  binds (its request) and, for each block it fills, the batch's address and
  the batch sample of each row. The runtime hands a writing pass's sink
  every block once, in source order (`VisibilitySink`), so the sink takes
  the queue's head. It checks the head against the block (one row per
  recorded sample, each on its sample's baseline) and fails the pass on a
  mismatch. At most the stream's blocks are queued
  (`casa_imaging_runtime::pass::STREAM_SLOTS`).
- **One updater a partition.** The batch route binds one request per
  partition. The sink binds a partition's updater when its request reaches
  the queue's head, before it finishes the last partition's. Updaters
  overlap, so the MSv2 session holds MAIN's write lock from the first
  updater (bound when the first block reaches the sink, which creates and
  seeds a missing `MODEL_DATA`) to the end of the pass.
  `SelectedVisibilityWrite` takes it, and creates the column, when the
  writer begins.
- **Reads beside writes.** The source's producer reads the next batches
  while the sink writes the last, on one MeasurementSet in one process,
  where file locks do not keep them apart. Each save rewrites `table.dat`,
  so the MSv2 observation reads what it needs from it (each data group's
  tile shape) once, when it opens, and an estimate reads no file the saves
  rewrite. The multi-partition write test found the read that did: about
  one run in fifteen tore it. That test also found that the pass runtime
  dropped a source's failure when its producer stopped first. The stream
  now reads everything the producer sent (`pass_laws`
  `a_source_failing_midway_fails_the_pass`).
- **Admission.** The write stays a `Residency::All` pass. Its charge (today
  `SelectedVisibilityWrite::retained_bytes`) is, on the batch route, the
  largest of each term over the partitions (`BatchRoute::write_bytes`),
  since the sink's buffers keep one partition's capacity while another's
  updater writes:
  - what its updater holds writing its largest batch
    (`UpdateSession::write_bytes`): on MSv2 each present row's cell of each
    target, as built or merged and as moved into the table, its tiles, and
    the seeding of a created `MODEL_DATA` in batches of at most the
    traversal's batch bytes; on MSv4 one partition's windows and the chunks
    they replace;
  - the patch the sink assembles (each column's values and the mask over
    the batch's samples, channels and products);
  - the provenance of the blocks in flight.

  `casa-ms`'s `msv2_update_memory` holds the updater's bound (created and
  seeded, masked and merged, two targets). The whole run's law
  (`a_visibility_write_stays_within_its_admission`) runs on both routes.
  The source's law (`batch_source_memory`) drives writing passes too, one
  of them over several batches of a partition.
- **Evidence.** The route law images a writing run's batch route on a copy
  of its MeasurementSet taken before either route writes
  (`route_law::copy_for_batches`, `<vis>.batches`), then compares each
  column written over every cell, 1,024 rows at a time. The copy needs
  the MeasurementSet's size again on disk; `imager_routes` removes it when
  the law holds.

The MSv2 counting law found two defects in the trial updater, both fixed in
this step. It read the cells it merges through the table's row cache, which
loads every row of MAIN (ADR-0008); merges now read past it, in one call per
batch. It seeded a created `MODEL_DATA` from two lists of every MAIN row;
seeding now goes a partition's rows at a time. The selected-row mutation
also moves its batch's cells into the table rather than cloning them, which
saves a copy of each cell on every MSv2 mutation writer.

### 2.7 What is removed at cutover

The cutover PR deletes all of the following:
- `MeasurementSetSource`;
- the selected-observation access, buffer, content plan and walk modules;
- `SelectedRows` and the row-based model types;
- `SelectedVisibilityWrite` (once nothing else uses it);
- the `casa-ms → casa-imaging-model` edge. Step 3b deepened it: today's
  route in `casa-ms` now calls `GridFtProjection`, the pointing law and the
  AW pointing groups from the model, and those callers go with the route.

The imaging dependency checker's grandfathered lists shrink accordingly.

## 3. Owner decisions

All five were decided on 2026-10-27, each as proposed below: imaging takes
`casa-observation` and `casa-ms` drops `casa-imaging-model`; geometry lives
in `casa-imaging-model`; selection is option (a); the thresholds are as
stated; and MSv4 imaging is part of the cutover's acceptance.

1. **Dependency direction (an Ask First item).**
   - Imaging gains `casa-observation` as a normal dependency:
     `casa-imaging-model` (geometry over catalogue types) and
     `casa-imaging-application` (the source).
   - `casa-ms` loses its normal dependency on `casa-imaging-model`.
   - This is D4's proposed amendment. `dependency-policy.json` needs its
     exact-set edits.
2. **Where geometry lives.** In `casa-imaging-model`, as D4 says (proposed
   here), or in a new imaging crate. The proposal keeps one crate.
3. **Selection (2.3).** Option (a) puts field and intent selections in
   `AccessRequest` (packet a) before this port; option (b) tests everything
   in the source. The proposal is (a).
4. **Blocking thresholds (section 5).** The proposal:
   - wall time at most +2% or two baseline standard deviations, whichever
     is larger, per workload and worker count;
   - peak RSS at most +5%;
   - images and models equal to today's to a normalized RMS of 1e-6.
5. **MSv4 imaging at cutover.** It comes almost free through
   `&dyn Observation`. The proposal makes it part of the acceptance (T1 on
   the trial's MSv4 conversions) rather than a later packet.

## 4. Order of work

Each step is its own PR with its own laws. Only the last switches production.

1. **Baseline (M0 for this packet).** The comparison harness and the
   recorded baseline of today's route (section 5). No code change to
   imaging.
2. **Selection on the request** (packet a, if decision 3 is (a)): field and
   intent selections, with laws on both stores.
3. **Geometry over batches** in imaging, beside today's, in two parts
   (2.2.1):
   - **3a. Boundary inputs for geometry.** Field reference epochs, per-row
     direction references, pointing brackets and per-partition receptor
     angles on both adapters; the converter carries pointing both ways.
     Laws: each input equals today's reading on MSv2 and survives an
     MSv2 → MSv4 → MSv2 round trip. Field ephemerides follow in their own
     PR before the cutover (#942).
   - **3b. Geometry** in `casa-imaging-model`. The equality law of 2.2 runs
     both routes on every T1 workload.
4. **`ObservationSource`** beside `MeasurementSetSource`, behind a run
   option. T0, T1 and T1.5 run on both. Field ephemerides (#942) land first,
   or the source refuses a field with `EPHEMERIS_ID` until they do: batch
   geometry would read its `PHASE_DIR` offset as a direction.
5. **Write-back** through update sessions. The model-column T1 tests and the
   counting law run on both routes. Its timing, on the write-back workload
   `standard-mfs-model-data-vla` among the rest, is step 6's blocking
   comparison (section 5).
6. **Cutover.**
   - The option's default flips, and 2.7's routes are removed in the same PR.
   - The blocking comparison must pass.
   - It needs an independent review of the persistence and the hot loop.

## 5. Baseline and blocking comparison

**Workloads** come from `resources/imaging-architecture/representative-science-matrix.json`:
- `standard-mfs-dirty-vla` (6.28 M samples), `standard-mfs-hogbom-vla` and
  `standard-mfs-model-data-vla` (write-back);
- `spectral-cube-dirty-vla` and `spectral-cube-clean-vla`;
- `continuum-subtracted-cube-alma`;
- `mosaic-alma`;
- `w-multifield-cube-vla`;
- `native-aw-evla` (pointing);
- `multidomain-outlier-vla` (domains);
- T1 (`cargo test -p casars-imager --test t1`) and the T1.5 refim cases.

**Measured per run:** from each `<imagename>.summary.json`, the per-phase
seconds, `process_peak_rss_bytes` and `peak_reserved_bytes`; and the
admitted block size (today's `rows_per_block`, then batch times). Each run
also records the commit, FFTW link, backend, precision, worker counts and
cache state, as `TESTING.md` "Imaging tiers and evidence" and
`.agents/skills/casa-rs-imaging-performance` require.

**Protocol:**
- A B B A on the Mac mini, with one worker beside four at every bar.
- The baseline is today's route at the commit the cutover branches from; it
  is re-run if `main` moves.
- The noise band comes from the baseline's own repeats.

**Harness:** `tools/perf/imager/run_workload.py` already runs a workload
against CASA (`scripts/bench-imager-vs-casa.sh`). It gains one mode that runs
two casa-rs builds on the same inputs and writes one comparison receipt per
workload: medians, the band, verdicts, and images compared by normalized
RMS. Receipts go where `docs/agent-reference.md` puts evidence (small
receipts under `casa-rs-evidence/imaging-port/`, a copy on the NAS),
summarized on the PR. Inputs are not copied off the Mac mini.

**Blocking:**
- A workload outside its thresholds (decision 4) blocks the cutover, whatever
  correctness says.
- A faster route is recorded, not a reason to change thresholds.

## 6. Risks

- **Opening index cost.** Gridding MAIN's keys at opening replaces three
  walks. On a large MS with a narrow selection it may cost more than today's
  replay of the selected rows. The trial measured it on `pilot.ms` only; step
  1 measures it on the T2 inputs.
- **Padding.** A partition whose times have few baselines (subarrays) pads
  many cells. Packing drops them, but the batch buffers are sized for the
  padded grid. The estimate reports the padded bytes, so admission sees them.
- **One producer thread.** Today's producer reads in row order. Batches allow
  parallel cursors (the trial's work units), but the design keeps one
  producer and leaves parallel production to a measured follow-up.
- **Geometry drift.** Moving the measures paths can move UVW or frequencies
  by rounding. The 1e-12 equality law and T1.5 against CASA bound it.
  Reproducibility of the last bit is not a goal (AGENTS.md).
