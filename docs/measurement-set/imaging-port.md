# Imaging on the shared observation batches (M2 packet c)

Status: accepted by the owner on 2026-10-27 (section 3); no code changes yet
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
| Receptor angles | Native AW compile keys `FEED` `RECEPTOR_ANGLE` by time and window | One vector per antenna | Per partition, as MSv4's per-partition `antenna_xds` holds them. The MSv2 adapter takes the partition's window rows and refuses a `FEED` that varies in time for an antenna and window, as XRADIO's converter does. |
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
  than the MSv2 did. Step 3b measures it on the native AW workloads; the
  owner decides whether the MSv4 route may differ there, or whether
  intervals should be recovered (from the sample spacing, say). The
  converter reports `INTERVAL`, `TARGET`, `NAME` and `TRACKING` as a table
  loss. Today's covering rule is casa-rs's, not casacore's (#948); step 3b
  settles which the batch route keeps.
- **AW pointing epochs.** Today's epochs follow MAIN order, which interleaves
  windows; batches are partition-major. Refresh decisions carry state across
  consecutive epochs, so the order can change which epochs refresh. CASA's
  imager iterates by field, then window, then time. Step 3b instruments both
  orders against CASA before choosing, and the equality law runs on the
  native AW workloads.

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
- **`RowContext`** keeps field, window (now partition) and original UVW. Its
  `RowAddress` becomes a batch-slot address that the write-back resolves.

### 2.6 Write-back

`savemodel` and the continuum `CORRECTED_DATA` write through an
`observation_io` update session. The prediction sink assembles each batch's
model values and patches them with `PatchValues`:
- the MSv2 session creates `MODEL_DATA` with CASA's initial model, as
  `SelectedVisibilityWrite` does, and clears what CASA's `addScrCols` clears
  (#912);
- an MSv4 store, which cannot gain a group, writes an existing model group
  or refuses at opening.

The write stays a `Residency::All` pass, admitted as today. Its counting law
(`a_visibility_write_stays_within_its_admission`) is kept, restated for
batches.

### 2.7 What is removed at cutover

The cutover PR deletes all of the following:
- `MeasurementSetSource`;
- the selected-observation access, buffer, content plan and walk modules;
- `SelectedRows` and the row-based model types;
- `SelectedVisibilityWrite` (once nothing else uses it);
- the `casa-ms → casa-imaging-model` edge.

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
   option. T0, T1 and T1.5 run on both.
5. **Write-back** through update sessions. The model-column T1 tests and the
   counting law run on both routes.
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
