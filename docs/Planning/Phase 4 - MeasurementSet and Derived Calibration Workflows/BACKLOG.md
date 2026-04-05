# MeasurementSet Closeout Inventory

Catalog of remaining gaps for practical MeasurementSet workflows using
`ms/MeasurementSets`, `derivedmscal`, and selected `MSSel`/TaQL integration.

---

## Status Legend

| Tag | Meaning |
|-----|---------|
| **IMPLEMENT** | Will be built in Phase 4 waves |
| **DEFER** | Out of scope for Phase 4 (reason given) |

---

## Extracted Into Waves

Items `1.1`-`8.3` and `12.1`-`12.3` were extracted into Phase 4 Waves 1-10.
Wave 11 extracted the non-public executor core from `12.4`; public `casars`
registration, `calwt`, and broader parity/benchmark closeout remain in backlog
item `12.4`.
Wave 12 extracted the public `calibrate` app registration and schema-backed CLI
surface from `12.4`; `calwt` and broader CASA parity/benchmark closeout remain
in backlog item `12.4`.
Wave 13 extracted the first real CASA `applycal` parity harness from `12.4`;
`calwt` and performance benchmarking remain in backlog item `12.4`.
Wave 14 extracted the real-MS benchmark harness from `12.4`; follow-on
performance optimization is still required because Rust is currently slower
than the 2x threshold on the benchmarked `applycal` workload, and `calwt`
remains in backlog item `12.4`.
Wave 15 extracted internal timing instrumentation from `12.4`; the benchmarked
`applycal` path is dominated by planning, `CORRECTED_DATA` seeding, and
MeasurementSet save overhead rather than the per-row correction kernel, and
`calwt` remains in backlog item `12.4`.
Wave 16 extracted the first fixed-cost optimization pass from `12.4`; opening
the MeasurementSet only once and saving only the mutated main table cut the
benchmarked Rust median from `0.86s` to about `0.72s`, but planning and main
table save remain the dominant costs and `calwt` remains in backlog item
`12.4`.
Wave 17 extracted a second apply optimization pass from `12.4`; skipping the
duplicate full-table validation before the main-table save cut the benchmarked
Rust median further to about `0.60s`, and finer planner timings showed the
remaining planning cost is overwhelmingly inside `MsSelection::apply`. Follow-on
selection experiments using a slot-indexed row scan and the TaQL path both
failed to beat the current structured selection path, so the remaining
performance gap is now primarily a storage-layer main-table persistence problem,
and `calwt` remains in backlog item `12.4`.
Wave 18 extracted `calwt` support, CLI exposure, synthetic `WEIGHT` /
`WEIGHT_SPECTRUM` regression coverage, and real CASA `applycal` weight parity
from `12.4`; the remaining work in `12.4` is performance follow-up on the
benchmarked apply path.
Wave 19 extracted the first limited `gaincal` solver cut from `12.5`: synthetic
`G` / `T` phase-only solves now write CASA-readable caltables that round-trip
through the Rust apply path, and the real-MS `G`-table path now passes
downstream parity against CASA `gaincal` via CASA `applycal` on `ngc5921.ms`.
The accepted contract for this first solver wave is downstream
`CORRECTED_DATA` / flag / weight agreement at the current solver-specific
tolerance, not raw caltable equality.
Wave 20 expanded `12.5` to the first `calmode='ap'` slice: synthetic `G` / `T`
amplitude-plus-phase solves now round-trip through the Rust apply path, and the
real-MS downstream parity harness now includes a dedicated CASA
`gaincal(..., calmode='ap')` case. That real-MS `ap` parity case now passes;
the key fix was to let the reference-antenna amplitude float during `ap`
iteration while continuing to pin only its phase. Broader `solint`,
prior-caltable preapply, and stronger solver factoring remain open under
`12.5`.
Wave 21 extracted the first broader `solint` and prior-caltable preapply slice
from `12.5`: the limited `gaincal` surface now supports
`solint='inf'|'int'|<seconds>` plus prior-caltable preapply through the same
apply planner/executor stack used by `calibrate`. Synthetic downstream tests
now cover integration-bucket solves, fixed-seconds solve grouping, and
residual solves against a supplied prior caltable, and the real-MS downstream
parity harness is now green for both `gaincal(..., solint='int')` and
`gaintype='T'` solves with prior `G` preapply. The remaining leverage under
`12.5` is solver factoring and broader solve semantics rather than baseline
`solint` / preapply support.
Wave 22 extracted the first solver-factoring cleanup from `12.5`: the limited
`gaincal` implementation is no longer a single monolithic module. Grouping and
selection logic now live separately from the numerical solve kernel and from
caltable writing, while the public solve surface and the existing synthetic and
real CASA downstream parity contracts remain unchanged. This closes the first
requested factoring step and lowers the risk of future work under `12.5`
expanding one handwritten file indefinitely. Remaining leverage is now broader
solve semantics and any future numerical-backend cleanup.
Wave 23 extracted the first `bandpass` slice from `12.6`: a library-first
`B Jones` solver with `solint='inf'`, explicit refant, prior gain-table
preapply, and CASA-compatible channelized caltable output. Synthetic downstream
tests are green, and the real-MS downstream parity harness is now green for
`bandpass(..., bandtype='B', solint='inf', gaintable=[prior G])` when the
resulting Rust table is applied in CASA alongside the same prior gain table.
Deferred under `12.6` for now are `solnorm`, `combine=*`, `BPOLY`, and the
inspection/statistics follow-ons.
Wave 24 extracted the first `calstat` slice from `12.6`: a library-first
statistics surface over calibration tables with global and grouped stats by
field, spectral window, antenna, and observation. The first cut supports
CASA-style complex axes (`amp`, `phase`, `real`, `imag`) over `CPARAM` plus
real-valued column stats, reports flagged-value counts separately, and has real
CASA parity coverage for global `calstat` amplitude statistics on a generated
phase-gain table. Deferred under `12.6` are `fluxscale`, `solnorm`,
`combine=*`, `BPOLY`, and broader inspection/UI work.
Wave 25 surfaces that same stats capability through the public `calibrate`
binary as `calibrate stats`, keeping the application layer thin over the
existing library API. The first CLI cut supports text or JSON output, axis and
datacolumn selection, and flag inclusion control while preserving the same slow
CASA parity contract for the underlying stats engine. The schema-driven
`casars` app surface still centers the apply workflow; broader stats-specific
form/schema work remains deferred under `12.6`.
Wave 28 extracts the first `gainfield='nearest'` slice from `12.7`: apply-plan
resolution now supports per-MS-field nearest calibration-field selection using
FIELD phase directions, and the executor uses that resolved mapping during
calibration lookup. Synthetic planner/executor regressions are green, and slow
real-MS parity is now closed against CASA `applycal(..., gainfield='nearest')`
on a generated multi-field gain-table case.
Wave 29 extracts the first broader solve semantics slice from `12.5` by adding
`combine='scan'` support to the limited `gaincal` path. Synthetic grouping
tests are green, and slow real-MS downstream parity is now closed against CASA
for `gaincal(..., combine='scan')` followed by `applycal`.
Wave 30 extracts the first `callib` slice from `12.7`: `casa-calibration` now
parses a narrow CASA callibrary file surface into the existing apply-plan/apply
stack, including `caltable`, `calwt`, per-entry `field`/`spw`/`obs`
applicability, `fldmap`, `spwmap`, and supported interpolation modes. Fast
tests cover relative-path resolution and per-entry applicability, and slow
real-MS parity is now closed against CASA `applycal(docallib=True, callib=...)`
on the existing `ngc5921.ms` phase-gain workload.
Wave 31 extracts `parang` from `12.7` into the implemented calibration
surface. The apply planner/executor now supports top-level
parallactic-angle correction for circular-feed layouts, including
mount-specific behavior through the derived-calculation engine. Slow
real-MS parity is now closed against CASA for baseline and BWG-mount
`applycal(..., parang=True)` cases, plus downstream `gaincal(..., parang=True)`
and `bandpass(..., parang=True, gaintable=[prior G])` workflows.
Wave 32 extracts the first float-parameter apply slice from `12.7` by adding
`FPARAM` support for `K Jones` delay tables to the apply path. The accepted
scope is intentionally narrow: read existing CASA `K` tables, materialize the
delay term as a diagonal complex Jones factor using the caltable spectral-window
pivot frequency, and apply it through the same `applycal`-class executor as
complex tables. Synthetic planner/executor tests are green, and slow real-MS
parity is now closed against CASA for `gaincal(..., gaintype='K')` followed by
`applycal` on `ngc5921.ms`.
Wave 34 closes the first `bandpass(..., combine='scan')` slice under `12.6`.
`casa-calibration` now groups `B Jones` solves across scan boundaries when
requested, preserves that behavior through the developer CLI, and checks the
result against CASA by applying both CASA- and Rust-produced `B` tables in CASA
on the multi-scan `field=1, spw=0` subset of `ngc5921.ms`. This wave also
fixes the shared CASA `bandpass` helper used by the slow parity suite and
revalidates the broader `bandpass` parity surface (`B`, `solnorm`, `parang`,
and `BPOLY`) with the corrected helper.
Wave 35 closes the remaining first-wave `bandpass combine=*` semantics by
extending the `B Jones` solver to `combine='field'` and
`combine='scan,field'`. Synthetic multi-field solves are green, and slow CASA
downstream parity is now closed for both combined-field variants on
`ngc5921.ms`. The accepted `combine='field'` contract uses a looser
solver-specific downstream tolerance than the tighter baseline `B`/`scan`
cases because the available real-MS workload does not pool multiple fields
inside the same scan bucket, so the comparison is dominated by the noisier
field-1 bandpass solve rather than a clean metadata-only distinction.
Wave 36 extracts the first structured launcher/UI slice for `calibrate`.
`casa-calibration` now exposes a managed-output envelope for the public
workflows, the `calibrate` schema declares `calibration-report-v1` managed
output for `casars`, and the launcher Overview tab now renders compact
structured summaries for apply, summary, plan, stats, solve, and fluxscale
results. This closes the first evaluation-focused `calibrate` UI surface
without moving calibration logic into the launcher.
Wave 37 extracts one more planner-side performance pass from `12.4` by
collapsing structured MS selection onto a single row scan instead of one
full-row scan per requested scalar column. The change preserves the existing
selection semantics and trims the benchmarked `planning_selection` median from
roughly `0.20s` to `0.19s`, but the end-to-end `applycal` ratio is still about
`6.5x` slower than CASA because MAIN-table save remains the dominant cost.
This closes the plausible local planner optimizations; the remaining leverage
under `12.4` is now deeper persistence work rather than more selection tweaks.

---

## Deferred To Later Phases

### 9.1 Full `MSSel` parser/grammar parity

**Status:** DEFER

**Reason:** Existing TaQL and typed selectors cover practical workflows; full
grammar cloning is large and lower leverage.

---

### 9.2 Full `msfits` parity

**Status:** DEFER

**Reason:** Depends on broader FITS and coordinate projection parity and is a
distinct risk boundary.

---

### 9.3 Full `DirectionCoordinate`/WCSLIB projection parity

**Status:** DEFER

**Reason:** Deferred in Phase 3 and still orthogonal to core MS table workflows.

---

### 10.1 Observatory-specific or lossy MS storage managers

**Status:** DEFER

**Reason:** Specialized deployment needs and significant standalone scope.

---

### 10.2 Full `derivedmscal` catalog and CASA task parity

**Status:** DEFER

**Reason:** Phase 4 targets core derived quantities and UDF hooks; full catalog
can expand after demand is clear.

---

### 11.1 MeasurementSet Plot Workspace Infrastructure in `casars`

**Status:** IMPLEMENT

**Reason:** Add a reusable `Plots` workspace to the MeasurementSet TUI so one
application can host multiple plot kinds with a catalog, canvas, and controls
pane tied to the last successful summary run.

---

### 11.2 Shared `PlotSpec`, CLI Plot Mode, and Plot Export Contract

**Status:** IMPLEMENT

**Reason:** Keep CLI and TUI plotting in parity by driving both through shared
plot specification types plus explicit `PNG` / raster-backed `PDF` export
support.

---

### 11.3 Initial Metadata Plot Catalog (`UV`, Antenna Layout, Scan Timeline, SPW Coverage)

**Status:** IMPLEMENT

**Reason:** These four plots exercise both richer TUI layout work and reusable
plot rendering without pulling raw visibility extraction into the first wave.

---

### 11.4 Raw Visibility Plots (`Amplitude vs Time`, `Phase vs Time`, `Amplitude vs UV Distance`)

**Status:** IMPLEMENT

**Reason:** The shared plot substrate now supports the most common curated
raw-MAIN-table views used from CASA `plotms`, using shared MeasurementSet
selection semantics plus vector-averaged visibility extraction from MS data.

---

### 11.5 Advanced Plot Interactions and Generic `plotms`-Style Axis Engine

**Status:** DEFER

**Reason:** Free-form axis selection, pan/zoom, and broader interaction afford a
different level of complexity than the curated first-wave catalog. Stabilize the
workspace and export contract before generalizing it.

---

### 11.6 True Vector Plot Export (`PDF` / `SVG`)

**Status:** DEFER

**Reason:** Raster-backed single-page PDF export is enough for the first wave.
True vector export should wait until the rendering abstraction is stable and the
required backend behavior is well understood.

---

### 11.7 Dense Dual-Y `msexplore` Readability and Stacked Alternatives

**Status:** IMPLEMENT

**Reason:** CASA-style dual-y overlays can become unreadable on dense
MeasurementSet plots with repeated timestamps or heavily overplotted samples.
The current `msexplore` dual-y support is semantically correct but can still
collapse visually even after marker/color differentiation. Follow-on work should
add multi-panel stacked alternatives for common paired views (for example
amplitude over phase vs time), plus density-aware warnings or fallback
presentation modes where overplotting hides the primary series.

---

### 11.8 Staged `msexplore` Flag Editing and CASA Parity Contract

**Status:** IMPLEMENT

**Reason:** `plotms`-style flag editing is the next major functional gap after
page/layout parity, but the acceptance contract must be stronger than visual
inspection alone. The implementation should:

- resolve rectangular edit regions against exact plotted-sample provenance
  (`row`, correlation, channel/bin) before any writeback occurs
- stage edits first, with explicit preview and explicit apply/discard
- write only MAIN `FLAG` / `FLAG_ROW`; do not treat `FLAG_CMD` as the source of
  truth for this wave

The test strategy must be split deliberately:

- **CI-runnable tests:** synthetic-MS coverage for region-to-sample planning,
  `extcorr` / `extchannel` expansion, preview/apply/discard behavior, and exact
  `FLAG` / `FLAG_ROW` writeback
- **Local CASA parity:** copy a real MS fixture, apply the Rust-staged edit to
  one copy, apply the same resolved sample edits to another copy using CASA
  table tooling, and compare exact `FLAG` / `FLAG_ROW` deltas plus before/after
  plotted-data manifests

Because the installed `casaplotms` RPC surface is scriptable for plot setup but
not for GUI-region replay, automated parity should compare resulting MeasurementSet
state and post-edit plotted data, not synthetic GUI clicks. Keep at least one
manual GUI spot check per edit behavior family (`flag`, `unflag`, `extcorr`,
`extchannel`) as a secondary confidence measure, but not as the main acceptance
path.

---

### 11.9 Dense-Plot Performance Beyond the Hard Point Cap

**Status:** DEFER

**Reason:** `msexplore` now enforces a hard request-level point cap so very
dense plots fail fast instead of trying to render arbitrarily many markers, but
that is only a safety rail. Follow-on performance work should replace the blunt
cap with smarter handling for dense plots:

- density-aware raster rendering for very large scatter clouds
- optional decimation or binning strategies that preserve obvious structure
- panel-parallel render/build execution for iterated and multi-plot pages
- lower-allocation grouping keys in the scatter builder hot path

---

### 12.2 `casa-calibration` Substrate with Permissive-Read / Strict-Write Policy

**Status:** IMPLEMENT

**Reason:** Build the new library crate that owns calibration-table IO,
normalized metadata, indexing, interpolation, apply logic, and later solve
logic, while keeping `casars` thin.

---

### 12.3 Apply Planner for Complex CASA Caltables

**Status:** IMPLEMENT

**Reason:** Separate table-chain resolution, `gainfield`, `spwmap`,
interpolation, and scratch-column planning from row execution so trial mode,
parity diffs, and multithreaded execution stay deterministic.

---

### 12.4 Public `calibrate` App with `applycal`-Class Execution

**Status:** IMPLEMENT

**Reason:** The first public release should actually calibrate an MS by
applying existing complex caltables and creating `CORRECTED_DATA` when absent.
Waves 11-13 cover the internal executor core, public app registration, and
first end-to-end CASA `applycal` parity. Wave 14 added the benchmark harness and
showed Rust is still well over the 2x slowdown threshold on the benchmarked
workload. Wave 15 added internal timing instrumentation and showed the dominant
costs are planning, `CORRECTED_DATA` seeding, and MeasurementSet `save()`. Wave 16
removed the duplicate MS open and subtable rewrites, which cut
`CORRECTED_DATA` setup sharply and improved end-to-end runtime, but planning
and main-table save still dominate the benchmarked workload. Wave 17 skipped
the duplicate main-table validation pass and added finer planner timing
breakdown, which isolated `MsSelection::apply` as the dominant planning
subphase but also showed that neither a slot-indexed row scan nor the TaQL path
outperformed the current structured selection implementation on the benchmarked
workload. The remaining path to parity now looks like deeper main-table
persistence work rather than another localized planner tweak. Wave 18 added
`calwt` support to the executor and CLI, aligned the weight path with CASA's
`resetWeightsUsingSigma()`-style behavior when `WEIGHT_SPECTRUM` exists, and
added real CASA parity coverage for `WEIGHT`; the remaining work is
performance-oriented rather than functional. A follow-on experiment to rewrite
MAIN with `StandardStMan` or mixed per-column storage-manager bindings was
rejected rather than landed: the current table-writer path still reports a
complex-array mismatch on those non-default save modes, so the remaining
performance leverage stays in deeper persistence work instead of a local
storage-manager flag flip.

Cold-start performance handoff for follow-on investigation:

- Reproduce with:
  `CAL_BENCH_REPEATS=2 scripts/bench-calibrate-vs-casa.sh`
- Current benchmarked workload:
  real `ngc5921.ms`, `field=0`, `spw=0`, timing excludes MS copy and caltable
  generation.
- Current representative numbers:
  Rust median `~0.59s`, CASA median `~0.091s`, ratio `~6.5x`.
- Current representative Rust timing breakdown:
  `planning_selection ~0.19s`, `save ~0.35s`, row compute `~0.007s`,
  row writeback `~0.001s`.
- Already tried and rejected as insufficient or broken:
  TaQL selection path, slot-indexed row scan, repeated local planner tweaks,
  `StandardStMan` rewrite, and mixed per-column storage-manager save bindings.
- Most likely file targets for the remaining work:
  `crates/casacore-ms/src/selection.rs`,
  `crates/casacore-ms/src/ms.rs`,
  `crates/casacore-tables/src/table/io.rs`,
  `crates/casacore-tables/src/storage/mod.rs`.
- Working hypothesis:
  the remaining parity gap is primarily MAIN-table persistence cost in
  `casacore-tables`, not calibration math, not row writeback, and no longer
  a high-leverage planner problem.

---

### 12.5 Limited `gaincal` (`G` / `T`, `p|ap`)

**Status:** IMPLEMENT

**Reason:** Solving should begin only after the caltable substrate and apply
path are trustworthy, using the same on-disk compatibility contract and
downstream parity checks. Keep the solve math behind library APIs, and add a
follow-on cleanup wave to split the numerical kernel from MS selection / table
IO / caltable writing. Reevaluate then whether parts of the solve path should
move onto a stronger reusable linear-algebra backend instead of expanding a
monolithic handwritten solver module. The current accepted scope now covers
`gaintype=G|T`, `calmode='p|ap'`, `solint='inf'|'int'|<seconds>`, explicit
refant, `smodel=[I,0,0,0]`, and prior-caltable preapply through the existing
apply planner/executor stack. Synthetic downstream tests are green for the
current `G` / `T` surface, including integration-bucket solves, fixed-seconds
grouping, and residual solves after prior calibration. The real-MS downstream
parity harness is now green against CASA for:

- `gaincal(..., gaintype='G', calmode='p', solint='inf')`
- `gaincal(..., gaintype='G', calmode='ap', solint='inf')`
- `gaincal(..., gaintype='G', calmode='p', solint='int')`
- `gaincal(..., gaintype='T', calmode='p', solint='int', gaintable=[prior G])`

Follow-on work under this item should now focus on stronger solver factoring,
broader solve semantics such as `combine=*`, and any future numerical-backend
cleanup rather than basic `p` / `ap`, `solint`, or preapply compatibility.
Wave 22 landed the first factoring pass by splitting the implementation into
separate grouping, kernel, and writer modules while keeping the public solve
API and the current downstream CASA parity coverage unchanged. Wave 29 then
lands the first broader solve semantics slice by supporting `combine='scan'`
through the same downstream parity contract. The accepted real-MS parity
surface under `12.5` now also includes
`gaincal(..., gaintype='G', calmode='p', solint='inf', combine='scan')`.
Wave 31 further extends that accepted parity surface to
`gaincal(..., gaintype='G', calmode='p', solint='inf', parang=True)`.

---

### 12.6 `bandpass`, `calstat`, and `fluxscale`

**Status:** IMPLEMENT

**Reason:** These workflows build naturally on the same caltable substrate but
carry broader scope and stricter workflow dependencies than the first apply
release. Wave 23 lands the first narrow `bandpass` cut as a library-first
`B Jones` solver with:

- `bandtype='B'`
- `solint='inf'`
- explicit refant
- prior gain-table preapply
- point-source `smodel=[I,0,0,0]`
- CASA-compatible channelized `CPARAM` output

The accepted contract for this first `bandpass` slice is downstream behavior:
synthetic residual correction through the Rust apply path and real-MS parity
against CASA by applying both CASA- and Rust-produced `B` tables in CASA
alongside the same prior `G` table. Follow-on work under `12.6` initially
focused on `solnorm`, `combine=*`, `fluxscale`, `BPOLY`, and any future
inspection surface rather than baseline `B`-table compatibility. Wave 24 adds
the first `calstat` slice as a library-first statistics/reporting surface over
calibration tables. It supports global and grouped stats by field/SPW/antenna/
observation, complex-axis transforms over `CPARAM`, and real-valued column
statistics with separate flagged-value accounting. Real CASA parity is closed
for the global amplitude stats produced by `calstat` on a generated `G` table.
Wave 25 then exposes that same capability through the public `calibrate`
binary via `calibrate stats`, with text/JSON output and explicit axis,
datacolumn, and flag-handling options. Wave 26 lands the first `fluxscale` slice as a
library-first bootstrap over complex `G Jones` / `T Jones` gain tables. The
accepted scope covers reference and transfer field selection by id/exact
name/simple glob, optional `refspwmap`, optional `gainthreshold`,
non-incremental or incremental output-table writing, and machine-readable
reporting of per-field/per-SPW transfer fluxes. Synthetic table-scaling tests
are green, and slow CASA parity is now closed on a real `ngc5921.ms`
amplitude-gain case by comparing both the derived transfer flux and the
transfer-field `CPARAM` values in the emitted fluxtable. Follow-on work under
`12.6` should now focus on `combine=*`, `BPOLY`, and any future app surface
for `fluxscale` rather than baseline fluxtable compatibility. Wave 27 closes
the first `solnorm` slice for `bandpass` by matching CASA's actual
`BJones::normalize()` behavior: per `(spw,time,antenna,pol)` row it
normalizes by a complex factor built from coherent mean phase and RMS
amplitude across channels. Synthetic caltable checks are green, and slow
real-MS parity is now closed by comparing downstream corrected data after
applying CASA- and Rust-produced normalized `B` tables in CASA.
Wave 31 further extends the accepted downstream parity surface to
`bandpass(..., bandtype='B', parang=True, gaintable=[prior G])`. Wave 33 then
lands the first `BPOLY` solve/write slice. `casa-calibration` now fits
legacy polynomial amplitude/phase rows from the same solved bandpass groups,
writes a CASA-readable legacy `BPOLY` table shape, and verifies the result by
applying both CASA- and Rust-produced `BPOLY` tables in CASA alongside the
same prior `G` table on `ngc5921.ms`. The accepted contract for this first
`BPOLY` slice is intentionally wider than the `B`-table path: on-disk
interoperability is exact enough that CASA `applycal` accepts the Rust-written
table, while the downstream corrected data close against CASA within a solver-
specific component-wise tolerance. Follow-on work under `12.6` should now
focus on any remaining `combine=*` semantics, richer `fluxscale`/`bandpass`
app surfaces, and optional BPOLY fit tightening rather than baseline BPOLY
compatibility. Wave 34 further extends the accepted downstream parity surface
to `bandpass(..., bandtype='B', combine='scan', gaintable=[prior G])`, using a
solver-specific downstream tolerance on the multi-scan `ngc5921.ms`
`field=1, spw=0` workload. Wave 35 then closes the remaining first-wave
`bandpass combine=*` semantics by extending that accepted downstream parity
surface to `combine='field'` and `combine='scan,field'`. Wave 36 adds the
first richer `calibrate` UI/workflow surface by wiring managed launcher output
through `casars`, including Overview rendering for stats and the other public
workflow reports. Follow-on work under `12.6` should now focus on broader
evaluation/help surfaces in `calibrate` and any future BPOLY fit tightening
rather than baseline `bandpass combine=*` compatibility.

---

### 12.7 Deferred Calibration Backlog (`parang`, `FPARAM`, `BPOLY`, broad plotting)

**Status:** DEFER

**Reason:** These features expand the compatibility surface significantly and
should not be represented as code TODOs before the complex `CPARAM` apply path
is stable. Wave 28 extracted `gainfield='nearest'` from this backlog into the
implemented apply surface, Wave 30 extracted the first narrow `callib` slice
into the public `calibrate` apply path, and Wave 31 extracted `parang` into
the implemented apply/solve surface for circular-feed layouts. Wave 32 then
extracts the first float-parameter family by supporting `FPARAM`-backed
`K Jones` delay tables in the apply path. Wave 33 further removes `BPOLY`
from the deferred set by landing a narrow solve/write/apply parity slice under
`12.6`. The remaining deferred items are now broader float-parameter families
beyond `K` and broader inspection/plotting work.
