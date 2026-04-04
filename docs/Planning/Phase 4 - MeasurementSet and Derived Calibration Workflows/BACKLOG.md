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

Items `1.1`-`8.3` and `12.1` were extracted into Phase 4 Waves 1-9.

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

### 12.2 `casacore-calibration` Substrate with Permissive-Read / Strict-Write Policy

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

---

### 12.5 Limited `gaincal` (`G` / `T`, `p|ap`)

**Status:** IMPLEMENT

**Reason:** Solving should begin only after the caltable substrate and apply
path are trustworthy, using the same on-disk compatibility contract and
downstream parity checks.

---

### 12.6 `bandpass`, `calstat`, and `fluxscale`

**Status:** IMPLEMENT

**Reason:** These workflows build naturally on the same caltable substrate but
carry broader scope and stricter workflow dependencies than the first apply
release.

---

### 12.7 Deferred Calibration Backlog (`gainfield=nearest`, `callib`, `parang`, `FPARAM`, `BPOLY`, broad plotting)

**Status:** DEFER

**Reason:** These features expand the compatibility surface significantly and
should not be represented as code TODOs before the complex `CPARAM` apply path
is stable.
