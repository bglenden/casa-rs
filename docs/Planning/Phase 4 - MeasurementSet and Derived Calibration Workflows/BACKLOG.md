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

Items `1.1`-`8.3` were extracted into Phase 4 Waves 1-8.

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

### 11.1 `listobs` Plot Workspace Infrastructure in `casars`

**Status:** IMPLEMENT

**Reason:** Add a reusable `Plots` workspace to the `listobs` TUI so one
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

**Status:** DEFER

**Reason:** These need a second wave of payload extraction, averaging, and
selection semantics from MAIN-table data. Ship the reusable plot substrate
first, then add raw visibility plots on top of it.

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
