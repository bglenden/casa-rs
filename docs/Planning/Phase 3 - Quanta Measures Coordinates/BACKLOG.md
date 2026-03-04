# Quanta/Measures/Coordinates Closeout Inventory

Catalog of remaining phase-level gaps relative to casacore C++ modules:
`casa/Quanta`, `measures` (including `TableMeasures` and `meas` UDF), and
`coordinates` core.

---

## Status Legend

| Tag | Meaning |
|-----|---------|
| **IMPLEMENT** | Will be built in Phase 3 waves |
| **DEFER** | Out of scope for Phase 3 (reason given) |

---

## Extracted Into Waves

Items `1.1`-`8.3` were extracted into Phase 3 Waves 1-8.

---

## Deferred To Later Phases

### 9.1 Full `DirectionCoordinate` WCSLIB/FITS Projection Parity

**Status:** DEFER

**Reason:** Large WCSLIB/FITS surface and projection edge cases are a separate
risk boundary; keep Phase 3 on measure and coordinate core.

---

### 9.2 Full `FITSCoordinateUtil` Round-Trip Coverage

**Status:** DEFER

**Reason:** FITS keyword mapping breadth is large and tightly coupled to
WCSLIB-heavy direction projection behavior.

---

### 10.1 Typed `MeasurementSet` API Layer (`ms/MeasurementSets`)

**Status:** DEFER

**Reason:** Depends on stable TableMeasures + measures foundations but is a
large domain in itself; stage as Phase 4.

---

### 10.2 `derivedmscal` Parity

**Status:** DEFER

**Reason:** Depends on full measures + measurement set workflows and is better
sequenced after typed MS APIs.

---

### 10.3 `msfits` Mapping

**Status:** DEFER

**Reason:** Depends on FITS and full MS surface; too broad for current phase.

---

### 11.1 Full `meas` TaQL UDF Catalog

**Status:** DEFER

**Reason:** Core subset is enough for Phase 3 validation; full catalog can be
expanded incrementally after UDF framework stabilizes.
