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
