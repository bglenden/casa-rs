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

**Reason:** The Phase 3 subset now covers the core measure conversions plus
the higher-value TaQL helpers added in the current waves: sidereal-time
extraction (`meas.last`, `meas.lst`), direction cosine output
(`meas.dircos`, `meas.directioncosine`), common direction aliases
(`meas.hadec`, `meas.azel`, `meas.app`, `meas.apparent`, `meas.ecl`,
`meas.gal`, `meas.sgal`, `meas.supergal`, `meas.supergalactic`,
`meas.itrfd`, `meas.itrfdir`, `meas.itrfdirection`), ITRF/WGS position
extractors (`meas.itrfxyz`, `meas.itrfll`, `meas.itrflonlat`,
`meas.itrfh`, `meas.itrfheight`, `meas.itrfllh`, `meas.wgs`,
`meas.wgsxyz`, `meas.wgsll`, `meas.wgslonlat`, `meas.wgsh`,
`meas.wgsheight`, `meas.wgsllh`), and rest/shift frequency helpers
(`meas.rest`, `meas.restfreq`, `meas.restfrequency`, `meas.shift`,
`meas.shiftfreq`, `meas.shiftfrequency`). Named-direction inputs are now
supported for the built-in calibrator names, `ZENITH`, and the main
solar-system bodies used by casacore TaQL, and `meas.riset` /
`meas.riseset` now evaluate in Rust TaQL. The earth-magnetic / IGRF helper
family is also implemented in the Rust measures layer with bundled IGRF12
coefficients aligned to casacore's measures-data source, including TaQL
quantity-driven explicit `meas.em*` inputs that distinguish XYZ vectors from
angle/length forms the same way casacore does. Full external
`ephemerides/Sources` catalog parity for non-built-in source names still
remains outstanding. The non-numeric `meas.help` introspection entry point is
now available in the Rust TaQL layer for UI/discoverability use.

---

### 11.2 Shared Record/Value Crate Extraction for Measure Record Serialization

**Status:** DEFER

**Reason:** `casa-types` still depends on record/value types that currently
live in the table layer. Splitting that shared data model into its own crate is
worth doing, but it cuts across package boundaries and should stay out of the
current wave unless a larger crate-graph cleanup is already in flight.

---

### 11.3 Resolve or Explicitly Document the casacore vs SOFA Direction Deviation

**Status:** DEFER

**Reason:** The remaining J2000/App deviation investigation is a focused
correctness audit, not a blocker for the current measure APIs. It should land
either as exact casacore parity work or as stronger documented divergence once
the expected behavior is confirmed.

---

### 11.4 `ephemerides/Sources` and `ephemerides/Lines` Catalog Import

**Status:** DEFER

**Reason:** The observatory catalog is implemented, but source and spectral-line
catalog tables are still missing from the Rust-native measures-data bundle.
They are not required by current measure/MS workflows and should be added as a
separate catalog wave.

---

### 11.5 Geomagnetic and Space-Weather Tables (`IGRF`, `IMF`, `KpApF107`)

**Status:** DEFER

**Reason:** These geodetic support tables are present in casacore-data but are
not consumed by the current Rust measures stack. Keep them deferred until a
concrete user-facing magnetic-field or space-weather workflow needs them.

---

### 11.6 Auxiliary Observatory/Velocity Catalogs (`SCHED_locations`, `VGEO`, `VTOP`)

**Status:** DEFER

**Reason:** The core `Observatories` table is now bundled, but these related
location/velocity catalogs remain unimplemented. They should be imported only
once there is a clear API consumer for them.

---

### 11.7 Broader Measures-Data Parity Audit and Refresh Tooling

**Status:** DEFER

**Reason:** The Rust-native observatory importer is implemented, but the rest of
the measures-data tree still needs a periodic parity audit and refresh workflow
so future bundled snapshots stay aligned with upstream casacore-data.

---

### 11.8 Repo-Controlled Refresh Path For Leap-Second and Geomagnetic Sources

**Status:** DEFER

**Reason:** The current Rust measures stack mixes three update stories:
bundled EOP snapshots with an explicit repo-local updater, leap-second data
embedded inside the `sofars` dependency, and geomagnetic coefficients embedded
inside the `igrf` dependency. Add a first-class `casa-*` refresh workflow so
maintainers can intentionally update and validate these sources from the repo
itself, record provenance/version dates, and keep bundled measures data aligned
with current IERS/IAGA releases without relying on ad hoc dependency bumps.
