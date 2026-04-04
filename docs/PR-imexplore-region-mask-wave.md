# ImExplore Native Region/Mask Persistence and UI Cleanup

## Summary

This PR finishes the `imexplore` region/mask wave as a mergeable cleanup pass rather than a feature spike.

It does four things:

1. moves persistent region, mask, region-profile, and region-stat behavior behind `casa-images` library APIs so `casars` is a thin client
2. switches persistent saved regions and masks to native casacore-compatible image metadata/table structures
3. adds Rust-vs-C++ interop coverage for supported and unsupported region classes, plus region-statistics equivalence checks
4. refactors the new `casars` image-browser left-pane manager behavior into reusable shell primitives instead of leaving it as `imexplore`-only UI glue

This PR stays Rust-first in production code. Casacore C++ is used only as a test oracle and interoperability reference.

## User-visible behavior

### `imexplore`

- saved editable regions are now persisted in native casacore image `regions` metadata
- persistent masks are now written as native named masks with default-mask metadata
- the left pane supports `Live`, `Regions`, and `Masks` through a shared selector/popup manager pattern
- saved regions can be loaded, renamed inline, deleted, and used to create masks
- masks can be viewed, selected as default, and deleted
- when an active closed region exists:
  - the spectrum shows a region-summed profile instead of a cursor-point profile
  - the point reticle is suppressed in the plane view
  - region statistics are shown in the left pane
- region-stat value units are auto-scaled with Rust quanta helpers (`Jy/beam` -> `mJy/beam`, `Hz` -> `GHz`, etc.) using engineering prefixes only

### Supported persistent editable region kinds in this wave

- `WCPolygon`
- `WCUnion` of `WCPolygon`

Everything else is rejected with explicit errors naming the casacore class.

## Library/API changes

### `casa-images`

- `OpenedImageView` now owns native saved-region CRUD and native mask operations
- region editing/manipulation remains polygon-based, but the persistent save/load/remove/rename path is library-owned
- region profile and region statistics are computed in Rust library code with the active region and image mask applied
- native region record encoding/decoding is split into focused helpers

Key behavior:

- save one closed polygon -> native `WCPolygon`
- save multiple closed polygons -> native `WCUnion` of polygons
- save with any open polygon -> explicit error
- `write_region_mask(...)` writes a native persistent mask and can set it as default

### `casa-coordinates`

- CASA-style record compatibility is improved for region persistence and temp-image interoperability
- `LinearCoordinate` now serializes `pc` as a real matrix record shape compatible with casacore C++

### `casa-test-support`

- added C++ image shim coverage for:
  - supported polygon and union regions
  - unsupported region kinds:
    - `WCBox`
    - `WCEllipsoid`
    - `WCIntersection`
    - `WCDifference`
    - `WCComplement`
    - `WCConcatenation`
    - `WCExtension`
    - `WCLELMask`
    - LC/pixel region path
- added Rust-vs-C++ statistics parity tests for supported saved regions
- stabilized the undefined-scalar AIPSIO cross-matrix path and moved it into its own test file
- tightened table/casacore global locking used by C++ interop tests

### `casars`

- extracted shared pane-manager helpers for:
  - selector button label
  - anchored popup geometry
  - popup hit-testing
  - checkbox hit-testing
- extracted browser-manager-specific app/UI helpers into focused modules
- kept the shell layout/header/footer/result-pane structure shared across apps

## Interoperability/parity evidence

### Native persistence

- Rust writes native saved polygon/union regions that C++ can reopen and classify correctly
- C++ writes native polygon/union regions that Rust can load into editable region state
- Rust writes native named/default masks that C++ can reopen
- C++ writes native named/default masks that Rust can reopen

### Unsupported region handling

Rust explicitly rejects unsupported saved region classes with class-named errors instead of silently degrading:

- `WCBox`
- `WCEllipsoid`
- `WCIntersection`
- `WCDifference`
- `WCComplement`
- `WCConcatenation`
- `WCExtension`
- `WCLELMask`
- LC/pixel region path

### Statistics equivalence

- supported saved polygon and union regions now have Rust-vs-C++ region statistics equivalence coverage for:
  - `npts`
  - `sum`
  - `mean`
  - `median`
  - `rms`
  - `sigma`
  - `min`
  - `max`

This validates the Rust region-application and masked/finiteness filtering path against casacore C++ while keeping production code fully in Rust.

## Structural cleanup

- split `casa-images` region persistence logic out of the large monolithic file into focused internal modules
- split `casars` image-browser left-pane manager behavior into reusable pane-manager/browser-manager modules
- removed stale/non-native persistence assumptions from comments and code paths
- kept app-specific rendering in `casars`, but avoided duplicating persistence/stat logic there

## Validation

Completed on this branch:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `scripts/run-coverage.sh`

Coverage:

- `77.93% coverage, 43674/56041 lines covered, +0.20% change in coverage`

Changed-area coverage was also strengthened directly rather than relying only on the global gate:

- `crates/casa-images/src/image_view/region_persistence.rs`
  - `199/258` lines covered (`77.13%`)
  - added focused tests for unsupported-class reporting, unit conversion failures, and quantum-vector parsing errors
- `crates/casars/src/app/browser_manager.rs`
  - `169/207` lines covered (`81.64%`)
  - added focused UI tests for popup selection/dismissal, empty-state actions, and row clipping/anchoring behavior
- `crates/casars/src/ui/browser_manager.rs`
  - `78/79` lines covered (`98.73%`)
  - now has near-complete coverage through the new manager rendering/hit-testing tests
- `crates/casars/src/pane_manager.rs`
  - `38/45` lines covered (`84.44%`)
  - added focused geometry and hit-testing tests for selector popup placement and checkbox targeting

## Review guide

### 1. Native persistence and image APIs

Focus files:

- `crates/casa-images/src/image.rs`
- `crates/casa-images/src/image_view.rs`
- `crates/casa-images/src/image_view/region_persistence.rs`
- `crates/casa-images/src/imagebrowser_session.rs`
- `crates/casa-images/src/mask.rs`

Questions:

- do the public `OpenedImageView` APIs look reusable outside `imexplore`?
- is the supported/unsupported region surface explicit and coherent?
- do native mask/region record writes match casacore expectations?

### 2. Interop and parity testing

Focus files:

- `crates/casa-test-support/src/cpp/casacore_cpp_image_shim.cpp`
- `crates/casa-test-support/src/lib.rs`
- `crates/casa-test-support/src/table_interop.rs`
- `crates/casa-test-support/tests/images_interop.rs`
- `crates/casa-test-support/tests/tables_cross_matrix_stman_aipsio_undefined.rs`

Questions:

- are the interop tests exercising the right supported and unsupported classes?
- are the locking/stability changes appropriate for the C++ bridge?
- are the Rust-vs-C++ stats comparisons sufficiently strong without being brittle?

### 3. `casars` shell/UI abstraction

Focus files:

- `crates/casars/src/pane_manager.rs`
- `crates/casars/src/app/browser_manager.rs`
- `crates/casars/src/ui/browser_manager.rs`
- `crates/casars/src/app.rs`
- `crates/casars/src/ui.rs`
- `crates/casars/src/tests.rs`

Questions:

- is the left-pane manager pattern general enough for future apps?
- does `imexplore` consume the shared abstraction rather than bypass it?
- is there any remaining `imexplore`-specific UI behavior that should still move into shared shell primitives?

### 4. Coordinate/temp-image compatibility fixes

Focus files:

- `crates/casa-coordinates/src/coordinate_system.rs`
- `crates/casa-coordinates/src/direction.rs`
- `crates/casa-coordinates/src/linear.rs`

Questions:

- do the CASA-style record layout changes look narrowly scoped and correct?
- does the `pc` matrix serialization match casacore C++ expectations without disturbing existing Rust round-trips?

## Out of scope / deferred

- new editable region shapes beyond polygon and union-of-polygons
- automatic migration of short-lived private JSON region persistence
- automatic migration of older non-native mask metadata
- deeper Ghostty cached-Kitty-image debugging for movie playback
- making region stats/summaries a standalone shared widget beyond the extracted pane-manager primitives
