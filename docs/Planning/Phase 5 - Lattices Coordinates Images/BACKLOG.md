# Lattices, Coordinates, and Images Closeout Inventory

Catalog of remaining phase-level gaps for `lattices`, full
`coordinates/CoordinateSystem` parity, and `images` with explicit iteration
workflows.

---

## Status Legend

| Tag | Meaning |
|-----|---------|
| **IMPLEMENT** | Will be built in Phase 5 waves |
| **DEFER** | Out of scope for Phase 5 (reason given) |

---

## Extracted Into Waves

Items `1.1`-`10.3` were extracted into Phase 5 Waves 1-10.

---

## Deferred To Later Phases

### 11.1 Advanced image analysis algorithms (deconvolution, synthesis imaging)

**Status:** DEFER

**Reason:** Higher-level science workflows beyond core storage/interoperability
scope.

---

### 11.2 Full CASA tool/task parity for images

**Status:** DEFER

**Reason:** Command/task parity is broad and should follow stable base APIs.

---

### 11.3 GPU/distributed lattice/image execution

**Status:** DEFER

**Reason:** Specialized performance target and independent architecture track.

---

### 11.4 Full nonlinear reprojection and mosaicking suite

**Status:** DEFER

**Reason:** Depends on complete coordinate and image-math expansion beyond
Phase 5 core.

---

### 12.1 First-Class Masked-Lattice Traversal

**Status:** DEFER

**Reason:** Traversal APIs still need a proper masked-lattice model with aligned
value/mask cursors and consistent read/write semantics across temporary and
persistent backends.

---

### 12.2 Richer Mutable Traversal Producer/Worker Helpers

**Status:** DEFER

**Reason:** Mutable traversal now shares cache-hint plumbing with the read-only
path, but it still lacks the higher-level producer/worker execution helpers used
by read-only reductions and chunk pipelines.

---

### 12.3 Optimized Small-Image Scalar Reduction Kernel

**Status:** DEFER

**Reason:** The current small-image fallback is a pragmatic performance tradeoff.
Replacing it with a tighter dedicated serial reduction kernel remains worthwhile,
but it needs its own benchmark-guided pass.

---

### 12.4 Image Browser Cursor Anchoring And Recenter Controls

**Status:** DEFER

**Reason:** `imexplore` now supports direct pixel activation by mouse click, but
follow-cursor viewport anchoring and an explicit recenter command need a
separate UX pass so they do not fight manual scrolling and horizontal pan.

### 12.5 Image Browser Source-Plane And Tile Cache Layer

**Status:** DEFER

**Reason:** `imexplore` currently recomputes source planes on demand. Large-cube
work needs an explicit bounded cache for decoded/source numeric planes and tile
reuse keyed by image identity, view window, and non-display axis selections.

### 12.6 Image Browser Interleaved I/O And Compute Pipeline

**Status:** DEFER

**Reason:** The current browser plane path is synchronous. Larger datasets need
worker-local paged-image handles plus an overlapped read/stats/downsample/stretch
pipeline so tile I/O and plane calculations can run concurrently instead of as
one monolithic request.

### 12.7 Image Browser Rendered Raster Cache And Plane Prefetch

**Status:** DEFER

**Reason:** `imexplore` needs a separate bounded raster cache keyed by source
plane identity, viewport pixel budget, and display settings, plus adjacent-plane
prefetch for responsive movie mode and cube stepping.

### 12.8 Image Browser Dropped Multi-Probe Spectrum Panels

**Status:** DEFER

**Reason:** Multiple independently pinned spectra with connector lines, drag and
resize handles, and shared probe state need a fuller windowing and hit-testing
model than the current linked Plane/Spectrum workspace wave.
