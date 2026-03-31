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

### 12.9 Image Browser Nonlinear WCS-Aware Axis Ticks And Labels

**Status:** DEFER

**Reason:** The current `imexplore` plane annotations assume locally linear
world-coordinate spacing across the displayed window. Proper sky-projection
labeling for strongly curved or rotated views, including cases like a pole near
the display center, needs a dedicated nonlinear tick-placement pass that samples
the full WCS rather than extrapolating from one cursor probe and one increment.

### 12.10 Image Browser Nonlinear WCS-Aware Region Rasterization

**Status:** DEFER

**Reason:** `imexplore` polygon regions are now stored in world coordinates, but
the current plane/mask rasterization projects vertices into pixel space once per
plane and fills a locally linear polygon there. Strongly curved sky views or
projections with significant distortion need a dedicated sampled-WCS region
rasterizer instead of straight-edge pixel-space filling.

### 12.11 Image Browser Persistent Editable Region Definitions

**Status:** DEFER

**Reason:** `imexplore` can currently convert an active WCS-native region into a
persistent image mask, but it does not yet persist the editable region
definition itself. A later wave should store named region objects with their WCS
vertices and metadata so they can be reopened, edited, and reused across
related images instead of only surviving as rasterized masks.

### 12.12 Image Browser Movie Visibility-Aware Stop Policy

**Status:** DEFER

**Reason:** Current movie playback can be stopped by incidental terminal mouse
drag events that happen while switching away from the app or when the `imexplore`
window is no longer meaningfully visible. The desired policy is likely to stop
playback only on deliberate in-app interaction or explicit visibility loss,
but that behavior should be decided separately from the current performance wave
so it does not distract from render/present bottleneck work.

### 12.13 Image Browser Production Plane-Movie Preload-Then-Place Path

**Status:** DEFER

**Reason:** The staged movie harnesses now show a clear split:

- Stage 1 (`preview-render`, no GUI) sustains `30 FPS`
- Stage 2 (`preload-then-place`, direct Ghostty/Kitty presentation) sustains `30 FPS`
- Stage 3 (minimal `ratatui` plane panel) falls to about `14 FPS`

That means the proven fast path for the plane pane is to preload one terminal
image per movie frame and then re-place those cached terminal-resident images
during playback, rather than pushing each frame through the normal
`ratatui-image` panel protocol path. The production `imexplore` movie path
should be refactored around that result.

The production wave should include:

- a `ratatui-graphics` API for terminal-resident pixel-buffer storage plus
  placement into a ratatui-defined pane rect
- a plane-pane movie mode that keeps ratatui responsible for layout/chrome but
  removes the animated plane image from the normal panel render path while movie
  playback is active
- a byte-budgeted frame store rather than an unbounded full-cube preload
- preload-one-cycle-if-it-fits behavior, otherwise a sliding movie-frame window
  around the current playback position
- explicit handling for resize/theme/pane-geometry invalidation and for fast
  stop/teardown without blanking

This should be implemented only after the staged harness results are used to
design the production API boundary, so the repo does not repeat the earlier
failed direct-overlay experiments inside `imexplore`.

### 12.14 Image Browser Live Spectrum Movie Acceleration

**Status:** DEFER

**Reason:** The current safe movie path can keep the plane pane correct by
using direct Kitty uploads, but the visible spectrum pane still goes through the
normal `ratatui-image` renderer and significantly reduces movie FPS. The
interim production tradeoff is to freeze or hide the spectrum during movie
playback so the plane pane remains responsive. A later wave should add a proper
movie-aware spectrum path, ideally with either throttled updates or an atomic
occurrence-based plane+spectrum presentation model that preserves performance
without desynchronizing the visible data.

That later wave should explicitly avoid treating the spectrum as a full redraw
problem on every movie step. Most of the spectrum pane is static across adjacent
movie frames: the trace geometry is usually unchanged, while the moving selected
sample marker and highlighted pixel/sample brightness are the parts that need to
update with the movie. The intended optimization direction is therefore to split
the spectrum into a reusable base plot plus lightweight per-frame overlays or
selection-state updates, so the spectrum can stay live during movie playback
without dragging plane FPS back down.
