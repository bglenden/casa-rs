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
