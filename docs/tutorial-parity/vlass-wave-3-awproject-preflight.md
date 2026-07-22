# VLASS Wave 3 AWProject Architecture Preflight

Truth class: approved-wave implementation preflight
Last reality check: 2026-07-21
Verification: `just docs-check`

Scope:

- wave lead: [#445](https://github.com/bglenden/casa-rs/issues/445)
- shared AWProject capability: [#52](https://github.com/bglenden/casa-rs/issues/52)
- bounded serial VLASS parity: [#448](https://github.com/bglenden/casa-rs/issues/448)
- canonical task and UI controls: [#450](https://github.com/bglenden/casa-rs/issues/450)

This preflight authorizes implementation inside the accepted issue boundary. It
does not change the frozen VLASS workload, CASA reference products, 32 GiB
memory ceiling, serial-correctness-first order, or 10x performance requirement.

## Outcome And Non-Fallback Rule

Implement one shared `awproject` projection plan that applies a real EVLA
frequency-dependent A term and a non-coplanar W term in the same convolution
function. The initial bounded path is Stokes I, 32 W planes, two parallel-hand
Mueller elements, `aterm=true`, `psterm=false`, `wbawp=true`,
`conjbeams=true`, `usepointing=true`, one parallactic-angle bin, MT-MFS with
`nterms=2`, and the frozen VLASS SPW/field selections.

`awproject`, `awp2`, and `widefield` must no longer silently select the existing
W-only projector. A request either produces a validated A+W plan or fails before
visibility materialization with the missing or mismatched capability named.
`awphpg` remains a distinct unavailable backend until it has a real HPG/GPU
implementation; it must not alias to CPU AWProject or WProject.

## Ownership Boundary

### `casa-imaging`

Owns the reusable science and execution contract:

- `ProjectionPlan`, including standard, mosaic/A-only, W-only, and A+W modes;
- validated A-term, W-term, parallactic-angle, conjugate-beam, frequency, and
  Mueller key dimensions;
- immutable convolution-function metadata and bounded pixel residency;
- CF selection, convolutional gridding/degridding, normalization state, MT-MFS
  Taylor accumulation, PB/weight semantics, and product meaning; and
- pure cache-key matching and rejection diagnostics.

The existing `ScreenProjector`, `WProjector`, mosaic normalization, MT-MFS
controller, and product builders remain the implementation substrate. The A+W
path combines their semantics in one CF plan; it does not run independent A and
W gridders and multiply completed images afterward.

### `casars-imager`

Owns the application boundary:

- resolves and authorizes the user-supplied `cfcache` path;
- extracts telescope, antenna/feed, SPW/DDID, FIELD, POINTING, time, and
  parallactic-angle inputs from the selected MeasurementSet rows;
- constructs the typed projection request and supplies bounded visibility
  streams;
- admits memory, workers, read-ahead, FFT, and backend resources through the
  existing immutable execution plan;
- persists CASA-compatible products and exposes every supported control through
  the canonical task contract and UI catalog; and
- records cold/warm cache evidence and stage timing.

### `casa-ms`, `casa-images`, and `casa-coordinates`

`casa-ms` remains the typed MeasurementSet access layer. `casa-images` reads
standard casacore `PagedImage<Complex>` CF tables and their `miscinfo` records.
`casa-coordinates` validates their UV, Stokes, and spectral coordinates. None of
these crates owns gridding policy.

No new top-level crate, application, dependency direction, or runtime model is
needed.

## Projection And Public Control Contract

The current split between `GridderMode` and `WTermMode` permits contradictory
states and enabled the AW-to-W-only alias. Replace that split at the execution
boundary with one typed projection plan. Compatibility parsing may still accept
the existing standard, mosaic, and wproject spellings, but there is one
validated semantic value passed to the core.

The shared AW request carries:

- `facets` and `psfphasecenter`;
- `vptable` when a non-default voltage-pattern table is requested;
- `wprojplanes`;
- `aterm`, `psterm`, `wbawp`, and `conjbeams`;
- `cfcache`;
- `usepointing` plus pointing-offset controls;
- compute and rotate parallactic-angle steps;
- `mosweight`, `normtype`, and `pblimit`; and
- the normal worker, memory, and backend execution controls.

For the frozen recipe, `facets=1`, no `vptable`, no distinct PSF phase center,
`wprojplanes=32`, A/W/WB/conjugate-beam terms enabled, PS term disabled,
`mosweight=false`, and `normtype='flatnoise'`. Unsupported combinations remain
typed errors rather than ignored fields. The ParameterCatalog, CLI/task JSON,
managed profile, workbench form, run summary, and provenance record all expose
the same canonical fields; frontends do not reimplement the science defaults.

## Existing CASA CF Cache Interoperability

The frozen CASA 6.7.5.9 runs published distinct immutable caches for the two
field selections:

```text
single field  /Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache/6.7.5.9/8e5679681214158629c7eb6113bc3b062d6105fbae64471905aa73de50080a69
all fields    /Volumes/GLENDENNING/casa-rs-vlass/issue-446/cf-cache/6.7.5.9/f6f947c5104f8da579f9411dd7087dd331c9e59034073a9fd68b5d6132cd281d
```

Each contains 1,024 `CFS_*.im` imaging kernels and 1,024 corresponding
`WTCFS_*.im` weight kernels: 16 frequency values by 32 W values by two
parallel-hand Mueller elements at one parallactic angle. A representative CF is
a standard `PagedImage<Complex32>` of shape `[360, 360, 1, 1]` with UV linear,
Stokes, and spectral coordinates. Its CASA `miscinfo` carries `BandName`,
`ConjFreq`, `ConjPoln`, `Diameter`, `MuellerElement`, `Name`, `OpCode`,
`ParallacticAngle`, `Sampling`, `TelescopeName`, `WIncr`, `WValue`, `Xsupport`,
and `Ysupport`.

The first implementation is a read-only interoperability adapter for that
existing format. It creates an immutable index from metadata only, then loads
only the CF cells selected by the current bounded block. Pixel residency is
bounded by the admitted cache allocation and uses deterministic LRU eviction;
the CF index and key metadata stay resident. Imaging and weight kernels are
paired and validated before a cell becomes visible to execution.

Cache identity includes:

- cache format and source identity;
- image shape, cell, projection, phase center, Stokes, and spectral definition;
- telescope/antenna diameter and voltage-pattern identity;
- selected SPW/frequency bucket and conjugate frequency;
- W-plane count, W increment, and W value;
- Mueller element and conjugate polarization;
- parallactic-angle bin and pointing policy;
- A, PS, W, WB-AWP, and conjugate-beam switches; and
- sampling, support, and pixel type.

Filename indices are diagnostics only; metadata is authoritative. Duplicate,
missing, unpaired, corrupt, non-finite, coordinate-mismatched, or key-mismatched
cells reject the plan. A cache miss does not silently synthesize a weaker CF.

The wave does not define or write a native casa-rs persisted CF format. If a
new writable or versioned cache format becomes necessary, stop for the
persisted-format review required by #448. The current run may reuse the frozen
CASA cache without that expansion.

## Memory And Concurrency

Serial CPU is the correctness reference. CF metadata discovery is bounded and
pixel loading is on demand. At most one admitted set of image/PSF/Taylor grids
and the configured CF-residency budget may be live; no worker owns a duplicate
12,150-square grid. The resource ledger accounts separately for:

- resident output/Taylor grids;
- bounded input blocks and POINTING state;
- CF index plus resident CF pixels;
- FFT scratch; and
- product-writing scratch.

Automatic memory planning uses the kernel's immediately available-memory
snapshot. An explicit `imaging_memory_target_mb` is instead the operator's
process budget and is honored even when it is larger than that snapshot. This
distinction is required for the approved 32 GiB full-geometry run: it permits
observable swap-backed execution without pretending those pages are currently
free, while the receipt retains both the requested budget and measured host
state.

The initial execution has one gridding owner and deterministic accumulation.
Later CPU tiling, read-ahead, Metal, and GPU CF residency must consume the same
immutable projection plan and stay within the admitted budget. They may change
placement and scheduling, not CF selection, normalization, or reduction order
without an independently measured numerical acceptance result.

## CASA Semantic Anchors

The local CASA source oracle is commit
`61020062cee290f5466cffed5ec5032e0c7a3434` (the checkout has unrelated local
changes). Relevant TransformMachines2 behavior is:

- `SynthesisImagerVi2.cc:2559-2586` selects AWProject and then wraps the chosen
  transform machine with `MultiTermFTNew` for MT-MFS;
- `SynthesisImagerVi2.cc:2644-2832` creates telescope-specific A/PS/W terms,
  loads `CFCache`, applies pointing and parallactic-angle controls, and passes
  the SPW frequency selection to both gridder and degridder;
- `AWProjectFT.cc:135` creates the EVLA A term and combined AW convolution
  function with the requested WB-AWP and conjugate-beam behavior;
- `CFCache.cc:235-612` indexes `CFS` and `WTCFS` image tables by PA, frequency,
  W value, and Mueller element, with optional lazy pixel loading; and
- `AWConvFunc.cc:1607-1894` forms each CF from the A, optional PS, and W screens,
  applies conjugate-frequency beam handling, derives support, and initializes
  paired imaging/weight cells.

These paths are semantic oracles, not Rust API templates.

## Focused Verification Before VLASS Scale

1. CASA-cache index tests cover the 1,024+1,024 inventory, exact key
   multiplicities, metadata-only discovery, paired lookup, and one known cell.
2. Synthetic cache fixtures cover missing weight pairs, duplicate keys,
   malformed names, inconsistent PA/frequency/W/Mueller coordinates, corrupt
   pixels, and non-finite values.
3. Kernel tests cover oversampled offset selection, positive/negative W
   conjugation, A-only/W-only limiting cases, pointing phase gradients,
   normalization, and degrid adjointness.
4. Small CASA oracles compare a point at phase center, an off-axis point, and a
   two-pointing mosaic for A-only, W-only, and combined A+W behavior.
5. MT-MFS tests require all Taylor PSF, residual, model, image, sumwt, weight,
   PB, PB-corrected, alpha, and alpha-error products with no topology alias.
6. A 32-pixel real-cache probe must reject before execution because the minimum
   paired CF support requires a grid of at least 33 pixels. The real 64-pixel
   field-1525 ladder must then pass for one, two, and all 16 SPWs through true
   AWProject, followed by the full 12,150-pixel serial run.

Only after the full serial result passes the frozen comparison contract do
memory, threading, and Metal/GPU iterations begin. Oracle review is triggered
by that first measured serial evidence, as specified by the approved wave.

## Stop Conditions

Stop and return for explicit review before:

- inventing or writing an unversioned casa-rs CF cache;
- adding full antenna-model synthesis beyond the accepted precomputed-cache
  slice;
- adding full-Stokes off-diagonal Mueller support;
- changing dependency direction, the process runtime model, or concurrency
  guarantees;
- exceeding the 32 GiB execution budget; or
- weakening the frozen product or numerical acceptance contract.
