# Apple Metal Gridding Experiment

Truth class: experimental
Last reality check: 2026-05-21
Verification: `swift run -c release MetalGridExperiment --samples 2000 --imsize 128 --distribution uniform --tile-edge 32 --skip-slow-baselines`; `swift run -c release MetalGridExperiment --samples 200000 --imsize 512 --support 3 --distribution uniform --tile-edge 64 --skip-slow-baselines`; `swift run -c release MetalGridExperiment --prepared-samples-json /private/tmp/metal_prepare_small/prepared_samples.json --samples 50000 --imsize 512 --cell-arcsec 0.05 --tile-edge 64`

## Purpose

This directory is an isolated feasibility track for a possible future Apple
Metal backend for casa-rs standard-MFS gridding/degridding. It is not the Wave 2
CPU implementation path and does not replace the bounded streaming fixed-halo
tile work.

The experiment branch was resynced to
`484d1c409 Add standard MFS fixed tile backend` from
`codex/imperformance-wave2-standard-mfs-accel`. The current Wave 2 source now
exposes the relevant future work-unit shape: row-block preparation, fixed tile
partitions, compact tile bucket records, bounded resident tile caches, and a
fixed-tile backend flag live in `crates/casa-imaging/src/execution.rs`. The
experiment should consume that shape rather than requiring persistent per-sample
tap plans.

## Running

```bash
cd tools/experiments/metal
swift run MetalGridExperiment --samples 20000 --imsize 512 --distribution uniform
swift run MetalGridExperiment --samples 20000 --imsize 512 --distribution cluster
swift run MetalGridExperiment --samples 20000 --imsize 512 --distribution boundary
```

The prototype compiles Metal shader source at runtime from Swift. That is
deliberate for experiment speed; a production backend should precompile
`.metal` sources to `.metallib` with the Xcode Metal toolchain.

To run the prepared-sample fixture path, first emit an existing imager prepare
oracle bundle and expand the sample JSON:

```bash
cargo run -p casars-imager --example emit_prepare_oracle_bundle -- \
  --output-dir /private/tmp/metal_prepare_small \
  --dataset-tier tier-a \
  --ms /private/tmp/mssel_test_small.ms \
  --imagename /private/tmp/unused \
  --imsize 512 \
  --cell-arcsec 1.0 \
  --gridder standard \
  --dirty-only \
  --ddid 1 \
  --phasecenter-field 0
gzip -dc /private/tmp/metal_prepare_small/prepared_samples.json.gz \
  > /private/tmp/metal_prepare_small/prepared_samples.json
swift run -c release MetalGridExperiment \
  --prepared-samples-json /private/tmp/metal_prepare_small/prepared_samples.json \
  --samples 50000 \
  --imsize 512 \
  --cell-arcsec 0.05 \
  --tile-edge 64
```

The fixture loader is intentionally an experiment bridge. It consumes
`PreparedVisibilitySampleTrace`, recomputes center cells from UVW/frequency and
the requested image geometry, and emits the compact Metal sample records. It
does not change any production casa-rs API.

## Initial Results On Apple M4

Release-mode runs on 2026-05-21 used the runtime-compiled Swift/Metal harness
on an Apple M4. The CPU reference is single-threaded f32 accumulation over the
same compact records. Error metrics compare Metal output to that CPU reference.

The harness now includes:

- global f32 atomic scatter
- combined degrid + residual + grid global atomic scatter
- cell-owner no-atomic baseline
- CPU-expanded sorted reduce
- fixed-tile bucket cell-owner
- CPU-built tile-cell sample-reference bins
- GPU-built tile-cell sample-reference bins with a CPU prefix/read bridge
- GPU-built tile-cell bins with a naive all-GPU prefix scan

| Case | Strategy | GPU s | Samples/s | Tap updates/s | Max error | Relative RMS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 200k synthetic uniform, 512, support 3 | global atomic | 0.011002 | 1.82e7 | 8.91e8 | 1.21e-7 | 1.23e-7 |
| 200k synthetic uniform, 512, support 3 | residual refresh global atomic | 0.011978 | 1.67e7 | 1.64e9 tap passes/s | 9.42e-8 | 1.21e-7 |
| 200k synthetic uniform, 512, support 3 | sorted reduce | 0.002922 | 6.84e7 | 3.35e9 | 0.00 | 0.00 |
| 200k synthetic uniform, 512, support 3 | tile bucket cell owner | 0.039742 | 5.03e6 | 2.47e8 | 1.05e-7 | 1.17e-7 |
| 200k synthetic uniform, 512, support 3 | tile cell bins | 0.004311 | 4.64e7 | 2.27e9 | 1.05e-7 | 1.17e-7 |
| 200k synthetic uniform, 512, support 3 | GPU-built tile cell bins | 0.023807 | 8.40e6 | 4.12e8 | 1.52e-7 | 1.56e-7 |
| 200k synthetic uniform, 512, support 3 | GPU-prefix tile cell bins | 0.018563 | 1.08e7 | 5.28e8 | 1.88e-7 | 1.56e-7 |
| 200k central cluster, 512, support 3 | global atomic | 0.023641 | 8.46e6 | 4.15e8 | 8.48e-6 | 1.34e-6 |
| 200k central cluster, 512, support 3 | residual refresh global atomic | 0.022728 | 8.80e6 | 8.62e8 tap passes/s | 1.10e-5 | 7.94e-7 |
| 200k central cluster, 512, support 3 | tile cell bins | 0.013336 | 1.50e7 | 7.35e8 | 1.07e-5 | 9.05e-7 |
| 200k central cluster, 512, support 3 | GPU-built tile cell bins | 0.023625 | 8.47e6 | 4.15e8 | 1.32e-5 | 1.72e-6 |
| 200k central cluster, 512, support 3 | GPU-prefix tile cell bins | 0.030447 | 6.57e6 | 3.22e8 | 1.05e-5 | 1.75e-6 |
| 200k synthetic uniform, 512, support 5 | global atomic | 0.030600 | 6.54e6 | 7.91e8 | 8.20e-8 | 1.70e-7 |
| 200k synthetic uniform, 512, support 5 | tile cell bins | 0.010460 | 1.91e7 | 2.31e9 | 1.05e-7 | 1.58e-7 |
| 200k synthetic uniform, 512, support 5 | GPU-prefix tile cell bins | 0.028567 | 7.00e6 | 8.47e8 | 1.35e-7 | 2.32e-7 |
| 20k synthetic uniform, 2048, support 3 | global atomic | 0.001179 | 1.70e7 | 8.31e8 | 1.86e-8 | 5.36e-8 |
| 20k synthetic uniform, 2048, support 3 | tile cell bins | 0.001334 | 1.50e7 | 7.35e8 | 1.86e-8 | 5.35e-8 |
| 20k synthetic uniform, 2048, support 3 | GPU-built tile cell bins | 0.003357 | 5.96e6 | 2.92e8 | 1.86e-8 | 5.36e-8 |
| fixture-derived compact samples, 22,078 accepted, 512, support 3 | global atomic | 0.011225 | 1.97e6 | 9.64e7 | 1.13e0 | 6.78e-7 |
| fixture-derived compact samples, 22,078 accepted, 512, support 3 | residual refresh global atomic | 0.010957 | 2.01e6 | 1.97e8 tap passes/s | 2.72e2 | 4.83e-7 |
| fixture-derived compact samples, 22,078 accepted, 512, support 3 | tile cell bins | 0.002180 | 1.01e7 | 4.96e8 | 3.87e-1 | 1.59e-7 |
| fixture-derived compact samples, 22,078 accepted, 512, support 3 | GPU-built tile cell bins | 0.004142 | 5.33e6 | 2.61e8 | 1.00e0 | 6.83e-7 |

The sorted-reduce rows exclude CPU-side expansion and sorting. For `200k`
uniform support-3, that preparation cost was `0.509869 s` and `161.0 MB` of
reduce buffers. For support-5, it rose to `1.666998 s` and `391.4 MB`. This is
not a viable production contract.

The tile-cell-bin rows keep compact samples and add one `u32` sample reference
per tap contribution. For `200k` uniform support-3, CPU bin preparation was
`0.141489 s`, used `50.6 MB`, and produced `9.8M` sample references. For
support-5, CPU bin preparation was `0.462270 s`, used `108.7 MB`, and produced
`24.2M` refs. This validates the reduction shape but confirms CPU-side bin
construction is still too expensive.

The GPU-built tile-cell-bin rows include GPU count, CPU prefix/read, GPU fill,
and GPU reduce, but exclude final host-side tile merge. For `200k` uniform
support-3, those were `0.003323 s` count, `0.003987 s` prefix/read,
`0.017507 s` fill, and `0.002977 s` reduce. For central clustering, reduce
rose to `0.016091 s` because only `2025` active tile cells hold all `9.8M`
sample references.

The GPU-prefix rows replace CPU prefix/read with a naive log-step Hillis-Steele
style scan over every tile-halo cell. It is correct, but too many command-buffer
passes make it unattractive. In the 2048 pressure case, all-GPU prefix storage
used `114.9 MB`, while the CPU-prefix GPU-built path used `85.1 MB` and still
spent `0.021995 s` in the read/prefix bridge. A production path needs a
tile-local/block prefix, not this global scan.

The 2048 grid case shows a separate slab pressure issue: kernel time stayed
small, but reading full grid buffers back to the host cost `0.015-0.030 s`.
Future cube/slab work should avoid round-tripping full planes after every small
block. The backend contract needs explicit stage ownership for PSF/residual
grids and should keep resident slab buffers on device until a bounded flush is
required.

Observed conclusions:

- Global scatter is a useful upper-bound microbenchmark, but central clustering
  roughly halves throughput and increases numerical delta because many threads
  contend for the same cells.
- The cell-owner kernel proves a no-atomic write shape and keeps deterministic
  sample-order accumulation per cell, but it is intentionally inefficient for
  sparse grids because each cell scans every sample.
- The sorted-reduce kernel proves that collision-free deterministic reduction
  is extremely fast once contributions are grouped by output cell. The current
  CPU-side expand-and-sort step is much too expensive to use directly.
- The tile-bucket cell-owner kernel is closest to the resynced Wave 2 fixed-tile
  shape: compact samples, tile offsets, halo tile buffers, and post-kernel tile
  merge. It avoids global atomics and CPU tap expansion, but the current
  one-thread-per-halo-cell scan is too much work at high sample counts.
- The tile-cell-bin kernel is the best bounded-tile result so far. Once the
  per-cell sample-reference bins exist, the GPU kernel beats global atomics even
  on the `200k` uniform case. Its blocker is not kernel speed; it is CPU bin
  construction and the still-expanded `u32` reference list.
- Building the tile-cell bins on GPU removes most of the CPU construction
  blocker. The remaining costs are the CPU prefix/read bridge and nondeterminism
  from atomic fill order. In dense central clusters, the reduce pass becomes the
  new bottleneck because a small number of active cells own nearly all refs.
- A fully GPU-resident prefix is feasible but not with the naive scan used here.
  The scan's GPU time and command-launch wall time erase the small CPU-prefix
  bridge savings. The useful follow-up is a tile-local block prefix, not more
  global scan tuning.
- A production Metal backend should therefore move next to tile-bucketed
  reductions: keep the compact row-block/tile input, group contributions inside
  bounded GPU work units, and reduce collisions before global writes without
  expanding a full block into persistent tap records on the CPU.

## Current Metal Capability Notes

- Compute kernels run over 1D, 2D, or 3D thread grids; threads are grouped into
  threadgroups that can share `threadgroup` memory. SIMD groups are determined
  by Metal and should be queried through the compute pipeline state, especially
  `threadExecutionWidth`.
  Citation: <https://developer.apple.com/documentation/metal/creating-threads-and-threadgroups>
- Modern Apple/Mac GPU families have practical threadgroup limits of up to 1024
  threads and 32 KB maximum total threadgroup memory allocation, with 16-byte
  threadgroup-memory alignment in the current Metal Feature Set Tables.
  Citation: <https://developer.apple.com/metal/Metal-Feature-Set-Tables.pdf>
- `MTLStorageMode.shared` is CPU/GPU-visible system memory and is the default
  for `MTLBuffer` on integrated GPUs and for buffers/textures on Apple silicon
  GPUs. CPU/GPU access still needs completion discipline before the other side
  reads modified contents.
  Citation: <https://developer.apple.com/documentation/metal/mtlstoragemode/shared>
- The resource model distinguishes `private`, `shared`, and `managed` storage.
  Private GPU-only resources can be better hot buffers when explicit copies are
  acceptable.
  Citation: <https://developer.apple.com/documentation/metal/resource-fundamentals>
- Command-buffer GPU elapsed time can be measured from `gpuEndTime -
  gpuStartTime` after completion.
  Citation: <https://developer.apple.com/documentation/metal/mtlcommandbuffer/gpustarttime>
- Xcode GPU capture and Instruments Metal System Trace are the right next tools
  once a kernel shape is credible.
  Citations: <https://developer.apple.com/documentation/xcode/capturing-a-metal-workload-in-xcode>,
  <https://developer.apple.com/documentation/xcode/metal-developer-workflows>
- FFT options remain unsettled. CPU FFT can stay in `rustfft` or use Accelerate
  vDSP through FFI for Apple-only experiments. GPU FFT should be evaluated
  separately through MPSGraph FFT or a custom Metal FFT, because gridding
  acceleration alone does not prove end-to-end imaging speedup.
  Citations: <https://developer.apple.com/documentation/accelerate>,
  <https://developer.apple.com/documentation/metalperformanceshadersgraph/mpsgraphfftdescriptor>
- Rust integration should prefer the current `objc2-metal` ecosystem. The
  `metal` crate still exists, but its docs recommend new development use
  `objc2` and `objc2-metal`.
  Citations: <https://docs.rs/objc2-metal>, <https://docs.rs/crate/metal/latest>

## Prototype Strategies

### Strategy A: Global Grid With Atomics

Each input sample gets one GPU thread. The thread applies a 7x7 support and
atomically scatters complex f32 tap contributions into a global grid.

The prototype uses an integer compare-and-swap loop to emulate atomic f32 add
into `atomic_uint` buffers. That keeps the experiment broadly buildable, but it
is not the desired production shape. Native floating atomics need runtime
feature gating, and the accumulation order is nondeterministic.

Expected footprint is compact: input samples, tap table, and two global grid
planes. Synchronization cost is high near dense UV center cells. Complex64
accumulation is not practical in this shape because 64-bit floating atomics are
not a good portability assumption. This maps directly to compact row-block
records, but it ignores tile ownership.

### Strategy B: Cell-Owner / Microtile Accumulation

Each output cell gets one GPU thread. That thread scans samples and accumulates
only contributions whose support covers the cell, then writes the cell once.

This prototype avoids global atomics and is deterministic in sample order, but
it performs too much work for sparse grids because every cell checks every
sample. It is a useful correctness and contention baseline, not an expected
production kernel. A production version would restrict each dispatch to a
tile's bucket, ideally backed by the existing fixed tile partition and compact
bucket records.

Expected footprint is tile-local in the future form, with no global collision in
the owner-write phase. The bottleneck is binning and per-cell sample scanning.
Complex64 is possible in private/thread-local registers for small reductions,
but global storage pressure doubles and Metal double throughput must be tested
before relying on it.

The current tile-bucket variant keeps one compact sample record per visibility,
plus a tile-offset side table, and writes halo-padded per-tile buffers for a
host-side deterministic merge. It confirms that the Wave 2 tile contract is a
reasonable Metal input shape, but it also shows the next kernel cannot scan an
entire tile bucket for every halo cell. It needs either per-cell binning inside
the tile, sample-parallel local accumulation, or a two-pass tile reduction.

The current tile-cell-bin variant builds that per-cell index on the CPU. It
stores only `u32` sample references instead of full complex contributions and
computes tap weights on device. It validates the algorithmic direction, but a
future backend should construct these bins on GPU or fuse binning with tile
bucket generation so CPU preparation does not dominate.

The GPU-built variant adds three steps around the same reducer: GPU count
per-tile-cell refs, CPU prefix-sum of the compact counts, and GPU fill of the
sample-reference list. It is now the best proof-of-feasibility path. To become a
production backend, the prefix step should move to GPU or be batched at tile
granularity, and the fill/reduce order needs a documented tolerance policy or a
stable ordering pass for products that need tighter reproducibility.

## Stable-Order And Tolerance Notes

The deterministic comparators are the CPU reference, sorted-reduce, and
tile-bucket cell-owner paths. The GPU fill paths use atomics and therefore do
not preserve sample order inside each active tile cell.

For `200k` central-cluster samples:

- global atomics: relative RMS `1.34e-6`
- residual-refresh global atomics: relative RMS `7.94e-7`
- deterministic tile-bucket cell-owner: relative RMS `9.05e-7`
- CPU-built tile-cell bins: relative RMS `9.05e-7`
- GPU-built tile-cell bins: relative RMS `1.72e-6`
- GPU-prefix tile-cell bins: relative RMS `1.75e-6`

This is still plausibly inside a CASA-compatible tolerance envelope for dirty
grids, but it is not a bitwise- or tight-reproducibility path. A production
backend should expose two modes if this matters: a fast atomic-fill mode and a
stable mode for hot cells or validation runs.

## Residual Refresh Prototype

The combined residual prototype reads a model grid, degrids a predicted
visibility for each sample, subtracts it from the observed visibility, and
atomically grids the residual in the same kernel. It is intentionally global
atomic first because the purpose was to test whether the residual-refresh data
flow is plausible on Metal.

On `200k` uniform support-3, combined residual refresh ran in `0.011978 s`,
very close to simple global gridding at `0.011002 s`, while doing one degrid tap
pass plus one grid tap pass per sample. On central-clustered samples, it ran in
`0.022728 s`, again tracking the same contention problem as global gridding.

The implication is that residual refresh can be fused on device, but it should
not use global atomics as the production shape. It should reuse the same
tile-cell grouping path as dirty/PSF gridding so the model-grid reads, residual
formation, and residual accumulation stay inside bounded tile or slab work
units.

### Strategy C: Bin Then Reduce

Samples are sorted or bucketed by tile/cell/kernel key before accumulation.
Collisions are reduced before global writes.

The current prototype expands each 7x7 tap contribution on the CPU, sorts by
output cell, and dispatches one reducer thread per active output cell. That
reducer is exact relative to the CPU reference because contributions are kept in
sample order within each cell. The GPU reduction itself is fast, but the CPU
preparation cost and expanded-buffer footprint are unacceptable as a production
contract.

This is still the best production direction if Metal is kept: move the grouping
and reduction toward the GPU and keep it bounded by the Wave 2 tile buckets.
The next version should dispatch over nonempty fixed tiles, generate tap
contributions from compact bucket samples on device, and reduce within
tile-local or cell-local ranges before a deterministic merge.

## Risk Table

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Floating atomics availability/performance | High | Treat global atomics as a baseline only; runtime feature-gate any native atomic path. |
| CASA-compatible tolerance under nondeterministic accumulation | High | Keep CPU reference comparisons and favor bin/reduce for parity-sensitive stages. |
| Threadgroup memory ceiling | Medium | Size microtiles from 32 KB limits and number of complex planes; do not assume all cube planes fit. |
| Host preparation hides GPU wins | Medium | Report host prep, buffer creation, kernel, download, and end-to-end timing separately. |
| Full-grid host round trips dominate large slabs | Medium | Keep PSF/residual/model slab buffers resident on device and flush only bounded products. |
| FFT remains CPU-bound | Medium | Keep FFT as a separate backend boundary; evaluate MPSGraph/custom FFT only after gridding evidence exists. |
| Rust binding churn | Medium | Isolate backend behind a macOS-only crate/module using `objc2-metal`; keep production crates dependency-clean until evidence justifies it. |
| Profiling gap | Medium | Use command-buffer timing first, then Xcode GPU capture and Metal System Trace for serious kernels. |

## Future Work-Unit Contract

The input should stay row-block and tile-bucket oriented:

```rust
#[repr(C)]
pub struct MetalGridSample {
    pub center_x: u32,
    pub center_y: u32,
    pub kernel_u: u16,
    pub kernel_v: u16,
    pub support_id: u16,
    pub grid_plane: u16,
    pub flags: u16,
    pub weight: f32,
    pub visibility_re: f32,
    pub visibility_im: f32,
}
```

This is a 32-byte record matching the Swift and Metal prototype layout. Future
records likely also need a tile id or tile-offset side table, per-plane offsets,
and optional PSF/residual mode flags. Kernel support tables should be shared per
block or per support id, not expanded into persistent per-sample tap products.

The backend-facing work unit should be closer to this shape than to a flat
visibility array:

```rust
pub struct MetalGridWorkUnit<'a> {
    pub grid_shape: [u32; 2],
    pub tile_edge: u32,
    pub support: u16,
    pub stage_planes: u16,
    pub samples: &'a [MetalGridSample],
    pub tile_offsets: &'a [u32],
    pub nonempty_tiles: &'a [u32],
    pub kernel_tables: &'a [f32],
    pub mode: MetalGridMode,
}

pub enum MetalGridMode {
    Dirty,
    Psf,
    ResidualRefresh {
        model_grid_plane: u16,
        residual_grid_plane: u16,
    },
}
```

The CPU streaming frontend should remain responsible for row-block preparation,
weighting, flag filtering, and bounded tile ownership. Metal should receive
already-bounded compact tile buckets, construct any per-cell collision groups
inside the bounded GPU work unit, and leave resident grids on device until the
caller asks for a bounded flush.
