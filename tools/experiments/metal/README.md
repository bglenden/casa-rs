# Apple Metal Gridding Experiment

Truth class: experimental
Last reality check: 2026-05-21
Verification: `swift run MetalGridExperiment --samples 2000 --imsize 128 --distribution uniform --tile-edge 32`; `swift run -c release MetalGridExperiment --samples 200000 --imsize 512 --distribution uniform --tile-edge 64`

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

## Initial Results On Apple M4

Release-mode runs on 2026-05-21 used the runtime-compiled Swift/Metal harness.
The CPU reference is single-threaded f32 accumulation over the same synthetic
records. Error metrics compare Metal output to that CPU reference.

| Case | Strategy | GPU s | Samples/s | Tap updates/s | Max error | Relative RMS |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 20k samples, 512, support 3, uniform | global atomic | 0.001753 | 1.14e7 | 5.59e8 | 3.03e-8 | 7.56e-8 |
| 20k samples, 512, support 3, uniform | cell owner | 0.088013 | 2.27e5 | 1.11e7 | 3.10e-8 | 7.25e-8 |
| 20k samples, 512, support 3, uniform | sorted reduce | 0.000214 | 9.33e7 | 4.57e9 | 0.00 | 0.00 |
| 20k samples, 512, support 3, uniform | tile bucket cell owner | 0.001149 | 1.74e7 | 8.53e8 | 3.10e-8 | 7.29e-8 |
| 20k samples, 512, support 3, uniform | tile cell bins | 0.001713 | 1.17e7 | 5.72e8 | 3.10e-8 | 7.29e-8 |
| 20k samples, 512, support 3, uniform | GPU-built tile cell bins | 0.000613 | 3.26e7 | 1.60e9 | 3.33e-8 | 7.77e-8 |
| 20k samples, 512, support 3, central cluster | global atomic | 0.003073 | 6.51e6 | 3.19e8 | 1.22e-6 | 6.01e-7 |
| 20k samples, 512, support 3, central cluster | cell owner | 0.079669 | 2.51e5 | 1.23e7 | 5.37e-7 | 2.45e-7 |
| 20k samples, 512, support 3, central cluster | sorted reduce | 0.000550 | 3.64e7 | 1.78e9 | 0.00 | 0.00 |
| 20k samples, 512, support 3, central cluster | tile bucket cell owner | 0.001395 | 1.43e7 | 7.02e8 | 1.07e-6 | 3.73e-7 |
| 20k samples, 512, support 3, central cluster | tile cell bins | 0.000893 | 2.24e7 | 1.10e9 | 1.07e-6 | 3.73e-7 |
| 20k samples, 512, support 3, central cluster | GPU-built tile cell bins | 0.000708 | 2.83e7 | 1.38e9 | 1.61e-6 | 6.54e-7 |
| 20k samples, 512, support 3, boundary | global atomic | 0.001874 | 1.07e7 | 5.23e8 | 1.80e-7 | 1.84e-7 |
| 20k samples, 512, support 3, boundary | cell owner | 0.087449 | 2.29e5 | 1.12e7 | 1.50e-7 | 1.29e-7 |
| 20k samples, 512, support 3, boundary | sorted reduce | 0.000266 | 7.52e7 | 3.69e9 | 0.00 | 0.00 |
| 20k samples, 512, support 3, boundary | tile bucket cell owner | 0.001342 | 1.49e7 | 7.30e8 | 1.79e-7 | 1.33e-7 |
| 20k samples, 512, support 3, boundary | tile cell bins | 0.000337 | 5.94e7 | 2.91e9 | 1.79e-7 | 1.33e-7 |
| 20k samples, 512, support 3, boundary | GPU-built tile cell bins | 0.000486 | 4.11e7 | 2.01e9 | 2.11e-7 | 2.02e-7 |
| 20k samples, 1024, support 3, uniform | global atomic | 0.001854 | 1.08e7 | 5.29e8 | 2.99e-8 | 6.00e-8 |
| 20k samples, 1024, support 3, uniform | cell owner | 0.272481 | 7.34e4 | 3.60e6 | 2.99e-8 | 5.95e-8 |
| 20k samples, 512, support 5, uniform | global atomic | 0.001573 | 1.27e7 | 1.54e9 | 2.30e-8 | 9.67e-8 |
| 20k samples, 512, support 5, uniform | cell owner | 0.090628 | 2.21e5 | 2.67e7 | 1.54e-8 | 8.72e-8 |
| 200k samples, 512, support 3, uniform | global atomic | 0.006619 | 3.02e7 | 1.48e9 | 1.08e-7 | 1.22e-7 |
| 200k samples, 512, support 3, uniform | sorted reduce | 0.001803 | 1.11e8 | 5.44e9 | 0.00 | 0.00 |
| 200k samples, 512, support 3, uniform | tile bucket cell owner | 0.011306 | 1.77e7 | 8.67e8 | 1.05e-7 | 1.17e-7 |
| 200k samples, 512, support 3, uniform | tile cell bins, edge 32 | 0.000777 | 2.57e8 | 1.26e10 | 1.20e-7 | 1.22e-7 |
| 200k samples, 512, support 3, uniform | tile cell bins, edge 64 | 0.000756 | 2.65e8 | 1.30e10 | 1.05e-7 | 1.17e-7 |
| 200k samples, 512, support 3, uniform | tile cell bins, edge 128 | 0.002853 | 7.01e7 | 3.44e9 | 9.22e-8 | 1.15e-7 |
| 200k samples, 512, support 3, uniform | GPU-built tile cell bins, edge 64 | 0.005487 | 3.64e7 | 1.79e9 | 2.09e-7 | 1.57e-7 |
| 200k samples, 512, support 3, central cluster | global atomic | 0.027597 | 7.25e6 | 3.55e8 | 7.79e-6 | 1.39e-6 |
| 200k samples, 512, support 3, central cluster | GPU-built tile cell bins, edge 64 | 0.008604 | 2.32e7 | 1.14e9 | 1.16e-5 | 1.73e-6 |

The sorted-reduce rows exclude CPU-side expansion and sorting. That preparation
cost was `0.023917 s` and `19.8 MB` of reduce buffers for the 20k uniform case,
and `0.254434 s` and `161.0 MB` for the 200k uniform case.
The tile-bucket rows use `--tile-edge 64`; tile-bucket preparation was
`0.000154 s` and `3.15 MB` for 20k uniform, and `0.000918 s` and `8.91 MB` for
200k uniform.
The tile-cell-bin rows keep compact samples and add one `u32` sample reference
per tap contribution. For `200k` uniform, tile-cell-bin preparation was
`0.072883 s`, used `50.6 MB`, and produced `9.8M` sample references. That is
far better than the sorted-reduce value expansion but still too expensive to do
on the CPU in a production path.
The GPU-built tile-cell-bin rows include GPU count, CPU prefix/read, GPU fill,
and GPU reduce, but exclude final host-side tile merge. For `200k` uniform,
those were `0.001099 s` count, `0.001571 s` prefix/read,
`0.003528 s` fill, and `0.000860 s` reduce. For `200k` central cluster, count
and fill stayed cheap but reduce rose to `0.005858 s` because only `2025` active
tile cells hold all `9.8M` sample references.

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
