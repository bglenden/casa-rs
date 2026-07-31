---
name: casa-rs-imaging-performance
description: Use when optimizing casa-rs imaging performance or correctness/performance parity against CASA, especially for MFS, cube, mosaic, W/AW projection, MT-MFS, multi-worker CPU, Metal/GPU, benchmark bundles, or large MeasurementSet streaming behavior.
---

# Skill: casa-rs imaging performance

## Purpose

Optimize casa-rs imaging modes against a frozen scientific acceptance envelope
without inheriting CASA's implementation structure or mistaking local kernel
tuning for end-to-end progress.

## Core workflow

1. Freeze the scientific floor before optimizing.
   Define the required products, metadata, numerical ceilings, source fidelity,
   noise, dynamic range, structured-difference, stability, and visual gates.
   Compare CASA and casa-rs products numerically and visually for the products
   the mode writes: `.image`, `.residual`, `.model`, `.psf`, `.pb`, `.weight`,
   `.sumwt`, `.image.pbcor`, and Taylor products where relevant. CASA and
   casa-rs panels must use the same color scale; difference panels must be
   labeled. Bit identity, arithmetic trajectory, component order, and marginal
   cutoff topology are non-goals unless the acceptance envelope requires them.
2. Establish physical lower bounds and an Amdahl model.
   Before choosing an optimization, identify mandatory output bytes and work,
   theoretical whole-run benefit if each stage disappeared, physical memory
   traffic, peak live state, and CPU/GPU transfer or synchronization costs.
   Stop optimizing a stage whose complete removal cannot materially change
   end-to-end wall time, unless it is required for memory feasibility or
   another accepted workload.
3. Compare competing representations.
   Carry at least two credible algorithm or dataflow representations until a
   bounded discriminator rejects one. Treat per-tap operator programs,
   compensation planes, dense model images, eager product arrays, and repeated
   full-image passes as implementation choices rather than semantic
   requirements. Existing infrastructure and sunk implementation cost do not
   justify preserving an inferior representation.
4. Use the right dataset tier for the question.
   Small rows are for correctness and debugging only. Medium and large rows are required before making closeout performance claims.
5. Instrument physical work before optimizing.
   Attribute time to MS open/read, selection, prepare, density/weighting,
   operator planning and materialization, gridding, destination zeroing and
   stores, FFT/normalization, degridding/residual refresh, minor cycle, Clark
   bookkeeping, PB/weight generation, product derivation and writing, and
   frontend/core totals. Where available, record physical bytes read/written,
   resident-set transitions, page faults, compression/swap, GPU occupancy,
   allocation, synchronization, stalls, and CPU/GPU transfers.
6. Give every experiment a falsifiable contract.
   State the architectural hypothesis, isolated implementation boundary,
   required metrics, correctness checks, abort gate, promotion gate, and the
   result that would falsify the broader architecture. Preserve both positive
   and negative evidence.
7. Avoid blind long runs.
   If a large run has no pass/stage progress or product output after a few minutes, stop and add progress instrumentation before waiting longer.
8. Reuse shared imaging infrastructure when it fits the winning representation.
   Extend shared streaming prepare, row/run preservation, bounded residency,
   worker planning, grouped GPU input contracts, and benchmark bundle code when
   they do not constrain the operator architecture. Do not create a mode-specific
   duplicate when a shared routine can be generalized.
9. Preserve CASA semantics while sharing mechanics.
   Cube, cubedata, mosaic, MT-MFS, W-projection, AW-style, MFS, and multiscale modes must keep their mode-specific CASA behavior.
10. Require an explicit liveness schedule.
    Never materialize all visibilities, per-tap operator expansions, image
    states, cube planes, or output products unless a lifetime analysis proves
    they must coexist. Keep authoritative iterative state minimal and derive
    final products lazily or tilewise while still charging their full
    computation and write cost to end-to-end time.
11. Compare serial, multi-worker, Metal, and replacement algorithms honestly.
   Keep serial CPU as a baseline. Do not assume fixed-tile, central quadrants, more workers, or Metal wins without total runtime and stage evidence.
12. Make `auto` cost based.
   Explicit parameters are good for debugging, but user-facing defaults should
   choose operator backend, worker count, buffers, strategies, and Metal
   eligibility from image size, sample count, support distribution, W range,
   direction-dependent state count, subgrid or tile occupancy, Taylor moments,
   live memory, and measured host/device characteristics. Never select by
   dataset identity.
13. Prefer explicit parameters over environment variables.
    Environment variables are acceptable for diagnostics, but performance behavior should be controllable through explicit API/CLI parameters.

## Breakthrough search

- Keep a portfolio spanning work elimination, representation changes, data
  movement, precision, CPU concurrency, and GPU residency. Do not spend a wave
  moving one local bottleneck among these categories.
- Use operator-anatomy discriminators before large rewrites. Separate planning,
  representation construction, decoding, arithmetic, destination traffic,
  accumulation precision, FFT/normalization, and product materialization.
- In pixels-much-greater-than-visibilities regimes, explicitly challenge dense
  global grids, expanded convolution programs, repeated full-image passes, and
  global-per-pointing work. Spatial tiles are an ownership substrate, not
  automatically the breakthrough.
- Maintain an approximation budget by stage. An approximate experiment must
  expose a tunable error control and an exact or higher-precision diagnostic
  fallback. Promote it only through the same frozen scientific floor.
- For a major architecture reset, seek an independent adversarial review when
  one is available. Then challenge its model with the actual source
  representation, counters, memory ledger, and prior negative experiments.
  Convert the surviving ideas into ranked, falsifiable experiments; do not
  copy an attractive architecture label into the plan without a discriminator.
- After repeated local changes produce only small deltas, stop that family and
  reopen the architecture portfolio. Revisit a rejected cache, tile, worker,
  precision, or algorithm choice only when new evidence invalidates its
  falsification.

## Iteration dataset scaling

When an estimated performance run will take many tens of minutes or more, create a smaller but mode-faithful row before optimizing. The goal is several timing turns per hour.

- Preserve the imaging mode's shape. For mosaic work, keep fields and pointings; for W/AW work, keep widefield geometry; for cube work, keep spectral-axis behavior.
- Prefer reducing sample volume over changing semantics: reduce selected channels or channel width, skip integrations/time rows, or reduce rows per field proportionally.
- Use a smaller image only when it does not hide the bottleneck being studied.
- Do not shrink until memory residency becomes toy-sized. The row should still exercise bounded streaming, worker planning, PB/weight generation, and CPU/GPU data movement.
- Label reduced rows as optimization-turnaround datasets, not final performance evidence.
- After a candidate speedup works on the reduced row, rerun medium or large evidence rows before closeout.
- For mosaics, do not drop fields unless the explicit question is single-field behavior; field distribution and PB/weight accumulation are part of the performance problem.
- If the first instrumented estimate is likely to exceed 30 minutes, stop and make a mode-faithful reduced row. If it is likely to exceed 60-90 minutes, continue only for final evidence or explicit user request.

## Correctness rules

- Use `tools/perf/imager/run_workload.py` bundles when possible, because they capture timings, comparisons, panels, and review gates together.
- Use beam-aware structured-difference metrics for imaging products. Raw adjacent-pixel correlation is not a primary structure test because the PSF correlates pixels.
- Treat low-amplitude but structured `.weight` or `.pb` differences as suspicious until instrumented or explicitly accepted.
- Use CASA compatibility switches narrowly. The CASA Hogbom inclusive-iteration behavior is a Hogbom compatibility mode, not a Clark or general clean switch.
- If a candidate misses the frozen scientific floor, do not hide it behind
  speedup numbers. Record explicit user signoff before changing the envelope.
- Reduction ordering, precision, discretization, and reconstruction history may
  change when the envelope permits it. Classify each experiment as an
  equivalent transformation, a tunable discretization, an explicit numerical
  approximation, or a different reconstruction algorithm.

## Timing rules

- Do not rerun CASA when dataset and CASA parameters are unchanged; treat the existing CASA timing as fixed unless CASA-side instrumentation, parameters, or data selection changed.
- For large runs, require progress lines per bounded pass so stalls can be attributed to density, prepare, gridding, residual refresh, PB/weight generation, or product writing.
- Report total wall time first, then stage timing. Tables that mix rows/columns from unrelated concerns are not useful.
- Include backend plan logs: worker count, tile/run plan, memory residency, grouped-input/cache status, and eligibility or rejection reasons.
- GPU is most useful when deconvolution, residual refresh, or compute-heavy gridding dominates. If prepare/I/O dominates, optimize streaming and row preparation first.
- Compare competing algorithms at identical scientific output boundaries,
  including planning, setup, data movement, FFTs, synchronization, product
  derivation, and writes. Kernel-only wins are evidence, not end-to-end claims.

## Anti-patterns

- Multi-hour opaque runs without pass/stage progress.
- Tiny-dataset performance claims.
- Speculating about bottlenecks instead of instrumenting.
- Rerunning CASA just because casa-rs changed.
- Mistaking a small subset of a large MS for full-dataset performance.
- Assuming multi-worker or Metal is faster without measured total runtime.
- Adding local fast paths that duplicate shared prepare, weighting, planner, or GPU code.
- Leaving old redundant paths in place after a shared path replaces them.
- Optimizing a materialized operator indefinitely without measuring a
  matrix-free or factorized alternative.
- Retaining eager dense state because a product is eventually required.
- Claiming a multi-x opportunity for a stage whose theoretical elimination has
  a small Amdahl benefit.
- Repeating a rejected cache, tile, worker, precision, or algorithm experiment
  without new evidence.
