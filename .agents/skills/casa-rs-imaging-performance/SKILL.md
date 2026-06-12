---
name: casa-rs-imaging-performance
description: Use when optimizing casa-rs imaging performance or correctness/performance parity against CASA, especially for MFS, cube, mosaic, W/AW projection, MT-MFS, multi-worker CPU, Metal/GPU, benchmark bundles, or large MeasurementSet streaming behavior.
---

# Skill: casa-rs imaging performance

## Purpose

Optimize casa-rs imaging modes without trading away CASA parity, mode semantics, or credible performance evidence.

## Core workflow

1. Establish correctness before claiming speed.
   Compare CASA and casa-rs products numerically and visually for the products the mode writes: `.image`, `.residual`, `.model`, `.psf`, `.pb`, `.weight`, `.sumwt`, `.image.pbcor`, and Taylor products where relevant. CASA and casa-rs panels must use the same color scale; difference panels must be labeled.
2. Use the right dataset tier for the question.
   Small rows are for correctness and debugging only. Medium and large rows are required before making closeout performance claims.
3. Instrument before optimizing.
   Attribute time to MS open/read, selection, prepare, density/weighting, gridding, degridding/residual refresh, minor cycle, Clark bookkeeping, PB/weight generation, product writing, and frontend/core totals.
4. Avoid blind long runs.
   If a large run has no pass/stage progress or product output after a few minutes, stop and add progress instrumentation before waiting longer.
5. Reuse shared imaging infrastructure.
   Extend shared streaming prepare, row/run preservation, bounded residency, worker planning, grouped GPU input contracts, and benchmark bundle code. Do not create a mode-specific duplicate when a shared routine can be generalized.
6. Preserve CASA semantics while sharing mechanics.
   Cube, cubedata, mosaic, MT-MFS, W-projection, AW-style, MFS, and multiscale modes must keep their mode-specific CASA behavior.
7. Never full-materialize large imaging inputs.
   A path that requires materializing all visibilities or cube planes for a large MS is an architecture bug. Fix bounded streaming once in shared I/O/prepare code and remove redundant misleading paths.
8. Compare serial, multi-worker, and Metal honestly.
   Keep serial CPU as a baseline. Do not assume fixed-tile, central quadrants, more workers, or Metal wins without total runtime and stage evidence.
9. Make `auto` usable.
   Explicit parameters are good for debugging, but user-facing defaults should choose reasonable worker counts, buffers, strategies, and Metal eligibility.
10. Prefer explicit parameters over environment variables.
    Environment variables are acceptable for diagnostics, but performance behavior should be controllable through explicit API/CLI parameters.

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
- If correctness regresses, do not hide it behind speedup numbers. Record explicit user signoff for any accepted residual issue.

## Timing rules

- Do not rerun CASA when dataset and CASA parameters are unchanged; treat the existing CASA timing as fixed unless CASA-side instrumentation, parameters, or data selection changed.
- For large runs, require progress lines per bounded pass so stalls can be attributed to density, prepare, gridding, residual refresh, PB/weight generation, or product writing.
- Report total wall time first, then stage timing. Tables that mix rows/columns from unrelated concerns are not useful.
- Include backend plan logs: worker count, tile/run plan, memory residency, grouped-input/cache status, and eligibility or rejection reasons.
- GPU is most useful when deconvolution, residual refresh, or compute-heavy gridding dominates. If prepare/I/O dominates, optimize streaming and row preparation first.

## Anti-patterns

- Multi-hour opaque runs without pass/stage progress.
- Tiny-dataset performance claims.
- Speculating about bottlenecks instead of instrumenting.
- Rerunning CASA just because casa-rs changed.
- Mistaking a small subset of a large MS for full-dataset performance.
- Assuming multi-worker or Metal is faster without measured total runtime.
- Adding local fast paths that duplicate shared prepare, weighting, planner, or GPU code.
- Leaving old redundant paths in place after a shared path replaces them.
