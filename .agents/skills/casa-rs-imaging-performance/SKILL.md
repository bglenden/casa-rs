---
name: casa-rs-imaging-performance
description: Use when optimizing casa-rs imaging performance or correctness/performance parity against CASA, especially for MFS, cube, mosaic, W/AW projection, MT-MFS, multi-worker CPU, Metal/GPU, benchmark bundles, or large MeasurementSet streaming behavior.
---

# Skill: casa-rs imaging performance

## Purpose

Optimize casa-rs imaging modes without trading away CASA parity, mode semantics, or credible performance evidence.

## Historical implementation reconnaissance

Before proposing or implementing an imaging performance change, inspect all
three relevant implementation lineages:

1. Start with casa-rs commit
   `fff9c2d553eace4b6a57b1df9ded4773f2263ceb`, the last optimized production
   snapshot that still contains the displaced pre-cutover imaging machinery.
   Use its code and deleted performance ledgers to identify proven data-flow,
   allocation, streaming, I/O, gridding, degridding, weighting, and scheduling
   techniques. Consult earlier history when the relevant technique predates
   that snapshot.
2. Inspect the corresponding CASA/casacore implementation under
   `/Users/brianglendenning/SoftwareProjects/casa` and
   `/Users/brianglendenning/SoftwareProjects/casacore`. Preserve CASA science
   semantics and use its mature hot-path choices as a performance floor, not a
   requirement to mirror its C++ structure.
3. Inspect the corresponding LibRA implementation under
   `/Users/brianglendenning/SoftwareProjects/libRA`, especially for newer
   gridding, prediction, weighting, CPU/GPU, and large-data techniques.

Record a compact old-to-current map before editing: the historical technique,
its measured or documented benefit, its current equivalent or absence, the
shared owner where it belongs now, and whether it is retained, adapted, or
rejected. Do not rediscover an already documented experiment unless the
architecture, workload, or hardware difference gives a concrete reason to
retest it.

Historical code is evidence, not an architecture template. Port the mechanism
through current owner boundaries and prefer one broadly reusable optimization
for multiple imaging modes. A narrow optimization is acceptable only when the
science or data shape is genuinely mode-specific, and it must remain cohesive
inside the canonical owner. Never restore a displaced package, compatibility
shim, fallback route, calculation-bearing frontend, duplicated planner, or old
dependency direction merely to recover speed.

## Core workflow

1. Complete the historical implementation reconnaissance above.
2. Establish correctness before claiming speed.
   Compare CASA and casa-rs products numerically and visually for the products the mode writes: `.image`, `.residual`, `.model`, `.psf`, `.pb`, `.weight`, `.sumwt`, `.image.pbcor`, and Taylor products where relevant. CASA and casa-rs panels must use the same color scale; difference panels must be labeled.
3. Use the right dataset tier for the question.
   Small rows are for correctness and debugging only. Medium and large rows are required before making closeout performance claims.
4. Instrument before optimizing.
   Attribute time to MS open/read, selection, prepare, density/weighting, gridding, degridding/residual refresh, minor cycle, Clark bookkeeping, PB/weight generation, product writing, and frontend/core totals.
5. Avoid blind long runs.
   If a large run has no pass/stage progress or product output after a few minutes, stop and add progress instrumentation before waiting longer.
6. Reuse shared imaging infrastructure.
   Extend shared streaming prepare, row/run preservation, bounded residency, worker planning, grouped GPU input contracts, and benchmark bundle code. Do not create a mode-specific duplicate when a shared routine can be generalized.
7. Preserve CASA semantics while sharing mechanics.
   Cube, cubedata, mosaic, MT-MFS, W-projection, AW-style, MFS, and multiscale modes must keep their mode-specific CASA behavior.
8. Never full-materialize large imaging inputs.
   A path that requires materializing all visibilities or cube planes for a large MS is an architecture bug. Fix bounded streaming once in shared I/O/prepare code and remove redundant misleading paths.
9. Compare serial, multi-worker, and Metal honestly.
   Keep serial CPU as a required performance gate, not merely a comparison
   row. On a matched workload whose CASA oracle is single-process, the
   `workers = 1` production path must independently meet the accepted CASA
   serial target before a multi-worker or device result can count as a
   performance success. More workers, Metal, or another accelerator may prove
   scaling, but may never compensate for or conceal a serial miss. Do not
   assume fixed-tile, central quadrants, more workers, or Metal wins without
   total runtime and stage evidence.
10. Make `auto` usable.
   Explicit parameters are good for debugging, but user-facing defaults should choose reasonable worker counts, buffers, strategies, and Metal eligibility.
11. Prefer explicit parameters over environment variables.
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

## Campaign control

Before the first implementation change, record each candidate's single causal
hypothesis, exact parent and candidate revision, predicted terminal effect,
discriminating metric, scientific ceiling, memory and swap limits, wall and
stage limits, automatic falsifier, known-correct fallback, and artifact
retention class.

Before admitting that candidate to production code, compute its optimistic
repeat-weighted end-to-end ceiling. Record the affected phase's measured wall
share, how often it occurs in the matched full workload, the fraction actually
touched, and the maximum removable fraction. A probe whose optimistic ceiling
is below the campaign's declared materiality threshold remains diagnostic
evidence; it is not an implementation candidate. Do not infer removable cost
from an inclusive profiler stack or apply a terminal-only percentage to every
major cycle.

- Maintain one active candidate at a time. A failed candidate must have its
  falsifier recorded and its code reverted or preserved as an immutable commit
  before another candidate begins. Do not stack a new hypothesis on a rejected
  implementation.
- Build and run a fast stage-local discriminator before a medium or large
  end-to-end candidate run. It must exercise the production kernel seam, retain
  the mode's load-bearing shape, and verify an exact or accepted scientific
  checksum. Setup or source capture may be outside the timed interval, but the
  resulting harness must never become a production fast path.
- Profile the phase paid most often by the full workload. Separate common
  intermediate work from terminal-only products, publication, and writeback,
  and prefer exclusive samples or coarse block-level timing over per-sample
  clocks that perturb the hot loop.
- Separate correctness, resource feasibility, and performance. Once a slow
  path passes the accepted scientific contract, freeze and land that baseline
  before optimizing it in a separate wave unless the approved scope explicitly
  couples them.
- Classify a failure before acting: scientific correctness, resource safety,
  performance, test/tooling, or fixture/provenance. Do not change production
  science code to repair a stale fixture, harness defect, or performance-only
  miss.
- A run projected over 30 minutes requires a passed mode-faithful turnaround
  receipt from the same source revision, binary, and effective configuration.
  A run projected over 90 minutes is final evidence only. Changing executable
  code or runtime configuration revokes the receipt and returns the candidate
  to the turnaround row.
- After a long-run failure, reproduce the failed stage with a bounded probe
  before another long run. After two full-resolution failures in one campaign,
  stop for an explicit continuation, fallback, or waiver decision.
- Long-run progress must report stage units completed and total, elapsed time,
  resource headroom, and a conservative completion forecast. Abort when the
  forecast exceeds the candidate's wall ceiling; log activity alone is not
  evidence of viable progress.
- Campaign approval may authorize repeated small and medium experiments inside
  these guards. It does not authorize an unbounded sequence of full-size runs
  or new architecture candidates.

Use a fail-closed execution receipt before substantive work. It must identify
the dataset and selection, CASA/reference and CF-cache identity, source order
and row blocking, candidate revision and binary, selected backend, and actual
CPU or device partitions executed. A skipped Metal test, unreachable target,
or mismatched fixture is a failed discriminator, not evidence about the
candidate.

## Correctness rules

- Use `tools/perf/imager/run_workload.py` bundles when possible, because they capture timings, comparisons, panels, and review gates together.
- Use beam-aware structured-difference metrics for imaging products. Raw adjacent-pixel correlation is not a primary structure test because the PSF correlates pixels.
- Treat low-amplitude but structured `.weight` or `.pb` differences as suspicious until instrumented or explicitly accepted.
- Use CASA compatibility switches narrowly. The CASA Hogbom inclusive-iteration behavior is a Hogbom compatibility mode, not a Clark or general clean switch.
- If correctness regresses, do not hide it behind speedup numbers. Record explicit user signoff for any accepted residual issue.
- For large-image visual review, generate same-scale CASA, casa-rs, and
  difference panels at representative random locations and around bright
  sources. Visual panels supplement rather than replace numerical and topology
  gates.
- Use an ordered acceptance ladder: fixture and execution receipt; one operator
  or source segment; cumulative checkpoint; pre-FFT or niter-zero products;
  one residual refresh; offline minor-cycle trajectory; medium CLEAN; final
  full-resolution evidence. A red stage vetoes later stages until the failure
  is explained or corrected.

## Timing rules

- Size a candidate discriminator so repeated measurements distinguish the
  affected stage from host variance and setup noise. Do not make a traversal
  or gridding discriminator longer merely by inflating unrelated FFT or
  product-writing work. Prefer interleaved repeated cohorts, report their
  variance, and label aggregation separately from problem-size evidence; the
  evidence must discriminate the proposed cause rather than satisfy an
  invented absolute duration.
- Before changing casa-rs performance code, obtain and freeze a corresponding
  CASA timing for the exact workload or component boundary being optimized.
  Match the dataset, selection, geometry, products, and timed stage. Do not use
  an end-to-end CASA time to anchor a component microbenchmark, or vice versa.
  Generate a missing matched CASA timing once, before optimization. If CASA
  cannot expose the boundary, document the closest measurable envelope and get
  explicit user approval before optimizing against an internally chosen target.
- Do not rerun CASA when dataset and CASA parameters are unchanged; treat the existing CASA timing as fixed unless CASA-side instrumentation, parameters, or data selection changed.
- For large runs, require progress lines per bounded pass so stalls can be attributed to density, prepare, gridding, residual refresh, PB/weight generation, or product writing.
- Report total wall time first, then stage timing. Tables that mix rows/columns from unrelated concerns are not useful.
- Include backend plan logs: worker count, tile/run plan, memory residency, grouped-input/cache status, and eligibility or rejection reasons.
- GPU is most useful when deconvolution, residual refresh, or compute-heavy gridding dominates. If prepare/I/O dominates, optimize streaming and row preparation first.

## Anti-patterns

- Multi-hour opaque runs without pass/stage progress.
- Tiny-dataset performance claims.
- Speculating about bottlenecks instead of instrumenting.
- Optimizing against an internally chosen target before obtaining a matched
  CASA baseline.
- Rerunning CASA just because casa-rs changed.
- Mistaking a small subset of a large MS for full-dataset performance.
- Assuming multi-worker or Metal is faster without measured total runtime.
- Adding local fast paths that duplicate shared prepare, weighting, planner, or GPU code.
- Leaving old redundant paths in place after a shared path replaces them.
- Using a full-resolution CLEAN run to discover a stage-local correctness or
  ownership defect.
- Continuing a precision or representation ladder after a material local
  improvement fails to move the first divergent operator, trajectory, or
  terminal product metric.
- Keeping multiple experimental representations, selectors, or shadow paths in
  the production diff until final cleanup.
