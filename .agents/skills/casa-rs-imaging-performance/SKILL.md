---
name: casa-rs-imaging-performance
description: Use when optimizing casa-rs imaging performance or correctness/performance parity against CASA, especially for MFS, cube, mosaic, W/AW projection, MT-MFS, multi-worker CPU, Metal/GPU, benchmark bundles, or large MeasurementSet streaming behavior.
---

# casa-rs imaging performance

Improve the executing code while preserving CASA science and agreed resource
limits. Separate a cheap implementation experiment from final acceptance.

## Start with the hot code

Follow the measured call chain into loops, cache misses, allocations, copies,
decoding, hashing, and I/O. Count work per sample, block, cell, and major cycle;
distinguish one-time preparation from costs paid repeatedly. Check compiler
settings and the actual library/backend before assuming an algorithm is slow.

Inspect the matching mechanism in the last optimized pre-cutover revision
`fff9c2d553eace4b6a57b1df9ded4773f2263ceb` early. Its source and deleted
performance ledgers can reveal useful locality, reuse, and streaming choices.
Consult CASA/casacore for disputed science semantics and LibRA for a relevant
technique; do not make every local experiment a three-repository survey.

Local upstreams are under `/Users/brianglendenning/SoftwareProjects/`:
`casa`, `casacore`, and `libRA`. Reuse proven mechanisms through the current
implementation; do not restore displaced packages or duplicate production
routes. Historical timings are comparable only when workload, backend, and
timed boundary match.

## Short implementation loop

Choose one concrete cost from existing evidence and try the smallest coherent
change that tests it. A brief hypothesis, parent/candidate command and result,
and affected correctness check are enough. Once the evidence selects the next
change, stop expanding instrumentation and test the change.

Use an existing focused test or production-path harness. Add a counter or timer
only for an unresolved decision. Prefer deterministic work-count regressions
(bytes hashed, copies, allocations, loads) to flaky elapsed-time assertions;
use paired timings separately to measure the benefit. Local A/B comparisons do not
require a full-workload speedup ceiling, a new CASA component timer, or a
campaign-sized receipt. Estimate wider benefit before an expensive run or an
end-to-end performance claim, not as a prerequisite to a cheap experiment.

Prefer repeated paired measurements on the same bounded workload when host
noise matters. Include the actual source/build, input selection, backend, cache
state, elapsed time, and relevant output check. Measure coarse phases or sample
the hot path without timing every scalar. Separate exclusive costs from nested
totals and read wall from physical disk time.

On a miss, examine how often work repeats and how much data it touches before
polishing cache bookkeeping. Consider reuse, traversal locality, redundant
passes, buffer reuse, and conversion costs. Validation is work too: distinguish
required behavior from its current number and placement of checks. Do not
silently bypass a promised guarantee; explicitly revise it and its tests when
authorized. Do not add a second production mode merely to preserve old machinery.

Keep one candidate at a time. Follow the existing hypothesis retry/escalation
limits; setup errors are not failed scientific hypotheses. Retain a known-good
parent and rejected results. A failed correctness check vetoes promotion, not
routine in-scope fixture repair or a new discriminating local check.

## Scale only after the local result

For iteration, reduce sample volume while retaining the mechanism under test:
field/pointing coverage for mosaics, widefield geometry for W/AW, and spectral
behavior for cubes. Do not make cache residency toy-sized when testing cache
behavior. A smaller row is diagnostic evidence, not final scientific acceptance.

A forecast beyond 30 minutes calls for a passed mode-faithful turnaround check
from the candidate build/configuration before the larger run. Runs beyond 90
minutes are for final evidence or explicit user direction. Preserve actual
user-approved wall, memory, swap, attempt, and overnight checkpoints; repairing
the harness or changing process never renews an exhausted allowance.
After two full-resolution failures in one campaign, stop for an explicit
continuation, fallback, or waiver decision. This does not limit cheap local A/Bs.

Before a long run, check data and disk availability and use progress/resource
monitoring that can stop at the agreed limits. Do not wait through opaque runs
or repeated unchanged failures. Reuse completed CASA references when their
inputs and parameters have not changed.

## Acceptance and reporting

Use the smallest existing check that exercises the change, then the applicable
issue-named acceptance. Reuse unaffected green evidence; do not mechanically
rerun an eight-stage ladder or broaden the ticket's gates.

CASA comparisons must preserve the mode's selection, weighting, normalization,
products, and scientific tolerances. For a parity difference, instrument the
first divergent computation in both implementations rather than trying blind
parameter changes. Use `tools/perf/imager/run_workload.py` bundles when useful.

Compare every required image/product numerically and visually at acceptance.
Use shared scales for CASA/casa-rs panels and labelled differences. Prefer
beam-aware structure metrics; check required topology, WCS, support, masks,
beam, flux, peaks, centroids, and nonfinite values. Treat structured weight/PB
differences seriously. Diagnostic bitwise checksums are not a substitute for
the approved scientific comparisons.

Keep serial CPU acceptance independent where required. Additional workers or
Metal cannot conceal a serial miss; report the backend and partitions actually
executed. A skipped or unreachable execution is not a passing comparison.
Never full-materialize a MeasurementSet or add per-worker full grids to obtain
a timing win. Preserve bounded streaming and shared infrastructure.

Report the measured outcome first: changed function, before/after time,
correctness result, and remaining limitation. A component improvement is not
an end-to-end speedup. Final performance claims require the approved workload
and matched CASA timing; correctness-only work does not acquire a speed target.
Keep logs and one current work summary, not a growing parallel narrative.
