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

Distinguish block I/O, block scheduling, and block numerical kernels. Large reads
or worker batches can still call an expensive object/adapter chain for every
sample. Follow into that chain: look for metadata reconstruction, allocations,
invariant checks, repeated indexing/geometry, generic dispatch, and strided access.
Bind plan/source/shape facts at their valid scope; do row/channel setup once and
operate on simple array slices with shared scientific primitives. Keep required
per-value validity/flag checks and reduction semantics. Reuse the science, not
necessarily a historical per-sample interface. Scalar arithmetic is not itself
the diagnosis; inspect compiler output when hoisting or vectorization is the
unresolved question. Do not prescribe SIMD or fast-math as a substitute for this.

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

Choose one concrete cost from existing evidence and make a coherent change that
tests it. One hypothesis can require coordinated caller, kernel, and writer edits;
batch those edits and builds instead of treating each connection as a candidate.
A brief hypothesis, parent/candidate command and result, and affected correctness
check are enough. Once the evidence selects the next change, stop expanding
instrumentation and test the change. Reach the complete application path early.

Use an existing focused test or production-path harness. Add a counter or timer
only for an unresolved decision. Prefer deterministic work-count regressions
(bytes hashed, copies, allocations, loads) to flaky elapsed-time assertions;
use before/after timings separately to measure the benefit. Local A/B comparisons
do not require a full-workload speedup ceiling, a new CASA component timer, or a
campaign-sized receipt. Estimate wider benefit before an expensive run or an
end-to-end performance claim, not as a prerequisite to a cheap experiment.

Start with one before/after observation and correctness checks for a clear effect.
Repeat only when noise or effect size leaves the decision unresolved; do not
impose a fixed pair count. If unchanged phases drift from historical timings,
run a current parent control before attributing that drift to the candidate.
Include source/build, selection, backend, cache state, elapsed time and output
checks; disclose single observations without claiming statistical significance.
Measure coarse phases or sample the hot path without timing every scalar.
Separate exclusive costs from nested totals and read wall from physical disk
time. Removed bytes or objects do not establish saved time: an enclosing timer
can include required inspection or arithmetic that survives the change. Forecast
savings only from work actually removed; distinguish cumulative traffic from
peak live storage and avoid double-counting nested or overlapping worker time.

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

## Choose a representative workload

For iteration, reduce sample volume while retaining the mechanism under test:
field/pointing coverage for mosaics, widefield geometry for W/AW, and spectral
behavior for cubes. Do not make cache residency toy-sized when testing cache
behavior. A smaller row is diagnostic evidence, not final scientific acceptance.

Use a mode-faithful turnaround check when the full workload would make iteration
impractical. Move to an authorized representative size when startup or cache
residency hides the relevant cost; do not exhaust small-case tweaks first.
Preserve actual user-approved wall, memory, swap, attempt, escalation and overnight
limits; do not invent fixed time or repetition gates. Repairing the harness or
changing process never renews an exhausted allowance.

Before a long run, check data and disk availability and use progress/resource
monitoring that can stop at the agreed limits. Do not wait through opaque runs
or repeated unchanged failures. Reuse completed CASA references when their
inputs and parameters have not changed.

## Acceptance and reporting

ADR-0014 removes production product attestation. Trusted in-process product
generation must transfer bounded windows to the CASA writer without content
hashing or verification-only full-array rereads for publication permission.
Do not optimize or recreate the retired mechanism, including initial hashes,
backing stores or phases used only by it. Preserve science, inventory, shape,
run/lifecycle and I/O checks plus atomic individual-image replacement. Failed
publication means an incomplete run requiring rerun; do not retain resumable
per-member recovery or whole-set rollback. Genuine persistence/external-input
checksums and diagnostic test/benchmark fingerprints remain distinct. Any
reintroduction needs an explicit ADR and user approval with a concrete failure
model and measured cost.

The rule also covers reconstruction-model ownership across execution attempts:
validate scientific values/support when introduced or modified, not by repeated
model hashes or verification-only lifecycle scans. Routine operational progress
must not serialize or hash full receipts/plans, or scan historical receipt
stores; current admission is authoritative. Exceptions need a concrete failure
model and demonstrated benefit.

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
Do not full-materialize a MeasurementSet or replicate an entire cube per worker
to evade bounded streaming. Account for necessary worker-local plane grids and
scratch in admission; concurrency must fit the agreed memory limits. Preserve
shared infrastructure.

Report the measured outcome first: changed function, before/after time,
correctness result, and remaining limitation. A component improvement is not
an end-to-end speedup. Final performance claims require the approved workload
and matched CASA timing; correctness-only work does not acquire a speed target.
Keep logs and one current work summary, not a growing parallel narrative.
