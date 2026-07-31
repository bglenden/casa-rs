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
   and negative evidence. Label material claims as `[proven]` for an algebraic
   identity or checked invariant, `[measured]` for a receipt-bound workload,
   commit, hardware, and counter set, `[projected]` for an explicit formula
   with measured inputs and sensitivity, or `[hypothesis]` for an untested
   causal or performance claim. Never let a projection silently graduate into
   a measurement. Close every experiment by promoting it, retaining it as an
   explicitly named production-inert reference/oracle, or deleting its
   executable path after preserving the receipt, counters, kill rationale,
   workload identity, and condition that would justify reconsideration.
7. Avoid blind long runs.
   If a large run has no pass/stage progress or product output after a few minutes, stop and add progress instrumentation before waiting longer.
8. Reuse shared imaging infrastructure when it fits the winning representation.
   Extend shared streaming prepare, row/run preservation, bounded residency,
   worker planning, grouped GPU input contracts, and benchmark bundle code when
   they do not constrain the operator architecture. Do not create a mode-specific
   duplicate when a shared routine can be generalized. A bounded disposable
   architecture discriminator may bypass shared abstraction, production API,
   generalized mode-coverage, and long-term reuse requirements when doing so
   isolates the physical question faster. It may not bypass workload receipt
   binding, semantic scope, memory and stage counters, fail-closed isolation,
   numerical comparison, or production feature gating. Keep it behind an
   experimental control and either delete it or integrate it through the
   shared boundary after the architecture decision.
9. Preserve CASA semantics while sharing mechanics.
   Cube, cubedata, mosaic, MT-MFS, W-projection, AW-style, MFS, and multiscale modes must keep their mode-specific CASA behavior.
10. Require an explicit liveness schedule.
    Never materialize all visibilities, per-tap operator expansions, image
    states, cube planes, or output products unless a lifetime analysis proves
    they must coexist. Keep authoritative iterative state minimal and derive
    final products lazily or tilewise while still charging their full
    computation and write cost to end-to-end time. Derived materialization is
    allowed when a formula bounds its production-geometry size, the phase
    liveness ledger includes persistent, derived, transform, writer, allocator,
    and system-reserve bytes, and that total fits the safe target envelope.
    The experiment must charge construction, subsequent reads, traffic, and
    eviction, and show that materialization beats recomputation rather than
    merely fitting in capacity.
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

## Architecture-reset gate

Enter architecture-reset mode when any of these is true:

- The incumbent cannot satisfy production-geometry memory liveness without
  swap or unimplemented oversubscription.
- Ideal removal of the stage being optimized cannot close a material fraction
  of the remaining end-to-end gap.
- Two successive changes preserve the same dominant scaling variable, move the
  bottleneck, or produce only local improvements.
- Persistent state scales with convolution support, expanded operator entries,
  dense iterative pixels, or output products that need not coexist.
- A replacement has a credible lower asymptotic work or state bound.

While this gate is active, freeze further production optimization of the
incumbent family. Retain it only as a benchmark or scientific comparator until
a replacement promotes. CASA defines the scientific envelope; its operator
discretization, arithmetic ordering, reconstruction history, and storage
structure are not implementation requirements unless the envelope says so.

## Incumbent falsification certificate

Before another local optimization, record:

1. The incumbent's dominant scaling variables.
2. Persistent and peak-live bytes as formulas, not only observations.
3. Physical reads, writes, updates, arithmetic, FFTs, and full-image passes.
4. The end-to-end Amdahl bound if the proposed stage became free.
5. Production-geometry memory and wall-time projections.
6. The observation that would falsify the incumbent as a production
   architecture.

If memory or lower-bound analysis already falsifies the incumbent, do not
repair it merely because it is implemented.

## Mandatory architecture portfolio

Carry candidates from at least four independent families until bounded
evidence rejects all but one:

- Work elimination: reduce full forward/adjoint applications, dense passes, or
  eagerly generated products.
- Measurement-operator replacement: use a different gridding, degridding,
  subgrid, facet, stacking, or transform architecture.
- Factorization: separate W, A, pointing, frequency, polarization, and
  convolution structure; test basis, separable, tensor, or parametric forms.
- Sparse or local iterative state: restrict authoritative reconstruction state
  to the scientifically active region and derive dense products lazily.
- Visibility reduction: exact coalescing or controlled averaging under an
  explicit error budget.
- A different reconstruction algorithm that reduces expensive operator calls.

Precision, worker count, cache policy, source order, tile size, GPU use, and
compression format are tactics within a family. They count as independent
architectures only when they change asymptotic state or work. At least one
candidate must eliminate the incumbent's dominant representation, and at least
one must eliminate repeated work.

## Dominant-representation challenge

For every large retained structure, state:

- Why it is semantically authoritative.
- Why it must exist at that resolution and for that lifetime.
- Why it cannot be generated or applied in bounded tiles, subgrids, facets,
  basis passes, or visibility-domain form.
- Its bytes per visibility, output pixel, operator state, and useful update.

Moving values into mmap, a GPU buffer, a compiled program, a descriptor heap,
or another process does not compress the representation. A factorized
representation must execute in factorized form; expanding it back to the
original patches or operator entries fails the challenge. Matrix-free state
must scale with compact visibility descriptors, physical model parameters,
basis coefficients, or bounded scratch, not summed materialized convolution
area. Mandatory dense products justify final dense computation and writes, not
dense authoritative iterative state or simultaneous product residency.

Before quotienting a physical term from a persisted operator, prove that the
term remains exactly recoverable after support truncation, cropping,
oversampling, discretization, and normalization. An inverse quotient that
requires unavailable pre-truncation state is not an implementation plan or
factorization result. In that case, factor the executable response itself or
obtain the missing pre-truncation representation. A rank result is actionable
only when the same compact factors drive the proposed runtime action and the
discriminator charges their construction.

When the winning hypothesis crosses a persisted-operator boundary, put an
availability gate before the rank or speed experiment. Inspect the operator
generator and prove that it can expose or reevaluate the required
pre-transform physical state. Inverse-transforming a cropped or tapered kernel
does not recover a pre-crop image-domain screen; it produces a different
window-convolved object. Kill or reroute the candidate at that gate instead of
building an approximation whose semantic source is already wrong.

Factor reconstruction and mandatory product formation separately when their
operators differ. A low-rank forward DDE screen does not imply that blindly
forming every pair of basis cross-products is the best normal, PSF, or weight
operator. Measure the rank of the required normal-screen family directly.
Likewise, report reconstruction-domain weighting over the complete active
model support separately from product-domain weighting over the scientifically
valid PB or weight domain. Do not average one passing domain with one failing
domain.

## Operator-invocation elimination

Before optimizing an expensive forward or adjoint application, count every
invocation in the real reconstruction trajectory and explain why each one
exists. Search first for exact fusion, persistent sufficient state, lazy
evaluation, incremental updates, and reconstruction changes that eliminate
whole applications. Compare total transformed work:

```text
old_calls * operator_cost
versus
new_calls * operator_cost
  + incremental_update_cost
  + persistent_state_traffic
  + changed_schedule_cost
```

Removed-call count and interaction count are projections, not performance
results. A call-elimination candidate must run its real update path through a
workload-bound break-even gate. Record unique response-key count, coefficient
or tap values visited, plan references, weighted reuse, batching width, update
traffic, resident state, and end-to-end wall time. Reject a factorization that
replaces a few full applications with repeated scans over a large operator
payload unless measured batching or reuse crosses the gate.

Classify exact substitution of one operator application separately from an
algorithmic removal of residual refreshes, synchronization points, or
major-cycle boundaries. The former may be an exact discretized
transformation; the latter changes the reconstruction schedule and must pass
the frozen scientific floor. A scale-zero delta discriminator does not promote
a multiscale path unless the nonzero-scale response is also exact or its
approximation is explicitly accepted.

Before factorizing an incumbent operator, test whether the scientific floor
requires the incumbent number of exact operator invocations. When intermediate
major-cycle refreshes are material, run one bounded schedule discriminator
with the required exact initial products, the proposed cheap or local
reconstruction state, and exactly one required final exact residual. Stop on
divergence, conspicuous artifacts, or a science-floor miss; do not require the
same component trajectory merely because the incumbent used it.

If that schedule fails, measure the correction actually needed by the cheap
solver:

```text
delta_h = h_exact_active_domain - h_minor_or_surrogate
```

High rank in the forward operator does not prove high rank in this correction.
Use randomized operator actions or workload-bound model deltas to measure its
norm, rank, locality, and update cost. Prefer an adaptive refresh trigger
driven by a bounded correction-error estimate over a copied fixed major-cycle
schedule. Charge the exact final residual and every mandatory dense product to
the candidate even when the reconstruction keeps only sparse or local state.

## Bounded architecture tournament

Give every candidate a card containing:

- Classification: exact transformation, tunable discretization, explicit
  approximation, or different reconstruction algorithm.
- Dominant scaling variables and expected full-size scaling.
- Persistent-state and phase-liveness equations.
- Counts of forward, adjoint, FFT, and dense-image passes.
- Physical byte traffic, arithmetic, expensive functions, launches, transfers,
  and synchronization.
- The cheapest discriminator using actual source signatures.
- Required metrics, science checks, pre-code kill condition, abort gate,
  promotion gate, and broader-family falsification.
- Full-size p50 and p90 projections for every required workload.

Any approximation must expose a tunable error control and an exact or
higher-precision diagnostic fallback.

Prefer discriminators that exercise real operator states, support
distribution, mask geometry, pointing distribution, and output moments without
producing every final product. Give a candidate one bounded discriminator and
at most one corrective optimization. Permit the correction only when counters
explain most of the miss and the identified change would cross the promotion
gate. Do not integrate a candidate whose optimistic physical lower bound,
memory liveness, or p90 projection cannot meet the final goal.

## Quantitative selection

For every required workload:

1. Measure mandatory input, output, FFT, zeroing, and dense-image costs.
2. Measure sequential, irregular, scatter, atomic, arithmetic, phase, launch,
   transfer, and synchronization speed of light on the target hardware.
3. Bound each serial phase by the maximum of its bandwidth, arithmetic, and
   expensive-operation limits.
4. Sum serial phase bounds and charge setup, transforms, materialization,
   product derivation, and writes.
5. Compute peak physical liveness by phase, including resident mmap pages, GPU
   allocations, and FFT workspaces.
6. Score a candidate by its worst normalized result across all required
   workloads; do not average away a losing mode.

Reject before implementation when the optimistic bound lacks margin, live
state exceeds the no-pressure memory budget, or complete stage elimination has
an immaterial Amdahl benefit. Rank survivors by expected end-to-end gap closure
per bounded discriminator effort, not implementation convenience.

### Backpropagate the end-to-end target

Translate a requested whole-run speedup into a budget for the candidate before
writing its implementation:

```text
candidate_time_max = current_total / target_speedup - fixed_non_candidate_time
```

Reject the target as impossible when that value is non-positive. When the
candidate changes the physical work count by a factor `rho`, its minimum
required throughput improvement over the incumbent stage is:

```text
required_throughput_multiplier =
  rho * incumbent_candidate_time / candidate_time_max
```

Use the target-hardware p90 for decisions and reserve explicit promotion
margin; merely landing on the mathematical pass/fail boundary is not a
production result. Require:

```text
measured_candidate_time
  + omitted_work_high_bound
  + uncertainty_reserve
  <= promotion_gate
```

The combined omitted-work and uncertainty reserve must be at least 30% of the
candidate budget, and at least 40% when the discriminator omits transforms,
representation construction, scatter, or product work. A fixed percentage is
a floor rather than a substitute for measured omitted-phase bounds. Use a
tighter reserve only when target-hardware measurements provide explicit upper
bounds for every omitted phase. State both the promotion budget and a looser
abort budget that closes the family only when even an optimistic lower bound
misses it.

### Do not price different representations with one counter

Interaction, tap, pair, pixel, FFT, and byte counts are useful rejection
filters only within representations whose physical operations have comparable
cost. A replacement may deliberately increase an arithmetic count while
changing irregular, memory-bound scatter/gather into dense, compute-bound
execution. Conversely, fewer nominal interactions may lose through materialized
state, random traffic, atomics, transforms, launches, or synchronization.

When a representation changes the physical character of the work, run one
matched target-hardware race that includes setup, packing, transforms,
scatter/gather, transfers, command encoding, synchronization, and bounded
state. Backpropagate the whole-run target to obtain the required kernel-family
budget first. A count-only miss may reject on a physical lower bound, but it
must not be presented as a measured runtime comparison across unlike
representations.

Falsify only the action family actually exercised. Failure of a direct
subgridder with `O(sum(n_p * L_p^2))` work does not reject a low-rank,
butterfly, coalesced, averaged, or NUFFT formulation that changes that action
count. Record the invariant that makes a family-level falsification valid.

## Forced portfolio reset

Suspend a representation family when two candidates fail promotion, a
candidate reaches its measured speed of light but still misses the goal, the
next change preserves the dominant scaling variable, the candidate can improve
only a small-Amdahl stage, or its full-size projection worsens with image,
field, direction-dependent state, or support count. Reopen it only when new
measurements invalidate the falsification.

Seek an independent adversarial review for a major reset when one is
available. Challenge the review with the actual source representation,
counters, memory ledger, and negative experiments. Convert surviving ideas
into tournament cards; never adopt an architecture label without a
discriminator.

## Promotion and path retirement

Promote only after the frozen science floor, medium and large end-to-end
scaling, bounded memory without unintended swap, every required workload, and
all setup, transfers, synchronization, derivation, and writes pass. `auto`
must select the winner from workload and hardware characteristics.

After promotion, make the winner the production default and remove losing
production implementations, adapters, duplicate planners, stale environment
controls, and unused artifact formats. Retain only an explicitly justified
serial or high-accuracy reference for tests or unsupported workloads. Do not
retain the old path "just in case."

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
- Renaming, relocating, or compiling the same materialized operator values.
- Expanding a compact or factorized description before each replay.
- Counting precision, scheduling, caching, worker, or GPU variants as
  independent architecture candidates.
- Optimizing one operator application without counting how many applications
  the reconstruction requires.
- Performing full-field work during masked reconstruction without proving it
  affects component selection.
- Leaving a superseded representation in production after its replacement
  promotes.
