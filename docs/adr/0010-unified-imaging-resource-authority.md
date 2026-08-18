# ADR-0010: Unified imaging resource authority

Status: accepted
Date: 2026-08-18
Truth class: normative
Supersedes:
Superseded by:

## Context

Imaging performance is constrained jointly by processor capacity, memory,
accelerators, storage, transfers, queues, caches, synchronization, and I/O
buffers. Current authority is fragmented across planners, application
eligibility ladders, spectral-slab logic, backend selection, worker counts,
caches, and environment-sensitive policy. A lifetime ledger alone cannot make
resource claims true without a work graph, scheduler, fences, cancellation, and
attribution.

## Decision

One process-level Resource Authority inventories and arbitrates imaging
resources across every frontend and concurrent run. Applications provide a
Resource Policy but do not inspect devices or choose implementations.

### Policy and topology

Resource Policy expresses interactive, balanced, exclusive, or explicit
override intent. Resource topology distinguishes physical capacity domains from
views of those domains, including unified host/device memory, so one capacity is
not double charged.

The topology includes:
- CPU classes and cores, host/NUMA/unified/device memory, accelerators, and
  transfer links
- capacity resources, shared rates, device occupancy, transfer engines,
  storage bandwidth/IOPS, and queue capacity
- persistent caches, storage reservations, synchronization, file descriptors,
  table locks, mapped/page-cache exposure, and every I/O buffer
- external host pressure and multi-run reservations

The initial runtime implements local CPU and one Metal device. Describing other
topology does not claim an implementation.

### Plan-bound execution DAG

An Execution Plan is bound to the exact Compiled Problem ID, Observation and
reference-data snapshots, implementation-registry version, Numerics Contract,
Resource Policy version, and Planner Cost Model Profile version. It contains the
complete work DAG, selected implementation alternatives, preparation nodes,
logical allocations, hard bounds, physical-slot assignments or assignment
rules, quiescence points, and pre-authorized adaptations.

Data censuses, prepared-artifact generation/loading, cache lookup, convolution
function work, FFT planning, JIT work, serialization, spill, prefetch, and
publication are explicit receipted work nodes. Run may not prepare, route,
replan, or perform unlisted work.

The scheduler owns dependency execution, synchronization, cancellation, and
fence completion. It may execute a plan lazily or concurrently but may not alter
scientific, product, or numerical semantics.

### Demand declarations and lifetimes

Every implementation alternative declares:
- a science and numerics capability predicate
- hard and preferred resident-memory demand
- transient and worker-private scratch
- thread-stack and allocator-fragmentation envelopes
- external-library, FFT, driver, JIT, and command-buffer envelopes
- source read-ahead, decoding, preparation, transfer, spill, serialization,
  writeback, mapped/page-cache exposure, queue depth, and table locks
- temporary, staged-output, final-output, and persistent-cache storage
- transfer, synchronization, preparation, recomputation, spill, and prefetch
  costs
- scaling formulas and declared quiescence points

Every allocation, worker, device queue, transfer, cache reservation, and I/O
buffer is attributable to an epoch-bearing Resource Lease. Logical liveness
extends through device, I/O, writeback, and publication fences. Physical-slot
reuse is legal only after fence completion and compatibility checks for
location, alignment, storage mode, layout, initialization, and access.

Plans explicitly cover cross-stage lifetimes such as global weighting density
and sum weights, current model and pending delta, MT-MFS cross terms, mosaic
sensitivity/PB accumulators, common-beam metadata, CF artifacts shared among
PSF/dirty/Major Cycles, double-buffered streams, and staged outputs awaiting
commit.

### Prepared artifacts and caches

Prepared convolution functions, spectral mappings, kernels, and other expensive
immutable artifacts use versioned content identities, integrity validation,
explicit cache budgets, and deterministic eviction. Hits, misses, generation,
loading, residency, and stale-artifact rejection appear in plans and receipts.
Compiled Rust representations and raw backend pointers are not stable persisted
contracts.

### Planning objective and pressure

Planning is lexicographic:

1. preserve science, capability, product, and Numerics Contracts;
2. prove hard capacity feasibility with reserved headroom;
3. satisfy the selected host-use policy;
4. minimize a conservative predicted wall-time objective including uncertainty.

The pressure order is physical-slot reuse, recomputation when preferable,
managed spill/prefetch, then operating-system compression or swap only when the
user explicitly authorizes normal OS behavior. OS compression or swap is never
counted as planned capacity.

A legal plan exposes prediction confidence, dominant uncertainty terms, and a
machine-readable infeasibility certificate when no alternative fits. Failed and
aborted executions are receipted and may constrain feasibility even when they
are not promoted into the performance cost model.

### Leases and adaptation

Resource Leases have epochs, hard ceilings, and preferred targets. An Execution
Plan may adapt only through listed transitions at declared quiescence points,
including bounded batch, tile, slab, stage, or Major-Cycle boundaries.

An authorized transition may change workers, batches, tiles, slabs, I/O depth,
cache retention, fusion, recomputation, spill, or prefetch. It may not change
selection, weighting, spectral mappings, Measurement Operator semantics,
Product Contract, or Numerics Contract. If no listed alternative is feasible,
execution fails atomically rather than improvising or falling back.

### Receipts and cost-model profiles

An Execution Receipt persists versioned projections of the effective Compiled
Problem and complete Execution Plan; all input, artifact, implementation, and
build identities; Resource Policy; predictions, confidence, and uncertainty;
actual time, residency, allocation, queue, transfer, I/O, storage, and cache
measurements; adaptations; output manifest; and final or failure status.

Receipts have bounded local retention and redacted paths and are never uploaded
automatically. Planner behavior changes only through an explicit command that
promotes a versioned Planner Cost Model Profile from reviewed comparable
receipts. Successful runs do not silently train future plans.

## Consequences

Positive:
- every optimization competes for the same physical and rate resources
- I/O, caches, staging, library workspaces, and asynchronous liveness cannot
  disappear from accounting
- memory and buffers can be reused safely across disjoint processing segments
- execution choices share one work graph and conservative cost model
- receipts reveal prediction errors, infeasible regions, and planner bypasses

Negative:
- implementations must declare bounded demands, alternatives, and quiescence
  points
- scheduling, cancellation, fences, artifact identity, and receipt projection
  are first-class subsystems
- precise capacity proof requires headroom models for external libraries and
  allocator/runtime overhead

Neutral / tradeoffs:
- raw wall time is advisory during ordinary architecture migrations, while hard
  resource bounds, asymptotic boundedness, plan/lease conformance, global
  weighting, and existing programme commitments remain hard gates
- durable Major-Cycle restart is desirable but is not required for the first
  resource implementation

## Alternatives considered

1. Keep independent per-mode and per-backend planners.
2. Let applications or implementations inspect hosts and choose resources.
3. Treat a memory limit and worker count as the whole resource model.
4. Maintain allocation lifetimes without a plan-owned work DAG and fences.
5. Rely on operating-system swap as elastic capacity.
6. Continuously self-tune from every successful run.

## Enforcement

This decision is enforced by:
- tests: plan binding, infeasibility evidence, demand upper bounds, allocation
  lifetimes, physical-slot reuse after fences, lease adaptation, cancellation,
  artifact identity, receipt projection, and Planner Cost Model Profile promotion
- lint/import/dependency rules: frontends and imaging implementations do not
  detect capacity, schedule unlisted work, or own resource policy
- CI checks: reject hidden buffers, unbounded materialization, duplicate full
  grids, missing staging reservations, invalid plans, planner bypasses, and
  unreceipted adaptations
- review trigger: stop before adding another resource authority, undeclared
  buffer/cache, hidden preparation path, online self-tuning, or planned swap
- none / guidance only:

## Drift detection

Suspect drift if:
- a mode/backend owns worker, cache, slab, tile, spill, or device eligibility
- application code inventories capacity or chooses implementations
- an allocation, queue, transfer, I/O buffer, or prepared artifact is absent
  from both plan and receipt
- unified memory is double charged or a worker duplicates a full grid
- physical storage is reused before asynchronous or publication fences complete
- execution exceeds a lease or changes schedule without a listed transition
