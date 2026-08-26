# ADR-0009: Mathematical imaging architecture

Status: accepted
Date: 2026-08-18
Truth class: normative
Supersedes:
Superseded by:

## Context

Imaging behavior is distributed across application dispatch, mode-specific
preparation, imaging modules, solver controllers, and product writers.
Continuum, spectral-line, mosaic, W/AW-projection, MT-MFS, CPU, and Metal paths
repeat or reinterpret scientific choices. Geometry, measurement-equation terms,
weighting, normalization, products, and migration ownership have no enforceable
dependency boundaries. A new vocabulary alone would permit the same architecture
to reappear behind new names.

## Decision

CASA-RS imaging follows one normative mathematical and ownership architecture.

### Ownership and dependency direction

Application and frontend layers may version-migrate and validate request syntax,
select a Resource Policy, invoke compile/plan/run, and present results. They do
not query devices, evaluate scientific capability/backend eligibility, select
gridders or solvers, construct science caches, allocate imaging work buffers, or
define or write scientific products.

Scientific definitions depend on no frontend, execution backend, or device API.
Observation and MeasurementSet interoperability are isolated behind observation
interfaces. Reconstruction depends on Measurement Operator and Normal State
contracts rather than casacore or concrete backends. Product semantics depend on
scientific state rather than solvers or execution implementations. Backends
implement declarative work-node and operator interfaces and do not own modes,
coordinates, weighting, normalization, or products.

There is one production engine behind the application-owned typed
installed-implementation availability boundary. `casa-imaging-application`
checks compiled and task-only requirements against the implementation installed
in the build before planning. Capabilities whose owners have not landed are
returned typed unavailable rather than delegated to an alternate
implementation. The repository migration matrix is planning and acceptance
evidence only; production neither embeds nor interprets it. CI enforces the
permitted crate/module graph and rejects forbidden edges.

### Compile, plan, and run boundary

`compile(request)` produces an immutable Compiled Problem containing logical
science, an Observation Snapshot manifest, reference-data identities, required
capabilities, coordinate and reconstruction contracts, measurement-equation and
weighting contracts, a Numerics Contract, and a Product Contract. It contains no
backend-specific prepared operator or resource allocation.

`plan(problem, resource_policy)` produces an immutable Execution Plan bound to
the exact problem and input identities. ADR-0010 defines its physical work and
resource contract.

`run(problem, plan)` validates the bindings and executes only the plan. It does
not silently recompile, perform capability routing, choose an unlisted backend,
or fall back to another implementation.

### Measurement equation and weighting

Let X be the declared model-coefficient space and D the selected unweighted
visibility-sample space. The complete logical measurement operator is A: X -> D.
A* is its adjoint under explicitly declared inner products. W is the
positive-semidefinite data metric containing flags, input weights, tapering, and
globally derived uniform or Briggs density weights.

The authoritative normal right-hand side is b = A* W d. For model x, the
authoritative normal residual is g(x) = A* W (d - A x), and the logical normal
operator is H = A* W A.

Any phase transformation, spectral sampling, polarization or Mueller mapping,
channel integration, or direction-dependent response that affects both
prediction and imaging belongs to a paired operator or explicitly paired
composition. Data-only interpolation may not bypass the adjoint contract.

Published normalization, PB correction, sensitivity division, residual scaling,
restoring-beam convolution, blanking, and unit conversion belong to the Product
Contract and its normalization rules. They are not silently folded into a
backend adjoint.

### Geometry and polarization

Compiled Geometry owns coordinate frames, image-domain charts, requested output
coordinates, user-visible facet and outlier definitions, and deterministic
transform specifications. It does not own sample-sized UVW, frequency, phase, or
pointing arrays. Selected Observation streams evaluate per-sample coordinates
from the compiled specifications.

Facet batching, tiling, halo sizing, and device partitioning are execution.
W-, AW-, and facet convolution kernels are Measurement Operator
implementations. Requested polarization coordinates belong to reconstruction;
feed/correlation basis, parallactic-angle behavior, and Mueller response belong
to the measurement equation and instrument response.

### Spectral semantics

Continuum and line share reconstruction, Measurement Operator, Normal State,
product, and execution interfaces. Four distinct contracts define spectral
behavior:

- Spectral Coordinate Law defines native/output frames, rest frequency,
  velocity convention, epoch/direction/ephemeris dependencies, channel
  boundaries, and output WCS.
- Spectral Sampling Operator defines paired channel integration or
  interpolation, coverage, flag/weight propagation, edge behavior, and the
  declared treatment of induced inter-channel covariance.
- Reconstruction Basis defines constant, Taylor, channel-local, or other model
  coefficient functions.
- Spectral Coupling Policy defines whether channels or basis blocks are
  independent, sequentially transformed, jointly reconstructed, regularized,
  or coupled through shared products such as a common restoring beam.

A cube uses a channel-local Reconstruction Basis but is not defined by that
basis and is not an application loop around a continuum imager. Sequential
continuum fitting/subtraction is an explicit visibility transform with fit
channels, weights, model, provenance, and output-weight semantics. Joint
continuum-plus-line reconstruction is a different capability requiring a block
normal operator with continuum-line cross terms and a separately accepted
scientific decision.

### Observation snapshot and side effects

Selected Observation is evaluated against an immutable logical Observation
Snapshot. The snapshot records source-table and selected-column consistency
identities, exact row/channel/correlation selection semantics, relevant metadata
generations, reference-data and ephemeris identities, and input-model identities.
Bulk samples remain bounded streams; snapshot does not mean materialization.

Read and write sets are explicit. Execution detects disallowed mutation of
selected data, flags, weights, or metadata before consuming mixed generations.
Scientific products and optional model-column writes use staging and commit
atomically only after final complete-data reconciliation and all required Product
Contract nodes succeed. Cancellation, input mutation, numerical failure,
resource failure, or output failure leaves neither partially published products
nor a partially committed model column.

### Major and Minor Cycles

A Major Cycle performs complete-data reconciliation over every selected sample
using the logical operator and one frozen weighting generation. Complete-data
excludes Minor Cycle approximations but does not imply bitwise identity, full
materialization, one resident grid, or a shift-invariant PSF.

A Normal State Generation identifies the Observation Snapshot, model generation,
weighting generation, normal right-hand side, normal-operator or PSF
approximation, sensitivity and sum-weight state, valid support, Numerics
Contract, and provenance. It may be partitioned, streamed, tiled, sparse,
manifest-based, or operator-backed.

A Minor Cycle consumes an immutable Minor Cycle View recording the approximation
identifier, valid domain, error or staleness bound, cycle threshold, and maximum
admissible update. It returns a Model Delta and Solver Evidence and stops before
violating that envelope. A final Major Cycle is mandatory before restoration and
publication.

### Products

A Product Contract and Product Graph own required/optional products,
authoritative source generations, WCS and axes, units and normalization,
per-plane/common-beam policy, residual scaling and restoration, PB/sensitivity
correction, blanking and validity, cross-product dependencies, output schema,
and atomic publication. Backends may provide primitive transforms but may not
define the meaning of image, residual, PSF, alpha, beam, mask, sensitivity, or
PB-corrected products.

### Migration

The repository migration matrix classifies each request capability as Native or
TemporarilyUnavailable for planning and acceptance. It is enforced by CI but is
not runtime policy: production code does not embed or interpret it, and requests
and execution receipts do not carry its rows or disposition. The application
checks the compiled problem and task-only requirements once against typed
installed-implementation availability before planning. An unavailable request
invokes no engine. A compile, availability, plan, or execution failure is
terminal, and execution never delegates a stage to an alternate implementation.

Migration Obligations are repository-enforced records. Each records the
capability key, current owner, reason, authoritative issue/evidence, Acceptance
Contract, destination ticket, transfer milestone, and deletion condition. A
capability transfers only when its native Acceptance Contract passes and the
same merge makes the displaced route unreachable. Useful algorithms move
directly to their authoritative owner; otherwise the displaced implementation
is deleted. A test-only differential harness may read frozen evidence but is
not production routing.

Before transfer, the application boundary returns typed unavailable for the
capability; implementation work lands only in its accepted destination owner.
After transfer it lands only in the native owner. Dual production ownership and
dual patching are prohibited.

## Consequences

Positive:
- code and verification follow the normal-equation and cycle mathematics
- geometry, spectral sampling, reconstruction, normalization, and execution
  have distinct enforceable owners
- continuum and line share infrastructure without claiming false semantic
  equivalence
- products and MeasurementSet side effects become atomic and generation-safe
- migrations have a same-merge ownership and deletion ratchet

Negative:
- capability may contract temporarily during migration
- broad internal APIs and dependency edges will change
- observation consistency and output transactions require new infrastructure
- every solver and backend must satisfy explicit science and product contracts

Neutral / tradeoffs:
- exact CLEAN component order and bitwise pixels are not preserved when the
  versioned Acceptance Contract remains satisfied
- the two proposed architecture ADRs contain several tightly coupled normative
  sections rather than creating an ADR for each vocabulary term

## Alternatives considered

1. Preserve mode-oriented runners and share utilities.
2. Translate LibRA/CASA C++ class and inheritance families directly.
3. Build one broad imaging-run object owning science, resources, solvers, and products.
4. Treat continuum, line, mosaic, and projections as independent top-level modes.
5. Let a channel-local basis alone define spectral-line semantics.
6. Allow capability transfer without deleting the displaced route.

## Enforcement

This decision is enforced by:
- tests: weighted-adjoint and linearity laws, spectral identity/nonidentity
  cases, cycle invariants, mutation/cancellation rollback, product-generation
  consistency, differential evidence, and versioned Acceptance Contracts
- lint/import/dependency rules: CI rejects forbidden frontend, backend,
  observation, reconstruction, and product dependencies
- CI checks: the migration matrix and obligations remain executable; transferred
  routes are unreachable; products/model-column writes are atomic
- review trigger: stop before adding a second public run interface, an alternate
  stage path, backend-owned science/product semantics, or unpaired sampling
- none / guidance only:

## Drift detection

Suspect drift if:
- a frontend or backend evaluates a science mode or writes a scientific product
- Geometry owns sample-sized arrays or execution tiling
- spectral interpolation is not represented in prediction and adjoint paths
- a Minor Cycle mutates authoritative residual state or exceeds its view envelope
- a capability can silently retry through another implementation
- a solver or backend defines normalization, restoration, or output semantics
