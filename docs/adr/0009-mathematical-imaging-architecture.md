# ADR-0009: Mathematical imaging architecture

Status: accepted
Date: 2026-08-18
Truth class: normative
Supersedes:
Superseded by: ADR-0016, in part (the migration rules and the attestation
and identity wording, since removed from this record)

## Context

Imaging behavior was distributed across application dispatch, mode-specific
preparation, imaging modules, solver controllers, and product writers.
Continuum, spectral-line, mosaic, W/AW-projection, MT-MFS, CPU, and Metal paths
repeated or reinterpreted scientific choices. Geometry, measurement-equation
terms, weighting, normalization and products had no enforceable dependency
boundaries. A new vocabulary alone would permit the same architecture to
reappear behind new names.

## Decision

CASA-RS imaging follows one normative mathematical and ownership architecture.

### Ownership and dependency direction

Application and frontend layers may version-migrate and validate request syntax,
select a Resource Policy, invoke compile and run, and present results. They do
not query devices, evaluate scientific capability/backend eligibility, select
gridders or solvers, construct science caches, allocate imaging work buffers, or
define or write scientific products.

Scientific definitions depend on no frontend, execution backend, or device API.
Observation and MeasurementSet interoperability are isolated behind observation
interfaces. Reconstruction depends on Measurement Operator and Normal State
contracts rather than casacore or concrete backends. Product semantics depend on
scientific state rather than solvers or execution implementations. Backends
implement declarative work and operator interfaces and do not own modes,
coordinates, weighting, normalization, or products.

There is one production engine behind the application-owned typed
installed-implementation availability boundary. `casa-imaging-application`
checks compiled and task-only requirements against the implementation installed
in the build before any work. Capabilities whose owners have not landed are
returned typed unavailable rather than delegated to an alternate
implementation. CI enforces the permitted crate graph and rejects forbidden
edges (ADR-0016).

### Compile, plan, and run boundary

`compile(request)` produces an immutable Compiled Problem containing logical
science, an Observation Snapshot (a manifest of the selection; bulk samples
remain bounded streams), required capabilities, coordinate and
reconstruction contracts, measurement-equation and weighting contracts, a
Numerics Contract, and a Product Contract. It contains no backend-specific
prepared operator or resource allocation.

Planning binds the compiled problem to a backend and a Resource Policy; each
phase is admitted against the host before it runs (ADR-0016 decision 4).

`run` executes only what was compiled and admitted. It does not silently
recompile, perform capability routing, choose an unlisted backend, or fall
back to another implementation.

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
channels, weights, model, provenance, and output-weight semantics (ADR-0011).
Joint continuum-plus-line reconstruction is a different capability requiring a
block normal operator with continuum-line cross terms and a separately accepted
scientific decision.

### Side effects

Read and write sets are explicit. Scientific products publish after the
final complete-data reconciliation. Optional `MODEL_DATA` follows ADR-0008:
the final pass predicts from the final model and writes the selected cells
in place under casacore's table lock. Interruption may leave partial derived
values, as in CASA; the next run recomputes them. Imaging adds no staging
column, backup generation, content digest, or rollback mechanism beyond
CASA-compatible table behavior.

### Major and Minor Cycles

A Major Cycle performs complete-data reconciliation over every selected sample
using the logical operator and one frozen weighting generation. Complete-data
excludes Minor Cycle approximations but does not imply bitwise identity, full
materialization, one resident grid, or a shift-invariant PSF.

A Normal State Generation comprises the model generation, weighting
generation, normal right-hand side, normal-operator or PSF approximation,
sensitivity and sum-weight state, valid support, and Numerics Contract. It may
be partitioned, streamed, tiled, sparse, or operator-backed.

A Minor Cycle consumes an immutable Minor Cycle View recording the approximation,
valid domain, error or staleness bound, cycle threshold, and maximum
admissible update. It returns a Model Delta and Solver Evidence and stops before
violating that envelope. A final Major Cycle is mandatory before restoration and
publication.

### Products

A Product Contract and Product Graph own required/optional products,
authoritative source generations, WCS and axes, units and normalization,
per-plane/common-beam policy, residual scaling and restoration, PB/sensitivity
correction, blanking and validity, cross-product dependencies, output schema,
and the atomic replacement of each product. Backends may provide primitive
transforms but may not define the meaning of image, residual, PSF, alpha, beam,
mask, sensitivity, or PB-corrected products. Generation needs no content
hashing or full-array reread to authorize publication (ADR-0014).

## Consequences

Positive:
- code and verification follow the normal-equation and cycle mathematics
- geometry, spectral sampling, reconstruction, normalization, and execution
  have distinct enforceable owners
- continuum and line share infrastructure without claiming false semantic
  equivalence
- each product is replaced atomically; MeasurementSet side effects follow CASA
  (ADR-0008)

Negative:
- every solver and backend must satisfy explicit science and product contracts

Neutral / tradeoffs:
- exact CLEAN component order and bitwise pixels are not preserved; agreement
  is judged by normalized tolerance

## Alternatives considered

1. Preserve mode-oriented runners and share utilities.
2. Translate LibRA/CASA C++ class and inheritance families directly.
3. Build one broad imaging-run object owning science, resources, solvers, and products.
4. Treat continuum, line, mosaic, and projections as independent top-level modes.
5. Let a channel-local basis alone define spectral-line semantics.

## Enforcement

This decision is enforced by:
- tests: the T0 laws (weighted adjoint, linearity, spectral identity and
  nonidentity, cycle invariants) and T1 end-to-end tests of `TESTING.md`
  "Imaging tiers and evidence"
- lint/import/dependency rules: `scripts/check-imaging-dependencies.py`
  rejects forbidden frontend, backend, observation, reconstruction, and
  product dependencies (ADR-0016)
- CI checks: `just arch-check`
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
