# CASA-RS Domain Language

CASA-RS implements radio-astronomy data and imaging workflows while preserving
casacore-compatible persistent data. This glossary names the scientific and
execution concepts used by the imaging architecture.

## Imaging science

**Compiled Problem**:
An immutable logical-science description for one imaging run, bound to an
Observation Snapshot, reference-data identities, reconstruction and coordinate
contracts, required capabilities, numerical policy, and requested products.
_Avoid_: Mode configuration, resolved execution plan

**Compiled Geometry**:
The immutable coordinate frames, image-domain charts, requested output
coordinates, and deterministic transform specifications needed to interpret
observations, models, and products. Sample-sized coordinate arrays are streamed,
not owned by geometry.
_Avoid_: Gridder configuration, sample coordinate cache

**Observation Snapshot**:
The immutable logical identity and consistency generation of selected MS data,
metadata, reference tables, ephemerides, and input models. It is a manifest,
not a materialized copy of bulk samples. Its compiler canonicalizes exact
per-MS row, SPW/channel, and correlation semantics plus selected column and
metadata generations and the independent existence/generation of the optional
`MODEL_DATA` column. Selected rows are retained only as counts and an ordered
row-sequence digest. Content identity ignores source location and request
order; a separate provenance identity preserves them.
_Avoid_: MeasurementSet clone, input path

**Observation Transaction**:
The snapshot-bound, compiler-derived read/write contract for one imaging run.
It names exact per-MS selections and generations, optional `MODEL_DATA` write
preconditions, typed physical observation-read nodes, exact per-required-product
private staging completions, and the sole atomic publication gate. The sole
`plan` entrypoint derives all read completion events and binds this declaration
to the exact Compiled Problem and physical-work identity. The terminal gate
follows every other completion, revalidates while holding every source lock,
ends controller polling on launch, and never exposes staging.
_Avoid_: Incremental output write, best-effort model save

**Selected Observation**:
The bounded stream evaluating an Observation Snapshot and compiled transform
specifications into visibility samples, flags, weights, and per-sample
coordinates. Chunking has no scientific identity.
_Avoid_: Observation snapshot, input rows

**Spectral Coordinate Law**:
The native and output frames, rest frequency, velocity convention,
epoch/direction/ephemeris dependencies, channel boundaries, and output WCS.
_Avoid_: Interpolation kernel, cube mode

**Spectral Sampling Operator**:
The paired channel integration or interpolation, coverage, flag/weight
propagation, edge behavior, and covariance approximation applied to samples.
_Avoid_: Spectral coordinate definition, reconstruction basis

**Reconstruction Basis**:
The model coefficient functions across frequency, including constant, Taylor,
and channel-local bases.
_Avoid_: Spectral mode, cube runner

**Spectral Coupling Policy**:
Whether spectral coefficients are independent, sequentially transformed,
jointly reconstructed, regularized, or coupled through shared products.
_Avoid_: Basis type, task mode

**Measurement Operator**:
The complete logical forward map A from model coefficients to unweighted
selected samples and its declared-inner-product adjoint A*, including paired
sampling and instrument response. The data metric W is a separate contract.
_Avoid_: Gridder, FT machine

**Normal State Generation**:
A versioned semantic generation identifying the observation, model, and
weighting generations; A*W(d-Ax); normal-operator or PSF approximation;
sensitivity and sum weights; valid support; numerics; and provenance. It need
not be one resident image.
_Avoid_: Dirty image bundle, solver scratch

**Minor Cycle View**:
An immutable bounded approximation with an identifier, valid domain,
error/staleness bound, threshold, and maximum admissible model update.
_Avoid_: Mutable residual image, solver workspace

**Major Cycle**:
A complete-data reconciliation that evaluates the current model against every
selected sample with a frozen weighting generation and produces an
authoritative Normal State Generation.
_Avoid_: Outer loop, residual refresh callback

**Minor Cycle**:
A bounded approximate reconstruction step that consumes a frozen normal-state
approximation and returns a model update plus convergence evidence.
_Avoid_: CLEAN loop, inner callback

**Product Contract**:
The required product graph, authoritative source generations, axes, units,
normalization, beam/restoration policy, validity, dependencies, schema, and
atomic publication rules for an imaging problem.
_Avoid_: Output files, sidecars

**Migration Obligation**:
A visible, executable record of a scientific capability that is intentionally
unavailable during architectural migration and must be restored before the
programme completes.
_Avoid_: Ignored test, temporary fallback

## Imaging execution

**Resource Policy**:
The frontend-selected interactive, balanced, exclusive, or explicitly
overridden host-use policy. It describes intent, not detected capacity.
_Avoid_: Backend choice, device inventory

**Resource Authority**:
The process-level owner of resource inventory, multi-run arbitration, planning,
leases, and accounting across CPU, memory, accelerators, storage, transfers,
queues, caches, and every I/O buffer.
_Avoid_: Application planner, mode eligibility

**Resource Lease**:
An epoch-bearing grant with hard ceilings and preferred targets that may
change at declared safe execution boundaries.
_Avoid_: Dynamic configuration, resource hint

**Execution Plan**:
An immutable problem-bound work DAG containing implementation alternatives,
preparation, logical allocations, hard bounds, physical-slot assignments,
quiescence points, and pre-authorized adaptations.
_Avoid_: Resolved mode, backend choice

**Execution Receipt**:
A versioned projection of the effective problem and plan, identities,
predictions and confidence, actual resource and I/O use, adaptations, output
manifest, and final or failed completion outcome.
_Avoid_: Log, metrics blob

**Acceptance Contract**:
The versioned, capability-specific baseline manifests, comparator rules,
thresholds, operator and metamorphic laws, resource bounds, and verification
tiers required for a capability/backend row to transfer or remain supported.
_Avoid_: Generic RMS tolerance, ad hoc test list
