# CASA-RS Domain Language

CASA-RS implements radio-astronomy data and imaging workflows while preserving
casacore-compatible persistent data. This glossary names the scientific and
execution concepts used by the imaging architecture.

## Imaging science

**Imaging Request**:
The one typed request for an imaging run, deserialized from the imager
parameters the provider catalog resolves and validated once. Every frontend
reaches it through the same catalog resolution; compiling it against its
MeasurementSet yields the Compiled Problem.
_Avoid_: CLI configuration, task request mirror

**Compiled Problem**:
An immutable logical-science description for one imaging run, bound to an
Observation Snapshot, reconstruction and coordinate contracts, required
capabilities, numerical policy, and requested products.
_Avoid_: Mode configuration, resolved execution plan

**Compiled Geometry**:
The immutable coordinate frames, image-domain charts, requested output
coordinates, and deterministic transform specifications needed to interpret
observations, models, and products. Sample-sized coordinate arrays are streamed,
not owned by geometry.
_Avoid_: Gridder configuration, sample coordinate cache

**Observation Snapshot**:
The immutable logical description of selected MS data. It is a manifest, not
a materialized copy of bulk samples. Its compiler canonicalizes exact per-MS
row, SPW/channel, and correlation semantics plus the selected data, flag, and
weight columns and whether MAIN has `CORRECTED_DATA`. Selected rows are
retained only as counts and the DATA_DESCRIPTION identifiers they use.
Sources keep request order and their locations. Nothing of it is stored in
the MeasurementSet, and nothing of it is hashed.
_Avoid_: MeasurementSet clone, input path

**Observation Transaction**:
The compiler-derived MeasurementSet read and write sets of one imaging run. The
read set names, for each snapshot source, its exact selection and the data,
flag and weight columns read. The write set names the selected cells of an
optional in-place `MODEL_DATA` write (the column is created when MAIN lacks
it) and, under a Sequential Continuum Transform, the output-role cells of an
optional `CORRECTED_DATA` write; compile refuses the latter when MAIN has no
such column. Writes run under casacore's table lock, as in CASA; an interrupted
write may leave partial derived values that the next run recomputes. Image
products are not part of it: each is staged and replaced on its own.
_Avoid_: Incremental output write, best-effort model save

**Selected Observation**:
The bounded stream evaluating an Observation Snapshot and compiled transform
specifications into visibility samples, flags, weights, and per-row
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

**Sequential Continuum Transform**:
A visibility-domain continuum fit and subtraction completed before an
independent channel-local line reconstruction. Its fit coefficients describe
the transform and are not a continuum image model.
_Avoid_: Image-domain subtraction

**Chart**:
One pass domain: an image domain, or one facet of a faceted image domain,
with its own operator about its own centre. The pass grids and predicts
charts; the normal state, the minor cycle and the products see image domains,
stitched from their charts.
_Avoid_: Mapper, facet store

**Measurement Operator**:
The complete logical forward map A from model coefficients to unweighted
selected samples and its declared-inner-product adjoint A*, including paired
sampling and instrument response. The data metric W is a separate contract.
_Avoid_: Gridder, FT machine

**Normal State Generation**:
The normal state one Major Cycle forms with its final model: A*W(d-Ax), the
normal-operator or PSF approximation, sensitivity and sum weights, and channel
validity, with the counts of the samples and blocks that formed it. It carries
no identity and is released together with the model that formed it. It need
not be one resident image.
_Avoid_: Dirty image bundle, solver scratch

**Minor Cycle View**:
What one minor cycle of one plane works on, read from the Normal State
Generation: the residual terms in Jy/beam, the PSF terms normalized to a
term-0 peak of 1, a summary of that PSF, and the support where components may
be placed. It is immutable; the cycle's threshold, iteration budget and gain
come from the cycle controls.
_Avoid_: Mutable residual image, solver workspace

**Major Cycle**:
A complete-data pass that evaluates the current model against every selected
sample under the run's imaging weights, formed once before the first pass, and
forms a Normal State Generation. A `MajorCycle` owns its final model and that
normal state and releases them together as one `MajorCycleCompletion`, which
the next major cycle consumes. Neither carries an identity.
_Avoid_: Outer loop, residual refresh callback

**Minor Cycle**:
A bounded approximate reconstruction step that consumes a frozen normal-state
approximation and returns a model update plus convergence evidence.
_Avoid_: CLEAN loop, inner callback

**Product Contract**:
The compiler-owned product list of an imaging problem: the normalization
boundary from unnormalized normal state; one entry per product with its role,
axes and coordinates, unit, normalization, beam rule, validity rule, and
stored mask, unit label and beam attachment; and the published members in
order. Each member is staged and replaced on its own, so a member already
replaced stays valid if a later one fails.
_Avoid_: Output files, sidecars

**Migration Obligation**:
A visible, executable record of a scientific capability that is intentionally
unavailable during architectural migration and must be restored before the
programme completes.
_Avoid_: Ignored test, temporary fallback

**Current-Only Profile Contract**:
A sparse human parameter contract that accepts exactly the current version and
canonical names, adopting current defaults for omitted optional values without
aliases, migrations, or fallback interpretation.
_Avoid_: Compatibility profile, historical run replay

## Observation access

The vocabulary of the MSv2/MSv4 observation boundary
(`crates/casa-observation`, `docs/measurement-set/trial/revision-2-design.md`).

**Partition**:
One store unit of an opened observation, described by its content: spectral
window, correlation products, intents, observing session and feed pair. An
MSv4 dataset is one; the MSv2 adapter divides MAIN rows the same way XRADIO's
converter does, so the same observation opens as the same partition set from
either store. A batch never spans two.
_Avoid_: Data description, SPW index, dataset path

**Sample**:
One (time, baseline) point of a partition's time × baseline grid: an MSv4
`[time, baseline]` cell, or an MSv2 row placed on that grid. Payload arrays are
`[sample, channel, correlation]`, sample `t × B + b`.
_Avoid_: Row, visibility (which is one value of a sample)

**Padding**:
A grid cell no acquisition fills (an antenna that dropped out, a baseline not
written): delivered as flag true, weight zero, NaN values and auxiliaries. The
validity rule excludes it; nothing is read from disk for it.
_Avoid_: Missing row, absent slot, occupancy

**Batch Shape**:
What one batch covers: the partition set, the channel window, the roles read,
the correlation products delivered, and the number of whole times the byte
budget admits. The estimate costs a batch shape before anything is read.
_Avoid_: Cursor shape, chunk size, block size

**Traversal Order**:
The order batches arrive. Within one cursor it is fixed: whole times of one
channel window of one partition, in time order. Across the observation it is
the consumer's, expressed by which cursors it binds and runs, and when.
_Avoid_: Iteration order, row order

**Payload Order**:
The element order inside one batch, `[sample, channel, correlation]`,
correlation fastest, as both stores deliver it without a transpose. A
channel-major view of the same memory is free; contiguity per plane is not.
_Avoid_: Layout, memory order

## Imaging execution

**Resource Policy**:
The frontend-selected interactive, balanced, exclusive, or explicit host-use
policy. It describes intent, not detected capacity.
_Avoid_: Backend choice, device inventory

**Host Resources**:
The host's threads, performance cores, free memory and Metal device, detected
once per process.
_Avoid_: Resource inventory, topology

**Core Allocation**:
The workers a Resource Policy gives a run's phases on its host: the compute
workers of the minor cycles and products, and the streaming workers of the
passes, which fit beside the source's producer thread within the policy's
core budget.
_Avoid_: Affinity, thread pinning

**Admission**:
The check that one phase's memory, as its owners report it, fits what the
Resource Policy leaves free beside the live reservations; an admitted phase
holds a Reservation until it ends, and a refused phase does not start.
_Avoid_: Planning, lease

**Run Summary**:
The one record a run leaves beside its products, whether it completes or
fails: the request echo, each phase with its wall time and peak resident
memory, the worker count, the backend, the minor-cycle totals, the product
list and a failed run's error (plan section 8.3). It is O(1) per phase.
_Avoid_: Execution receipt, log, metrics blob

**Acceptance Contract**:
The versioned, capability-specific baseline manifests, comparator rules,
thresholds, operator and metamorphic laws, resource bounds, and verification
tiers required for a capability/backend row to transfer or remain supported.
_Avoid_: Generic RMS tolerance, ad hoc test list
