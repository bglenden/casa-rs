# Intermediate observation contracts

Status: revision 6; authorized as the M0/M1 trial contract, not frozen
Truth class: design proposal
Date: 2026-10-25

This is the interface between persistent observations and numerical consumers.
The names and Rust sketches are provisional; the semantics below are the design
authorized for the [M0/M1 trial](trial-handoff.md). Trial authorization does not
finalize the design. M1 must turn the sketches into a minimal compilable API
with worked imaging, solve, update, and source-free creation examples before
broad implementation starts.

**Revision 6** folds in what the trial settled with the owner: the
decisions D1 to D25 of [the trial's design record](trial/revision-2-design.md)
and the refinements of its revision 3 ([trial README](trial/README.md),
"Revision 3"). The governing principle is that MSv4 is the future: the
interface takes MSv4's native shape and the MSv2 adapter assembles MSv2 data
into it. Passages that the decisions changed are marked "Revision 6" below;
the trial's `casa-observation` crate is the executable form of them. Where
this document and the decisions still disagree, the decisions are what the
trial implements and what review should hold the code to.

The bounded intermediate is provisionally named `VisibilityBatch`. Reserve
chunk/tile for physical storage and compute window for a kernel's view. The name
does not require reading visibility payloads for a flags-only request.

## 1. Contract rules

| Rule | Required behavior |
| --- | --- |
| C1: source independence | No physical row IDs, Zarr keys, storage managers, or MS-version branches in numerical processing loops. |
| C2: faithful values | Coordinates and references are native casa-rs Measures types/views, directly accepted by its conversion APIs. Guaranteed canonical coordinate units or explicitly requested units at binding; explicit frames/precision, native flag and weight semantics, full data-group association, ordered baselines. No silent scientific transform. |
| C3: layout independence | Separate sample topology, memory layout, scientific grouping, and storage chunking. |
| C4: bounded ownership | Every live payload, index, decode buffer, cache, packed copy and queue is admitted; borrowed views cannot outlive/refill their storage. Independent work can retire separately; shared storage stays live until its last use completes. |
| C5: invertible updates | Sample-preserving reorder/selection retains a checked inverse source map; reduction does not. Shared variables have explicit mutation scope. |
| C6: honest guarantees | Unknown acquisition occupancy is representable; unsupported profiles/layouts and non-atomic writes are explicit. |
| C7: bulk execution | Bulk conversion is a requirement across Measures and quanta, including future measure families. Bind numeric kind, layout and invariant context outside scalar loops; use native array views and family-specific kernels. No required per-value object allocation, virtual dispatch, coordinate reconstruction or repeated invariant setup/provider lookup. |

## 2. Topology and identity

An opened observation supplies a catalogue of partitions, spectral coordinates,
correlation setups, antennas/feeds, fields/sources, data groups, and available
metadata. IDs are opaque and scoped to that opened catalogue and partition
namespace. They are not casacore subtable row numbers, globally unique names,
or serialized identities. Backends retain their own mapping. Large pointing,
ephemeris, and time-dependent metadata are queried in bounded windows; opening
does not require a sample-sized metadata snapshot.

The catalogue distinguishes partition-local references from observation-wide
entity handles. `casa-ms` owns an explicit local-to-observation binding for
antennas/feeds, fields/sources, spectral windows and correlation setups; data
groups remain partition-scoped. Shared entities across partitions bind to one
observation handle only when source associations or a declared metadata-matching
policy establish equivalence. Array position and display name alone are not
evidence. Different source contexts remain distinct; ambiguous associations
require an explicit caller mapping or typed rejection. Compare relevant
metadata directly, without global UUIDs, payload hashes or source sidecars.

Calibration owns a separate `CalibrationBinding` between that observation
catalogue and a calibration artifact's antenna/field/SPW namespace. Its writer
creates CASA-compatible subtables and translates observation handles to their
persisted indices; it never serializes session handles. Its reader rebinds those
subtable entities to a newly opened observation using explicit source namespace
associations or a validated, unambiguous metadata/user mapping. Preserve CASA's
existing v2 associations and explicit SPW mapping semantics where those
namespaces are established; do not replace them with a name-only heuristic.
Equivalent spectral grids alone do not prove that two SPWs share a solution.
M1 must solve, persist, close, permute partition/catalogue enumeration, reopen
and apply, including overlapping antenna subsets and ambiguous-name rejection.

Creation uses a `CatalogueBuilder` with the same entity definitions and
validation, owned by the producer until binding and then by the creation
session. Producer handles map to destination entities independently of any
opened source. A creation batch addresses `TargetRegion`s, not `UpdateMapRef`s,
and can transfer recyclable buffers directly to the sink under backpressure.

A batch is homogeneous in spectral coordinate context and correlation setup.
Different shapes/SPWs yield separate batches, even when one scientific group
spans them. A field/phase centre may vary inside a batch, including a mosaic.
The request can select multiple named data groups. Each group retains its own
UVW and field/source context and explicit alias relationships to other groups.

For native MSv4, distinguish a dimension from an auxiliary coordinate:
`VISIBILITY[time, baseline, frequency, polarization]` carries `field_name[time]`.
A sequential mosaic may therefore have fields `[A, A, B, B, A]` along its time
axis without a field axis or any averaging across fields. Optional field-based
partitioning produces separate datasets instead; the common API must preserve
the correct sample-to-field association in either case, including selection,
slicing and reordering. Per-antenna pointing remains a separate association.
This example does not establish a representation for arbitrary simultaneous
beams/phase centres or per-baseline field changes. A native v4 write must prove
that its field dependencies fit the chosen schema mapping/partitioning or reject;
it must not discard them to fit a time-only field coordinate.
In particular, before writing that mapping, validate that all populated
baselines at each destination time agree on the field. Do not copy XRADIO's
maximum-ID reduction as a consistency check; preserve conflicts through valid
partitioning or reject. Distinct acquisitions must not overwrite one another
in the same time/baseline cell. The
[converter evidence](architecture.md#converter-evidence-for-mosaics) distinguishes
observed upstream behavior from the proposed preservation contract.

```rust
// Semantic sketch of the trial's shape (Revision 6); casa-observation has it.
struct VisibilityBatch<'a> {
    catalogue: &'a Arc<Catalogue>, // per-partition constants live there once (D1)
    partition: PartitionId,
    time: EpochView<'a>,              // whole times of the partition's grid (D3)
    baselines: &'a [OrderedBaseline], // the partition's table, from the catalogue
    field: ArrayView1<'a, FieldId>,   // per time
    scan: Option<ArrayView1<'a, ScanId>>,
    timing: SampleTiming<'a>,         // time centroid, interval, exposure
    frequency: FrequencyView<'a>,     // a window of the catalogue's
    correlations: &'a [Correlation],  // the delivered products (D17, D18)
    group: DataGroupView<'a>,         // [sample, channel, correlation] payload, UVW
    fragments: &'a [GroupFragment],   // scientific groups (section 4)
    // Update maps (section 7) are not part of the trial.
}
```

**Revision 6 (D3): one topology.** A batch is whole times of its
partition's time × baseline grid, MSv4's own: `sample = time_index ×
baseline_count + baseline_index`, field and scan per time, timestamps and
baseline pairs held once (O(T+B), not O(T×B)). There is no irregular
topology. The MSv2 adapter places rows on the grid when the
MeasurementSet is opened; a cell no acquisition fills is **padding** (flag
true, weight 0, values, UVW and time auxiliaries NaN), which the validity
rule excludes. Two rows claiming one cell are distinct acquisitions the
partition keys failed to separate: opening refuses them, naming both rows,
rather than discarding or averaging either. Partitions are content keys as
XRADIO's converter forms them (D2, D4), so the same observation opens as
the same partition set from either store.

Keep three different facts separate:

- **Selection:** which logical elements the request includes.
- **Acquisition occupancy:** known present, known absent, or unknown; may be a
  constant or a compact view, without requiring a byte for every element.
  (Revision 6: a batch carries absence only as padding, flagged; the
  estimate reports each partition's padded fraction before reading.)
- **Validity and flags:** numerical validity of requested values and native
  scientific flag state, potentially varying by channel/correlation.

In external MSv4, NaN padding need not retain original row existence; XRADIO's
boolean padding may be `false`. Neither an unflagged value nor a physical Zarr
chunk proves an acquisition occurred. Missing chunks may legally mean fill
values. Preserve unknown occupancy rather than fabricate a measurement or
recover an imaginary MSv2 row number. The computational validity policy must
exclude unusable requested values without confusing that with acquisition
history. A flags-only request does not read visibilities just to infer history.

The backend's addressability of an existing array slot is separate from whether
that slot contains a measured sample. Ordinary updates select existing logical
elements; writing new measurements into holes requires an explicit creation or
fill policy, not accidental expansion of a selection. Converting to a row
format must explicitly resolve unknown occupancy or report the ambiguity.

## 3. Numeric arrays, metadata, and data groups

### Units and measure references

The normal processing interface guarantees canonical coordinate units, so a
kernel never has to inspect a string to discover whether a frequency is in Hz.
Unit-bearing view types or unit-specific accessors enforce the guarantee;
concrete names below are illustrative, not an accepted API.

| Quantity | Default processing representation |
| --- | --- |
| Frequency, channel width and bandwidth | Hz, for example `frequency_hz()` or `FrequencyView<Hertz>` |
| UVW and Cartesian positions/distances | m |
| Angular coordinates | rad, with coordinate representation and reference retained |
| Durations, integration intervals and exposure | s |
| Epochs | A precision-preserving typed epoch with explicit origin/representation and time scale; seconds for duration/offset access, not an unlabelled f64 Unix timestamp |
| Physical velocities, where requested | m/s; frequency-to-velocity mapping still needs an explicit rest frequency and convention |

`AccessRequest` has unit requirements with these defaults. A consumer can
explicitly request another supported, dimensionally compatible unit at bind
time. Success guarantees that unit for the resulting view throughout its bound
scope. A MHz view must not satisfy a Hz-only kernel signature or an accessor
named `frequency_hz()`. Native-unit inspection remains explicitly identified;
it cannot silently weaken the canonical processing contract.

Resolve unit syntax, dimensional compatibility and conversion factors at their
metadata scope. Convert shared coordinate axes once when possible, otherwise
fuse scaling with an already-required fill/decode/pack. Identity conversions do
not require a copy or a multiply-by-one pass. Account additional buffers and
numeric range/rounding under the precision policy; do not promise exact numeric
preservation for every floating-point unit conversion. Return an error for an
unsupported/incompatible conversion, without handing the caller unexpected units.

Canonical coordinates do not mean converting every quantity to SI base units.
Jy is the proposed canonical flux-density unit when that meaning is established;
uncalibrated/instrumental or other physical quantities retain a declared role
and require compatible consumer expectations. A Jy request cannot infer a flux
calibration. Do not rescale all visibility data into W m^-2 Hz^-1 by default:
besides being inconvenient, inverse-variance weights in those units can exceed
f32 range. If a declared amplitude conversion is `V_out = a * V_in`, associated
inverse-variance weights transform as `w_out = w_in / |a|^2`, and a declared
matching noise sigma as `sigma_out = |a| * sigma_in`. Apply these rules only when
the weight/sigma meaning is established; do not infer a relationship between
arbitrary native columns. Unit requests and output plans must remain consistent
across associated roles and aliases, without silently mutating unselected data.

Each measure view associates values with quantity kind, unit, reference/frame,
coordinate representation and declared axis dependencies. References shared by
an axis or batch are stored once; variable references have checked indexed views
or require compatible batch splitting. Slicing/reordering preserves those
associations. There is no single universal frame for a batch and no per-visibility
measure object requirement. Unknown essential metadata is an explicit error for
processing; any schema-defined legacy default must be identified by the profile.

Unit normalization does not perform a reference-frame or time-scale conversion.
Hz in TOPO and Hz in LSRK remain distinct; UTC does not become TAI by expressing
time in seconds. Explicit scientific transforms use casa-rs Measures and its
declared context/provider (epoch, position, direction, leap-second/EOP data as
needed). The owner requires direct Measures types and efficient bulk conversion,
and authorizes substantial Measures refactoring to achieve them. The earlier
zero-workspace-dependency goal for `casa-observation` is superseded by this
requirement; see [proposed ownership](architecture.md#3-proposed-ownership).

Computational normalization leaves persistent units/metadata unchanged. Same-store
patches convert back to the destination's declared units with representability,
scope and precision checks; source-free creation uses its output profile. Reading
or inspecting a dataset does not rewrite its units or introduce rounding into
untouched persistent values.

### Shared Measures types and bulk conversion

**System-wide requirement:** all supported Measures families must offer natural,
efficient bulk operations. LSRK↔TOPO is one non-exhaustive example. Frequency
conversion plus one other family is not sufficient acceptance of this design.
The same contract applies to present and future scalar/vector measures and
compatible quanta conversions, while each retains its own mathematics and
scientific accuracy requirements. This does not assert support for currently
unsupported reference-frame routes.

| Family / illustrative operation | Reusable work and value-dependent work |
| --- | --- |
| Directions: celestial, terrestrial or horizon references | Prepare common epoch/site/model terms; transform direction arrays, retaining required per-direction nonlinear aberration, deflection or refraction work. |
| Epochs and time scales | Reuse route/provider state and operate on split-precision arrays; evaluate varying EOP/relativistic terms and leap-second boundaries at their required scope. A whole array need not share one offset. |
| Positions and spatial vectors | Bind units, reference and ellipsoid/model; apply Cartesian/geodetic and vector operations in bulk, retaining necessary per-position nonlinear work. |
| Baselines and UVW | Reuse epoch/phase-centre geometry for many vectors; preserve ordered-baseline and tangent-basis conventions and changing contexts. |
| Frequencies, radial velocities and Doppler conventions | Reuse valid context-dependent terms; apply spectral reference and convention transforms, including nonlinear relations and explicit rest-frequency/source-velocity context. LSRK↔TOPO is one route. |
| Earth-magnetic and other supported vector measures | Reuse relevant frame/model state for bulk vector conversions; supply genuinely position/time-dependent inputs at their proper scope. |
| Quanta/unit conversions | Bind dimensional compatibility and scaling once, then apply to numeric arrays; retain any additional semantics of the quantity. |

**One semantic type system:** the batch's frequencies, directions, positions,
epochs, durations, UVW references, units, and frame identifiers are owned by
casa-rs Measures/quanta. Extend or rework those APIs to accept borrowed arrays
with shared or indexed reference metadata directly. `FrequencyView`, `EpochView`,
`DirectionView` and analogous names in this proposal denote native Measures
views, not observation-specific wrappers that must be converted to Measures.
The current `MFrequency`/`FrequencyRef`, `MEpoch`/`EpochRef`/`MjdHighPrec`,
`MDirection`/`DirectionRef`, `MPosition`/`PositionRef`, `MeasFrame` and `UvwFrame`
are starting points; their existing shape and internal interfaces are not
compatibility constraints. Coordinate representation changes such as angles
versus direction cosines remain real operations within the one Measures system.
Observation-specific entity IDs and axis associations remain in the catalogue.

The I/O boundary decodes MSv2 MEASINFO/QuantumUnits or MSv4 measure attributes
into these native references once at their valid scope. Persistent codes,
strings, unknown metadata and original units stay available to the writer for
faithful encoding. There is no subsequent MS-measure → computation-measure type
translation. Scalar APIs and bulk APIs use the same reference semantics and
scientific algorithms. Avoid a `Vec<MFrequency>` or a fully populated
`MeasFrame` per element: use numeric buffers plus checked shared/indexed
metadata. Split-precision epoch arrays and vector-coordinate views must remain
directly usable without collapsing them into an f64 MJD or allocating scalar
objects. A scalar accessor can construct a small value on the stack if needed.

**Prepare, then apply:** Measures owns the conversion route and its bulk
execution, including these three scopes:

| Scope | Work |
| --- | --- |
| Bind once for a conversion request | Validate source/destination references and units, choose the route and scientific/interpolation policy, bind the prepared Measures provider and output representation. |
| Prepare each required scientific context | Resolve relevant source/destination context and model data, in bounded batches when they vary; derive reusable factors, matrices, interpolation data or other family-specific state. |
| Apply to a numeric slice | Run the family's typed arithmetic into a supplied reusable output buffer, or an explicitly supported in-place view. Avoid repeated invariant route/frame/provider setup, per-value allocation and locking. Retain required per-value mathematics and validity checks. |

Varying input coordinates can themselves require different context evaluations.
Bulk preparation must support that dependence without pretending one matrix,
offset or factor fits every value. Keep required auxiliary data bounded and
available to the numeric kernel; optimize repeated work, not away the science.
The shared protocol permits specialized typed scalar/vector kernels, nonlinear
and iterative operations, and per-element error/validity policies. It does not
require a universal matrix transform or a generic tensor engine.

For example, for LSRK↔TOPO at a fixed context, the expensive work is independent of channel
frequency. The bulk kernel can apply prepared Doppler factors across the axis.
Preserve the accepted hop arithmetic/rounding when needed; collapsing all hops
to a single multiplication is an optimization to validate, not a precondition.
Frequency-to-velocity conventions and other nonlinear measures need their own
prepared operations rather than assuming every transform is a scale.

```rust
// Proposed protocol, not existing method names or one universal transform type.
// Bind a family-specific direction/epoch/position/frequency/... transform.
// Input and output are that family's native Measures array/buffer types.
for group in bounded_context_groups {
    let prepared = transform.prepare(group.source_frame(), group.target_frame())?;
    let converted = prepared.apply_into(group.measure_view(), &mut output)?;
    consume(converted); // carries the resulting Measures reference and units
}
```

Source and destination contexts are distinct inputs where required, for example
a moving observation and a fixed image reference epoch. Reference-enum equality
alone does not establish an identity transform when the relevant physical
contexts differ. Missing essential context or unsupported frames fail binding
or preparation; nonfinite values, range errors and partial-output behavior have
an explicit policy, never a silently valid result.

**Reuse only where valid:** a prepared transform is tied to every input that
affects it: references/units, relevant epochs, directions, positions, model and
provider state, source velocity, and interpolation policy. Context changes
invalidate or advance that state. Do not reuse one Doppler factor throughout
an observation merely because it fits in one batch. Identical contexts across
baselines/correlations can share an axis or transform. Time-varying output can
use bounded context-by-channel views; never require a full
time×baseline×channel×correlation frequency array. Context grouping and caches
must themselves be bounded, without indexing the entire observation first.
Approximate reuse over a time/direction interval requires an explicit science
tolerance and validation against the declared reference computation.

**Work-unit ownership:** prepared coefficients are immutable and shareable;
output and mutable conversion scratch follow the admitted work unit. No global
conversion lock, hidden worker pool, or mandatory per-thread copy of an entire
coordinate array. Existing CASA-compatible ordered interpolation has state:
preserve its declared sequence across batch boundaries. Prepare that state in
the required order and hand immutable prepared transforms to workers, with a
bounded queue; completion order must not redefine the interpolation anchors.
Any independent-context mode is an explicit scientific policy and tested
against its own scalar/oracle semantics, not an accidental consequence of
resetting a converter per worker.

The existing MS-specific prepared-factor code moves into the Measures engine;
storage and imaging do not maintain competing routes. Processing planners
request scientific transformations explicitly. An adapter may execute a bound
Measures plan while filling a batch to avoid extra passes, but cannot invent a
different conversion or perform an undeclared frame change on open. Returned
views carry the resulting Measures reference. Frequency-coordinate conversion
alone does not regrid visibilities; interpolation/resampling remains an
explicit scientific operation. Other families likewise retain their meaning;
the system-wide requirement is bulk execution with appropriate reuse, including
when substantial value-dependent work remains.

Current mechanisms and duplication are pinned in the
[Measures source map](README.md#source-map-for-external-reviewers). M1's
`measures-bulk` packet must prove direct use by consumers, scalar/bulk/CASA
agreement, bounded independent work and measured performance against both the
current scalar and already-prepared production paths for the affected families.

### Layouts and scalar precision

Expose typed views with checked extents, axis labels, element strides and
contiguity guarantees.

**Revision 6 (D9, D22): one payload order.** Every payload view is
`[sample, channel, correlation]`, correlation fastest, the order both stores
hold, so no adapter transposes. A consumer that wants channel-major or a
calibration layout takes a permuted or sliced `ndarray` view of the batch;
there is no layout choice in the request. The payload is `Complex<f32>`;
the other value kinds below are not realized by the trial. The paragraph
below is kept for the strides and gathers it describes.

Arbitrary sparse gathers are represented by explicit index
views or packed by the backend, never disguised as a contiguous slice. Arbitrary sparse gathers are represented by explicit index
views or packed by the backend, never disguised as a contiguous slice. A
rank-four view is a compatible factoring of the sample axis into time and
baseline, not a v4-specific operator path. M1 must choose only the
stride/permutation forms real kernels need and match them once per batch.
Arbitrary-rank tensors, negative
strides, and unrestricted permutations are not initial requirements. A kernel
that requires contiguous correlation vectors can state that as a hard
requirement. A fixed channel axis within a batch does not imply uniform channel
spacing, ascending order, or a single frequency frame across the observation.

For factorable axes, filling timestamps and baseline pairs costs O(T+B) entries,
not O(T×B) headers. This law does not pretend UVW, occupancy or genuinely
sample-dependent metadata is separable. M1 measures total metadata bytes and
hot-loop cost on few-channel data as well as many-channel data. Retain the
shared topology view pending that evidence; neither an extra kernel nor a
mandatory transpose follows from the presence of a time/baseline axis.

```rust
enum VisibilityValues<'a> {
    Complex32(VisibilityView<'a, Complex<f32>>),
    Complex64(VisibilityView<'a, Complex<f64>>),
    Real32(VisibilityView<'a, f32>),
    Real64(VisibilityView<'a, f64>),
}

struct DataGroupView<'a> {
    id: DataGroupId,
    values: Option<VisibilityValues<'a>>,
    flags: Option<NativeFlagView<'a>>,
    weights: Option<WeightView<'a>>,
    uvw: Option<UvwView<'a>>,
    field_and_source: FieldSourceContext<'a>,
    associations: GroupAssociations<'a>,
}
```

### Buffer ownership and array views

The batch carries the **actual numerical data**, accessed through typed array
views. A view is a borrowed reference into an existing numeric buffer, with
shape and element strides. It can cover the whole array or a rectangular/strided
subset; making such a view does not copy its elements. Indexing it reads the
stored value directly. Arbitrary gathers require the explicit index/packing
representation described above; a regular strided view cannot express every
selection.

Before delivery, the source completes the requested read/decode/conversion/
packing needed by that batch. Accessing a visibility, flag or weight through a
delivered view does not perform backend I/O or decode a chunk. The backing
buffers may belong to the caller, session or an admitted work unit; the borrowed
`VisibilityBatch<'a>` need not itself own their allocations. Its lifetime keeps
the storage valid and prevents refill while the borrow is live. The nested
`groups` field supplies access to the payload arrays, not just their catalogue
descriptions. Dispatch on type/layout happens outside the numerical loop.

For independent workers, transfer the buffer owner or share read-only storage
under the lifetime rules in section 5, then borrow local views. A small view
can keep a much larger allocation live until its last use. Writable views require
exclusive access or proven disjoint mutable regions; a broadcast view is not a
set of independent writable cells. Borrowing a view does not by itself authorize
a persistent update.

### Using ndarray

**Owner-agreed direction:** use `ndarray` for numerical arrays
and borrowed views, with thin domain types carrying checked axis/layout
semantics, native Measures metadata, flag/weight meaning and update provenance.
This agreement selects the array library; concrete owner/view APIs and the
overall design remain open for iteration and the M1 evidence requirements apply.
There is no architectural reason to build a competing general array library.
The workspace already declares `ndarray = "0.17"` and locks 0.17.2;
[`casa-values::ArrayValue`](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-values/src/lib.rs#L336)
already holds typed `ArrayD` payloads.

An [`ArrayView3<'a, Complex<f32>>`](https://docs.rs/ndarray/0.17.2/ndarray/type.ArrayView3.html)
is a natural building block for the three-axis example below. Prefer fixed-rank
views for the admitted layouts and a compatible rank-four view for factored
axes; this does not require copying an existing dynamic-rank allocation. An
owning `Array3`/`Array4` or a reusable flat allocation can supply views. Choose
the owner separately from the consumer view, so a kernel does not require a
particular allocation or reference-counting policy.

Slicing an array/view returns another borrowed `ArrayView` (or `ArrayViewMut`
for exclusive mutable access). For example, on the `[2, 2, 4]` sample-major
array below, `values.slice(s![.., 1..2, ..])` selects channel 1 with shape
`[2, 1, 4]` without copying; the view retains strides `[8, 4, 1]` and is not
contiguous across samples. `Slice`/`SliceInfo` and the `s!` macro describe the
selection, rather than owning its elements. A regular Rust `&[T]` is available
only when layout permits: `as_slice()` requires contiguous standard order;
`as_slice_memory_order()` permits contiguous storage in its physical order.
Neither accessor silently packs a noncontiguous view.

`ndarray` supplies shape/stride-aware access, slicing and read-only broadcasting;
it does not supply scientific axis meanings, Measures references, resource
admission, storage scheduling or write provenance. Preserve the closed layout
contract even though the library supports more layouts. Use contiguous slices
or typed iterators where appropriate in hot loops; an `ArrayView` does not by
itself guarantee a contiguous slice or efficient loop order. Do not introduce
implicit owned copies, packing, broadcast expansion or copy-on-write mutation
into a path that promises borrowed access. Charge any required copies and
allocation lifetimes explicitly. The library's
[ownership and layout documentation](https://docs.rs/ndarray/0.17.2/ndarray/struct.ArrayBase.html)
is part of the M1 review basis. M1 must prove direct views, buffer reuse and
cross-worker handoff against existing performance before freezing concrete
types; no performance result is claimed by this proposal.

### Concrete batch example

Illustrative values, not an observed dataset or a finalized constructor API:
one time, two ordered baselines, two channels, four correlations. The
sample-major values and boolean flags each have shape `[2, 2, 4]` and element
strides `[8, 4, 1]`; flattening uses `i = s*8 + c*4 + p`. `Complex32` above
means `Complex<f32>` (64 bits total), not a 32-bit complex scalar.

```text
time = MJD 60000 TAI + 0 s (split epoch representation)
baselines = [(antenna 0/feed 0, antenna 0/feed 0),
             (antenna 0/feed 0, antenna 1/feed 0)]
frequency_hz = [1_000_000_000, 1_001_000_000]  // TOPO
correlations = [XX, XY, YX, YY]
field(time) = A; phase_centre(A) = (1.0, 0.5) rad, ICRS
uvw_m[sample, uvw] = [[0, 0, 0], [100, -40, 20]]
// UVW is referred to that phase centre with the declared baseline convention.

groups[0].values[sample, channel, correlation] = Complex<f32>, Jy
  sample 0, channel 0: [10+0i,  1+2i,       1-2i,       8+0i]
  sample 0, channel 1: [12+0i,  0.5+1i,     0.5-1i,     9+0i]
  sample 1, channel 0: [ 3+4i,  0.2-0.1i,  -0.3+0.4i,   2-1i]
  sample 1, channel 1: [ 4+3i,  0.1+0.2i,   0.5-0.2i,   1+2i]

groups[0].flags[sample, channel, correlation] = bool
  sample 0, channel 0: [false, false, false, false]
  sample 0, channel 1: [false, false, false, false]
  sample 1, channel 0: [false, false, false, false]
  sample 1, channel 1: [ true, false, false, false]

groups[0].weights[sample, channel, correlation] = f32, channel stride 0
  sample 0: [ 4, 2, 2,  4]
  sample 1: [16, 8, 8, 16]
// Declared inverse-variance weights in Jy^-2 for this example: one per
// correlation (MSv2 WEIGHT), seen through every channel by a zero channel
// stride, without expanding the eight stored numbers (Revision 6, D5).
```

Both acquisitions are known present; all values are finite, both samples are
selected, and no row flag is set (Revision 6, D5: an MSv2 `FLAG_ROW` is
folded into every flag of its row when the batch is filled). The illustrative consumer excludes flagged
elements. At `(sample=1, channel=0, correlation=XX)` it reads `3+4i`, flag=false,
weight=16, frequency=1 GHz and UVW=(100,-40,20) m. The XX value at channel 1
remains `4+3i` in the buffer but is excluded by its flag. Data are not replaced
by zero to express exclusion. Different data groups have their own bound array
associations; this example shows only one requested group.

### Role presence and precision policy

Options mean a role was not requested; a missing required role is a bind error.
Native absence/undefined cells require their own validity representation, not
an empty slice that can be mistaken for a valid all-zero array. Data-group
associations bind semantic roles to variables, including aliasing; labels such
as raw/corrected/model are conveniences, not universally reserved variable
names. Legacy DATA/CORRECTED_DATA/MODEL_DATA map to explicit group definitions
with their shared flag/weight/geometry associations.

Match scalar kind and layout once, then invoke a typed loop. Persisted and
compute precision are separate. A caller can request native scalar precision,
lossless widening, or explicitly approved narrowing; defaults do not narrow
complex128 to complex64. The writer checks destination representability too.
Packing and conversion are charged and fused with decode/fill where possible.
The initial imaging binding explicitly requests complex64 values (Rust
`Complex<f32>`) and, where scientifically approved, narrowing from complex128
during fill to feed the existing operator. Calibration states its own precision
policy. Grid/accumulator precision is a different contract: an f64 grid does
not by itself authorize input narrowing. M1 tests finite-range/invalid-value
behavior and the existing science tolerances; no second f64 imaging route is
introduced merely to avoid declaring the conversion.
Real-valued legacy FLOAT_DATA remains representable, without claiming all
single-dish algorithms or XRADIO spectra are implemented by this programme.
Existing LAG_DATA and other optional v2 columns retain their current generic
preservation/transform capabilities; they are not silently reinterpreted as
frequency visibilities to fit this enum.

### Autocorrelations and real-valued data

For the same antenna, feed and receptor, the ideal self-product
`XX = <E_X conjugate(E_X)> = <|E_X|^2>` is real and nonnegative; likewise YY
(or RR/LL). But an antenna's full polarization correlation matrix also includes
`XY = <E_X conjugate(E_Y)>`, which can be complex. Ideally `YX = conjugate(XY)`.
Products of different feeds/signals likewise need not be real merely because
the antenna IDs match. Stored or processed data are preserved as supplied;
opening must not enforce ideal reality/Hermitian symmetry by changing values.

MSv2 [`DATA` is complex and `FLOAT_DATA` is real](https://github.com/casacore/casacore/blob/a1399b6b99907bd14021920a089403a93c10aba1/ms/MeasurementSets/MSMainEnums.h#L176).
Autocorrelations stored in DATA retain its complex element type, including the
real-valued parallel hands. In the pinned MSv4 schema,
[`VISIBILITY` is complex64/complex128](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L1020),
whereas [`SPECTRUM` is real](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L1001)
with `[time, antenna_name, frequency, polarization]` dimensions. The latter is
a separate single-dish representation, outside the initial v4 VisibilityXds
profile; it is not a per-baseline float subtype inside VISIBILITY.

Preserve the scalar kind of each selected data variable, subject to the explicit
precision/conversion request. Do not infer a real dtype from `antenna1 == antenna2`
or introduce a per-element real/complex variant. If a consumer requests only
real self-products from complex storage, any real projection requires a defined
policy for imaginary components and writable provenance; it is not an implicit
lossless conversion. Normal mixed auto/cross visibility batches use a homogeneous
complex array and one typed numerical loop. Real legacy FLOAT_DATA can use the
existing real variants under the supported v2 profile.

### Coordinate dependencies

Metadata has declared axis dependencies and checked broadcast views. Constants,
per-time, per-baseline, per-sample, and scientifically required channel or
correlation dependence must not become duplicated per-element objects. Preserve:

- Ordered antenna/feed pairs and autocorrelations; the stored UVW convention,
  unit and reference frame. Swapping antennas is not an incidental optimization:
  it requires UVW sign changes, complex conjugation and cross-hand remapping.
- Time midpoint, time centroid, additional precision, epoch origin/time scale,
  interval and effective exposure. Use an epoch representation capable of
  retaining the source's split precision; do not blindly collapse all timestamps
  to one f64 Unix-seconds field. Distinguish absent metadata from defaults.
- Frequency coordinates, signed channel widths, effective bandwidth/resolution,
  frame/rest-frequency information, and correlation labels/order/feed basis.
- Data-group-specific field/source/phase-centre context, moving-source/ephemeris
  dependencies, and each antenna's pointing. Phase centre is not pointing.

Measures owns the reference-frame mathematics and reusable UVW transforms;
the scientific geometry owner chooses their context and requested projection.
Bind reusable transforms once at their valid scope; compute genuinely
time-dependent quantities at the required cadence. Do not reproduce that engine
inside either storage adapter or an imaging-specific Measures implementation.

### Flags and weights

`NativeFlagView` retains boolean or unsigned bitmask dtype/width, reason-bit
definitions, legacy row flags, and requested category flags. (Revision 6, D5:
the trial delivers boolean flags with `FLAG_ROW` folded in and no separate
row-flag view; bitmask and category flags are not realized.) A separate
effective-validity projection combines requested role validity and flag policy
for computation. MSv4 nonzero bitmasks are flagged; policy must not quietly
ignore unknown bits. A boolean projection is insufficient for lossless writes.
Generic operations preserve native categories and unknown bits even when the
consumer does not interpret them.

New v4 boolean flags are the default for boolean input, including v2 conversion.
For an existing bitmask, generic flagging requests `UNSPECIFIED_BIT` unless the
algorithm supplies a more specific reason; inability to apply a correction may
request `POSTPROC_BIT`. Bind that declared reason to its actual bit position;
POSTPROC is not the default for every flagging operation. Never assume the
position from a suggested label list or set an arbitrary bit. Missing or
ambiguous reason metadata requires an explicit caller bit mapping or a supported
structural plan to declare one, otherwise reject before mutation. Clear only
explicitly selected reasons; clearing an effective boolean must not clear all
unknown reasons. A conversion to boolean may preserve nonzero/effective-flag
semantics but loses reason provenance, which the conversion policy must report.

`WeightView` represents native precision, units/meaning and broadcast axes:
for example per-sample/per-correlation WEIGHT versus channel-dependent
WEIGHT_SPECTRUM. Preserve the source array without mandatory expansion. The
request names the weight role and fallback rule for absent or undefined cells;
record which rule was bound. SIGMA and SIGMA_SPECTRUM are not silently equated
with weights; conversion is explicit. Nonfinite or negative weights remain
detectable and follow the algorithm's established science policy. The backend
does not invent unit weights or turn unusable measurements into zero data.
For fallback-derived values, retain which native role/cell supplied them at the
necessary per-sample granularity. Reading a fallback does not authorize writing
the preferred role, or expanding a broadcast scalar's mutation scope.

## 4. Access requests and scientific groups

The processing algorithm's planner builds this request from the operation's
requirements and selected task parameters. It sends the request through the
common source interface to the MS adapter during binding. The numerical kernel
then consumes the bound views; it does not have to issue I/O or describe its
access pattern again for every value. The exchange has two directions:

```text
planning:  algorithm planner -- AccessRequest --> MS adapter
           algorithm planner <-- realization / cost / limits -- MS adapter
execution: algorithm        <-- VisibilityBatch / windows ---- MS adapter
```

The access pattern is explicit, not inferred from callbacks or from a requested
array layout alone. It covers selected roles/extents; required or permitted
traversal order; scientific group boundaries and fragment/interleaving rules;
compute-window shape, contiguity and required neighborhoods/overlap; and revisits
or repeated passes. An expected pass count may be an estimate (for example
adaptive major cycles); binding states what depends on that estimate. A revisit
request does not imply all earlier values remain cached.

```rust
struct AccessRequest {
    selection: ObservationSelection,
    roles: RoleSelection,
    grouping: ScientificGrouping,
    layouts: AcceptedLayouts,
    precision: PrecisionRequest,
    units: UnitRequirements, // canonical defaults; explicit per-role overrides
    traversal: TraversalRequirements,
    revisits: RevisitRequirements, // passes/reuse semantics; estimates identified
    compute_windows: ComputeWindowRequirements,
    delivery_limits: DeliveryLimits,
}

struct GroupFragment {
    key: ScientificGroupKey,
    position: FragmentPosition, // Whole, First, Middle, Last
}

enum GroupRealization {
    Sequential,
    Concurrent { max_live_groups: usize },
}
```

**Revision 6: what the trial's request holds.** `AccessRequest` names the
partition set (selected by content from the catalogue), one data group, the
roles (payload, UVW, time auxiliaries, scan), a channel window, a
`CorrelationRequest` (the products delivered and whether their flags are
their own or the OR over every product, D17), the scientific grouping keyed
by field and scan as production `gaincal` keys solutions (D13 to D15), and a
byte budget. The budget admits whole times and counts the batch and the
cursor's decode scratch for it (D7, D10, revision 3);
`Observation::estimate` costs that batch shape without reading payload and
binding returns the same estimate. Layout and precision requests are gone
(D9, D22); revisits and compute-window requirements are realized as views
and repeated traversals rather than request fields.

Hard requirements include requested data/selection, semantics of solution bins,
ordering needed for an algorithm, allowed numeric conversions, accepted layouts
and stride constraints. Preferences include target aggregation size and locality
when scientifically interchangeable. Resource limits come from an admitted
budget, not an uncoordinated request to allocate memory. Bind returns the exact
realization or a typed error plus the limiting capability/minimum working set.

The adapter chooses physical reads, chunk aggregation, bounded prefetch/reuse and
packing among the permitted realizations. It must not silently change scientific
grouping or a required order. A new phase with incompatible access requirements
binds a new realization under the same observation/resource ownership; changing
requirements does not require introducing a second processing engine.

The binding summary includes physical read units, decode units, selected bytes,
and expected read/decode amplification for the requested channel windows and
number of traversals. Distinguish exact shape-based decode counts from estimates
of compressed bytes and cache reuse; measure both during execution. A full-band
compressed chunk cannot supply a narrow channel window at narrow-window decode
cost. A bound that cannot admit the minimum decode unit fails explicitly.

Compute-window constraints describe what a kernel can borrow or process at once.
Delivery limits bound the source's aggregate batch and come from admission and
explicit algorithm requirements. A small compute window does not impose the
same limit on native I/O aggregation. The backend may deliver many such windows
in one batch unless the algorithm explicitly requires otherwise.

Scientific groups are formed from declared semantic keys: for example a time
bin plus field and SPW-combination rules from calibration. They can span
partitions, batches, physical files, and storage chunks. Time-bin boundaries and
reference epochs are explicit. The storage backend executes the requested
grouping; it does not decide which fields/SPWs a solver may combine.

The algorithm declares accepted group realizations. `Sequential` completes one
group at a time; `Concurrent` permits storage-order fragments for a bounded set
of live groups. The algorithm supplies an accounted state bound as a function
of actual group shape (baselines, channels, correlations and solver/window
state, including time-deduplication sets and robust statistics), not a universal
constant bytes-per-group. Admission includes the live states and maps.
Neither realization mandates a full-selection row index.
A single visibility batch may lend ranges/index views to several groups without
re-reading or copying the payload. Binding chooses an accepted realization and
reports any index/reorder bytes and repeated physical reads.

`First/Middle/Last/Whole` describe each group's fragments, not global stream
order. A control event can end a group after its last payload batch, including
an empty group. Signal completion exactly once: `Last/Whole` when already proven
at delivery, otherwise a later `GroupEnd`; do not both end and reopen the same
group to satisfy a memory limit. Closure requires a proven completion frontier
for that group's selection (for example a validated traversal order covering every partition),
or actual end-of-stream. A timestamp crossing a bin in an unordered/interleaved
stream is insufficient. Concurrent groups with no earlier closure remain live
to end-of-stream. Admission must establish the live-set bound or use an explicit
counting/planning pass; reaching it cannot silently drop, evict or prematurely
solve a group. Offer an admitted sequential/indexed or multiple-pass realization
when the request permits one; otherwise reject. No visibility replay store is
introduced. Cancellation/error is not a successful group-end event.

Terminal markers belong to the complete group-specific fragment, not each
compute window borrowed from it. Slicing must not duplicate `Last/Whole` or
publish completion before all those windows are consumed. `GroupEnd` establishes
that no more input will arrive; it does not establish that earlier worker jobs
have completed. The consumer finalizes only after all prior fragments/windows
for that key have been consumed/joined under the existing executor. M1 tests
split windows, delayed completion and empty groups without another scheduler.
(Revision 6: `GroupCompletion` joins the two, handing out a move-only
`WorkToken` per unit of work, so a unit ends once and only begun work can
end.)

A solver declaring splittable accumulation consumes fragments and retains only
its admitted sufficient state. If a scientific algorithm genuinely needs all
samples in a group simultaneously, it declares that requirement and fails
admission when it cannot fit; the backend does not secretly materialize it.
Revisiting a group for an iterative solver is a new bounded pass. If ordering
cannot be realized within available memory and an explicitly supported bounded
index strategy, return an error rather than add an unplanned visibility replay
store. Imaging need not request calibration grouping.

## 5. Ownership, reuse, and resource accounting

```rust
// API shape to prove in M1, not a trait definition ready to paste into Rust.
let mut cursor = source.bind_read(request, admitted_resources)?;
let mut buffers = cursor.allocate_buffers()?;
while let Some(event) = cursor.next(&mut buffers)? {
    match event {
        ReadEvent::Batch(batch) => process(batch.borrow_window(window)?),
        ReadEvent::GroupEnd(end) => finish_group(end)?,
    }
} // each borrow ends before refill/reuse
```

Buffers are caller/session owned with reusable capacity. A batch borrows their
arrays and retained context; safe Rust prevents refill or mutation while views
remain borrowed. Pipelining uses a bounded admitted pool of buffers and transfers
exclusive ownership; no `Arc` clone is permission to retain unlimited batches.
Coordinate/group catalogue references can be shared without cloning payloads.

Revision 6 realizes both. A batch's buffers keep a shared reference to the
catalogue they were checked against, so an owned batch is self-contained
(D1). Owned batches are filled into a bounded `BufferPool`: a buffer is
taken only for a batch, the producer waits while none is free, and a
`FilledBatch` returns its buffer when dropped, or when its last `Arc` is
(D12, revision 3). A pool of `n` buffers holds at most `n` times the
estimate's held bytes per batch, the figure admission reserves; connecting
the pool to the runtime's admission is M2 work.

### Independent work-unit lifetimes

The loop above illustrates synchronous use, not a requirement that all consumers
finish a batch together. A **work unit** executes one or more compute windows on
one worker or a cooperating subset of the existing admitted team. This execution
group is distinct from a scientific group such as a calibration solution bin.
Ownership follows the work, without requiring permanent OS-thread affinity or a
new pool/scheduler. A consumer can have private work-unit state and scratch,
share read-only inputs with its cooperating workers, and use worker-local
scratch whose shorter lifetime permits reuse.

The API must support safe Rust transfer of buffer ownership between work units
and scoped sharing of borrowed views. Mutable compute arrays have one owner or
proven disjoint mutable regions; disjoint logical windows alone do not establish
disjoint persistent updates (C5 still applies). M1 must prove the owning/borrowed
API, including safe cross-worker handoff, before selecting concrete types.
Ownership operations remain at batch/work-unit granularity, outside scalar loops.

Independent work may finish out of delivery order when the request's semantics
permit it. A completed unit can return its private buffers and let another
admitted unit run while slower work continues. The representation imposes no
global barrier: required joins come from scientific dependencies, declared
ordering, or conflicting writes. Reuse waits for actual last use, including
outstanding device operations where applicable; dispatch or cancellation alone
does not establish completion. Group-input completion remains separate from
completion of the jobs consuming it, as specified in section 4. Ordered outputs
and pending writes must not introduce an unbounded completion/reorder queue.

A slow window borrowing one large backing allocation keeps that **entire
allocation** live; releasing other slices does not free their share of it.
Large native I/O aggregation therefore does not imply one indivisible lifetime
for all subsequent computation. Realizations can use independently owned pieces
or explicitly budgeted packing when prolonged sharing would pin too much memory.
Account actual backing allocations once, all simultaneous private scratch and
packed copies, and bounded in-flight/ready/write queues. The existing resource
owner supplies backpressure when the admitted pool is exhausted. Measure pinned
bytes, copy/decode costs and stalls before choosing the sharing/packing policy;
do not solve lifetime skew by allocating an unbounded tail of batches.

For cube CLEAN, visibility batches belong to gridding/prediction passes. The
longer-lived model, residual, mask and solver state belong to the imaging owners;
a channel's minor cycle must not retain an input visibility batch merely because
that channel needs more iterations. Independent channel work can finish and
reuse private scratch at different times. Required image state remains available
for later major cycles/products under existing image storage and admission.
Cross-channel coupling or major-cycle/stopping rules can still require joins;
this contract does not change the science or replicate the whole cube per worker.

### Accounting and backend realization

Backends can fill directly into a requested layout or lend a decoded chunk view
when lifetime, precision and strides permit it. Otherwise they disclose and
budget a packed copy. No universal zero-copy promise is made for compression,
type conversion, transposition, sparse gathers, or GPU upload. Instrument
bytes read/decoded/copied, allocation counts, call counts and peak live storage
at meaningful batch/phase boundaries, not every scalar.

Admission includes selected payloads, native decoded chunks/tiles, codec scratch,
conversion and permutation scratch, coordinate/index arrays, caches, prefetch
and write queues, and consumer compute scratch simultaneously alive. Reuse the
existing host/resource policy and worker team. A codec's unavoidable minimum
chunk/shard scratch must be discovered or conservatively bounded before work;
if it exceeds the budget, fail with the minimum requirement. Chunk caches are
bounded by bytes and actual ownership, not just entry count.

Long-lived store state is part of the bound too: per-key locks, file handles,
metadata/decoder caches and absent-chunk lookups must not grow without limit
with total objects visited. M1 scans increasing prefixes of present and missing
chunk keys and repeated passes at fixed budgets; retained allocations must
plateau under the admitted cache/registry limits. A dependency pin that retains
every visited key is not accepted by merely accounting for an ever-growing map.
Pin/audit `zarrs_filesystem` and codec dependencies, not just `zarrs`.

Prove the actual W1/W4 decode path uses the admitted executor. Configure nested
concurrency per operation, including minimums; a target of one alone does not
prove serial execution. W1 must not fall through to a global thread pool. Count
executing threads and simultaneously live encoded/decoded/codec buffers; avoid
racing process-global configuration changes.

No legacy `rows_per_block` limit constrains a factored MSv4 array. Logical
compute-window size and physical aggregation can grow independently when the
budget allows. A larger visibility batch must not increase the number of repeated
per-sample adapter operations or metadata allocations.

## 6. Source and sink lifecycle

The common lifecycle is `open/create → inspect/select → bind → fill/patch/append
→ finish`. Source/sink traits describe this behavior; `casa-ms` chooses the
concrete backend and uses enum or static dispatch at the binding/batch boundary.
An implementation can use separate read, update and creation session types to
make invalid transitions unrepresentable. It need not force all operations into
a huge trait whose unsupported methods panic.

Capabilities distinguish reads, value updates, new-role/data-group creation,
structural append, creation/publication, and locking guarantees. Metadata-only
inspection, selections and diagnostics use the same catalogue. Table tools that
explicitly operate on generic casacore tables remain table-specific.

Keep source ownership/locks across the intended run. Plan read+write access
before acquiring locks: no implicit upgrade from a retained read lock while
other independent readers remain. A mixed read/update session owns the required exclusive
access and explicit read/write roles. Original-input semantics require disjoint
underlying read/write variables or element regions after resolving data-group
and broadcast aliases in each role's native domain (the mutation closure in
section 7), or a proven read-before-write traversal covering every read of an
element. Different role/group names alone do not establish disjointness.
Reject overlapping or revisit patterns that cannot meet that rule;
do not imply snapshot semantics or retain a hidden whole-array/replay copy.
A batch address cannot be used after closing/reopening its
source, even at the same path. Normal Rust ownership/session membership checks
provide this protection, without hashes or persistent identity markers.

For final-pass MODEL_DATA writing, use one observation owner with planned write
intent. Create/validate the destination column under its write ownership before
traversal, then lend the reader and writer views of that same owner. Read DATA
and write MODEL_DATA with alias/scope checks; do not independently reopen the
same table and attempt a hidden upgrade. If a preceding read-only phase existed,
close its conflicting borrows/handles before the explicit ownership transition.
Independent read-only sessions may coexist; a conflicting independent writer
or mixed session in the process is refused, never waited on. Existing casacore
cross-process locks remain authoritative for v2.

The initial local v4 profile supports one mutation owner per store in this
process and requires the caller to exclude external writers/structural changes.
Its capability report must say that it supplies no cooperating cross-process
lock protocol with XRADIO and no consistent snapshot for concurrent readers.
An independently reopened oracle reads after successful finish. Atomic object
replacement below does not make concurrent multi-object or multi-read-shard
observations consistent, nor prevent another writer from overwriting a change.

## 7. Updates and creation

An `UpdateMapRef` refers to a session-owned inverse map from selected logical
elements to a named writable role's native address domain (cells, scalars or
chunk coordinates). That domain need not equal the visibility array's domain.
A permutation or selection
composes that mapping. A source may use implicit rectangular strides or compact
row maps; it need not allocate an address for each visibility. M1 must prove
borrowed-map and bounded queued-write lifetimes without retaining read buffers
until the whole pass finishes.

`RoleUpdateMaps` is a scoped accessor to that provenance, not one visibility
map reused for every group/role. It yields role-specific maps for visibility,
row weights, spectral weights, flags and other writable roles with their own
native domains. A patch's `role` must match the role bound into its map; shared
underlying destinations are still detected as aliases/conflicts.

```rust
enum PatchValues<'a> {
    ReplaceVisibility(VisibilityValues<'a>),
    ReplaceWeights(WeightView<'a>),
    ChangeFlagBits(FlagBitChanges<'a>),
    SetRowFlags(RowFlagChanges<'a>),
}

struct ObservationPatch<'a> {
    destination: UpdateMapRef<'a>,
    role: WritableRole,
    scope: MutationScope, // includes group and broadcast aliases in native domain
    changed_region: LogicalRegion<'a>,
    values: PatchValues<'a>,
}
```

Patches identify actual changed regions/roles, not a requirement to rewrite all
fields returned by a read. Bit operations preserve unselected/unknown bits.
Changing an effective boolean flag must not silently clear FLAG_ROW or discard
FLAG_CATEGORY. Setting/clearing row flags is a separate operation. Patches
outside selected extents, with incompatible shape/precision, from another
session, or with ambiguous destinations fail validation before submission.

Resolve conflicts and the full affected logical extent **after** mapping to the
native role and expanding both data-group aliases and broadcast aliases. For
example, one v2 WEIGHT scalar of 2 supplies channels 0 and 1. A request to set
only channel 0's effective weight to 8 cannot silently modify that scalar and
therefore channel 1. Require an explicit whole-scalar scope, a supported plan
that creates/initializes a channel-resolved destination under bounded memory, or
rejection. Logically disjoint channel patches may conflict at that same scalar.
Fallback from undefined WEIGHT_SPECTRUM is resolved per cell; a patch names its
actual intended destination independently of the fallback read. Calibration
owns any reduction from channel weights into row summary WEIGHT and submits
both native-role patches when appropriate; storage never invents that inverse.

Repeated reads of the same destination are allowed, but overlapping queued
writes require an explicitly supported composition/order rule or rejection;
there is no default last-writer-wins behavior. Averaging, resampling, or
reductions lose their source update map. Their results go to an explicit output
or scientifically defined scatter operation, not an invented inverse.

Groups may share flags, weights, UVW, or field/source variables. A mutation must
either explicitly affect that shared variable and its aliases or first create
an independent variable and rebind the selected group under a supported
structural-write plan. Neither unintended cross-group mutation nor hidden
whole-variable copying is allowed. The bind/preflight summary names aliases
and the cost of an explicitly requested detachment. Legacy columns can have
less expressive associations; reject a requested mapping they cannot represent.

The sink coalesces bounded patches for each physical update unit. For Zarr that
may be a shard rather than one logical chunk. Disjoint logical slices do not
imply independent physical writes. One admitted owner serializes/coalesces each
unit; within-process exclusion does not promise safety against another process
or object-store client. Selective writes preserve other chunks/cells, codecs,
tiling, unrelated columns, extension metadata and keywords.

When the sink has the complete replacement content for every preserved byte/bit
of an independently writable physical unit, read zero old destination payload
bytes. Metadata/index reads are counted separately. Full logical-chunk coverage
is insufficient when a shard contains other chunks; full-column coverage is
insufficient when a casacore tile co-stores other columns. Partial flag-bit
updates need old bits unless the complete replacement is already available in
a valid retained buffer. Partial writes coalesce within the admitted update
window, reading/decompressing a covering unit at most once per such flush when
the backend permits it; report rereads across windows. Do not promise one read
per entire run by retaining an unbounded dirty-chunk cache.

Perform a **structural preflight** before mutation: required roles/managers,
profile/dtype representability, alias scope, locks/concurrency capability, buffer
budget and output metadata support. Do not scan/hash the payload just to grant
permission. Data-dependent errors may still occur while streaming. Successful
updates preserve values outside the declared mutation closure. In-place failure
may leave partial completion across units; v2 keeps its current CASA-compatible
failure semantics, without inventing per-cell atomicity.

**Proposed local v4 guarantee:** replace each encoded object (chunk or shard,
and separately each metadata object) through bounded temporary storage and an
atomic same-filesystem replacement, rather than truncating the live object.
On a supported filesystem, I/O failure or process termination during replacement
leaves the previous published state (including an absent key representing fill)
or the new complete object at its path; cancellation between
flushes leaves earlier units changed and later units unchanged. This is a new
profile proposal for owner review, not a guarantee supplied by ADR-0008 or by
arbitrary `zarrs` stores. Reject selected mutation where the required replacement
capability cannot be established instead of silently falling back to truncation.

Charge encoded/decoded buffers, temporary disk space, cleanup state and
concurrent replacements. Temporary publication files have bounded ownership and
do not become dataset identities, backups, recovery journals or retained
generations. M1 injects short writes/ENOSPC and terminates during replacement,
then independently reopens and checks selected and unselected sentinels in that
unit and other units, including failed first materialization of an absent chunk.
The guarantee covers object integrity under those process
and I/O failures; power-loss durability additionally depends on file/directory
synchronization and filesystem behavior. Declare and test the chosen finish
durability boundary, measure its synchronization cost, and do not equate rename
or per-file `sync_all` with crash-safe publication of a dataset.

Return failures and completed/indeterminate scope known without a durable
journal. Related variable and metadata objects can still represent a partially
completed operation; structural operations must define their publication order
and failure diagnostics. There is no whole-store transaction or rollback.
Flush/finish errors are reported; dropping an unfinished session is not success.

Creation takes a scientific catalogue and an explicit storage configuration
(MS profile, managers or chunks/codecs, target transport, precision). Once bound,
`append`/`write_region` consume the same batch vocabulary as reads, with explicit
target extents rather than source maps. Record growth strategy up front:
casacore row append, Zarr known-shape region writes, and Zarr resize are distinct
capabilities. An unimplemented append capability is rejected before creation.

New local datasets use sibling staging and publish only after successful
completion where filesystem semantics provide that guarantee. Do not assume
object stores can rename a dataset atomically. Cross-format creation preflights
metadata/flag/occupancy mappings and rejects unapproved loss. Unknown extensions
stay backend-retained for same-format updates; cross-format transfer requires a
defined mapping or an explicit unsupported/loss decision, not private sidecars.
The [conversion profile](architecture.md#conversion-fidelity) names the known
losses; a source-free creator must satisfy its destination metadata contract
without relying on retained metadata from an input dataset.

## 8. Worked counterexamples for contract review

1. **Sparse v2, dense v4:** two integrations, three baselines, one absent
   acquisition, one real NaN measurement and one duplicate acquisition. Read
   native topologies without merging duplicates; numerical validity is not
   proof of original occupancy. Arbitrary external v4 cannot recover the
   original row set. Require a declared conversion policy or rejection.
2. **Mosaic plus two SPWs:** field varies over time; SPWs have different channel
   counts/frames. Keep batches homogeneous while imaging visits both partitions
   and a solve may group their fragments. Do not pad them into one invented
   frequency axis or equate a field with antenna pointing.
   Compare compatible fields combined along time with the same data partitioned
   by field, retaining repeated visits and sample-to-field/pointing associations.
3. **Calibrate then write:** gather channels in requested order, solve from
   fragments, apply gains, patch corrected data and selected flag bits. Preserve
   untouched channels, native weights/flag categories, group aliases and actual
   storage bindings. Reject writing averaged solve inputs back as if invertible.
4. **Large v4 batch:** decode multiple chunks into one factored batch; borrow
   many smaller compute windows. Peak memory counts both decoded and packed
   storage where they overlap. Demonstrate fewer I/O/decode calls or reduced
   elapsed time without inflating kernel scratch.
5. **Failure:** cancel between two chunk/tile flushes. Release ownership, report
   partial completion and do not claim rollback or successful group completion.
   Also fail inside v4 object replacement and prove the old-or-new object rule,
   including unselected regions. New staged output is never published as complete.

These become executable contract laws in M1. They are not substitutes for the
representative science, performance and independent interoperability gates.
