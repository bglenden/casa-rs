# Intermediate observation contracts

Status: draft revision 2; incorporates review input, not accepted or frozen
Truth class: design proposal
Date: 2026-10-23

This is the interface between persistent observations and numerical consumers.
The names and Rust sketches are provisional; the semantics below are the design
to review with the owner. Neither the external reviews nor this revision
finalize the design. M1 must turn the sketches into a minimal compilable API
with worked imaging, solve, update, and source-free creation examples before
broad implementation starts.

## 1. Contract rules

| Rule | Required behavior |
| --- | --- |
| C1: source independence | No physical row IDs, Zarr keys, storage managers, or MS-version branches in scientific kernels. |
| C2: faithful values | Explicit units/frames/precision, native flag and weight semantics, full data-group association, ordered baselines. No silent scientific transform. |
| C3: layout independence | Separate sample topology, memory layout, scientific grouping, and storage chunking. |
| C4: bounded ownership | Every live payload, index, decode buffer, cache, packed copy and queue is admitted; borrowed views cannot outlive/refill their storage. Independent work can retire separately; shared storage stays live until its last use completes. |
| C5: invertible updates | Sample-preserving reorder/selection retains a checked inverse source map; reduction does not. Shared variables have explicit mutation scope. |
| C6: honest guarantees | Unknown acquisition occupancy is representable; unsupported profiles/layouts and non-atomic writes are explicit. |
| C7: bulk execution | Bind numeric kind, layout and context outside scalar loops; no required per-sample allocation, virtual call, or coordinate reconstruction. |

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

A block is homogeneous in spectral coordinate context and correlation setup.
Different shapes/SPWs yield separate blocks, even when one scientific group
spans them. A field/phase centre may vary inside a block, including a mosaic.
The request can select multiple named data groups. Each group retains its own
UVW and field/source context and explicit alias relationships to other groups.

```rust
// Semantic sketch; private constructors validate dimensions and scope.
enum SampleTopology<'a> {
    Irregular { samples: SampleCoordinates<'a> },
    TimeBaseline {
        times: EpochAxis<'a>,
        baselines: &'a [OrderedBaseline],
        occupancy: OccupancyView<'a>,
    },
}

struct ObservationBlock<'a> {
    context: BlockContext<'a>,
    topology: SampleTopology<'a>,
    coordinates: CoordinateViews<'a>,
    groups: &'a [DataGroupView<'a>],
    grouping: GroupedSamples<'a>, // optional ranges/index views, no payload copy
    update_maps: Option<RoleUpdateMaps<'a>>, // lends a checked map per native role
}
```

`Irregular` is a sample list with axis-labelled coordinates, usually one entry
per integration and ordered baseline. Each sample still carries a
channel-by-correlation array. Repeated baseline/time coordinates are distinct
acquisitions; equality of coordinates does not imply identity. Do not discard
duplicates or average them while opening. Variable row shapes are split into
compatible blocks. This does not impose the historical physical row order.

`TimeBaseline` factors coordinates and can carry large native v4 arrays without
replicating timestamps and antenna pairs for every slot. It is a coordinate
topology, not a separate element order or numerical kernel. A compatible
contiguous `[time, baseline, channel, correlation]` window binds as sample-major
with `sample = time_index * baseline_count + baseline_index`, without copying
values or materializing one coordinate header per slot. Check strides and
selection first: not every sliced/permuted window permits that view.
Dense-to-irregular packing is permissible
when requested and budgeted. Irregular-to-dense conversion is allowed only if
the destination can represent duplicates and holes under the declared policy;
otherwise it is rejected. The initial rectangular form has one slot per
time/baseline coordinate, so duplicate acquisitions require irregular form or
separate fragments with distinct identity.

Keep three different facts separate:

- **Selection:** which logical elements the request includes.
- **Acquisition occupancy:** known present, known absent, or unknown; may be a
  constant or a compact view, without requiring a byte for every element.
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

Expose typed views with checked extents, axis labels, element strides and
contiguity guarantees. The initial numerical layouts are a small closed set:

| Layout | Logical dimensions | Typical use |
| --- | --- | --- |
| Sample-major | `[sample, channel, correlation]` | Existing row-compatible bulk path, streaming application |
| Channel-major | `[channel, sample, correlation]` | Channel-local imaging and some solves |

These name element order as well as axes; a request also declares which strides
are acceptable. Arbitrary sparse gathers are represented by explicit index
views or packed by the backend, never disguised as a contiguous slice. Either
layout may retain `TimeBaseline` coordinates; a rank-four view is a compatible
factoring of its axes, not a v4-specific operator path. M1 must choose only the
stride/permutation forms real kernels need and match them once per block.
Arbitrary-rank tensors, negative
strides, and unrestricted permutations are not initial requirements. A kernel
that requires contiguous correlation vectors can state that as a hard
requirement. A fixed channel axis within a block does not imply uniform channel
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

Options mean a role was not requested; a missing required role is a bind error.
Native absence/undefined cells require their own validity representation, not
an empty slice that can be mistaken for a valid all-zero array. Data-group
associations bind semantic roles to variables, including aliasing; labels such
as raw/corrected/model are conveniences, not universally reserved variable
names. Legacy DATA/CORRECTED_DATA/MODEL_DATA map to explicit group definitions
with their shared flag/weight/geometry associations.

Match scalar kind and layout once, then invoke a typed loop. Persisted and
compute precision are separate. A caller can request exact/native values,
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

Reference-frame conversion and projected UVW belong to the scientific geometry
owner. Bind reusable transforms once at their valid scope; compute genuinely
time-dependent quantities at the required cadence. Do not reproduce that engine
inside both storage adapters.

### Flags and weights

`NativeFlagView` retains boolean or unsigned bitmask dtype/width, reason-bit
definitions, legacy row flags, and requested category flags. A separate
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

```rust
struct AccessRequest {
    selection: ObservationSelection,
    roles: RoleSelection,
    grouping: ScientificGrouping,
    layouts: AcceptedLayouts,
    precision: PrecisionRequest,
    traversal: TraversalRequirements,
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

Hard requirements include requested data/selection, semantics of solution bins,
ordering needed for an algorithm, allowed numeric conversions, accepted layouts
and stride constraints. Preferences include target aggregation size and locality
when scientifically interchangeable. Resource limits come from an admitted
budget, not an uncoordinated request to allocate memory. Bind returns the exact
realization or a typed error plus the limiting capability/minimum working set.

The binding summary includes physical read units, decode units, selected bytes,
and expected read/decode amplification for the requested channel windows and
number of traversals. Distinguish exact shape-based decode counts from estimates
of compressed bytes and cache reuse; measure both during execution. A full-band
compressed chunk cannot supply a narrow channel window at narrow-window decode
cost. A bound that cannot admit the minimum decode unit fails explicitly.

Compute-window constraints describe what a kernel can borrow or process at once.
Delivery limits bound the source's aggregate block and come from admission and
explicit algorithm requirements. A small compute window does not impose the
same limit on native I/O aggregation. The backend may deliver many such windows
in one block unless the algorithm explicitly requires otherwise.

Scientific groups are formed from declared semantic keys: for example a time
bin plus field and SPW-combination rules from calibration. They can span
partitions, blocks, physical files, and storage chunks. Time-bin boundaries and
reference epochs are explicit. The storage backend executes the requested
grouping; it does not decide which fields/SPWs a solver may combine.

The algorithm declares accepted group realizations. `Sequential` completes one
group at a time; `Concurrent` permits storage-order fragments for a bounded set
of live groups. The algorithm supplies an accounted state bound as a function
of actual group shape (baselines, channels, correlations and solver/window
state, including time-deduplication sets and robust statistics), not a universal
constant bytes-per-group. Admission includes the live states and maps.
Neither realization mandates a full-selection row index.
A single I/O block may lend ranges/index views to several groups without
re-reading or copying the payload. Binding chooses an accepted realization and
reports any index/reorder bytes and repeated physical reads.

`First/Middle/Last/Whole` describe each group's fragments, not global stream
order. A control event can end a group after its last payload block, including
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
        ReadEvent::Block(block) => process(block.borrow_window(window)?),
        ReadEvent::GroupEnd(end) => finish_group(end)?,
    }
} // each borrow ends before refill/reuse
```

Buffers are caller/session owned with reusable capacity. A block borrows their
arrays and retained context; safe Rust prevents refill or mutation while views
remain borrowed. Pipelining uses a bounded admitted pool of buffers and transfers
exclusive ownership; no `Arc` clone is permission to retain unlimited blocks.
Coordinate/group catalogue references can be shared without cloning payloads.

### Independent work-unit lifetimes

The loop above illustrates synchronous use, not a requirement that all consumers
finish a block together. A **work unit** executes one or more compute windows on
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
Ownership operations remain at block/work-unit granularity, outside scalar loops.

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
do not solve lifetime skew by allocating an unbounded tail of blocks.

For cube CLEAN, visibility blocks belong to gridding/prediction passes. The
longer-lived model, residual, mask and solver state belong to the imaging owners;
a channel's minor cycle must not retain an input visibility block merely because
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
at meaningful block/phase boundaries, not every scalar.

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
budget allows. A larger I/O block must not increase the number of repeated
per-sample adapter operations or metadata allocations.

## 6. Source and sink lifecycle

The common lifecycle is `open/create → inspect/select → bind → fill/patch/append
→ finish`. Source/sink traits describe this behavior; `casa-ms` chooses the
concrete backend and uses enum or static dispatch at the binding/block boundary.
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
A block address cannot be used after closing/reopening its
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
`append`/`write_region` consume the same block vocabulary as reads, with explicit
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
   counts/frames. Keep blocks homogeneous while imaging visits both partitions
   and a solve may group their fragments. Do not pad them into one invented
   frequency axis or equate a field with antenna pointing.
3. **Calibrate then write:** gather channels in requested order, solve from
   fragments, apply gains, patch corrected data and selected flag bits. Preserve
   untouched channels, native weights/flag categories, group aliases and actual
   storage bindings. Reject writing averaged solve inputs back as if invertible.
4. **Large v4 block:** decode multiple chunks into one factored block; borrow
   many smaller compute windows. Peak memory counts both decoded and packed
   storage where they overlap. Demonstrate fewer I/O/decode calls or reduced
   elapsed time without inflating kernel scratch.
5. **Failure:** cancel between two chunk/tile flushes. Release ownership, report
   partial completion and do not claim rollback or successful group completion.
   Also fail inside v4 object replacement and prove the old-or-new object rule,
   including unselected regions. New staged output is never published as complete.

These become executable contract laws in M1. They are not substitutes for the
representative science, performance and independent interoperability gates.
