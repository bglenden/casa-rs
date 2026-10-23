# Intermediate observation contracts

Status: proposed contract revision 0; subject to D1–D4 review and M1 trials
Truth class: design proposal
Date: 2026-10-23

This is the interface between persistent observations and numerical consumers.
The names and Rust sketches are provisional; the semantics below are the design
to review. M1 must turn the sketches into a minimal compilable API with worked
imaging, solve, and update examples before broad implementation starts.

## 1. Contract rules

| Rule | Required behavior |
| --- | --- |
| C1: source independence | No physical row IDs, Zarr keys, storage managers, or MS-version branches in scientific kernels. |
| C2: faithful values | Explicit units/frames/precision, native flag and weight semantics, full data-group association, ordered baselines. No silent scientific transform. |
| C3: layout independence | Separate sample topology, memory layout, scientific grouping, and storage chunking. |
| C4: bounded ownership | Every live payload, index, decode buffer, cache, packed copy and queue is admitted; borrowed views cannot outlive/refill their storage. |
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
    group_fragment: GroupFragment,
    update_map: Option<UpdateMapRef<'a>>,
}
```

`Irregular` is a sample list with axis-labelled coordinates, usually one entry
per integration and ordered baseline. Each sample still carries a
channel-by-correlation array. Repeated baseline/time coordinates are distinct
acquisitions; equality of coordinates does not imply identity. Do not discard
duplicates or average them while opening. Variable row shapes are split into
compatible blocks. This does not impose the historical physical row order.

`TimeBaseline` factors coordinates and can carry large native v4 arrays without
replicating timestamps and antenna pairs for every slot. A view may stride over
that array without flattening it. Dense-to-irregular packing is permissible
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
| Factored | `[time, baseline, channel, correlation]` with an explicitly supported axis permutation | Native MSv4 aggregation and time/baseline solves |

These name element order as well as axes; a request also declares which strides
are acceptable. Arbitrary sparse gathers are represented by explicit index
views or packed by the backend, never disguised as a contiguous slice. M1 must
choose a minimal set of useful factored permutations from real kernels and
prove they can be matched once per block. Arbitrary-rank tensors, negative
strides, and unrestricted permutations are not initial requirements. A kernel
that requires contiguous correlation vectors can state that as a hard
requirement. A fixed channel axis within a block does not imply uniform channel
spacing, ascending order, or a single frequency frame across the observation.

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

`WeightView` represents native precision, units/meaning and broadcast axes:
for example per-sample/per-correlation WEIGHT versus channel-dependent
WEIGHT_SPECTRUM. Preserve the source array without mandatory expansion. The
request names the weight role and fallback rule for absent or undefined cells;
record which rule was bound. SIGMA and SIGMA_SPECTRUM are not silently equated
with weights; conversion is explicit. Nonfinite or negative weights remain
detectable and follow the algorithm's established science policy. The backend
does not invent unit weights or turn unusable measurements into zero data.

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
```

Hard requirements include requested data/selection, semantics of solution bins,
ordering needed for an algorithm, allowed numeric conversions, accepted layouts
and stride constraints. Preferences include target aggregation size and locality
when scientifically interchangeable. Resource limits come from an admitted
budget, not an uncoordinated request to allocate memory. Bind returns the exact
realization or a typed error plus the limiting capability/minimum working set.

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

The initial grouped stream completes one group before starting the next, with
`First/Middle/Last` fragments, or emits `Whole`. This bounds active accumulator
count. Backends may need an address index/reorder pass; its construction,
storage, and lifetime are included in admission and diagnostics. A full
observation-sized resident index is not a free planning cost. A group ending
after filtering every element produces an explicit empty-group event when the
algorithm needs a no-solution result. Cancellation/error never masquerades as
`Last` or successful end-of-stream.

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
while let Some(block) = cursor.fill_next(&mut buffers)? {
    process(block.borrow_window(window)?);
} // each borrow ends before refill/reuse
```

Buffers are caller/session owned with reusable capacity. A block borrows their
arrays and retained context; safe Rust prevents refill or mutation while views
remain borrowed. Pipelining uses a bounded admitted pool of buffers and transfers
exclusive ownership; no `Arc` clone is permission to retain unlimited blocks.
Coordinate/group catalogue references can be shared without cloning payloads.

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
other readers remain. A mixed read/update session owns the required exclusive
access and explicit read/write roles. Original-input semantics require disjoint
underlying read/write variables or element regions after resolving data-group
aliases, or a proven read-before-write traversal covering every read of an
element. Different role/group names alone do not establish disjointness.
Reject overlapping or revisit patterns that cannot meet that rule;
do not imply snapshot semantics or retain a hidden whole-array/replay copy.
A block address cannot be used after closing/reopening its
source, even at the same path. Normal Rust ownership/session membership checks
provide this protection, without hashes or persistent identity markers.

## 7. Updates and creation

An `UpdateMapRef` refers to a session-owned inverse map from selected logical
elements to actual source cells/chunk coordinates. A permutation or selection
composes that mapping. A source may use implicit rectangular strides or compact
row maps; it need not allocate an address for each visibility. M1 must prove
borrowed-map and bounded queued-write lifetimes without retaining read buffers
until the whole pass finishes.

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
    scope: MutationScope, // NamedVariableAliases or explicitly detached group
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

Perform a **structural preflight** before mutation: required roles/managers,
profile/dtype representability, alias scope, locks/concurrency capability, buffer
budget and output metadata support. Do not scan/hash the payload just to grant
permission. Data-dependent errors may still occur while streaming. In-place
failure may leave partial changes, consistent with CASA's normal semantics;
return the failure and completed-scope information available without a durable
journal. There is no rollback copy, generation marker, or whole-store atomicity
claim. Flush/finish errors are reported; dropping an unfinished session is not
success.

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
   partial failure, preserve unselected regions, and do not claim rollback or
   successful group completion. New staged output is never published as complete.

These become executable contract laws in M1. They are not substitutes for the
representative science, performance and independent interoperability gates.
