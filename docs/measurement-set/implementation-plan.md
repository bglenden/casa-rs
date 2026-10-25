# MeasurementSet implementation plan

Status: revision 5; M0/M1 trial authorized, production migration awaits acceptance
Truth class: design proposal
Date: 2026-10-24
Design inputs: [architecture](architecture.md), [draft contracts](contracts.md)

## 1. Delivery strategy

The owner authorized proceeding with the trial on 2026-10-24. The
[trial handoff](trial-handoff.md) defines how another agent starts M0/M1 and
submits checkpoints to the design reviewer. D1–D6 are its working direction;
concrete interfaces remain subject to trial evidence and review. The initial
assignment is the two-example exploration below, followed by design iteration;
the full M1 checklist is the eventual gate, not the first coding assignment.

Prove the shared representation before distributing broad implementation.
Use competent, lower-cost agents for bounded implementation packets with
accepted contracts and concrete tests. Use more capable independent agents for
contract design, ambiguous science/format decisions, milestone reviews, and
regression diagnosis. The implementer does not approve its own milestone.

Do not preserve existing Rust interfaces with permanent compatibility layers.
Migrate each production consumer to its new owner and remove the old domain
route in the same accepted change. Generic casacore table APIs remain useful
and are not part of that deletion requirement. Keep baseline binaries and
test-only comparison harnesses instead of two production implementations.

The dependency order is:

```text
M0: inventory, profiles, baseline
             |
M1: executable contract trial (v2 + real v4, imaging + solve + update)
             |
M2: v2 production cutover slices --- M3: v4 production backend
             |                           |
             +------ M4: remaining consumers/producers
                             |
                M5: integrated acceptance and cleanup
```

M2 and M3 can proceed in parallel only after M1 approval. Do not let many agents
invent slightly different versions of the common batch contract. M4 packets
may begin when their required backend operations are accepted. Per the owner's
2026-10-23 decision, MSv3 is deferred until a concrete dataset/producer requires
it. It is not a completion dependency for this MSv2 + MSv4 programme.

### Early exploration: two worked examples

Begin with the [handoff's two examples](trial-handoff.md#first-assignment-two-examples-then-adjust):
a few small dirty-image channel planes and a gain solve with solution groups
crossing batch boundaries. Both use real v2/v4 inputs, the same common batch
types and existing production numerical consumers. Establish the affected
paths' fixtures, dependencies and baseline before modifying them. Review the
first working example, try the second, then revise the shared API with the
owner before expanding implementation. Alongside these examples, run the
[early MSv4 interoperability check](trial-handoff.md#early-msv4-interoperability-check):
Rust reads XRADIO-written data, and XRADIO independently opens, validates and
loads a fresh Rust-written dataset. Compare actual arrays and coordinate/group
semantics, not just schema validity or aggregates. This requires a narrow
known-shape Rust writer for the tested profile during the first pass.

The initial pass is intentionally partial. Beyond that narrow writer, it need
not complete general updates/creation, all-family Measures coverage, failure
injection or the full benchmark matrix. Record unsupported cases and missing
evidence rather than constructing
unexercised abstractions. Preserve all M0/M1 acceptance requirements below;
neither milestone is complete merely because the examples and interoperability
probe run. After the learning review, complete M0 and the remaining M1 packets before stabilization
or production migration.

## 2. M0 — establish facts and immutable baselines

**Owner:** integration lead with a format reviewer and performance reviewer.
**Output:** linked work issues, exact support matrix, fixture recipes and
baseline evidence; no speculative mass refactor.

- Inventory callers by behavior, using the starting map below. Classify generic
  table tools, independent calibration solution tables, tests and oracle code
  separately from observation-domain bypasses that must migrate.
- Record the current commit/build, dependencies (including actual FFTW links),
  hardware, worker settings, selected inputs, cache states and accepted outputs.
  Keep a runnable baseline of current production routes before changing them.
  Historical #648/#659 results inform diagnosis; they do not replace a matched
  current baseline.
- Pin XRADIO commit, Python dependency lock, Zarr/profile and the complete Rust
  store/codec graph and features, including `zarrs_filesystem`. Audit per-key
  retention, file-handle caches, partial updates, concurrency and synchronization
  in that exact graph; a development-tree fix is not evidence for a release pin.
  Build small public/reproducible fixtures with the actual Python writer; inspect
  metadata and bytes with an independent reader. No assumption that a schema
  example is an interoperable file.
- Keep explicit profile detection so a v3-labelled or otherwise unsupported
  schema is not silently treated as v2. A detailed v3 producer audit is deferred
  with its implementation, not required to approve M0.
- Identify existing production managers, optional columns, mutation capabilities,
  selection/weight fallback policies, and expected error behavior to preserve.
  Include FLOAT_DATA, LAG_DATA, FLAG_CATEGORY and SIGMA_SPECTRUM in the audit.
  Expand the [conversion fidelity table](architecture.md#conversion-fidelity)
  to the complete accepted input schema; distinguish preserved effective values
  from lost native roles/provenance and define each rejection/loss policy.
- Inventory Measures types, coordinate/frame translation layers and existing
  prepared conversion fast paths. Pin scalar and ordered CASA-compatible
  behavior, provider/model state and scientific tolerances. Baseline both the
  scalar and any prepared production paths across all supported measure families,
  including directions, epochs, positions, baselines/UVW, spectral/radial-velocity/
  Doppler measures, magnetic vectors and units. Separate route binding, context
  preparation and value-dependent work; do not credit removal of repeated work
  that the current consumer already avoids. LSRK↔TOPO is one benchmark case.
- Define a **blocking** comparison harness for selected bulk reads, selected
  writes, representative production imaging/calibration, and flagdata tfcrop
  and rflag. Record peak live bytes and bytes per selected sample for flagging,
  including sample objects, copied groups, masks and touched-sample sets.
  Establish noise bands from the actual baseline and name critical workloads
  before optimizing.

**Gate:** stronger reviewers approve the inventory, claimed profiles, immutable
baseline, and acceptance cases. Missing data/oracles are reported as missing,
not green. Baseline results and exact rerun commands are attached to the issue
or PR. Heavy runs obey existing disk and resource limits; no new large data are
downloaded merely to approve this documentation proposal.

## 3. M1 — executable contract trial and first stability checkpoint

**Owner:** architecture lead; cheaper agents may implement isolated fixture or
adapter pieces against a reviewed sketch. Do not delegate unresolved core
semantics to implementation agents.

**Deliverables:** minimal `casa-observation` crate using authoritative Measures
types, a native Measures bulk-conversion API and proposed dependency-policy
update; rustdoc/examples; one reusable v2 read path and one real local MSv4
read/decode path; bounded addressed-write trial for each; production-kernel
integration probes and source-free creation, not a synthetic array API
demonstration alone. The same native Measures types/views must compile through
MS read, Measures conversion, calibration, imaging and creation without
conversion into another domain type. Rework Measures and remove duplicate
coordinate types; do not add a permanent translation layer. Proposed edges are
`casa-observation → casa-types` and `casa-imaging-model → {casa-observation,
casa-types}`; confirm the resulting graph and error/provider ownership before
parallel migration. The owner's refactoring authorization does not freeze the
specific API sketch or waive the milestone evidence.

Exercise all of the following before declaring the contract stable enough for
parallel migration:

1. The same imaging operator consumes compatible selections from irregular v2
   and factored v4 coordinates, with sample-major and channel-major layouts.
   Compare results and count copies, calls, allocations and peak memory.
   Use `ndarray` numerical views under thin domain types, as agreed with the
   owner. Prove compatible whole-array and sliced views borrow the existing
   allocation without copying,
   and reuse capacity across fills. Check fixed-rank access, admitted strides,
   read-only broadcasts and safe worker handoff with actual kernels. Compare
   with existing production paths; record each required pack/owned copy and
   peak backing-allocation retention before freezing owner/view types.
   Record each real consumer's access request and the adapter's bound realization;
   verify promised traversal/grouping/window requirements reach that consumer,
   with repeated-pass estimates and physical I/O costs identified explicitly.
   Prove that compatible native time/baseline axes bind as sample-major without
   a value transpose; measure O(T+B) axis metadata separately from unavoidable
   per-slot UVW/occupancy on few-channel and many-channel fixtures.
   Bind canonical Hz/m/rad/s and explicit compatible unit requests. Test no-op
   conversions without extra payload copies, scaled coordinate axes with unchanged
   scientific values, dimensional/range failures, and persistence in original
   units after selected writes. A Hz-only consumer must not accept a MHz view.
   Test declared Jy/mJy amplitude and inverse-variance weight/sigma consistency;
   unknown instrumental units must not acquire an invented flux calibration.
   Confirm unit changes do not silently change spectral frames or time scales,
   and supported epoch formats retain split precision under independent oracles.
   The `measures-bulk` packet below is a prerequisite for accepting this direct
   coordinate interface, not a later optional optimization.
2. Gain and bandpass accumulation consume split scientific groups, including
   multi-SPW rules, empty groups, reordered correlations and a group larger than
   the allowed resident data. Confirm grouping is independent of batch size.
   Add four interleaved SPWs and late out-of-order samples. A legal admitted
   concurrent realization reads the payload once without a full-selection row index;
   count delivered fragments/packing and all retained accumulator state too.
   Compare sequential realization's index/repeated-read costs. Validate time-bin
   origin and group closure across partitions; no eviction or early close.
   Split terminal fragments into compute windows and delay worker completion:
   `Last/Whole` must not finalize twice or early, and a delayed `GroupEnd` waits
   for prior work. Include an empty group and bounded per-batch address views.
   Solve, write a CASA-compatible caltable, close, permute catalogue/partition
   enumeration, reopen and apply; include overlapping antenna subsets, reordered
   SPWs and ambiguous names so namespace rebinding is tested, not inferred.
3. Flags-only read, undefined WEIGHT_SPECTRUM fallback, FLOAT_DATA where already
   supported, typed c32/c64 dispatch, f16 weight widening, bit flags/categories,
   row flags, and native weight broadcast all have explicit tests.
   Include a mixed auto/cross batch with all four polarization products: preserve
   real parallel-hand auto values in complex storage and nonzero imaginary XY/YX
   auto values without per-element dtype dispatch. Preserve supplied departures
   from ideal reality/Hermitian symmetry; no silent imaginary-part removal.
   Contrast supported real FLOAT_DATA without extending the initial v4 profile
   to SpectrumXds. Delivered payload views require no backend read/decode on
   element access; count those operations separately from consumer arithmetic.
4. Reorder/gather selected values and bulk-write them back through inverse maps.
   Include duplicate reads/conflicting writes, group aliases, unknown flag bits,
   narrow-channel updates, distinct UVW/field contexts, unselected sentinel data,
   and failure during a flush. Reduced data cannot retain writable provenance.
   Exercise reading and updating the same role, different roles aliasing one
   underlying variable, and a requested revisit: prove disjoint underlying
   regions or a legal read-before-write traversal, or typed rejection, with no
   hidden snapshot or whole-array cache.
   Add scalar WEIGHT=2 shared by two channels: changing only channel 0 must
   preserve channel 1 or be explicitly widened/materialized/rejected. Test
   per-cell fallback and simultaneous native row/spectral-weight patches.
   Complete physical-unit replacement reads zero old destination payload;
   partial bits, co-stored columns and supported shard subregions preserve untouched
   content and count metadata/index reads separately. Test final-pass MODEL_DATA
   creation/traversal with one planned mixed owner, plus conflicting sessions.
5. Sparse times/baselines, duplicate acquisitions, NaNs and v4 fill values do not
   collapse presence into flags. Ordered baselines/cross-hands, split time
   precision, time centroids, moving pointing and spectral frames survive.
   Convert a small sequential mosaic with repeated field visits, both with and
   without field partitioning; compare every selected/reordered sample's field
   and phase centre to the original MSv2. Add conflicting fields on different
   baselines at the same time, and distinct acquisitions colliding in one
   time/baseline cell: valid partitioning must preserve them or conversion must
   reject, never select the largest field ID or silently overwrite a sample.
6. A v4 visibility batch spans multiple chunks and feeds smaller borrowed kernel
   windows. The resource accounting includes decode/packing/index/write scratch,
   and an oversized physical working set fails before unsafe allocation.
   Exercise multiple work units and cooperating worker groups with deliberately
   unequal completion times. With independently owned buffers and available
   admission, fast work retires/reuses buffers and further work progresses while
   one unit is held. Repeat with windows sharing an allocation: it must remain
   pinned until the last user finishes, without refill or an unbounded queue.
   Include bounded packing, private/shared scratch, ordered-output backpressure,
   and cancellation with work still in flight. Count unique backing allocations,
   pinned bytes, copies and stalls, and demonstrate safe cross-worker handoff.
   A cube integration probe separates streamed visibility lifetimes from uneven
   channel minor-cycle work, retaining required image state and scientific joins
   without retaining visibilities throughout CLEAN or duplicating the whole cube.
   Scan increasing present/missing chunk-key prefixes and repeated major-cycle
   passes at fixed budgets: live store/cache/registry state must plateau.
   On the actual compressed multichunk path, verify W1 has no hidden parallel
   decode and W4 uses only the admitted team, counting threads and simultaneous
   encoded/decoded/codec buffers. No reliance on a concurrency target alone.
7. Actual XRADIO reads Rust-written trial data and Rust reads XRADIO-written
   data. Independently test every minimum case in the conversion fidelity table
   using the original v2 data, XRADIO output, and Rust conversion or rejection.
   Keep FLAG_ROW, INTERVAL versus EXPOSURE, duplicate acquisitions, weights and
   SIGMA/category losses explicit. The converter is an oracle for its actual
   output conventions, not sole authority for CASA science or recoverability.
8. A real simulation/import producer seam constructs a validated catalogue and
   batches without opening an input MS, copying retained backend metadata or
   acquiring inverse source maps. Create fresh v2 and known-shape v4 outputs;
   independently reopen with CASA/casacore and XRADIO. Exercise multi-SPW/field
   metadata, target-region addressing, recyclable producer/writer buffers,
   bounded backpressure and failure before publication. This is a narrow seam
   trial, not the entire simulator/import migration.
9. Inject short writes/ENOSPC and terminate during local v4 object replacement.
   Independently reopen and prove the previous published state (including an
   absent fill key) or new complete chunk/supported shard, including unselected
   sentinels and failed first materialization. Unsupported sharding is rejected
   before mutation. Test failure between related objects separately: it is
   partial operation completion, not a transaction. Account encoded buffers and
   temporary disk; measure synchronization cost and define finish durability
   separately from process-failure atomicity. Do not claim power-loss guarantees
   from a process-kill test.

### M1 Measures bulk-conversion trial

Implement the [shared Measures contract](contracts.md#shared-measures-types-and-bulk-conversion)
across the Measures system. M0 supplies the complete family/route inventory;
M1 requires representative executable bulk cases for every currently supported
family in the contract's table, including directions, epochs, positions,
baselines/UVW, frequency, radial velocity, Doppler, magnetic vectors and units.
Frequency plus one other family does not satisfy this gate. Exercise real
consumers of spectral and non-spectral transformations. The matrix is not
exhaustive: newly added families must satisfy the same bulk contract. Existing
route/science coverage remains binding; unsupported routes stay explicit.
The trial must demonstrate all of the following:

- Native borrowed frequency/direction/epoch/position views and reference enums
  pass directly from the MS adapter to Measures and consumers. Validate shared
  and indexed metadata without per-value measure objects or loss of split
  epoch precision. Include vector-coordinate and scalar-array paths, affine
  and nonlinear conversions, and homogeneous as well as varying contexts.
- Reuse prepared state at its valid scope for each family. Test directions over
  multiple epochs/sites, epoch arrays crossing leap-second/EOP boundaries,
  Cartesian/geodetic position arrays, baseline/UVW arrays with changing phase
  centres, Doppler/radial-velocity convention and reference changes, magnetic
  vector frames, and compatible unit arrays. Preserve necessary per-value
  nonlinear/iterative calculations; do not assume all transforms are scales.
- As one spectral example, bind LSRK↔TOPO once, prepare fixed-context coefficients once, apply to many
  frequencies in a supplied reusable buffer. Cover identity and explicit unit
  changes, channel centres/edges, noncontiguous admitted views, moving epochs,
  mosaics with changing direction, and a fixed destination image epoch distinct
  from source epochs. Equal reference enums with different relevant contexts
  must not silently take the identity path. Error tests cover missing context,
  unsupported references, invalid values and declared partial-output behavior.
- Compare fixed-context and ordered bulk results to the appropriate scalar
  policy and independent CASA/casacore oracle using identical provider data and
  models. Preserve existing accepted interpolation behavior and family-specific
  angular, time, distance, frequency, velocity and field-strength tolerances,
  dimensionless convention checks, split precision and affected science
  outputs. Exercise both sides of interpolation refresh/leap-second boundaries
  and explicit provider/context changes. Do not relax established narrow-channel
  or coordinate checks on the strength of an image-only NRMS comparison.
- Split the same ordered input across batches/windows and workers with delayed
  completion. Ordered context preparation keeps its state and anchors; prepared
  immutable transforms can be shared while work retires independently. Prove
  bounded scratch/cache/queue residency, with no global hot-loop lock or hidden
  worker pool. Frame/reference/context changes prevent stale coefficient reuse.
- Measure W1 and admitted parallel execution over small and large scalar/vector arrays
  and fixed/varying contexts. Report binding and preparation costs separately
  from apply throughput, provider calls, route searches, allocations/copies,
  synchronization and peak retained bytes. Work scales with required contexts
  plus required value-dependent work, without repeating invariant preparation
  per element. Include current scalar, existing prepared production where
  available, and new bulk paths for each family,
  using equal scientific policies and work. Show a useful gain where setup
  dominates and preserve the existing fast path within measured noise; quantify
  the actual consumer effect separately from microbenchmark gains. A loop over
  scalar conversions does not satisfy the bulk acceptance requirement.

### M1 exit checklist and packet ownership

These are planned fixture/packet names, not commands claimed to exist today.
Before assignment, the M0/M1 lead supplies exact fixture recipes, runnable test
commands and result locations in each linked issue. A packet's implementer may
be a cheaper agent after its contract is reviewed; the independent reviewer is
a stronger agent/person. The lead integrates shared API decisions.

| Packet / fixture family | Accountable owner | Independent gate |
| --- | --- | --- |
| `layout-precision` | Contract + imaging owners | One raw primitive type, sample/channel layouts, factorable axes, guaranteed/requested units and inverse write scaling, actual operator science and input narrowing. |
| `measures-bulk` | Measures + contract + imaging/calibration owners | System-wide family coverage, direct native views, consolidation of duplicate types/routes, prepare/apply and necessary value-dependent work, ordered interpolation and independent workers, family-specific scalar/CASA science and performance baselines. |
| `group-lifecycle` | Calibration owner | Interleaved SPWs/late samples, admitted concurrent state, proven closure, sequential comparison, solve→caltable→reopen→apply. |
| `work-unit-lifetimes` | Contract + runtime + imaging owners | Unequal worker/group completion, safe handoff and reuse, shared-allocation pinning, bounded copies/queues/cancellation, cube visibility versus minor-cycle state. |
| `native-role-updates` | Storage + calibration owners | Broadcast/group aliases, undefined fallback, full/partial unit counts, bit semantics, mixed-owner MODEL_DATA and conflict rejection. |
| `conversion-fidelity` | Format owner | Each audited mapping/loss, original/converter/Rust/reverse-reader evidence, no invented repair of lost information. |
| `source-free-create` | Simulation/import + storage owners | No input MS, standard metadata, target extents, buffer recycling/backpressure and publication failure. |
| `store-retention-executor` | Storage + runtime owners | Locked dependency graph, present/missing-key plateau and repeated passes, W1/W4 thread/buffer counts. |
| `object-failure` | Storage owner | Mid-replacement fault/kill, independent reopen, previous state (including absence) or complete new object, temporary space and finish/durability cost. |
| `production-access-cost` | Imaging/calibration/performance owners | Named MFS/cube/bandpass rows and both chunk geometries; matched within-v4 and v2 evidence, no count-only acceptance. |

Every row requires passing evidence for M1 acceptance. A missing prerequisite
must be documented and keeps M1 blocked; recording it is not acceptance.
No checklist entry is satisfied by source inspection alone.

Review the contract as code: lifetime behavior, allowed axis permutations,
metadata dependence, error cases, array layout, and mutation scope. Publish the
smallest worked Rust examples for imaging, calibration and selected update.
Measure early overhead against the M0 path before investing in broad migration.
These trials need not satisfy the entire release matrix yet.

**Gate:** independent contract + science/format reviews, first performance
comparison, and owner acceptance of the resulting D1–D6 contract revision and
any proposed ADR changes. Record an
accepted contract revision. All blocking findings resolved before M2/M3
parallel implementation. No claim of production v4 support at this checkpoint.

## 4. M2 — production MSv2 adapter and first consumers

**Packets:** (a) catalogue/selection and read binding, (b) bounded update/create,
(c) imaging source integration, (d) gain/bandpass/applycal integration. Sequence
shared facade changes; parallelize only disjoint consumer edits.

Preserve storage-manager bindings, endian behavior, variable cell shapes,
undefined cells, narrow selections, coordinate evaluation and native write
semantics. Reuse current typed bulk fast paths rather than rebuild them through
generic per-cell values. Retain the existing byte-budgeted source plan and
selected-channel staging/amplification model. Generalize its delivery unit from
MAIN rows to topology slots and physical decode units; do not rebuild resource
admission merely because the current result is named `rows_per_block`.
Keep the efficient v2 sample-major realization.

Move imaging's source preparation and operator windows onto the shared batches
within the existing single pass/worker/resource architecture. Move calibration
away from MAIN-row assumptions and whole-selection/group materialization where
the accepted solver contract permits fragment accumulation. Its solution-table
writer remains a separate persistence concern; do not break CASA caltables.

**Gate:** contract laws, affected T0/T1/T1.5 and existing interop tests, new
blocking v2 comparison against M0, and stronger review of persistence plus the
executing hot loop. Preserve both single-worker performance and parallel
behavior. Remove migrated production routes in the same cutover PR. A failing
performance comparison blocks cutover even if correctness passes.

## 5. M3 — production native MSv4 backend

**Packets:** (a) profile validation/catalogue/groups, (b) typed array access and
layout realization, (c) creation/update and metadata retention, (d) independent
interop/fixture tests. Separate metadata and payload work only once shared
structures are accepted; keep one owner for structural writes and aliases.

Implement the accepted local interferometric `VisibilityXds` schema
4.0.1/Zarr-v3 profile using pinned `zarrs`.
Test explicit dtypes/codecs, endian/fill/string conventions, dimension names,
coordinates, nested field/source/pointing metadata, named groups and aliases.
Preserve unrelated arrays/extensions during selective mutation. Reject unknown
required schema features and unsupported codecs before dependent work where
metadata permits; report data-dependent failures honestly when discovered.

Use bounded chunk aggregation, direct fill when available, one update owner per
physical object, and caller-admitted concurrency. Do not add Dask, Python, a
second runtime, private MS identity metadata, or a visibility replay cache.
Known-shape creation and selected updates are required; resizing/appending is
advertised only for implemented, tested capabilities. Reuse the common consumer
path already exercised in M2; no v4-specific imaging or calibration algorithm.

**Gate:** bidirectional XRADIO/Rust read/create/update checks, cross-format science
through actual imaging and calibration, admitted-memory laws, no-Python runtime
execution, and measured aggregation benefit with no statistically supported
production-task regression. Compare a reasonable accepted smaller-aggregation
realization against the larger candidate on the **same v4 store**, holding chunk
geometry, selected roles/elements, precision, workers, admitted memory budget,
cache state and
scientific work fixed. Require a real mechanism benefit and no significant task
slowdown under M0's noise method, unless the owner explicitly accepts a tradeoff.
Fewer calls alone cannot pass a slower task. An unchanged task time with a
measured component benefit is valid; do not claim an end-to-end speedup from it.

The new harness owns these mandatory rows; M0 chooses exact existing scientific
fixtures and records their recipes/parameters before assignment:

| Workload row | Store variants | Required comparisons/counters |
| --- | --- | --- |
| MFS continuum | XRADIO-default full-frequency chunks; frequency-chunked equivalent | Matched smaller/larger v4 aggregation, science-equal v2 production reference, calls/read/decode/copy bytes, wall/CPU and pipeline stalls. |
| Restricted-wave cube, narrow and wider channel selections | Same two variants, with a budget requiring multiple plane waves | Per-wave and per-major-cycle encoded bytes, decoded bytes, repeat decodes and bytes per selected sample; image-state I/O and peak buffers; matched v2 tiles and within-v4 comparison. |
| Bandpass solve with interleaved SPWs | Same two variants, sequential/concurrent traversal where accepted | Group/index/state bytes, payload traversal and packing, solution parity, wall/CPU and within-v4 aggregation comparison. |
| Flags-only and full-/partial-unit updates | Relevant boolean/bitmask and tiled/chunked fixtures; supported sharding or typed rejection | No unrequested visibility payload, old-destination payload versus metadata/index reads, bounded coalescing, replacement/sync cost. |

Use W1 and admitted parallel runs, with cache conditions recorded. The cube row
also measures whether decode supplies the consumer fast enough; workers come
from existing admission, not a new source pool. Distinguish poor persistent
chunk geometry from interface overhead. A small test-only direct-native Zarr
reference can isolate the latter only when it returns the same selected typed
values/layout and performs equivalent required validity handling; it is not a
second production route. Cross-format timings alone cannot isolate that cost.
Record the first accepted v4 production revision as the baseline for M4/M5.
Neither this gate nor the matched v2 context requires v4 to beat v2 universally.

## 6. M4 — migrate remaining consumers and producers

Use this inventory as a starting point; M0 checks the current tree rather than
treating textual references as proof of production ownership.

| Area | Starting code | Migration/acceptance obligation |
| --- | --- | --- |
| Existing observation access | `casa-ms/src/visibility_buffer.rs`, `selected_observation_buffer.rs`, `selected_observation/`, `observation_owner/` | Consolidate into one common path; retain buffer reuse, selection and source ownership. |
| Imaging | `casa-imaging-application/src/imaging/source.rs`, `casa-imaging-runtime/src/source_access.rs`, `pass/`, `casa-imaging-operator/src/sample.rs` | Same operator and pass driver; source aggregation independent of compute windows; geometry remains scientific. |
| Calibration | `casa-calibration/src/{session,solve,plan,execute,gencal}.rs`, `solve/grouping.rs` | Solve/apply/metadata access uses observation contracts; separately preserve calibration-table outputs and CASA science. |
| Flagging/transforms | `casa-ms/src/{flagging,transform,selection,spectral_selection}.rs` | Bit/category/row flag semantics, averaging/output provenance, optional columns, selected updates/new outputs; tfcrop/rflag peak-memory and timing gates below. |
| Observation metadata/plots | `casa-ms/src/derived/`, `selected_observation/access/geometry.rs`, `ephemeris.rs`, `selected_pointing.rs`, `subtables/pointing.rs`, plot/listobs/msexplore paths | Catalogue/window access, native metadata, moving fields/pointing; remove MAIN/DDI assumptions at domain boundary. |
| Simulation | `casa-simulation/src/write/`, including `subtables.rs` and the admitted pipeline | Common creation batches, multi-SPW/mosaic metadata, prediction/write boundedness, CASA-paired outputs. |
| Import | `casa-vla` importer | Preserve supported variable shapes, column metadata and FLAG_CATEGORY; use common creation when output format is selected. |
| Task/Python surfaces | `casars`, `casars-frontend-services`, `casars-python` | Format detection and native dispatch, generated bindings/parameters where affected; low-level generic table tools stay explicit table tools. |

For each packet, name its currently reachable production entrypoint, destination,
old code to remove, operations/formats required, tests and benchmark rows. Existing
MSv2 features must keep working; a not-yet-implemented v4 feature returns typed
unavailable and remains outstanding in the programme's support matrix. Do not
mark a consumer migrated merely because it calls a new wrapper that still leaks
physical rows into the algorithm.

Flagging must remove full-selection `Vec<Sample>`, copied-group and touched-sample
growth from its migrated route, not wrap those allocations in a batch API.
Its plan declares algorithm-specific fitting/statistics windows, halos, passes,
extension state and mutation ordering. A sufficient-state O(groups) bound is
not assumed: tfcrop's group fit and extension can require resident arrays, and
rflag's local stencil does not eliminate global threshold/extension work.
Retain existing scientific windows; arbitrary I/O fragments must not become new
fit boundaries. Use an equivalent admitted multi-pass realization, an admitted
resident group, or explicit resource refusal; a changed science window requires
separate owner/science approval. No existing supported capability is silently
removed to pass the resource gate.

Statistics and all reads requiring original flags complete before overlapping
flag patches are published, using a bounded decision/write pass when needed.
Do not queue a whole group's sample-sized patches merely to delay writes.
Count peak live bytes as admitted payload/fit windows, masks/halos, aggregate
state and bounded update queues. Increase group count while holding maximum
scientific group/window shape fixed to catch hidden full-selection growth;
separately grow group shape and check predicted state growth or explicit
admission refusal. M4 compares tfcrop/rflag with M0 time/memory and CASA science at different
I/O fragment boundaries. Failed science or a critical performance regression
blocks migration even when the memory curve improves.

**Gate:** per-packet acceptance and deletion review; milestone review audits the
remaining observation bypasses and creation routes. Frontend signature/parameter
changes include generated catalogues, UniFFI regeneration where relevant, and
Python package tests. No computational work moves into Python.

## 7. Deferred extension — MSv3 profiles

This is not a current work packet or completion gate. If a concrete need arises,
first audit the actual schema/producer and approve a named profile. Reuse the
casacore persistence layer; implement explicit association mapping rather than
assuming DATA_DESC_ID and v2 weight layout. Share the native batch and mutation
contracts with v2/v4. Keep named producer dialects distinct.

**Future acceptance:** independent producer→Rust and Rust→reader checks for every
claimed operation/profile, selected update preservation, accepted science comparisons,
and performance relative to that profile's native access. Note-264-only schema
tests and Rust self-roundtrips are useful but insufficient. If a usable independent
writer/reader does not exist, obtain a reviewed oracle/fixture strategy and owner
decision; do not advertise complete v3 interoperability on prose alone.

## 8. M5 — programme acceptance

Complete the promised support matrix and consumer inventory. Run the affected
representative science and release interoperability/performance gates. Compare
final v2 production behavior to M0, accounting for equal scientific work; confirm
the v4 aggregation benefit still reaches the production route without a task
regression against M3's accepted v4 baseline. Audit imports,
runtime dependencies, resource ownership, and removal of displaced domain paths.

Publish user-facing format/capability documentation, explicit conversion limits,
creation examples, and source/sink rustdoc. Update current architecture and the
accepted decisions to the implemented reality. Stronger independent reviewers
review the integrated system, not just the sum of green component tests. Owner
approves completion only with no unresolved scope or acceptance reduction.

## 9. Evidence and executable gates

The baseline is `TESTING.md`, the
representative matrix (`resources/imaging-architecture/representative-science-matrix.json`),
and the performance skill (`.agents/skills/casa-rs-imaging-performance/SKILL.md`).
Use four independent evidence classes:

| Evidence | What must be proved |
| --- | --- |
| Contract laws | Selection/layout invariance, coordinate/flag/weight semantics, grouping fragments, inverse writes, alias scope, failure/ownership, and deterministic allocation/I/O counts. |
| Science | Production-path T0/T1, affected CASA-paired T1.5/T2 workloads: representative MFS, cube, mosaic, multi-SPW, full-Stokes/cross-hands, gain/bandpass/applycal and flag/transform outputs. Preserve established NRMS (typically ≤0.001), sum-weight and other product-specific limits, WCS/beam/flux/topology/validity checks. |
| Resources/performance | Same host, toolchain/build options, linked FFTW, data, selection, precision, workers and cache state; equal work, including major-cycle counts; wall and CPU beside bytes/calls/copies/allocations and measured peak memory versus admission. W1 as well as parallel; CPU/Metal where affected. |
| Persistent interoperability | Existing Rust/C++ RR/RC/CR/CC cases and actual CASA reopen for v2; pinned XRADIO↔Rust for v4; unselected and unknown-extension preservation on update. Future v3 claims require their own independent oracle. |

Do not substitute a one-million-element toy for the repository's representative
science requirements. Reuse unchanged accepted oracle outputs; repeat timings
when noise prevents a conclusion rather than prescribe an arbitrary count.
No statistically supported regression in the named critical v2 workloads or
the accepted v4 production/aggregation comparisons is accepted without an
explicit owner decision. An increase in steady-state
allocations/copies or read amplification requires an explicitly justified,
independently reviewed realization change with no critical-workload regression;
merely reporting an extra full-buffer copy does not satisfy acceptance.
Deterministic count laws
catch mechanisms; they do not prove elapsed-time speedup. No fixed universal
percentage tolerance is invented before M0 measures noise.

Use existing commands where applicable: `just quick`, `just arch-check`,
`scripts/check-imaging-dependencies.py`, touched-crate clippy with warnings
denied, affected focused tests, `scripts/test-python-package.sh` when its surface
changes, and milestone/release commands from `TESTING.md`. Each implementation
issue must copy **exact runnable commands and fixture locations** for its scope
before it is assigned. M0/M1 add focused new contract/interop/benchmark commands
with fail-on-missing-prerequisite acceptance mode; their names are not invented
here as if they already existed.

Existing performance scripts are insufficient by themselves: release performance
can skip when casacore is absent, and the generic `ms_perf_vs_cpp` cell benchmark
only warns about slow results. Setting `CASA_RS_ENFORCE_PERF=1` does not make
that particular benchmark a blocking selected-observation regression gate.
The new M0 comparison harness must explicitly fail on required regressions and
missing prerequisites. A skipped oracle or unsupported test host is **not** a
pass. Never claim performance or interoperability from docs checks.

Keep compact evidence in GitHub: commit/build and oracle lock, exact commands,
fixture origin/recipe/license, conditions, counts/timings and numeric summaries,
failures/skips and the reviewers' conclusion. Durable large artifacts may live
in the repository's established storage, but external reviewers need sufficient
public recipes and summaries to assess each claim. Tests may fingerprint inputs;
that does not authorize runtime payload hashes or source sidecars.

## 10. Review and change control

The owner has authorized M0/M1 trial implementation using D1–D6 as its working
direction. Updating text or passing documentation checks does not stabilize
the interfaces or authorize M2–M5 production migration. Keep the design and
trial PRs draft and the external discussion visible. M1 acceptance, subsequent
milestone authorization and permission to merge remain explicit later steps.

Every implementation packet/PR includes:

1. Work issue, accepted contract revision and prerequisite milestone links.
2. One outcome, owned modules/files, production entrypoint, replacement/deletion
   scope, and the specific contract rules C1–C7 that apply.
3. Permitted local choices (private helpers, equivalent loops) versus decisions
   requiring escalation (shape/precision/units, grouping, aliasing, provenance,
   persistent mapping, resource owner, new dependencies or changed science).
4. Exact tests/benchmark commands, prerequisites, expected comparisons and
   required evidence. A less capable agent should not have to invent its own
   acceptance definition.
5. Named independent reviewer role, findings/resolutions, and any open owner
   decision. No implementer may weaken a gate to close its own packet.

During M1, expect frequent small contract revisions backed by counterexamples
and trials. The architecture lead consolidates them before implementation fans
out. At M1 acceptance, record the first stable revision. During M2–M5, ordinary
private implementation changes need no design ceremony, but contract changes
require a focused GitHub proposal covering the old/new behavior, motivating
case, all affected readers/writers/consumers, persistent consequences, memory
and performance cost, and updated laws/examples. A stronger reviewer approves
semantic changes; the owner resolves scope, persistent-compatibility, major
architecture and performance tradeoffs. Update code and design together.

Do not equate stability with never correcting an error. Correctness discoveries
can reopen a decision at any stage, with affected packets paused and a concrete
migration plan. Later changes should become rarer because more invariants and
production evidence have been established, not because surprises are concealed.

Review at M1, M2, M3, M4 and M5 examines contracts plus actual executing
code/evidence. User-facing decision summaries should be brief; detailed findings
belong on the relevant GitHub lines. Fable and ChatGPT can independently review
the same PRs using [the source map](README.md#source-map-for-external-reviewers).
Keep design approval and code approval explicit, and follow the repository's
PR merge policy only after all required decisions and reviews are resolved.
