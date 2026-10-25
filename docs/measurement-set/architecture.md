# Native observation I/O for MSv2 and MSv4

Status: revision 5; working design authorized for M0/M1 trial, not frozen
Truth class: design proposal
Date: 2026-10-24
Companion documents: [contracts](contracts.md), [implementation plan](implementation-plan.md)

Revision 5 retains Pro and Fable's review changes and owner discussion of
independent lifetimes, guaranteed units, the `VisibilityBatch` name and explicit
access-pattern negotiation from processing planners to adapters. Native Measures
types and bulk conversion are now owner requirements; this changes the proposed
type ownership and adds a prerequisite Measures trial to M1.
The owner also agrees to `ndarray` numerical arrays/views under thin domain
types; concrete ownership APIs remain subject to M1 evidence and review.
On 2026-10-24 the owner agreed to proceed with the [M0/M1 trial](trial-handoff.md).
The detailed APIs remain open to trial evidence and owner iteration; interface
stabilization and broad production migration follow the M1 acceptance gate.

## 1. Outcome and boundaries

Imaging, calibration, flagging, transforms, and simulation exchange the same
scientific observation types regardless of persistent MS format. An algorithm
specifies the information, grouping, ordering, precision, and memory layout it
needs. A source binds those requirements to the storage implementation and
returns bounded batches. A sink accepts bounded creation batches or addressed
updates. Only opening, creation, capabilities, and diagnostics expose storage
profiles; scientific inner loops do not branch on MS versions.

The processing and I/O paths run in native Rust without Python, Xarray, Dask,
CASA, or casacore C++ at runtime. Commodity codec libraries may contain native
C; this proposal does not promise an all-Rust transitive dependency graph.
Python/XRADIO and CASA/casacore remain independent interoperability oracles and
optional surrounding tools.

Internal API compatibility is not a goal. Delete displaced routes as each
consumer moves. Existing persistent MSv2 interoperability and performance are
goals; a cleaner API is not permission to regress them. MSv4 must be able to
benefit from large native array reads, not inherit a row-count ceiling from the
old implementation. These are measured requirements, not assertions of free
abstraction or universal zero-copy.

XRADIO image persistence, new calibration-table formats, distributed scheduling,
and general Xarray/Dask emulation are outside this programme. CASA image output
remains usable with MSv4 input. Shared Zarr transport/codec code may later serve
an image backend; image coordinates, masks, beams, and products retain their
existing owners.

## 2. The important distinctions

| Concept | Meaning and owner |
| --- | --- |
| Persistent partition | A compatible spectral/polarization and observation context; backend discovers and maps it. Not necessarily one field or pointing. |
| Data group | A named association of visibility, flags, weights, UVW, and field/source context. Variables can be shared between groups. |
| Scientific group | Samples an algorithm treats together, such as a calibration solution interval. May span batches and partitions. |
| Storage chunk/tile | Physical decode/update unit chosen at creation or already present on disk. |
| Visibility batch | Bounded aggregation delivered by a source; may contain many physical chunks. |
| Compute window | A borrowed subset or explicitly packed projection of a batch for a kernel. |
| Work unit | One or more windows executed by a worker or cooperating workers on the existing admitted team. Owns or borrows buffers until its actual completion; distinct from a scientific group. |

Use `VisibilityBatch` for the shared intermediate. Avoid the observatory meaning
of observation/scheduling blocks, and reserve chunk/tile for physical storage.
The name remains provisional; the crate still describes complete observations.

An MSv2 MAIN row contains a channel-by-correlation visibility matrix for an
integration/baseline/data-description context, not one polarization scalar
spectrum. MSv3 beta changes schema and column associations; it is not merely
another label for today's MSv2 files. MSv4's principal visibility variable is
`[time, baseline_id, frequency, polarization]` within a dataset; a Processing
Set collects datasets. In the pinned schema, `field_name` is an auxiliary
coordinate indexed by time, not another dimension of the visibility array.
Sequential mosaic fields can therefore vary along time within a compatible
partition: at each integration the coordinate identifies that integration's
field. This does **not** require all fields of a mosaic to share one hypercube.
XRADIO's converter permits additional `FIELD_ID` partitioning, yielding separate
datasets per field and other partition keys. Its default extra partition scheme
is empty and is explicitly recommended for rapidly changing phase centres in
on-the-fly mosaics; mandatory partition keys still apply.
Different spectral windows generally occupy different partitions, including
when continuum and line data are processed together.

The antenna beam-pointing history is separately associated by time and antenna
in `pointing_xds`. It is not the same as the correlated field/phase centre.
See the pinned [visibility dimensions](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L1020),
[time-indexed field coordinate](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L1734),
[optional field partitioning](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/convert_msv2_to_processing_set.py#L97),
and [mandatory keys](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/partition_queries.py#L39).

The shared interface therefore cannot use a casacore row number, DATA_DESC_ID,
one field, or one spectral window for the whole observation as its universal
identity. It also cannot assume every observation is a complete time–baseline
rectangle. See [topology and identity](contracts.md#2-topology-and-identity).

### Converter evidence for mosaics

This behavior is implemented in XRADIO's
[`convert_msv2_to_processing_set`](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/convert_msv2_to_processing_set.py#L67),
not just permitted by its schema. The
[conversion code](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/conversion.py#L2080)
places MAIN `FIELD_ID` values into a time-by-baseline array, reduces it to one
field ID per time, and assigns `field_name=("time", field_names)`. The
[field/source helper](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/create_field_and_source_xds.py#L792)
loads the distinct fields, appends the original field ID to each name to make
names unique, and looks up the time-ordered names, including repeated visits.
Ordinary phase-centre directions live in the field/source dataset keyed by
field name; ephemeris fields have additional time-dependent coordinates.

The pinned upstream tests include conversions of real mosaic datasets:

| Fixture | What the test exercises |
| --- | --- |
| VLA `SNR_G55_10s.split.ms` | [`test_VLA`](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/tests/stakeholder/test_measure_set_stakeholder.py#L753) uses the [default test schemes](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/tests/stakeholder/test_measure_set_stakeholder.py#L336), both `[]` and `["FIELD_ID"]`. |
| ALMA solar mosaic `ALMA_uid___A002_X1003af4_X75a3.split.avg.ms` | [`test_alma_ephemeris_mosaic`](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/tests/stakeholder/test_measure_set_stakeholder.py#L597) converts with `[]`, selects a target SPW/line/intent, requires one resulting dataset, and checks ephemeris variables. |
| VLASS `VLASS3.2.sb45755730.eb46170641.60480.16266136574.split.v6.ms` | [`test_vlass`](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/tests/stakeholder/test_measure_set_stakeholder.py#L697) uses `[]` and explicitly avoids field partitioning because of partition proliferation. |

The shared test helper writes and reopens Zarr, checks schema and weighted
visibility aggregates. These are source-inspected upstream tests, **not tests
run for this proposal**, nor a proof of every original row's field association.

The reduction above is specifically `np.max(field_id, axis=1)`, with `-42` for
missing baselines. It assumes all present baselines at a given time share a
field; that operation does not validate the assumption. If populated baselines
have different field IDs, the reduction chooses the largest, losing their
distinction. Proposed casa-rs conversion must validate agreement within each
destination time slice, partition compatibly or reject. It must also preserve
distinct acquisitions that would otherwise collide in a time/baseline cell.
The common intermediate remains able to express sample-dependent fields.

## 3. Proposed ownership

```text
imaging operator/runtime   calibration   flagging/transforms   simulation
              \               |                |                /
               casa-observation: scientific batch/access contracts
                                  |
                       casa-ms: open/create/bind
                      /                         \
          casacore MSv2 adapter           XRADIO MSv4 adapter
                 |                            |
             casa-tables                    zarrs
```

The diagram shows the participating owners, not a dependency of the contract
crate on its implementations. The algorithm planner sends an `AccessRequest`
downward through the common interface; the adapter returns its bound realization
and streams `VisibilityBatch` views upward. Selection, traversal/order, grouping,
windows/overlap, layout, units and revisits travel in the request. Only the
planner negotiates I/O; numerical inner loops consume the resulting views.
The proposed dependency direction is:

| Owner | Responsibilities | Dependencies and exclusions |
| --- | --- | --- |
| `casa-types::measures` / `quanta` | Canonical coordinate/reference/unit types, scalar and borrowed bulk views, prepared conversion routes and kernels | Existing numerical foundations and injected Measures provider; no dependency on MS, observation contracts, imaging, storage discovery or task execution. Rework the Measures APIs as needed. |
| New `casa-observation`, policy layer `science` | Observation catalogue identities, ordered antenna/feed pairs, topology, payload/coordinate association, batches, access requirements and source/sink contracts | Uses `casa-types` Measures/quanta types directly; no duplicate measure vocabulary, filesystem, task runtime or independent frame-conversion engine. |
| `casa-imaging-model`, policy layer `science` | Compiled imaging geometry and scientific problem definitions | Proposed dependencies on `casa-observation` and `casa-types` for their shared contracts/Measures types; no dependency on storage, execution or a device. |
| `casa-ms` | Dataset lifecycle, catalogue access, backend dispatch, source binding, bounded fill/update/create adapters | `casa-observation`, existing storage/measures facilities, and pinned `zarrs`. Initial adapters can be modules; no new storage crate is required merely for symmetry. |
| `casa-tables` | Existing generic casacore table and storage-manager machinery | Retains its table API and persistent compatibility. It does not depend on observation or imaging contracts. |
| Imaging/calibration and other domain owners | Request layouts, evaluate geometry, solve/project/reduce, apply scientific transforms | Consume `casa-observation`; no table/Zarr addressing in their numerical loops. |
| Existing execution owners | Resource admission, worker teams, cancellation, pass lifecycle | Continue to govern I/O/decode/packing work; no independent backend worker pool or scheduler. |

The owner requires the MS interfaces to use casa-rs Measures types directly,
including their reference frames, and authorizes substantial Measures API
reworking for natural bulk operation. This supersedes the earlier proposal to
place independent coordinate primitives in a dependency-free `casa-observation`.
The preferred implementation home is the existing `casa-types` Measures/quanta
foundation, extended with native array views and prepared bulk conversions.
`casa-observation` may re-export those exact types; it does not define another
`Epoch`, frequency-frame enum or direction type that needs conversion to them.
Imaging's duplicate raw coordinate types are consolidated into Measures while
imaging retains its compiled projections and geometry. Preserve split epoch
precision and distinguish Measures errors from imaging compile errors.

The initial proposed dependency chain is
`casa-imaging-model → casa-observation → casa-types → casa-values`, with a direct
model-to-`casa-types` edge for Measures use. A later justified crate extraction
must move the one authoritative type/engine definition and its consumers
together; it cannot introduce a parallel MS-specific representation. No new
crate extraction is required merely to keep the old dependency-free ideal.

Existing `SelectedObservationRunRow` must not simply be renamed and moved: it
mixes physical row/DDI identities with imaging projection. Factor observation
identity into the catalogue, use native Measures views for coordinates and
retain derived imaging types with their owner. M1 passes the same Measures
types through read, conversion, calibration, imaging and creation, without
boundary translation or per-value object construction.

Bulk conversion is a system-wide Measures/quanta responsibility covering all
supported families and future additions; LSRK↔TOPO is one example. Bind the route once, prepare
context-dependent coefficients once at their valid scope, then apply to many
values in reusable buffers. Consolidate the existing
`PreparedFrequencyFrameConversion` in `casa-ms` and the stateful
`MFrequencyConverter` into this shared capability without losing their distinct
scientific policies. Preserve CASA-compatible ordered interpolation where
required; preparation order and worker completion order are separate. Details
are in [shared Measures types and bulk conversion](contracts.md#shared-measures-types-and-bulk-conversion).

New dependency edges from `casa-imaging-model`, `casa-ms`, calibration, imaging operator/runtime, and
affected applications to `casa-observation` require a reviewed update of
`resources/imaging-architecture/dependency-policy.json`
and `ARCHITECTURE.md`. Preserve ADR-0016's single
compile/plan/run path, shared operators, and resource authority. There is no new
execution DAG, runtime migration matrix, receipt system, plan hash, or replay
store. One source binding may be revisited each major cycle without caching the
whole visibility dataset.

The policy delta must register `casa-observation` in `science` with its explicit
`casa-types` dependency, change the model's exact dependency list to include
`casa-observation` and `casa-types`, and enumerate each actual consumer edge.
Audit the resulting transitive graph and Measures provider boundary; neither
the contract nor the pure model may gain table/file I/O through this change.
The checker already permits same-layer
edges; do not broadly relax its layer rules. This and the corresponding
ADR-0016 raw-type ownership amendment are proposed for owner review, not applied
to the accepted policy in this documentation PR.

## 4. Access is negotiated once, then executed in bulk

1. `open` detects a supported schema/profile and supplies bounded metadata
   access, named data groups, and precise read/write capabilities. Selection
   resolves semantic catalogue IDs within this opened observation.
2. The processing algorithm's planner compiles an `AccessRequest`: required columns/roles, selections,
   scientific group boundaries, legal topology/layouts, precision, units, and ordering
   constraints, compute windows/neighborhoods and repeated-pass/revisit semantics.
   It specifies what can be split, not a physical tile size. The request travels
   through the common source interface to the selected adapter before iteration.
3. The backend binds a realization under the existing resource budget. Its
   summary states actual layout, aggregation, scratch/decode/queue requirements,
   reorder costs, physical read/decode amplification per window and traversal,
   and minimum physical working set. An unsatisfied hard
   requirement is an error; it is never silently weakened.
4. The admitted cursor repeatedly fills reusable storage and lends batch views.
   Format/type dispatch happens per binding or batch, outside numerical loops.
   The algorithm borrows compute windows or requests accounted packing.
5. A write session batches addressed patches and coalesces changes at actual
   tile/chunk/shard boundaries. Finishing reports completion or partial failure
   according to the selected backend's declared guarantee.

The backend chooses among the algorithm's permitted layouts and traversal orders
using concrete storage geometry. It need not build a general cost optimizer.
Start with explicit, inspectable heuristics and measure them. Existing tiles
may force whole-cell or whole-chunk reads for narrow selections; expose and
account for that amplification. A larger visibility batch does not require a larger
kernel window or duplicated kernel scratch.

The normal processing views guarantee Hz, m, rad and s for the corresponding
coordinate quantities. Explicit alternative units are negotiated at binding;
unit parsing/checks stay outside the numerical loop. Flux-density data use Jy
when that meaning is established, with coherent weight/sigma scaling when their
semantics permit it; arbitrary instrumental data are not inferred to be calibrated.
Frames and time scales remain explicit and scientifically converted only on
request. See [units and measure references](contracts.md#units-and-measure-references).

Work units may finish independently and reuse their private buffers while other
units continue, subject to scientific/order/write dependencies. Sharing a large
allocation can let one slow window pin it; the realization must account for that
lifetime or use admitted independent pieces/packing. Cube minor-cycle state has
its own imaging owner and does not extend the lifetime of streamed visibilities.
See [independent work-unit lifetimes](contracts.md#independent-work-unit-lifetimes);
this extends the representation's requirements, not the number of schedulers or
resource authorities.

| Consumer | Typical request, not a universal mandate |
| --- | --- |
| Continuum imaging | Selected groups/SPWs; UVW, frequency, correlation, validity and weights; bounded sample/channel-major views with irregular or factored coordinates. Revisit selection each major cycle. |
| Cube imaging | Frequency-local work with many samples per channel and explicit spectral mapping. Full-frequency storage chunks may be decoded repeatedly across plane waves; bind reports that cost and admits feasible traversal alternatives. |
| Gain/bandpass solve | Scientific grouping by solution interval and configured field/SPW rules; correlations/channels arranged for the solver; sequential or bounded concurrent fragments feed admitted accumulators. |
| Apply calibration | Streaming visibility/flag/weight updates, with gains interpolated by the calibration owner. No need to materialize a whole solve group. |
| Flagging | Metadata plus flags and requested diagnostics; flags-only operations read no visibility payload unless the algorithm requests it. |

Layouts do not perform science. Reordering/packing may preserve samples; channel
averaging, correlation conversion, spectral interpolation, UVW rotation,
conjugation, and calibration change their interpretation and belong to explicit
scientific operators. Their output cannot inherit writable source provenance
without a separately defined update rule.

XRADIO's pinned converter defaults to time chunks spanning the other axes,
targeting roughly 128 MiB for the largest variable; it splits frequency and
then baseline when even one time step exceeds the codec limit. Therefore a cube
processed in channel waves can decode an entire band once per wave. Aggregation
alone cannot repair that geometry. The existing imaging resource planner may
choose a wider admitted plane wave, bounded chunk reuse, or a proven compatible
traversal that accounts for both visibility and image-state I/O. It cannot make
all plane grids resident merely because a decoded chunk fits. No hidden grid
paging scheme, whole-observation cache, or rechunk-on-open is authorized here.
If available realizations must reread, report and measure that amplification.

Creation exposes workload-oriented chunk policies. The proposed cube-oriented
default splits frequency as well as time/baseline, with concrete extents selected
from channel access and decode budgets; M1 benchmarks determine the actual
heuristic. A continuum-oriented policy may favor full-frequency chunks. Record
the resolved chunk shape at creation and preserve existing stores on update.
M3 compares both layouts on continuum, restricted-wave cube and bandpass work;
no claim that one chunk shape is best for every algorithm follows.

## 5. Persistent support profiles

The pinned MSv4 schema constrains the main coordinate units: frequency in Hz,
UVW in m, time attributes in s, and angular coordinates in rad (spatial distances
in m). Time also declares `format` and `scale`; both must be decoded with the
units, and alternative time-format combinations need oracle evidence rather than
a bare unit rescale. This is not an all-SI rule: visibility `units` is a string
defaulting to Jy. Noncanonical data must be validated against the declared
profile; do not silently label a nonconforming store as compliant MSv4.

There is no single `supports_v4` boolean. Track read, create, selected update,
structural extension/append, conversion fidelity, codecs, and concurrency
separately. This table is a target, not an implemented-capability claim.

| Profile | Planned scope | Acceptance oracle |
| --- | --- | --- |
| Existing CASA MSv2 | Retain current read/create/update and supported storage managers, including variable cell shapes and extension metadata | CASA/casacore ↔ casa-rs; existing release cross-matrix and science workloads |
| XRADIO MSv4 schema 4.0.1, local Zarr v3 | First native v4 target is interferometric `VisibilityXds`: read, create, selected updates of supported existing variables and planned group creation | Pinned XRADIO/Xarray/Zarr environment ↔ Rust in both directions, plus CASA science comparisons on matched MSv2 data |
| Note 264 MSv3 beta | Deferred pending a concrete dataset/producer need; no present completion dependency or generic support claim | Future independent producer/reader for the precise accepted profile; self-roundtrip alone is insufficient |
| Producer dialects such as SKA MSV3B | Deferred named profiles; never infer full note-264 compliance from a product name | Future pinned producer release, inspected schema, independent read/write fixtures |
| MSv4 schema 4.0.0 / Zarr v2 | Distinct compatibility profiles only when independently tested; not implied by 4.0.1/Zarr v3 | Version-specific fixtures and upgrade rules |
| Remote/object stores | Keep transport capability pluggable; local implementation does not claim distributed-safe update or atomic publication | Separate transport/concurrency evidence before advertising support |

The owner selected MSv2 + MSv4 with MSv3 deferred on 2026-10-23. The shared
contract must not hard-code v2 associations in ways that preclude a future
adapter. Initial v4 scope does not include XRADIO's antenna-axis `SpectrumXds`
schema; that is distinct from retaining current real-valued MSv2 capabilities.

Start the v4 codec profile with uncompressed and XRADIO's bytes/Blosc-LZ4 path,
native complex64/128, supported boolean/unsigned flags, and numeric weight/UVW
types. Preserve dtype or explicitly reject it; f16 weights/UVW can widen exactly
to f32 for computation without implying permission to change persistent dtype.
M0/M1 must lock exact crate/dependency versions and fixture environments, inspect
dimension names, strings, fill values, endian codecs, CF/Xarray attributes,
nested metadata, and consolidation behavior. Generic Zarr support is necessary
but is not proof of XRADIO interoperability.

Pin the complete Rust store/codec feature graph, including `zarrs_filesystem`,
and the complete Python oracle environment. Inspected zarrs 0.23.14 references
filesystem 0.3.9, whose per-key lock registry grows with visited keys; the
inspected development tree instead uses filesystem 0.4.0 with different cache,
locking and sync behavior. These are feasibility findings, not a selected
compatible dependency pair. M0/M1 must select a bounded realization and prove
long-session retention, admitted codec concurrency, and replacement working set.

The proposed local v4 write profile uses per-object atomic replacement under
exclusive mutation ownership, with no dataset snapshot/transaction or shared
cross-process lock protocol. See [failure and creation rules](contracts.md#7-updates-and-creation).
Default new flags are boolean; reason-preserving integer flags require an
explicit width and reason mapping. Creation without an input MS must use the
same catalogue and batch vocabulary, not depend on backend-retained metadata.

MSv4 does not imply that current CASA directly opens that format. Maintain
CASA compatibility for MSv2 and require XRADIO compatibility for MSv4; conversion
to CASA is an explicit supported mapping, with a preflight loss/unsupported
report where semantics cannot be preserved. No arbitrary MSv2→v4→MSv2 exact-row
roundtrip is promised. Original row existence can be unknowable after NaN
padding, and format extensions may have no destination representation.

Successful same-format updates retain unselected data, unknown extensions, and physical
storage arrangements. Do not normalize an existing dataset on open. Data-group
aliasing and flag semantics require special care; the
[write contract](contracts.md#7-updates-and-creation) is authoritative for this
proposal.

### Conversion fidelity

This is the minimum audited loss table for the pinned XRADIO converter, not a
claim that every MSv2 extension has been inventoried. M0 enumerates every column,
keyword and subtable in the accepted input profiles. Any unmapped item needs an
explicit preservation mapping, a reported loss decision or rejection before
conversion; same-format updates continue to preserve it. Default casa-rs
conversion rejects unapproved loss. The table proposes conversion behavior for
owner review, not a change to the MSv4 schema.

| MSv2 source fact | Pinned XRADIO conversion / v4 representation | Proposed casa-rs creation and reverse-conversion rule |
| --- | --- | --- |
| DATA, CORRECTED_DATA, MODEL_DATA and their associations | Named correlated-data variables/data groups; group context includes flags, weights, UVW and field/source | Map every selected group explicitly; preserve associations/aliases and numeric precision, or reject. Group labels do not recreate missing native roles automatically. |
| FLAG | Copied to boolean FLAG | Preserve element badness. Existing integer flags retain their declared reasons on same-format writes; projection to boolean loses reasons unless a separately accepted category mapping exists. |
| FLAG_ROW | Omitted; FLAG is not automatically ORed with it | Encode effective `FLAG OR FLAG_ROW` for every affected channel/correlation. Report loss of row-versus-element provenance; reverse conversion cannot recover the original row flag. Never infer omitted row flags when reading an already converted file. |
| FLAG_CATEGORY | Not mapped | Require an explicit reason/category mapping or reject/report approved loss. Do not assume category reasons equal the cumulative FLAG, or silently discard them. |
| INTERVAL | Median if values vary, stored as scalar `time.integration_time` | Preserve nominal interval through compatible partitioning or a separately accepted representation; otherwise reject/report approved loss. A median cannot recover per-sample values. Do not put INTERVAL into effective integration time. |
| EXPOSURE | Maps to EFFECTIVE_INTEGRATION_TIME when present | Preserve independently of nominal INTERVAL; neither implies the other. Reverse mapping uses this variable, not the nominal sampling interval. |
| TIME, TIME_CENTROID and extra-precision metadata | Time/centroid have different roles; centroid extra precision is supported, but not every legacy precision column is mapped | Audit each supplied precision field and epoch/unit conversion; preserve split precision where supported or reject/report loss. Optional absence is not evidence that midpoint and centroid were originally equal. |
| FIELD_ID per acquisition | Converted to `field_name(time)` using the maximum populated baseline field ID at each time; optional field partitioning is available | Validate field agreement within each destination time slice; partition compatibly or reject conflicting fields. Preserve associations under selection/reordering and repeated mosaic visits. A reader cannot reconstruct distinctions already discarded upstream. See [converter evidence](#converter-evidence-for-mosaics). |
| WEIGHT and WEIGHT_SPECTRUM | Spectrum preferred; row weights may be broadcast on fallback, and distinct row summaries may be dropped | Bind fallback per cell and report the native-role information retained/lost. Keep effective channel values; preserve a distinct row summary only through an accepted mapping. Reverse reduction is an explicit scientific policy, never an automatic inverse. |
| SIGMA and SIGMA_SPECTRUM | Not mapped | Preserve through an accepted explicit mapping or reject/report approved loss. Do not reconstruct as `1/sqrt(WEIGHT)` without a separately justified scientific relation. |
| Duplicate `(time, ordered baseline)` acquisitions within one partition | Dense converter keeps the last row | Reject or split into valid compatible partitions while preserving every acquisition; never alter times or average to force uniqueness. A reader cannot recover acquisitions already discarded upstream. |
| Absent slots versus actual NaN-valued acquisitions | NaN payload padding, possibly FLAG=false; standard output need not retain legacy row occupancy | Retain unknown occupancy; reverse row creation requires an explicit declared policy or rejection. Chunk presence and flags do not prove measured-row existence. |
| Original row/subtable IDs, other optional columns/keywords | Some IDs are replaced by local names/subsets; many optional fields have no standard mapping | Use explicit catalogue/caltable namespace bindings. Inventory remaining metadata; no opaque sidecar is used to claim a lossless CASA roundtrip. |

For each row, M1 retains the original MSv2 fixture, reads the pinned converter's
actual output, tests the proposed Rust conversion or typed rejection, and checks
what an independent reverse reader can establish. Where the converter lost
information, compare the Rust reader to the file that actually exists and
separately document divergence from the original MSv2. It must not invent a
repair. Scientific paired fixtures used to claim equivalence must either avoid
those losses or apply an explicit, independently checked mapping.

## 6. Existing decisions and deliberate changes

Retain [ADR-0008](../adr/0008-casacore-storage-and-bounded-measurementset-writes.md)
for table storage-manager bindings, bounded writes, locking, and ordinary
in-place partial failure. Extend its observation-facing scope to the new
source/sink interface and define Zarr-specific guarantees in a reviewed new ADR;
do not silently apply casacore locks or filesystem rename promises to an object
store. Retain [ADR-0015](../adr/0015-run-local-imaging-ownership-without-content-attestation.md)
and [ADR-0016](../adr/0016-imaging-foundation.md): ownership establishes
within-run association, with no private source markers, payload hashes, whole
column backups, or verification-only data rereads.

The dependency-policy update and revised raw observation ownership are explicit
architecture decisions. M1 proposes the exact ADR additions/amendments and
updates to current descriptive documentation. Accepted ADRs are not modified
by this proposal. Existing persistent contracts are expanded only through
profile-specific reviewed tests; an unavailable capability is never reported
as complete.

Rejected designs include forcing v4 through per-row table calls, densifying all
legacy rows, letting storage dictate the only kernel layout, a Python bridge on
the hot path, a universal per-sample trait object, and retaining a second
production imaging route for comparison. Frozen binaries/test harnesses provide
the old performance baseline without maintaining duplicate production engines.

## 7. Reference baseline

Sources were inspected on 2026-10-23. XRADIO source links are pinned to
`3ed765867515095d852eb68c073165c1f6c58761`; its source declares schema 4.0.1.
The source package version string is not evidence of a published release.
Implementation acceptance must record its full oracle dependency lock.

- [MSv2, casacore note 229](https://casacore.github.io/casacore-notes/229.html)
  and [MSv3 beta, note 264](https://casacore.github.io/casacore-notes/264.html).
- [CASAdocs: MS v2.0 layout](https://casadocs.readthedocs.io/en/stable/notebooks/casa-fundamentals.html#MS-v2.0-Layout)
  explicitly names MSv2 as CASA's internal working format;
  [casacore's version check](https://github.com/casacore/casacore/blob/a1399b6b99907bd14021920a089403a93c10aba1/ms/MeasurementSets/MeasurementSet.cc#L926)
  requires version 2.0. These establish CASA support, not an exhaustive survey
  of observatory data or a claim that no v3 variants exist.
- [SKA receive-core format documentation](https://developer.skao.int/projects/ska-sdp-realtime-receive-core/en/latest/measurement_set_utilities_and_format.html):
  MSV3B is a producer profile to investigate, not a proven universal v3 oracle.
- [XRADIO schema, including data groups](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py)
  and [MSv4 overview](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/docs/source/measurement_set/overview.rst).
- MSv4 unit evidence: [literal coordinate units and time measures](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/schema/measures.py#L56),
  [frequency in Hz](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L432),
  [UVW in m](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L637),
  and [visibility units defaulting to Jy](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L1035).
- [XRADIO Zarr format configuration](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/_utils/zarr/config.py)
  and [conversion defaults](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/convert_msv2_to_processing_set.py).
- [Padding values](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/_utils/list_and_array.py)
  and [optional conversion test row indices](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/conversion.py#L2148).
- Conversion-loss evidence: [MAIN role mapping](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/msv2_to_msv4_meta.py#L3),
  [weight preference](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/conversion.py#L916),
  [interval median](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/conversion.py#L1865),
  [EXPOSURE destination](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/schema.py#L1785),
  and [duplicate/padding behavior](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/_tables/read.py#L1494).
- [XRADIO default chunk sizing](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/conversion.py#L427)
  and [suggested flag meanings](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/docs/source/measurement_set/schema.rst#L344).
- [Zarr v3 specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html),
  [Xarray encoding conventions](https://docs.xarray.dev/en/stable/internals/zarr-encoding-spec.html),
  [zarrs](https://docs.rs/zarrs/0.23.14/zarrs/), and
  [zarrs object-store adapter](https://docs.rs/zarrs_object_store/).
- Store feasibility evidence: [release filesystem dependency](https://github.com/zarrs/zarrs/blob/f2c77ea616507cd5837f32eafb4f4bf317db8896/Cargo.toml#L66),
  [retained per-key locks and in-place writes](https://github.com/zarrs/zarrs/blob/f2c77ea616507cd5837f32eafb4f4bf317db8896/zarrs_filesystem/src/lib.rs#L164),
  [release concurrency initialization](https://github.com/zarrs/zarrs/blob/f2c77ea616507cd5837f32eafb4f4bf317db8896/zarrs/src/array/concurrency.rs#L23),
  and [different development-store behavior](https://github.com/zarrs/zarrs/blob/7b13a7fac68bbdcdd543a2b805df9de48e5c4b20/zarrs_filesystem/src/lib.rs#L79).
- [Inspected zarr-python atomic local replacement](https://github.com/zarr-developers/zarr-python/blob/957a5f682b46b4055d5619d01b4a3a3cc36c04db/src/zarr/storage/_local.py#L107)
  is evidence about that commit, not every XRADIO dependency lock, dataset
  consistency or power-loss durability.
- [XRADIO image schema](https://xradio.readthedocs.io/en/latest/image_data/schema.html):
  a separate schema, deferred from this visibility programme.
