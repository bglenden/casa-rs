# Native observation I/O for MSv2 and MSv4

Status: proposed; MSv2/MSv4 scope confirmed, detailed D1–D6 decisions await review
Truth class: design proposal
Date: 2026-10-23
Companion documents: [contracts](contracts.md), [implementation plan](implementation-plan.md)

## 1. Outcome and boundaries

Imaging, calibration, flagging, transforms, and simulation exchange the same
scientific observation types regardless of persistent MS format. An algorithm
specifies the information, grouping, ordering, precision, and memory layout it
needs. A source binds those requirements to the storage implementation and
returns bounded blocks. A sink accepts bounded creation batches or addressed
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
| Scientific group | Samples an algorithm treats together, such as a calibration solution interval. May span blocks and partitions. |
| Storage chunk/tile | Physical decode/update unit chosen at creation or already present on disk. |
| I/O block | Bounded aggregation delivered by a source; may contain many physical chunks. |
| Compute window | A borrowed subset or explicitly packed projection of a block for a kernel. |

An MSv2 MAIN row contains a channel-by-correlation visibility matrix for an
integration/baseline/data-description context, not one polarization scalar
spectrum. MSv3 beta changes schema and column associations; it is not merely
another label for today's MSv2 files. MSv4's principal visibility variable is
`[time, baseline_id, frequency, polarization]` within a dataset; a Processing
Set collects datasets. Mosaic fields can vary along time within a partition.
Different spectral windows generally occupy different partitions, including
when continuum and line data are processed together.

The shared interface therefore cannot use a casacore row number, DATA_DESC_ID,
one field, or one spectral window for the whole observation as its universal
identity. It also cannot assume every observation is a complete time–baseline
rectangle. See [topology and identity](contracts.md#2-topology-and-identity).

## 3. Proposed ownership

```text
imaging operator/runtime   calibration   flagging/transforms   simulation
              \               |                |                /
               casa-observation: scientific block/access contracts
                                  |
                       casa-ms: open/create/bind
                      /                         \
          casacore MSv2 adapter           XRADIO MSv4 adapter
                 |                            |
             casa-tables                    zarrs
```

The diagram shows data flow, not that the contract crate depends on its
implementations. The proposed dependency direction is:

| Owner | Responsibilities | Dependencies and exclusions |
| --- | --- | --- |
| New `casa-observation` | Typed raw scientific metadata, array/block views, access requirements, source/sink traits, capabilities and errors | No workspace dependencies, filesystem, task runtime, or scientific projection engine. Reuse small Rust numeric crates. |
| `casa-ms` | Dataset lifecycle, catalogue access, backend dispatch, source binding, bounded fill/update/create adapters | `casa-observation`, existing storage/measures facilities, and pinned `zarrs`. Initial adapters can be modules; no new storage crate is required merely for symmetry. |
| `casa-tables` | Existing generic casacore table and storage-manager machinery | Retains its table API and persistent compatibility. It does not depend on observation or imaging contracts. |
| Imaging/calibration and other domain owners | Request layouts, evaluate geometry, solve/project/reduce, apply scientific transforms | Consume `casa-observation`; no table/Zarr addressing in their scientific kernels. |
| Existing execution owners | Resource admission, worker teams, cancellation, pass lifecycle | Continue to govern I/O/decode/packing work; no independent backend worker pool or scheduler. |

`casa-imaging-model` remains workspace-dependency-free and owns compiled imaging
science. Raw observation metadata is distinct from projected imaging geometry.
Existing `SelectedObservationRunRow` must not simply be renamed and moved: it
mixes physical row/DDI identities with imaging projection. Factor raw types into
the new contract, retain derived types with their scientific owner, and convert
or bind at block scope. Do not introduce parallel geometry implementations.

New dependency edges from `casa-ms`, calibration, imaging operator/runtime, and
affected applications to `casa-observation` require a reviewed update of
[`dependency-policy.json`](../../resources/imaging-architecture/dependency-policy.json)
and [`ARCHITECTURE.md`](../../ARCHITECTURE.md). Preserve ADR-0016's single
compile/plan/run path, shared operators, and resource authority. There is no new
execution DAG, runtime migration matrix, receipt system, plan hash, or replay
store. One source binding may be revisited each major cycle without caching the
whole visibility dataset.

## 4. Access is negotiated once, then executed in bulk

1. `open` detects a supported schema/profile and supplies bounded metadata
   access, named data groups, and precise read/write capabilities. Selection
   resolves semantic catalogue IDs within this opened observation.
2. The algorithm compiles an `AccessRequest`: required columns/roles, selections,
   scientific group boundaries, legal topology/layouts, precision, and ordering
   constraints. It specifies what can be split, not a physical tile size.
3. The backend binds a realization under the existing resource budget. Its
   summary states actual layout, aggregation, scratch/decode/queue requirements,
   reorder costs, and minimum physical working set. An unsatisfied hard
   requirement is an error; it is never silently weakened.
4. The admitted cursor repeatedly fills reusable storage and lends block views.
   Format/type dispatch happens per binding or block, outside numerical loops.
   The algorithm borrows compute windows or requests accounted packing.
5. A write session batches addressed patches and coalesces changes at actual
   tile/chunk/shard boundaries. Finishing reports completion or partial failure
   according to the selected backend's declared guarantee.

The backend chooses among the algorithm's permitted layouts and traversal orders
using concrete storage geometry. It need not build a general cost optimizer.
Start with explicit, inspectable heuristics and measure them. Existing tiles
may force whole-cell or whole-chunk reads for narrow selections; expose and
account for that amplification. A larger I/O block does not require a larger
kernel window or duplicated kernel scratch.

| Consumer | Typical request, not a universal mandate |
| --- | --- |
| Continuum imaging | Selected groups/SPWs; UVW, frequency, correlation, validity and weights; bounded sample-major or factored windows accepted by the operator. Revisit selection each major cycle. |
| Cube imaging | Frequency-local work with many samples per channel and explicit spectral mapping. Storage may aggregate adjacent frequency chunks while compute windows remain small. |
| Gain/bandpass solve | Scientific grouping by solution interval and configured field/SPW rules; correlations/channels arranged for the solver; bounded fragments feed accumulators. |
| Apply calibration | Streaming visibility/flag/weight updates, with gains interpolated by the calibration owner. No need to materialize a whole solve group. |
| Flagging | Metadata plus flags and requested diagnostics; flags-only operations read no visibility payload unless the algorithm requests it. |

Layouts do not perform science. Reordering/packing may preserve samples; channel
averaging, correlation conversion, spectral interpolation, UVW rotation,
conjugation, and calibration change their interpretation and belong to explicit
scientific operators. Their output cannot inherit writable source provenance
without a separately defined update rule.

## 5. Persistent support profiles

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

MSv4 does not imply that current CASA directly opens that format. Maintain
CASA compatibility for MSv2 and require XRADIO compatibility for MSv4; conversion
to CASA is an explicit supported mapping, with a preflight loss/unsupported
report where semantics cannot be preserved. No arbitrary MSv2→v4→MSv2 exact-row
roundtrip is promised. Original row existence can be unknowable after NaN
padding, and format extensions may have no destination representation.

Same-format updates retain unselected data, unknown extensions, and physical
storage arrangements. Do not normalize an existing dataset on open. Data-group
aliasing and flag semantics require special care; the
[write contract](contracts.md#7-updates-and-creation) is authoritative for this
proposal.

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
- [XRADIO Zarr format configuration](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/_utils/zarr/config.py)
  and [conversion defaults](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/convert_msv2_to_processing_set.py).
- [Padding values](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/_utils/list_and_array.py)
  and [optional conversion test row indices](https://github.com/casangi/xradio/blob/3ed765867515095d852eb68c073165c1f6c58761/src/xradio/measurement_set/_utils/_msv2/conversion.py#L2148).
- [Zarr v3 specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html),
  [Xarray encoding conventions](https://docs.xarray.dev/en/stable/internals/zarr-encoding-spec.html),
  [zarrs](https://docs.rs/zarrs/0.23.14/zarrs/), and
  [zarrs object-store adapter](https://docs.rs/zarrs_object_store/).
- [XRADIO image schema](https://xradio.readthedocs.io/en/latest/image_data/schema.html):
  a separate schema, deferred from this visibility programme.
