# MeasurementSet boundary: rules

Truth class: normative
Last reality check: 2026-11-02

casa-rs reads and writes MeasurementSet v2 and XRADIO MSv4 through one
boundary. `casa-observation` holds the shared contract: the catalogue, the
access request and its estimate, the traversal that delivers
`VisibilityBatch`es, scientific groups and their completion, the buffer
pool, and update sessions. `casa_ms::observation_io` holds the adapters
behind it: opening, the MSv2 and MSv4 stores, the converter and the MSv2
creation plan. Their rustdoc is the contract: what each type holds, what
each store delivers, and what it refuses. This file keeps only what the
code does not state: the rules that bind changes to the boundary, the owner
decisions behind it, and what each capability not yet offered must meet.
Moving the remaining consumers onto the boundary is tracked in #1055.

## Rules

| Rule | Required behavior |
| --- | --- |
| C1: source independence | No physical row IDs, Zarr keys, storage managers or MS-version branches in numerical processing loops. Only opening, creation, capabilities and diagnostics see the store. |
| C2: faithful values | Coordinates and references are native casa-rs Measures types and views, accepted directly by its conversion APIs. Canonical units; explicit frames and precision; native flag and weight semantics; full data-group association; ordered baselines. No silent scientific transform: what the profile cannot represent is refused. |
| C3: layout independence | Sample topology, memory layout, scientific grouping and storage chunking stay separate. |
| C4: bounded ownership | Every live payload, index, decode buffer, cache, packed copy and queue is admitted; borrowed views cannot outlive or refill their storage. Independent work retires separately; shared storage stays live until its last use. |
| C5: invertible updates | A sample-preserving reorder or selection keeps a checked inverse map to the store; a reduction does not, and its result goes to an explicit output. Shared variables have an explicit mutation scope. |
| C6: honest guarantees | Unsupported profiles, layouts and native features, and non-atomic writes, are explicit typed refusals. |
| C7: bulk execution | Bind numeric kind, layout and invariant context outside scalar loops; use native array views and family-specific kernels. No per-value object allocation, virtual dispatch, coordinate reconstruction or repeated setup or provider lookup. |

Moving a consumer onto the boundary deletes its old route in the same
change: no compatibility layer and no second production route kept for
comparison (a baseline build serves that). A consumer has not moved while a
wrapper still leaks physical rows or `DATA_DESC_ID` into its algorithm.

## Owner decisions in force

- **Formats** (2026-10-23): MSv2 and MSv4 (XRADIO schema 4.0.1 on local
  Zarr v3). MSv3 is deferred until a concrete dataset or producer needs it;
  it then comes as a named profile with its own independent producer and
  reader checks, never inferred from a product name. Object stores, MSv4
  image persistence and Zarr v2 are separate later work.
- **Shape** (2026-10-24): the interface takes MSv4's native shape, and the
  MSv2 adapter assembles MSv2 into it at opening.
- **Arrays** (2026-10-24): numerical arrays and views are `ndarray`'s,
  under thin domain types that carry axis meaning, Measures metadata, and
  flag and weight meaning.
- **Measures** (2026-10-24): the boundary uses `casa-types` Measures and
  quanta types directly, with no observation-specific measure vocabulary,
  and the Measures APIs may be reworked for bulk use (below).

## Capabilities not yet offered

Each is refused today with a typed error, or offered only in part. It is
offered when its obligation is met.

| Capability | Obligation |
| --- | --- |
| Units and conversions requested at binding | A requested unit holds for the view's bound scope; conversions fused with the fill; identity conversions copy nothing; weights transform as `w / \|a\|²` under an amplitude scale `a`. |
| Precision other than `Complex<f32>`, `bool`, `f32`, `f64` | Native precision, lossless widening or approved narrowing per role, charged and fused with decode; the writer checks representability. |
| Real-valued data (`FLOAT_DATA`, MSv4 `SPECTRUM`) | Its own typed variant under its own profile, never reinterpreted as visibilities. |
| More than one data group per traversal | Each group keeps its UVW and field and source context, and explicit alias relationships to the others. |
| Unknown occupancy | Representable in the batch; conversion to rows resolves it by a declared policy or reports the ambiguity, never inventing an MSv2 row; writing into holes needs an explicit fill policy. |
| Native flags: bitmask dtypes, reason bits, `FLAG_CATEGORY`, row flags as their own view | Dtype and reason bits preserved on writes; generic flagging bound to the store's actual unspecified bit; clearing an effective flag never clears unknown reasons, `FLAG_ROW` or `FLAG_CATEGORY`. |
| Per-cell weight fallback | An undefined sigma or weight cell falls back per cell, recorded per cell, instead of failing the read. |
| Updates beyond the write-back sessions | Bit flags and flag categories; structural writes (a new MSv4 data group, or rebinding a group to its own copy of a shared variable); a composition rule or refusal for conflicting queued patches; per-batch MSv2 planning; a process killed during MSv4 chunk replacement, `ENOSPC` and stray temporary files, checked by reopening selected and unselected sentinels. |
| Creation beyond the MSv4 writer and the MSv2 creation plan | An MSv4 from several observations; failure injection at publication; the writer's held chunk admitted with the producer's batches; omitting padding without a counting pass. |
| Calibration binding in production | `gainfield` and nearest-field selection through bound fields; bandpass and other per-channel tables; tables solved over combined windows; `FEED` and receptor association; field binding tolerant of ephemeris fields. |
| Ephemerides | XRADIO's optional ephemeris variables carried through the boundary and the converter (#998). |
| Baseline-dependent polarization (`polarization_mixed`) | A checked per-baseline basis mapping in the catalogue. |
| `TIME_CENTROID_EXTRA_PRECISION` | The residual carried through the epoch view, slicing, packing and writing; adding it to a large `f64` is no substitute. |
| Four-dimensional `EFFECTIVE_INTEGRATION_TIME` | Per-channel exposure in the batch's timing. |
| Resource admission of the whole read and write path | Everything a traversal or writer holds is admitted through the runtime's `HostResources` with cancellable backpressure, including zarrs codec scratch (found or conservatively bounded before work, failing with the minimum it needs) and store caches bounded in bytes. |
| Decode on the admitted executor | Executing threads and live codec buffers counted on the real decode path; no fall-through to a global pool; in-partition parallel decode (#1016). |
| Bulk Measures conversion | Below. |

Updates are addressed in the store's native domain. A patch's affected
extent and its conflicts are resolved after expanding data-group aliases and
broadcast aliases: setting one channel's effective weight where MSv2 holds
one `WEIGHT` for every channel needs an explicit whole-scalar scope, a
channel-resolved destination created under bounded memory, or refusal.
Calibration owns any reduction of channel weights into a row `WEIGHT` and
submits both patches; storage never invents that inverse. There is no
default last-writer-wins.

The MSv4 write guarantees (per-object atomic replacement, staging and
publication, no cross-process lock protocol) and any change to ADR-0008's
locking and partial-failure rules need a reviewed new ADR before they are
advertised as persistent guarantees.

### Bulk Measures conversion

Every supported Measures and quanta family offers bulk operation, present
and future: directions, epochs and time scales, positions, baselines and
UVW, frequencies, radial velocities and Doppler conventions, magnetic and
other vector measures, and unit conversions. LSRK↔TOPO is one example, not
the acceptance. The shape is prepare, then apply:
- bind the route, units and policy once per conversion request;
- prepare context-dependent terms (matrices, factors, interpolation state)
  at their valid scope, in bounded groups where the context varies;
- apply to numeric slices into reusable buffers, keeping the per-value
  nonlinear or iterative work each family needs, with no per-value object,
  allocation, lock or route search.

A prepared transform is tied to every input that affects it, so a context
change invalidates it; equal reference enums with different contexts are not
an identity. CASA-compatible ordered interpolation keeps its sequence across
batches and workers. Acceptance for each family: consumers use it directly;
scalar, bulk and CASA agree to that family's tolerances; and timings, with
binding and preparation reported apart from apply, are compared against
both the scalar path and any already-prepared production path.
