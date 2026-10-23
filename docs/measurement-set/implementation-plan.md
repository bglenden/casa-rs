# MeasurementSet implementation plan

Status: proposed; dependent on architecture and contract approval
Truth class: design proposal
Date: 2026-10-23
Design inputs: [architecture](architecture.md), [contract revision 0](contracts.md)

## 1. Delivery strategy

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
invent slightly different versions of the common block contract. M4 packets
may begin when their required backend operations are accepted. Per the owner's
2026-10-23 decision, MSv3 is deferred until a concrete dataset/producer requires
it. It is not a completion dependency for this MSv2 + MSv4 programme.

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
- Pin XRADIO commit, Python dependency lock, Zarr/profile and `zarrs` candidate.
  Build small public/reproducible fixtures with the actual Python writer; inspect
  metadata and bytes with an independent reader. No assumption that a schema
  example is an interoperable file.
- Keep explicit profile detection so a v3-labelled or otherwise unsupported
  schema is not silently treated as v2. A detailed v3 producer audit is deferred
  with its implementation, not required to approve M0.
- Identify existing production managers, optional columns, mutation capabilities,
  selection/weight fallback policies, and expected error behavior to preserve.
  Include FLOAT_DATA, LAG_DATA, FLAG_CATEGORY and SIGMA_SPECTRUM in the audit.
- Define a **blocking** comparison harness for selected bulk reads, selected
  writes, and representative production imaging/calibration. Establish noise
  bands from the actual baseline and name critical workloads before optimizing.

**Gate:** stronger reviewers approve the inventory, claimed profiles, immutable
baseline, and acceptance cases. Missing data/oracles are reported as missing,
not green. Baseline results and exact rerun commands are attached to the issue
or PR. Heavy runs obey existing disk and resource limits; no new large data are
downloaded merely to approve this documentation proposal.

## 3. M1 — executable contract trial and first stability checkpoint

**Owner:** architecture lead; cheaper agents may implement isolated fixture or
adapter pieces against a reviewed sketch. Do not delegate unresolved core
semantics to implementation agents.

**Deliverables:** minimal `casa-observation` crate and proposed dependency-policy
update; rustdoc/examples; one reusable v2 read path and one real local MSv4
read/decode path; bounded addressed-write trial for each; production-kernel
integration probes, not a synthetic array API demonstration alone.

Exercise all of the following before declaring the contract stable enough for
parallel migration:

1. The same imaging operator consumes compatible selections from irregular v2
   and factored v4, with sample-major and one useful channel/factored layout.
   Compare results and count copies, calls, allocations and peak memory.
2. Gain and bandpass accumulation consume split scientific groups, including
   multi-SPW rules, empty groups, reordered correlations and a group larger than
   the allowed resident data. Confirm grouping is independent of block size.
3. Flags-only read, undefined WEIGHT_SPECTRUM fallback, FLOAT_DATA where already
   supported, typed c32/c64 dispatch, f16 weight widening, bit flags/categories,
   row flags, and native weight broadcast all have explicit tests.
4. Reorder/gather selected values and bulk-write them back through inverse maps.
   Include duplicate reads/conflicting writes, group aliases, unknown flag bits,
   narrow-channel updates, distinct UVW/field contexts, unselected sentinel data,
   and failure during a flush. Reduced data cannot retain writable provenance.
   Exercise reading and updating the same role, different roles aliasing one
   underlying variable, and a requested revisit: prove disjoint underlying
   regions or a legal read-before-write traversal, or typed rejection, with no
   hidden snapshot or whole-array cache.
5. Sparse times/baselines, duplicate acquisitions, NaNs and v4 fill values do not
   collapse presence into flags. Ordered baselines/cross-hands, split time
   precision, time centroids, moving pointing and spectral frames survive.
6. A v4 I/O block spans multiple chunks and feeds smaller borrowed kernel
   windows. The resource accounting includes decode/packing/index/write scratch,
   and an oversized physical working set fails before unsafe allocation.
7. Actual XRADIO reads Rust-written trial data and Rust reads XRADIO-written
   data. Independently check v2→v4 semantics that the converter might omit,
   particularly FLAG_ROW. The Python converter is an oracle for its output
   conventions, not sole authority for CASA science.

Review the contract as code: lifetime behavior, allowed axis permutations,
metadata dependence, error cases, array layout, and mutation scope. Publish the
smallest worked Rust examples for imaging, calibration and selected update.
Measure early overhead against the M0 path before investing in broad migration.
These trials need not satisfy the entire release matrix yet.

**Gate:** independent contract + science/format reviews, first performance
comparison, and owner approval of D1–D6 and any proposed ADR changes. Record an
accepted contract revision. All blocking findings resolved before M2/M3
parallel implementation. No claim of production v4 support at this checkpoint.

## 4. M2 — production MSv2 adapter and first consumers

**Packets:** (a) catalogue/selection and read binding, (b) bounded update/create,
(c) imaging source integration, (d) gain/bandpass/applycal integration. Sequence
shared facade changes; parallelize only disjoint consumer edits.

Preserve storage-manager bindings, endian behavior, variable cell shapes,
undefined cells, narrow selections, coordinate evaluation and native write
semantics. Reuse current typed bulk fast paths rather than rebuild them through
generic per-cell values. Replace row-limit policy with bytes/layout-aware
planning, while retaining the efficient v2 sample-major realization.

Move imaging's source preparation and operator windows onto the shared blocks
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
execution, and measured aggregation benefit. Demonstrate that larger native
aggregation reduces physical calls/decodes/copies or elapsed time for a
representative workload; a larger configured batch alone does not satisfy this
requirement. Report component benefit separately from any end-to-end speedup.

## 6. M4 — migrate remaining consumers and producers

Use this inventory as a starting point; M0 checks the current tree rather than
treating textual references as proof of production ownership.

| Area | Starting code | Migration/acceptance obligation |
| --- | --- | --- |
| Existing observation access | `casa-ms/src/visibility_buffer.rs`, `selected_observation_buffer.rs`, `selected_observation/`, `observation_owner/` | Consolidate into one common path; retain buffer reuse, selection and source ownership. |
| Imaging | `casa-imaging-application/src/imaging/source.rs`, `casa-imaging-runtime/src/source_access.rs`, `pass/`, `casa-imaging-operator/src/sample.rs` | Same operator and pass driver; source aggregation independent of compute windows; geometry remains scientific. |
| Calibration | `casa-calibration/src/{session,solve,plan,execute,gencal}.rs`, `solve/grouping.rs` | Solve/apply/metadata access uses observation contracts; separately preserve calibration-table outputs and CASA science. |
| Flagging/transforms | `casa-ms/src/{flagging,transform,selection,spectral_selection}.rs` | Bit/category/row flag semantics, averaging and output provenance, optional columns, selected updates and new outputs. |
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

**Gate:** per-packet acceptance and deletion review; milestone review audits the
remaining observation bypasses and creation routes. Frontend signature/parameter
changes include generated catalogues, UniFFI regeneration where relevant, and
Python package tests. No computational work moves into Python.

## 7. Deferred extension — MSv3 profiles

This is not a current work packet or completion gate. If a concrete need arises,
first audit the actual schema/producer and approve a named profile. Reuse the
casacore persistence layer; implement explicit association mapping rather than
assuming DATA_DESC_ID and v2 weight layout. Share the native block and mutation
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
the v4 aggregation benefit still reaches the production route. Audit imports,
runtime dependencies, resource ownership, and removal of displaced domain paths.

Publish user-facing format/capability documentation, explicit conversion limits,
creation examples, and source/sink rustdoc. Update current architecture and the
accepted decisions to the implemented reality. Stronger independent reviewers
review the integrated system, not just the sum of green component tests. Owner
approves completion only with no unresolved scope or acceptance reduction.

## 9. Evidence and executable gates

The baseline is [`TESTING.md`](../../TESTING.md), the
[representative matrix](../../resources/imaging-architecture/representative-science-matrix.json),
and the [performance skill](../../.agents/skills/casa-rs-imaging-performance/SKILL.md).
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
No statistically supported regression in the named critical v2 workloads is
accepted without an explicit owner decision. An increase in steady-state
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
