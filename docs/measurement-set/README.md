# MeasurementSet architecture review

Status: draft revision 4; owner iteration pending, not implementation authorization
Truth class: design proposal
Date: 2026-10-23
Repository baseline: `221ec71545fe36a758ab0869e7512349b5db7c0e`

This proposal gives MSv2 and XRADIO MSv4
one native Rust observation interface. Algorithms request scientific grouping
and memory layout; each backend realizes that request with bounded I/O. Existing
Rust interfaces may change completely. Persistent interoperability, science,
and measured performance remain acceptance requirements.

## Reading order

For a guided introduction, open the standalone [HTML walkthrough](walkthrough.html)
in a browser (download the file when reading on GitHub). It builds the revision 4
proposal in twelve steps, with interactive layout/completion examples, review
questions, and links to the exact draft sources. It works offline; optional notes
stay in the browser and can be exported. The Markdown contracts below remain the
detailed proposal; the walkthrough does not finalize any decision.

| Document | Review focus |
| --- | --- |
| [Architecture](architecture.md) | Boundaries, decisions, format scope, and alternatives; start here for the overall design. |
| [Intermediate contracts](contracts.md) | The arrays and metadata shared by storage, imaging, calibration, and flagging; the principal design review. |
| [Implementation plan](implementation-plan.md) | Sequenced work packets, evidence, agent responsibilities, and change control. |

Everything needed to review the proposal is committed here or linked to public
sources. Review does not require access to the author's execution environment.
The Rust sketches describe contracts; they are not implemented or compilable
library APIs yet. No performance or format-interoperability experiment is
claimed by this documentation PR.

This revision incorporates Pro and Fable's reviews of commit `526d706c` and
owner discussion of work groups that finish at different rates, canonical units,
explicit access-pattern negotiation, and a provisional `VisibilityBatch` name
that avoids observatory terminology.
The [mosaic converter evidence](architecture.md#converter-evidence-for-mosaics)
traces actual XRADIO field mapping and upstream VLA/ALMA/VLASS tests, including
the consistency assumption that casa-rs must validate before a v4 write.
Step 04 now expands the nested data group into a
[concrete numeric batch](contracts.md#concrete-batch-example), with selectable
visibility/flag/weight elements and explicit
[autocorrelation scalar semantics](contracts.md#autocorrelations-and-real-valued-data).
It distinguishes [buffer ownership from array views](contracts.md#buffer-ownership-and-array-views):
the batch carries actual numerical data, and a borrowed view provides direct
access without copying. [Using `ndarray`](contracts.md#using-ndarray) is the
owner-agreed direction, with thin scientific types and M1 checks for buffer
reuse, ownership transfer and performance; concrete types remain open for review.
The owner also requires [native Measures types and bulk conversion](contracts.md#shared-measures-types-and-bulk-conversion),
authorizing substantial Measures refactoring. The revised ownership keeps those
types and conversions in the Measures foundation, directly used by the batch.
Bulk operation applies across all supported Measures/quanta families and future
additions. LSRK↔TOPO is a non-exhaustive example; the M1 gate requires a family
coverage matrix, including scalar/vector and nonlinear operations.
The documents and PR remain provisional: neither addressing a comment nor an
agent recommendation finalizes the design. The MSv2 + MSv4 scope, MSv3 deferral,
Measures requirement and use of `ndarray` are owner directions; they do not
approve the remaining contracts or authorize broad implementation before review.

## Decisions for the owner and independent reviewers

1. **D1 — Shared representation:** approve irregular and rectangular topology,
   typed array views, explicit numeric precision, and separate presence,
   validity, flags, weights, and coordinate context. Guarantee canonical coordinate
   units by default and bind explicit unit requests before computation. Use native
   Measures types/views and prepared bulk transformations directly. Reject a universal
   per-physical-MS-row object interface or a universal dense cube. Factored
   coordinates can accompany the same sample-major values without a transpose.
2. **D2 — Algorithm-led access:** approve hard layout/grouping requirements
   with bounded fragments, sequential or bounded concurrent group realization,
   proven group completion, and backend chunk aggregation within those
   requirements. Calibration grouping does not require a whole solve in memory.
3. **D3 — Ownership and writes:** approve reusable borrowed batches with
   independent work-unit lifetimes and explicit shared-allocation pinning, opaque
   session-local inverse mappings, native-role broadcast/group alias scopes,
   and bulk patches. The proposed local v4 profile replaces individual objects
   atomically; it does not promise a dataset transaction or power-loss durability.
4. **D4 — Boundaries:** approve a small shared `casa-observation` contract crate,
   using the authoritative Measures/quanta types in `casa-types`, with
   `casa-imaging-model` depending on both; keep compiled geometry in imaging and
   storage adapters under `casa-ms`. Consolidate MS-specific conversion helpers
   into Measures. This supersedes a dependency-free observation base and is an
   explicit proposed amendment of the model's current dependency rule.
   Keep one existing execution/resource owner.
5. **D5 — Support scope:** approve native local MSv4 read/create/update first,
   no Python on the processing path, preservation of existing MSv2 support,
   with MSv3 deferred until concrete datasets justify a named profile.
   Object-store mutation and XRADIO
   image persistence require later, explicit work.
6. **D6 — Sequence and stability:** approve an early executable contract trial
   across imaging, calibration, writes, and source-free creation before broad migration. Freeze
   semantics after that trial, with reviewed changes still possible.

Approval can accept some decisions and request changes to others. The owner
confirmed the MSv2 + MSv4 scope and deferral of MSv3 on 2026-10-23. The detailed
contracts and other decisions remain proposed. MSv3 is a beta specification
with producer-specific variants; no generic MSv3 interoperability is promised.
The interface should accommodate a future adapter without making it a
completion dependency now.

The main choices for the next owner discussion are D2's concurrent grouping and
closure, D3's object-replacement/durability boundary, D4's shared primitive owner,
and the proposed creation/conversion policies (frequency chunking for cubes and
explicit loss/rejection rules). Dependency versions, exact chunk extents and
performance feasibility remain M0/M1 evidence questions. The updated plan makes
those experiments mandatory rather than treating them as settled by prose.

## GitHub review protocol

Keep the proposal PR in **draft** while the owner and independent reviewers,
including Anthropic Fable, review it. Leave findings on the relevant Markdown
lines using GitHub's normal review process. Each finding should identify the
violated invariant or concrete counterexample, the consequence, and a proposed
resolution when known. Use blocking findings for correctness, unsupported
claims, unbounded memory, performance regressions, or irreversible information
loss; distinguish those from editorial preferences.

The author responds in the PR and updates these documents. Resolve a finding
only when its resolution is in the diff or the owner explicitly accepts the
tradeoff. Record accepted D1–D6 decisions in the PR; absence of a comment is not
approval. Architecture and implementation-plan approval are separate from
passing documentation checks. The accepted ADRs remain binding until their
explicit amendments are reviewed; this proposal changes none of them.

Implementation PRs link the accepted design revision and their work issue.
Milestone reviewers receive exact commands, fixture provenance, and compact
results in GitHub, including failures and skips. Large data need not live in
Git, but fixture recipes, small legal fixtures, environment requirements, and
numerical summaries must not exist only in a private filesystem. The
[implementation plan](implementation-plan.md#10-review-and-change-control)
specifies how the cheaper implementers and stronger reviewers work together.

## Source map for external reviewers

The following casa-rs links are pinned to the inspected baseline
`221ec71545fe36a758ab0869e7512349b5db7c0e`. Use the PR's **Files changed** view
for the proposed documents and normal repository browsing for context. Search
the named symbols in these files when line numbers have moved. Implementation
PRs must update the baseline and link the changed production entrypoints.

| Question | Source to inspect |
| --- | --- |
| How do current arrays and requests work? | [selected_observation_buffer.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/selected_observation_buffer.rs): `SelectedObservationBufferRequest`, `SelectedObservationNumericColumns`, buffer capacity and read footprint. |
| Where are physical rows and current buffer limits bound? | [visibility_buffer.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/visibility_buffer.rs), [selected_observation/access.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/selected_observation/access.rs), and [content_plan.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/selected_observation/content_plan.rs). |
| What is raw metadata versus imaging projection today? | [selected_observation_sample.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-imaging-model/src/selected_observation_sample.rs): `SelectedObservationRunRow`; [operator sample.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-imaging-operator/src/sample.rs): `SampleBlock`. |
| What Measures types and bulk mechanisms already exist? | [Measures modules](https://github.com/bglenden/casa-rs/tree/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-types/src/measures); [MFrequencyConverter](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-types/src/measures/frequency.rs#L201) retains ordered aberration/dUT1 state but its scalar `convert_hz` still finds a route and prepares frames; [MeasFrame and its cache](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-types/src/measures/frame.rs#L99); [UvwFrame](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-types/src/measures/baseline.rs#L30) prepares geometry for many baselines. |
| Which duplicate Measures interfaces should be consolidated? | [Imaging frequency/epoch types](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-imaging-model/src/geometry.rs#L678), [frame-enum translation](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/selected_observation/spectral_evaluation.rs#L642), and [MS-specific prepared frequency factors](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/spectral_selection.rs#L1815). Preserve the [ordered spectral-envelope behavior](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/selected_observation/spectral_evaluation.rs#L122), rather than benchmarking only against a repeated scalar conversion the production path already avoids. |
| Who owns imaging source execution and resources? | [runtime source_access.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-imaging-runtime/src/source_access.rs), [runtime pass](https://github.com/bglenden/casa-rs/tree/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-imaging-runtime/src/pass), and [application source.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-imaging-application/src/imaging/source.rs). |
| What does calibration currently assume? | [session.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-calibration/src/session.rs), [solve/grouping.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-calibration/src/solve/grouping.rs), and [execute.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-calibration/src/execute.rs). |
| What write performance/semantics must survive? | [visibility_write.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/observation_owner/visibility_write.rs), [write_session.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/write_session.rs), [flagging.rs](https://github.com/bglenden/casa-rs/blob/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-ms/src/flagging.rs). |
| Which producer paths also need migration? | [simulation write pipeline](https://github.com/bglenden/casa-rs/tree/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-simulation/src/write) and [casa-vla](https://github.com/bglenden/casa-rs/tree/221ec71545fe36a758ab0869e7512349b5db7c0e/crates/casa-vla). |
| What existing architectural constraints bind the proposal? | [ADR-0008](../adr/0008-casacore-storage-and-bounded-measurementset-writes.md), [ADR-0009](../adr/0009-mathematical-imaging-architecture.md), [ADR-0015](../adr/0015-run-local-imaging-ownership-without-content-attestation.md), [ADR-0016](../adr/0016-imaging-foundation.md), [dependency policy](../../resources/imaging-architecture/dependency-policy.json). These relative links resolve in the PR branch. |
| What counts as validation? | [TESTING.md](../../TESTING.md), [performance skill](../../.agents/skills/casa-rs-imaging-performance/SKILL.md), [representative science matrix](../../resources/imaging-architecture/representative-science-matrix.json). |
| What defines MSv4 and its stack? | [Architecture reference baseline](architecture.md#7-reference-baseline): pinned XRADIO schema, group definitions, padding, conversion and Zarr configuration; Zarr specification, Xarray conventions, and Rust library links. |

For a focused independent review, read the proposed contracts alongside the
existing buffer/write APIs and the pinned XRADIO schema. Check whether any real
source data would lose meaning, whether an algorithm must know the storage
format, whether promised writes are implementable with bounded memory, and
whether the milestone evidence can actually disprove the performance claims.
