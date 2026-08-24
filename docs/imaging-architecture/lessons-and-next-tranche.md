# Imaging architecture lessons and next-tranche contract

Truth class: normative programme delivery
Last reality check: 2026-08-22
Verification: `just docs-check`; `just arch-check`

This document records the delivery consequences learned while implementing and
merging T01-T12 and T14, together with the subsequently archived multi-ticket
T13 composition experiment. It refines
ticket order and ownership; it does not reduce any #486 outcome, Acceptance
Contract, persistent interoperability requirement, or deletion ratchet.

The migration matrix remains the sole capability/product inventory. GitHub
issues remain the authoritative work record. This document defines how the
remaining owners meet those records without recreating cross-ticket authority.

## Lessons from the first tranche

### A dependency means a landed interface

A ticket may depend only on an interface merged to `main`, not on an
unreviewed commit, stash, composition branch, or anticipated type from another
ticket. Exploratory composition is useful evidence, but it cannot become the
acceptance implementation for any participating ticket.

If a required owner interface is absent, stop at that seam and correct the
ticket graph. Do not bridge the gap with a raw digest, caller-filled map,
test-only public constructor, placeholder completion, compatibility adapter, or
second authority.

### Planning, completion, and publication are different phases

The logical compiler owns immutable commitments. A scientific owner later
mints completion evidence from the actual bounded execution. Product
publication consumes an exact set of those typed completions and yields a
seal. These records have distinct identities and schema revisions:

1. compiler-owned commitment;
2. owner-minted, attempt-bound completion;
3. planned product generation;
4. authorized product-generation seal; and
5. durable publication result.

None may be reconstructed from another record's digest. Runtime transports the
closed evidence envelope and enforces attempt/fence placement; it does not
reinterpret science or maintain a second source-role catalog.

### Exact evidence is derived, not declared

Caller-supplied `ProductKind` staging maps, raw artifact digests, broad count
reports, and open string maps collapse identities and create parallel
authorities. Exact product staging derives from one Product Graph publication
member/layout ledger. Exact observation completion derives from the compiled
selection plus the actual ordered sample traversal. Exact receipt validation
uses closed typed projections and rejects missing, additional, reordered, or
unknown members.

Capability-scoped completion owners are affine, fresh for one execution
attempt, and available only after the owning work node's fences settle. A
physical row-read completion is not a full Selected Observation traversal; a
weighting-generation completion is not a weighting replay completion.

### Bounded streams must remain fallible and one-pass

An interface that accepts only borrowed elements from an ordinary `Iterator`
quietly requires retaining the entire stream. Native observation and
reconstruction interfaces therefore consume owned or lending-equivalent
bounded blocks and preserve source errors, including a failure discovered on
the terminal poll. Coverage, content identity, and resource measurement are
computed in the same bounded pass; no full-MeasurementSet row list, hidden
prepass, or uncharged duplicate block is permitted.

The Observation Snapshot remains the storage-owner logical-manifest
commitment. T17 adds a separate content-derived Selected Observation generation
from the actual canonical sample stream. The latter can demonstrate equal
content across distinct logical sources but never replaces the snapshot,
provenance, access capability, or attempt evidence.

### Visibility is the irreversible boundary

T08 established the required publication choreography: durably prepare the
exact staged result and terminal candidate, perform the sole external
visibility operation once, then promote or retain fail-closed reconciliation
evidence. No fallible scientific validation, receipt mutation that can change
the run result, retry, or alternate publisher is legal after visibility.
Pre-publication artifacts are `Staged`, never `Published`.

### Merge checkpoints are architecture gates

The first tranche showed that waiting for a large composition branch increases
semantic conflict and hides ticket ownership. Merge a reviewed ticket when its
deep interface is complete and it unblocks another ticket. A checkpoint is
appropriate even when the programme is far from complete.

Each active ticket has one clean worktree and one linked pull request. A mixed
experiment is preserved under an explicitly non-acceptance branch and is mined
ticket by ticket. Completed work is not left unmerged merely to batch a wave.

### Direct ticket closure policy

For #486, these rules supersede process text only. Every T01-T68 technical
contract, Acceptance Contract, scientific limit, CASA-interoperability
requirement, resource law, deletion requirement, full-wave gate, and final
programme gate remains unchanged.

1. **One real ticket.** A ticket is active only with a linked open pull request
   containing a material code or acceptance-test commit, or while an
   issue-named gate is running. Worktrees, plans, reading, agents, and intent do
   not count. Keep one implementation ticket active; allow a second only when
   both depend solely on merged interfaces and touch no common ownership
   surface.
2. **Code, gate, or blocker.** Within 45 minutes of selection, open the linked
   pull request with a material code/test commit, start a required pre-edit
   named gate, or report the exact failed command or contract, file/type/symbol,
   evidence, and next action. During non-gate work, another 45 minutes may not
   pass without a material commit or concrete blocker. Ask only at the existing
   stop points. In-scope Rust API changes are already approved.
3. **One acceptance boundary.** Run only ticket-required or directly affected
   identity, schema, resource, architecture, and immediate-seam gates. Generic
   workflows, `just verify`, GUI, docs, Python, unrelated crates, and broad
   campaigns are non-gating unless the issue requires them. One independent
   reviewer may block only by citing a ticket clause or programme invariant and
   showing the violating code or missing/failing evidence. Preferences,
   generalization, compatibility padding, unrelated tests, and speculative
   hardening cannot block. Findings arrive once; permit at most one consolidated
   repair and a closure check limited to those findings and repair-caused
   regressions.
4. **Close immediately.** When the named gates pass and the single review is
   clear, merge the reviewed SHA, close the issue, and record the commands,
   results, and review decision under standing programme authority. Do not wait
   for generic CI, another approval or review, dashboard refresh, cleanup, or
   release. An exact user instruction to merge or close a named pull request
   as-is overrides process after known deficits are reported. The full-wave gate
   and final programme-wide verification after T68 remain mandatory.

The 45-minute clock pauses while a named gate is visibly running; record its
command and start once, then its result. The post-repair closure check is not a
second review and may not reopen design or inspect untouched work.

## Corrected next tranche

| Order | Ticket | Sole outcome in this tranche | Explicit exclusion |
|---|---|---|---|
| 1 | T13/#499 | Product Contract, Product Graph topology, exact publication-layout ledger, and atomic store protocol | No future scientific completion catalog and no real product seal cutover |
| 2 | T15/#501 | Private synthetic walking skeleton across the landed T01-T14 interfaces | No public fake source/completion types and no second runner |
| 3a | T17/#503 | Bounded Selected Observation values, access/traversal validation, content generation, and owner-minted completion | Physical ObservationRead evidence alone cannot satisfy traversal |
| 3b | T28/#514 | Solver-independent model-generation owner, ingest/reprojection, and Model Delta application | No dependency on Major Cycle or Product Generation Authority |
| 4 | T18/#504 | Frozen global weighting generation and distinct bounded replay | No caller-count completion and no chunk-local weighting |
| 5 | T19/#505 | Serial CPU complete-data operator consuming only weighted blocks and replay completion | No re-binding of W and no product authority |
| 6 | T20/#506 | Major-Cycle reconciliation joining T19 output with the T28 model lifecycle | No independently forgeable normal-state/final-model pair |
| 7 | T21/#507 | Bounded Högbom Model Deltas against named model generations | No authoritative residual mutation |
| 8 | T22/#508 | Continuum product algorithms plus the first real Product Generation Authority, planned-generation/seal cutover, and atomic publication | No raw/manual generation path retained |
| 9 | T23/#509 | Continuum Acceptance Contract, matrix transfer, and legacy-route deletion | No native-to-legacy fallback |

T17 and T28 may proceed in parallel after T15 because neither owns the other's
interface. T20 waits for both the complete-data operator and model lifecycle.
This replaces the stale T28-after-T20 ordering without changing either
ticket's scientific outcome.

### Independent landed-interface tracks

The live blocker graph also makes T50/#536 and T61/#547 ready. They may proceed
in separate worktrees without becoming dependencies of the continuum tranche:

- T50 owns prepared implementation artifacts such as convolution functions,
  spectral maps, and kernels. A `PreparedArtifact` is not a scientific
  Product Graph artifact, product-generation member, or publication seal. T50
  may reuse lower-level atomic-storage mechanics but cannot share or extend the
  product authority. Its content-addressed store uses a private casa-rs schema;
  CASA CFS/WTCFS caches remain read-only sources through the existing validated
  adapter, with no mutation or CASA-visible sidecar.
- T61 projects only the canonical request and matrix semantics already merged
  to `main`. It does not predeclare a later ticket's control, default,
  capability, or validation rule. Each later owner extends the same catalog in
  the merge that lands its stable contract.

If either track touches the same receipt, identity, or checker files as the
continuum tranche, merge one reviewed owner first and rebase the other on that
landed interface. File-level parallelism is not permission for competing
semantic owners.

## Target native module ownership

The current package table in `ARCHITECTURE.md` remains descriptive until each
owner lands. The target direct workspace dependencies after the relevant
cutover are:

| Owner | Direct workspace dependencies |
|---|---|
| `casa-imaging-model` | none |
| native Selected Observation path in `casa-ms` | `casa-imaging-model` plus storage/foundation crates; no legacy, application, task-runtime, or imaging-runtime dependency |
| `casa-imaging-reconstruction` | `casa-imaging-model` |
| `casa-imaging-products` | `casa-imaging-model`, `casa-imaging-reconstruction` |
| `casa-imaging-runtime` | `casa-imaging-model`, `casa-ms`, `casa-imaging-reconstruction`, `casa-imaging-products` |

- `casa-imaging-model` remains dependency-free and owns compiler commitments,
  logical Product Graph topology, and closed backend-free value schemas.
- `casa-ms` is already the logical observation layer. T17 makes its native
  Selected Observation path depend inward on the model and own retained source
  access, canonical bounded traversal, and traversal completion. The current
  crate's legacy/application-shaped dependencies may not leak into that path;
  T17 removes or relocates them as necessary before the path is classified
  native. The model never depends outward on `casa-ms`.
- `casa-imaging-reconstruction` depends only on the model (plus ecosystem
  numerics), consumes model-owned block values through execution composition,
  and owns model, weighting, replay, complete-data, and normal-state algorithms
  and their opaque completions. It does not import MeasurementSet/storage APIs.
- `casa-imaging-products` depends on the model and reconstruction owners. It
  owns the entire product-generation construction capability: typed source
  catalog, planned generation, artifact identities, authority, seal, product
  algorithms, and publication projection. Leaving raw generation construction
  in the model would preserve the bypass and make this module shallow.
- `casa-imaging-runtime` depends inward on the owners it schedules. It retains
  physical plans, leases, layouts, attempts, fences, receipt I/O, and the sole
  publication capability; it does not own scientific completion meaning.

No dependency is added merely to predeclare this shape. The owning ticket adds
the crate/edge, machine-readable policy, matrix ownership, tests, and caller
migration atomically.

## Ticket and merge discipline

- Begin a ticket from current `origin/main` after every listed blocker is
  closed and its interface is present there.
- Fix ownership and public shape before parallel fixture, implementation, and
  evidence work begins inside the ticket.
- Run focused owner-interface tests and affected composition gates. A
  downstream branch is not a substitute for the owning ticket's test surface.
- Before opening the next dependent ticket, merge the reviewed predecessor and
  rebase the next clean worktree on that merge.
- Treat schema/identity changes as a declared cascade. Update every affected
  projection, pin, receipt validator, and matrix/checker ratchet in the owning
  change; do not leave recomputation placeholders in an acceptance branch.
- Mechanical governance revisions, content digests, generated baselines, and
  checker constants that faithfully record an already-approved in-scope change
  are implementation bookkeeping and require no separate user approval. Stop
  only when such an update changes scope, scientific or interoperability
  semantics, an acceptance threshold, or another externally meaningful
  contract.
- Keep status dashboards as derived views of GitHub issue/linked-PR state.
  Manual fallback data is conservative and never an alternate work record.

## Persistent interoperability boundary

The new commitment, completion, generation, seal, plan, and receipt records are
CASA-RS control/evidence schemas. They do not alter casacore MeasurementSet or
image-table persistence. Any proposal for a new MS sidecar, intrinsic dataset
identifier, co-committed receipt, or other CASA-visible persisted structure
still stops for a separate interoperability decision and Rust/C++ evidence.
