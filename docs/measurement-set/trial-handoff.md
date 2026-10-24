# MSv2/MSv4 trial handoff

Status: authorized for M0/M1 trial; interfaces not frozen; no production cutover
Truth class: design proposal
Date: 2026-10-24
Design review: [PR #878](https://github.com/bglenden/casa-rs/pull/878)
Design branch: `codex/ms-v4-architecture-plan`

The owner has agreed to proceed with a trial implemented by another agent and
reviewed by the design agent. Start with M0's facts, fixtures and baseline, then
implement M1's executable contracts. This authorizes trial implementation and
the agreed internal refactoring; it does not freeze the interfaces, waive
evidence, approve a merge, or start M2–M5's broad production migration.

## Starting point

Everything needed to understand the design is in this branch or its public
source links. No previous chat, `/tmp` artifact or access to the design agent's
environment is required. The branch contains documentation, not an implemented
MSv4 backend or completed M0/M1 experiments. The implementer must establish the
execution environment and generate the trial evidence.

From a clean checkout, fetch the design branch and create a separate trial
branch. Do not overwrite the design branch or reset an existing working tree:

```sh
git fetch origin codex/ms-v4-architecture-plan
git switch -c codex/ms-v4-contract-trial FETCH_HEAD
git rev-parse HEAD
```

Record that exact starting SHA in the trial PR. The last design snapshot before
this handoff is [`dcb6f0837b2a80ae8ce5703a3453123df333eb14`](https://github.com/bglenden/casa-rs/commit/dcb6f0837b2a80ae8ce5703a3453123df333eb14).
The historical source map uses `221ec71545fe36a758ab0869e7512349b5db7c0e`.
Neither identifier is a measured performance baseline: M0 must retain and
measure the actual starting production code on the trial host. If the branch
incorporates newer production changes, update source references and baselines.

Read, in order:

1. Repository [AGENTS.md](../../AGENTS.md), [TESTING.md](../../TESTING.md) and
   [environment/data guidance](../agent-reference.md). Read applicable domain
   skills before performance or implementation work.
2. [Architecture](architecture.md), then [contracts](contracts.md), especially
   C1–C7, native Measures, array ownership, access/grouping, writes and failures.
   The [walkthrough](walkthrough.html) explains the same design visually.
3. [Implementation plan](implementation-plan.md), M0 and all of M1, including
   the Measures trial and ten-row exit checklist. Later milestones provide
   context, not permission to skip the trial gate.
4. [Pinned source map](README.md#source-map-for-external-reviewers),
   [external references](architecture.md#7-reference-baseline) and PR #878's
   Pro/Fable reviews. Current documents incorporate the selected review input;
   old review suggestions do not supersede the revised contract.

## Implementation and review checkpoints

The implementing agent owns execution and evidence collection. The design
agent independently reviews the checkpoint diff and results through GitHub.
Keep the trial PR draft and base it on `codex/ms-v4-architecture-plan` while
the design PR is unmerged, so its diff isolates implementation. Include
`Work source: owner-authorized M0/M1 trial following #878` and link this handoff.
Do not mark either PR ready or merge without the owner's later instruction.

| Checkpoint | Implementer supplies | Review boundary |
| --- | --- | --- |
| 0 — M0 facts and baseline | Caller/Measures inventory; supported profiles; locked XRADIO/Python and Rust store/codec graph; reproducible small fixtures and independent readers; current production baselines and noise bands; exact commands and prerequisite status. | Review before replacing production paths. Missing prerequisites remain explicit blockers, not successful skips. |
| 1 — executable shared types | Minimal `casa-observation`, native Measures/quanta and `ndarray` views; compilable imaging/solve/update/creation examples; owner/borrow APIs, layouts, typed errors and proposed dependency/ADR changes. | Review concrete public contracts before expanding adapter and consumer work. Keep subsequent API changes focused and accompanied by motivating cases. |
| 2 — real reads and consumers | Actual local v2/v4 reads into the common arrays; the same production imaging operator, calibration grouping/closure and cube/work-unit lifetime probes; system-wide bulk Measures coverage. | Review science, copies, retained memory and W1/admitted parallel execution. A synthetic tensor demonstration alone does not satisfy this checkpoint. |
| 3 — writes, creation and failure | Addressed full/partial writes, aliases/fallbacks and mixed ownership; source-free producer seam; independent CASA/XRADIO reopen; conversion fidelity, store retention and object-failure tests. | Review persistent mappings, unselected/unknown data preservation, failure guarantees and bounded resources. |
| 4 — M1 acceptance | Every M1 packet mapped to passing evidence, real workload comparisons, unresolved findings and a proposed first stable contract revision. | Independent milestone review plus owner acceptance before M2/M3. No production MSv4-support claim from the trial alone. |

These checkpoints sequence the existing plan; they do not reduce its scope.
Every row of the [M1 exit checklist](implementation-plan.md#m1-exit-checklist-and-packet-ownership)
is still required, including `production-access-cost`. Intermediate reviews may
examine partial work, but partial evidence cannot close M1. Prepare exact fixture
recipes and runnable commands before assigning bounded implementation packets;
the trial lead must not leave acceptance criteria for cheaper agents to invent.

## Agreed constraints

- MSv2 + local interferometric MSv4 `VisibilityXds`, initial schema 4.0.1/Zarr
  v3 profile. Defer MSv3, `SpectrumXds`, XRADIO images and remote mutation.
- Rust production computation and I/O; Python may generate fixtures or provide
  independent XRADIO/CASA oracles. Preserve CASA persistent interoperability.
- `ndarray` arrays/views under thin domain types, carrying actual decoded
  numeric data. Native Measures/quanta types are shared directly. Bulk Measures
  covers every supported family, not only frequency or a loop of scalar calls.
- Algorithm-led access; irregular/factored topology and admitted layouts;
  separate scientific groups, compute windows, physical chunks and work units.
  Independent retirement, bounded queues, actual allocation accounting and the
  existing execution/resource owner are requirements.
- Preserve native flags, weights, precision and coordinate semantics; no
  invented data, implicit narrowing, lossy metadata repair or new persisted
  compatibility markers. Use explicit conversion/rejection policies.
- Existing Rust APIs may be reworked. Preserve a runnable baseline for matched
  comparisons; do not retain a second production engine as a compatibility path.
- The agreed dependency direction is `casa-observation -> casa-types` and
  `casa-imaging-model -> {casa-observation, casa-types}`. Include the exact
  policy change and proposed ADR amendment in checkpoint 1's review. Do not
  silently rewrite accepted ADRs or bypass the dependency checker.
- Implementation helpers are local choices. Changes to shared semantics,
  persistent fidelity, resource ownership or acceptance thresholds need a
  focused design/evidence update and review; the owner resolves major tradeoffs.

## Evidence the reviewer needs

Create a compact trial evidence index in the trial branch, for example
`docs/measurement-set/trial/README.md`, and link it from the PR. Include:

- Exact code/build/toolchain, dependency/features/oracle versions, linked FFTW,
  host, worker settings, data selections and cache conditions.
- Fixture generation commands, origin/license and expected schema/results.
  Use existing resolver conventions; do not assume workstation-specific paths
  in the design notes exist on the implementer's machine.
- Exact runnable test/benchmark commands and exit status for each checklist
  row. Add fail-on-missing-prerequisite acceptance mode where existing tests
  skip. New commands are deliverables of the trial, not tools claimed to exist
  in this design branch. Existing gates include `just quick`, `just arch-check`,
  touched-crate clippy and affected science/interop checks from `TESTING.md`.
- Matched science metrics, wall/CPU times, I/O/decode bytes and calls,
  allocations/copies, peak retained/pinned bytes, thread counts and stalls.
  Use family-specific Measures tolerances and the existing science gates;
  determine performance noise from measurements, not an invented percentage.
- Every failure/skip, unavailable oracle, proposed contract change and reviewer
  disposition. A green docs check is not implementation evidence.

Keep reproducible recipes and compact summaries on GitHub. Large inputs/results
follow repository storage rules. Request each review with the trial PR URL,
checkpoint number and exact commit; a reviewer cannot infer another agent's
private working state. The design agent will review the submitted diff and
evidence before recommending the next checkpoint or interface stabilization.
