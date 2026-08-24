# Agent Operating Contract

Truth class: normative
Last reality check: 2026-08-04
Verification: just docs-check

## Purpose

Implement native Rust libraries and applications for casacore-compatible
persistent data while preserving on-disk interoperability.

## Scope Of This File

Keep this always-loaded contract short and practical. Put only durable,
repo-wide behavior here. Use the closest authoritative source for details:

- `ARCHITECTURE.md`: workspace boundaries and dependency direction
- `TESTING.md`: test selection, evidence, CI, data, and GUI gate policy
- `docs/agent-reference.md`: situational workstation, CASA, data, release, and
  TUI evidence recipes
- `apps/casars-mac/AGENTS.md`: native macOS workbench rules
- `.agents/skills/`: repository-specific domain procedures; generic development
  procedures come from user-level skills and are not vendored here
- accepted ADRs: durable design decisions

## Truth Order

1. Code, tests, CI, and interoperability behavior.
2. Accepted ADRs.
3. `ARCHITECTURE.md` and `TESTING.md`.
4. GitHub issues and board state.
5. `docs/Planning/` is historical unless a file says otherwise.

## Essential Commands

- Discover commands: `just --list`
- Setup: `just setup`
- Fast loop: `just quick`
- Pre-review gate: `just verify`
- Architecture/docs: `just arch-check`, `just docs-check`, `just graph`

Use `TESTING.md` to select heavier gates. For raw Cargo checks outside `just`,
set `CARGO_INCREMENTAL=0`.

## Engineering Direction

This project is early and is not constrained by an existing external user
base. Prefer the best long-term code, architecture, API, and testing shape over
the smallest local patch, even when that means changing more in-repo code now.

- Reduce and consolidate public API surface when it improves the design.
  Remove weak APIs, duplicate paths, compatibility shims, and awkward
  abstractions instead of preserving them solely because they exist. Update
  call sites, tests, docs, and examples in the same change.
- Private crates and substantial dependencies are allowed when they create a
  cleaner ownership boundary or materially improve the implementation. Assess
  license, build, distribution, and maintenance effects rather than rejecting
  them because of size alone.
- Before adding library functionality, search the existing Rust surface for
  reusable or composable behavior. Unless repository search is already the
  current agent's normal role, delegate that search to a sub-agent so the main
  implementation context stays focused.
- Before implementing behavior that exists in CASA/casacore C++, inspect the
  relevant upstream task, tool, or library path and preserve its semantics
  unless there is an explicit reason to diverge.
- For parity or correctness differences, instrument both implementations
  instead of relying on blind parameter experiments or speculative fixes.
- Prefer idiomatic Rust over direct C++ API mirroring; this is not a wrapper
  around casacore C++.
- Use the shared least-squares helper for polynomial or linear least-squares
  solves; do not add ad hoc normal-equation or Gaussian-elimination solvers.
- Use `casa-*` for reusable libraries and `casars-*` for app/runtime crates.
- Public API docs belong in source comments rendered by `cargo doc`; new
  casacore-C++ functionality should have roughly corresponding doxygen depth.
- Do not add backlog-style `TODO`, `FIXME`, `XXX`, or `HACK` comments unless
  they reference a GitHub issue.

## Plan First And Anti-Slop

For substantial code changes, plan the target structure before editing. Identify
existing behavior to reuse, the canonical ownership boundary, the
migration/deletion path, and the affected verification. A small local change
needs only a proportionate plan; do not add ceremony for its own sake.

- Prefer the simplest coherent architecture, not the smallest patch or the
  fewest lines.
- Avoid unnecessary defensive handling, type-system escapes, one-use
  scaffolding, narration comments, trivial wrappers, speculative abstractions,
  helper proliferation, deep nesting, and repeated conditional ladders.
- Do not keep appending unrelated decisions to an existing complexity hotspot.
  Redistribute responsibility along real domain or ownership boundaries.
- After correctness, perform a bounded anti-slop pass over touched and directly
  exposed code. Simplify findings inside the approved scope and report larger
  adjacent erosion instead of silently expanding the approved work.

## Work Record And State

GitHub issues and pull requests are the authoritative work record. Generic
shaping, research, TDD, diagnosis, design, review, and conflict-resolution
procedures belong to the globally installed Matt Pocock skills; this repository
stores only casa-rs-specific policy and domain guidance.

The casa-rs repository source is fully open source and publicly available on
GitHub. Agents may copy repository source to any execution host or location
needed for authorized project work; do not classify repository source copying
as sensitive-data egress. This does not authorize copying credentials, secrets,
unrelated personal data, or non-public external datasets.

- Before implementation, the issue or an equivalent user-approved record states
  the outcome, included issues, non-goals, acceptance evidence, and stop
  conditions.
- Approved outcome, included issues, and acceptance checks must not be reduced,
  deferred, or moved out of scope without explicit user signoff recorded in the
  issue and pull request. Newly discovered adjacent work may use a follow-up.
- Issue-driven pull requests include `Work issue: #N`. Automation or gate
  repairs without a real issue include `Work source: automation <name>`. Use
  `Closes #N` only when merge should close that issue.
- The GitHub Project `Status` field is the only board state: `Todo` means queued
  but not active, `In Progress` means implementation or review is active, and
  `Done` means the authoritative issue is closed with its evidence recorded.
  Opening or reopening an issue moves it to `Todo`; a linked open pull request
  moves it to `In Progress`; closing that pull request without merge returns it
  to `Todo` unless another linked pull request remains open; closing the issue
  moves it to `Done`. Draft/readiness and review state live on the pull request,
  not in another board field. A merge does not mean `Done` until the issue closes.

For programme #486, the direct ticket closure policy in
`docs/imaging-architecture/lessons-and-next-tranche.md` supersedes the generic
state rule above. A ticket is `In Progress` only with a linked open pull request
containing a material code or acceptance-test commit, or while an issue-named
gate is running. Worktrees, assignments, plans, reading, delegated agents, and
intent are not activity. Normally one implementation ticket is active; a
second is allowed only when both depend solely on merged interfaces and touch
no common ownership surface.

## Final Authority

Implementation authority does not include final merge, branch/worktree cleanup,
or release. Those actions require an independent final review of scope, diff,
acceptance evidence, and current checks, plus explicit user authorization for
the action. The final reviewer must be independent of the implementation pass.

For an exact pull request, an explicit user instruction to merge or close out
"as-is" after the agent has reported the outstanding review or check evidence
is an informed waiver of the independent-final-review and current-check
requirements for that merge. Obey the waiver instead of restoring the default
gate. Record the waived evidence and the user's direction on the pull request
and issue. The waiver does not change the accepted scientific, persistence, or
interoperability contract and does not authorize release, branch/worktree
cleanup, or another pull request.

For programme #486, the single independent contract review defined by the
direct ticket closure policy is the only review gate. When the issue-named
gates are green and that review has no unresolved blocker, standing programme
authority authorizes immediate merge and issue closure without another review
or user approval. An exact instruction to merge or close a named pull request
as-is overrides process after known deficits are reported; no repository rule
may add another review or check. Cleanup, release, and branch/worktree deletion
remain separate stop points.

## Stop And Ask Before

- adding a new top-level app or product family
- adding or expanding public APIs, persisted formats, provider-contract
  bundles, or other external contracts; scoped API removal and consolidation
  inside an approved work item do not require separate permission
- changing dependency direction, runtime model, concurrency guarantees, or a
  major performance algorithm
- moving approved outcome, included issues, or acceptance checks into a
  follow-up, deferral, non-goal, or out-of-scope bucket
- expanding implementation beyond the approved outcome or directly exposed
  supporting work
- weakening or deleting tests without replacement
- editing accepted ADRs except for explicitly requested supersession metadata
- committing directly to `main`
- merging, pruning branches, deleting worktrees, or publishing a release without
  explicit user authorization and an independent final review, except for an
  exact merge covered by the informed as-is waiver above

For programme #486, in-scope non-persistent Rust API changes are already
approved, and intermediate merges are covered by the direct ticket closure
policy. Persisted CASA-interoperable formats, cleanup, release, and
branch/worktree deletion remain stop points.

## Project Boundaries

- Follow `docs/provider-contracts.md` for provider contracts. Versioned schema
  bundles are the boundary contract and must not become a second source of
  science semantics.
- Follow `docs/casars-tui-framework.md` for `casars` TUI work.
- Scientific-notebook work follows ADR-0007. A material interaction change
  returns to its prototype approval gate before production integration.
- Follow `apps/casars-mac/AGENTS.md` for native workbench changes.
- Use `docs/agent-reference.md` for CASA/C++ oracle execution, shared-data
  locations, TUI capture, release, and install recipes.

## Verification

- `just quick` is the normal iteration gate; `just verify` is the default full
  pre-review gate.
- One current green run in a documented equivalent local or hosted environment
  is sufficient. Do not duplicate a green gate solely for assurance.
- Reuse recent green evidence when no code, test, build, dependency, or runtime
  configuration change could affect it. Documentation-only or review-only
  changes require only the affected checks.
- Before long gates, confirm required data and disk headroom. Unavailable data
  and disk pressure, including `No space left on device`, are environmental
  evidence rather than source regressions unless the failure reproduces with
  those prerequisites available.
- Release/tag-only smoke, install, coverage, interoperability, and performance
  gates are not routine pull-request requirements unless requested or required
  by the approved work. `TESTING.md` owns the exact matrix.
- For programme #486 T01-T68, routine `just verify` and generic workflow jobs
  are not ticket gates. Run only the issue-named and directly affected focused
  gates. Broad verification remains mandatory at the explicit full-wave and
  final post-T68 milestones.

## Done

Work is complete only after relevant tests pass; one current `just verify`
result or recorded exclusion exists; the issue and pull request record the
actual acceptance evidence; docs and ADRs match reality; and every
approved-scope deferral records explicit user signoff. Merge, cleanup, and
release remain separate independently reviewed actions.

For programme #486, the direct ticket closure policy supersedes the generic
`Done` rule above: issue-named focused gates and the single contract review are
sufficient for ticket closure. Do not create a per-ticket `just verify`
exclusion or another merge-authority review.
