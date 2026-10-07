# Agent Operating Contract

Truth class: normative
Last reality check: 2026-10-07
Verification: just docs-check

casa-rs is a native Rust implementation of casacore/CASA libraries and
applications.

## Core Contracts And Priorities

- **Interoperability:** data written here must be readable by casacore C++,
  and vice versa. This is non-negotiable.
- **Agreement with CASA:** outputs should agree with CASA, not bit for bit,
  but typically to about 0.001 normalized RMS. CASA has bugs; a justified
  divergence is fine when it is explained and recorded (see
  `docs/CASA (C++) bugs.md`).
- **Performance is a top priority.** Measure before and after; do not trade
  speed away without saying so.
- **Clarity follows the mathematics.** Structure code around the underlying
  equations. Imaging modes share one structure (one compile/plan/run path and
  shared operators) rather than growing quasi-independent per-mode paths.
  Prefer deleting and consolidating over adding parallel code.

## Where Things Live

- `ARCHITECTURE.md`: crates, boundaries, dependency direction
- `TESTING.md`: which gates to run, CI, test data, GUI testing
- `docs/agent-reference.md`: workstation, CASA/C++ oracle, data and storage
  locations, release recipes
- `docs/adr/`: accepted decisions; binding, and not edited without approval
- `apps/casars-mac/AGENTS.md`: macOS workbench rules
- `.agents/skills/`: casa-rs domain skills (e.g. imaging performance); generic
  workflows come from the user-level skills
- `GLOSSARY.md`: domain vocabulary; use its terms

When sources disagree: code/tests/CI > ADRs > ARCHITECTURE/TESTING > issues.

## Commands And Toolchain

- Rust 1.99 or newer (`rustup update stable`); set `CARGO_INCREMENTAL=0` for
  raw cargo. Install `cargo-nextest` (`brew install cargo-nextest`) so tests run
  in parallel; without it `scripts/test-workspace.sh` runs them serially.
- `just quick` is the normal gate; `just verify` is for milestones and
  releases; `just --list` shows everything else.
- Swift tests: `cargo build -p casars-frontend-services --lib`, then
  `swift test --package-path apps/casars-mac`.
- `scripts/check-imaging-dependencies.py` (in `just arch-check`) enforces the
  imaging crate layering and source rules of ADR-0016; its grandfathered-file
  lists only shrink.

## Engineering Rules

- Before implementing casacore/CASA behaviour, read the upstream C++ and keep
  its semantics unless there is a stated reason to diverge. For parity
  differences, instrument both implementations; do not guess.
- casa-rs tasks are native implementations. CASA is a test and evidence
  oracle only; never ship a task, app, or surface that delegates to CASA
  (casatasks, casatools, or a CASA install) at runtime
  (`scripts/check-no-casa-runtime.py` enforces this).
- Idiomatic Rust, not a C++ API mirror. Search for existing behaviour before
  adding code.
- Crate names: `casa-*` for libraries, `casars-*` for apps and runtimes.
- Public APIs get rustdoc comparable in depth to the casacore doxygen.
- No `TODO`, `FIXME`, `XXX`, or `HACK` without a GitHub issue reference.

## Storage

The internal disk is small. Do not fill it.

- Internal disk: source, builds, and only small durable items needed for quick
  tests on small datasets (`~/SoftwareProjects/casa-rs-evidence/`).
- Large or long-lived artifacts go on the NAS (`/Volumes/home/casa-rs/`). The
  external disk (`/Volumes/GLENDENNING/`) is working space for large datasets
  and runs, never the only copy of anything that matters.
- Development artifacts (intermediate images, sweep outputs, probe results)
  are disposable. Delete them once the code has moved past them; keep only
  what an issue or PR needs as evidence, summarized there.
- Worktrees share the main `target/` (`CARGO_TARGET_DIR`) or delete their own
  `target/` when done; a separate build tree is tens of GB.
- Check free space before large runs. If the internal disk would drop below
  about 40 GB free, stop and ask.
- Evidence needed later never lives only in a temporary directory.

## Work And Git

- Issues and pull requests are the work record. PRs say `Work issue: #N` (or
  `Work source: <reason>`); use `Closes #N` only when merge should close it.
- One PR per outcome, not per edit. Iterate as a draft; mark it ready once
  (see Merging: ready means merge when green).
- Do not commit directly to `main`, except docs-only changes that pass
  `just docs-check`.
- Merge `main` into long-lived branches regularly.
- Remove the worktrees and branches you create once their work is merged or
  pushed. `just tidy` lists leftovers already merged into `main`;
  `just tidy --apply` removes them.
- The repository is public; copying its source to any host is fine. Never copy
  credentials, secrets, or non-public datasets.

## Ask First

- New top-level apps or product families
- Adding or changing public APIs, persisted formats, provider-contract bundles,
  or other external contracts (removing APIs inside approved work is fine)
- Changing dependency direction, the runtime or concurrency model, or a major
  performance algorithm
- Editing accepted ADRs
- Reducing approved scope or acceptance checks, or weakening tests without a
  replacement
- Merging, releasing, or deleting branches and worktrees you did not create

## Merging

- Marking a PR ready for review is the go-ahead to merge: mark it ready, then
  immediately run `gh pr merge --auto --merge`; GitHub merges it once the
  required CI checks on `main` pass.
- Mark a PR ready without asking once its gates are green, any independent
  review it needs is done with every finding resolved, and no deviation or
  question is open for the owner; then report that it is merging. Ask first
  when any of those is missing, when the PR touches an Ask First item, or when
  it is a PR you did not create.
- Science, persistence, and interoperability changes need an independent
  review by a separate agent or person before they are marked ready. Docs,
  tests, and tooling need only green CI.
- If CI fails after a PR is marked ready, auto-merge waits; fix it on the
  branch, or convert the PR back to a draft if the fix is not quick.
- "Merge as-is" from the user waives the review and check gates for that PR:
  merge with `gh pr merge --admin --merge` and record the waiver on the PR.

## Verification And Done

- Run the gates the change can affect; one green run is enough.
- Missing test data or disk space is an environment problem, not a code
  regression; say so.
- Done means the relevant gates are green, the evidence is recorded on the
  issue or PR, and the docs match reality.

## Imaging Foundation (#648, tickets IF-0 to IF-11)

The owner-approved plan
`docs/imaging-architecture/imaging-foundation-plan-20261007.md` and ADR-0016
are the work contract. Each ticket body carries outcome, deletion rows and
acceptance tests; plan section 10 is the review checklist; section 9.3 defines
the review gates R1–R4. Gates per ticket are its T0/T1/T1.5 tests, clippy
`-D warnings` on touched crates, the dependency checker and `just quick`;
`just verify` runs once at IF-11. The Rust API changes, deletions and
dependency-direction changes written in the plan are pre-approved; persisted
CASA-interoperable formats, cleanup and release still need approval. Record
disagreements between plan and code under `## Deviations` on the ticket as
they occur. Programme #486 and its rules are retired.
