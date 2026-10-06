# Agent Operating Contract

Truth class: normative
Last reality check: 2026-10-06
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
  raw cargo.
- `just quick` is the normal gate; `just verify` is for milestones and
  releases; `just --list` shows everything else.
- Swift tests: `cargo build -p casars-frontend-services --lib`, then
  `swift test --package-path apps/casars-mac`.
- Editing a file pinned in `resources/imaging-architecture/migration-matrix.json`
  requires `python3 scripts/refresh-baseline-digests.py` in the same change.

## Engineering Rules

- Before implementing casacore/CASA behaviour, read the upstream C++ and keep
  its semantics unless there is a stated reason to diverge. For parity
  differences, instrument both implementations; do not guess.
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
- One PR per outcome, not per edit. Iterate as a draft; mark it ready once.
- Do not commit directly to `main`, except docs-only changes that pass
  `just docs-check`.
- Merge `main` into long-lived branches regularly.
- Remove the worktrees and branches you create once their work is merged or
  pushed.
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

- Merges need the user's go-ahead and green CI (or a local `just quick` where
  CI cannot cover the change).
- Science, persistence, and interoperability changes also need an independent
  review by a separate agent or person. Docs, tests, and tooling merge on green.
- "Merge as-is" from the user waives the review and check gates for that PR;
  record the waiver on the PR.

## Verification And Done

- Run the gates the change can affect; one green run is enough.
- Missing test data or disk space is an environment problem, not a code
  regression; say so.
- Done means the relevant gates are green, the evidence is recorded on the
  issue or PR, and the docs match reality.

## Programme #486 (Imaging Architecture)

Until T68 closes #486, its tickets follow the closure policy in
`docs/imaging-architecture/lessons-and-next-tranche.md`: issue-named focused
gates plus one independent contract review, and green gates with no blocker
authorize merge and closure. In-scope non-persistent Rust API changes are
pre-approved; persisted formats, cleanup, and release still need approval.
