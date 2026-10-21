# Agent Operating Contract

Truth class: normative
Last reality check: 2026-10-20
Verification: just docs-check

casa-rs is a native Rust implementation of casacore/CASA libraries and
applications. Its user-facing surface is the Python package and the headless
task providers (ADR-0017); there is no GUI or terminal UI.

## Core Contracts And Priorities

- **Interoperability:** data written here must be readable by casacore C++,
  and vice versa. This is non-negotiable.
- **Agreement with CASA:** outputs should agree with CASA, not bit for bit,
  but typically to about 0.001 normalized RMS. CASA has bugs; a justified
  divergence is fine when it is explained and recorded (see
  `docs/CASA (C++) bugs.md`).
- **Bitwise reproducibility is not a goal.** Deconvolution is nonlinear and
  runs on low-S/N data, so results that differ only through rounding (FFTW
  plan choice, summation order, a near-tie broken the other way) are
  equally valid; compare them by normalized RMS. Do not give up speed or
  add tie-breaking rules to make runs bit-identical.
- **Performance is a top priority.** Measure before and after; do not trade
  speed away without saying so.
- **Clarity follows the mathematics.** Structure code around the underlying
  equations. Imaging modes share one structure (one compile/plan/run path and
  shared operators) rather than growing quasi-independent per-mode paths.
  Prefer deleting and consolidating over adding parallel code.

## Where Things Live

- `ARCHITECTURE.md`: crates, boundaries, dependency direction
- `TESTING.md`: which gates to run, CI, test data
- `docs/agent-reference.md`: workstation, CASA/C++ oracle, data and storage
  locations, release recipes
- `docs/adr/`: accepted decisions; binding, and not edited without approval
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
- Python package tests: `scripts/test-python-package.sh`; regenerate the
  UniFFI module with `scripts/generate-frontend-bindings.sh` after changing
  `casars-frontend-services`.
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
- Computation lives in Rust. The Python package presents CASA's names and
  signatures, converts data and starts the work. Logic that CASA writes in
  Python, such as task scripts and `simutil`, is ported to Rust (ADR-0018).
- Idiomatic Rust, not a C++ API mirror. Search for existing behaviour before
  adding code.
- Crate names: `casa-*` for libraries, `casars-*` for apps and runtimes.
- Public APIs get rustdoc comparable in depth to the casacore doxygen.
- Size limits are enforced, not reviewed. Functions stay under clippy's
  `too_many_lines` threshold (`clippy.toml`, code lines only). Files stay
  under 1,500 lines (`scripts/check-file-length.py`, in `just arch-check`).
  Split new code rather than add an exception; the existing exceptions only
  shrink (#705).
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
- Changing persisted formats, provider-contract bundles, or other external
  contracts. Rust APIs between workspace crates are not external contracts:
  add, change or remove them whenever that serves the work (a measured
  performance gain, a simpler design) and record the change on the issue or
  PR
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
- The agent that opens a PR owns it until it merges: watch its CI through
  the host's PR monitor (`CLAUDE.md` names the Claude app's), rerun a job
  that failed on a known flaky test, fix real failures on the branch without
  asking, and report outcomes rather than progress. Auto-merge waits while
  CI is red; convert the PR back to a draft if the fix is not quick.
- "Merge as-is" from the user waives the review and check gates for that PR:
  merge with `gh pr merge --admin --merge` and record the waiver on the PR.

## Verification And Done

- Run the gates the change can affect; one green run is enough.
- Missing test data or disk space is an environment problem, not a code
  regression; say so.
- Done means the relevant gates are green, the evidence is recorded on the
  issue or PR, and the docs match reality.

## Imaging

The imaging foundation (#648, tickets IF-0 to IF-11) is complete. ADR-0016
and its dependency checker stay binding. The plan
`docs/imaging-architecture/imaging-foundation-plan-20261007.md` records the
design as landed (section 5), the review checklist every imaging change is
held to (section 10) and the deviations taken (section 12). An imaging
change runs its T0 and T1 tests, the T1.5 rows its code touches
(`TESTING.md`), clippy `-D warnings` on touched crates, the dependency
checker and `just quick`; an owner that changes what it keeps adds a
counting law. Evidence runs follow `TESTING.md` "Imaging tiers and
evidence". Open imaging work is in #445, #447–#450, #653, #690, #714 and
#719–#721. Programme #486 and its rules are retired.
