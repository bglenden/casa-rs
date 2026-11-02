# Agent Operating Contract

Truth class: normative
Last reality check: 2026-11-02
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
- `just quick` is the full local gate (CI runs the same); `just verify` is
  for milestones and releases; `just --list` shows everything else.
- Python package tests: `scripts/test-python-package.sh`. Python binds Rust
  through PyO3 only, in `casars._core` (`crates/casars-python/src`).
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
  (ready means merge when green).
- Do not commit directly to `main`, except docs-only changes that pass
  `just docs-check`.
- Merge `main` into long-lived branches regularly.
- Remove the worktrees and branches you create once their work is merged or
  pushed. `just tidy` lists leftovers already merged into `main`;
  `just tidy --apply` removes them.
- The repository is public; copying its source to any host is fine. Never copy
  credentials, secrets, or non-public datasets.

## Decide, Record, Continue

Agents do not wait for the owner. The owner steers by reading what was
decided, afterwards, not by approving it first.

- Carry an authorized task through implementation, checks, review, CI,
  merge and cleanup, then take the next item of the same issue. Resumed work
  and PRs taken over follow the same rule.
- A choice that is not on the stop list below is yours: make it, record it
  and continue. List each one under `## Decisions` in the PR body (what, why,
  the alternative not taken) and add the `owner-review` label. Nothing waits
  for that review; a decision the owner overturns becomes ordinary follow-up
  work.
- When one item is blocked (an unreachable host, missing data, a failing
  oracle), say why on its issue and go on to the next item. A session ends
  only when no authorized work it can do remains.
- Keep long work's state in one status comment on its issue, edited in place,
  not in handoff documents in the tree.
- Acceptance checks are not weakened to finish. Work that cannot be finished
  stays open on its issue, stated there; the PR does not claim it. A
  relaxation `TESTING.md` reserves for the owner (a matrix-row exception, a
  review-trigger rule) is such open work, not a reason to stop.

Stop and ask only before:

1. deleting or overwriting anything outside the repository and your own
   scratch space (NAS or GLENDENNING data, releases, branches you did not
   create);
2. a change casacore C++ could not read, or one that stops casa-rs reading
   data it wrote before;
3. editing an accepted ADR, or starting a new top-level app or product
   family;
4. taking the internal disk below about 40 GB free;
5. handling credentials, secrets or non-public data.

Everything else is decided and recorded: scope splits, API changes between
workspace crates, task-provider contract versions, algorithm choices, and
CASA deviations.

### CASA deviations

Record each one in `docs/CASA (C++) bugs.md` (inputs, CASA's behaviour,
casa-rs's, and why), decided by its class:

- CASA crashes, aborts, reads out of bounds, loses or corrupts data, or
  contradicts its own documentation or evident intent: casa-rs corrects it.
- CASA's behaviour is deliberate, or users' results depend on it: casa-rs
  follows it.
- A mode casa-rs does not implement yet: refuse it with an error that names
  an issue.
- Anything else: choose, record it under `## Decisions` with `owner-review`,
  and continue.

## Review

- A PR that changes science results, on-disk bytes, or a contract outside
  the workspace (a persisted format, a task's parameters or provider
  contract) gets one independent review before it is marked ready. Docs,
  tests, tooling, and changes whose results are checked unchanged
  (refactors, performance work) need only green CI.
- The reviewer is a fresh agent without this conversation's context: Astra
  (`gpt-6-astra`) at `xhigh` in Codex; `CLAUDE.md` names Claude Code's.
  Brief it with the revision, the diff, the contracts it touches and the
  evidence, and ask it to look for shallow or tautological tests too.
- One round. Fix blocking and should-fix findings or answer them on the PR;
  decline optional ones with a reason. A fix needs no second review unless it
  adds a mechanism the reviewer did not see.
- Start the review once the diff is stable, and keep working while it runs.

## Checks And Merging

- Before pushing, run what the change can affect: `cargo fmt`, clippy
  `-D warnings` and the tests of the crates you touched, and `just
  arch-check`. CI runs the whole workspace; run `just quick` locally only
  when the change reaches across many crates. One green run is enough.
- When the checks are green and any required review is resolved, mark the
  PR ready and run `gh pr merge --auto --merge`; GitHub merges it once the
  required CI checks pass. Report that it is merging and move on.
- The agent that opens or takes over a PR watches it to merge: the host's PR
  monitor (`CLAUDE.md` names the Claude app's), or bounded CLI checks. Rerun
  a job that failed on a known flaky test, fix real failures on the branch,
  and convert the PR back to a draft, saying why, if the fix is not quick.
- "Merge as-is" from the owner waives the review and check gates for that
  PR: merge with `gh pr merge --admin --merge` and record the waiver on it.
- Missing test data or disk space is an environment problem, not a code
  regression; say so.
- Done means CI is green, the PR body says what was run and what it showed,
  the decisions are listed, and the docs match the code.

## Imaging

The imaging foundation (#648, tickets IF-0 to IF-11) is complete. ADR-0016
and its dependency checker stay binding. The plan
`docs/imaging-architecture/imaging-foundation-plan-20261007.md` records the
design as landed (section 5), the review checklist every imaging change is
held to (section 10) and the deviations taken (section 12). An imaging
change runs its T0 and T1 tests, the T1.5 rows its code touches
(`TESTING.md`), clippy `-D warnings` and the tests of the crates it
touches, and the dependency checker; an owner that changes what it keeps
adds a counting law. Evidence runs follow `TESTING.md` "Imaging tiers and
evidence". Open imaging work is in #445, #447–#450, #653, #690, #714 and
#719–#721. Programme #486 and its rules are retired.
