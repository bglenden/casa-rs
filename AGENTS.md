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
- `docs/adr/`: accepted decisions; binding, and changed or added only after
  external review (see Review)
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
  about 40 GB free, free space first (stop list item 4 below), then ask.
- Evidence needed later never lives only in a temporary directory.

## Work And Git

- Issues and pull requests are the work record. PRs say `Work issue: #N` (or
  `Work source: <reason>`); use `Closes #N` only when merge should close it.
- A PR is the CI gate in front of `main`, not a document. One PR per
  outcome, not per edit. It lands as one squashed commit whose message is
  the PR's title and description, so write those as the commit message: a
  subject line; what changed and why; `Evidence:`; `Decisions:`; and
  `Review:` when there was one. Branch commits are working history and are
  not kept.
- A PR that needs no review opens ready, with auto-merge set at once. One
  that needs a review opens as a draft until its findings are fixed.
- Work longer than a day lands in increments at least daily, refusing what
  is not finished yet, rather than growing one long branch. Merge `main`
  into a branch that does live longer. After a squash merge, start new work
  from `main`; do not reuse the merged branch.
- Do not commit directly to `main`, except docs-only changes that pass
  `just docs-check`.
- Remove the worktrees and branches you create once their work is merged or
  pushed. `just tidy` lists leftovers already merged into `main`, squashed
  or not; `just tidy --apply` removes them.
- The repository is public; copying its source to any host is fine. Never copy
  credentials, secrets, or non-public datasets.

## Decide, Record, Continue

Agents do not wait for the owner. The owner steers by reading what was
decided, afterwards, not by approving it first.

- Carry an authorized task through implementation, checks, review, CI,
  merge and cleanup, then take the next item of the same issue. Resumed work
  and PRs taken over follow the same rule.
- A choice that is not on the stop list below is yours: make it, record it
  and continue. List each one under `Decisions:` in the PR description
  (what, why, the alternative not taken) and add the `owner-review` label. Nothing waits
  for that review; a decision the owner overturns becomes ordinary follow-up
  work.
- Do not end a turn on a report, a recommendation, an offer ("Shall I…",
  "tell me if you want…") or a question whose answer you would act on
  anyway: act on the recommendation and record it. A loop's or tool's own
  stopping rule is a checkpoint, not the end of the task.
- Where a skill asks you to confirm an action on your own work (archiving or
  reinitialising its history, a scope extension inside the stated goal),
  this file is that confirmation.
- A defect you find on the way is fixed in the PR when small, or filed as an
  issue; it is not just mentioned.
- When one item is blocked (an unreachable host, missing data, a failing
  oracle), say why on its issue and go on to the next item. A session ends
  only when no authorized work it can do remains, or at the end point its
  issue or plan names; there, report rather than start unnamed work.
- Before ending a turn that waits on CI, a merge, a review or a run, arrange
  to be woken: the host's PR monitor, or one background wait (such as
  `gh pr checks N --watch`), which is not polling. Never leave the owner to
  ask for status. Before a long or overnight run, check what fails cheaply
  first (inputs, admission, a short dry run), and schedule it yourself.
- Keep long work's state in one status comment on its issue, edited in place,
  not in handoff documents in the tree. A handoff to another agent is that
  comment plus a startup prompt that names no model or host-specific tool.
  On resuming a branch, follow `AGENTS.md` as it is on `origin/main`, not
  the branch's copy.
- Remove your own leftovers (worktrees, branches, scratch, remote copies) as
  the last step, without asking.
- Acceptance checks are not weakened to finish. Work that cannot be finished
  stays open on its issue, stated there; the PR does not claim it. A
  relaxation `TESTING.md` reserves for the owner (a matrix-row exception, a
  review-trigger rule) is such open work, not a reason to stop.

Ask the owner only before the following, asynchronously where the host
allows, and keep working on whatever does not depend on the answer:

1. deleting or overwriting anything outside the repository and your own
   scratch space (NAS or GLENDENNING data, releases, branches you did not
   create);
2. a change casacore C++ could not read, or one that stops casa-rs reading
   data it wrote before;
3. implementing a design for a broad new capability, or changing or adding
   an ADR, before its external review (see Review);
4. taking the internal disk below about 40 GB free, once deleting your own
   disposable artifacts and running the `cleanup-disk` skill (where the
   host has it) cannot keep it above;
5. handling credentials, secrets or non-public data.

Everything else is decided and recorded: scope splits and extensions within
the stated goal, API changes between workspace crates, task-provider
contract versions, algorithm choices, and CASA deviations.

### CASA deviations

Record each one in `docs/CASA (C++) bugs.md` (inputs, CASA's behaviour,
casa-rs's, and why), decided by its class:

- CASA crashes, aborts, reads out of bounds, loses or corrupts data, or
  contradicts its own documentation or evident intent: casa-rs corrects it.
- CASA's behaviour is deliberate, or users' results depend on it: casa-rs
  follows it.
- A mode casa-rs does not implement yet: refuse it with an error that names
  an issue.
- Anything else: choose, record it under `Decisions:` with `owner-review`,
  and continue.

## Review

Three levels, by how far a change reaches.

- **Isolated fixes and features: a demonstration test, no review.** A bug
  fix carries a test that fails before the fix and passes after. A new
  feature carries a test that runs it end to end against an independent
  reference: CASA or casacore where they have the behaviour, otherwise an
  analytic result, never the code's own output. A change to on-disk bytes
  adds the casacore interop cases `TESTING.md` requires. The PR's Evidence
  section names the test and what it showed. Docs, tests, tooling, and
  refactors or performance work whose results are checked unchanged need
  only green CI.
- **Changes that are not isolated: one local review.** These reach shared
  machinery other tasks rely on (table storage managers, the
  MeasurementSet layer, the imaging operator and runtime, the task front
  end) or span several areas, as an imaging speedup touching FFTs, MS I/O
  and the imager does. The agent in charge starts a fresh-context reviewer
  on its own host, even when (as is normal) the reviewer is the more able
  model: Fable at `xhigh` in Claude Code, Astra (`gpt-6-astra`) at `xhigh`
  in Codex. It reads the findings itself; never ask the owner to run, relay
  or announce this review.
- **Designs for broad new capabilities, and ADR changes or additions:
  external review before implementation.** A broad capability is a new
  task or tool family, crate family, app or storage format, or a change to
  architecture several areas depend on. The reviewer is a human or the
  other vendor's model: Astra at `xhigh` for Claude-led work, Fable at
  `xhigh` for Codex-led work. Post the design and a self-contained review
  brief on its issue, ask the owner to arrange the review, and keep working
  on whatever does not depend on it.

No review loops. Each change gets at most one review, one round:

- Fix blocking and should-fix findings, each with its own test, or answer
  them on the PR or issue; decline optional ones with a reason. The fixes
  are never reviewed again, at any level.
- A finding that needs more than a fix (a redesign, new scope) becomes a
  follow-up issue at its own level, not a second round on this change.
- An externally reviewed design is not reviewed again when it is built;
  the implementation takes level 1 or 2 by its own reach.

Reviews are expensive, above all external ones and `xhigh` agents, so
spend them deliberately:

- Use one reviewer, not a panel, and review once: a final, green diff, or a
  stable design, never drafts.
- Batch related small changes into one review.
- Brief it narrowly: the revision, the diff or design, the contracts it
  touches, where the risk is, and what the tests already show, so it does
  not re-derive them. Ask for blocking and should-fix findings, ranked,
  including shallow or tautological tests; not a sweep of the repository.
- When unsure whether a change is isolated, strengthen its demonstration
  test rather than add a review.
- Keep working while a review runs.

## Checks And Merging

- Before pushing, run what the change can affect: `cargo fmt`, clippy
  `-D warnings` and the tests of the crates you touched, and `just
  arch-check`. CI runs the whole workspace; run `just quick` locally only
  when the change reaches across many crates. One green run is enough.
- Set auto-merge with `gh pr merge --auto --squash` when a PR opens ready,
  or when a reviewed draft is marked ready; GitHub merges it once the
  required CI checks pass. Ready means merge when green. Report that it is
  merging and move on.
- The agent that opens or takes over a PR watches it to merge: the host's PR
  monitor (`CLAUDE.md` names the Claude app's), or bounded CLI checks. Rerun
  a job that failed on a known flaky test (and note it on the flake's
  issue), fix real failures on the branch, and convert the PR back to a
  draft, saying why, if the fix is not quick. A timing gate that misses by
  about its noise is rerun before anyone is asked about it.
- "Merge as-is" from the owner waives the review and check gates for that
  PR: merge with `gh pr merge --admin --squash` and record the waiver in
  its description.
- Missing test data or disk space is an environment problem, not a code
  regression; say so.
- Done means CI is green, the PR description (the commit message) says what
  was run and what it showed and lists the decisions, and the docs match the
  code.

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
