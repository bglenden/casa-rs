# Agent Operating Contract

Truth class: normative
Last reality check: 2026-07-18
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
- `.agents/skills/`: repeatable WDAD procedures
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
  call sites, tests, docs, and examples in the same wave.
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

## WDAD

This repo uses Scaled WDAD v0.4.

- Use `wdad-backlog-to-ready`, `wdad-wave-implementation`, and
  `wdad-pr-merge` for board transitions. Use the sidecar review skills when
  risk justifies them.
- Approved outcome, included issues, acceptance checks, and stop conditions
  are the scope contract. Do not defer or descope them without explicit user
  signoff recorded in the issue closeout and PR.
- Before Review, code waves record a bounded `refactor` pass over touched or
  directly exposed code. No-code waves record why it is not applicable.
- Issue-driven wave PRs include `Wave issue: #N`. Gate or automation repairs
  without a real issue include `Wave source: automation <name>`. Use
  `Closes #N` only when merge should close that issue.

## Stop And Ask Before

- adding a new top-level app or product family
- adding or expanding public APIs, persisted formats, provider-contract
  bundles, or other external contracts; scoped API removal and consolidation
  inside an approved wave do not require separate permission
- changing dependency direction, runtime model, concurrency guarantees, or a
  major performance algorithm
- moving approved outcome, included issues, or acceptance checks into a
  follow-up, deferral, non-goal, or out-of-scope bucket
- expanding a pre-review refactor beyond touched or directly exposed code
- weakening or deleting tests without replacement
- editing accepted ADRs except for explicitly requested supersession metadata
- committing directly to `main`
- merging, pruning branches, or deleting worktrees unless the user invoked
  `wdad-pr-merge` or asked directly

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
  wave gate.
- One current green run in a documented equivalent local or hosted environment
  is sufficient. Do not duplicate a green gate solely for assurance.
- Reuse recent green evidence when no code, test, build, dependency, or runtime
  configuration change could affect it. Documentation-only or review-only
  changes require only the affected checks.
- Release/tag-only smoke, install, coverage, interoperability, and performance
  gates are not routine merge requirements unless requested or required by the
  wave. `TESTING.md` owns the exact matrix.

## Done

A wave is done only after relevant tests pass; one current `just verify` result
or recorded exclusion exists; refactor evidence is recorded or not applicable;
issue closeout records the actual result; docs and ADRs match reality; any
approved-scope deferral records explicit user signoff; and medium/high-risk
work receives the needed sidecar review.
