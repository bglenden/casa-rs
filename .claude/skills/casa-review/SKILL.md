---
name: casa-review
description: Read-only casa-rs review of a ticket, wave, PR, branch, uncommitted change, or explicitly requested full codebase. Check current scope, science/persistence contracts, ownership, and applicable acceptance evidence.
argument-hint: [wave N | branch BRANCH | full]
allowed-tools: Read, Glob, Grep, Bash(git *), Bash(CARGO_INCREMENTAL=0 cargo *), Agent
context: fork
---

# casa-rs Review

Review the requested source and acceptance evidence without modifying project files. Test commands may create normal build artifacts; do not edit source, stage changes, or commit. Apply the current repository contract, not a historical checklist.

## Scope and authority

Read applicable `AGENTS.md`, `ARCHITECTURE.md`, `TESTING.md`, and the issue or accepted request. Resolve the named wave through the current work record; planning documents are historical unless marked otherwise. An explicit full-codebase review may survey broadly, while branch, ticket, and WIP reviews stay within the changed capability and directly affected contracts.

For branch review, pin the requested base and head and compare their merge-base diff. Use the PR's actual base/head when reviewing a PR. For uncommitted work, inspect `git diff HEAD` and relevant untracked files separately; use `git diff --cached` or `git diff` for explicitly staged-only or unstaged-only requests. Ask about the comparison only when it cannot be inferred reliably. Report the exact reviewed state.

The issue-named gates and the applicable independent-review policy determine acceptance. In programme #486, use the single contract review and focused ticket gates; do not invent per-ticket broad verification or another merge gate. A review does not grant merge, release, or cleanup authority.

## Evidence to examine

Choose the relevant areas below; missing unrelated coverage is not a blocker for a scoped change.

- **Persistence and interoperability:** for changed on-disk behavior, trace corresponding CASA/casacore semantics and inspect the required RR/RC/CR/CC matrix and applicable endian/type/shape/storage-manager cases. Locate current tests through manifests and source, rather than assuming old root-level crate paths. Preserve approved persistence contracts.
- **Science and behavior:** compare the implementation against the accepted outcome and required products, tolerances, selection, weighting, and normalization. A skipped or unreachable case is not a pass. Trace the affected execution path and substantive test coverage.
- **Performance and resources:** assess only the work item's performance/resource contract or a concrete regression risk. Reuse applicable measured evidence; do not impose a universal Rust/C++ ratio or run all benchmarks for every review. Distinguish component timings from end-to-end claims and diagnostic workloads from acceptance.
- **Public docs and examples:** inspect changed APIs, caller migration, examples, and rustdoc against current documentation requirements. Compare relevant upstream semantics/doxygen depth where needed; do not require unrelated demo parity.
- **Ownership and crate boundaries:** use current `ARCHITECTURE.md`, accepted ADRs, manifests, and call/data flow. Check dependency direction and public exposure for the changed boundary; do not freeze an old crate list into the review procedure.

## Verification

Reuse current green results when the reviewed source, build, dependencies, and runtime configuration cannot affect them. Run additional checks only to resolve a material uncertainty or satisfy a required gate. Before long runs, verify data, disk, and the agreed time/resource allowance.

Use documented focused commands from `TESTING.md`, the issue, or the relevant manifest. Set `CARGO_INCREMENTAL=0` for raw Cargo checks. The `casa-test-support` performance targets require `--features performance-tests`; select the actual target/test, and include `--ignored` only when the selected tests are ignored. Inspect executed test names and counts so zero matching tests cannot count as acceptance. Do not copy a generic `vs_cpp` filter as a universal performance gate.

Use available collaboration tools for bounded independent evidence gathering when it helps; otherwise inspect locally. Preserve independent-review requirements when they apply. The parent verifies findings and consolidates duplicate evidence at the acceptance boundary.

## Report

Lead with actionable findings ranked by impact, each with its triggering condition, consequence, precise file location, and governing contract. Distinguish defects from suggestions and unavailable evidence. State the reviewed revisions/worktree state, checks actually executed or reused, and any material limitation. When there are no actionable findings, say so; do not manufacture gaps to fill a fixed five-section report.
