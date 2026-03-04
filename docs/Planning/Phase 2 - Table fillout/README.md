# Phase Implementation Rules

**IMPORTANT: All agents MUST read and follow AGENTS.md (repo root) before any wave work. The rules, quality gates, and architecture decisions there are mandatory and apply to all waves.**

## This file
Do not be wordy - leave as much context as possible.

## Source of truth
   - casacore-c++ binaries and headers are installed via homebrew, sources are in ../casacore. (Be open to the possibility of bugs in the C++, but likely only in edge cases not the mainline core)

## Wave creation
   - New wave planning files should be created from BACKLOG.md, and after creation should be deleted from BACKLOG.md
   - Name it like "Wave 11 - Implement some new feature.md", where the Wave number is unique and at the end of the list of existing waves

## Wave planning
      - After the Wave .md file is created, review it against the following desiderata:
         * Tests should include extensive testing against all types scalar and array, and extensive 2x2 c++ interop tests to confirm correctness
         * Have simple performance tests with release flags (c++ and rust) looking for gross performance issues (2x cutoff)
         * Demo programs: use existing c++ demo programs where possible, with the relevant c++ code as comments above the rust code lines
         * Closeout artefacts and criteria must be clearly defined and written down
         * Do not be too wordy, assume the wave .md files will primarily read by another agent with limited context room.
         * Prefer a simpler API that allows access to all c++ capabilities over more elaborate ones with many options to achieve the same results.

## Wave assignment to an agent for implementation  
     - Ask the user what to do about any uncommitted/untracked files. He may or may not want to start with a clean slate.
     - append - TAKEN to the wave.md filename.
     - The agent may want to create an additional internal planning process to elaborate the implementation details.

## Wave completion
   - Audit against closeout artifacts and criteria
   - Do a final audit against casacore-c++ checking for anything important capability that has been accidentally (not deliberately) overlooked.
   - Quality gates (pre-commit hook enforces fmt + clippy + SPDX automatically; also run these before closeout):
     * `cargo test --workspace`
     * `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`
     * `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
   - SPDX header (`// SPDX-License-Identifier: LGPL-3.0-or-later`) on all new `.rs` files (enforced by pre-commit hook)
   - Demo program updated or added if the wave adds user-visible workflow changes
   - Lessons learned section added to the wave spec file
   - Commit and push to whatever branch this was developed on. Any merges will be handled separately.
   - Append - FINISHED to the filename
