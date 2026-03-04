# Phase Implementation Rules

## This file
Do not be wordy - leave as much context as possible.

## Source of truth
   - casacore-c++ binaries and headers are installed via homebrew, sources are in ../casacore. (Be open to the possibility of bugs in the C++, but likely only in edge cases not the mainline core)
 
## Wave creation
   - New wave planning files should be created from BACKLOG.md, and after creation should be deleted from BACKLOG.md
   - Name it like "Wave 11 - Implement some new feature.md", where the Wave number is unique and at the end of the list of existing waves
 
## Wave planning
      - After the Wave .md file is created, review it against the following desiderata:
         * Prefer a red/green TDD lifecycle
         * Tests should include extensive testing against all types scalar and array, and extensive 2x2 c++ interop tests to confirm correctness
         * Have simple performance tests with release flags (c++ and rust) looking for gross performance issues (2x cutoff)
         * Demo programs should be written using existing c++ demo programs where possible, with the relevant c++ code as comments above the rust code lines.
         * Implemention items not for API users should be in non-public crates
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
   - Commit and push to whatever branch this was developed on. Any merges will be handled separately.
   - Append - FINISHED to the filename

   ## Possible concerns to incorporate in this document
   
     1. Quality gate commands are missing. Phase 1 explicitly lists cargo fmt, cargo clippy, cargo test, cargo tarpaulin, and RUSTDOCFLAGS doc build. Phase 2 says "audit
     against closeout artifacts and criteria" but doesn't list the commands anywhere. An agent working only from Phase 2 might not know to run tarpaulin or check doc
     warnings. I'd either repeat the commands or add "See Phase 1 README for the standard quality gate commands" as a reference.
     2. SPDX headers, demo updates, lessons learned not mentioned. Phase 1 requires SPDX headers on new files, demo program updates, and a lessons-learned section in each
     wave spec. Phase 2 mentions demo programs in the planning desiderata but not in the completion checklist. Worth adding to "Wave completion" or trusting that each wave
     file will include them.
     4. "Commit and push to whatever branch" is more relaxed than Phase 1's "single focused commit." This seems intentional and fine for multi-commit waves, but worth being
     explicit about whether squashing is desired.
     6. No mention of the AGENTS.md / CLAUDE.md rules. Phase 1 didn't either, but since Phase 2 is explicitly agent-targeted, it might be worth a one-liner: "All agents must
      also follow AGENTS.md."
