# Performance Tooling

This directory holds reusable performance-analysis tooling for the repository.

## Layout

- `imexplore/`
  - `README.md`: feature-specific tracing and analysis notes
  - `report.py`: JSONL-to-summary report script

## Conventions

- Runtime code emits structured traces into `/tmp` by default, or into the path
  selected by feature-specific environment variables.
- JSONL is the source of truth for post-run analysis.
- Human-readable `.log` files are convenience summaries.
- New performance features should live under `tools/perf/<feature>/`.
- Shared parsing helpers can be added later under `tools/perf/common/` when more
  than one feature needs them.

## Why This Exists

System profilers can tell us where CPU time is spent, but they do not know
feature-specific semantics such as:

- which movie frame was requested
- whether a frame was dropped or skipped
- whether the plane came from a cache hit or a miss
- how browser, backend, render, and present latency split for one frame

That semantic layer belongs in app-owned traces. Once it exists, system tools
such as Instruments or `sample` can still be used alongside it.
