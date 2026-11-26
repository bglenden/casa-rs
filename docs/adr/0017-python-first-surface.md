# ADR-0017: Rust computation and CASA-shaped Python interfaces

Status: accepted
Date: 2026-11-12
Amended: 2026-11-25, clean-sheet module reorganization: `casars.tasks` runs
in-process via PyO3 over `surfaces/casa-tasks`; `casars-cli` stays standalone;
legacy `casars.data` compatibility note removed per ADR-0020.
Truth class: normative
Supersedes: ADR-0018 and ADR-0019; their original text is in Git history.

The product is the `casars` Python package and the standalone `casars-cli` task
executables. Use existing Python tools for notebooks, interactive inspection,
and plot rendering. Rust produces typed plot data. casa-rs ships no separate
GUI, terminal UI, session provider, or launcher.

- Rust owns computation, persistence, and parallelism, including the
  orchestration CASA implements in Python. Keep Rust APIs idiomatic.
- `casars.tasks` presents CASA's names, signatures, and return shapes.
  Python converts arguments and results, maps errors, and runs tasks
  **in-process** through PyO3 over `surfaces/casa-tasks` (with owner-aware
  table lock handoff, per-invocation `Cancel` token and `SIGINT` cleanup, and
  Python `Last`-file isolation) rather than mandatory subprocess spawning of
  external binaries. Standalone `casars-cli` executables expose the same
  surfaces on the command line. The shared task adapter owns defaults,
  normalization, and validation (ADR-0006). Ordinary Python task calls do
  not load or write managed Last state.
- `casars.tools` exposes CASA-named PyO3 classes through `casars._core`,
  the sole native extension. Declare signatures and effective defaults in
  Rust and compare with the installed CASA binding. Share conversions.
  CASA-independent resource controls are keyword-only constructor
  arguments, with no environment lookup. Tool state is `Send`; release the
  GIL during disk access, array copies, and long calls.
- Do not ship `casatasks` or `casatools` alias packages; they would shadow
  CASA. CASA itself is used only for tests and evidence.

The Python package and task CLI tests cover the boundary and compare the
observable behavior with CASA. See [Provider Contracts](../provider-contracts.md)
for invocation and discovery details.
