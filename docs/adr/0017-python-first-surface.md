# ADR-0017: Rust computation and CASA-shaped Python interfaces

Status: accepted
Date: 2026-11-12
Truth class: normative
Supersedes: ADR-0018 and ADR-0019; their original text is in Git history.

The product is the `casars` Python package and the cataloged headless task
executables. Use existing Python tools for notebooks, interactive inspection,
and plot rendering. Rust produces typed plot data. casa-rs ships no separate
GUI, terminal UI, session provider, or launcher.

- Rust owns computation, persistence, and parallelism, including the
  orchestration CASA implements in Python. Keep Rust APIs idiomatic.
- `casars.tasks` presents CASA's names, signatures, and return shapes.
  Python converts arguments and results, maps errors, and starts each
  task's executable directly. The shared task runtime owns defaults,
  normalization, and validation (ADR-0006). Ordinary Python task calls do
  not load or write managed Last state.
- `casars.tools` exposes CASA-named PyO3 classes through `casars._core`,
  the sole native extension. Declare signatures and effective defaults in
  Rust and compare with the installed CASA binding. Share conversions.
  CASA-independent resource controls are keyword-only constructor
  arguments, with no environment lookup. Tool state is `Send`; release the
  GIL during disk access, array copies, and long calls. Legacy
  `casars.data.Image` and `Table` are tracked separately in #839.
- Do not ship `casatasks` or `casatools` alias packages; they would shadow
  CASA. CASA itself is used only for tests and evidence.

The Python package and task CLI tests cover the boundary and compare the
observable behavior with CASA. See [Provider Contracts](../provider-contracts.md)
for invocation and discovery details.
