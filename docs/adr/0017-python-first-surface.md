# ADR-0017: Python and headless task providers are the product surface

Status: accepted
Date: 2026-10-20
Truth class: normative
Supersedes: 0005; 0007; the stateful-session provider and session-surface
parts of 0003 and 0006
Superseded by:

Amended: 2026-10-27, on owner direction (#931): Python binds Rust through
PyO3 only (item 4, Enforcement and Drift detection). This was the first stage
of #931.
Amended: 2026-10-28, on owner direction (#931 PR 3): the `casars` launcher
is retired and Python starts each task's executable directly (items 1 and 4,
Enforcement and Drift detection, in their final wording).

## Context

From May to October 2026 casa-rs carried several interactive surfaces beside
its Python package and command-line tasks: a ratatui terminal launcher with
image and table browsers (`casars`), a native macOS SwiftUI workbench
(`apps/casars-mac`, ADR-0005), a scientific-notebook and AI-assistant program
(`casa-notebook`, ADR-0007), JSONL browser-session providers (`imexplore
--session`, `tablebrowser`) with their own parameter surfaces, and Rust-side
plot rasterisation (PNG/PDF through `plotters`, `image` and `printpdf`). The
experiments showed that one computational codebase and one task/parameter
contract can serve several frontends, but each frontend added code, tests,
CI time (a macOS GUI gate on every pull request) and support crates that
competed with the scientific work.

The project's direction is a Python package that mirrors CASA's Python
interfaces (casatasks-style task calls and `ia`/`tb`-style tools) so casa-rs
can replace CASA in existing scripts. Interactive inspection and plotting are
better served by Python tools (NumPy, Matplotlib, Jupyter) than by
project-owned GUIs.

The last state of the interactive surfaces is preserved at the tag
`ui-experiments-2026-10-20` (commit `42ba2012`).

## Decision

1. The product surfaces are the Python package (`casars`) and the task
   executables listed in the application catalog.
2. casa-rs has no terminal UI, native GUI, notebook or assistant program,
   interactive browser sessions, or Rust-side plot rasterisation. Every
   parameter surface is a one-shot task; `SurfaceKind` and `SurfaceDefinition`
   keep a single task variant so the serialized contracts keep their shape.
3. Plots are typed data. Rust builds plot documents and text point manifests
   (`msexplore`/`plotms --plot-format txt`); Python renders them with
   Matplotlib (`casars.data.plot_matplotlib`, `casars.imexplore`).
4. The Python package binds in-process Rust functionality through PyO3 in
   `casars._core`. Task calls start the catalog-selected task-provider
   executable directly as a child process; a provider may host a task
   family. Python and the executable use the shared task front end in
   `casa-task-runtime`. Ordinary Python task calls neither load nor save
   managed Last state; command-line lifecycle policy is explicit. There is
   no separate `casars` launcher or UniFFI layer.
5. New interactive surfaces, if ever wanted again, start from a new ADR
   rather than from the retired code.

## Consequences

Positive:
- The workspace loses the terminal, GUI, notebook, browser-protocol and
  rasterisation crates and dependencies (`ratatui*`, `crossterm`, `plotters`,
  `image`, `printpdf`, `rusqlite`, UniFFI Swift bindings and others).
- CI no longer needs a macOS GUI job; the required checks are `lint_test`,
  `python_package` and `docs`.
- Contract and runtime code no longer carries session-only branches.

Negative:
- There is no point-and-click inspection; users inspect data from Python.
- `msexplore`/`plotms` no longer write PNG or PDF files.

Neutral / tradeoffs:
- Catalog presentation fields (`show_in_tui`, `show_in_swift`, `shell_kind`,
  `interaction`) and the provider CLI parser schema
  (`casa_ms::presentation::UiCommandSchema`) remain until the CASA-shaped
  Python work reshapes them.

## Alternatives considered

1. Keep the Swift GUI as the primary surface: rejected; Python reaches the
   intended users and existing CASA scripts directly.
2. Keep the TUI as a lightweight viewer: rejected; it duplicated the GUI's
   browsing model and carried terminal-graphics dependencies for little gain.
3. Keep the code dormant behind feature flags: rejected; dormant code still
   costs build time, review and maintenance, and the tag preserves it.

## Enforcement

This decision is enforced by:
- tests:
  - the Python package tests: `test_task_runner.py` (a task call starts the
    executable its catalog override names with `--json-run -`, streams its
    log, and leaves Last untouched on success, failure and interruption) and
    `test_task_end_to_end.py` (a real executable, found in a development
    checkout's build);
  - `scripts/test-task-cli-hosts.py` (every catalog executable is on the
    shared task front end and answers its machine actions);
  - the command line's Last lifecycle and its equivalence with `--json-run`:
    the front end's tests in `casa-task-runtime`,
    `crates/casars-imexplore/tests/imexplore_cli.rs` and the imager's
    `t1::routes`;
  - the provider-contract and catalog tests (26 task surfaces, every
    application a task, no launcher, no session kinds), and the resolver's
    tests in `casa-provider-contracts`;
- lint/import/dependency rules: none beyond the workspace manifests;
- CI checks: `just quick`, `scripts/test-python-package.sh` in the
  `python_package` job (its binding snapshots pin what the binding returns to
  Python, and it tests the built wheel and sdist), the `suite_install` job
  (`scripts/test-install-suite.sh --hide-checkout`: the installed suite's
  commands and Python, with the checkout moved aside), `just docs-check`;
- review trigger: any pull request that adds a UI, rendering or session
  dependency or crate.

## Drift detection

Suspect drift if:
- `Cargo.lock` gains `ratatui`, `crossterm`, `plotters`, `image`, `printpdf`
  or `uniffi`;
- a parameter surface or provider declares a non-task kind;
- a second native module appears beside `casars._core`;
- a crate spawns catalog executables on Python's behalf.

These pair with the direct-launch, no-Last and command-line-equivalence
tests above.
