# Documentation Index

Truth class: current descriptive
Last reality check: 2026-10-20
Verification: just docs-check

This directory holds stable project documentation.

## User Surfaces

casa-rs is used from the Python package `casars` and the headless `casars`
task host (`casars run`, `casars params`); see
[ADR-0017](adr/0017-python-first-surface.md). All computation is in Rust,
and the Python package is only its interface; see
[ADR-0018](adr/0018-rust-computes-python-is-the-interface.md).

- [`python/index.md`](python/index.md)
  - the Python package: data access, tasks, parameters, and plotting
- [`casars-calibrate-user-guide.md`](casars-calibrate-user-guide.md)
  - user guide for the calibration tasks
- [`task-parameters.md`](task-parameters.md)
  - accepted sparse TOML profile, Last-state, and CLI/Python parameter
    contract
- [`reference/task-parameters.md`](reference/task-parameters.md)
  - generated catalog of every task parameter surface
- [`provider-contracts.md`](provider-contracts.md)
  - canonical provider schema model for task and object surfaces

## Published docs

- MkDocs site root: `https://bglenden.github.io/casa-rs/`
- Rust API reference: `https://bglenden.github.io/casa-rs/rustdoc/`
- install guide: [`install.md`](install.md)
- CASA VLA parity runbook:
  [`casa-vla-importvla-parity.md`](casa-vla-importvla-parity.md)

## Agent And Developer Reference

- [`agent-reference.md`](agent-reference.md)
  - situational workstation, CASA/C++, shared-data, release, and install
    guidance kept out of the always-loaded root `AGENTS.md`
- [`CASA (C++) bugs.md`](CASA%20(C%2B%2B)%20bugs.md)
  - canonical CASA parity defect notes, including the
    [Hogbom `niter` off-by-one](CASA%20(C%2B%2B)%20bugs.md#casa-hogbom-niter-off-by-one-bug)

## Program Reference

`docs/tutorial-parity/` keeps the tutorial-parity reference material that is
still in use. Retired phase plans and finished-wave evidence were removed on
2026-10-05; retrieve them from git history at commit `08542713b7`.

Canonical active planning and work status live in GitHub issues and pull
requests. The active simulation programme (#841) follows
[`simulation/simulation-plan-20261020.md`](simulation/simulation-plan-20261020.md).

The proposed [MeasurementSet architecture and implementation plan](measurement-set/README.md)
covers shared native observation contracts for MSv2 and XRADIO MSv4, with MSv3
deferred pending a concrete need. It is a design under review, not a statement
of implemented format support.

## Documentation conventions

This directory should contain stable reference material, not temporary PR-only
review notes. Architecture and user-facing guides should be written so they can
remain useful after the branch that introduced them is merged.
