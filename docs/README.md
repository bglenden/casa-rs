# Documentation Index

Truth class: current descriptive
Last reality check: 2026-11-27
Verification: just docs-check

Use casa-rs from the Python package `casars` or the task executables in the
application catalog, such as `calibrate gaincal ...`. Python calls the
shared Rust task providers in process through `casars._core`.
[Python-first surface](adr/0017-python-first-surface.md).

## User documentation

- [Install](install.md): the suite installer, release channels, and
  building from source
- [Python](python/index.md): data access, tasks, CASA tools, parameter
  profiles and plotting, with the [data API](python/data.md), the
  [calibrate tasks](python/tasks-calibrate.md),
  [compatibility](python/versioning.md) and the
  [API reference](python/api.md)
- [Calibration tasks user guide](casars-calibrate-user-guide.md)
- [Simulation imaging and analysis](casars-simanalyze-user-guide.md)
- [ALMA and ACA simulation](casars-simalma-user-guide.md)
- [Task parameter profiles](task-parameters.md): sparse TOML profiles, Last
  state, and the task command line
- [Task parameter reference](reference/task-parameters.md) and
  [CASA compatibility](reference/casa-compatibility.md), both generated
- [Rust API](rust-api.md)

The published site is `https://bglenden.github.io/casa-rs/`, with the Rust
API reference at `https://bglenden.github.io/casa-rs/rustdoc/`; both are
built from `main`.

## Contracts and developer reference

- [Provider contracts](provider-contracts.md): the schema bundles,
  application catalog and task front end that the executables and Python
  share
- [CASA (C++) bugs](CASA%20(C%2B%2B)%20bugs.md): every recorded divergence
  from CASA or casacore, and the CASA behaviours casa-rs follows for parity
- [Agent reference](agent-reference.md): workstation, CASA/C++ oracle, data
  and storage locations, and release recipes
- [Release process](release-process.md) for milestone triage and documentation
  cleanup
- `adr/`: accepted architecture decisions
- [MeasurementSet boundary](measurement-set/contracts.md): its rules and
  the obligations for capabilities not yet offered
- [Simulation plan](simulation/simulation-plan-20261020.md): requirements,
  decisions and the deferred phases
- [Tutorial parity](tutorial-parity/README.md): the First Look runner

At the repository root,
[ARCHITECTURE.md](https://github.com/bglenden/casa-rs/blob/main/ARCHITECTURE.md)
describes the crates and their boundaries,
[TESTING.md](https://github.com/bglenden/casa-rs/blob/main/TESTING.md) the
gates and evidence rules, and
[GLOSSARY.md](https://github.com/bglenden/casa-rs/blob/main/GLOSSARY.md) the
domain vocabulary.

This directory holds stable reference material. Open work, status and history
live in GitHub issues and pull requests.
