# Documentation Index

This directory holds stable project documentation.

## `casars` TUI

- [`casars-tui-framework.md`](casars-tui-framework.md)
  - architecture and app-authoring rules for the shell family
- [`casars-calibrate-user-guide.md`](casars-calibrate-user-guide.md)
  - current user-facing guide for the `calibrate` workflow app
- [`provider-contracts.md`](provider-contracts.md)
  - canonical provider schema model for task, session, and object surfaces
- [`tablebrowser-protocol.md`](tablebrowser-protocol.md)
  - protocol contract for `tablebrowser --session`
- [`kitty-graphics-protocol-details.md`](kitty-graphics-protocol-details.md)
  - notes on the kitty graphics backend used by `imexplore`

## Published docs

- MkDocs site root: `https://bglenden.github.io/casa-rs/`
- Rust API reference: `https://bglenden.github.io/casa-rs/rustdoc/`
- install guide: [`install.md`](install.md)
- CASA VLA parity runbook:
  [`casa-vla-importvla-parity.md`](casa-vla-importvla-parity.md)

## Planning

Phase plans live under:

- `docs/Planning/Phase 2 - Table fillout/`
- `docs/Planning/Phase 3 - Quanta Measures Coordinates/`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`
- `docs/Planning/Phase 5 - Lattices Coordinates Images/`

Canonical backlog tracking for the migrated non-imaging phases now lives in GitHub issues.
Use the issue tracker for active deferred work, and treat the planning directories as
process/history docs rather than the live backlog.

## Documentation conventions

This directory should contain stable reference material, not temporary PR-only
review notes. Architecture and user-facing guides should be written so they can
remain useful after the branch that introduced them is merged.
