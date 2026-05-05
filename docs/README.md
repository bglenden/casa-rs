# Documentation Index

Truth class: current descriptive
Last reality check: 2026-04-19
Verification: just docs-check

This directory holds stable project documentation.

## User Interfaces

- [`mac-native-gui-spec.md`](mac-native-gui-spec.md)
  - proposed product spec for an AI-enhanced native macOS radio astronomy
    workbench
- [`mac-native-gui-mockups.md`](mac-native-gui-mockups.md)
  - visual mockups and layout agreement notes for the native macOS workbench
- [`../apps/casars-mac/README.md`](../apps/casars-mac/README.md)
  - SwiftPM commands for the fixture-backed native macOS clickable prototype
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

## Planning And Program Reference

Historical phase plans and program-reference docs live under:

- `docs/tutorial-parity/`
- `docs/Planning/Phase 2 - Table fillout/`
- `docs/Planning/Phase 3 - Quanta Measures Coordinates/`
- `docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/`
- `docs/Planning/Phase 5 - Lattices Coordinates Images/`

Canonical active planning and wave status live in GitHub Issues / Project.
Treat the planning directories as historical or program-reference docs rather
than the live backlog. The imaging parity program remains useful reference
material, but it is not the canonical status surface.

## Documentation conventions

This directory should contain stable reference material, not temporary PR-only
review notes. Architecture and user-facing guides should be written so they can
remain useful after the branch that introduced them is merged.
