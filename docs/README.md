# Documentation Index

Truth class: current descriptive
Last reality check: 2026-07-18
Verification: just docs-check

This directory holds stable project documentation.

## User Interfaces

- [`mac-native-gui-spec.md`](mac-native-gui-spec.md)
  - proposed product spec for an AI-enhanced native macOS radio astronomy
    workbench
- [`mac-native-gui-mockups.md`](mac-native-gui-mockups.md)
  - visual mockups and layout agreement notes for the native macOS workbench
- [`apps/casars-mac/README.md`](https://github.com/bglenden/casa-rs/blob/main/apps/casars-mac/README.md)
  - SwiftPM commands for the fixture-backed native macOS clickable prototype
- [`casars-tui-framework.md`](casars-tui-framework.md)
  - architecture and app-authoring rules for the shell family
- [`casars-calibrate-user-guide.md`](casars-calibrate-user-guide.md)
  - current user-facing guide for the `calibrate` workflow app
- [`task-parameters.md`](task-parameters.md)
  - accepted sparse TOML profile, Last-state, and cross-surface parameter
    contract
- [`scientific-notebooks-and-assistant.md`](scientific-notebooks-and-assistant.md)
  - accepted Markdown notebook, execution receipt, tutorial, Python, local
    corpus, assistant, and prototype-first wave design
- [`assistant-security.md`](assistant-security.md)
  - current Wave 4 sidecar authority, corpus ownership, context-egress,
    approval, isolated execution, and credential boundaries
- [`reference/task-parameters.md`](reference/task-parameters.md)
  - generated catalog of every task and session parameter surface
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
- tutorial learning packs:
  [`tutorial-parity/tutorial-learning-packs.md`](tutorial-parity/tutorial-learning-packs.md)

## Agent And Developer Reference

- [`agent-reference.md`](agent-reference.md)
  - situational workstation, CASA/C++, shared-data, release, install, and TUI
    evidence guidance kept out of the always-loaded root `AGENTS.md`
- [`CASA (C++) bugs.md`](CASA%20(C%2B%2B)%20bugs.md)
  - canonical CASA parity defect notes, including the
    [Hogbom `niter` off-by-one](CASA%20(C%2B%2B)%20bugs.md#casa-hogbom-niter-off-by-one-bug)
- [`apps/casars-mac/AGENTS.md`](https://github.com/bglenden/casa-rs/blob/main/apps/casars-mac/AGENTS.md)
  - scoped native macOS workbench contract

## Program Reference

`docs/tutorial-parity/` keeps the tutorial-parity reference material that is
still in use. Retired phase plans and finished-wave evidence were removed on
2026-10-05; retrieve them from git history at commit `08542713b7`.

Canonical active planning and work status live in GitHub issues and pull
requests.

## Documentation conventions

This directory should contain stable reference material, not temporary PR-only
review notes. Architecture and user-facing guides should be written so they can
remain useful after the branch that introduced them is merged.
