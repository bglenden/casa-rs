# Native macOS Workbench Contract

Truth class: normative
Last reality check: 2026-07-18
Verification: just docs-check

## Authoritative Sources

- ADR-0007 owns notebook, tutorial, assistant, trust, and prototype-first
  architecture.
- Root `TESTING.md` owns GUI test selection, remote-worker setup, signing,
  privacy-safe project locations, and evidence policy.
- `README.md` in this directory owns developer commands and launch recipes.
- `gui-journeys.json` is the single journey policy; execute it through
  `script/gui_acceptance.py`.

## Boundaries

- Swift owns native interaction and presentation. Rust owners provide
  persisted notebook, task, provider, data, and scientific semantics; do not
  redefine those contracts in Swift fixtures or DTOs.
- Fixture schemas and deterministic adapters remain test/prototype details and
  must not become public or persisted contracts.
- A material interaction change governed by ADR-0007 returns to a launchable,
  deterministic prototype and explicit interaction approval before production
  adapters are changed.
- Do not add a second GUI journey planner, selector override, timeout table, or
  artifact-copy policy beside `gui-journeys.json`.

## Verification

- XCTest/XCUIAutomation is the executable end-to-end layer for user-visible
  behavior. Add stable accessibility identifiers and update the smallest
  complete critical journeys when interaction behavior changes.
- Prefer core/store tests, debug-state checks, and deterministic capture during
  implementation. Run the consolidated GUI gate at coherent handoff and
  pre-review points; use focused UI tests only to diagnose a consolidated-gate
  failure.
- GUI automation is remote-first when the configured logged-in worker is
  available. One current green local or remote run is sufficient.
- Local GUI automation owns an exclusive foreground window. Finish compilation
  first, announce the focus-taking window, and run the batch uninterrupted.
- Use only the privacy-safe disposable project and signing paths documented in
  `TESTING.md`; never improvise locations under `~/Library` or another app's
  container.
