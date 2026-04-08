# Wave 40 - `casars` guided workflow shell polish

## Goal

Push the new shell-family model from “architecturally present” to “actually
usable”, with `calibrate` behaving like a guided calibration workflow instead
of a schema-filtered form.

## Extracted Scope

- keep the fixed shell-family layout active for shipped apps
- make `calibrate` context, products, chain rows, and callibrary entries native
  workflow content rather than raw argument fields
- add workflow-stage guidance and stage-specific parameter grouping
- prime inspection defaults and diagnostic presets from workflow actions
- auto-advance workflow stage guidance after successful solve/apply/stats runs
- keep real ratatui smoke coverage on `msexplore`, `tablebrowser`, `calibrate`,
  and `imexplore`

## Delivered

- shared shell presentation helpers now cover inspect/browser/workflow overview
  and workflow display primitives
- `WorkflowShell` now owns:
  - native context rows for selected fields / refant / fluxscale roles
  - native products and apply-chain rows
  - native callibrary entry inspection and editing
  - stage guidance rows (`Goal`, `Produces`, `Hint`, `Action`)
  - grouped stage parameters by operator-facing subsection
  - automatic inspection defaults after solve/stats runs
  - automatic recommended-next-stage progression after successful workflow runs
- `InspectShell` and `BrowserShell` now use framework-owned overview helpers
- the smoke harness runs all shipped apps against real local fixtures,
  including a real image fixture for `imexplore`

## Validation

- `cargo fmt --all -- --check`
- `cargo clippy -p casa-calibration -p casars --all-targets -- -D warnings`
- `cargo test -p casa-calibration --lib`
- `cargo test -p casars --lib`
- `scripts/smoke-casars-apps.py`

## Outcome

This is the first checkpoint where the shell-family model is ready for real
interactive user evaluation rather than only developer-facing architectural
validation. The remaining leverage is no longer basic shell structure; it is
future reuse of the same shell primitives for additional workflow apps such as
imaging.
