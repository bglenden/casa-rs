# GUI/TUI Task Execution Matrix

Truth class: checked artifact
Last reality check: 2026-05-09
Verification: `just quick`; `cargo test -p casars-frontend-services`; `cargo test -p casars`; `swift test --package-path apps/casars-mac`

`resources/task-execution-matrix.json` is the canonical machine-readable list
for issue #226 and the already-landed shared-catalog work from issue #231. It
contains one row for every current shared catalog task plus the additional CASA
task families named by the GUI/TUI parity inventory.

Each row records the scope-control fields required by #226: surface kind,
interaction model, row disposition, approved closeout scope, provider/schema
source, protocol name/version, install status, GUI/TUI visibility and
invocation status, provider resolution path, frontend exposure,
context-option/default/validation source, full parameter coverage, omitted
controls, mutation class, confirmation/dry-run/backup/restore behavior, smoke
evidence, and any signoff reference.

`resources/tutorial-task-parameter-audit.json` is the companion checked artifact
for tutorial-level usefulness. It records the task parameters used by the
current tutorial parity notes and runners, then the frontend-services test loads
the corresponding UI schemas and fails if any listed parameter is missing or
hidden from the TUI. The Swift tests also build a real `casars-imager` command
from the schema to verify that tutorial-grade tclean controls round-trip through
the GUI generic runner.

Rows where `tui_status`, `gui_status`, or `full_control_status` are not
`invokable`, `covered`, or `launcher` are not issue-closeout evidence. They are
explicit work or signoff points. In particular:

- `mstransform` is in the shared catalog and installed. Its binary emits the
  schema needed for schema-driven TUI invocation and generic Swift task
  invocation.
- `imager` is in the shared catalog and installed. The GUI now uses the same
  schema-backed generic task panel as the TUI instead of the former reduced
  dirty-imaging panel. Its controls cover the tutorial `tclean` modes audited
  in `resources/tutorial-task-parameter-audit.json`, including MFS/cube,
  MT-MFS, W-projection, multiscale, automask, primary-beam correction,
  model-column saving, outlier files, and managed output.
- `impv`, `imsubimage`, `immath`, `imregrid`, `feather`, and `importfits` emit
  image-analysis schemas and are promoted into the TUI through the shared
  catalog and the generic Swift task panel.
- `flagdata` and `flagmanager` have shared catalog/schema rows and can be
  invoked from the TUI and the generic Swift task panel. The Swift store blocks
  these mutating tasks until the user explicitly confirms the mutation/product
  write.
- `split`, `plotms`, `imhead`, `imstat`, `uvcontsub`, `gencal`, `gaincal`,
  `bandpass`, `fluxscale`, and `applycal` are first-class shared-catalog tasks.
  Their schemas are projected from the underlying provider binaries with hidden
  defaults or subcommands where needed, so the GUI and TUI expose task-shaped
  parameters instead of the broader provider surface.
- `imcollapse`, `imfit`, `impbcor`, `widebandpbcor`, `concat`, `statwt`,
  `hanningsmooth`, `clearcal`, `delmod`, `ft`, `imcontsub`, `simanalyze`, and
  `simalma` are shared-catalog tasks backed by the local CASA `casatasks`
  installation through `casars-casa-task`. This exposes the real CASA task
  parameter surface to both the GUI and TUI while native Rust implementations
  remain future work. Their rows record the user signoff that real scientific
  workflow validation belongs in a separate wave.
- `plotcal` is a shared-catalog task backed by the native calibration plot
  payload builder because the local CASA 6.7.5 task package no longer exports a
  `plotcal` function.

The Swift GUI and other frontends can read the matrix through
`task_execution_matrix_json()`. Dataset-grounded option lists and defaults are
available through `task_context_options_json(dataset_path)`, which derives
fields, spectral windows, scans, antennas, correlations, data columns, and other
selector values from the same dataset probe used by the GUI.
