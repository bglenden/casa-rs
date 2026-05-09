# GUI/TUI Task Execution Matrix

Truth class: checked artifact
Last reality check: 2026-05-08
Verification: `cargo test -p casars-frontend-services task_execution_matrix_covers_catalog_and_known_inventory_gaps`; `swift test --package-path apps/casars-mac --filter WorkbenchStoreTests/testGenericMutatingTaskRequiresConfirmationBeforeStart`

`resources/task-execution-matrix.json` is the canonical machine-readable list
for issue #226 and the already-landed shared-catalog work from issue #231. It
contains one row for every current shared catalog task plus the additional CASA
task families named by the GUI/TUI parity inventory.

Rows where `tui_status`, `gui_status`, or `full_control_status` are not
`invokable`, `covered`, or `launcher` are not issue-closeout evidence. They are
explicit work or signoff points. In particular:

- `mstransform` is in the shared catalog and installed. Its binary emits the
  schema needed for schema-driven TUI invocation and generic Swift task
  invocation.
- `impv`, `imsubimage`, `immath`, `imregrid`, `feather`, and `importfits` emit
  image-analysis schemas and are promoted into the TUI through the shared
  catalog and the generic Swift task panel. They remain partial until
  full-control verification and product refresh evidence are recorded.
- `flagdata` and `flagmanager` have shared catalog/schema rows and can be
  invoked from the TUI and the generic Swift task panel. The Swift store blocks
  these mutating tasks until the user explicitly confirms the mutation/product
  write.
- `split` is represented by `mstransform`; `uvcontsub`, `gencal`, `gaincal`,
  `bandpass`, `fluxscale`, and `applycal` are represented by `calibrate`.
- Remaining provider-gap rows such as `statwt`, `simanalyze`, and `simalma` are
  tracked so they cannot disappear from the parity scope without user signoff.

The Swift GUI and other frontends can read the matrix through
`task_execution_matrix_json()`. Dataset-grounded option lists and defaults are
available through `task_context_options_json(dataset_path)`, which derives
fields, spectral windows, scans, antennas, correlations, data columns, and other
selector values from the same dataset probe used by the GUI.
