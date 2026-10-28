# casa-rs-python

`casa-rs-python` is the Python distribution for `casa-rs`. It installs as
`casars`.

The public surface is split into three concerns:

- `casars.data` for in-process access to persistent CASA-style images, tables,
  and typed MeasurementSet plot documents (drawn with Matplotlib by
  `casars.data.plot_matplotlib`)
- `casars.parameters` for the shared task profile lifecycle
- `casars.tasks` for the generated CASA-named task functions and generic
  `run()` entry point

`casars.imexplore` adds NumPy/Matplotlib helpers for reading and drawing image
planes.

A task call resolves its parameters through the Rust runtime, finds the
task's executable through the Rust resolver, and starts it directly with
`--json-run -`: the values the call set go to its stdin, its log lines reach
`sys.stderr` as they arrive, and its typed reply on stdout says how it ended.
A Python task call saves no Last. Every task draft also exposes the exact
versioned Rust provider invocation; completions retain that projection,
requests the catalog refuses raise `ValueError` before anything starts, and a
task that does not succeed raises `casars.tasks.TaskExecutionError`. Python
does not own provider schemas, parameter defaults, capability interpretation,
or task-result decoding.

The executable comes from the task's `CASARS_<APP>_BIN` override, a suite
root (`CASARS_SUITE_ROOT`, or the installed suite this package belongs to),
or, for a package built from a checkout, that checkout's `debug` or `release`
build. A wheel installed outside a suite, such as one from PyPI, needs
`CASARS_SUITE_ROOT` or the override to run tasks; the in-process
`casars.data`, `casars.tools` and `casars.simutil` need no executables.

ADR-0006 defines the implemented `casars.parameters` API and sparse TOML
profile contract shared with the task executables' command lines. All 26
catalog tasks have generated CASA-named wrappers and concrete typing stubs
under the single `casars.tasks` namespace; the generated implementation stays
private. The wrappers take CASA's parameters; a draft runs with
`TaskParameters.run(...)` or `casars.tasks.run(task, draft)`, and a saved
profile as `tasks.gaincal(**casars.parameters.load(path))`.

Python binds Rust through PyO3 alone. The wheel contains one native module,
`casars._core`, which carries the parameter runtime, task results and
MeasurementSet plot documents as well as the `Image` and `Table` NumPy object
layer and the `casars.tools` classes. See the profile guide for source
precedence, versioning, and managed Last paths.

Documentation:

- project docs: <https://bglenden.github.io/casa-rs/>
- parameter profiles: <https://bglenden.github.io/casa-rs/task-parameters/>
- Rust API docs: <https://bglenden.github.io/casa-rs/rustdoc/>
