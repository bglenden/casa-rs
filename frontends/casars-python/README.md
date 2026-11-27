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

Python calls the shared Rust task adapters through `casars._core`. A call
resolves CASA's parameters, releases the GIL during execution, and streams
its logs to `sys.stderr`. The wheel includes the adapters. A Python task call
leaves managed Last profiles untouched. A task draft can also project the
request for an explicit CLI invocation.

Parameter validation raises `ValueError` or `TypeError`. A refusal by a task
raises `casars.tasks.TaskParameterError`, and a failed run raises
`casars.tasks.TaskExecutionError`. CASA-shaped wrappers return CASA's value.
The generic runner also returns the task's logs and declared products.

ADR-0006 defines the implemented `casars.parameters` API and sparse TOML
profile contract shared with the task executables' command lines. Every
catalog task has a generated CASA-named wrapper and concrete typing stub
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
