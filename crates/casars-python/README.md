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

The generated task wrappers render a current-contract TOML draft through the Rust
runtime and delegate execution, safety controls, and Last persistence to
`casars run`. Every task draft also exposes the exact versioned Rust provider
invocation; completions retain that projection, and requests the catalog
refuses raise `ValueError` before process launch. Python does not own provider
schemas, parameter defaults, capability interpretation, task-result decoding,
or a second provider subprocess engine.

ADR-0006 defines the implemented `casars.parameters` API and sparse TOML
profile contract shared with the `casars` command line. All 27 catalog tasks
have generated CASA-named wrappers and concrete typing stubs under the single
`casars.tasks` namespace; the generated implementation stays private. These
profile-aware wrappers support Defaults, Last, Last Successful, a named TOML
file, or an existing typed draft.

Python binds Rust through PyO3 alone. The wheel contains one native module,
`casars._core`, which carries the parameter runtime, task results and
MeasurementSet plot documents as well as the `Image` and `Table` NumPy object
layer and the `casars.tools` classes. See the profile guide for source
precedence, versioning, and managed Last paths.

Documentation:

- project docs: <https://bglenden.github.io/casa-rs/>
- parameter profiles: <https://bglenden.github.io/casa-rs/task-parameters/>
- Rust API docs: <https://bglenden.github.io/casa-rs/rustdoc/>
