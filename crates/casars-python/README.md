# casa-rs-python

`casa-rs-python` is the Python distribution for `casa-rs`. It installs as
`casars`.

The public surface is split into three concerns:

- `casars.data` for in-process access to persistent CASA-style images, tables,
  and typed MeasurementSet plot documents
- `casars.parameters` and `casars.sessions` for the shared task/session profile
  lifecycle
- `casars.tasks` for the generated CASA-named task functions and generic
  `run()` entry point

The generated task wrappers render a current-contract TOML draft through the Rust
runtime and delegate execution, safety controls, and Last persistence to
`casars run`. They record project-aware attempts as Python-initiated receipts,
accept an optional existing notebook filename or stable ID, and expose a
one-run recording bypass. Python does not own provider schemas, parameter
defaults, task-result decoding, or a second provider subprocess engine.

ADR-0006 defines the implemented `casars.parameters` API and sparse TOML
profile contract shared with CLI, TUI, and GUI surfaces. All 40 catalog tasks
have generated CASA-named wrappers and concrete typing stubs under the single
`casars.tasks` namespace; the generated implementation stays private. The two
browser sessions have generated
conveniences in `casars.sessions`. These profile-aware wrappers support
Defaults, Last, Last Successful, a named TOML file, or an existing typed draft.

Generated UniFFI is the primary Python application binding. The wheel contains
one native `_core` library that exports both the generated frontend symbols and
the deliberately retained PyO3 object layer. PyO3 remains only for `Image` and
`Table`, where high-volume NumPy array transfer is the required exception;
tasks, parameters, sessions, task results, and MeasurementSet plot documents
all use the generated Rust boundary. See the profile guide for source
precedence, versioning, and managed Last paths.

Documentation:

- project docs: <https://bglenden.github.io/casa-rs/>
- parameter profiles: <https://bglenden.github.io/casa-rs/task-parameters/>
- Rust API docs: <https://bglenden.github.io/casa-rs/rustdoc/>
