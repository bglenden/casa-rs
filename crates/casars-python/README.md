# casa-rs-python

`casa-rs-python` is the Python distribution for `casa-rs`. It installs as
`casars`.

The public surface is split into three layers:

- `casars.data` for in-process access to persistent CASA-style images and tables
- `casars.parameters`, `casars.tasks.run`, `casars.tasks.profiles`, and
  `casars.sessions` for the shared catalog-driven task/session profile
  lifecycle
- specialized modules such as `casars.tasks.calibrate` and
  `casars.tasks.importvla` for raw provider request/result object APIs

The common task wrappers render a current-contract TOML draft through the Rust
runtime and delegate execution, safety controls, and Last persistence to
`casars run`. They record project-aware attempts as Python-initiated receipts,
accept an optional existing notebook filename or stable ID, and expose a
one-run recording bypass. Specialized machine-protocol wrappers continue to
send canonical JSON invocations directly to providers and require a matching
`--protocol-info` response before first use; because they have no project
context, they do not infer a workspace or record implicitly.

ADR-0006 defines the implemented `casars.parameters` API and sparse TOML
profile contract shared with CLI, TUI, and GUI surfaces. All 40 catalog tasks
have generated CASA-named wrappers and concrete typing stubs under the
idiomatic `casars.tasks.profiles` namespace (`casars.tasks.catalog` is the
generated implementation module); the two browser sessions have generated
conveniences in `casars.sessions`. These profile-aware wrappers support
Defaults, Last, Last Successful, a named TOML file, or an existing typed draft.

The specialized task modules keep the distinct JSON/provider object protocols
for callers that need their structured request/result types. They do not load
or save parameter profiles, do not update managed Last state, and are not a
second parameter-semantics layer. See the profile guide for source precedence,
versioning, and managed Last paths.

Documentation:

- project docs: <https://bglenden.github.io/casa-rs/>
- parameter profiles: <https://bglenden.github.io/casa-rs/task-parameters/>
- Rust API docs: <https://bglenden.github.io/casa-rs/rustdoc/>
