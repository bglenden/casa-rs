# Compatibility

## Parameter document compatibility

Sparse TOML documents carry two independent versions:

- `casars.format` versions the document envelope and TOML conventions.
- `casars.contract` versions the selected task or session definition.

Loading resolves sparse overrides against the current provider definition.
Newly added optional parameters receive current defaults. Omitted parameters
whose defaults changed also receive the new default and produce a compatibility
warning. Renames and type/value changes require an explicit ordered migration;
future or unmigratable versions fail before invocation.

This behavior preserves user intent rather than freezing an exact historical
request. Use a separate resolved run manifest when exact replay is required.
The Python implementation uses the shared Rust runtime and must not maintain an
independent migration or default table.

Managed state normally lives at:

```text
<workspace>/.casa-rs/parameters/<surface-id>/last.toml
<workspace>/.casa-rs/parameters/<surface-id>/last-successful.toml
```

`CASA_RS_STATE_DIR` redirects the managed root without changing workspace path
resolution. Session surfaces use only `last.toml`. An explicit source profile
is never overwritten without an explicit save.

## Provider protocol compatibility

Python task launch metadata comes from the provider-owned application catalog.
An explicit `binary=` argument or module `configure(binary=...)` value selects
that exact file. Otherwise `CASARS_LAUNCH_MODE` selects exactly one policy:

- `installed_suite` (the default) uses the catalog entry's override variable,
  or the single path `<CASARS_SUITE_ROOT>/bin/<executable>`. Without an explicit
  suite root, `~/.local/opt/casa-rs/current` is the root.
- `development_workspace` uses the single path
  `<CASARS_DEVELOPMENT_WORKSPACE>/target/debug/<executable>`.

A missing file is an error in the selected mode. Python does not inspect the
current directory, package ancestors, neighboring build profiles, or `PATH`,
and it never switches launch modes automatically.

Before the first task execution for a resolved binary, the wrapper requires a matching protocol descriptor from `calibrate --protocol-info`.

The current protocol contract is:

- protocol name: `casa_calibration_task`
- protocol version: `1`
- surface kind: `task`

If the binary reports a different protocol name or version, the Python wrapper raises immediately instead of attempting a best-effort invocation. This is the guard against version skew between the Python package and the task binary.

Release tags also build and publish Python package artifacts separately from the
Rust binaries:

- GitHub release assets always receive the built wheels, source distribution,
  suite bundles, standalone binary bundles, and installer script.
- PyPI publication is attempted from the same workflow when `PYPI_API_TOKEN` is configured.

That means Python package compatibility has two layers:

1. package-version compatibility with the published wheel or sdist
2. runtime protocol compatibility with the resolved `calibrate` binary

The `casars.data` surface has its own narrower stability boundary in v1:

- stable in v1: reading existing persistent images and tables
- stable in v1: writing pixel slices into existing persistent images with `Image.put_slice`
- deferred from v1: image creation, coordinate-system authoring, and broader image metadata editing

The direct object-surface contract published by `casars.data.protocol_info()`
and `casars.data.schema_bundle()` is currently:

- protocol name: `casars_data_objects`
- protocol version: `1`
- surface kind: `object`

That boundary is intentional. The v1 compatibility promise covers file-backed data access and pixel updates, not the full CASA image-authoring surface.

The installer-managed suite install layout behind steps 4-6 is:

```text
~/.local/opt/casa-rs/<version>/
  bin/
    casars
    calibrate
  python/
    ...
  wheels/
    ...
~/.local/opt/casa-rs/stable -> ~/.local/opt/casa-rs/<stable-version>
~/.local/opt/casa-rs/rc -> ~/.local/opt/casa-rs/<rc-version>
~/.local/opt/casa-rs/current -> ~/.local/opt/casa-rs/<version>
~/.local/bin/
  casars -> ~/.local/opt/casa-rs/current/bin/casars
  calibrate -> ~/.local/opt/casa-rs/current/bin/calibrate
  casars-stable -> ~/.local/opt/casa-rs/stable/bin/casars
  calibrate-stable -> ~/.local/opt/casa-rs/stable/bin/calibrate
  casars-rc -> ~/.local/opt/casa-rs/rc/bin/casars
  calibrate-rc -> ~/.local/opt/casa-rs/rc/bin/calibrate
```

That lets Python, the TUI, and standalone executables behave as one installed suite while keeping `~/.local/bin` as the only `PATH` entry users generally need.
