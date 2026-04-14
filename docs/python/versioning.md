# Compatibility

`casars.tasks.calibrate` resolves binaries in this order:

1. explicit `binary=` argument on the function call
2. module configuration via `casars.tasks.calibrate.configure(binary=...)`
3. the `CASARS_CALIBRATE_BIN` environment variable
4. the suite root override `CASARS_SUITE_ROOT`, resolved as `<suite-root>/bin/calibrate`
5. a suite-installed sibling binary discovered relative to the installed Python package
6. the conventional user install root `~/.local/opt/casa-rs/current/bin/calibrate`
7. repo-local development binaries in `target/debug` or `target/release`
8. `PATH`

Before the first task execution for a resolved binary, the wrapper requires a matching protocol descriptor from `calibrate --protocol-info`.

The current protocol contract is:

- protocol name: `casa_calibration_task`
- protocol version: `1`

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
