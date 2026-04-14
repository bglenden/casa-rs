# Compatibility

`casars.tasks.calibrate` resolves binaries in this order:

1. explicit `binary=` argument on the function call
2. module configuration via `casars.tasks.calibrate.configure(binary=...)`
3. the `CASARS_CALIBRATE_BIN` environment variable
4. repo-local development binaries in `target/debug` or `target/release`
5. `PATH`

Before the first task execution for a resolved binary, the wrapper requires a matching protocol descriptor from `calibrate --protocol-info`.

The current protocol contract is:

- protocol name: `casa_calibration_task`
- protocol version: `1`

If the binary reports a different protocol name or version, the Python wrapper raises immediately instead of attempting a best-effort invocation. This is the guard against version skew between the Python package and the task binary.

Release tags also build and publish Python package artifacts separately from the
Rust binaries:

- GitHub release assets always receive the built wheels and source distribution.
- PyPI publication is attempted from the same workflow when `PYPI_API_TOKEN` is configured.

That means Python package compatibility has two layers:

1. package-version compatibility with the published wheel or sdist
2. runtime protocol compatibility with the resolved `calibrate` binary

The `casars.data` surface has its own narrower stability boundary in v1:

- stable in v1: reading existing persistent images and tables
- stable in v1: writing pixel slices into existing persistent images with `Image.put_slice`
- deferred from v1: image creation, coordinate-system authoring, and broader image metadata editing

That boundary is intentional. The v1 compatibility promise covers file-backed data access and pixel updates, not the full CASA image-authoring surface.
