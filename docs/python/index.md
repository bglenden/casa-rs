# Python Support

`casa-rs-python` installs as `casars` and splits the public surface into two layers:

- `casars.data` for in-process access to persistent CASA-style images and tables.
- `casars.tasks.calibrate` for higher-level calibration task execution through the `calibrate` binary.

The package deliberately keeps those layers separate.

- `casars.data` is stateful and file-backed. It is intended for interoperability with the Python ecosystem through NumPy-native reads and writes.
- `casars.tasks.calibrate` is stateless at the Python boundary. It serializes one canonical Rust request, invokes `calibrate --json-run -`, and returns the canonical JSON result envelope.

For images specifically, v1 includes pixel-slice writes on existing persistent images. It does not include Python-side image creation, coordinate-system authoring, or general image metadata editing.

`imager` bindings, `MeasurementSet` Python objects, and image creation / coordinate authoring remain out of scope for v1.

Tagged releases build Python artifacts for:

- Linux `x86_64`
- macOS `arm64`

plus a source distribution for environments that need to build from source.

For suite installation and shell `PATH` setup, see the top-level
[Install](../install.md) guide.
