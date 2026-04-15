# casa-rs-python

`casa-rs-python` is the Python distribution for `casa-rs`. It installs as
`casars`.

The public surface is split into two layers:

- `casars.data` for in-process access to persistent CASA-style images and tables
- `casars.tasks.calibrate` for higher-level calibration task execution through
  the `calibrate` binary
- `casars.tasks.importvla` for higher-level VLA export archive scans and imports
  through the `casars-importvla` binary

The Python task wrappers do not reconstruct long CLI flag lists. They send one
canonical Rust JSON request to the relevant task binary with `--json-run -` and
require a matching `--protocol-info` response before first use.

Documentation:

- project docs: <https://bglenden.github.io/casa-rs/>
- Rust API docs: <https://bglenden.github.io/casa-rs/rustdoc/>
