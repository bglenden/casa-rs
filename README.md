# casa-rs

casa-rs is a native Rust implementation of casacore-compatible persistent data
and related workflows. The workspace now includes both reusable `casa-*`
libraries and terminal applications built on top of them.

This README is for users of the repo's libraries and applications.
Contributor/developer policy is in `AGENTS.md`.

## Documentation

- API docs: [bglenden.github.io/casa-rs](https://bglenden.github.io/casa-rs/)
- docs index: [`docs/README.md`](docs/README.md)
- `casars` framework guide:
  [`docs/casars-tui-framework.md`](docs/casars-tui-framework.md)
- `casars calibrate` user guide:
  [`docs/casars-calibrate-user-guide.md`](docs/casars-calibrate-user-guide.md)

## Current Status

Library crates:

- `casa-types`: scalar/array/record values plus quanta and measures foundations.
- `casa-tables`: table persistence, storage, and schema-facing APIs.
- `casa-ms`: MeasurementSet summaries, selection, and plotting support.
- `casa-lattices`: N-dimensional lattice abstractions and storage backends.
- `casa-coordinates`: coordinate-system support for astronomical images.
- `casa-images`: images, masks, regions, and image-browser support.

Applications:

- `casars`: ratatui shell for interactive `casa-rs` applications.
- `msexplore`: MeasurementSet inspection in `InspectShell`.
- `tablebrowser`: generic table browsing in `BrowserShell`.
- `imexplore`: image browsing plus region/mask workflows in `BrowserShell`.
- `calibrate`: calibration solve/apply/stats/inspection workflows in `WorkflowShell`.

Supporting internal crates include `casa-calibration`, `casa-aipsio`,
`casa-measures-data`, `casa-measures-tools`, `casa-test-support`,
`casars-tablebrowser-protocol`, and `casars-imagebrowser-protocol`.

Naming:

- `casa-*` crates are reusable libraries.
- `casars-*` crates are app/runtime protocol crates for the terminal application layer.
- The repo implements casacore-compatible behavior in native Rust; it is not a Rust wrapper around casacore C++.

## Casacore C++ Module Coverage

Status legend:
- `Available now`: implemented and usable in this repo today.
- `Planned`: explicitly scoped in an existing phase plan.
- `Deferred/Not planned`: intentionally out of current scope.

| casacore-c++ module | casa-rs status | Notes |
|---|---|---|
| `casa` | Partial / Available now | Core value model (`casa-types`) exists; broader `casa` utility surface is not a parity target. |
| `tables` | Available now + Planned | Core table persistence APIs exist; closeout parity tracked in [Phase 2](docs/Planning/Phase%202%20-%20Table%20fillout/README.md). |
| `measures` | Available now + Planned | Bundled EOP data and coordinate conversion support exist; broader parity tracked in [Phase 3](docs/Planning/Phase%203%20-%20Quanta%20Measures%20Coordinates/README.md). |
| `meas` (TaQL UDF) | Partial / Available now | Core subset exists; broader catalog tracked in [Phase 3](docs/Planning/Phase%203%20-%20Quanta%20Measures%20Coordinates/README.md). |
| `ms` | Available now + Planned | Typed MeasurementSet summaries, selection, plotting, and calibration workflows exist; closeout tracked in [Phase 4](docs/Planning/Phase%204%20-%20MeasurementSet%20and%20Derived%20Calibration%20Workflows/README.md). |
| `derivedmscal` | Partial / Available now | Core solve/apply/stats/inspection workflows exist through `casa-calibration`; remaining parity tracked in [Phase 4](docs/Planning/Phase%204%20-%20MeasurementSet%20and%20Derived%20Calibration%20Workflows/README.md). |
| `coordinates` | Available now + Planned | Coordinate-system foundations exist; broader parity tracked in [Phase 3](docs/Planning/Phase%203%20-%20Quanta%20Measures%20Coordinates/README.md) and [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `lattices` | Available now + Planned | Core lattice abstractions and storage backends exist; further closeout tracked in [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `images` | Available now + Planned | Image, mask, region, and browser workflows exist; further closeout tracked in [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `fits` | Deferred/Not planned (full parity) | No full casacore `fits` parity phase; targeted FITS/WCS interop only. |
| `msfits` | Deferred | Deferred in planning; depends on broader FITS and MS parity. |
| `scimath`, `scimath_f` | Not planned | Prefer Rust community math/fitting/statistics crates when needed. |
| `python`, `python3` | Deferred until needed | No current parity target for casacore Python converters/bindings. |
| `mirlib` | Not planned | Out of scope for this Rust implementation. |

Phase progress is tracked in the phase-specific `docs/Planning/Phase */`
directories rather than this README, because the wave counts change frequently.

## Quick Start

From this repository workspace:

```bash
cargo test --workspace
```

## Terminal Launcher

`casars` is the framework-owned ratatui shell family for supported `casa-rs`
applications.

Run it from the workspace with:

```bash
cargo run -p casars
```

Current shipped apps:

- `msexplore` via `InspectShell`
- `tablebrowser` via `BrowserShell`
- `imexplore` via `BrowserShell`
- `calibrate` via `WorkflowShell`

The framework guide for adding new apps lives at:

- [`docs/casars-tui-framework.md`](docs/casars-tui-framework.md)

The current `calibrate` operator guide lives at:

- [`docs/casars-calibrate-user-guide.md`](docs/casars-calibrate-user-guide.md)

Plot text rendering is platform-dependent today. On macOS, `casars` uses
Plotters' system-font (`ttf`) path so charts pick up real platform fonts. On
non-macOS targets, it uses Plotters' `ab_glyph` path instead so the workspace
does not depend on Linux `fontconfig` just to build the launcher and its plots.
That keeps CI portable, but chart text metrics and font appearance may differ
slightly across platforms.

Common keys:

- `Tab`: switch focus between the parameter and result panes
- `Shift-Tab`: move focus backward
- `Enter`: activate the selected row or open a picker
- `[` / `]`: switch result tabs
- `j` / `k` or arrow keys: move through lists and rows
- `r`: run the selected stage or action
- `a`: show or hide advanced fields where supported
- `t`: toggle between `dense_ansi` and `rich_panel` themes
- `x`: cancel the running command
- `q`: quit

Mouse interactions:

- single click: focus a pane, select a field, switch result tabs, or toggle a section header
- double click on a text field: enter edit mode
- wheel scroll: scroll the pane under the pointer
- drag the center divider: resize the parameter/result split and persist the ratio

Launcher-integrated commands still expose `--ui-schema`, but new apps should
not treat a raw schema dump as their primary UX. The shell-family conventions in
`docs/casars-tui-framework.md` are the required contract for new `casars`
applications.

## Minimal Example (`casa-types`)

```rust
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

let temperature = Value::Scalar(ScalarValue::Float64(273.15));

let spectrum = Value::Array(ArrayValue::from_f32_vec(vec![1.0, 2.0, 3.0]));

let metadata = Value::Record(RecordValue::new(vec![
    RecordField::new("name", Value::Scalar(ScalarValue::String("demo".into()))),
    RecordField::new("temperature", temperature.clone()),
    RecordField::new("spectrum", spectrum.clone()),
]));
```

## Demo Programs

Each crate that wraps a C++ casacore module includes a Rust demo program
equivalent to the corresponding C++ test/demo. These demos:

- Show idiomatic Rust usage of the crate's public API.
- Include the essential C++ source as comments for comparison.
- Are runnable via `cargo run -p <crate> --example <name>`.

| Crate | Demo | C++ original | Run |
|---|---|---|---|
| `casa-aipsio` | `t_aipsio` | `tAipsIO.cc` | `cargo run -p casa-aipsio --example t_aipsio` |
| `casa-tables` | `t_table` | `tTable.cc` | `cargo run -p casa-tables --example t_table` |

Demo source lives in each crate's `examples/` directory. The demo logic
is in a `demo` module within the crate, so `cargo doc` renders it alongside
the API docs.

## IERS Earth Orientation Parameter Data

casa-rs bundles a snapshot of the IERS `finals2000A.data` file for automatic
dUT1 and polar motion lookup during coordinate conversions. This data is
compiled into the binary so no external files or network access are needed
at runtime.

### How it works

When you create a `MeasFrame` with `.with_bundled_eop()`, the bundled EOP
table is used automatically for UT1↔UTC conversions and polar motion in
celestial-to-terrestrial coordinate transforms.

The runtime search order for EOP data (via `load_eop()`) is:

1. `$CASA_RS_DATA/finals2000A.data` — environment variable override
2. `~/.casa-rs/data/finals2000A.data` — user-local data directory
3. Bundled snapshot (always available)

### Updating EOP data

The bundled snapshot should be refreshed periodically (the
`bundled_data_not_stale` test will fail when the data is older than 6 months).

**Command-line update** — download the latest data to `~/.casa-rs/data/`:

```bash
cargo run --example update_eop -p casa-measures-data --features update
```

Or specify a custom directory:

```bash
cargo run --example update_eop -p casa-measures-data --features update -- --data-dir /path/to/data
```

**Programmatic update:**

```rust
use casa_measures_data::update::{download_and_install, UpdateResult};
use std::path::Path;

match download_and_install(Path::new("/path/to/data"))? {
    UpdateResult::Updated(path, summary) => println!("Updated: {}", path.display()),
    UpdateResult::AlreadyCurrent(summary) => println!("Already current"),
}
```

**Refreshing the bundled snapshot** (for maintainers preparing a release):

```bash
# Download latest to a temp directory
cargo run --example update_eop -p casa-measures-data --features update -- --data-dir /tmp/eop

# Copy into the crate's data directory and commit
cp /tmp/eop/finals2000A.data crates/casa-measures-data/data/finals2000A.data
git add crates/casa-measures-data/data/finals2000A.data
git commit -m "data: refresh bundled IERS EOP snapshot"
```

### Release checklist

The bundled data staleness test runs automatically with `cargo test --workspace`:

```bash
cargo test -p casa-measures-data bundled_data_not_stale
```

This test fails when the bundled data's last measured entry is older than
180 days. If it fails during a release, refresh the bundled snapshot before
publishing.

## Git Hooks

This repo includes a lightweight pre-commit hook in `.githooks/pre-commit`
that checks staged Rust files for the required SPDX header:
`// SPDX-License-Identifier: LGPL-3.0-or-later`.

Enable it once per clone with:

```bash
git config core.hooksPath .githooks
```

CI still runs the full-repo SPDX check as a backstop.

## License

Licensed under the [GNU Lesser General Public License v3.0 or later](COPYING.LESSER)
(SPDX: `LGPL-3.0-or-later`).
