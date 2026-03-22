# casa-rs

Rust foundations for casacore-compatible persistent data structures.

This README is for API users. Contributor/developer policy is in `AGENTS.md`.

## API Documentation

[**Browse the API docs**](https://bglenden.github.io/casa-rs/)

Public API crates:

- `casacore-types`: scalar/array/record value model.
- `casacore-tables`: table-facing API crate (current facade is intentionally small).

`casacore-aipsio` is primarily an internal implementation crate used by table internals.

## Current User-Facing Capabilities

- Scalars including complex and string values.
- N-dimensional arrays via `ndarray`-backed `ArrayValue`.
- Records (`name -> value` fields) with recursive value support.

## Casacore C++ Module Coverage

Status legend:
- `Available now`: implemented and usable in this repo today.
- `Planned`: explicitly scoped in an existing phase plan.
- `Deferred/Not planned`: intentionally out of current scope.

| casacore-c++ module | casa-rs status | Notes |
|---|---|---|
| `casa` | Partial / Available now | Core value model (`casacore-types`) exists; broader `casa` utility surface is not a parity target. |
| `tables` | Available now + Planned | Core table persistence APIs exist; closeout parity tracked in [Phase 2](docs/Planning/Phase%202%20-%20Table%20fillout/README.md). |
| `measures` | Planned | Scoped in [Phase 3](docs/Planning/Phase%203%20-%20Quanta%20Measures%20Coordinates/README.md). |
| `meas` (TaQL UDF) | Planned (core subset) | Core subset in [Phase 3](docs/Planning/Phase%203%20-%20Quanta%20Measures%20Coordinates/README.md); full catalog deferred. |
| `ms` | Planned | Typed MeasurementSet workflows in [Phase 4](docs/Planning/Phase%204%20-%20MeasurementSet%20and%20Derived%20Calibration%20Workflows/README.md). |
| `derivedmscal` | Planned (core subset) | Core derived quantities in [Phase 4](docs/Planning/Phase%204%20-%20MeasurementSet%20and%20Derived%20Calibration%20Workflows/README.md); full parity deferred. |
| `coordinates` | Planned | Coordinate core in Phase 3; broader parity in [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `lattices` | Planned | Scoped in [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `images` | Planned | Scoped in [Phase 5](docs/Planning/Phase%205%20-%20Lattices%20Coordinates%20Images/README.md). |
| `fits` | Deferred/Not planned (full parity) | No full casacore `fits` parity phase; targeted FITS/WCS interop only. |
| `msfits` | Deferred | Deferred in planning; depends on broader FITS and MS parity. |
| `scimath`, `scimath_f` | Not planned | Prefer Rust community math/fitting/statistics crates when needed. |
| `python`, `python3` | Deferred until needed | No current parity target for casacore Python converters/bindings. |
| `mirlib` | Not planned | Out of scope for this Rust implementation. |

Phase progress snapshot by wave file status (2026-03-04): Phase 2 `1/24`,
Phase 3 `0/8`, Phase 4 `0/8`, Phase 5 `0/10` finished.

## Quick Start

From this repository workspace:

```bash
cargo test --workspace
```

## Terminal Launcher

`casars` is a ratatui-based terminal launcher for supported `casa-rs` command
line applications.

Run it from the workspace with:

```bash
cargo run -p casars
```

Current v1 coverage:

- `listobs` with a schema-driven parameter pane
- collapsible parameter sections and sticky UI theme/split preferences
- structured MeasurementSet summary rendering with tabbed result views
- raw stdout/stderr views for troubleshooting
- mouse support for focus, clicks, wheel scrolling, and divider dragging
- cancel support while a command is running

Default keys:

- `Tab`: switch focus between the parameter and result panes
- `Up` / `Down`: move through parameter rows
- `Enter` / `Space`: toggle a section, edit a text field, or toggle a boolean field
- `h` / `l` or `Left` / `Right`: switch result tabs
- `j` / `k` or `PgUp` / `PgDn`: scroll the active result view
- `r`: run the selected application
- `a`: show or hide advanced parameters
- `t`: toggle between `dense_ansi` and `rich_panel` themes and persist the choice
- `x`: cancel the running command
- `q`: quit

Mouse interactions:

- single click: focus a pane, select a field, switch result tabs, or toggle a section header
- double click on a text field: enter edit mode
- wheel scroll: scroll the pane under the pointer
- drag the center divider: resize the parameter/result split and persist the ratio

Launcher-integrated commands should expose `--ui-schema` so `casars` can build
the parameter form from machine-readable metadata emitted by the executable
itself. The intended convention is that argv parsing, `--help`, and
`--ui-schema` all come from the same internal command schema so they stay in
sync.

## Minimal Example (`casacore-types`)

```rust
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

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
| `casacore-aipsio` | `t_aipsio` | `tAipsIO.cc` | `cargo run -p casacore-aipsio --example t_aipsio` |
| `casacore-tables` | `t_table` | `tTable.cc` | `cargo run -p casacore-tables --example t_table` |

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
cargo run --example update_eop -p casacore-measures-data --features update
```

Or specify a custom directory:

```bash
cargo run --example update_eop -p casacore-measures-data --features update -- --data-dir /path/to/data
```

**Programmatic update:**

```rust
use casacore_measures_data::update::{download_and_install, UpdateResult};
use std::path::Path;

match download_and_install(Path::new("/path/to/data"))? {
    UpdateResult::Updated(path, summary) => println!("Updated: {}", path.display()),
    UpdateResult::AlreadyCurrent(summary) => println!("Already current"),
}
```

**Refreshing the bundled snapshot** (for maintainers preparing a release):

```bash
# Download latest to a temp directory
cargo run --example update_eop -p casacore-measures-data --features update -- --data-dir /tmp/eop

# Copy into the crate's data directory and commit
cp /tmp/eop/finals2000A.data crates/casacore-measures-data/data/finals2000A.data
git add crates/casacore-measures-data/data/finals2000A.data
git commit -m "data: refresh bundled IERS EOP snapshot"
```

### Release checklist

The bundled data staleness test runs automatically with `cargo test --workspace`:

```bash
cargo test -p casacore-measures-data bundled_data_not_stale
```

This test fails when the bundled data's last measured entry is older than
180 days. If it fails during a release, refresh the bundled snapshot before
publishing.

## License

Licensed under the [GNU Lesser General Public License v3.0 or later](COPYING.LESSER)
(SPDX: `LGPL-3.0-or-later`).
